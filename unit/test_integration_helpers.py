"""Pure orchestration guards: no real Git commands, sockets, or server process.

The only real file contents used here are small temporary digest fixtures.
These checks protect launch preconditions; real integration evidence is
reported separately by the explicit integration command.
"""

import hashlib
import os
from pathlib import Path
import socket
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, call, patch

from tvcache_checks.integration import (
    _ready, available_port, binary_digest, run_integration, source_revision, upstream_environment,
)
from tvcache_checks.tree import UnsupportedMetadata


class SourceRevisionTests(unittest.TestCase):
    def setUp(self):
        self.source = Path(__file__).resolve().parent

    def test_clean_root_records_head_after_provenance_checks(self):
        revision = "a" * 40
        with patch("tvcache_checks.integration.subprocess.check_output",
                   side_effect=[str(self.source) + "\n", "", revision + "\n"]) as command:
            self.assertEqual(source_revision(self.source), {"revision": revision, "clean": True})
        base = ["git", "-C", str(self.source)]
        self.assertEqual(command.call_args_list, [
            call(base + ["rev-parse", "--show-toplevel"], text=True, stderr=subprocess.PIPE, timeout=10),
            call(base + ["status", "--porcelain", "--untracked-files=normal"], text=True,
                 stderr=subprocess.PIPE, timeout=10),
            call(base + ["rev-parse", "HEAD"], text=True, stderr=subprocess.PIPE, timeout=10),
        ])

    def test_modified_or_untracked_checkout_is_rejected(self):
        for status in [" M src/main.rs\n", "?? unexpected.py\n"]:
            with self.subTest(status=status):
                with patch("tvcache_checks.integration.subprocess.check_output",
                           side_effect=[str(self.source), status]) as command:
                    with self.assertRaisesRegex(ValueError, "modified source checkout"):
                        source_revision(self.source)
                    self.assertEqual(command.call_count, 2)

    def test_nested_directory_is_not_accepted_as_checkout_root(self):
        with patch("tvcache_checks.integration.subprocess.check_output",
                   return_value=str(self.source.parent)) as command:
            with self.assertRaisesRegex(ValueError, "root of its Git checkout"):
                source_revision(self.source)
            self.assertEqual(command.call_count, 1)

    def test_git_error_is_not_converted_into_clean_provenance(self):
        failure = subprocess.CalledProcessError(128, ["git", "rev-parse"])
        with patch("tvcache_checks.integration.subprocess.check_output", side_effect=failure):
            with self.assertRaises(subprocess.CalledProcessError):
                source_revision(self.source)


class PortGuardTests(unittest.TestCase):
    def test_invalid_configuration_is_rejected_before_constructing_socket(self):
        with patch("tvcache_checks.integration.socket.socket") as constructor:
            for port in [True, False, -1, 65536, "7879", None, 1.5]:
                with self.subTest(port=port), self.assertRaises(ValueError):
                    available_port(port)
            constructor.assert_not_called()

    def test_port_reservation_uses_loopback_and_closes_its_mock_socket(self):
        for requested, assigned in [(0, 49152), (7879, 7879)]:
            with self.subTest(requested=requested):
                with patch("tvcache_checks.integration.socket.socket") as constructor:
                    reservation = constructor.return_value.__enter__.return_value
                    reservation.getsockname.return_value = ("127.0.0.1", assigned)
                    self.assertEqual(available_port(requested), assigned)
                    constructor.assert_called_once_with(socket.AF_INET, socket.SOCK_STREAM)
                    reservation.bind.assert_called_once_with(("127.0.0.1", requested))
                    constructor.return_value.__exit__.assert_called_once()

    def test_occupied_port_error_is_not_treated_as_permission_to_attach(self):
        with patch("tvcache_checks.integration.socket.socket") as constructor:
            reservation = constructor.return_value.__enter__.return_value
            reservation.bind.side_effect = OSError("address already in use")
            with self.assertRaisesRegex(OSError, "already in use"):
                available_port(7879)
            reservation.getsockname.assert_not_called()
            constructor.return_value.__exit__.assert_called_once()


class BinaryDigestTests(unittest.TestCase):
    def test_small_binary_fixture_matches_standard_sha256(self):
        payload = b"synthetic-server-binary\x00\x01\xff\n"
        with tempfile.TemporaryDirectory(prefix="tvcache-digest-") as directory:
            path = Path(directory) / "fixture.bin"
            path.write_bytes(payload)
            self.assertEqual(binary_digest(path), hashlib.sha256(payload).hexdigest())

    def test_empty_fixture_has_standard_empty_digest(self):
        with tempfile.TemporaryDirectory(prefix="tvcache-digest-") as directory:
            path = Path(directory) / "empty.bin"
            path.touch()
            self.assertEqual(binary_digest(path), hashlib.sha256(b"").hexdigest())


class UpstreamEnvironmentTests(unittest.TestCase):
    def test_optimized_assertions_are_removed_without_mutating_caller_environment(self):
        source = Path("/explicit/team/tvclient")
        inherited = {"PYTHONOPTIMIZE": "2", "PYTHONPATH": "old-path", "TVCACHE_PORT": "old-port",
                     "PYTHONDONTWRITEBYTECODE": "0", "KEEP_THIS": "fixture"}
        with patch.dict(os.environ, inherited, clear=True):
            before = dict(os.environ)
            result = upstream_environment(source, 7879)
            self.assertNotIn("PYTHONOPTIMIZE", result)
            self.assertEqual(result["PYTHONPATH"], str(source))
            self.assertEqual(result["TVCACHE_PORT"], "7879")
            self.assertEqual(result["PYTHONDONTWRITEBYTECODE"], "1")
            self.assertEqual(result["KEEP_THIS"], "fixture")
            self.assertEqual(dict(os.environ), before)

    def test_missing_optimization_setting_is_not_added(self):
        with patch.dict(os.environ, {"KEEP_THIS": "fixture"}, clear=True):
            result = upstream_environment(Path("chosen-client"), 12345)
            self.assertNotIn("PYTHONOPTIMIZE", result)
            self.assertEqual(result["PYTHONPATH"], "chosen-client")
            self.assertEqual(result["TVCACHE_PORT"], "12345")
            self.assertEqual(dict(os.environ), {"KEEP_THIS": "fixture"})


class IntegrationPreflightTests(unittest.IsolatedAsyncioTestCase):
    async def test_unsupported_client_blocks_before_loading_client_or_launching(self):
        source = Path(__file__).resolve().parent
        with patch("tvcache_checks.integration.source_revision", return_value={"revision": "a" * 40, "clean": True}), \
             patch("tvcache_checks.integration.make_client_factory",
                   side_effect=UnsupportedMetadata("Client lacks Claim/Inspect/Publish")), \
             patch("tvcache_checks.integration.load_real_client") as loader, \
             patch("tvcache_checks.integration.available_port") as port, \
             patch("tvcache_checks.integration.tempfile.mkdtemp") as artifacts, \
             patch("tvcache_checks.integration.subprocess.Popen") as launch:
            with self.assertRaisesRegex(UnsupportedMetadata, "Claim/Inspect/Publish"):
                await run_integration(source, source, source / "not-written.json")
            loader.assert_not_called()
            port.assert_not_called()
            artifacts.assert_not_called()
            launch.assert_not_called()

    async def test_invalid_case_configuration_is_rejected_before_provenance_or_launch(self):
        source = Path(__file__).resolve().parent
        with patch("tvcache_checks.integration.source_revision") as provenance, \
             patch("tvcache_checks.integration.available_port") as port, \
             patch("tvcache_checks.integration.subprocess.Popen") as launch:
            for options in [dict(callers=1), dict(timeout=0), dict(timeout=-1)]:
                with self.subTest(options=options), self.assertRaises(ValueError):
                    await run_integration(source, source, source / "not-written.json", **options)
            provenance.assert_not_called()
            port.assert_not_called()
            launch.assert_not_called()

    async def test_dirty_sources_cannot_reach_capability_check_or_launch(self):
        source = Path(__file__).resolve().parent
        with patch("tvcache_checks.integration.source_revision", side_effect=ValueError("modified source checkout")), \
             patch("tvcache_checks.integration.make_client_factory") as factory, \
             patch("tvcache_checks.integration.available_port") as port, \
             patch("tvcache_checks.integration.subprocess.Popen") as launch:
            with self.assertRaisesRegex(ValueError, "modified source checkout"):
                await run_integration(source, source, source / "not-written.json")
            factory.assert_not_called()
            port.assert_not_called()
            launch.assert_not_called()

    async def test_missing_built_binary_does_not_reserve_a_port_or_launch(self):
        source = Path(__file__).resolve().parent
        with patch("tvcache_checks.integration.source_revision", return_value={"revision": "a" * 40, "clean": True}), \
             patch("tvcache_checks.integration.make_client_factory"), \
             patch("tvcache_checks.integration.load_real_client"), \
             patch("tvcache_checks.integration.Path.is_file", return_value=False), \
             patch("tvcache_checks.integration.available_port") as port, \
             patch("tvcache_checks.integration.tempfile.mkdtemp") as artifacts, \
             patch("tvcache_checks.integration.subprocess.Popen") as launch:
            with self.assertRaisesRegex(FileNotFoundError, "Build the selected real server first"):
                await run_integration(source, source, source / "not-written.json")
            port.assert_not_called()
            artifacts.assert_not_called()
            launch.assert_not_called()

    async def test_exited_owned_child_is_rejected_before_testing_any_listener(self):
        process = SimpleNamespace(poll=lambda: 1)
        with patch("tvcache_checks.integration.asyncio.open_connection", new_callable=AsyncMock) as connect, \
             patch("tvcache_checks.integration.Path.read_text") as read_log:
            with self.assertRaisesRegex(RuntimeError, "exited before becoming ready"):
                await _ready(process, 7879, Path("not-read.log"))
            connect.assert_not_called()
            read_log.assert_not_called()

    async def test_missing_or_wrong_post_bind_marker_never_probes_a_competing_listener(self):
        for contents in ["not listening yet", "listening on 0.0.0.0:9999 with tokio"]:
            with self.subTest(contents=contents):
                process = SimpleNamespace(poll=Mock(side_effect=[None, 1]))
                with patch("tvcache_checks.integration.Path.read_text", return_value=contents) as read_log, \
                     patch("tvcache_checks.integration.asyncio.sleep", new_callable=AsyncMock) as pause, \
                     patch("tvcache_checks.integration.asyncio.open_connection", new_callable=AsyncMock) as connect:
                    with self.assertRaisesRegex(RuntimeError, "exited before becoming ready"):
                        await _ready(process, 7879, Path("owned-server.log"))
                    read_log.assert_called_once_with(encoding="utf-8")
                    pause.assert_awaited_once_with(.05)
                    connect.assert_not_called()

    async def test_owned_post_bind_marker_precedes_the_first_tcp_probe(self):
        events = []
        reads = 0
        process = SimpleNamespace(poll=Mock(return_value=None))
        writer = SimpleNamespace(close=Mock(), wait_closed=AsyncMock())

        def read_log(**kwargs):
            nonlocal reads
            reads += 1
            events.append("log-missing" if reads == 1 else "log-marker")
            return "" if reads == 1 else "INFO listening on 0.0.0.0:7879 with tokio\n"

        def pause(_duration):
            events.append("sleep")

        def connect(_host, _port):
            events.append("connect")
            return None, writer

        with patch("tvcache_checks.integration.Path.read_text", side_effect=read_log), \
             patch("tvcache_checks.integration.asyncio.sleep", new_callable=AsyncMock, side_effect=pause), \
             patch("tvcache_checks.integration.asyncio.open_connection", new_callable=AsyncMock,
                   side_effect=connect) as connection:
            await _ready(process, 7879, Path("owned-server.log"))
            self.assertEqual(events, ["log-missing", "sleep", "log-marker", "connect"])
            connection.assert_awaited_once_with("127.0.0.1", 7879)
            writer.close.assert_called_once_with()
            writer.wait_closed.assert_awaited_once_with()


if __name__ == "__main__":
    unittest.main()
