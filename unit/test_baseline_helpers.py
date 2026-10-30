"""CPU-only tests for baseline report helpers, not real-server coverage."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tvcache_checks.baseline import load_real_client, make_task_id, snapshot_report


class BaselineHelperTests(unittest.TestCase):
    def test_task_ids_are_fresh_and_namespaced(self) -> None:
        first = make_task_id("team9-checks")
        second = make_task_id("team9-checks")
        self.assertNotEqual(first, second)
        self.assertRegex(first, r"^team9-checks:baseline:[0-9a-f]{32}$")

    def test_namespace_rejects_empty_and_unsafe_names(self) -> None:
        for invalid in ("", "name with spaces", "../other", "a" * 81, "名前"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                make_task_id(invalid)

    def test_client_source_must_contain_real_package(self) -> None:
        with self.assertRaises(FileNotFoundError):
            load_real_client(Path(__file__))

    def test_client_import_refuses_already_loaded_other_source(self) -> None:
        alien_module = SimpleNamespace(__file__="/different-checkout/tvclient/__init__.py")
        with tempfile.TemporaryDirectory(prefix="tvcache-source-") as directory:
            source = Path(directory)
            (source / "tvclient").mkdir()
            # Only a source-path fixture; the alien-module guard must reject
            # before this empty file is ever imported. No real client needed.
            (source / "tvclient/client.py").touch()
            with patch.dict(sys.modules, {"tvclient": alien_module}):
                with self.assertRaisesRegex(RuntimeError, "already loaded from another source"):
                    load_real_client(source)

    def test_snapshot_report_preserves_empty_value_and_missing_metadata(self) -> None:
        root = SimpleNamespace(
            parent_path=None, node_id=None, state="legacy", generation=None,
            value=None, sandbox_id=None,
        )
        child = SimpleNamespace(
            parent_path=(), node_id=None, state="legacy", generation=None,
            value="", sandbox_id=None,
        )
        snapshot = SimpleNamespace(task_id="fixture", legacy=True, nodes={(): root, ("empty",): child})
        report = snapshot_report(snapshot, "after insert")
        self.assertEqual(report["node_count_including_root"], 2)
        self.assertIsNone(report["nodes"][0]["parent_path"])
        self.assertEqual(report["nodes"][1]["parent_path"], [])
        self.assertEqual(report["nodes"][1]["value"], "")
        self.assertIsNone(report["nodes"][1]["node_id"])
        self.assertTrue(report["legacy"])

    def test_snapshot_report_retains_actual_lifecycle_metadata(self) -> None:
        node = SimpleNamespace(
            parent_path=None, node_id=0, state="root", generation=0,
            value=None, sandbox_id=None,
        )
        snapshot = SimpleNamespace(task_id="fixture", legacy=False, nodes={(): node})
        report = snapshot_report(snapshot, "initial")
        self.assertEqual(report["nodes"][0]["node_id"], 0)
        self.assertEqual(report["nodes"][0]["state"], "root")
        self.assertEqual(report["nodes"][0]["generation"], 0)
        self.assertFalse(report["legacy"])


if __name__ == "__main__":
    unittest.main()
