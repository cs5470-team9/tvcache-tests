"""Adapter self-tests with scripted transport replies, not a cache/server.

The fixtures below never calculate cache decisions. They return literal replies
so tests can verify wire argument mapping, connection/lease discipline, and
truthful metadata normalization. Real server evidence is collected separately.
"""

import copy
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tvcache_checks.team_adapter import make_client_factory, normalize_team_dump
from tvcache_checks.tree import UnsupportedMetadata


def raw_dump():
    return {"task": {"root": {
        "node_id": 0, "status": "ready", "generation": 0, "value": None,
        "children": {"prefix": {
            "node_id": 4, "status": "ready", "generation": 1,
            "value": None, "sandbox_id": "fixture-snapshot", "children": {
                "read": {"node_id": 9, "status": "pending", "generation": 2,
                         "value": None, "children": {}}
            }
        }}
    }, "next_node_id": 10}}


def reply(status=1, node_id=9, generation=2, value=None):
    return SimpleNamespace(status=status, node_id=node_id, generation=generation, value=value)


class LeaseFixture:
    def __init__(self, outcomes):
        self.outcomes = outcomes
        self.released = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *unused):
        self.released += 1


class TransportFixture:
    instances = []

    def __init__(self, **kwargs):
        self.constructor = kwargs
        self.writer = None
        self.closed = 0
        self.claim_reply = [reply()]
        self.inspect_reply = [reply(2)]
        self.publish_reply = SimpleNamespace(value_inserted=True, sandbox_inserted=False)
        self.dump_reply = raw_dump()
        self.calls = []
        self.leases = []
        type(self).instances.append(self)

    async def __aenter__(self):
        self.writer = object()
        return self

    async def __aexit__(self, *unused):
        self.closed += 1
        self.writer = None

    async def claim(self, task, prefix, entries):
        self.calls.append(("claim", task, prefix, entries))
        lease = LeaseFixture(self.claim_reply)
        self.leases.append(lease)
        return lease

    async def inspect(self, task, prefix, entries, wait_ms):
        self.calls.append(("inspect", task, prefix, entries, wait_ms))
        return SimpleNamespace(outcomes=self.inspect_reply)

    async def publish(self, task, path, generation, value, sandbox_id, runtime):
        self.calls.append(("publish", task, path, generation, value, sandbox_id, runtime))
        return self.publish_reply

    async def dump_all(self):
        self.calls.append(("dump_all",))
        return self.dump_reply


class NormalizationTests(unittest.TestCase):
    def test_normalizes_only_structural_state_and_preserves_raw_metadata(self):
        original = raw_dump()
        untouched = copy.deepcopy(original)
        normalized = normalize_team_dump(original)
        self.assertEqual(original, untouched)
        root = normalized["task"]["root"]
        self.assertEqual((root["status"], root["node_id"], root["generation"]), ("root", 0, 0))
        prefix = root["children"]["prefix"]
        self.assertEqual((prefix["status"], prefix["node_id"], prefix["generation"]), ("prefix", 4, 1))
        self.assertEqual(prefix["sandbox_id"], "fixture-snapshot")
        self.assertEqual(prefix["children"]["read"], original["task"]["root"]["children"]["prefix"]["children"]["read"])
        self.assertEqual(normalized["task"]["next_node_id"], 10)

    def test_empty_ready_string_is_not_reclassified_as_prefix(self):
        raw = raw_dump()
        raw["task"]["root"]["children"]["prefix"].update(value="")
        self.assertEqual(normalize_team_dump(raw)["task"]["root"]["children"]["prefix"]["status"], "ready")

    def test_missing_metadata_blocks_instead_of_fabricating_it(self):
        for field in ("node_id", "status", "generation", "value"):
            with self.subTest(field=field):
                raw = raw_dump()
                del raw["task"]["root"][field]
                with self.assertRaises(UnsupportedMetadata):
                    normalize_team_dump(raw)

    def test_unexpected_root_is_rejected(self):
        for field, value in [("node_id", 5), ("node_id", False), ("generation", 1),
                             ("status", "pending"), ("value", "result")]:
            with self.subTest(field=field, value=value):
                raw = raw_dump()
                raw["task"]["root"][field] = value
                with self.assertRaises(AssertionError):
                    normalize_team_dump(raw)

    def test_invalid_tree_or_private_metadata_is_rejected(self):
        for field, value in [("children", []), ("owner_token", "private"),
                             ("owner_connection_id", 123), ("state", "pending")]:
            with self.subTest(field=field):
                raw = raw_dump()
                raw["task"]["root"][field] = value
                with self.assertRaises(AssertionError):
                    normalize_team_dump(raw)
        raw = raw_dump()
        raw["task"]["root"]["children"]["prefix"]["children"]["read"]["value"] = "premature"
        with self.assertRaises(AssertionError):
            normalize_team_dump(raw)


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        TransportFixture.instances = []
        self.loader = patch("tvcache_checks.team_adapter.load_real_client", return_value=TransportFixture)
        self.loader.start()
        self.addCleanup(self.loader.stop)

    def factory(self, **kwargs):
        return make_client_factory(Path("explicit-checkout"), **kwargs)

    async def test_factory_uses_requested_checkout_without_connecting(self):
        factory = self.factory(host="127.0.0.1", port=18001, timeout=3)
        self.assertEqual(TransportFixture.instances, [])
        first, second = factory(), factory()
        self.assertIsNot(first, second)
        first_transport, second_transport = TransportFixture.instances
        self.assertIsNone(first_transport.writer)
        self.assertEqual(first_transport.constructor, dict(host="127.0.0.1", port=18001, timeout=3, retries=0))
        self.assertIsNot(first_transport, second_transport)
        self.assertEqual(factory.adapters, [first, second])

    async def test_owner_mapping_releases_lease_but_preserves_connection(self):
        adapter = self.factory()()
        transport = TransportFixture.instances[-1]
        async with adapter:
            writer = transport.writer
            owner = await adapter.lookup_or_claim("task", ("prefix", "read"), "a1")
            self.assertEqual((owner.status, owner.node_id, owner.generation), ("owner", 9, 2))
            self.assertEqual(transport.calls, [("claim", "task", ["prefix"], ["read"])])
            self.assertEqual(transport.leases[0].released, 1)
            self.assertIs(transport.writer, writer)
            self.assertEqual(transport.closed, 0)
            self.assertNotIn("owner_token", json.dumps(owner.public_fields()))
        self.assertEqual(transport.closed, 1)

    async def test_each_nonowner_status_maps_without_token(self):
        for code, status, value in [(0, "missing", None), (2, "pending", None),
                                    (3, "ready", ""), (4, "failed", None)]:
            with self.subTest(status=status):
                adapter = self.factory()()
                transport = TransportFixture.instances[-1]
                transport.claim_reply = [reply(code, value=value)]
                async with adapter:
                    result = await adapter.lookup_or_claim("task", ("read",), "a1")
                    self.assertEqual(result.status, status)
                    self.assertEqual(result.value, value)
                    self.assertIsNone(result.owner_token)
                    if code == 0:
                        self.assertIsNone(result.node_id)
                        self.assertIsNone(result.generation)

    async def test_inspect_uses_nonclaiming_zero_wait_request(self):
        adapter = self.factory()()
        transport = TransportFixture.instances[-1]
        async with adapter:
            result = await adapter.inspect("task", ("prefix", "read"))
            self.assertEqual(result.status, "pending")
            self.assertEqual(transport.calls, [("inspect", "task", ["prefix"], ["read"], 0)])
            transport.inspect_reply = [reply(1)]
            with self.assertRaisesRegex(AssertionError, "Read-only inspect"):
                await adapter.inspect("task", ("read",))

    async def test_publish_uses_same_connection_and_generation_without_wire_token(self):
        adapter = self.factory()()
        transport = TransportFixture.instances[-1]
        async with adapter:
            owner = await adapter.lookup_or_claim("task", ("prefix", "read"), "a1")
            await adapter.publish("task", ("prefix", "read"), node_id=9, generation=2,
                                  owner_token=owner.owner_token, value="")
            self.assertEqual(transport.calls[-1], ("publish", "task", ["prefix", "read"], 2, "", None, 1))
            with self.assertRaisesRegex(AssertionError, "owner claim"):
                await adapter.publish("task", ("prefix", "read"), node_id=9, generation=2,
                                      owner_token=owner.owner_token, value="duplicate")

    async def test_wrong_token_id_generation_path_or_adapter_cannot_publish(self):
        adapter, other = self.factory()(), self.factory()()
        async with adapter, other:
            owner = await adapter.lookup_or_claim("task", ("read",), "a1")
            cases = [(adapter, "task", ("read",), 9, 2, object()),
                     (adapter, "task", ("read",), 8, 2, owner.owner_token),
                     (adapter, "task", ("read",), 9, 3, owner.owner_token),
                     (adapter, "task", ("other",), 9, 2, owner.owner_token),
                     (adapter, "other", ("read",), 9, 2, owner.owner_token),
                     (other, "task", ("read",), 9, 2, owner.owner_token)]
            for target, task, path, node_id, generation, token in cases:
                with self.subTest(task=task, path=path, node_id=node_id, generation=generation):
                    with self.assertRaisesRegex(AssertionError, "owner claim"):
                        await target.publish(task, path, node_id=node_id, generation=generation,
                                             owner_token=token, value="result")

    async def test_reconnected_owner_is_not_original_capability(self):
        adapter = self.factory()()
        transport = TransportFixture.instances[-1]
        async with adapter:
            owner = await adapter.lookup_or_claim("task", ("read",), "a1")
            transport.writer = object()
            with self.assertRaisesRegex(AssertionError, "connection changed"):
                await adapter.publish("task", ("read",), node_id=9, generation=2,
                                      owner_token=owner.owner_token, value="result")

    async def test_server_publication_rejection_is_failure(self):
        adapter = self.factory()()
        transport = TransportFixture.instances[-1]
        transport.publish_reply.value_inserted = False
        async with adapter:
            owner = await adapter.lookup_or_claim("task", ("read",), "a1")
            with self.assertRaisesRegex(AssertionError, "server rejected"):
                await adapter.publish("task", ("read",), node_id=9, generation=2,
                                      owner_token=owner.owner_token, value="result")

    async def test_raw_dump_capture_is_independent_of_normalization_and_source(self):
        factory = self.factory()
        adapter = factory()
        transport = TransportFixture.instances[-1]
        async with adapter:
            normalized = await adapter.dump_all()
            self.assertEqual(normalized["task"]["root"]["status"], "root")
            self.assertEqual(adapter.raw_dumps[0]["task"]["root"]["status"], "ready")
            normalized["task"]["root"]["node_id"] = 50
            transport.dump_reply["task"]["root"]["node_id"] = 60
            self.assertEqual(factory.raw_dumps[0]["task"]["root"]["node_id"], 0)

    async def test_bad_wire_results_fail_without_silently_releasing_context(self):
        for outcomes in [[], [reply(), reply()], [reply(True)], [reply(99)], [reply(3, value=None)]]:
            with self.subTest(outcomes=outcomes):
                adapter = self.factory()()
                transport = TransportFixture.instances[-1]
                transport.claim_reply = outcomes
                async with adapter:
                    with self.assertRaises(AssertionError):
                        await adapter.lookup_or_claim("task", ("read",), "a1")
                    self.assertEqual(transport.leases[-1].released, 1)

    async def test_inactive_or_invalid_paths_do_not_send_requests(self):
        adapter = self.factory()()
        with self.assertRaises(RuntimeError):
            await adapter.inspect("task", ("read",))
        async with adapter:
            for path in [(), ["read"], (1,)]:
                with self.subTest(path=path), self.assertRaises(ValueError):
                    await adapter.inspect("task", path)

    async def test_missing_pending_methods_are_blocked(self):
        with patch("tvcache_checks.team_adapter.load_real_client", return_value=object):
            with self.assertRaises(UnsupportedMetadata):
                self.factory()

    async def test_invalid_connection_parameters_are_rejected_before_import(self):
        for arguments in [dict(host=""), dict(port=True), dict(port=0), dict(timeout=0), dict(timeout=True)]:
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                self.factory(**arguments)


if __name__ == "__main__":
    unittest.main()
