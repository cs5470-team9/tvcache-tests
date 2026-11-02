"""Pure lifecycle-report helper tests; these never simulate a cache server."""

import unittest
from types import SimpleNamespace

from tvcache_checks.lifecycle_cases import _one, _public, _same_node, _wait_ms, run_lifecycle_checks


def outcome(status=1, node_id=1, generation=1, value=None):
    return SimpleNamespace(status=status, node_id=node_id, generation=generation, value=value)


class LifecycleHelperTests(unittest.TestCase):
    def test_valid_protocol_outcomes_include_empty_ready_value(self):
        for row in [outcome(status=0, node_id=0, generation=0), outcome(),
                    outcome(status=2), outcome(status=3, value=""), outcome(status=4)]:
            self.assertIs(_one(SimpleNamespace(outcomes=[row])), row)

    def test_incorrect_outcomes_are_rejected(self):
        for row in [outcome(status=99), outcome(status=True), outcome(node_id=True),
                    outcome(generation=0), outcome(status=3), outcome(status=2, value="premature")]:
            with self.subTest(row=row), self.assertRaises(AssertionError):
                _one(SimpleNamespace(outcomes=[row]))
        for rows in [[], [outcome(), outcome()]]:
            with self.assertRaises(AssertionError):
                _one(SimpleNamespace(outcomes=rows))

    def test_public_fields_are_explicit_and_do_not_copy_ownership_markers(self):
        row = outcome()
        row.owner_token = "private-fixture-marker"
        row.owner_connection_id = 999
        self.assertEqual(_public(row), dict(status="owner", node_id=1, generation=1, value=None))

    def test_identity_and_generation_are_checked_separately(self):
        old = outcome()
        _same_node(outcome(), old)
        _same_node(outcome(generation=2), old, generation=2)
        for changed in [outcome(node_id=2), outcome(generation=2)]:
            with self.assertRaises(AssertionError):
                _same_node(changed, old)

    def test_wait_duration_is_capped_below_normal_request_timeouts(self):
        for timeout in [0.1, 1, 5, 30]:
            wait = _wait_ms(timeout)
            self.assertGreater(wait, 0)
            self.assertLess(wait, timeout * 1000)
            self.assertLessEqual(wait, 2000)


class LifecycleArgumentTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_configuration_is_rejected_before_connecting(self):
        def no_connection(**kwargs):
            self.fail("Invalid configuration attempted a connection")

        for host, port, namespace, timeout in [
            ("", 7879, "checks", 5), ("localhost", 0, "checks", 5),
            ("localhost", 7879, "../other", 5), ("localhost", 7879, "checks", 0),
        ]:
            with self.subTest(host=host, port=port, namespace=namespace, timeout=timeout):
                with self.assertRaises(ValueError):
                    await run_lifecycle_checks(no_connection, host, port, namespace, timeout=timeout)


if __name__ == "__main__":
    unittest.main()
