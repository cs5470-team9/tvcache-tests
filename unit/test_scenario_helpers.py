"""Exercise fixture synchronization, not a simulated cache implementation."""

import asyncio
import unittest

from tvcache_checks.scenarios import ControlledTool, canonical_call, run_pending


class ControlledToolTests(unittest.IsolatedAsyncioTestCase):
    async def test_execution_enters_before_gate_opens(self):
        tool = ControlledTool()
        path = ("setup A", "read")
        task = asyncio.create_task(tool.execute(path, "A"))
        try:
            await asyncio.wait_for(tool.wait_entered(path), 1)
            self.assertEqual(tool.counts[path], 1)
            self.assertFalse(task.done())
            tool.release.set()
            self.assertEqual(await asyncio.wait_for(task, 1), "A")
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_duplicate_executions_are_counted_not_hidden(self):
        tool = ControlledTool()
        path = ("read",)
        tool.release.set()
        self.assertEqual(await asyncio.gather(tool.execute(path, "A"), tool.execute(path, "A")), ["A", "A"])
        self.assertEqual(tool.counts[path], 2)

    async def test_histories_have_separate_counters(self):
        tool = ControlledTool()
        tool.release.set()
        await tool.execute(("A", "read"), "A")
        await tool.execute(("B", "read"), "B")
        self.assertEqual(tool.counts[("A", "read")], 1)
        self.assertEqual(tool.counts[("B", "read")], 1)

    async def test_invalid_scenario_does_not_connect(self):
        def forbidden_factory():
            raise AssertionError("Must not connect")
        with self.assertRaisesRegex(ValueError, "Only T01"):
            await run_pending(forbidden_factory, "T99", "task")
        with self.assertRaisesRegex(ValueError, "callers"):
            await run_pending(forbidden_factory, "T01", "task", callers=1)


class CanonicalCallTests(unittest.TestCase):
    def test_argument_order_is_canonical_and_values_are_distinct(self):
        self.assertEqual(canonical_call("read", a="1", b="2"), canonical_call("read", b="2", a="1"))
        self.assertNotEqual(canonical_call("read", path="A"), canonical_call("read", path="B"))
