"""Gated scenarios for a real pending-node server supplied through an adapter."""

import asyncio
from collections import Counter
from dataclasses import asdict, dataclass
import json
import time

from .contracts import ClientFactory, Outcome, Path
from .tree import TreeSnapshot, assert_same_identity, inspect_task


def canonical_call(tool: str, **arguments: str) -> str:
    return json.dumps({"tool": tool, "arguments": arguments}, sort_keys=True, separators=(",", ":"))


def snapshot_dict(snapshot: TreeSnapshot) -> dict:
    return {
        "task_id": snapshot.task_id,
        "legacy": snapshot.legacy,
        "nodes": [asdict(snapshot.nodes[path]) for path in sorted(snapshot.nodes)],
    }


class ControlledTool:
    """A controlled tool, not a mock cache: count entry, then wait on a gate."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.counts: Counter[Path] = Counter()
        self._entered: dict[Path, asyncio.Event] = {}
        self._lock = asyncio.Lock()

    async def execute(self, path: Path, value: str) -> str:
        async with self._lock:
            self.counts[path] += 1
            self._entered.setdefault(path, asyncio.Event()).set()
        await self.release.wait()
        return value

    async def wait_entered(self, path: Path) -> None:
        await self._entered.setdefault(path, asyncio.Event()).wait()


@dataclass(frozen=True)
class Group:
    path: Path
    value: str
    callers: int


async def _seed_prefix(factory: ClientFactory, task_id: str, path: Path, value: str) -> None:
    async with factory() as client:
        claim = (await client.lookup_or_claim(task_id, path, "fixture-setup")).validate()
        if claim.status != "owner":
            raise AssertionError("Fixture namespace is not cold, or a new prefix was not claimable")
        await client.publish(task_id, path, node_id=claim.node_id,
                             generation=claim.generation, owner_token=claim.owner_token, value=value)
        ready = (await client.inspect(task_id, path)).validate()
        if (ready.status, ready.value, ready.node_id, ready.generation) != (
            "ready", value, claim.node_id, claim.generation
        ):
            raise AssertionError("Published fixture prefix did not become the expected ready node")


async def _wait_ready(client, task_id: str, path: Path) -> Outcome:
    delay = .001
    while True:
        result = (await client.inspect(task_id, path)).validate()
        if result.status == "ready":
            return result
        if result.status != "pending":
            raise AssertionError(f"Unexpected status while waiting: {result.status}")
        # Only completion polling uses backoff. Contention is established by
        # barriers, actual pending replies, and an execution gate, not sleeps.
        await asyncio.sleep(delay)
        delay = min(delay * 2, .05)


async def _contended(
    factory: ClientFactory, task_id: str, groups: list[Group], report: dict,
) -> dict:
    if not groups or len({group.path for group in groups}) != len(groups):
        raise ValueError("Need one distinct path per contention group")
    if any(group.callers < 2 for group in groups):
        raise ValueError("Each group needs at least two callers")
    start = asyncio.Event()
    claims: asyncio.Queue = asyncio.Queue()
    tool = ControlledTool()
    tasks: list[asyncio.Task] = []
    events = report.setdefault("events", [])

    def record(event: str, path: Path, attempt: str, result: Outcome | None = None) -> None:
        row = {"event": event, "path": list(path), "attempt_id": attempt,
               "monotonic_s": time.monotonic()}
        if result is not None:
            row.update(result.public_fields())
        events.append(row)

    async def worker(group: Group, attempt: str) -> Outcome:
        await start.wait()
        try:
            async with factory() as client:
                claim = (await client.lookup_or_claim(task_id, group.path, attempt)).validate()
                record("claim", group.path, attempt, claim)
                claims.put_nowait((group.path, claim, None))
                if claim.status == "owner":
                    record("execute-start", group.path, attempt)
                    value = await tool.execute(group.path, group.value)
                    record("execute-end", group.path, attempt)
                    await client.publish(task_id, group.path, node_id=claim.node_id,
                                         generation=claim.generation,
                                         owner_token=claim.owner_token, value=value)
                    record("publish", group.path, attempt)
                elif claim.status != "pending":
                    raise AssertionError(f"Cold fixture returned {claim.status}, expected owner/pending")
                result = await _wait_ready(client, task_id, group.path)
                if (result.node_id, result.generation) != (claim.node_id, claim.generation):
                    raise AssertionError("Logical node or ownership generation changed during success")
                if result.value != group.value:
                    raise AssertionError("Result crossed a state-history boundary or has the wrong value")
                record("result-returned", group.path, attempt, result)
                return result
        except Exception as error:
            # A pre-claim failure must not leave the coordinator waiting for
            # an impossible number of claim observations.
            claims.put_nowait((group.path, None, error))
            raise

    async with factory() as observer:
        before = inspect_task(await observer.dump_all(), task_id)
        expected_before = {()} | {group.path[:-1] for group in groups}
        before.assert_shape(expected_before)
        report.setdefault("snapshots", {})["before"] = snapshot_dict(before)
        try:
            for index, group in enumerate(groups):
                for caller in range(group.callers):
                    tasks.append(asyncio.create_task(worker(group, f"group-{index}-attempt-{caller}")))
            start.set()
            observed: dict[Path, list[Outcome]] = {group.path: [] for group in groups}
            for _ in range(sum(group.callers for group in groups)):
                path, claim, error = await claims.get()
                if error is not None:
                    raise error
                observed[path].append(claim)
            for group in groups:
                outcomes = observed[group.path]
                if Counter(result.status for result in outcomes) != {"owner": 1, "pending": group.callers - 1}:
                    raise AssertionError(f"Expected one owner and the remaining waiters for {group.path!r}")
                if len({(r.node_id, r.generation) for r in outcomes}) != 1:
                    raise AssertionError("Equivalent callers received different node IDs/generations")
                await tool.wait_entered(group.path)
                if tool.counts[group.path] != 1:
                    raise AssertionError("Duplicate tool execution while the owner is held")
            node_ids = {outcomes[0].node_id for outcomes in observed.values()}
            if len(node_ids) != len(groups):
                raise AssertionError("Different state histories coalesced into the same node ID")
            if any(task.done() for task in tasks):
                raise AssertionError("A caller completed before the controlled execution was released")
            pending = inspect_task(await observer.dump_all(), task_id)
            pending.assert_shape(expected_before | set(observed))
            for path in before.nodes:
                if pending.nodes[path] != before.nodes[path]:
                    raise AssertionError("A shared prefix changed while claiming its child")
            for path, outcomes in observed.items():
                pending.assert_node(path, state="pending", node_id=outcomes[0].node_id,
                                    generation=outcomes[0].generation, value=None)
            report["snapshots"]["pending"] = snapshot_dict(pending)
            tool.release.set()
            await asyncio.gather(*tasks)
            completed = inspect_task(await observer.dump_all(), task_id)
            assert_same_identity(pending, completed)
            targets = set(observed)
            for path, node in pending.nodes.items():
                if path not in targets and completed.nodes[path] != node:
                    raise AssertionError("Unrelated node changed during publication")
            for group in groups:
                original = observed[group.path][0]
                completed.assert_node(group.path, state="ready", node_id=original.node_id,
                                      generation=original.generation, value=group.value)
                async with factory() as warm_client:
                    warm = (await warm_client.lookup_or_claim(task_id, group.path, "warm-lookup")).validate()
                    if (warm.status, warm.node_id, warm.generation, warm.value) != (
                        "ready", original.node_id, original.generation, group.value
                    ):
                        raise AssertionError("Warm lookup was not the same completed node/result")
                if tool.counts[group.path] != 1:
                    raise AssertionError("Warm hit executed a tool")
            report["snapshots"]["ready"] = snapshot_dict(completed)
            warm_snapshot = inspect_task(await observer.dump_all(), task_id)
            assert_same_identity(completed, warm_snapshot)
            if completed.nodes != warm_snapshot.nodes:
                raise AssertionError("Warm hit changed node lifecycle, value, or placement")
            report["snapshots"]["warm"] = snapshot_dict(warm_snapshot)
            report["tool_executions"] = [
                {"path": list(path), "count": count} for path, count in tool.counts.items()
            ]
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    return report


async def run_pending(
    factory: ClientFactory, case: str, namespace: str, *, callers: int = 8,
    timeout: float = 10.0, report: dict | None = None,
) -> dict:
    """Run against a real adapter. Missing metadata/API must never count as pass."""
    if case not in {"T01", "T02"}:
        raise ValueError("Only T01 and T02 are implemented")
    if callers < 2 or timeout <= 0:
        raise ValueError("Need callers >= 2 and a positive timeout")
    report = report if report is not None else {}
    report.update({"case": case, "task_id": namespace, "status": "running", "limitations": [
        "Controlled tool fixtures: no real sandbox restoration or model behavior is tested.",
        "Call-ID/harness response mapping is not tested by these low-level scenarios.",
        "Failure/takeover scenarios are specified but not implemented yet.",
    ]})

    async def scenario() -> dict:
        read = canonical_call("read_fixture", path="fixture.txt")
        states = ["A"] if case == "T01" else ["A", "B"]
        groups = []
        for state in states:
            prefix = (canonical_call("fixture_setup", state=state),)
            await _seed_prefix(factory, namespace, prefix, f"fixture-state-{state}")
            groups.append(Group(prefix + (read,), state, callers if case == "T01" else 2))
        return await _contended(factory, namespace, groups, report)

    try:
        result = await asyncio.wait_for(scenario(), timeout=timeout)
    except BaseException:
        report["status"] = "failed"
        raise
    result["status"] = "passed"
    return result
