"""Opt-in protocol checks against the real pending-node client and server.

Only tiny string fixtures are published. T08 uses one gated in-process fixture
execution; no shell tool, sandbox, model, eviction, global drain, or fake cache
is involved. These cases establish protocol
behavior, not exactly-once external effects or correctness after a real crash.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
import re
import time
import uuid


MISSING, OWNER, PENDING, READY, FAILED = range(5)
_STATUS = {MISSING: "missing", OWNER: "owner", PENDING: "pending", READY: "ready", FAILED: "failed"}
_ENTRY = "lifecycle-fixture"


def _ensure(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _one(response):
    _ensure(len(response.outcomes) == 1, "Expected exactly one entry outcome")
    outcome = response.outcomes[0]
    _ensure(type(outcome.status) is int and outcome.status in _STATUS,
            "Unknown protocol outcome status")
    _ensure(type(outcome.node_id) is int and outcome.node_id >= 0,
            "Node ID must be a nonnegative integer")
    _ensure(type(outcome.generation) is int and outcome.generation >= 0,
            "Generation must be a nonnegative integer")
    if outcome.status != MISSING:
        _ensure(outcome.node_id > 0 and outcome.generation > 0,
                "An executable outcome needs a real node ID and generation")
    if outcome.status == READY:
        _ensure(isinstance(outcome.value, str), "A ready result must be a string, including empty")
    else:
        _ensure(outcome.value is None, "A non-ready outcome must not expose a completed result")
    return outcome


def _public(outcome) -> dict:
    return {"status": _STATUS[outcome.status], "node_id": outcome.node_id,
            "generation": outcome.generation, "value": outcome.value}


def _same_node(actual, expected, *, generation: int | None = None) -> None:
    _ensure(actual.node_id == expected.node_id, "Node identity changed across a lifecycle transition")
    _ensure(actual.generation == (expected.generation if generation is None else generation),
            "Unexpected ownership generation")


def _node(raw: dict, task_id: str) -> dict:
    return raw[task_id]["root"]["children"][_ENTRY]


async def _capture(client, report: dict, stage: str, task_ids: list[str]) -> dict:
    """Retain real raw task-only dumps; never serialize unrelated graph data."""
    full = await client.dump_all()
    tasks = {task_id: full[task_id] for task_id in task_ids if task_id in full}
    report["snapshots"].append({"stage": stage, "tasks": tasks,
                                "absent_tasks": [task_id for task_id in task_ids if task_id not in full]})
    return tasks


async def _cold(client, report: dict, task_ids: list[str]) -> None:
    raw = await _capture(client, report, "before", task_ids)
    _ensure(not raw, "Fresh UUID task namespace unexpectedly exists; refusing to overwrite it")


async def _claim(client, report: dict, task_id: str):
    async with await client.claim(task_id, [], [_ENTRY]) as response:
        _ensure(response.sandbox_id is None and response.sandbox_path is None,
                "A root-level fixture unexpectedly acquired a sandbox")
        outcome = _one(response)
    report["observations"].append({"operation": "claim", "task_id": task_id, **_public(outcome)})
    return outcome


async def _inspect(client, report: dict, task_id: str, *, wait_ms: int = 0):
    response = await client.inspect(task_id, [], [_ENTRY], wait_ms=wait_ms)
    _ensure(response.sandbox_id is None and response.sandbox_path is None,
            "Read-only inspect unexpectedly acquired a sandbox")
    outcome = _one(response)
    report["observations"].append({"operation": "inspect", "task_id": task_id,
                                    "wait_ms": wait_ms, **_public(outcome)})
    return outcome


async def _publish(client, report: dict, task_id: str, generation: int,
                   value: str | None, *, accepted: bool = True) -> None:
    result = await client.publish(task_id, [_ENTRY], generation, value, None, runtime=1)
    _ensure(result.value_inserted is accepted, "Publication acceptance did not match the ownership contract")
    _ensure(result.sandbox_inserted is False, "A fixture publication unexpectedly stored a sandbox")
    report["observations"].append({"operation": "publish", "task_id": task_id,
                                    "generation": generation, "value_present": value is not None,
                                    "accepted": result.value_inserted})


@asynccontextmanager
async def _clients(client_class, host: str, port: int, timeout: float, count: int):
    async with AsyncExitStack() as stack:
        clients = [await stack.enter_async_context(client_class(
            host=host, port=port, timeout=timeout, retries=0,
        )) for _ in range(count)]
        yield clients


async def _task_isolation(client_class, host, port, timeout, report):
    first_task, second_task = report["task_ids"]
    async with _clients(client_class, host, port, timeout, 3) as (first, second, observer):
        await _cold(observer, report, report["task_ids"])
        a = await _claim(first, report, first_task)
        b = await _claim(second, report, second_task)
        _ensure(a.status == b.status == OWNER, "Identical paths in distinct tasks must have separate owners")
        # IDs are per-task: equal integers across tasks are valid, not evidence of sharing.
        await _capture(observer, report, "both-pending", report["task_ids"])
        await _publish(first, report, first_task, a.generation, "result-A")
        await _publish(second, report, second_task, b.generation, "result-B")
        for task_id, original, value in [(first_task, a, "result-A"), (second_task, b, "result-B")]:
            observed = await _inspect(observer, report, task_id)
            _ensure(observed.status == READY and observed.value == value, "Result leaked across task boundaries")
            _same_node(observed, original)
        await _capture(observer, report, "ready", report["task_ids"])
        report["checks"] += ["Separate owners for the same path under different tasks",
                               "Distinct task-specific published values and stable per-task identity"]


async def _completion_before_poll(client_class, host, port, timeout, report):
    task_id = report["task_ids"][0]
    async with _clients(client_class, host, port, timeout, 2) as (owner, waiter):
        await _cold(waiter, report, [task_id])
        original = await _claim(owner, report, task_id)
        _ensure(original.status == OWNER, "Cold call did not produce an owner")
        await _capture(waiter, report, "pending", [task_id])
        await _publish(owner, report, task_id, original.generation, "published-before-poll")
        await _capture(waiter, report, "published-before-inspect", [task_id])
        result = await _inspect(waiter, report, task_id, wait_ms=_wait_ms(timeout))
        _ensure(result.status == READY and result.value == "published-before-poll",
                "Late long-poll missed an already published result")
        _same_node(result, original)
        await _capture(waiter, report, "ready", [task_id])
        report["checks"].append("Long-poll issued after publication sees completion within the case deadline")


async def _empty_result(client_class, host, port, timeout, report):
    task_id = report["task_ids"][0]
    async with _clients(client_class, host, port, timeout, 2) as (owner, other):
        await _cold(other, report, [task_id])
        original = await _claim(owner, report, task_id)
        _ensure(original.status == OWNER, "Cold call did not produce an owner")
        await _publish(owner, report, task_id, original.generation, "")
        inspected = await _inspect(other, report, task_id)
        warm = await _claim(other, report, task_id)
        for result in (inspected, warm):
            _ensure(result.status == READY and result.value == "", "Valid empty result became a miss or pending placeholder")
            _same_node(result, original)
        await _capture(other, report, "empty-ready-and-warm", [task_id])
        report["checks"].append("Empty string is ready on inspect and warm claim, with the original ID/generation")


async def _failure_retry(client_class, host, port, timeout, report):
    task_id = report["task_ids"][0]
    async with _clients(client_class, host, port, timeout, 2) as (owner, waiter):
        await _cold(waiter, report, [task_id])
        original = await _claim(owner, report, task_id)
        pending = await _claim(waiter, report, task_id)
        _ensure(original.status == OWNER and pending.status == PENDING, "Expected an owner and a pending claimant")
        _same_node(pending, original)
        await _capture(waiter, report, "pending", [task_id])
        await _publish(owner, report, task_id, original.generation, None)
        failed = await _inspect(waiter, report, task_id)
        _ensure(failed.status == FAILED, "Explicit failure did not settle pending state")
        _same_node(failed, original)
        await _capture(waiter, report, "failed", [task_id])
        retry = await _claim(waiter, report, task_id)
        _ensure(retry.status == OWNER, "Failed node was not reclaimable")
        _same_node(retry, original, generation=original.generation + 1)
        await _publish(waiter, report, task_id, retry.generation, "retry-result")
        ready = await _inspect(owner, report, task_id)
        _ensure(ready.status == READY and ready.value == "retry-result", "Retry did not become ready")
        _same_node(ready, retry)
        await _capture(owner, report, "retried-ready", [task_id])
        report["checks"].append("Explicit failure settles waiters; retry preserves ID and increments generation")


def _wait_ms(timeout: float) -> int:
    # Long-poll deadline remains below the real client's request timeout.
    return max(1, min(2000, int(timeout * 1000 / 2)))


class _DecodeCheckpoint:
    """Observe real response decoding without buffering or inventing any bytes.

    decode_response reaches readexactly only after the real request was written
    and drained. This establishes the cancellation point without timing sleeps;
    it does not force the server's internal long-poll subscription schedule.
    """

    def __init__(self, reader):
        self.reader = reader
        self.entered = asyncio.Event()

    async def readexactly(self, count: int) -> bytes:
        self.entered.set()
        return await self.reader.readexactly(count)

    def __getattr__(self, name):
        return getattr(self.reader, name)


async def _waiter_cancellation(client_class, host, port, timeout, report):
    task_id = report["task_ids"][0]
    report["limitations"] = [
        "One gated in-process execution, not a real shell command, container, or external side effect.",
        "Root-level Inspect holds no sandbox lease; this does not prove reclamation of an interrupted Find/Claim lease.",
        "Decoder-entry events establish sent requests, not a forced server-internal subscription interleaving.",
        "Owner and surviving waiter use separate connections; cancellation may fail other claims owned by the cancelled connection.",
    ]
    async with _clients(client_class, host, port, timeout, 4) as (owner, cancelled, survivor, observer):
        await _cold(observer, report, [task_id])
        original = await _claim(owner, report, task_id)
        first_waiter = await _claim(cancelled, report, task_id)
        second_waiter = await _claim(survivor, report, task_id)
        _ensure(original.status == OWNER and first_waiter.status == second_waiter.status == PENDING,
                "Cancellation fixture requires one owner and two pending waiters")
        _same_node(first_waiter, original)
        _same_node(second_waiter, original)
        marker = 2345
        _ensure(await observer.report_fork_cost(task_id, marker) == marker, "Fork-cost response sentinel was not seeded")
        pending_dump = await _capture(observer, report, "pending-before-cancellation", [task_id])
        _ensure(pending_dump[task_id]["sandbox_references"] == {}, "Root-level fixture unexpectedly leased a sandbox")

        release, entered = asyncio.Event(), asyncio.Event()
        execution_count = 0
        expected_value = "T08-owner-result"

        async def execute_owner():
            nonlocal execution_count
            execution_count += 1
            entered.set()
            await release.wait()
            await _publish(owner, report, task_id, original.generation, expected_value)
            return expected_value

        async def wait_ready(client):
            while True:
                result = await _inspect(client, report, task_id, wait_ms=_wait_ms(timeout))
                if result.status == READY:
                    return result
                _ensure(result.status == PENDING, "Cancellation disrupted another claimant's lifecycle")

        old_writer = cancelled.writer
        cancelled_reader = _DecodeCheckpoint(cancelled.reader)
        survivor_reader = _DecodeCheckpoint(survivor.reader)
        cancelled.reader, survivor.reader = cancelled_reader, survivor_reader
        owner_task = asyncio.create_task(execute_owner())
        cancelled_task = survivor_task = None
        try:
            await entered.wait()
            cancelled_task = asyncio.create_task(wait_ready(cancelled))
            survivor_task = asyncio.create_task(wait_ready(survivor))
            await cancelled_reader.entered.wait()
            await survivor_reader.entered.wait()
            _ensure(not cancelled_task.done() and not survivor_task.done() and not owner_task.done(),
                    "All participants must still be active while the owner gate is held")
            cancelled_task.cancel()
            try:
                await cancelled_task
            except asyncio.CancelledError:
                report["observations"].append({"operation": "cancel-in-flight-inspect", "cancelled_error_preserved": True})
            else:
                raise AssertionError("Cancelled waiter suppressed cancellation")
            detached = cancelled.reader is None and cancelled.writer is None
            report["connection_detached_after_cancel"] = detached
            still_pending = await _inspect(observer, report, task_id)
            _ensure(still_pending.status == PENDING and not owner_task.done() and not survivor_task.done(),
                    "Cancelling one waiter disrupted the owner or surviving waiter")
            _same_node(still_pending, original)
            await _capture(observer, report, "owner-and-survivor-unaffected", [task_id])

            release.set()
            _ensure(await owner_task == expected_value, "Owner did not finish the controlled execution")
            surviving_result = await survivor_task
            _ensure(surviving_result.value == expected_value, "Surviving waiter received the wrong result")
            _same_node(surviving_result, original)
            report["checks"] += ["Cancellation preserved CancelledError",
                                   "Owner and surviving waiter completed with original node ID/generation"]
            report["tool_executions"] = execution_count
            await _capture(observer, report, "owner-published-survivor-completed", [task_id])

            # Deliberately use another response type. On the unfixed client the
            # late real Inspect frame is consumed here as the ForkCost reply.
            try:
                recovered = await cancelled.get_fork_cost(task_id)
            except Exception as error:
                report["reuse_error"] = {"type": type(error).__name__, "message": str(error)}
                raise
            _ensure(recovered == marker, "Reused client consumed the wrong response")
            _ensure(detached and cancelled.writer is not old_writer,
                    "Cancelled transaction was not discarded before opening a fresh connection")
            report["reconnected_after_cancel"] = True
            ready = await _inspect(cancelled, report, task_id)
            _ensure(ready.status == READY and ready.value == expected_value, "Reconnected client received a stale result")
            _same_node(ready, original)
            warm = await _claim(cancelled, report, task_id)
            _ensure(warm.status == READY and warm.value == expected_value, "Warm reuse after cancellation missed")
            _same_node(warm, original)
            _ensure(execution_count == 1, "Cancellation or client reuse caused duplicate fixture execution")
            final = await _capture(observer, report, "ready-after-reconnect-and-warm-hit", [task_id])
            graph = final[task_id]
            _ensure(graph["sandbox_references"] == {}, "Fixture left a sandbox lease")
            _ensure(set(graph["root"]["children"]) == {_ENTRY} and not _node(final, task_id)["children"],
                    "Cancellation or warm reuse added unexpected tree nodes")
            report["checks"] += ["Client reconnects and decodes ForkCost rather than the cancelled Inspect response",
                                   "Fresh Inspect/warm claim preserve the completed value, node ID and generation",
                                   "Exactly one controlled execution; no pending fixture node or introduced sandbox lease"]
        finally:
            release.set()
            tasks = [task for task in (owner_task, cancelled_task, survivor_task) if task is not None]
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def _disconnect(client_class, host, port, timeout, report):
    task_id = report["task_ids"][0]
    async with _clients(client_class, host, port, timeout, 2) as (owner, waiter):
        await _cold(waiter, report, [task_id])
        original = await _claim(owner, report, task_id)
        pending = await _claim(waiter, report, task_id)
        _ensure(original.status == OWNER and pending.status == PENDING, "Expected an owner and a pending claimant")
        _same_node(pending, original)
        await _capture(waiter, report, "pending-before-owner-close", [task_id])
        started = asyncio.Event()

        async def poll():
            started.set()
            return await _inspect(waiter, report, task_id, wait_ms=_wait_ms(timeout))

        waiting = asyncio.create_task(poll())
        try:
            await started.wait()
            _ensure(not waiting.done(), "Long-poll returned before the pending owner was disconnected")
            await owner.close()
            failed = await waiting
            _ensure(failed.status == FAILED, "Disconnect did not resolve the long-poll to failed")
            _same_node(failed, original)
            await _capture(waiter, report, "failed-after-owner-close", [task_id])
            retry = await _claim(waiter, report, task_id)
            _ensure(retry.status == OWNER, "Disconnected owner's node was not reclaimable")
            _same_node(retry, original, generation=original.generation + 1)
            await _publish(waiter, report, task_id, retry.generation, "after-disconnect")
            await _capture(waiter, report, "retried-ready", [task_id])
        finally:
            if not waiting.done():
                waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)
        report["checks"].append("Owner connection close resolves a pending long-poll and permits a fenced retry")
        report["limitations"].append(
            "Client scheduling starts the poll before closing the owner; this does not force a particular server-internal subscription interleaving."
        )


async def _stale_publication(client_class, host, port, timeout, report):
    task_id = report["task_ids"][0]
    async with _clients(client_class, host, port, timeout, 2) as (old_owner, new_owner):
        await _cold(new_owner, report, [task_id])
        old = await _claim(old_owner, report, task_id)
        _ensure(old.status == OWNER, "Cold call did not produce an owner")
        await _publish(old_owner, report, task_id, old.generation, None)
        new = await _claim(new_owner, report, task_id)
        _ensure(new.status == OWNER, "Failed call did not transfer ownership")
        _same_node(new, old, generation=old.generation + 1)
        await _capture(new_owner, report, "new-generation-pending", [task_id])
        await _publish(old_owner, report, task_id, old.generation, "old-owner-stale", accepted=False)
        await _publish(old_owner, report, task_id, new.generation, "wrong-connection", accepted=False)
        await _publish(new_owner, report, task_id, old.generation, "wrong-generation", accepted=False)
        pending = await _inspect(old_owner, report, task_id)
        _ensure(pending.status == PENDING, "Rejected publication changed the current pending state")
        _same_node(pending, new)
        await _capture(old_owner, report, "unchanged-after-rejections", [task_id])
        await _publish(new_owner, report, task_id, new.generation, "current-result")
        await _publish(old_owner, report, task_id, old.generation, "late-stale", accepted=False)
        ready = await _inspect(old_owner, report, task_id)
        _ensure(ready.status == READY and ready.value == "current-result", "Stale publication overwrote the current result")
        _same_node(ready, new)
        await _capture(old_owner, report, "current-ready", [task_id])
        report["checks"] += ["Wrong connection and stale generation are independently rejected",
                               "Rejected late publication preserves the current completed value"]


async def _readonly_inspection(client_class, host, port, timeout, report):
    task_id = report["task_ids"][0]
    async with _clients(client_class, host, port, timeout, 2) as (owner, observer):
        await _cold(observer, report, [task_id])
        missing = await _inspect(observer, report, task_id)
        _ensure(missing.status == MISSING and missing.node_id == 0 and missing.generation == 0,
                "Inspection of an absent task returned a non-missing identity")
        after_miss = await _capture(observer, report, "after-missing-inspect", [task_id])
        _ensure(task_id not in after_miss, "Read-only inspection created a task graph")
        original = await _claim(owner, report, task_id)
        _ensure(original.status == OWNER, "Cold call did not produce an owner")
        pending = await _claim(observer, report, task_id)
        _ensure(pending.status == PENDING, "Other connection did not observe pending")
        _same_node(pending, original)
        before = await _capture(observer, report, "before-pending-inspects", [task_id])
        for _ in range(3):
            observed = await _inspect(observer, report, task_id)
            _ensure(observed.status == PENDING, "Read-only inspection changed pending state")
            _same_node(observed, original)
        after = await _capture(observer, report, "after-pending-inspects", [task_id])
        _ensure(before == after, "Pending inspections mutated the dumped task or counters")
        await _publish(owner, report, task_id, original.generation, "inspection-result")
        before = await _capture(observer, report, "before-ready-inspects", [task_id])
        for _ in range(3):
            observed = await _inspect(observer, report, task_id)
            _ensure(observed.status == READY and observed.value == "inspection-result", "Read-only inspection changed ready result")
            _same_node(observed, original)
        after = await _capture(observer, report, "after-ready-inspects", [task_id])
        _ensure(before == after, "Ready inspections mutated the dumped task or counters")
        hits = _node(after, task_id).get("number_of_cache_hits")
        _ensure(type(hits) is int and hits == 1, "Inspect was counted as a cache claim/hit")
        report["checks"] += ["Inspect on an absent task does not allocate a graph",
                               "Pending and ready inspections preserve the complete raw task and its hit counters"]


async def run_lifecycle_checks(client_class, host: str, port: int, namespace: str,
                               *, timeout: float = 5.0) -> dict:
    """Run isolated real protocol cases, recording failures without hiding them.

    Every case is bounded as a whole, including connection and cleanup. A
    refused connection, unsupported API, timeout, or contradictory observation
    fails the case; nothing is converted into a successful skip.
    """
    if not isinstance(host, str) or not host.strip() or not 1 <= port <= 65535:
        raise ValueError("Need a nonempty host and valid TCP port")
    if timeout <= 0 or not isinstance(namespace, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,80}", namespace):
        raise ValueError("Need a positive timeout and a namespace of 1–80 safe ASCII characters")
    cases = []
    definitions = [
        ("T03", "Task isolation", _task_isolation, 2),
        ("T04", "Completion before first long-poll", _completion_before_poll, 1),
        ("T05", "Empty ready result", _empty_result, 1),
        ("T06", "Explicit failure and retry", _failure_retry, 1),
        ("T07", "Owner disconnect and waiting caller", _disconnect, 1),
        ("T08", "Waiter cancellation and clean connection reuse", _waiter_cancellation, 1),
        ("T09", "Stale generation and wrong-connection publication", _stale_publication, 1),
        ("I01", "Read-only inspection and counters", _readonly_inspection, 1),
    ]
    for case_id, name, scenario, task_count in definitions:
        prefix = f"{namespace}:{case_id}:{uuid.uuid4().hex}"
        report = {"case": case_id, "name": name, "status": "running",
                  "task_ids": [f"{prefix}:{index}" for index in range(task_count)],
                  "checks": [], "observations": [], "snapshots": [],
                  "limitations": ["Protocol string fixtures only; no actual tool execution or sandbox restoration."]}
        cases.append(report)
        start = time.monotonic()
        try:
            await asyncio.wait_for(scenario(client_class, host, port, timeout, report), timeout=timeout)
        except Exception as error:
            report.update(status="failed", error_type=type(error).__name__, error=str(error))
        else:
            report["status"] = "passed"
        report["elapsed_s"] = time.monotonic() - start
    return {"mode": "real-pending-lifecycle", "status": "passed" if all(
                case["status"] == "passed" for case in cases) else "failed",
            "cases": cases,
            "limitations": ["CPU-side protocol fixtures; no GPU/model/container or exactly-once external-effect guarantee.",
                              "Case timeouts are hang deadlines, not performance measurements."]}
