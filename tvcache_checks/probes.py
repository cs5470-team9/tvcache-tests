"""Offline consistency checks for authoritative, complete lifecycle probe logs.

These checks validate recorded events, not the truthfulness of instrumentation,
tool results, sandbox restoration, or real-server performance. No services or
tools are executed. The stream must begin before the observed cache lifecycle;
attaching to an already warm cache is not supported by this first contract.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .tree import UnsupportedMetadata, inspect_task


class IncompleteProbeLog(AssertionError):
    """Missing, sampled, or dropped evidence prevents a correctness verdict."""


@dataclass
class _Lifecycle:
    generation: int
    owner: str
    phase: str = "pending"
    started: bool = False
    ended: bool = False


def _integer(record: dict, field: str, minimum: int = 0) -> int:
    value = record.get(field)
    if type(value) is not int or value < minimum:
        raise AssertionError(f"{field} must be an integer >= {minimum}")
    return value


def _text(record: dict, field: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise AssertionError(f"{field} must be a nonempty string")
    return value


def _reject_owner_tokens(value: Any) -> None:
    if isinstance(value, dict):
        if "owner_token" in value:
            raise AssertionError("Private owner tokens must not appear in probe logs")
        for child in value.values():
            _reject_owner_tokens(child)
    elif isinstance(value, list):
        for child in value:
            _reject_owner_tokens(child)


def validate_probe_log(path: Path, *, require_tree: bool = False) -> dict:
    """Validate one run's JSONL and return a payload-free evidence summary.

    ``seq`` is an authoritative single-collector total order, not a timestamp sort
    or client-local counter. Its scope must include execution instrumentation.
    Sequences start at zero and are contiguous. Completion metadata
    must explicitly attest to an unsampled, lossless capture. Retries may open
    the next generation only after a failed generation has terminated; live
    lease takeover is intentionally outside this initial contract.
    """
    counts: Counter[str] = Counter()
    claims: Counter[str] = Counter()
    identities: dict[tuple[str, tuple[str, ...]], int] = {}
    placements: dict[tuple[str, int], tuple[str, ...]] = {}
    lifecycles: dict[tuple[str, tuple[str, ...]], _Lifecycle] = {}
    snapshot_paths: dict[str, set[tuple[str, ...]]] = {}
    last_node_seq: dict[str, int] = {}
    last_snapshot_seq: dict[str, int] = {}
    run_id = None
    capture_mode = None
    last_seq = -1
    finished = False
    snapshots = 0

    def identity(task: str, key: tuple[str, ...], node_id: int) -> None:
        prior_id = identities.setdefault((task, key), node_id)
        if prior_id != node_id:
            raise AssertionError(f"Node ID changed at {task!r}, {key!r}")
        prior_path = placements.setdefault((task, node_id), key)
        if prior_path != key:
            raise AssertionError(f"Node ID {node_id} reused or moved in {task!r}")

    with Path(path).open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                record: Any = json.loads(line)
                if not isinstance(record, dict):
                    raise AssertionError("Each JSONL record must be an object")
                _reject_owner_tokens(record)
                event = _text(record, "event")
                if finished:
                    raise AssertionError("Events follow the final run_end")
                seq = _integer(record, "seq")
                _integer(record, "ts_ns")
                if seq <= last_seq:
                    raise AssertionError("seq must increase strictly")
                if seq != last_seq + 1:
                    raise IncompleteProbeLog("Probe sequence has missing records")
                last_seq = seq
                event_run = _text(record, "run_id")
                if run_id is None:
                    if event != "run_start" or seq != 0:
                        raise IncompleteProbeLog("Capture must begin with run_start at seq=0")
                    if type(record.get("schema_version")) is not int or record["schema_version"] != 1:
                        raise AssertionError("Expected schema_version=1")
                    capture_mode = record.get("capture_mode")
                    if not isinstance(capture_mode, str) or capture_mode not in {"correctness", "performance"}:
                        raise AssertionError("Unknown capture_mode")
                    if record.get("events_complete") is not True or record.get("sampled", False) is not False:
                        raise IncompleteProbeLog("Complete, unsampled events are required")
                    run_id = event_run
                    counts[event] += 1
                    continue
                if event_run != run_id:
                    raise AssertionError("Mixed run_id values")
                counts[event] += 1
                if event == "run_end":
                    if record.get("completed") is not True:
                        raise IncompleteProbeLog("Run did not complete")
                    if "dropped_events" not in record:
                        raise IncompleteProbeLog("Missing dropped_events count")
                    if _integer(record, "dropped_events") != 0:
                        raise IncompleteProbeLog("Probe events were dropped")
                    if any(item.phase == "pending" or item.started != item.ended for item in lifecycles.values()):
                        raise IncompleteProbeLog("Unfinished execution or unpublished owner at run_end")
                    finished = True
                    continue

                task = _text(record, "task_id")
                if event == "tree_snapshot":
                    if capture_mode != "correctness":
                        raise AssertionError("Full tree dumps are not allowed in performance captures")
                    stage = record.get("stage")
                    if not isinstance(stage, str) or stage not in {"before", "pending", "ready", "warm"}:
                        raise AssertionError("Unknown tree snapshot stage")
                    tree = inspect_task(record.get("dump"), task, require_metadata=True)
                    previous_paths = snapshot_paths.get(task, set())
                    if not previous_paths.issubset(tree.nodes):
                        raise AssertionError("Tree snapshot removed previously observed nodes without a reset/eviction event")
                    for key, node in tree.nodes.items():
                        identity(task, key, node.node_id)
                        if node.state in {"pending", "ready", "failed"} and (task, key) not in lifecycles:
                            raise AssertionError("Executable snapshot node has no observed owner lifecycle")
                    for (known_task, key), item in lifecycles.items():
                        if known_task == task:
                            tree.assert_node(
                                key, state=item.phase, node_id=identities[(task, key)],
                                generation=item.generation,
                            )
                    snapshot_paths[task] = set(tree.nodes)
                    last_snapshot_seq[task] = seq
                    snapshots += 1
                    continue

                if event not in {"claim", "status", "execute_start", "execute_end", "publish"}:
                    raise AssertionError(f"Unknown event: {event!r}")
                raw_path = record.get("path")
                if not isinstance(raw_path, list) or not raw_path or any(not isinstance(part, str) for part in raw_path):
                    raise AssertionError("path must be a nonempty list of strings")
                key = tuple(raw_path)
                node_id = _integer(record, "node_id")
                generation = _integer(record, "generation", 1)
                attempt = _text(record, "attempt_id")
                identity(task, key, node_id)
                last_node_seq[task] = seq
                lifecycle_key = (task, key)
                item = lifecycles.get(lifecycle_key)
                outcome = record.get("outcome")

                if event == "claim" and outcome == "owner":
                    if item is None:
                        if generation != 1:
                            raise IncompleteProbeLog("First observed owner must have generation=1")
                    elif item.phase != "failed" or generation != item.generation + 1:
                        raise AssertionError("Duplicate owner or invalid retry generation")
                    lifecycles[lifecycle_key] = _Lifecycle(generation, attempt)
                    claims["owner"] += 1
                    continue
                if item is None:
                    raise AssertionError("Node event has no preceding owner claim")
                if generation != item.generation:
                    raise AssertionError("Stale or unclaimed generation")
                if event in {"claim", "status"}:
                    if not isinstance(outcome, str) or outcome not in {"owner", "pending", "ready", "failed"}:
                        raise AssertionError("Unknown claim/status outcome")
                    expected = "pending" if outcome == "owner" else outcome
                    if expected != item.phase or (outcome == "owner" and attempt != item.owner):
                        raise AssertionError("Claim/status outcome contradicts recorded lifecycle")
                    if event == "claim":
                        claims[outcome] += 1
                    continue
                if attempt != item.owner:
                    raise AssertionError("Only the recorded owner may execute or publish")
                if item.phase != "pending":
                    raise AssertionError("Execution/publication follows terminal publication")
                if event == "execute_start":
                    if item.started:
                        raise AssertionError("Multiple executions for one node generation")
                    item.started = True
                elif event == "execute_end":
                    if not item.started or item.ended:
                        raise AssertionError("Execution end has no unmatched start")
                    item.ended = True
                else:
                    status = record.get("status")
                    if not isinstance(status, str) or status not in {"ready", "failed"}:
                        raise AssertionError("Publication status must be ready or failed")
                    if item.started and not item.ended:
                        raise AssertionError("Publication precedes execution end")
                    if status == "ready" and (not item.ended or record.get("value_present") is not True):
                        raise AssertionError("Ready publication requires completed execution and value_present=true")
                    item.phase = status
            except (IncompleteProbeLog, UnsupportedMetadata):
                raise
            except (AssertionError, ValueError) as exc:
                raise AssertionError(f"Line {line_number}: {exc}") from exc

    if run_id is None or not finished:
        raise IncompleteProbeLog("Missing run_start or final run_end")
    if not claims["owner"]:
        raise IncompleteProbeLog("No node lifecycle was captured")
    if require_tree:
        for task, node_seq in last_node_seq.items():
            if last_snapshot_seq.get(task, -1) <= node_seq:
                raise IncompleteProbeLog("Each observed task needs a final tree snapshot after its last node event")
    return {
        "run_id": run_id,
        "capture_mode": capture_mode,
        "event_count": sum(counts.values()),
        "events": dict(counts),
        "claims": {"owners": claims["owner"], "waiters": claims["pending"], "hits": claims["ready"], "failed": claims["failed"]},
        "count_semantics": "Observed response events, not distinct waiting consumers, delivered results, or saved executions.",
        "executions": counts["execute_start"],
        "publications": counts["publish"],
        "published_nodes": sum(item.phase in {"ready", "failed"} for item in lifecycles.values()),
        "snapshot_count": snapshots,
        "guarantees_checked": [
            "stable task/path/node identities", "one owner and at most one recorded execution per generation",
            "owner-only publication after execution", "claim/status lifecycle consistency",
            "snapshot agreement with recorded lifecycle" if snapshots else "metadata-only capture; tree not checked",
        ],
        "limitations": [
            "Relies on complete, authoritative lifecycle instrumentation; unseen execution cannot be excluded.",
            "Does not prove result contents, waiter delivery, call-ID mapping, sandbox restoration, or performance.",
            "No live lease takeover or attachment to an already warm cache; failed generations may be retried.",
            "No workload-specific expected topology oracle; controlled T01/T02 tests check the intended fixture shape.",
        ],
    }
