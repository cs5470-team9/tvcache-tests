# Buffered probe logs: proposed handoff

The baseline and cache implementation record events; this repository checks
files after the run. The checker never polls the measured baseline. This format
is a proposal to agree with Arjun, Joseph, and Neil, not an already integrated
server feature.

## Keep correctness and timing runs separate

Correctness runs use tiny deterministic fixtures, controlled execution gates,
and tree dumps at a few checkpoints. They can afford richer instrumentation.
Measured DeepSWE runs should use small buffered metadata records, with no full
tree dump, synchronous console output, or extra probe RPC at each lookup. Queue
serialization and file writes off the execution path where possible; finish
flushing outside the timed region and count any dropped records.

Logging is not free. Compare matched logging-on/off runs before interpreting
timings; record the capture mode and instrumentation revision with the results.
Do not count status polls as hits or repeated pending responses as duplicate
executions avoided.

## Important ordering constraint

The first validator accepts **one complete, authoritatively ordered lifecycle
stream**. It is directly suitable for a small controlled correctness capture.
It does not reconstruct causality from independent process clocks. Tool execution
is client-side in the supplied architecture, so the Rust server's log alone
cannot attest to actual tool entry.

Do not add per-tool synchronization or RPCs merely to produce this stream during
GPU measurements. Keep the natural server and harness logs independently
buffered, with producer IDs, local sequence numbers, and shared task/node/
generation/attempt IDs. Joining those logs with explicit causal references is a
follow-up integration task. Timestamp-sorting them and inventing `seq` values
does not satisfy this validator's ordering guarantee. Until that integration is
agreed, use the strict checker for controlled correctness captures, not as a
claim that arbitrary performance logs prove coalescing.

## Version 1: complete lifecycle capture

One JSON object per line, UTF-8. Common fields:

| Field | Meaning |
| --- | --- |
| `event` | One of the event names below |
| `run_id` | Nonempty ID for this capture; never mixed with another run |
| `seq` | Contiguous authoritative ordering for this run, beginning at 0 |
| `ts_ns` | Nonnegative monotonic timestamp in nanoseconds; not a cross-host clock |
| `task_id` | Required on node and tree events |
| `path` | Existing canonical history/call path, a nonempty array of strings on node events |
| `node_id` | Nonnegative per-task integer on node events; stable throughout this capture |
| `generation` | Positive execution generation on node events; not a substitute for node identity |
| `attempt_id` | Nonempty execution/requester identity on node events |

`run_start` has `schema_version: 1`, `capture_mode: "correctness"` or
`"performance"`, and `events_complete: true`. Set `sampled: true` or
`events_complete: false` honestly if coverage is partial; the checker will
report `blocked`, not a correctness pass. `performance` allows the same complete
metadata stream without tree dumps; it does not relax the ordering requirement
or turn checker output into a performance measurement.

| Event | Extra fields and where to observe it |
| --- | --- |
| `claim` | `outcome: owner/pending/ready/failed`, at the actual atomic cache decision |
| `status` | Same outcomes, on read-only inspection; not counted as a claim/hit |
| `execute_start` | At actual tool entry, not at a cache miss or scheduling decision |
| `execute_end` | When that owner execution terminates |
| `publish` | `status: ready/failed`; successful publication also has `value_present: true` |
| `tree_snapshot` | `task_id`, `stage: before/pending/ready/warm`, and `dump` in existing `DumpAll` shape |
| `run_end` | `completed: true`, `dropped_events: 0`, emitted after all producers drain |

Capture from a fresh task before its first owner claim, then continue through
publication and warm reuse. Attaching to an already warm graph is unsupported.
Every executable node in a snapshot must have its observed owner lifecycle;
structural root/prefix nodes need not be executions. No eviction, server restart,
or live lease takeover is modeled. A terminal failed generation can retry as
the next generation on the same node.

The successful path requires one recorded owner and at most one actual tool
execution per node/generation, execution end before publication, and matching
states/IDs on subsequent claims and snapshots. Incomplete work or a missing
footer blocks the verdict. Contradictory events fail it. A failed publication
may occur before execution starts, but cannot terminate an execution still
recorded as running.

`value_present` is true for a valid empty result too. Do not copy stdout into
performance events. Never log private owner/publication tokens, Hugging Face
tokens, or other credentials. Canonical paths can contain tool arguments and
may be sensitive; keep actual logs private. Only synthetic fixtures belong in
this repository.

## Tree evidence and limits

Snapshot nodes use the existing nested `children` map plus `node_id`, explicit
`status` (or `state`), and `generation` for executable nodes. Ready nodes have a
string result, including `""`; pending nodes have no completed value. Parent
edges are derived from the nested tree, not another index.

```bash
python3 run_checks.py probes fixtures/example_probe_log.jsonl --require-tree
python3 run_checks.py probes /path/to/capture.jsonl --output results/check-001.json
```

The example is synthetic. The checker compares snapshots with observed
lifecycles and stable placement; it cannot know that an extra structural branch
is unwanted without an independent expected fixture. `--require-tree` requires
final tree coverage, not every T01/T02 checkpoint or an expected-tree oracle.
Use the gated scenarios for known expected topology and outputs. Passive event
consistency alone does not prove waiter delivery, result contents, call-ID
mapping, side-effect restoration, crash safety, or exactly-once external effects.

## Next integration step

The team's `pending-nodes` branches now expose real node IDs, lifecycle states,
Claim/Inspect/Publish, and checkpoint dumps. The adapter and live correctness
cases are connected; see [integration_status.md](integration_status.md).
They do not yet emit this complete JSONL format. Agree on cheap capture points
and multi-producer causal references with the harness/evaluation owners before
treating rollout logs as complete evidence.

The sequential `Find/Insert` baseline on the implementation `main` branches
still has no pending API. Do not synthesize owner/pending events or node IDs to
make that baseline pass. Its saved tree can be inspected with `tree --legacy`;
its existing output/performance logs remain useful without claiming coalescing.
