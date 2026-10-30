# TVCache correctness testbench

Testing owner: Chia-Heng Liu. Python 3.10+; standard library only for offline
checks. Run commands from this repository's root (or `Project/testing` in the
original SysML workspace).

This implements the testbench, **not** Arjun's pending-node cache. No protocol
opcodes, cache entries, ownership rules, or reference-repository code have been
changed. Passing self-tests verifies the checker and fixtures, not a working
parallel cache or DeepSWE baseline.

The offline checks consume files after capture; they do not connect to the
baseline, poll nodes, start tools, or reserve GPUs. Sample captures and
self-tests are synthetic fixtures, not evidence that the real cache passed.
Result files and actual run logs stay local and are ignored by Git.

Verified October 30, 2026: **82 checker self-tests passed, no skips**. No real
cache, container, model, or GPU was exercised by this verification.

## Run now: test the testbench

```bash
python3 run_checks.py selftest
```

For a JSON self-test report, add a new output filename:

```bash
python3 run_checks.py selftest --output results/selftest-001.json
```

Existing report files are never overwritten. Reports distinguish `passed`,
`failed`, and `blocked`, with exit codes 0, 1, and 2 respectively.

## Validate captured probe logs offline

Ask the implementation to record small buffered events during a run; validate
the capture afterwards. See [probe_log_format.md](probe_log_format.md) for the
proposed fields, instrumentation points, and capture completeness requirements.
The logger is not yet implemented in the real server or harness.

```bash
python3 run_checks.py probes fixtures/example_probe_log.jsonl --require-tree
python3 run_checks.py probes /path/to/run.jsonl --output results/probes-001.json
```

The first command checks a synthetic example. With an actual complete capture,
the validator checks recorded owner/execution uniqueness, stable node IDs and
paths, pending/ready/failed transitions, and any included tree snapshots.
Partial, sampled, dropped, or empty captures are `blocked`, not a passing
correctness result. No socket, live polling, or GPU work is involved.

Two capture modes keep diagnostics separate from measurement:

- `correctness`: full event coverage plus selected tree checkpoints on a tiny
  controlled fixture. Use `--require-tree` when tree evidence is required.
- `performance`: metadata-only events; no tree dumps or tool-output bodies.
  Logging still costs something; compare logging on/off before trusting timings.

Passive logs alone cannot prove correct tool values, that all waiters received
their result, response call-ID mapping, or sandbox restoration. The dedicated
T01/T02 tests below provide controlled contention and known expected fixture
trees; later integration cases cover the remaining behavior.

## Probe every node in a saved tree

```bash
python3 run_checks.py tree \
  fixtures/example_pending_tree.json \
  --task-id example-task
```

The supplied JSON is an **illustrative fixture**, not output from a server run.
Replace it with Arjun's actual `DumpAll` JSON to inspect a real saved tree.
The checker reports each node's path, parent path, node ID, lifecycle state,
generation, value, and sandbox reference. It rejects duplicate IDs, malformed
trees, pending nodes with completed values, and ready nodes with absent results.
An empty string is a valid ready result.

The current baseline dump does not contain the proposed IDs/lifecycle metadata.
Use `--legacy` to check its structure and values without inferring node states.
Without `--legacy`, missing metadata is reported as blocked—not as passing
pending-node coverage. Stable-identity checks require actual metadata.

## Opt in later: real sequential-cache smoke check

Only run against an approved dedicated test server. The command writes small
fixtures under a fresh UUID task namespace; it does not delete cache entries,
evict a teammate's data, drain sandbox references globally, or start a server.

```bash
python3 run_checks.py baseline \
  --dedicated-server --host 127.0.0.1 --port 7878 \
  --client-source /path/to/tvclient \
  --output results/baseline-001.json
```

This imports the actual [course client](https://github.com/abhivijay96/tvclient)
from the explicitly chosen checkout. It is not bundled in this repository.
Within the original workspace, `Project/references/tvclient/` is also discovered
automatically if present.
It checks prefix-first insertion, branches for two histories, empty-string
results, misses, and unchanged tree shape after lookups. Six node-by-node
snapshots are included in the report. Missing node metadata stays labeled
`legacy`. These tiny fixtures do not test real files or sandbox restoration.

Use a compatible dedicated [course server](https://github.com/abhivijay96/tvcache-rust).
A connection error fails
the check; it is not converted into a skip or a pass. No real-server check has
been run as part of the initial implementation.

## T01 and T02: adapter to Arjun's real implementation

The scenarios are implemented in `tvcache_checks/scenarios.py`. They use an
adapter because the pending-node wire/API has not yet been agreed and the
current course server does not implement it. There is no bundled replacement
cache or simulated server that could be mistaken for the real implementation.

Arjun's adapter module should provide a synchronous factory returning a fresh
async client context manager. Each client implements
`tvcache_checks.contracts.PendingClient`:

| Method | Test-side behavior |
| --- | --- |
| `lookup_or_claim(task_id, path, attempt_id)` | Atomically return an `Outcome` with owner, pending, ready, or failed status; waiters return pending immediately |
| `inspect(task_id, path)` | Read-only status for polling; never create or claim a node |
| `publish(task_id, path, node_id=..., generation=..., owner_token=..., value=...)` | Complete a claimed fixture node, or raise on rejection |
| `dump_all()` | Return the existing task/root/children JSON structure with normalized node metadata |

Return `Outcome` instances from claim/status calls, not raw wire messages.
These names are local adapter methods, not a demand to use the same wire names.
Root/prefix/ready/pending/failed metadata can be normalized in the adapter;
do not invent missing server IDs or states simply to pass assertions.
The factory must open a distinct connection/context for every invocation.
Do not share one existing `TVCacheClient` concurrently across the callers.

Once a real adapter is available, put its directory on `PYTHONPATH` and run:

```bash
PYTHONPATH=/path/to/arjuns/adapter \
python3 run_checks.py pending \
  --dedicated-server --adapter arjun_adapter:client_factory \
  --case all --output results/pending-001.json
```

The adapter should configure its host/port through its own configuration and
must point to a dedicated test instance. No credentials should be hardcoded
or included in reports. Without `--adapter`, this mode records `blocked` and
does not connect to anything.

### What the scenarios actually verify

- **T01:** 8 attempts claim the same child at a valid common history. A start
  barrier creates contention and a gate holds the actual fixture execution.
  One owner and 7 pending responses must refer to the same node ID/generation.
  The tool-entry counter must stay at 1; no result may return before release.
- **T02:** two valid histories with the same read invocation. Two callers per
  history must share one execution within that history, but results A and B
  and their node IDs must remain distinct across histories.
- **Node probes:** `before`, `pending`, `ready`, and `warm` snapshots capture
  every node in the fixture task. The checker compares exact edges, shared
  prefixes, stable IDs, status/generation, and result presence. A warm hit must
  add neither a node nor an execution.

Setup prefix nodes are controlled test fixtures published through the same
claim API. They represent state histories for this protocol test; they are not
evidence of real setup-command execution or container restoration.

The scenarios do **not yet cover** owner failure/takeover, real sandbox state
restoration, whole-turn layouts, or actual harness call-ID mapping. These remain
in the [test matrix](test_plan.md). `--timeout` is a hang deadline, not a
performance target. Reports log public outcomes but omit owner publication tokens.

## Files

- `run_checks.py`: CLI and report/status handling.
- `tvcache_checks/tree.py`: read-only tree and lifecycle assertions.
- `tvcache_checks/contracts.py`: minimal adapter types.
- `tvcache_checks/scenarios.py`: gated T01/T02 scenarios and controlled tool.
- `tvcache_checks/baseline.py`: real sequential client smoke check.
- `tvcache_checks/probes.py`: offline JSONL lifecycle and snapshot validation.
- `unit/`: self-tests of parsing, assertions, contracts, and synchronization.
- [Interface proposal](minimal_interface_proposal.md) and
  [Arjun checklist](arjun_interface_checklist.md): handoff requirements.
