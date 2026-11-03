# TVCache correctness testbench

Testing owner: Chia-Heng Liu. Python 3.10+; standard library only for offline
checks. Run commands from this repository's root. In the original SysML
workspace the canonical checkout is `Project/team_repos/tvcache-tests`;
`Project/testing` is an older export.

This implements the testbench, **not** Arjun's pending-node cache. No protocol
opcodes, cache entries, ownership rules, or reference-repository code have been
changed. Passing self-tests verifies the checker and fixtures, not a working
parallel cache or DeepSWE baseline.

The offline checks consume files after capture; they do not connect to the
baseline, poll nodes, start tools, or reserve GPUs. Sample captures and
self-tests are synthetic fixtures, not evidence that the real cache passed.
Result files and actual run logs stay local and are ignored by Git.

Verified November 3, 2026: **133 checker self-tests passed, no skips**. That
command tests the checker only; live client/server results are separate below.

## T08 cancellation update — November 3

T08 now checks cancellation after an actual Inspect request is sent: one owner
and the surviving waiter complete, while the cancelled client must discard its
old connection and reconnect before making another request. The old client
reproduces `expected ForkCost, got ClaimResponse`; the fixed candidate passes
all ten live cases, the sequential smoke check, and existing implementation
suites. Seven permanent client transport regressions also pass.

The client fix in [PR #1](https://github.com/cs5470-team9/tvclient/pull/1) has
merged, and the tested version is now on **`tvclient/main` at `a910f4d`**.
It includes opt-in coalescing plus the cancellation fix. The server still needs
its `pending-nodes` revision for `coalesce=True`; server `main` is unchanged.
Use the paired SHAs below for the full current suite. The original
client `57d3727` is an intentional failing negative control for T08, not the
latest all-green pin. See [t08_status.md](t08_status.md) for before/after evidence,
reproduction, and ownership/lease limits.

## Live team integration

The real pending-node client and Rust server are now connected. The first
live CPU run passed the baseline smoke check, T01/T02, seven additional
protocol cases, and three implementation-supplied Python suites. A second run
with 32 contending callers also passed. These are correctness results, not
DeepSWE performance or real-container restoration results. See
[integration_status.md](integration_status.md) for the original November 1
coverage and limits, and the T08 update above for current candidate coverage.

The tested client is on `main`; the tested server is still on `pending-nodes`.
Keep this test repository on `main` and pin the paired implementation checkouts
to the verified commits:

```bash
# Place the three repositories beside each other if cloning a fresh workspace.
git clone https://github.com/cs5470-team9/tvcache-tests.git
git clone https://github.com/cs5470-team9/tvclient.git
git clone https://github.com/cs5470-team9/tvcache-rust.git
git -C tvclient switch --detach a910f4d8afa68aeed11c86995692c13410261a4e
git -C tvcache-rust switch --detach aa239b6a3b64c485b0189f55ced19926dd6df621

cd tvcache-rust
cargo test --locked
cargo build --locked
cd ../tvcache-tests
PYTHONPATH=../tvclient python3 -m unittest discover -s ../tvclient/tests -v
python3 run_checks.py integration --launch-local-server \
  --server-source ../tvcache-rust --client-source ../tvclient \
  --upstream-tests --summary --output results/integration-001.json
```

Rust/Cargo is needed to build the real server; the Python client and checker
need no extra Python packages. The integration command refuses dirty source
checkouts, selects a free port, starts its own server with eviction disabled,
and stops that owned process in cleanup. It never attaches to an existing
server or reserves a GPU. Existing report files are not overwritten.

**The current implementation binds all network interfaces.** The launch flag
explicitly opts in to a short-lived, dedicated server containing synthetic
fixtures only. Do not use it to hold secrets or on an untrusted network. Local
checks connect through `127.0.0.1`. Server logs, raw/normalized snapshots, source
revisions, binary hash, and individual outcomes are retained locally under
`results/`, which is not published.

For a server you already deliberately started for testing, use the built-in
adapter without launching another server:

```bash
python3 run_checks.py pending --dedicated-server \
  --client-source ../tvclient --host 127.0.0.1 --port 7879 \
  --case all --output results/pending-001.json
```

These active probes/gates belong in correctness runs. Measured rollouts should
still use buffered logs checked afterwards; the actual JSONL probe logger and
multi-producer causal joining remain integration work, not a completed feature.

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

The original baseline on the implementation repositories' `main` branches does
not contain the proposed IDs/lifecycle metadata.
Use `--legacy` to check its structure and values without inferring node states.
Without `--legacy`, missing metadata is reported as blocked—not as passing
pending-node coverage. Stable-identity checks require actual metadata.

For a raw dump from the team's `pending-nodes` revision, add `--team-format`.
This explicitly maps its ready/null root to `root` and valueless ready child
nodes to structural `prefix`; real IDs, generations, values, and edges remain
unchanged. An empty string stays a ready result. Root generation 0 is accepted
only for the structural root.

## Standalone sequential-cache smoke check

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
the check; it is not converted into a skip or a pass. Add `--team-format` when
using the feature branch's raw dump encoding. The integration command above
already selects that mapping and keeps the unmodified raw checkpoints too.

## T01 and T02: adapter to Arjun's real implementation

The scenarios are implemented in `tvcache_checks/scenarios.py`. The built-in
adapter in `tvcache_checks/team_adapter.py` calls the real `Claim`, `Inspect`,
`Publish`, and `DumpAll` APIs on the team's `pending-nodes` revisions. There is
no bundled replacement cache or simulated server. The original baseline client
is detected as unsupported before opening a connection.

For other implementations, an adapter module can provide a synchronous factory returning a fresh
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
The built-in adapter does this. Real ownership is enforced by the server's
connection identity and generation, not a wire token. Its private local marker
only guards against accidental publication through a different/reconnected
adapter. Claim-result sandbox leases are released without closing the owner
connection. Private ownership markers are never logged.

To supply a different adapter, put its directory on `PYTHONPATH` and run:

```bash
PYTHONPATH=/path/to/arjuns/adapter \
python3 run_checks.py pending \
  --dedicated-server --adapter arjun_adapter:client_factory \
  --case all --output results/pending-001.json
```

The adapter should configure its host/port through its own configuration and
must point to a dedicated test instance. No credentials should be hardcoded
or included in reports. Without either `--adapter` or `--client-source`, this
mode records `blocked` and does not connect to anything.

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

T01/T02 themselves do not cover owner failure/takeover; the integration command
runs separate protocol cases for failure, retry, disconnect, and stale owners.
T08 separately verifies waiter cancellation/connection reuse with a gated
in-process fixture. It does not test interrupted Find/Claim lease recovery.
Real sandbox state restoration, whole-turn layouts, and
actual harness call-ID mapping remain in the [test matrix](test_plan.md).
`--timeout` is a hang deadline, not a
performance target. Reports log public outcomes but omit owner publication tokens.

## Files

- `run_checks.py`: CLI and report/status handling.
- `tvcache_checks/tree.py`: read-only tree and lifecycle assertions.
- `tvcache_checks/contracts.py`: minimal adapter types.
- `tvcache_checks/scenarios.py`: gated T01/T02 scenarios and controlled tool.
- `tvcache_checks/baseline.py`: real sequential client smoke check.
- `tvcache_checks/probes.py`: offline JSONL lifecycle and snapshot validation.
- `tvcache_checks/team_adapter.py`: adapter to the team's real pending-node API.
- `tvcache_checks/lifecycle_cases.py`: isolated failure/identity/inspection cases.
- `tvcache_checks/integration.py`: owned-server launcher, provenance, and cleanup.
- `unit/`: self-tests of parsing, assertions, contracts, and synchronization.
- [Interface proposal](minimal_interface_proposal.md) and
  [Arjun checklist](arjun_interface_checklist.md): handoff requirements.
