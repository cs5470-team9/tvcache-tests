# Live integration status

November 1, 2026 (America/New_York). Runs used this Mac's CPU, with no NERSC job,
GPU allocation, model serving, or actual container backend.

## Tested implementation pair

| Component | Revision | Location |
| --- | --- | --- |
| Rust server | `aa239b6a3b64c485b0189f55ced19926dd6df621` | [pending-nodes](https://github.com/cs5470-team9/tvcache-rust/tree/aa239b6a3b64c485b0189f55ced19926dd6df621) |
| Python client | `57d3727fe77200a4d598b7380b6aba5bd23641f7` | [pending-nodes](https://github.com/cs5470-team9/tvclient/tree/57d3727fe77200a4d598b7380b6aba5bd23641f7) |

Both checkouts were clean. Their `main` branches still contain the baseline;
integration did not merge or edit either implementation branch. The server
built with Rust 1.99.1 using `cargo build --locked`; all **35 Rust unit tests**
passed with `cargo test --locked`.
The TCP tests require permission to bind local sockets. A restricted automation
run hit `Operation not permitted`; the permitted runs passed. A socket-permission
failure is an environment issue, not evidence that the corresponding protocol
assertion failed.
Separately, **130 checker self-tests** passed with no skips. Those unit tests
do not themselves establish live integration coverage.

## Real client/server results

The independent testbench connected through the real Python client to a fresh
Rust TCP server with eviction disabled. A second fresh-server run increased
T01 contention from 8 to 32 callers. Both runs passed the following checks:

| Check | Observed result |
| --- | --- |
| Sequential smoke | Prefix-first insertion, branches, exact/empty values, misses, unchanged lookup topology |
| T01: identical concurrent key | One owner/execution; 7 pending claimants at 8 callers, and 31 at 32; warm lookup added no execution |
| T02: different state histories | One execution per history; separate nodes and correct A/B results |
| T03: different tasks | Same path remained isolated by task, with distinct task-specific values |
| T04: publication before first poll | Later long-poll saw the completed value without a lost completion |
| T05: empty result | Empty string remained ready on inspect and warm claim |
| T06: explicit failure | Failed node could be re-claimed with the same ID and a higher generation |
| T07: owner disconnect | Waiting caller observed failure and could retry with a higher generation |
| T09: stale/wrong owner | Old-generation and wrong-connection publications were rejected without corrupting the valid result |
| I01: read-only inspection | No graph created by a miss; pending/ready inspections left dumped task state and hit counters unchanged |

T01/T02 compared expected fixture edges and real IDs at before/pending/ready/
warm checkpoints. The adapter preserved untouched raw dumps alongside its
explicit root/prefix normalization. T06/T07/T09 used protocol string fixtures,
not actual tool effects or crash recovery of a sandbox.

Three implementation-supplied suites also passed in the 8-caller run:
`test_claim_client.py`, `test_coalescing.py`, and `test_coalescing_random.py`.
The executor suite used its documented `MockSandbox`, not a real container.
The randomized suite completed 5 seeds and observed coalesced waits; this is
finite stress coverage, not proof of all schedules.

## Evidence and limits

Detailed final-launcher reports are `results/integration-20261101-002.json` and
`results/integration-20261101-033.json`, with adjacent artifact directories for
server and upstream-suite logs. These files are intentionally Git-ignored, not
public repository artifacts. The permitted Rust test log is also retained at
`results/rust-unit-tests-20261101-permitted.txt`. Each integration report records
source SHAs, binary hash,
raw/normalized checkpoints, individual outcomes, and owned-server cleanup.
The temporary servers were stopped after the runs. Earlier initial captures
`integration-20261101-001.json` and `integration-20261101-032.json` are retained
locally too; report files were not overwritten.

These results establish initial protocol integration, not the whole project:

- Real-container restoration, harness call-ID mapping, whole-turn semantics,
  waiter cancellation, and end-to-end DeepSWE evaluation remain open.
- The executor's timeout/failure fallback may execute uncoalesced. The results
  do not establish exactly-once external side effects after failure or timeout.
- Testbench checkpoint polling is for dedicated correctness runs only. No
  structured buffered probe logger or cross-process causal join has been
  integrated into measured rollouts yet.
- The implementation listens on all interfaces. The launcher requires explicit
  opt-in, uses only synthetic data, checks its own post-bind readiness message,
  and terminates only the process it started.

Reproduce from the pinned pair with the commands in [README.md](README.md),
using a new output filename each time. Re-run after implementation changes;
passing these revisions is not a claim about future branches or deployments.
