# T08: waiter cancellation and connection reuse

November 3, 2026 (America/New_York). CPU-only correctness work; no GPU,
NERSC allocation, model, or actual container backend was used.

## Implementation and review

The real client fix is [tvclient PR #1](https://github.com/cs5470-team9/tvclient/pull/1).
Review was requested from Arjun Saini; the PR has now merged into `pending-nodes`.
At the user's request, team client `main` was fast-forwarded to the merged
`a910f4d8afa68aeed11c86995692c13410261a4e`. Its source tree is identical to the
verified candidate `5eda159648ccc40164d08c72a97f8d2184ba0dde`, including opt-in
coalescing and the cancellation fix. The old `57d3727` is retained only as a
negative control, not the current feature head. The Rust server remains unchanged at
`aa239b6a3b64c485b0189f55ced19926dd6df621`.

Only the team's client repository was promoted. Its GitHub "forked from" label
records the original course repository; no upstream repository was modified or
detached. Server `main` is still the baseline, so enabling `coalesce=True`
requires the tested server's `pending-nodes` revision. Default coalescing remains off.

The client discards a cancelled in-flight transaction's connection because the
wire protocol has no request IDs. `close()` detaches its reader/writer before
awaiting transport shutdown; the next request opens a fresh connection. The
cancelled request is not replayed and cancellation still propagates. Cancelling
a caller that is only queued for the lock must not close the active caller's
connection.

## Permanent tests

Seven socket-free regressions live with the client in
`tests/test_client_cancellation.py`: whole and partial late responses,
cancellation during drain, repeated cancellation during close, queued-lock
cancellation, immediate stream detachment, and healthy connection reuse.
They exercise the actual client and decoder with byte/transport fixtures, not
a simulated cache. They fail on the old source (2 passes, 2 failures, 3 errors)
and all pass on the fixed candidate.

The testbench's `lifecycle_cases.py` now includes live T08:

1. Claim a fresh root-level node with one owner and two pending waiters, all
   on separate connections; hold a single controlled execution behind a gate.
2. Observe both real decoders entering their response reads after requests
   were written/drained. Cancel one waiter without relying on a fixed sleep.
3. Verify the original node stays pending and the owner/surviving waiter are
   unaffected. Release the gate, publish, and check their result/identity.
4. Reuse the cancelled client for `get_fork_cost`, a different response type,
   then Inspect and a warm Claim. Verify a fresh connection, correct responses,
   stable ID/generation, one execution, the expected tree, and no fixture lease.

The reader proxy observes reads only; it never buffers, drains, or fabricates
network data. Its three self-tests are part of the **133 checker self-tests**,
all passing with no skips. These checker tests are separate from live coverage.

## Before/after live evidence

| Run | Expected/observed result |
| --- | --- |
| Original client `57d3727` | T08 fails with `expected ForkCost, got ClaimResponse`; owner and surviving waiter already completed correctly with one execution. Other nine cases and sequential smoke pass. |
| Fixed client `5eda159`, 8-call contention suite | All ten cases and sequential smoke pass. T08 detaches/reconnects correctly; three existing implementation Python suites also pass. |
| Fixed client `5eda159`, 32-call contention suite | All ten cases and sequential smoke pass on a second fresh server. |
| Promoted client `main` at `a910f4d` | Seven client regressions, sequential smoke, all ten live cases and the three implementation Python suites pass again. |

T08 itself always uses one owner plus two waiters; 8/32 refers to the suite's
T01 stress level. Raw task-only checkpoints, observations, source revisions,
binary hash, and cleanup results are retained locally in:

- `results/t08-20261103-before-fix.json`
- `results/t08-20261103-after-fix.json`
- `results/t08-20261103-after-fix-32.json`
- `results/t08-20261103-client-main.json`
- `results/t08-client-before-fix-unittest.txt`
- `results/t08-client-after-fix-unittest.txt`

Those captures and adjacent server/upstream logs are Git-ignored, not public
artifacts. All test servers were stopped after the runs.

## Reproduce

Use the fixed client/server pins in [README.md](README.md), build the server,
then run from this test repository:

```bash
PYTHONPATH=../tvclient python3 -m unittest discover -s ../tvclient/tests -v
python3 run_checks.py selftest
python3 run_checks.py integration --launch-local-server \
  --server-source ../tvcache-rust --client-source ../tvclient \
  --upstream-tests --summary --output results/t08-NEW.json
```

To reproduce the negative control, use a separate clean client checkout at
`57d3727` and pass it as `--client-source`. A failed T08 is expected for that
old revision. Do not weaken the test or put connection cleanup in the adapter
to make the old implementation appear to pass.

## Boundaries

- Decoder-entry events establish an in-flight request, not a forced internal
  server subscription interleaving. A server may notice a closed waiter only
  after its long-poll ends.
- Owner/survivor use separate connections. Closing a connection that also owns
  other pending keys legitimately fails those claims under existing policy.
- This fixture's root-level Inspect has no sandbox lease. The fix does not
  establish reclamation of unknown interrupted Find/Claim leases.
- One controlled in-process execution is not proof of exactly-once external
  effects, shell-command termination, or real container restoration.
