# Pending-node and parallel-turn correctness tests

Owner: Chia-Heng Liu (proposed) · October 28, 2026

Scope: Team 9's current project proposal, step 4. Start with
single-key coalescing, then test the initial whole-turn implementation. Per-call
and hybrid layouts are later comparisons, not prerequisites for the first tests.

November 1 status: the team `pending-nodes` client/server are connected and
live CPU checks pass for T01–T07, T09, and I01 (read-only inspection), plus the
sequential smoke check. See [integration_status.md](integration_status.md) and
[README.md](README.md) for revisions and commands. T01 passed at 8 and 32 callers.
The matrix below remains the broader plan: unlisted cases are not claimed as
passed. Adapter method names are test-side names; real wire methods are
Claim/Inspect/Publish, with connection-bound ownership.

## First handoff to Arjun

The initial interface/recovery policy is documented in the team's
[implementation contract](https://github.com/cs5470-team9/tvcache-rust/blob/aa239b6a3b64c485b0189f55ced19926dd6df621/docs/pending_nodes.md).
The connected testbench exercises that actual cache, not a second cache
implementation that could pass its own tests.

The first two tests are T01 and T02. They need no model, GPU, DeepSWE container,
or real filesystem snapshot: use controlled tool execution with known inputs,
results, and an execution counter. Later restoration tests require the real
sandbox adapter and coordination with Joseph.

## Proposed observable contract

Concrete low-cost draft: [minimal_interface_proposal.md](minimal_interface_proposal.md).
Add monotonic per-task node IDs to claim/lookup responses and the existing
tree dump. The testbench will verify tree shape and node lifecycles as well
as outputs. The original proposal is retained as background; the current
adapter maps to the implemented contract and retains raw metadata beside
normalization.

These are behaviors to agree on, not a prescription for wire opcodes or storage.

| Operation/event | Minimum information needed by the tests |
| --- | --- |
| Claim or lookup | Logical cache key; whether the caller is the execution owner, a waiter, or a completed-result hit |
| Ownership | Owner token/generation, or equivalent server-side ownership identity |
| Publish success | Result and any required post-state reference; only the valid owner can complete the node |
| Await completion | A bounded way to receive the published result, failure, or retry decision without a lost wake-up |
| Owner failure/expiry | A specified error/recovery policy; no pending entry can strand callers forever |
| Test inspection | Enough events/status to establish which caller claimed, waited, executed, and completed, without depending on arbitrary sleeps |

The ownership decision must be atomic: a separate miss lookup followed by
insertion can still let two callers both execute. A pending entry is not a
completed-result hit. Synchronization must also cover the race between seeing
pending, subscribing to completion, and the owner publishing its result.

The logical key must distinguish task identity, compatible input-state history,
and the canonical call or call group. Attempt IDs and current call IDs identify
requesters but must not prevent otherwise eligible reuse across attempts.
Equal command text alone is not a sufficient cache key.

## T01 — identical concurrent calls execute once

### Fixture

- One unique task namespace and one valid, common state-history prefix.
- A deterministic read-only tool with a known result, such as reading a
  controlled fixture value. Avoid network, clock, and random inputs.
- Initially 8 callers representing separate attempts. Each has its own
  connection/executor and distinct request/call IDs.
- A test-controlled execution gate and a thread/process-safe execution counter.
  Increment the counter at the actual tool entry point, not at cache lookup.

### Schedule

1. Make the same eligible logical key available to all callers and release a
   start barrier. Do not allow the first tool execution to finish yet.
2. Observe the owner and confirm the other callers have reached the pending
   key. The API/event interface must make this observable.
3. While execution is held, check that only one owner exists, the execution
   count is 1, and no caller has received a successful completed result.
4. Release the execution gate. Await all caller outcomes with a bounded timeout.
5. Look up the completed key once more to check the ordinary warm-hit path.

### Assertions

- Exactly one tool execution in this successful, no-retry scenario.
- One owner and 7 waiters while the owner is held, provided the pending lookup
  response is exposed; otherwise equivalent observable server events.
- All 8 callers receive the same correct tool result, without mixed responses
  or an empty pending placeholder being treated as a result.
- At the client/agent response boundary, each result retains its caller's call
  ID. If this is outside the first server test, verify it separately in T10.
- The node is completed. No unresolved waiters remain for this key.
- The later warm lookup does not increment the execution counter.
- Owner, waiters, and warm hit refer to the same node ID. Dumps before/after
  publication show one child under the expected prefix, the same ID throughout,
  and the pending → ready transition without duplicate node insertion.

Use explicit barriers/events to establish the schedule. A timeout catches a
hang; it is not a latency target. Repeat with 2, 8, and 32 callers after the
initial case is stable. Repetition is useful stress coverage, not proof that
all possible interleavings are correct.

## T02 — different input-state histories do not coalesce

Use one task and identical tool arguments with two distinct state-changing
prefixes. For example, one prefix sets a fixture file to A and another sets it
to B; the current call reads that file. The histories must be valid paths in
the actual cache, not simply arbitrary version labels attached by the fixture.

Launch two callers for each history. Hold both owner executions so they overlap,
then release them and gather the results.

Assertions:

- Two logical keys and two executions total: one owner per state history.
- The two callers at state A receive A; the two at state B receive B.
- Same-history duplicates share work, but no result crosses the state boundary.
- Subsequent warm lookups return the appropriate results without execution.
- The tree dump contains the expected two branches with distinct child IDs
  under their state-history parents; shared ancestors are not duplicated.

The first protocol test can use controlled state fixtures. Record that this
does not prove real container restoration; T13 tests that separately.

## Test matrix

| ID | Case | Expected behavior | Stage |
| --- | --- | --- | --- |
| T01 | Same task, state history, and call; simultaneous requests | One normal-path execution; all eligible callers receive the result | First |
| T02 | Same task/call, different valid histories | Separate executions and correct state-specific results | First |
| T03 | Same history/call under different tasks | No cross-task coalescing or result leakage | Core |
| T04 | Owner completes before another caller subscribes | Late caller receives completion; no lost wake-up | Core |
| T05 | Cached result is empty or otherwise false-like | Result is still a completed hit, not pending/missing | Core |
| T06 | Owner explicitly fails | Waiters receive the agreed error/retry outcome; no false success or hang | Core |
| T07 | Owner connection/process disappears | Specified expiry/recovery resolves waiters; stale pending state is bounded | Core |
| T08 | Waiter cancels | Other waiters and the valid owner remain correct; cleanup follows policy | Core |
| T09 | Old owner publishes after ownership transfers | Old completion cannot overwrite the valid generation's result | If takeover is supported |
| T10 | Different call IDs for equivalent calls | Each caller gets its own response IDs; IDs do not accidentally defeat reuse | Client integration |
| T11 | Identical whole turns arrive concurrently | One eligible group execution; every call's result and ID is assembled correctly | Whole-turn integration |
| T12 | Calls finish in a different order from issue order | No swapped results or IDs; output follows the agreed response contract | Whole-turn integration |
| T13 | Cache hit on a state-changing setup call, followed by a new call | Later execution sees restored/replayed effects in an isolated sandbox | Real sandbox integration |
| T14 | One call in a whole turn fails or is cancelled | Explicit group/result policy; no incomplete state published as a fully successful turn | Whole-turn integration |
| T15 | Only part of a requested group is cached | Behavior matches the chosen layout: partial reuse where supported, or documented group miss | Layout-dependent; expand for per-call/hybrid |
| T16 | Whole turns `[A,B]` and `[A,C]` | Different whole-turn entries; do not assume per-call sharing in the initial layout | Whole-turn integration |
| T17 | Duplicate invocation occurs twice within one turn | Both logical occurrences receive correctly mapped results; any sharing follows the agreed state/side-effect policy | Whole-turn integration |
| T18 | Inspect tree before claim, while pending, after publish, and on a warm hit | Correct edges, stable node IDs, one logical node per key, and expected status transitions | First/core |

For T11, test the same ordered call list first. Whether reordered calls form an
equivalent group is a design decision; do not assume unordered equivalence for
calls with side effects. No-cache and existing sequential-cache behavior are
controls, not expected to satisfy pending-node coalescing assertions.

## Failure and state-safety limits

The no-failure case should execute once. A lease/retry protocol does not by
itself guarantee exactly-once external effects after crashes: the old owner may
have changed state before losing ownership. Agree on safe recovery, attempt
isolation, and publication fencing before testing takeover. Do not automatically
rerun a state-changing call while an older execution may still be active.

Returning an old writer's stdout is not sufficient for T13. Validate actual
state in the resumed sandbox, and confirm that attempts do not share a writable
environment. A failed mutation must not silently become a reusable successful
state. The exact recovery/restore policy must come from the implementation
contract rather than a testbench assumption.

## Logs and execution discipline

Measured baseline runs use buffered probe files checked offline, without live
testbench polling. Full tree dumps belong in dedicated correctness captures,
not the GPU timing path. The first proposed format and its ordering limitations
are in [probe_log_format.md](probe_log_format.md); the offline validator checks
recorded lifecycle consistency, not an independent expected workload or result.

For each test, record a unique namespace, source revisions, case ID, task/state
key, attempt/turn/call IDs, owner generation where available, events, tool-entry
counts, outcomes, and monotonic timestamps. Suggested events include claim,
wait, execute-start/end, publish, result-returned, timeout, cancellation, and
cleanup. Coordinate names with Neil rather than creating a competing log schema.

Use a dedicated server instance. Give tests unique task namespaces and explicitly
control warm/cold state. Do not clear a teammate's cache or run destructive
eviction tests against it. Each concurrent caller needs a compatible connection;
the existing client documents sequential requests on each persistent connection.

## Questions to settle before implementation

- What API atomically chooses owner, waiter, or completed hit?
- Does waiting use a request, subscription, or polling, and how is the
  completion-before-subscription race handled?
- How are owner failure, expiry, retries, and stale completion treated?
- Is ownership per call or per whole turn in the first implementation?
- What state reference accompanies a completed state-changing call/group?
- Does result availability also guarantee snapshot readiness, or does the
  existing lazy restore/replay path supply the required state later?
- Which events can tests observe, and which result/call-ID mapping belongs in
  the cache client versus the harness?

Claim/Inspect/Publish, node metadata, ownership fencing, terminal failure,
disconnect handling, and long-poll behavior are implemented and connected at
the pinned revisions. Later layout, harness, and real-sandbox policies still
need coordination; do not report checker self-tests as live-cache coverage or
protocol failure/retry checks as crash-safe external effects.

## Handoff checklist

- [x] Draft the first two test cases and broader matrix.
- [x] Identify the minimum interface and failure-policy questions.
- [x] Propose cheap node IDs and tree verification using the existing dump.
- [x] Implement the node inspector and gated T01/T02 workflows behind an adapter.
- [x] Run testbench self-tests; no real cache/model was exercised.
- [x] Add offline lifecycle probe-log checks and synthetic regression fixtures.
- [ ] Agree on baseline/cache capture fields and multi-producer causal ordering.
- [x] Map Arjun's documented contract and pin the tested revisions.
- [x] Connect and run T01/T02 against the real server/client on CPU, at 8 and 32 callers.
- [x] Add/run task isolation, completion-before-poll, empty results, failure/retry,
  disconnect, stale publication, and read-only inspection/counter cases.
- [ ] Add waiter cancellation and additional targeted race/stress schedules.
- [ ] Integrate whole-turn/call-ID and actual sandbox-restoration tests.

Reference sources inspected: current proposal;
[team client](https://github.com/cs5470-team9/tvclient) interfaces;
[team server](https://github.com/cs5470-team9/tvcache-rust) `types.rs` and
`request_handler.rs`. Reference repositories are not bundled here.
