# Pending-node interface: first testing handoff

Draft for discussion with Arjun · Chia-Heng Liu · October 28, 2026

November 1 update: Arjun's [implemented contract](https://github.com/cs5470-team9/tvcache-rust/blob/aa239b6a3b64c485b0189f55ced19926dd6df621/docs/pending_nodes.md)
answers the initial handoff. The built-in team adapter is connected and live
CPU checks pass; see [integration_status.md](integration_status.md). The
questions below are retained as the original checklist, not missing core APIs.

Suggested low-cost interface: keep task/path keys, assign each new node a
per-task integer ID, return that ID with owner/pending/ready outcomes, and expose
it in the existing `DumpAll`. No full state hash, UUID, separate parent index,
or new diagnostic endpoint is needed for the initial tests.

The concrete draft is in [minimal_interface_proposal.md](minimal_interface_proposal.md).
For measured runs, prefer buffered probe logs validated offline over live
testbench polling. See [probe_log_format.md](probe_log_format.md) for the
proposed capture points, fields, and ordering limits. No extra per-tool probe
RPC is requested on the measurement path; multi-process log joining still
needs agreement with the harness/evaluation owners.

The first tests will check:

1. Concurrent identical requests at the same task/state execute once, and all
   waiters receive the correct result and refer to the same node ID.
2. The same command at different state histories is not coalesced; the tree
   contains distinct children under the right state-history parents.
3. Pending → ready preserves node identity/placement, and a warm hit adds
   neither a node nor an execution.

Before connecting these tests, can the implementation contract specify:

- An atomic claim/lookup response distinguishing owner, waiter, and completed hit.
- How the owner publishes completion and waiters retrieve it without missing a wake-up.
- The owner identity/generation and behavior on failure, disconnect, and timeout.
- Whether retries/takeover are supported, and how stale-owner publication is rejected.
- Whether the first pending key represents one call or a whole turn, and what
  task/state/history information forms that key.
- The result/state-reference format and where current call IDs are attached.
- Events or status that let a test confirm callers are waiting before releasing
  a controlled tool execution.
- IDs and explicit lifecycle metadata in the existing nested tree dump. The
  test can derive parent/child edges, so no extra parent-ID field is required.

Read-only status polling is acceptable initially if it is simpler than waiter
notifications. It must be bounded, must not miss published results, and must
not count every poll as a new hit. Ownership generation/token is separate from
node identity so stale publication can be rejected if takeover is supported.

The tests will use a dedicated CPU-side server instance, separate caller
connections, execution gates, and an actual tool-entry counter. No GPU or model
is needed. Initial state fixtures do not substitute for later real-sandbox
restoration tests.

The broader cases are in [test_plan.md](test_plan.md). Core APIs now map to the
implemented Claim/Inspect/Publish contract. Whole-turn layout, actual harness
call-ID handling, real sandbox restoration, and buffered capture integration
remain to be settled/tested with their respective owners.
