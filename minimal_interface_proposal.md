# Minimal pending-node interface and tree checks

Draft for Arjun and Chia-Heng · October 28, 2026

The aim is to expose enough information for correctness tests without adding
expensive state hashing, a second graph index, or a new diagnostics service.
This is a proposed contract; the pending-node interface is not implemented yet.

## 1. Reuse the existing key and tree

Keep the existing task ID and canonical path as the lookup key. The path already
identifies the state-history prefix and current call. No new environment hash
or UUID is needed for this first implementation. This reuses the baseline's
state-equivalence assumptions; it does not prove arbitrary environments equivalent.

For whole-turn storage, one path entry may represent the agreed ordered group
of calls. Per-call versus whole-turn layout remains a design choice. Caller
call IDs should not become part of a reusable key.

The current Rust `Node` has no numeric node ID. A cheap addition would be:

- `node_id`: a monotonically assigned integer within one task graph.
- Root ID: 0.
- `next_node_id`: incremented only when a new node is created, under the
  graph's existing mutex. Do not use a live node count that can decrease on eviction.
- IDs are scoped to the task and server lifetime. Tests use fresh task
  namespaces and do not compare IDs across restarts.

Keep `node_id` unchanged when pending becomes ready or failed. If a removed
node is recreated, allocate a fresh ID rather than reusing the old one.
Do not store a second copy of the full path or parent pointer just for testing;
the current nested tree already contains that structure.

## 2. Return the ID in normal lookup/claim responses

The client needs an atomic lookup-or-claim operation. A separate Find followed
by Insert still permits duplicate owners. The logical outcomes are:

| Outcome | Meaning | Minimal response information |
| --- | --- | --- |
| `owner` | This caller created/claimed the pending execution | `node_id`, `generation`, owner token |
| `pending` | Another caller owns the execution | Same `node_id` and `generation` |
| `ready` | A completed result is available | Same `node_id`, generation, result, and state reference where applicable |
| `failed` | Execution failed or ownership expired under the agreed policy | `node_id`, generation, error/recovery outcome |

The request already supplies task/path, so these do not need to be repeated
in every response. The test client can log them alongside the returned node ID.
Do not include the owner's private publication token in waiter responses or
diagnostic dumps.

Example conceptual responses, not a wire-protocol specification:

```json
{"status": "owner", "node_id": 12, "generation": 1, "owner_token": "opaque"}
{"status": "pending", "node_id": 12, "generation": 1}
{"status": "ready", "node_id": 12, "generation": 1, "value": "A"}
```

Use an explicit status rather than inferring readiness from a nonempty value.
An empty result can be valid, and an old snapshot-only node can have no cached
value. The existing prefix/snapshot nodes and the root must be distinguished
from executable pending nodes.

## 3. Separate logical node identity from execution ownership

`node_id` identifies the reusable tree node; it does not grant permission to
publish. Use an owner token/generation to validate success or failure reports.
If takeover is supported, change the generation and reject late publication
from the previous owner without changing the logical node ID.

Perform claim and publication state transitions under the existing per-task
graph lock, but release that lock before running tools or waiting. Waiting
must not block the owner's publication.

For a low-effort first version, waiters can poll a read-only status lookup with
bounded backoff and a deadline. A notification/subscription mechanism can follow
if needed. The completed state must remain discoverable if publication occurs
before the first poll; polls must not create nodes or claim ownership.
Repeated status polls must not inflate completed-hit or coalescing counts.
Tests can disable eviction on their dedicated server to make the basic lifecycle
deterministic; production eviction rules still need a policy.

Owner failure can initially produce a terminal failure if that is the agreed
policy. Do not require automatic takeover for the first successful-coalescing
test. If retry is later added, its state recovery and stale-owner protection
need tests before use with state-changing operations.

## 4. Extend the existing tree dump

The server already has `DumpAll`, which serializes each task's nested `root`
and `children`. Add the new node metadata to that serialization rather than
building another endpoint. Tests derive each parent from the nested structure
and recover the edge key from its `children` dictionary.

Useful diagnostic fields are:

- `node_id` and explicit lifecycle state.
- Current ownership generation, where relevant.
- Existing snapshot/state-reference fields, without private ownership tokens.
- A way to distinguish absent and valid empty results; explicit status or
  value presence is enough, so no new result hash is required.

No waiter list or waiter count is required initially: the test can record each
caller's `pending` response before releasing the execution gate. Add counters
only if the implementation already tracks them or a later test needs them.

Use the dump only at test checkpoints, not after every production lookup. Keep
fixture outputs small and nonsensitive. If a metadata-only dump is later added,
omit result bodies by default. The existing dump is consistent within a task
graph, not necessarily one global instant across every task; gate execution and
inspect the test task to obtain meaningful snapshots.

## 5. Verify the tree, not just the result

### Same key under concurrent contention

Pre-create a valid completed prefix, then release eight callers for the same
child call while its execution is held behind a gate.

Check the dump while pending:

- Exactly one new child beneath the expected prefix, not eight nodes.
- The owner's and all seven waiters' returned IDs match that child's ID.
- Its state is pending and no successful result is visible.

After publication:

- The same node is ready, with the same ID, parent, and edge key.
- Its result matches the fixture; the tool-entry counter remains 1.
- A warm hit returns the same ID and adds no tree node or execution.

### Same call under two different state histories

```text
task root
├── setup state A
│   └── read fixture → A
└── setup state B
    └── read fixture → B
```

Check that both branches exist, each read belongs to the correct parent, and
their read node IDs differ even though the call text matches. Duplicate callers
within each branch must share that branch's read node. A common earlier prefix,
if present, should remain a single shared ancestor.

Compare identity equality, edges, states, and node-count changes—not particular
allocated integers or `HashMap` iteration order. Node counts should use a
before/after difference in the isolated task rather than assume how the root
is counted internally.

For the initial whole-turn design, assert one node for the ordered group under
the correct parent. Do not require child nodes for every member call unless
that representation has been chosen. Call IDs are checked in the assembled
response, not by requiring different logical nodes for each caller.

## Proposed handoff

Arjun: expose node ID/status on claim and completed lookup, preserve those IDs
in the existing dump, and document the ownership/failure policy.

Chia-Heng: build the expected fixture tree on the test side, compare it with
the dump at pending/ready checkpoints, and check execution counts and returned
IDs. Do not infer correctness solely from server-reported IDs: compare the
actual path/edge structure, tool outputs, and state effects as well.

Still to agree: exact wire messages, root/prefix status encoding, polling versus
notification, ownership tokens, and whether takeover is part of the first patch.
The field names above can map to Arjun's implementation; no additional API
framework or storage redesign is being requested.
