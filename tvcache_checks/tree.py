"""Read-only checks for the existing DumpAll tree and proposed node metadata.

The course baseline does not expose lifecycle metadata. A legacy inspection is
useful for shape checks, but must never be mistaken for a successful lifecycle
test. Missing proposed metadata therefore has its own exception type.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any


Path = tuple[str, ...]
_UNSET = object()
_STATES = {"root", "prefix", "pending", "ready", "failed"}
_EXECUTION_STATES = {"pending", "ready", "failed"}


class UnsupportedMetadata(AssertionError):
    """The server's dump lacks metadata required by the proposed contract."""


@dataclass(frozen=True)
class NodeView:
    path: Path
    parent_path: Path | None
    node_id: int | None
    state: str
    generation: int | None
    value: str | None
    sandbox_id: str | None


@dataclass(frozen=True)
class TreeSnapshot:
    task_id: str
    nodes: Mapping[Path, NodeView]
    legacy: bool = False

    def assert_shape(self, expected_paths: Iterable[Path]) -> None:
        """Compare exact edge paths, without relying on dictionary order."""
        expected = set(expected_paths)
        actual = set(self.nodes)
        if actual != expected:
            raise AssertionError(
                f"Tree shape differs: missing={sorted(expected - actual)!r}, "
                f"unexpected={sorted(actual - expected)!r}"
            )

    def assert_node(
        self,
        path: Path,
        *,
        state: str,
        node_id: Any = _UNSET,
        generation: Any = _UNSET,
        value: Any = _UNSET,
        sandbox_id: Any = _UNSET,
    ) -> NodeView:
        """Check a node; explicit None is different from an omitted assertion."""
        if path not in self.nodes:
            raise AssertionError(f"Node path not found: {path!r}")
        node = self.nodes[path]
        for field, expected in (
            ("state", state),
            ("node_id", node_id),
            ("generation", generation),
            ("value", value),
            ("sandbox_id", sandbox_id),
        ):
            if expected is not _UNSET and getattr(node, field) != expected:
                raise AssertionError(
                    f"Node {path!r} {field}: expected {expected!r}, "
                    f"got {getattr(node, field)!r}"
                )
        return node


def _integer(value: Any, label: str, minimum: int) -> int:
    # bool is an int subclass, but is not a valid ID or generation.
    if type(value) is not int or value < minimum:
        raise AssertionError(f"{label} must be an integer >= {minimum}: {value!r}")
    return value


def inspect_task(
    dump: Any, task_id: str, require_metadata: bool = True
) -> TreeSnapshot:
    """Parse one isolated task from the server's nested DumpAll JSON object.

    Unknown fields are deliberately ignored. ``status`` is the proposed wire
    field; ``state`` is also accepted as an adapter-friendly spelling. Values
    and sandbox references retain their existing string-or-null encoding.
    ``number_of_nodes`` is not interpreted: root counting is implementation
    dependent, and topology is checked directly instead.
    """
    if not isinstance(dump, dict):
        raise AssertionError("DumpAll response must be a dictionary")
    if task_id not in dump:
        raise AssertionError(f"Task not present in DumpAll: {task_id!r}")
    task = dump[task_id]
    if not isinstance(task, dict) or not isinstance(task.get("root"), dict):
        raise AssertionError("Task must contain a root node dictionary")

    nodes: dict[Path, NodeView] = {}
    seen_ids: dict[int, Path] = {}
    missing_metadata: list[str] = []
    legacy = False
    stack: list[tuple[Path, dict[str, Any]]] = [((), task["root"])]
    seen_objects: set[int] = set()
    while stack:
        path, raw = stack.pop()
        # JSON cannot contain object cycles; also reject aliased in-memory
        # fixtures, which cannot represent independent serialized tree nodes.
        if id(raw) in seen_objects:
            raise AssertionError(f"Node dictionary reused or cyclic at {path!r}")
        seen_objects.add(id(raw))
        if raw.get("owner_token") is not None:
            raise AssertionError(f"Node {path!r} leaks a private owner token in diagnostics")
        children = raw.get("children")
        if not isinstance(children, dict):
            raise AssertionError(f"Node {path!r} children must be a dictionary")
        value = raw.get("value")
        sandbox_id = raw.get("sandbox_id")
        for label, item in (("value", value), ("sandbox_id", sandbox_id)):
            if item is not None and not isinstance(item, str):
                raise AssertionError(f"Node {path!r} {label} must be string or null")

        node_id = None
        if "node_id" in raw:
            node_id = _integer(raw["node_id"], f"Node {path!r} node_id", 0)
            if node_id in seen_ids:
                raise AssertionError(
                    f"Duplicate node_id {node_id}: {seen_ids[node_id]!r} and {path!r}"
                )
            seen_ids[node_id] = path
        else:
            legacy = True
            missing_metadata.append(f"{path!r}: node_id")

        has_status = "status" in raw or "state" in raw
        if "status" in raw and "state" in raw and raw["status"] != raw["state"]:
            raise AssertionError(f"Node {path!r} has conflicting status/state fields")
        state = raw.get("status", raw.get("state", "legacy"))
        if has_status:
            if not isinstance(state, str) or state not in _STATES:
                raise AssertionError(f"Node {path!r} has invalid lifecycle state: {state!r}")
            if state == "root" and path:
                raise AssertionError(f"Only the task root may have root status: {path!r}")
            if not path and state != "root":
                raise AssertionError("The task root must have root status")
        else:
            legacy = True
            missing_metadata.append(f"{path!r}: status")

        generation = None
        if "generation" in raw and raw["generation"] is not None:
            minimum = 0 if not path and state == "root" else 1
            generation = _integer(raw["generation"], f"Node {path!r} generation", minimum)
        elif state in _EXECUTION_STATES:
            legacy = True
            missing_metadata.append(f"{path!r}: generation")
        if state == "pending" and value is not None:
            raise AssertionError(f"Pending node {path!r} exposes a completed value")
        if state == "ready" and not isinstance(value, str):
            raise AssertionError(f"Ready node {path!r} must contain a string result")

        nodes[path] = NodeView(
            path, path[:-1] if path else None, node_id, state,
            generation, value, sandbox_id,
        )
        for edge, child in children.items():
            if not isinstance(edge, str):
                raise AssertionError(f"Node {path!r} child edge must be a string")
            if not isinstance(child, dict):
                raise AssertionError(f"Child {path + (edge,)!r} must be a dictionary")
            stack.append((path + (edge,), child))

    # Validate structure before reporting unsupported metadata, so malformed
    # legacy output is not misreported as a harmless missing capability.
    if require_metadata and missing_metadata:
        raise UnsupportedMetadata(
            "DumpAll lacks proposed node metadata: " + "; ".join(missing_metadata)
        )
    return TreeSnapshot(task_id, nodes, legacy)


def assert_same_identity(before: TreeSnapshot, after: TreeSnapshot) -> None:
    """Require the same task and exact ID-to-path placement across checkpoints."""
    if before.legacy or after.legacy:
        raise UnsupportedMetadata("Stable node identity requires lifecycle metadata")
    if before.task_id != after.task_id:
        raise AssertionError("Cannot compare identities across different tasks")
    after.assert_shape(before.nodes)
    for path, old in before.nodes.items():
        new = after.nodes[path]
        if new.node_id != old.node_id or new.parent_path != old.parent_path:
            raise AssertionError(f"Node identity or parent changed at {path!r}")


def assert_transition(
    before: TreeSnapshot,
    after: TreeSnapshot,
    changed_path: Path,
    expected_state: str,
) -> None:
    """Check one-node change without accepting added nodes or moved identities.

    This helper intentionally allows the target's result, state reference, and
    generation to change. Tests assert the expected generation explicitly when
    publication or takeover semantics require it.
    """
    assert_same_identity(before, after)
    after.assert_node(changed_path, state=expected_state)
    for path, old in before.nodes.items():
        if path != changed_path and after.nodes[path] != old:
            raise AssertionError(f"Unexpected change outside target node: {path!r}")
