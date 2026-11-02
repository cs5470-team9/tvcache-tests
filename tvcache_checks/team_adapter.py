"""Adapter for the team's actual ``pending-nodes`` client/server protocol.

Ownership lives in the server's TCP connection, not a wire token. The private
local marker below only prevents accidental publication through another adapter;
the real server still enforces connection identity and generation fencing.
No cache, lifecycle, node ID, or execution result is simulated here.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from .baseline import load_real_client
from .contracts import ClientFactory, Outcome, Path as CachePath
from .tree import UnsupportedMetadata


_STATUS = {0: "missing", 1: "owner", 2: "pending", 3: "ready", 4: "failed"}


def normalize_team_dump(dump: Any) -> dict:
    """Copy a raw team dump, naming its structural root/prefix explicitly.

    Team NodeStatus uses ``ready`` even for the empty root and snapshot-only
    prefix nodes. Map only that documented encoding, preserving IDs, generation
    numbers (including root generation zero), values, topology, and counters.
    Missing metadata blocks rather than receiving manufactured replacement IDs.
    """
    if not isinstance(dump, dict):
        raise AssertionError("Team DumpAll response must be a dictionary")
    result = copy.deepcopy(dump)
    for task_id, graph in result.items():
        if not isinstance(task_id, str) or not isinstance(graph, dict) or not isinstance(graph.get("root"), dict):
            raise AssertionError("Team task must contain a root node dictionary")
        stack = [((), graph["root"])]
        seen: set[int] = set()
        while stack:
            path, node = stack.pop()
            if id(node) in seen:
                raise AssertionError("Team tree contains an aliased or cyclic node")
            seen.add(id(node))
            for field in ("node_id", "status", "generation", "value"):
                if field not in node:
                    raise UnsupportedMetadata(f"Team dump lacks required {field} metadata")
            if type(node["node_id"]) is not int or node["node_id"] < 0:
                raise AssertionError("Team node_id must be a nonnegative integer")
            if type(node["generation"]) is not int or node["generation"] < (1 if path else 0):
                raise AssertionError("Team generation is invalid")
            status = node["status"]
            if not isinstance(status, str) or status not in {"ready", "pending", "failed"}:
                raise AssertionError("Unexpected raw team node status")
            if "state" in node and node["state"] != status:
                raise AssertionError("Conflicting team status/state fields")
            if node.get("owner_token") is not None or node.get("owner_connection_id") is not None:
                raise AssertionError("Team dump exposes private ownership metadata")
            if not isinstance(node.get("children"), dict):
                raise AssertionError("Team children must be a dictionary")
            value = node.get("value")
            if value is not None and not isinstance(value, str):
                raise AssertionError("Team value must be string or null")
            if status in {"pending", "failed"} and value is not None:
                raise AssertionError("Uncompleted team node exposes a successful result")
            if not path:
                if node["node_id"] != 0 or node["generation"] != 0 or status != "ready" or value is not None:
                    raise AssertionError("Unexpected team root schema; expected ready/id=0/generation=0/no value")
                node["status"] = "root"
            elif status == "ready" and value is None:
                node["status"] = "prefix"
            if "state" in node:
                node["state"] = node["status"]
            for edge, child in node["children"].items():
                if not isinstance(edge, str) or not isinstance(child, dict):
                    raise AssertionError("Team children must map string edges to node dictionaries")
                stack.append((path + (edge,), child))
    return result


def _path(path: CachePath) -> tuple[list[str], list[str]]:
    if not isinstance(path, tuple) or not path or any(not isinstance(edge, str) for edge in path):
        raise ValueError("Adapter path must be a nonempty tuple of strings")
    return list(path[:-1]), [path[-1]]


class TeamPendingAdapter:
    """One live client/connection per adapter; no observer can publish as owner."""

    def __init__(self, client: Any):
        self._client = client
        self._active = False
        self._owned: dict[tuple[str, CachePath, int, int], tuple[object, object]] = {}
        self.raw_dumps: list[dict] = []

    async def __aenter__(self) -> "TeamPendingAdapter":
        if self._active:
            raise RuntimeError("Adapter context is already active")
        await self._client.__aenter__()
        self._active = True
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        try:
            await self._client.__aexit__(exc_type, exc, traceback)
        finally:
            self._active = False
            self._owned.clear()

    def _require_active(self) -> None:
        if not self._active:
            raise RuntimeError("Adapter must be used within its live async context")

    def _outcome(self, response: Any, task_id: str, path: CachePath, *, allow_owner: bool) -> Outcome:
        outcomes = response.outcomes
        if not isinstance(outcomes, list) or len(outcomes) != 1:
            raise AssertionError("Single-entry request must return exactly one outcome")
        raw = outcomes[0]
        if type(raw.status) is not int or raw.status not in _STATUS:
            raise AssertionError("Unknown team claim status")
        status = _STATUS[raw.status]
        if status == "missing":
            if raw.value is not None:
                raise AssertionError("Missing team outcome has a result")
            return Outcome("missing", None, None).validate()
        token = None
        if status == "owner":
            if not allow_owner:
                raise AssertionError("Read-only inspect unexpectedly granted ownership")
            if type(raw.node_id) is not int or type(raw.generation) is not int:
                raise AssertionError("Owner ID and generation must be integers")
            key = (task_id, path, raw.node_id, raw.generation)
            if key in self._owned:
                raise AssertionError("Connection received a duplicate owner grant")
            writer = getattr(self._client, "writer", None)
            if writer is None:
                raise AssertionError("Owner grant has no live connection")
            token = object()
            self._owned[key] = (token, writer)
        result = Outcome(status, raw.node_id, raw.generation, raw.value, token)
        return result.validate()

    async def lookup_or_claim(self, task_id: str, path: CachePath, attempt_id: str) -> Outcome:
        self._require_active()
        if not isinstance(attempt_id, str) or not attempt_id.strip():
            raise ValueError("attempt_id must be nonempty")
        prefix, entries = _path(path)
        # Release only the returned prefix sandbox lease, not the connection.
        async with await self._client.claim(task_id, prefix, entries) as claimed:
            return self._outcome(claimed, task_id, path, allow_owner=True)

    async def inspect(self, task_id: str, path: CachePath) -> Outcome:
        self._require_active()
        prefix, entries = _path(path)
        response = await self._client.inspect(task_id, prefix, entries, wait_ms=0)
        return self._outcome(response, task_id, path, allow_owner=False)

    async def publish(
        self, task_id: str, path: CachePath, *, node_id: int, generation: int,
        owner_token: object, value: str,
    ) -> None:
        self._require_active()
        _path(path)
        if type(node_id) is not int or type(generation) is not int or not isinstance(value, str):
            raise AssertionError("Publication requires integer IDs/generation and a string fixture result")
        key = (task_id, path, node_id, generation)
        saved = self._owned.get(key)
        if saved is None or saved[0] is not owner_token:
            raise AssertionError("Publication does not match this adapter's owner claim")
        if getattr(self._client, "writer", None) is not saved[1]:
            raise AssertionError("Owner connection changed; original ownership cannot be reused")
        result = await self._client.publish(task_id, list(path), generation, value, None, runtime=1)
        if result.value_inserted is not True:
            raise AssertionError("Real server rejected owner publication")
        if result.sandbox_inserted is not False:
            raise AssertionError("Unexpected sandbox insertion for a value-only fixture")
        del self._owned[key]

    async def dump_all(self) -> dict:
        self._require_active()
        raw = await self._client.dump_all()
        self.raw_dumps.append(copy.deepcopy(raw))
        return normalize_team_dump(raw)


class _TeamClientFactory:
    def __init__(self, client_class: type, host: str, port: int, timeout: float):
        self._client_class, self._host, self._port, self._timeout = client_class, host, port, timeout
        self.adapters: list[TeamPendingAdapter] = []

    def __call__(self) -> TeamPendingAdapter:
        client = self._client_class(host=self._host, port=self._port, timeout=self._timeout, retries=0)
        adapter = TeamPendingAdapter(client)
        self.adapters.append(adapter)
        return adapter

    @property
    def raw_dumps(self) -> list[dict]:
        """Collected raw snapshots; callers must filter reports to fixture tasks."""
        return [dump for adapter in self.adapters for dump in adapter.raw_dumps]


def make_client_factory(
    client_source: Path, host: str = "127.0.0.1", port: int = 7879,
    timeout: float = 5,
) -> ClientFactory:
    """Load the requested real checkout; block unsupported baseline clients.

    No connection opens until an adapter's async context is entered. ``adapters``
    and ``raw_dumps`` on the returned factory provide optional capture access.
    Captures may contain other tasks: filter them before writing test reports.
    """
    if not isinstance(host, str) or not host.strip() or type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("Need a nonempty host and a valid integer TCP port")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
        raise ValueError("timeout must be positive")
    client_class = load_real_client(client_source)
    required = ("claim", "inspect", "publish", "dump_all", "__aenter__", "__aexit__")
    missing = [name for name in required if not callable(getattr(client_class, name, None))]
    if missing:
        raise UnsupportedMetadata("Client lacks pending-node methods: " + ", ".join(missing))
    return _TeamClientFactory(client_class, host, port, timeout)
