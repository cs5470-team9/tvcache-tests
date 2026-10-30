"""Small, opt-in smoke check against the actual sequential TVCache server.

This is not a mock server and does not test pending-node coalescing. Every run
creates one fresh task namespace, inserts tiny string fixtures without sandbox
handles, and leaves that task in place for inspection. It never evicts data or
drains a shared server's sandbox-reclamation queue.
"""

from __future__ import annotations

import importlib
import re
import sys
import uuid
from pathlib import Path
from typing import Any


def make_task_id(namespace: str) -> str:
    """Allocate a recognizable task name without colliding with a prior run."""
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,80}", namespace):
        raise ValueError("namespace must contain 1–80 ASCII letters, digits, _, ., :, or -")
    return f"{namespace}:baseline:{uuid.uuid4().hex}"


def load_real_client(client_source: Path) -> type:
    """Import the requested checkout, refusing an already-loaded different copy.

    The source argument is the repository directory containing ``tvclient/``.
    No pip installation, container-manager import, or server process is needed.
    Imported modules remain loaded; a second call cannot silently switch versions.
    """
    source = client_source.expanduser().resolve()
    expected = source / "tvclient" / "client.py"
    if not expected.is_file():
        raise FileNotFoundError(f"client source must contain tvclient/client.py: {source}")
    package_dir = (source / "tvclient").resolve()
    for name, module in list(sys.modules.items()):
        if name == "tvclient" or name.startswith("tvclient."):
            origin = getattr(module, "__file__", None)
            if origin is None or not Path(origin).resolve().is_relative_to(package_dir):
                raise RuntimeError(f"{name} is already loaded from another source: {origin}")
    sys.path.insert(0, str(source))
    try:
        module = importlib.import_module("tvclient.client")
    finally:
        # Remove only the path inserted here, without changing earlier entries.
        if sys.path and sys.path[0] == str(source):
            sys.path.pop(0)
        else:
            sys.path.remove(str(source))
    if Path(module.__file__).resolve() != expected.resolve():
        raise RuntimeError(f"wrong tvclient checkout imported: {module.__file__}")
    return module.TVCacheClient


def snapshot_report(snapshot: Any, stage: str) -> dict:
    """Report only the fixture task, never unrelated tasks from DumpAll."""
    return {
        "stage": stage,
        "task_id": snapshot.task_id,
        "legacy": snapshot.legacy,
        "node_count_including_root": len(snapshot.nodes),
        "nodes": [
            {
                "path": list(path),
                "parent_path": None if node.parent_path is None else list(node.parent_path),
                "node_id": node.node_id,
                "state": node.state,
                "generation": node.generation,
                "value": node.value,
                "sandbox_id": node.sandbox_id,
            }
            for path, node in sorted(snapshot.nodes.items())
        ],
    }


async def run_baseline(
    host: str,
    port: int,
    client_source: Path,
    namespace: str,
    *,
    timeout: float = 5.0,
    retries: int = 0,
) -> dict:
    """Exercise real Insert/Find/DumpAll operations; errors fail the run.

    Callers must explicitly opt in to the server writes. Connection failure is
    raised, not converted into a passing or skipped test. ``timeout`` applies
    to each client operation; no duplicate write is retried after publication.
    """
    from .tree import inspect_task

    if not 1 <= port <= 65535:
        raise ValueError("port must be between 1 and 65535")
    if timeout <= 0 or retries < 0:
        raise ValueError("timeout must be positive and retries nonnegative")
    task_id = make_task_id(namespace)
    client_class = load_real_client(client_source)
    snapshots: list[dict] = []
    checks: list[dict] = []
    expected_values: dict[tuple[str, ...], str] = {}
    fixture = [
        (("state-A",), "setup A"),
        (("state-A", "read"), "result from A"),
        (("state-A", "empty"), ""),
        (("state-B",), "setup B"),
        (("state-B", "read"), "result from B"),
    ]

    async with client_class(host=host, port=port, timeout=timeout, retries=retries) as client:
        initial_dump = await client.dump_all()
        if task_id in initial_dump:
            raise AssertionError("fresh task namespace already exists; refusing to overwrite it")
        checks.append({"name": "fresh task namespace", "passed": True})

        async def capture(stage: str) -> None:
            snapshot = inspect_task(await client.dump_all(), task_id, require_metadata=False)
            snapshot.assert_shape({(), *expected_values})
            for path, expected in expected_values.items():
                actual = snapshot.nodes[path]
                if actual.value != expected:
                    raise AssertionError(f"incorrect dumped value at fixture path {path!r}")
                if actual.sandbox_id is not None:
                    raise AssertionError(f"unexpected sandbox handle at fixture path {path!r}")
            snapshots.append(snapshot_report(snapshot, stage))

        for path, value in fixture:
            inserted = await client.insert(task_id, list(path), value, None, 1)
            if not inserted.value_inserted or inserted.sandbox_inserted:
                raise AssertionError(f"fixture insertion rejected or sandbox unexpectedly inserted: {path!r}")
            expected_values[path] = value
            await capture("insert " + "/".join(path))
        checks.append({"name": "prefix-first insertion and exact tree edges", "passed": True})

        async def check_lookup(path: tuple[str, ...], expected: str | None) -> None:
            # A future implementation may return a leased sandbox. Always use
            # the real client's context manager so the lease is released.
            async with await client.find(task_id, list(path)) as found:
                if found.value != expected:
                    raise AssertionError(f"incorrect lookup value at fixture path {path!r}")

        for path, value in fixture:
            await check_lookup(path, value)
        checks.append({"name": "all exact values including valid empty string", "passed": True})
        checks.append({"name": "same call under distinct history parents remains distinct", "passed": True})

        await check_lookup(("state-A", "missing"), None)
        await check_lookup(("missing-root-edge",), None)
        checks.append({"name": "missing paths return no value", "passed": True})
        await capture("after hit and miss lookups")
        checks.append({"name": "lookups preserve fixture topology and values", "passed": True})

    return {
        "mode": "real-server-sequential-baseline",
        "status": "passed",
        "host": host,
        "port": port,
        "client_source": str(client_source.expanduser().resolve()),
        "task_id": task_id,
        "checks": checks,
        "snapshots": snapshots,
        "node_metadata_available": not snapshots[-1]["legacy"],
        "limitations": [
            "Legacy servers expose no explicit lifecycle state or node ID; missing fields are not inferred.",
            "This smoke check does not claim pending-node, concurrency, failure, or restoration coverage.",
            "Fixture nodes contain no sandbox handles; the isolated task is left on the server for inspection.",
        ],
    }
