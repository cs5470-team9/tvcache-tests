"""Own a fresh CPU test server and exercise the real paired team checkouts.

No server is downloaded, built, attached to, or left running implicitly. The
caller supplies a built binary and explicitly opts in through the CLI. These
are correctness fixtures, not GPU performance measurements.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import uuid

from .baseline import load_real_client, run_baseline
from .lifecycle_cases import run_lifecycle_checks
from .scenarios import run_pending
from .team_adapter import make_client_factory, normalize_team_dump
from .tree import UnsupportedMetadata


def source_revision(source: Path) -> dict:
    source = source.expanduser().resolve()
    def git(*args: str) -> str:
        return subprocess.check_output(
            ["git", "-C", str(source), *args], text=True, stderr=subprocess.PIPE,
            timeout=10,
        ).strip()
    if Path(git("rev-parse", "--show-toplevel")).resolve() != source:
        raise ValueError("Source must be the root of its Git checkout")
    dirty = git("status", "--porcelain", "--untracked-files=normal")
    if dirty:
        raise ValueError(f"Refusing an unrecorded modified source checkout: {source}")
    return {"revision": git("rev-parse", "HEAD"), "clean": True}


def available_port(requested: int) -> int:
    if type(requested) is not int or not 0 <= requested <= 65535:
        raise ValueError("Port must be 0 (choose a free port) or 1–65535")
    # Never attach to a preexisting cache. The child must successfully bind its
    # own port; startup also checks that our child has not exited.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
        reservation.bind(("127.0.0.1", requested))
        return reservation.getsockname()[1]


def binary_digest(binary: Path) -> str:
    digest = hashlib.sha256()
    with binary.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


async def _ready(process: subprocess.Popen, port: int, log_path: Path) -> None:
    deadline = asyncio.get_running_loop().time() + 10
    while asyncio.get_running_loop().time() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Owned test server exited before becoming ready; see server.log")
        # The port reservation is released before exec. Do not mistake a
        # competing listener for our child in that handoff race: require the
        # pinned implementation's own post-bind log before any TCP probe/write.
        marker = f"listening on 0.0.0.0:{port} with tokio"
        if marker not in log_path.read_text(encoding="utf-8"):
            await asyncio.sleep(.05)
            continue
        try:
            _, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), .2)
        except (OSError, asyncio.TimeoutError):
            await asyncio.sleep(.05)
            continue
        writer.close()
        await writer.wait_closed()
        if process.poll() is not None:
            raise RuntimeError("Owned test server exited during startup")
        return
    raise TimeoutError("Owned test server did not become ready within 10 seconds")


def upstream_environment(client_source: Path, port: int) -> dict[str, str]:
    env = dict(os.environ, PYTHONPATH=str(client_source), TVCACHE_PORT=str(port),
               PYTHONDONTWRITEBYTECODE="1")
    # The implementation suites use assert. Never silently disable their
    # assertions just because the caller's shell requested optimized Python.
    env.pop("PYTHONOPTIMIZE", None)
    return env


async def _run_upstream(server_source: Path, client_source: Path, port: int, artifacts: Path) -> list[dict]:
    """Selected existing implementation tests; not independent testbench tests."""
    reports = []
    for name in ("test_claim_client.py", "test_coalescing.py", "test_coalescing_random.py"):
        script = server_source / "tests" / name
        log = artifacts / f"upstream-{name}.log"
        env = upstream_environment(client_source, port)
        with log.open("x", encoding="utf-8") as stream:
            process = await asyncio.create_subprocess_exec(
                sys.executable, str(script), cwd=str(script.parent), env=env,
                stdout=stream, stderr=subprocess.STDOUT,
            )
            timed_out = False
            try:
                await asyncio.wait_for(process.wait(), 90)
            except asyncio.TimeoutError:
                timed_out = True
            finally:
                if process.returncode is None:
                    process.terminate()
                    try:
                        await asyncio.wait_for(process.wait(), 5)
                    except asyncio.TimeoutError:
                        process.kill()
                        await process.wait()
        reports.append({"script": name, "status": "passed" if process.returncode == 0 and not timed_out else "failed",
                        "returncode": process.returncode, "timed_out": timed_out, "log": str(log)})
    return reports


async def run_integration(
    server_source: Path, client_source: Path, output: Path, *,
    server_binary: Path | None = None, port: int = 0, callers: int = 8,
    timeout: float = 15, upstream_tests: bool = False,
) -> dict:
    if callers < 2 or timeout <= 0:
        raise ValueError("Need callers >= 2 and a positive scenario timeout")
    server_source, client_source = server_source.expanduser().resolve(), client_source.expanduser().resolve()
    revisions = {"server": source_revision(server_source), "client": source_revision(client_source)}
    # Capability-check before launching any service. Baseline main clients block.
    make_client_factory(client_source)
    client_class = load_real_client(client_source)
    binary = (server_binary or server_source / "target/debug/tvcache").expanduser().resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise FileNotFoundError("Build the selected real server first: cargo build --locked")
    selected_port = available_port(port)
    output.parent.mkdir(parents=True, exist_ok=True)
    artifacts = Path(tempfile.mkdtemp(prefix=output.stem + "-artifacts-", dir=output.parent)).resolve()
    command = [str(binary), f"--port={selected_port}", "--eviction-policy=none"]
    report = {
        "status": "running", "mode": "real-team-integration", "sources": revisions,
        "binary_sha256": binary_digest(binary),
        "artifacts": str(artifacts), "server": {"command": command, "host_used_by_tests": "127.0.0.1",
            "bind_address": "0.0.0.0", "port": selected_port, "stopped": False},
        "cases": [], "limitations": [
            "Dedicated CPU correctness capture, not a GPU timing or DeepSWE evaluation.",
            "T01/T02 execute a gated deterministic fixture, not a real container tool.",
            "No harness call-ID mapping, real sandbox restoration, whole-turn layout, or crash-safe exactly-once effects proven.",
            "Raw dumps are checkpoints, not the proposed authoritative probe JSONL stream.",
            "Binary hash and clean source revisions are recorded separately; build the binary from those sources before running.",
            "The implementation binds all interfaces. This fresh short-lived server contains synthetic fixtures only.",
        ],
    }
    process = None
    log_path = artifacts / "server.log"
    with log_path.open("x", encoding="utf-8") as log:
        try:
            process = subprocess.Popen(command, cwd=server_source, stdout=log, stderr=subprocess.STDOUT,
                                       env=dict(os.environ, RUST_LOG="info"), start_new_session=True)
            report["server"]["pid"] = process.pid
            await _ready(process, selected_port, log_path)
            namespace = "team-integration:" + uuid.uuid4().hex
            try:
                baseline = await run_baseline("127.0.0.1", selected_port, client_source, namespace,
                                               timeout=timeout, dump_transform=normalize_team_dump)
                report["baseline"] = baseline
            except Exception as error:
                report["baseline"] = {"status": "failed", "error_type": type(error).__name__, "error": str(error)}
            factory = make_client_factory(client_source, port=selected_port, timeout=min(timeout, 5))
            task_ids = set()
            for case in ("T01", "T02"):
                case_report = {}
                task_id = f"{namespace}:{case}"
                task_ids.add(task_id)
                report["cases"].append(case_report)
                try:
                    await run_pending(factory, case, task_id, callers=callers, timeout=timeout, report=case_report)
                except UnsupportedMetadata as error:
                    case_report.update(status="blocked", error=str(error))
                except Exception as error:
                    case_report.update(status="failed", error_type=type(error).__name__, error=str(error))
            report["raw_pending_snapshots"] = [
                {task: graph for task, graph in dump.items() if task in task_ids}
                for dump in factory.raw_dumps
            ]
            report["lifecycle"] = await run_lifecycle_checks(client_class, "127.0.0.1", selected_port,
                                                             namespace, timeout=timeout)
            if upstream_tests:
                report["upstream_tests"] = await _run_upstream(server_source, client_source, selected_port, artifacts)
            states = {report["baseline"]["status"], report["lifecycle"]["status"]}
            states.update(case["status"] for case in report["cases"])
            states.update(test["status"] for test in report.get("upstream_tests", []))
            report["status"] = "failed" if "failed" in states else "blocked" if "blocked" in states else "passed"
        except Exception as error:
            report.update(status="failed", error_type=type(error).__name__, error=str(error))
        finally:
            if process is not None:
                if process.poll() is None:
                    process.terminate()
                try:
                    await asyncio.to_thread(process.wait, timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    await asyncio.to_thread(process.wait)
                report["server"].update(stopped=True, returncode=process.returncode)
    return report
