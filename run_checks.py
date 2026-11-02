#!/usr/bin/env python3
"""TVCache testbench: self-tests, offline node probes, and opt-in live checks."""

import argparse
import asyncio
from datetime import datetime, timezone
import importlib
import json
from pathlib import Path
import sys
import unittest
import uuid

from tvcache_checks.baseline import run_baseline
from tvcache_checks.integration import run_integration
from tvcache_checks.probes import IncompleteProbeLog, validate_probe_log
from tvcache_checks.scenarios import run_pending, snapshot_dict
from tvcache_checks.tree import UnsupportedMetadata, inspect_task
from tvcache_checks.team_adapter import make_client_factory, normalize_team_dump


HERE = Path(__file__).resolve().parent


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="mode", required=True)
    own = commands.add_parser("selftest", help="Test the checker/fixtures, not a real cache")
    own.add_argument("--output", type=Path)
    tree = commands.add_parser("tree", help="Probe every node in a saved DumpAll JSON file")
    tree.add_argument("dump", type=Path)
    tree.add_argument("--task-id", required=True)
    tree.add_argument("--legacy", action="store_true", help="Allow missing IDs/status; do not infer lifecycle")
    tree.add_argument("--team-format", action="store_true", help="Normalize the team's documented raw root/prefix states")
    tree.add_argument("--output", type=Path)
    probes = commands.add_parser("probes", help="Validate recorded probe JSONL offline; no server connection")
    probes.add_argument("log", type=Path)
    probes.add_argument("--require-tree", action="store_true", help="Require a correctness-run tree snapshot")
    probes.add_argument("--output", type=Path)
    baseline = commands.add_parser("baseline", help="Write tiny isolated fixtures to a real sequential cache")
    baseline.add_argument("--dedicated-server", action="store_true", required=True,
                          help="Confirm this is an approved dedicated test instance")
    baseline.add_argument("--host", default="127.0.0.1")
    baseline.add_argument("--port", type=int, default=7878)
    baseline.add_argument("--client-source", type=Path, help="Checkout containing tvclient/client.py (not bundled)")
    baseline.add_argument("--namespace", default="chia-heng-checks")
    baseline.add_argument("--timeout", type=float, default=5.0)
    baseline.add_argument("--team-format", action="store_true", help="Use explicit team pending-nodes dump normalization")
    baseline.add_argument("--output", type=Path, required=True)
    pending = commands.add_parser("pending", help="T01/T02 using an adapter to the real pending-node cache")
    pending.add_argument("--dedicated-server", action="store_true", required=True)
    adapter = pending.add_mutually_exclusive_group()
    adapter.add_argument("--adapter", help="module:factory returning a fresh async client context per call")
    adapter.add_argument("--client-source", type=Path, help="Use the built-in adapter for the team's pending-nodes checkout")
    pending.add_argument("--host", default="127.0.0.1")
    pending.add_argument("--port", type=int, default=7879)
    pending.add_argument("--case", choices=["T01", "T02", "all"], default="all")
    pending.add_argument("--namespace", default="chia-heng-checks")
    pending.add_argument("--callers", type=int, default=8, help="T01 caller count; T02 uses two per history")
    pending.add_argument("--timeout", type=float, default=10.0, help="Overall deadline per case, including setup")
    pending.add_argument("--output", type=Path, required=True)
    integration = commands.add_parser("integration", help="Launch and stop a fresh real CPU server for team integration")
    integration.add_argument("--launch-local-server", action="store_true", required=True,
                             help="Explicitly allow a short-lived dedicated server; the implementation binds all interfaces")
    integration.add_argument("--server-source", type=Path, required=True)
    integration.add_argument("--client-source", type=Path, required=True)
    integration.add_argument("--server-binary", type=Path, help="Defaults to <server-source>/target/debug/tvcache; build first")
    integration.add_argument("--port", type=int, default=0, help="0 chooses a free port; occupied ports are refused")
    integration.add_argument("--callers", type=int, default=8)
    integration.add_argument("--timeout", type=float, default=15)
    integration.add_argument("--upstream-tests", action="store_true", help="Also run 3 implementation-supplied Python suites using fixture sandboxes")
    integration.add_argument("--summary", action="store_true", help="Print compact outcomes; retain full evidence in --output")
    integration.add_argument("--output", type=Path, required=True)
    return root


def adapter_factory(spec: str):
    module_name, separator, name = spec.partition(":")
    if not separator or not module_name or not name or ":" in name:
        raise ValueError("Adapter must be module:factory")
    factory = getattr(importlib.import_module(module_name), name)
    if not callable(factory):
        raise TypeError("Adapter factory is not callable")
    return factory


async def pending_cases(args, report: dict) -> None:
    if not args.adapter and args.client_source is None:
        report.update({"status": "blocked", "reason": (
            "No real pending-node adapter selected. Supply --client-source for the "
            "team's pending-nodes checkout, or --adapter for another implementation. "
            "See README.md; no live test was attempted."
        )})
        return
    if args.callers < 2 or args.timeout <= 0:
        raise ValueError("Need callers >= 2 and positive timeout")
    factory = (make_client_factory(args.client_source, args.host, args.port, min(args.timeout, 5))
               if args.client_source is not None else adapter_factory(args.adapter))
    cases = ["T01", "T02"] if args.case == "all" else [args.case]
    report["adapter"] = args.adapter or "built-in team pending-nodes adapter"
    report["cases"] = []
    task_ids = set()
    for case in cases:
        task_id = f"{args.namespace}:{case}:{uuid.uuid4().hex}"
        task_ids.add(task_id)
        case_report = {}
        report["cases"].append(case_report)
        try:
            await run_pending(factory, case, task_id, callers=args.callers,
                              timeout=args.timeout, report=case_report)
        except UnsupportedMetadata as error:
            case_report.update(status="blocked", error=str(error))
        except Exception as error:
            case_report.update(status="failed", error_type=type(error).__name__, error=str(error))
    if args.client_source is not None:
        report["raw_snapshots"] = [{task: graph for task, graph in dump.items() if task in task_ids}
                                   for dump in factory.raw_dumps]
    states = {case["status"] for case in report["cases"]}
    report["status"] = "failed" if "failed" in states else "blocked" if "blocked" in states else "passed"


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.output is not None and args.output.exists():
        print(f"Refusing to overwrite existing report: {args.output}", file=sys.stderr)
        return 2
    report = {"mode": args.mode, "created_at_utc": datetime.now(timezone.utc).isoformat()}
    try:
        if args.mode == "selftest":
            suite = unittest.defaultTestLoader.discover(str(HERE / "unit"))
            result = unittest.TextTestRunner(verbosity=2, stream=sys.stderr).run(suite)
            report.update({"status": "passed" if result.wasSuccessful() and result.testsRun > 0 else "failed",
                           "tests_run": result.testsRun, "skipped": len(result.skipped),
                           "limitations": ["Only the testbench itself was tested; no real cache, sandbox, or model was exercised."]})
        elif args.mode == "tree":
            raw = json.loads(args.dump.read_text())
            normalized = normalize_team_dump(raw) if args.team_format else raw
            snapshot = inspect_task(normalized, args.task_id,
                                    require_metadata=not args.legacy)
            report.update({"status": "passed", "snapshot": snapshot_dict(snapshot),
                           "limitations": ["Offline structure/node-state inspection only, not concurrency or execution coverage."]})
        elif args.mode == "baseline":
            source = args.client_source
            if source is None:
                candidate = HERE.parent / "references/tvclient"
                if not (candidate / "tvclient/client.py").is_file():
                    report.update(status="blocked", reason=(
                        "Course client is not bundled. Supply --client-source /path/to/tvclient; "
                        "no live test was attempted."
                    ))
                else:
                    source = candidate
            if source is not None:
                report.update(asyncio.run(run_baseline(args.host, args.port, source,
                                                       args.namespace, timeout=args.timeout, retries=0,
                                                       dump_transform=normalize_team_dump if args.team_format else None)))
        elif args.mode == "probes":
            report.update(validate_probe_log(args.log, require_tree=args.require_tree))
            report["status"] = "passed"
        elif args.mode == "integration":
            report.update(asyncio.run(run_integration(args.server_source, args.client_source, args.output,
                server_binary=args.server_binary, port=args.port, callers=args.callers,
                timeout=args.timeout, upstream_tests=args.upstream_tests)))
        else:
            asyncio.run(pending_cases(args, report))
    except (UnsupportedMetadata, IncompleteProbeLog) as error:
        report.update(status="blocked", error=str(error))
    except Exception as error:
        report.update(status="failed", error_type=type(error).__name__, error=str(error))
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x") as handle:
            json.dump(report, handle, indent=2)
            handle.write("\n")
    printed = report
    if args.mode == "integration" and args.summary:
        printed = {"status": report["status"], "report": str(args.output),
                   "sources": report.get("sources"), "baseline": report.get("baseline", {}).get("status"),
                   "cases": [{"case": case["case"], "status": case["status"]} for case in report.get("cases", [])],
                   "lifecycle": [{"case": case["case"], "status": case["status"]} for case in report.get("lifecycle", {}).get("cases", [])],
                   "upstream_tests": [{"script": test["script"], "status": test["status"]} for test in report.get("upstream_tests", [])],
                   "server_stopped": report.get("server", {}).get("stopped"),
                   "error": report.get("error")}
    print(json.dumps(printed, indent=2))
    return {"passed": 0, "failed": 1, "blocked": 2}[report["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
