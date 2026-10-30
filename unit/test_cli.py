"""Verify offline/blocked CLI paths without opening sockets or starting services."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


TESTING = Path(__file__).resolve().parents[1]


class CommandLineTests(unittest.TestCase):
    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, str(TESTING / "run_checks.py"), *map(str, args)],
            capture_output=True, text=True, timeout=5,
            env=os.environ | {"PYTHONDONTWRITEBYTECODE": "1"},
        )

    def test_offline_probe_reports_every_fixture_node(self):
        result = self.run_cli("tree", TESTING / "fixtures/example_pending_tree.json", "--task-id", "example-task")
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        nodes = report["snapshot"]["nodes"]
        self.assertEqual(len(nodes), 3)
        self.assertEqual([node["state"] for node in nodes], ["root", "ready", "pending"])
        self.assertEqual(nodes[-1]["parent_path"], ["setup-A"])

    def test_no_pending_adapter_is_blocked_not_passed(self):
        with tempfile.TemporaryDirectory(prefix="tvcache-cli-") as directory:
            target = Path(directory) / "report.json"
            result = self.run_cli("pending", "--dedicated-server", "--output", target)
            self.assertEqual(result.returncode, 2, result.stderr)
            report = json.loads(target.read_text())
            self.assertEqual(report["status"], "blocked")
            self.assertIn("no live test was attempted", report["reason"])

    def test_existing_report_is_not_overwritten(self):
        with tempfile.TemporaryDirectory(prefix="tvcache-cli-") as directory:
            target = Path(directory) / "report.json"
            target.write_text("preserve this report")
            result = self.run_cli("pending", "--dedicated-server", "--output", target)
            self.assertEqual(result.returncode, 2)
            self.assertIn("Refusing to overwrite", result.stderr)
            self.assertEqual(target.read_text(), "preserve this report")

    def test_missing_metadata_has_explicit_blocked_result(self):
        with tempfile.TemporaryDirectory(prefix="tvcache-cli-") as directory:
            source = Path(directory) / "legacy.json"
            source.write_text(json.dumps({"t": {"root": {"children": {}, "value": None}}}))
            result = self.run_cli("tree", source, "--task-id", "t")
            self.assertEqual(result.returncode, 2)
            self.assertEqual(json.loads(result.stdout)["status"], "blocked")
            legacy = self.run_cli("tree", source, "--task-id", "t", "--legacy")
            self.assertEqual(legacy.returncode, 0)
            self.assertTrue(json.loads(legacy.stdout)["snapshot"]["legacy"])

    def test_invalid_pending_state_fails_not_blocked(self):
        with tempfile.TemporaryDirectory(prefix="tvcache-cli-") as directory:
            fixture = json.loads((TESTING / "fixtures/example_pending_tree.json").read_text())
            fixture["example-task"]["root"]["children"]["setup-A"]["children"]["read-fixture"]["value"] = "premature"
            source = Path(directory) / "bad.json"
            source.write_text(json.dumps(fixture))
            result = self.run_cli("tree", source, "--task-id", "example-task")
            self.assertEqual(result.returncode, 1)
            self.assertEqual(json.loads(result.stdout)["status"], "failed")

    def test_private_token_in_dump_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="tvcache-cli-") as directory:
            fixture = json.loads((TESTING / "fixtures/example_pending_tree.json").read_text())
            fixture["example-task"]["root"]["children"]["setup-A"]["owner_token"] = "private"
            source = Path(directory) / "bad.json"
            source.write_text(json.dumps(fixture))
            result = self.run_cli("tree", source, "--task-id", "example-task")
            self.assertEqual(result.returncode, 1)
            self.assertIn("private owner token", json.loads(result.stdout)["error"])

    def test_complete_synthetic_probe_log_is_checked_offline(self):
        result = self.run_cli("probes", TESTING / "fixtures/example_probe_log.jsonl", "--require-tree")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["executions"], 1)
        self.assertEqual(report["snapshot_count"], 4)
        self.assertEqual(report["claims"]["waiters"], 1)

    def test_truncated_probe_capture_is_blocked(self):
        with tempfile.TemporaryDirectory(prefix="tvcache-cli-") as directory:
            target = Path(directory) / "partial.jsonl"
            records = (TESTING / "fixtures/example_probe_log.jsonl").read_text().splitlines()
            target.write_text("\n".join(records[:-1]) + "\n")
            result = self.run_cli("probes", target)
            self.assertEqual(result.returncode, 2, result.stderr + result.stdout)
            self.assertEqual(json.loads(result.stdout)["status"], "blocked")

    def test_duplicate_execution_in_probe_capture_fails(self):
        with tempfile.TemporaryDirectory(prefix="tvcache-cli-") as directory:
            target = Path(directory) / "duplicate.jsonl"
            records = [json.loads(line) for line in
                       (TESTING / "fixtures/example_probe_log.jsonl").read_text().splitlines()]
            records.insert(4, dict(records[3]))
            for seq, record in enumerate(records):
                record["seq"] = seq
            target.write_text("\n".join(json.dumps(record) for record in records) + "\n")
            result = self.run_cli("probes", target)
            self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
            self.assertIn("Multiple executions", json.loads(result.stdout)["error"])
