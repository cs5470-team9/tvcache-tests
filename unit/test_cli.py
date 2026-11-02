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

    def test_team_tree_normalization_preserves_real_root_generation(self):
        raw = {"task": {"root": {"node_id": 0, "status": "ready", "generation": 0,
                "value": None, "children": {"call": {"node_id": 4, "status": "ready",
                "generation": 1, "value": "", "children": {}}}}}}
        with tempfile.TemporaryDirectory(prefix="tvcache-cli-") as directory:
            source = Path(directory) / "team.json"
            source.write_text(json.dumps(raw))
            result = self.run_cli("tree", source, "--task-id", "task", "--team-format")
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
            nodes = json.loads(result.stdout)["snapshot"]["nodes"]
            self.assertEqual((nodes[0]["state"], nodes[0]["generation"]), ("root", 0))
            self.assertEqual((nodes[1]["node_id"], nodes[1]["value"]), (4, ""))

    def test_server_launch_requires_explicit_opt_in(self):
        with tempfile.TemporaryDirectory(prefix="tvcache-cli-") as directory:
            target = Path(directory) / "report.json"
            result = self.run_cli("integration", "--server-source", directory,
                                  "--client-source", directory, "--output", target)
            self.assertEqual(result.returncode, 2)
            self.assertIn("--launch-local-server", result.stderr)
            self.assertFalse(target.exists())

    def test_existing_integration_report_blocks_all_work(self):
        with tempfile.TemporaryDirectory(prefix="tvcache-cli-") as directory:
            target = Path(directory) / "report.json"
            target.write_text("existing evidence")
            result = self.run_cli("integration", "--launch-local-server", "--server-source", directory,
                                  "--client-source", directory, "--output", target, "--summary")
            self.assertEqual(result.returncode, 2)
            self.assertIn("Refusing to overwrite", result.stderr)
            self.assertEqual(target.read_text(), "existing evidence")

    def test_only_one_pending_adapter_source_can_be_selected(self):
        with tempfile.TemporaryDirectory(prefix="tvcache-cli-") as directory:
            result = self.run_cli("pending", "--dedicated-server", "--adapter", "other:factory",
                                  "--client-source", directory, "--output", Path(directory) / "out.json")
            self.assertEqual(result.returncode, 2)
            self.assertIn("not allowed with argument", result.stderr)

    def test_client_without_pending_api_blocks_before_connection(self):
        with tempfile.TemporaryDirectory(prefix="tvcache-cli-") as directory:
            source = Path(directory)
            (source / "tvclient").mkdir()
            (source / "tvclient/__init__.py").touch()
            # Only an API-shape fixture, not a fake cache or running client.
            (source / "tvclient/client.py").write_text("class TVCacheClient:\n    pass\n")
            result = self.run_cli("pending", "--dedicated-server", "--client-source", source,
                                  "--output", source / "out.json")
            self.assertEqual(result.returncode, 2, result.stderr + result.stdout)
            self.assertIn("lacks pending-node methods", json.loads(result.stdout)["error"])
