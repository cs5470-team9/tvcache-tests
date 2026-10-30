"""Explicit fixture logs exercise the validator, never a real or fake cache."""

import copy
import json
import tempfile
import unittest
from pathlib import Path

from tvcache_checks.probes import IncompleteProbeLog, validate_probe_log
from tvcache_checks.tree import UnsupportedMetadata


def fixture_log():
    common = dict(run_id="fixture-run", task_id="task", path=["setup", "read"], node_id=7, generation=1, attempt_id="first")
    records = [
        dict(event="run_start", run_id="fixture-run", schema_version=1, capture_mode="correctness", events_complete=True),
        dict(common, event="claim", outcome="owner"),
        dict(common, event="execute_start"),
        dict(common, event="claim", outcome="pending", attempt_id="second"),
        dict(common, event="status", outcome="pending", attempt_id="probe"),
        dict(common, event="execute_end"),
        dict(common, event="publish", status="ready", value_present=True),
        dict(common, event="status", outcome="ready", attempt_id="probe"),
        dict(common, event="claim", outcome="ready", attempt_id="third"),
        dict(event="run_end", run_id="fixture-run", completed=True, dropped_events=0),
    ]
    return order(records)


def order(records):
    for number, record in enumerate(records):
        record.update(seq=number, ts_ns=number * 100)
    return records


def tree_record(stage="ready", status="ready", generation=1, node_id=7):
    leaf = dict(node_id=node_id, status=status, generation=generation, value="" if status == "ready" else None, children={})
    root = dict(node_id=0, status="root", children={"setup": dict(node_id=1, status="prefix", children={"read": leaf})})
    return dict(event="tree_snapshot", run_id="fixture-run", task_id="task", stage=stage, dump={"task": {"root": root}})


class ProbeTests(unittest.TestCase):
    def validate(self, records=None, **options):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "capture.jsonl"
            with path.open("w", encoding="utf-8") as stream:
                for record in fixture_log() if records is None else records:
                    stream.write(json.dumps(record) + "\n")
            return validate_probe_log(path, **options)

    def test_complete_coalescing_fixture(self):
        result = self.validate()
        self.assertEqual(result["claims"], dict(owners=1, waiters=1, hits=1, failed=0))
        self.assertEqual(result["executions"], 1)
        self.assertEqual(result["published_nodes"], 1)

    def test_performance_metadata_is_valid_but_not_a_performance_claim(self):
        records = fixture_log()
        records[0]["capture_mode"] = "performance"
        result = self.validate(records)
        self.assertEqual(result["snapshot_count"], 0)
        self.assertTrue(any("performance" in text for text in result["limitations"]))

    def test_tree_snapshots_match_pending_ready_warm(self):
        records = fixture_log()
        records.insert(4, tree_record("pending", "pending"))
        records.insert(-1, tree_record())
        records.insert(-1, tree_record("warm"))
        self.assertEqual(self.validate(order(records), require_tree=True)["snapshot_count"], 3)

    def test_tree_before_can_have_only_root(self):
        record = tree_record("before")
        record["dump"]["task"]["root"]["children"] = {}
        records = fixture_log()
        records.insert(1, record)
        self.validate(order(records))

    def test_tree_cannot_reassign_id_or_contradict_state(self):
        for tree in [tree_record(node_id=8), tree_record(status="pending"), tree_record(generation=2)]:
            with self.subTest(tree=tree):
                records = fixture_log()
                records.insert(-1, tree)
                with self.assertRaises(AssertionError):
                    self.validate(order(records))

    def test_tree_cannot_drop_a_recorded_node(self):
        records = fixture_log()
        tree = tree_record()
        tree["dump"]["task"]["root"]["children"] = {}
        records.insert(-1, tree)
        with self.assertRaisesRegex(AssertionError, "Node path not found"):
            self.validate(order(records))

    def test_tree_cannot_add_an_unobserved_executable_node(self):
        records = fixture_log()
        tree = tree_record()
        tree["dump"]["task"]["root"]["children"]["extra"] = dict(
            node_id=90, status="ready", generation=1, value="extra", children={},
        )
        records.insert(-1, tree)
        with self.assertRaisesRegex(AssertionError, "no observed owner lifecycle"):
            self.validate(order(records))

    def test_later_snapshot_cannot_drop_a_prefix(self):
        before = tree_record("before")
        before["dump"]["task"]["root"]["children"] = {
            "extra": dict(node_id=90, status="prefix", children={}),
        }
        records = fixture_log()
        records.insert(1, before)
        records.insert(-1, tree_record())
        with self.assertRaisesRegex(AssertionError, "removed previously observed nodes"):
            self.validate(order(records))

    def test_repeated_pending_claims_are_response_counts_not_saved_work(self):
        records = fixture_log()
        records.insert(4, copy.deepcopy(records[3]))
        summary = self.validate(order(records))
        self.assertEqual(summary["claims"]["waiters"], 2)
        self.assertIn("not distinct", summary["count_semantics"])

    def test_tree_metadata_missing_is_unsupported_not_failure(self):
        records = fixture_log()
        tree = tree_record()
        del tree["dump"]["task"]["root"]["node_id"]
        records.insert(-1, tree)
        with self.assertRaises(UnsupportedMetadata):
            self.validate(order(records))

    def test_performance_disallows_full_dumps(self):
        records = fixture_log()
        records[0]["capture_mode"] = "performance"
        records.insert(-1, tree_record())
        with self.assertRaisesRegex(AssertionError, "performance"):
            self.validate(order(records))

    def test_incomplete_sampled_dropped_missing_footer(self):
        for mutation in ["incomplete", "sampled", "dropped", "footer", "header", "uncompleted", "dropcount"]:
            with self.subTest(mutation=mutation):
                records = fixture_log()
                if mutation == "incomplete":
                    records[0]["events_complete"] = False
                elif mutation == "sampled":
                    records[0]["sampled"] = True
                elif mutation == "dropped":
                    records[-1]["dropped_events"] = 1
                elif mutation == "uncompleted":
                    records[-1]["completed"] = False
                elif mutation == "dropcount":
                    del records[-1]["dropped_events"]
                elif mutation == "footer":
                    records.pop()
                else:
                    records.pop(0)
                with self.assertRaises(IncompleteProbeLog):
                    self.validate(records)

    def test_tree_requirement_is_not_silently_skipped(self):
        with self.assertRaises(IncompleteProbeLog):
            self.validate(require_tree=True)

    def test_required_tree_must_be_final_for_each_task(self):
        for stage in ["before", "pending"]:
            with self.subTest(stage=stage):
                records = fixture_log()
                tree = tree_record(stage, "pending")
                if stage == "before":
                    tree["dump"]["task"]["root"]["children"] = {}
                    records.insert(1, tree)
                else:
                    records.insert(4, tree)
                with self.assertRaisesRegex(IncompleteProbeLog, "final tree snapshot"):
                    self.validate(order(records), require_tree=True)

    def test_required_tree_cannot_cover_only_one_of_two_tasks(self):
        records = fixture_log()
        other = copy.deepcopy(records[1:9])
        for record in other:
            record["task_id"] = "other-task"
        records[-1:-1] = other + [tree_record()]
        with self.assertRaisesRegex(IncompleteProbeLog, "Each observed task"):
            self.validate(order(records), require_tree=True)

    def test_private_owner_tokens_rejected_everywhere_without_echo(self):
        for index, nested in [(0, False), (3, False), (-1, False), (3, True)]:
            with self.subTest(index=index, nested=nested):
                records = fixture_log()
                token = {"owner_token": "DO_NOT_ECHO_PRIVATE_TOKEN"}
                if nested:
                    records[index]["details"] = [token]
                else:
                    records[index].update(token)
                with self.assertRaisesRegex(AssertionError, "Private owner tokens") as context:
                    self.validate(records)
                self.assertNotIn("DO_NOT_ECHO_PRIVATE_TOKEN", str(context.exception))

    def test_sequence_gap_is_incomplete_evidence(self):
        records = fixture_log()
        records.pop(3)
        with self.assertRaisesRegex(IncompleteProbeLog, "missing records"):
            self.validate(records)

    def test_empty_log_is_incomplete(self):
        with self.assertRaises(IncompleteProbeLog):
            self.validate([])

    def test_start_end_only_has_no_evidence(self):
        records = fixture_log()
        with self.assertRaises(IncompleteProbeLog):
            self.validate(order([records[0], records[-1]]))

    def test_duplicate_or_reverse_sequences_and_mixed_runs(self):
        for field, value in [("seq", 1), ("seq", 0), ("run_id", "other")]:
            with self.subTest(field=field, value=value):
                records = fixture_log()
                records[3][field] = value
                with self.assertRaises(AssertionError):
                    self.validate(records)

    def test_bad_fields_are_failures(self):
        for field, value in [("node_id", True), ("node_id", -1), ("generation", 0), ("ts_ns", "1"), ("seq", True), ("path", []), ("path", "read"), ("path", [1]), ("attempt_id", ""), ("task_id", "")]:
            with self.subTest(field=field, value=value):
                records = fixture_log()
                records[1][field] = value
                with self.assertRaises(AssertionError) as context:
                    self.validate(records)
                self.assertNotIsInstance(context.exception, IncompleteProbeLog)

    def test_orphan_waiter_hit_execution_and_publish_are_rejected(self):
        for index in [2, 3, 6, 8]:
            with self.subTest(index=index):
                records = fixture_log()
                records[1] = copy.deepcopy(records[index])
                with self.assertRaisesRegex(AssertionError, "preceding owner"):
                    self.validate(order(records))

    def test_duplicate_owner_and_execution_are_rejected(self):
        for index in [1, 2]:
            with self.subTest(index=index):
                records = fixture_log()
                records.insert(3, copy.deepcopy(records[index]))
                with self.assertRaises(AssertionError):
                    self.validate(order(records))

    def test_waiter_cannot_execute_or_publish(self):
        for index in [2, 5, 6]:
            with self.subTest(index=index):
                records = fixture_log()
                records[index]["attempt_id"] = "second"
                with self.assertRaisesRegex(AssertionError, "Only the recorded owner"):
                    self.validate(records)

    def test_ready_cannot_precede_publish_and_pending_cannot_follow_it(self):
        for index, outcome in [(3, "ready"), (7, "pending"), (7, "failed")]:
            with self.subTest(index=index, outcome=outcome):
                records = fixture_log()
                records[index]["outcome"] = outcome
                with self.assertRaisesRegex(AssertionError, "contradicts"):
                    self.validate(records)

    def test_publication_requires_end_and_present_value(self):
        for mutation in ["noend", "nostart", "missingvalue", "repeatend"]:
            with self.subTest(mutation=mutation):
                records = fixture_log()
                if mutation == "noend":
                    records.pop(5)
                elif mutation == "nostart":
                    records.pop(2)
                elif mutation == "missingvalue":
                    records[6]["value_present"] = False
                else:
                    records.insert(6, copy.deepcopy(records[5]))
                with self.assertRaises(AssertionError):
                    self.validate(order(records))

    def test_failed_generation_can_retry_with_same_id(self):
        records = fixture_log()
        records[6].update(status="failed", value_present=False)
        records = records[:7] + [dict(records[7], outcome="failed")]
        retry = copy.deepcopy(fixture_log()[1:9])
        for record in retry:
            record.update(generation=2)
        records += retry + [fixture_log()[-1]]
        result = self.validate(order(records))
        self.assertEqual(result["executions"], 2)
        self.assertEqual(result["claims"]["owners"], 2)

    def test_owner_failure_before_execution_is_terminal(self):
        records = fixture_log()
        records = records[:2] + [dict(records[6], status="failed", value_present=False), records[-1]]
        self.assertEqual(self.validate(order(records))["executions"], 0)

    def test_stale_generation_or_changed_node_id_is_rejected(self):
        for field, value in [("generation", 2), ("node_id", 8)]:
            with self.subTest(field=field):
                records = fixture_log()
                records[6][field] = value
                with self.assertRaises(AssertionError):
                    self.validate(records)

    def test_task_and_history_separate_identity_scope(self):
        records = fixture_log()
        other = copy.deepcopy(records[1:9])
        for record in other:
            record["task_id"] = "other-task"
        records[-1:-1] = other
        result = self.validate(order(records))
        self.assertEqual(result["executions"], 2)
        records = fixture_log()
        other = copy.deepcopy(records[1:9])
        for record in other:
            record.update(path=["different setup", "read"], node_id=9)
        records[-1:-1] = other
        self.assertEqual(self.validate(order(records))["executions"], 2)

    def test_node_id_cannot_be_reused_at_different_path(self):
        records = fixture_log()
        other = dict(records[1], path=["different setup", "read"])
        records.insert(-1, other)
        with self.assertRaisesRegex(AssertionError, "reused or moved"):
            self.validate(order(records))

    def test_unfinished_owner_is_incomplete(self):
        records = fixture_log()
        with self.assertRaises(IncompleteProbeLog):
            self.validate(order(records[:5] + [records[-1]]))

    def test_unknown_and_postfooter_events_fail(self):
        records = fixture_log()
        records[3]["event"] = "future_event"
        with self.assertRaisesRegex(AssertionError, "Unknown event"):
            self.validate(records)
        records = fixture_log()
        records.append(copy.deepcopy(records[7]))
        with self.assertRaisesRegex(AssertionError, "run_end"):
            self.validate(order(records))


if __name__ == "__main__":
    unittest.main()
