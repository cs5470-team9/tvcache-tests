"""CPU-only parser and invariant tests; these do not test a live cache server."""

import copy
import unittest

from tvcache_checks.tree import (
    UnsupportedMetadata,
    assert_same_identity,
    assert_transition,
    inspect_task,
)


def node(node_id, status="prefix", children=None, **fields):
    result = {
        "node_id": node_id,
        "status": status,
        "children": {} if children is None else children,
        "value": None,
        "sandbox_id": None,
    }
    if status in {"pending", "ready", "failed"}:
        result["generation"] = 1
    result.update(fields)
    return result


def dump_tree():
    return {"task": {"root": node(0, "root", {
        "setup A": node(7, children={
            "read fixture": node(19, "pending"),
        }),
        "setup B": node(2, children={
            "read fixture": node(44, "ready", value="B"),
        }),
    }), "number_of_nodes": 4}}


class TreeTests(unittest.TestCase):
    def test_nested_branches_and_nonsequential_ids(self):
        snapshot = inspect_task(dump_tree(), "task")
        snapshot.assert_shape([
            (), ("setup A",), ("setup A", "read fixture"),
            ("setup B",), ("setup B", "read fixture"),
        ])
        first = snapshot.assert_node(
            ("setup A", "read fixture"), state="pending", node_id=19,
            generation=1, value=None,
        )
        second = snapshot.assert_node(
            ("setup B", "read fixture"), state="ready", value="B",
        )
        self.assertEqual(first.parent_path, ("setup A",))
        self.assertNotEqual(first.node_id, second.node_id)
        self.assertIsNone(snapshot.nodes[()].parent_path)

    def test_empty_string_is_valid_ready_result(self):
        dump = dump_tree()
        dump["task"]["root"]["children"]["setup B"]["children"]["read fixture"]["value"] = ""
        inspect_task(dump, "task").assert_node(
            ("setup B", "read fixture"), state="ready", value="",
        )

    def test_pending_must_not_expose_a_value(self):
        for value in ["", "premature"]:
            with self.subTest(value=value):
                dump = dump_tree()
                dump["task"]["root"]["children"]["setup A"]["children"]["read fixture"]["value"] = value
                with self.assertRaisesRegex(AssertionError, "Pending node"):
                    inspect_task(dump, "task")

    def test_ready_requires_result_even_when_metadata_is_optional(self):
        dump = dump_tree()
        dump["task"]["root"]["children"]["setup B"]["children"]["read fixture"]["value"] = None
        with self.assertRaisesRegex(AssertionError, "Ready node"):
            inspect_task(dump, "task", require_metadata=False)

    def test_duplicate_ids_are_rejected_within_one_task(self):
        dump = dump_tree()
        dump["task"]["root"]["children"]["setup B"]["children"]["read fixture"]["node_id"] = 19
        with self.assertRaisesRegex(AssertionError, "Duplicate node_id"):
            inspect_task(dump, "task")

    def test_same_ids_in_different_tasks_are_allowed(self):
        dump = dump_tree()
        dump["other"] = copy.deepcopy(dump["task"])
        inspect_task(dump, "task")
        inspect_task(dump, "other")

    def test_missing_metadata_is_distinct_from_malformed_output(self):
        legacy = {"task": {"root": {
            "children": {"call": {"children": {}, "value": ""}},
            "sandbox_id": "snapshot",
        }}}
        with self.assertRaises(UnsupportedMetadata):
            inspect_task(legacy, "task")
        snapshot = inspect_task(legacy, "task", require_metadata=False)
        self.assertTrue(snapshot.legacy)
        snapshot.assert_shape([(), ("call",)])
        snapshot.assert_node(("call",), state="legacy", value="", node_id=None)
        with self.assertRaises(UnsupportedMetadata):
            assert_same_identity(snapshot, snapshot)

    def test_missing_generation_is_unsupported(self):
        dump = dump_tree()
        del dump["task"]["root"]["children"]["setup A"]["children"]["read fixture"]["generation"]
        with self.assertRaisesRegex(UnsupportedMetadata, "generation"):
            inspect_task(dump, "task")

    def test_snapshot_only_prefix_is_not_pending(self):
        dump = dump_tree()
        dump["task"]["root"]["children"]["setup A"]["sandbox_id"] = "snapshot-A"
        inspect_task(dump, "task").assert_node(
            ("setup A",), state="prefix", value=None,
            sandbox_id="snapshot-A", generation=None,
        )

    def test_malformed_fields(self):
        mutations = [
            ("children", []), ("children", {"edge": None}),
            ("children", {17: node(100)}),
            ("value", 7), ("sandbox_id", {}),
            ("node_id", True), ("node_id", -1), ("node_id", 1.0),
            ("status", "owner"), ("status", None), ("status", []),
            ("generation", True), ("generation", -1), ("generation", 0),
        ]
        for field, value in mutations:
            with self.subTest(field=field, value=value):
                dump = dump_tree()
                dump["task"]["root"][field] = value
                with self.assertRaises(AssertionError) as context:
                    inspect_task(dump, "task")
                self.assertNotIsInstance(context.exception, UnsupportedMetadata)

    def test_malformed_legacy_tree_is_not_unsupported(self):
        with self.assertRaises(AssertionError) as context:
            inspect_task({"task": {"root": {"children": []}}}, "task")
        self.assertNotIsInstance(context.exception, UnsupportedMetadata)

    def test_top_level_errors(self):
        for dump in [[], {}, {"task": None}, {"task": {"root": []}}]:
            with self.subTest(dump=dump), self.assertRaises(AssertionError):
                inspect_task(dump, "task")

    def test_state_alias_and_unknown_extra_fields(self):
        dump = dump_tree()
        root = dump["task"]["root"]
        root["state"] = root.pop("status")
        root["future_metadata"] = {"allowed": True}
        inspect_task(dump, "task")
        root["status"] = "pending"
        with self.assertRaisesRegex(AssertionError, "conflicting"):
            inspect_task(dump, "task")

    def test_root_and_prefix_states_are_distinct(self):
        dump = dump_tree()
        dump["task"]["root"]["status"] = "prefix"
        with self.assertRaisesRegex(AssertionError, "root status"):
            inspect_task(dump, "task")
        dump = dump_tree()
        dump["task"]["root"]["children"]["setup A"]["status"] = "root"
        with self.assertRaisesRegex(AssertionError, "Only the task root"):
            inspect_task(dump, "task")

    def test_pending_to_ready_preserves_identity_and_other_nodes(self):
        dump = dump_tree()
        before = inspect_task(dump, "task")
        target = dump["task"]["root"]["children"]["setup A"]["children"]["read fixture"]
        target.update(status="ready", value="A", sandbox_id="state-after-A")
        after = inspect_task(dump, "task")
        assert_transition(before, after, ("setup A", "read fixture"), "ready")
        after.assert_node(("setup A", "read fixture"), state="ready", generation=1)
        with self.assertRaisesRegex(AssertionError, "state"):
            assert_transition(before, after, ("setup A", "read fixture"), "failed")

    def test_transition_detects_unrelated_changes(self):
        dump = dump_tree()
        before = inspect_task(dump, "task")
        dump["task"]["root"]["children"]["setup B"]["sandbox_id"] = "changed"
        after = inspect_task(dump, "task")
        with self.assertRaisesRegex(AssertionError, "outside target"):
            assert_transition(before, after, ("setup A", "read fixture"), "pending")

    def test_identity_rejects_reallocated_id(self):
        dump = dump_tree()
        before = inspect_task(dump, "task")
        dump["task"]["root"]["children"]["setup A"]["node_id"] = 500
        with self.assertRaisesRegex(AssertionError, "identity"):
            assert_same_identity(before, inspect_task(dump, "task"))

    def test_identity_rejects_changed_parent(self):
        dump = dump_tree()
        before = inspect_task(dump, "task")
        root_children = dump["task"]["root"]["children"]
        root_children["setup B"]["children"]["other read"] = root_children["setup A"]["children"].pop("read fixture")
        with self.assertRaisesRegex(AssertionError, "shape"):
            assert_same_identity(before, inspect_task(dump, "task"))

    def test_identity_rejects_added_nodes_and_other_task(self):
        dump = dump_tree()
        before = inspect_task(dump, "task")
        dump["task"]["root"]["children"]["new"] = node(99)
        with self.assertRaisesRegex(AssertionError, "shape"):
            assert_same_identity(before, inspect_task(dump, "task"))
        dump["other"] = copy.deepcopy(dump["task"])
        with self.assertRaisesRegex(AssertionError, "different tasks"):
            assert_same_identity(before, inspect_task(dump, "other"))

    def test_shape_and_node_helpers_report_mismatches(self):
        snapshot = inspect_task(dump_tree(), "task")
        with self.assertRaisesRegex(AssertionError, "shape"):
            snapshot.assert_shape([()])
        with self.assertRaisesRegex(AssertionError, "not found"):
            snapshot.assert_node(("missing",), state="ready")
        with self.assertRaisesRegex(AssertionError, "value"):
            snapshot.assert_node(("setup B", "read fixture"), state="ready", value=None)

    def test_cycles_are_rejected(self):
        root = node(0, "root")
        root["children"]["cycle"] = root
        with self.assertRaisesRegex(AssertionError, "reused or cyclic"):
            inspect_task({"task": {"root": root}}, "task")


if __name__ == "__main__":
    unittest.main()
