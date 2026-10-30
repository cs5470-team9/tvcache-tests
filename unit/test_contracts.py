"""Check the test-side response boundary; these are not server tests."""

import unittest

from tvcache_checks.contracts import Outcome


class OutcomeTests(unittest.TestCase):
    def test_valid_owner_and_empty_ready_value(self):
        self.assertEqual(Outcome("owner", 3, 1, owner_token=0).validate().node_id, 3)
        self.assertEqual(Outcome("ready", 3, 1, value="").validate().value, "")

    def test_token_is_not_logged(self):
        owner = Outcome("owner", 3, 1, owner_token="private-token").validate()
        self.assertNotIn("owner_token", owner.public_fields())
        self.assertNotIn("private-token", str(owner.public_fields()))

    def test_non_owner_cannot_receive_publication_token(self):
        with self.assertRaisesRegex(AssertionError, "leaked"):
            Outcome("pending", 3, 1, owner_token="token").validate()

    def test_pending_value_is_not_a_result(self):
        with self.assertRaisesRegex(AssertionError, "Uncompleted"):
            Outcome("pending", 3, 1, value="").validate()

    def test_missing_is_not_ready(self):
        self.assertEqual(Outcome("missing", None, None).validate().status, "missing")
        with self.assertRaisesRegex(AssertionError, "Ready"):
            Outcome("ready", 3, 1).validate()

    def test_owner_requires_token(self):
        with self.assertRaisesRegex(AssertionError, "token"):
            Outcome("owner", 3, 1).validate()

    def test_invalid_metadata(self):
        for node_id, generation in [(True, 1), (-1, 1), (1, False), (1, 0)]:
            with self.subTest(node_id=node_id, generation=generation), self.assertRaises(AssertionError):
                Outcome("pending", node_id, generation).validate()

    def test_unknown_state(self):
        with self.assertRaisesRegex(AssertionError, "Unknown"):
            Outcome("invented", 1, 1).validate()
