from __future__ import annotations

import unittest

from immune.patterns.structural_rules import (
    StructuralLimits,
    check_circular_references,
    check_id_format,
    check_known_tool,
    check_nesting_depth,
    check_required_fields,
    check_string_length,
)


class TestStructuralRules(unittest.TestCase):
    def test_required_fields_pass(self):
        payload = {"session_id": "x", "skill_name": "a", "tool_name": "t"}
        self.assertIsNone(check_required_fields(payload))

    def test_required_fields_fail(self):
        payload = {"session_id": "x", "skill_name": "a"}
        self.assertIsNotNone(check_required_fields(payload))

    def test_string_length_pass(self):
        payload = {"session_id": "x", "skill_name": "a", "tool_name": "t", "msg": "ok"}
        self.assertIsNone(check_string_length(payload, StructuralLimits(max_string_length=10)))

    def test_string_length_fail(self):
        payload = {"session_id": "x", "skill_name": "a", "tool_name": "t", "msg": "x" * 20}
        self.assertIsNotNone(check_string_length(payload, StructuralLimits(max_string_length=10)))

    def test_nesting_depth_pass(self):
        payload = {"session_id": "x", "skill_name": "a", "tool_name": "t", "args": {"a": {"b": 1}}}
        self.assertIsNone(check_nesting_depth(payload, StructuralLimits(max_nesting_depth=5)))

    def test_nesting_depth_fail(self):
        payload = {"session_id": "x", "skill_name": "a", "tool_name": "t", "args": {"a": {"b": {"c": {"d": {"e": 1}}}}}}
        self.assertIsNotNone(check_nesting_depth(payload, StructuralLimits(max_nesting_depth=3)))

    def test_circular_references_pass(self):
        payload = {"session_id": "x", "skill_name": "a", "tool_name": "t", "args": {"k": [1, 2]}}
        self.assertIsNone(check_circular_references(payload))

    def test_circular_references_fail(self):
        payload = {"session_id": "x", "skill_name": "a", "tool_name": "t"}
        payload["self"] = payload
        self.assertIsNotNone(check_circular_references(payload))

    def test_id_format_pass(self):
        payload = {
            "session_id": "018f5c3a-1234-7abc-8abc-1234567890ab",
            "skill_name": "safe",
            "tool_name": "tool",
        }
        self.assertIsNone(check_id_format(payload))

    def test_id_format_fail(self):
        payload = {"session_id": "not-a-uuid", "skill_name": "safe", "tool_name": "tool"}
        self.assertIsNotNone(check_id_format(payload))

    def test_known_tool_bootstrap_mode(self):
        payload = {"session_id": "x", "skill_name": "a", "tool_name": "unknown"}
        self.assertIsNone(check_known_tool(payload, StructuralLimits(known_tools=frozenset())))

    def test_known_tool_enforced_fail(self):
        payload = {"session_id": "x", "skill_name": "a", "tool_name": "unknown"}
        limits = StructuralLimits(known_tools=frozenset({"web_fetch", "file_write"}))
        self.assertIsNotNone(check_known_tool(payload, limits))

    def test_known_tool_enforced_pass(self):
        payload = {"session_id": "x", "skill_name": "a", "tool_name": "web_fetch"}
        limits = StructuralLimits(known_tools=frozenset({"web_fetch", "file_write"}))
        self.assertIsNone(check_known_tool(payload, limits))

    def test_id_format_ignores_non_id_fields(self):
        payload = {"session_id": "018f5c3a-1234-7abc-8abc-1234567890ab", "skill_name": "safe", "tool_name": "tool", "note": "not-a-uuid"}
        self.assertIsNone(check_id_format(payload))

    def test_nesting_depth_boundary(self):
        payload = {"session_id": "x", "skill_name": "a", "tool_name": "t", "args": {"a": {"b": 1}}}
        self.assertIsNone(check_nesting_depth(payload, StructuralLimits(max_nesting_depth=3)))


if __name__ == "__main__":
    unittest.main()
