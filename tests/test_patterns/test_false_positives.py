from __future__ import annotations

import unittest

from immune.patterns.false_positive_allowlist import FALSE_POSITIVE_CASES, is_allowlisted
from immune.patterns.ipi_patterns import check_ipi


class TestFalsePositives(unittest.TestCase):
    """Legitimate patterns must NOT be blocked after allowlist context is applied."""

    def test_false_positive_cases_not_flagged(self):
        for case in FALSE_POSITIVE_CASES:
            with self.subTest(case_id=case.case_id):
                matches = check_ipi(case.text)
                if case.case_id in {"FP-002", "FP-005", "FP-003", "FP-007"}:
                    self.assertTrue(is_allowlisted(case.text, case.field_name))
                else:
                    self.assertEqual(matches, [], f"{case.case_id} unexpectedly matched")

    def test_allowlist_exact_match(self):
        case = FALSE_POSITIVE_CASES[0]
        self.assertTrue(is_allowlisted(case.text, case.field_name))

    def test_allowlist_field_sensitive(self):
        case = FALSE_POSITIVE_CASES[0]
        self.assertFalse(is_allowlisted(case.text, "arguments"))


if __name__ == "__main__":
    unittest.main()
