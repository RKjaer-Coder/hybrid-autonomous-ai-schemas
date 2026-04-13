"""Integration test for the real M4 financial routing backend."""

from __future__ import annotations

import unittest

from eval.backends.financial_backend import Backend
from eval.harnesses.harness_m4 import M4Harness


class FinancialBackendTests(unittest.TestCase):
    def test_m4_harness_runs_against_real_financial_backend(self):
        result = M4Harness().run(Backend())
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["routing_match_rate"], 1.0)
        self.assertEqual(result["ledger_persistence_rate"], 1.0)
        self.assertEqual(result["g3_enforcement_rate"], 1.0)
        self.assertEqual(result["routing_path_coverage"], 7)
        self.assertEqual(result["false_autonomous_spend"], 0)


if __name__ == "__main__":
    unittest.main()
