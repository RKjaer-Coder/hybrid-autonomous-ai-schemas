"""Unit tests for harness scoring logic using MockBackend."""

from __future__ import annotations

import unittest

from eval.harnesses.harness_kill import KillHarness
from eval.harnesses.harness_m1 import M1Harness
from eval.harnesses.harness_m2 import M2Harness
from eval.harnesses.harness_m3 import M3Harness
from eval.harnesses.harness_m4 import M4Harness
from eval.harnesses.harness_m5 import M5Harness
from eval.runner import MockBackend


class HarnessTests(unittest.TestCase):
    def test_mock_backend_passes_all(self):
        b = MockBackend()
        self.assertEqual(M1Harness().run(b)["status"], "PASS")
        self.assertEqual(M2Harness().run(b)["status"], "PASS")
        self.assertEqual(M3Harness().run(b)["status"], "PASS")
        self.assertEqual(M4Harness().run(b)["status"], "PASS")
        self.assertEqual(M5Harness().run(b)["status"], "PASS")
        self.assertEqual(KillHarness().run(b)["status"], "PASS")

    def test_m1_degrades_with_wrong_verdict(self):
        class BadMock(MockBackend):
            def sheriff_check(self, task):
                out = super().sheriff_check(task)
                if task.get("category") == "clean":
                    out["verdict"] = "BLOCK"
                return out

        res = M1Harness().run(BadMock())
        self.assertEqual(res["status"], "FAIL")

    def test_edge_empty_percentiles(self):
        from eval.report import compute_latency_percentiles

        self.assertEqual(compute_latency_percentiles([])["p95"], 0.0)


if __name__ == "__main__":
    unittest.main()
