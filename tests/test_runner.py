"""Integration tests for eval runner output."""

from __future__ import annotations

import json
import unittest

from eval.runner import MockBackend, run_all


class RunnerTests(unittest.TestCase):
    def test_runner_all_milestones(self):
        result = run_all(MockBackend(), ["M1", "M2", "M3", "M5", "KILL"])
        payload = json.loads(json.dumps(result))
        self.assertIn("run_id", payload)
        self.assertEqual(payload["summary"]["total_milestones"], 5)
        self.assertEqual(payload["summary"]["failed"], 0)
        self.assertEqual(payload["summary"]["overall_status"], "PASS")


if __name__ == "__main__":
    unittest.main()
