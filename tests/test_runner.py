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

    def test_global_timeout_marks_milestone_failure(self):
        result = run_all(MockBackend(), ["M1"], global_timeout_s=0.0, backend_path="mock")
        self.assertEqual(result["summary"]["failed"], 1)
        self.assertEqual(result["milestones"]["M1"]["error_type"], "RunTimeout")

    def test_timeout_without_backend_path_is_rejected(self):
        with self.assertRaises(ValueError):
            run_all(MockBackend(), ["M1"], per_milestone_timeout_s=1.0, backend_path=None)

    def test_stateful_backend_reinit_guard_is_enforced(self):
        with self.assertRaises(ValueError):
            run_all(
                MockBackend(),
                ["M1", "M2"],
                global_timeout_s=10.0,
                backend_path="custom_backend.module",
            )


if __name__ == "__main__":
    unittest.main()
