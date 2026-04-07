"""Tests for eval fixture generators and structural validity."""

from __future__ import annotations

import datetime as dt
import json
import unittest
import uuid

from eval.fixtures.kill_recommender import generate_calibration_set
from eval.fixtures.m1_immune_system import generate_known_bad_inputs, generate_m1_test_set
from eval.fixtures.m2_memory_integrity import generate_m2_test_set
from eval.fixtures.m3_task_execution import generate_m3_test_set
from eval.fixtures.m5_council_calibration import generate_m5_test_set


def _is_uuid_v7(value: str) -> bool:
    u = uuid.UUID(value)
    return u.version == 7


def _is_iso(value: str) -> bool:
    dt.datetime.fromisoformat(value)
    return True


class FixtureTests(unittest.TestCase):
    def test_m1_structural(self):
        m1 = generate_m1_test_set()
        self.assertEqual(len(m1["clean_tasks"]), 20)
        self.assertEqual(len(m1["known_bad_inputs"]), 3)
        self.assertGreaterEqual(len({t["task_type"] for t in m1["clean_tasks"]}), 4)
        cats = {x["category"] for x in m1["known_bad_inputs"]}
        self.assertEqual(len(cats), 3)
        for t in m1["clean_tasks"] + m1["known_bad_inputs"]:
            self.assertTrue(_is_uuid_v7(t["task_id"]))
            if isinstance(t["structured_fields"], dict):
                json.dumps(t["structured_fields"])

    def test_m2_structural(self):
        m2 = generate_m2_test_set()
        self.assertEqual(len(m2["memory_roundtrips"]), 50)
        self.assertEqual(len(m2["relevance_queries"]), 10)
        self.assertEqual(len(m2["wal_recovery_nodes"]), 10)
        self.assertEqual(len({x["node_type"] for x in m2["memory_roundtrips"]}), 9)
        for r in m2["memory_roundtrips"]:
            self.assertTrue(_is_uuid_v7(r["roundtrip_id"]))
            self.assertTrue(1 <= r["trust_tier"] <= 4)
            for p in r["provenance_links"]:
                self.assertTrue(_is_uuid_v7(p))

    def test_m3_structural(self):
        m3 = generate_m3_test_set()
        self.assertEqual(len(m3["e2e_scenarios"]), 10)
        self.assertEqual(len(m3["failure_scenarios"]), 2)
        self.assertEqual(len(m3["validation_outputs"]), 20)
        self.assertGreaterEqual(len({x["task_type"] for x in m3["e2e_scenarios"]}), 3)
        self.assertTrue(all(s["force_failure"] for s in m3["failure_scenarios"]))

    def test_m5_structural(self):
        m5 = generate_m5_test_set()
        labels = [x["ground_truth"]["label"] for x in m5["test_opportunities"]]
        self.assertEqual(labels.count("GOOD"), 2)
        self.assertEqual(labels.count("BAD"), 2)
        self.assertEqual(labels.count("AMBIGUOUS"), 1)
        self.assertEqual({x["domain"] for x in m5["research_scenarios"]}, {1, 2, 3, 4, 5})

    def test_kill_structural_and_score(self):
        items = generate_calibration_set()
        self.assertEqual(len(items), 20)
        from collections import Counter

        outcomes = Counter(x["ground_truth"]["actual_outcome"] for x in items)
        self.assertEqual(outcomes["killed_correct"], 5)
        self.assertEqual(outcomes["continued_waste"], 3)
        for p in items:
            self.assertTrue(_is_uuid_v7(p["project_id"]))
            computed = round(sum(s["weight"] * s["raw_score"] for s in p["kill_signals"]), 4)
            self.assertEqual(computed, p["kill_score"])

    def test_seed_determinism(self):
        self.assertEqual(generate_known_bad_inputs(seed=777), generate_known_bad_inputs(seed=777))


if __name__ == "__main__":
    unittest.main()
