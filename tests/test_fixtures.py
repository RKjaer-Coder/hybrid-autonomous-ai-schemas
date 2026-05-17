"""Tests for eval fixture generators and structural validity."""

from __future__ import annotations

import datetime as dt
import json
import unittest
import uuid

from eval.fixtures.kill_recommender import generate_calibration_set
from eval.fixtures.live_project_handoff import generate_first_live_project_test_set
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

    def test_first_live_project_handoff_structural(self):
        handoff = generate_first_live_project_test_set()
        fixture = handoff["fixture"]
        self.assertEqual(handoff["name"], "first_live_project_handoff")
        self.assertEqual(fixture["project"]["cloud_spend_cap_usd"], 0.0)
        self.assertFalse(fixture["project"]["external_commitments_allowed"])
        self.assertEqual([task["phase"] for task in fixture["tasks"]], ["validate", "build", "ship", "operate"])
        self.assertTrue(all(_is_uuid_v7(task["task_id"]) for task in fixture["tasks"]))
        self.assertTrue(_is_uuid_v7(fixture["project"]["project_id"]))
        self.assertTrue(_is_uuid_v7(fixture["artifact_expectations"]["artifact_id"]))
        self.assertFalse(fixture["side_effect_expectations"]["autonomous_delivery_allowed"])
        self.assertFalse(fixture["side_effect_expectations"]["replay_reexecutes_side_effect"])
        blocked = {cap for task in fixture["tasks"] for cap in task["blocked_capabilities"]}
        self.assertIn("paid_provider_call", blocked)
        self.assertIn("send_message", blocked)

    def test_manual_patch_gate_rehearsal_is_review_only(self):
        handoff = generate_first_live_project_test_set()
        rehearsal = handoff["manual_patch_gate_rehearsal"]
        authority = rehearsal["authority"]
        self.assertEqual(rehearsal["patch_packet_id"], "known-bad-follow-on-f45960c737b4cd4e3657f9e5")
        self.assertEqual(authority["required_authority"], "operator_gate")
        self.assertTrue(authority["manual_application_only"])
        self.assertFalse(authority["autonomous_patch_application_enabled"])
        self.assertFalse(authority["active_frontier_promotion"])
        self.assertFalse(authority["route_updates_enabled"])
        self.assertFalse(authority["side_effect_replay_enabled"])
        self.assertEqual(authority["default_on_timeout"], "keep_current_behavior")
        self.assertIn("open_normal_code_review_pr", rehearsal["review_sequence"])
        self.assertIn("autonomous_patch_application", rehearsal["blocked_autonomous_actions"])

    def test_hermes_adapter_validation_harness_covers_pre_live_checklist(self):
        handoff = generate_first_live_project_test_set()
        harness = handoff["hermes_adapter_validation_harness"]
        surfaces = {check["surface"] for check in harness["checks"]}
        self.assertEqual(harness["hermes_version_floor"], "0.13.0")
        self.assertEqual(len(harness["checks"]), 10)
        self.assertEqual(
            surfaces,
            {
                "kanban_worker_lifecycle",
                "goal_checkpoint_gateway_resume",
                "no_agent_cron_watchdog",
                "provider_plugins_and_model_profiles",
                "mcp_sse_oauth_forwarding",
                "native_dashboard_controls",
                "platform_allowlists_redaction_media",
                "lm_studio_local_provider_routes",
                "target_machine_recovery",
                "break_glass_halt",
            },
        )
        self.assertTrue(all(check["durable_evidence_required"] for check in harness["checks"]))
        self.assertTrue(all(check["blocked_without_evidence"] for check in harness["checks"]))
        self.assertTrue(all(not check["live_controls_enabled_after_pass"] for check in harness["checks"]))
        self.assertTrue(all(not check["replay_executes_external_effects"] for check in harness["checks"]))

    def test_first_live_project_dry_run_is_end_to_end_and_local_only(self):
        handoff = generate_first_live_project_test_set()
        dry_run = handoff["dry_run"]
        self.assertEqual([phase["phase"] for phase in dry_run["phases"]], ["validate", "build", "ship", "operate"])
        self.assertTrue(all(phase["event_before_projection"] for phase in dry_run["phases"]))
        self.assertTrue(all(not phase["external_side_effects_executed"] for phase in dry_run["phases"]))
        self.assertTrue(dry_run["acceptance"]["local_artifact_only"])
        self.assertTrue(dry_run["acceptance"]["operator_gate_before_external_delivery"])
        self.assertFalse(dry_run["acceptance"]["autonomous_customer_commitments_allowed"])
        self.assertTrue(dry_run["close_path"]["feedback_ingested"])
        self.assertTrue(dry_run["close_path"]["close_or_continue_requires_operator_gate"])

    def test_authority_boundary_gauntlet_fails_closed(self):
        handoff = generate_first_live_project_test_set()
        gauntlet = handoff["authority_boundary_gauntlet"]
        cases = gauntlet["cases"]
        self.assertEqual(gauntlet["activation_effect"], "none")
        self.assertEqual(len(cases), 8)
        self.assertTrue(all(case["kernel_event_required_before_state_change"] for case in cases))
        self.assertTrue(all(not case["live_controls_enabled"] for case in cases))
        verdict_by_action = {case["attempted_action"]: case["expected_verdict"] for case in cases}
        self.assertEqual(verdict_by_action["provider_plugin_paid_call"], "blocked")
        self.assertEqual(verdict_by_action["native_dashboard_write_to_operator_gate"], "projection_only")
        self.assertEqual(verdict_by_action["replay_external_message_or_purchase"], "reconstruct_only")
        self.assertEqual(verdict_by_action["promote_local_model_route"], "shadow_only")
        self.assertEqual(verdict_by_action["read_sensitive_or_client_artifact"], "denied")
        self.assertEqual(verdict_by_action["apply_self_improvement_patch_from_packet"], "review_only")

    def test_seed_determinism(self):
        self.assertEqual(generate_known_bad_inputs(seed=777), generate_known_bad_inputs(seed=777))
        self.assertEqual(generate_first_live_project_test_set(seed=777), generate_first_live_project_test_set(seed=777))


if __name__ == "__main__":
    unittest.main()
