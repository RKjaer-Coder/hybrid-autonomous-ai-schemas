from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from kernel import (
    KernelSelfImprovement,
    KernelStore,
    SelfImprovementEvalRecord,
    SelfImprovementPatchReviewPacket,
    SelfImprovementPromotionPacket,
    SelfImprovementProposal,
    SelfImprovementRollbackRecord,
    self_improvement_command,
)
from kernel.records import sha256_text


class KernelSelfImprovementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = KernelStore(Path(self.tmp.name) / "kernel.db")
        self.si = KernelSelfImprovement(self.store)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def proposal(self) -> SelfImprovementProposal:
        return SelfImprovementProposal(
            proposal_id="proposal-harness-summary-v2",
            target_type="harness",
            target_id="research.quick_summary.prompt@v1",
            problem_evidence=[
                "artifact://replay/failure-examples/unsupported-claim-001",
                "kernel:model_eval_runs/shadow/quick-summary-regression",
            ],
            proposed_change="Add a citation-presence check and tighten the final summary format.",
            expected_benefit="Reduce unsupported claim leakage in quick research summaries.",
            risk_assessment="Low runtime risk; possible concision loss measured by regression eval.",
            eval_plan="Run replay, known-bad regression, and shadow scoring before any promotion packet.",
            rollback_plan="Restore research.quick_summary.prompt@v1 and keep candidate traces for audit.",
            authority_required="operator_gate",
            proposer_type="agent",
            proposer_id="system-improvement-worker",
            affected_policy_areas=[],
            data_classes=["public", "internal"],
        )

    def eval_record(self, proposal_id: str) -> SelfImprovementEvalRecord:
        return SelfImprovementEvalRecord(
            eval_id="eval-harness-summary-v2-replay",
            proposal_id=proposal_id,
            eval_type="replay",
            baseline_ref="harness://research.quick_summary.prompt@v1",
            candidate_ref="harness://research.quick_summary.prompt@v2-candidate",
            dataset_refs=[
                "artifact://evals/research/quick-summary/regression-2026-05",
                "artifact://evals/research/quick-summary/known-bad-2026-05",
            ],
            metrics={
                "overall": 0.91,
                "unsupported_claim_rate": 0.0,
                "citation_coverage": 0.98,
                "latency_delta_ms_p95": 120,
            },
            regression_thresholds={
                "unsupported_claim_rate_max": 0.0,
                "citation_coverage_min": 0.95,
                "latency_delta_ms_p95_max": 500,
            },
            failure_examples=[],
            side_effect_safety={
                "reexecuted_side_effects": False,
                "external_intents_reconstructed_only": True,
            },
            status="passed",
        )

    def test_proposal_eval_packet_rollback_and_replay_comparison_are_kernel_owned(self):
        proposal = self.proposal()
        proposal_id = self.si.record_proposal(
            self_improvement_command(
                "self_improvement.proposal.record",
                "proposal-harness-summary-v2",
                requested_by="agent",
                requester_id="system-improvement-worker",
                requested_authority="operator_gate",
                payload={"target_id": proposal.target_id},
            ),
            proposal,
        )
        eval_record = self.eval_record(proposal_id)
        eval_id = self.si.record_eval(
            self_improvement_command("self_improvement.eval.record", "eval-harness-summary-v2-replay"),
            eval_record,
        )
        decision = self.si.promotion_decision(
            proposal=proposal,
            question="Approve the quick-summary harness v2 candidate after replay and known-bad evals?",
            evidence_refs=[eval_id, *proposal.problem_evidence],
            confidence=0.91,
        )
        decision_id = self.store.create_decision(
            self_improvement_command("decision.create", "decision-harness-summary-v2"),
            decision,
        )
        packet = SelfImprovementPromotionPacket(
            packet_id="packet-harness-summary-v2",
            proposal_id=proposal_id,
            decision_id=decision_id,
            recommendation="approve",
            required_authority="operator_gate",
            eval_record_ids=[eval_id],
            evidence_refs=[eval_id, *proposal.problem_evidence],
            risk_flags=["operator_gate_required_before_active_harness_change"],
            gate_packet={
                "decision_type": "system_improvement",
                "proposal_id": proposal_id,
                "authority_route": "operator_gate",
            },
            default_on_timeout="keep_current_behavior",
        )
        packet_id = self.si.create_promotion_packet(
            self_improvement_command("self_improvement.promotion_packet.create", "packet-harness-summary-v2"),
            packet,
        )
        rollback = SelfImprovementRollbackRecord(
            rollback_id="rollback-harness-summary-v2",
            proposal_id=proposal_id,
            packet_id=packet_id,
            previous_ref="harness://research.quick_summary.prompt@v1",
            rollback_reason="Operator chose to restore previous harness after post-promotion regression.",
            receipt_ref="artifact://receipts/self-improvement/rollback-harness-summary-v2",
            receipt_hash=sha256_text("rollback receipt"),
            status="applied",
        )
        rollback_id = self.si.record_rollback(
            self_improvement_command("self_improvement.rollback.record", "rollback-harness-summary-v2"),
            rollback,
        )
        comparison = self.si.compare_replay_to_projection(
            self_improvement_command("self_improvement.replay.compare", "compare-self-improvement"),
        )

        self.assertEqual(rollback_id, rollback.rollback_id)
        self.assertTrue(comparison.matches, comparison.mismatches)
        self.assertEqual(comparison.projection_proposals[0]["status"], "rolled_back")
        self.assertEqual(comparison.projection_eval_records[0]["authority_effect"], "evidence_only")
        self.assertEqual(comparison.projection_promotion_packets[0]["required_authority"], "operator_gate")

    def test_patch_review_packet_is_durable_review_only_and_replayable(self):
        proposal = self.proposal()
        proposal_id = self.si.record_proposal(
            self_improvement_command("self_improvement.proposal.record", "proposal-patch-review"),
            proposal,
        )
        eval_id = self.si.record_eval(
            self_improvement_command("self_improvement.eval.record", "eval-patch-review"),
            self.eval_record(proposal_id),
        )
        decision = self.si.promotion_decision(
            proposal=proposal,
            question="Prepare a patch review packet for the quick-summary harness candidate?",
            evidence_refs=[eval_id, *proposal.problem_evidence],
            confidence=0.91,
        )
        decision_id = self.store.create_decision(
            self_improvement_command("decision.create", "decision-patch-review"),
            decision,
        )
        packet_id = self.si.create_promotion_packet(
            self_improvement_command("self_improvement.promotion_packet.create", "packet-patch-review"),
            SelfImprovementPromotionPacket(
                packet_id="packet-patch-review",
                proposal_id=proposal_id,
                decision_id=decision_id,
                recommendation="approve",
                required_authority="operator_gate",
                eval_record_ids=[eval_id],
                evidence_refs=[eval_id, *proposal.problem_evidence],
                risk_flags=["operator_gate_required_before_active_harness_change"],
                gate_packet={"decision_type": "system_improvement", "proposal_id": proposal_id},
                default_on_timeout="keep_current_behavior",
            ),
        )
        patch_packet = SelfImprovementPatchReviewPacket(
            patch_packet_id="patch-review-harness-summary-v2",
            proposal_id=proposal_id,
            promotion_packet_id=packet_id,
            target_ref="harness://research.quick_summary.prompt@v1",
            patch_ref="artifact://patches/research/quick-summary-v2.diff",
            patch_hash=sha256_text("diff --git a/research.prompt b/research.prompt"),
            changed_paths=["skills/research_domain/skill.py", "tests/test_skills/test_research_domain.py"],
            apply_instructions="Operator may apply this patch in a clean branch after reviewing the diff.",
            verification_plan="Run focused research-domain tests, known-bad replay, then full suite.",
            rollback_ref="harness://research.quick_summary.prompt@v1",
            evidence_refs=[eval_id, "artifact://patches/research/quick-summary-v2.diff"],
            blocked_autonomous_actions=[
                "active_behavior_mutation",
                "autonomous_patch_application",
                "frontier_route_update",
                "external_side_effect_reexecution",
            ],
            required_authority="operator_gate",
        )
        patch_packet_id = self.si.prepare_patch_review_packet(
            self_improvement_command(
                "self_improvement.patch_review.prepare",
                "patch-review-harness-summary-v2",
                requested_by="kernel",
                requester_id="self-improvement-review",
                requested_authority="operator_gate",
            ),
            patch_packet,
        )
        comparison = self.si.compare_replay_to_projection(
            self_improvement_command("self_improvement.replay.compare", "compare-self-improvement-patch-review"),
        )

        self.assertEqual(patch_packet_id, patch_packet.patch_packet_id)
        self.assertTrue(comparison.matches, comparison.mismatches)
        self.assertEqual(comparison.projection_patch_review_packets[0]["authority_effect"], "review_only")
        self.assertEqual(comparison.projection_patch_review_packets[0]["required_authority"], "operator_gate")
        self.assertIn(
            "autonomous_patch_application",
            comparison.projection_patch_review_packets[0]["blocked_autonomous_actions"],
        )

    def test_workers_cannot_prepare_patch_review_packets(self):
        proposal = self.proposal()
        proposal_id = self.si.record_proposal(
            self_improvement_command("self_improvement.proposal.record", "proposal-worker-patch-review"),
            proposal,
        )
        eval_id = self.si.record_eval(
            self_improvement_command("self_improvement.eval.record", "eval-worker-patch-review"),
            self.eval_record(proposal_id),
        )
        decision = self.si.promotion_decision(proposal=proposal, question="Approve candidate?", evidence_refs=[eval_id])
        decision_id = self.store.create_decision(
            self_improvement_command("decision.create", "decision-worker-patch-review"),
            decision,
        )
        packet_id = self.si.create_promotion_packet(
            self_improvement_command("self_improvement.promotion_packet.create", "packet-worker-patch-review"),
            SelfImprovementPromotionPacket(
                proposal_id=proposal_id,
                decision_id=decision_id,
                recommendation="approve",
                required_authority="operator_gate",
                eval_record_ids=[eval_id],
                evidence_refs=[eval_id],
                risk_flags=[],
                gate_packet={"decision_type": "system_improvement"},
                default_on_timeout="keep_current_behavior",
            ),
        )
        with self.assertRaises(PermissionError):
            self.si.prepare_patch_review_packet(
                self_improvement_command(
                    "self_improvement.patch_review.prepare",
                    "worker-patch-review",
                    requested_by="agent",
                    requester_id="worker",
                    requested_authority="operator_gate",
                ),
                SelfImprovementPatchReviewPacket(
                    proposal_id=proposal_id,
                    promotion_packet_id=packet_id,
                    target_ref="harness://research.quick_summary.prompt@v1",
                    patch_ref="artifact://patches/research/quick-summary-v2.diff",
                    patch_hash=sha256_text("patch"),
                    changed_paths=["skills/research_domain/skill.py"],
                    apply_instructions="Apply after operator approval.",
                    verification_plan="Run tests.",
                    rollback_ref="harness://research.quick_summary.prompt@v1",
                    evidence_refs=[eval_id],
                    blocked_autonomous_actions=[
                        "active_behavior_mutation",
                        "autonomous_patch_application",
                        "frontier_route_update",
                        "external_side_effect_reexecution",
                    ],
                    required_authority="operator_gate",
                ),
            )

    def test_workers_cannot_downgrade_pinned_policy_or_create_promotion_packets(self):
        pinned = SelfImprovementProposal(
            proposal_id="proposal-policy-bypass",
            target_type="policy",
            target_id="capability-broker",
            problem_evidence=["artifact://incident/bypass-attempt"],
            proposed_change="Relax capability broker checks for convenience.",
            expected_benefit="Less friction.",
            risk_assessment="Unsafe.",
            eval_plan="None.",
            rollback_plan="Restore policy.",
            authority_required="single_agent",
            proposer_type="agent",
            proposer_id="worker",
            affected_policy_areas=["capability_broker"],
            data_classes=["internal"],
        )
        with self.assertRaises(PermissionError):
            self.si.record_proposal(
                self_improvement_command(
                    "self_improvement.proposal.record",
                    "proposal-policy-bypass",
                    requested_by="agent",
                    requester_id="worker",
                    requested_authority="single_agent",
                ),
                pinned,
            )

        proposal = self.proposal()
        proposal_id = self.si.record_proposal(
            self_improvement_command("self_improvement.proposal.record", "proposal-worker-packet"),
            proposal,
        )
        eval_id = self.si.record_eval(
            self_improvement_command("self_improvement.eval.record", "eval-worker-packet"),
            self.eval_record(proposal_id),
        )
        decision = self.si.promotion_decision(
            proposal=proposal,
            question="Approve candidate?",
            evidence_refs=[eval_id],
            confidence=0.91,
        )
        decision_id = self.store.create_decision(
            self_improvement_command("decision.create", "decision-worker-packet"),
            decision,
        )
        packet = SelfImprovementPromotionPacket(
            proposal_id=proposal_id,
            decision_id=decision_id,
            recommendation="approve",
            required_authority="operator_gate",
            eval_record_ids=[eval_id],
            evidence_refs=[eval_id],
            risk_flags=[],
            gate_packet={"decision_type": "system_improvement"},
            default_on_timeout="keep_current_behavior",
        )
        with self.assertRaises(PermissionError):
            self.si.create_promotion_packet(
                self_improvement_command(
                    "self_improvement.promotion_packet.create",
                    "worker-promotion-packet",
                    requested_by="agent",
                    requester_id="worker",
                    requested_authority="operator_gate",
                ),
                packet,
            )

    def test_eval_replay_must_not_reexecute_side_effects(self):
        proposal = self.proposal()
        proposal_id = self.si.record_proposal(
            self_improvement_command("self_improvement.proposal.record", "proposal-side-effect-safety"),
            proposal,
        )
        unsafe = self.eval_record(proposal_id)
        object.__setattr__(unsafe, "side_effect_safety", {"reexecuted_side_effects": True})

        with self.assertRaises(PermissionError):
            self.si.record_eval(
                self_improvement_command("self_improvement.eval.record", "eval-side-effect-unsafe"),
                unsafe,
            )

    def test_evidence_pipeline_creates_operator_gated_portfolio_and_compares_replay(self):
        run = self.si.run_evidence_pipeline(
            self_improvement_command(
                "self_improvement.evidence_pipeline.run",
                "pipeline-readiness",
                requested_by="kernel",
                requester_id="self-improvement-evidence-pipeline",
                requested_authority="operator_gate",
            ),
            as_of="2026-05-16T00:00:00+00:00",
            signals=[
                {
                    "source": "replay_readiness",
                    "target_type": "eval",
                    "target_id": "replay.corpus.activation",
                    "evidence_refs": ["artifact://runtime/replay_readiness_report.json"],
                    "proposed_change": "Grow replay corpus before broader harness promotion.",
                    "expected_benefit": "Representative promotion evidence.",
                    "risk_assessment": "Evidence-only.",
                    "eval_plan": "Replay known-bad and regression traces.",
                    "rollback_plan": "Keep current harness active.",
                    "metrics": {"overall": 0.42, "known_bad_source_traces": 2},
                    "eval_status": "needs_more_data",
                    "recommendation": "needs_more_data",
                },
                {
                    "source": "harness_candidate",
                    "target_type": "harness",
                    "target_id": "research_domain.summary.prompt",
                    "evidence_refs": ["artifact://runtime/harness_candidate_report.json"],
                    "proposed_change": "Promote citation-presence prompt candidate after operator review.",
                    "expected_benefit": "Fewer unsupported claims.",
                    "risk_assessment": "Shadow only until gated.",
                    "eval_plan": "Replay and shadow comparison.",
                    "rollback_plan": "Restore current prompt.",
                    "eval_type": "shadow",
                    "metrics": {"overall": 0.91, "unsupported_claim_rate": 0.0},
                    "eval_status": "passed",
                    "recommendation": "approve",
                },
            ],
        )

        self.assertEqual(run.status, "recorded")
        self.assertEqual(len(run.proposal_ids), 2)
        self.assertEqual(len(run.eval_record_ids), 2)
        self.assertEqual(len(run.promotion_packet_ids), 2)
        self.assertEqual(run.portfolio_items[1]["recommendation"], "approve")
        self.assertIn("autonomous_model_promotion", run.blocked_autonomous_actions)

        comparison = self.si.compare_replay_to_projection(
            self_improvement_command(
                "self_improvement.replay.compare",
                "compare-pipeline",
                requested_by="kernel",
            )
        )
        self.assertTrue(comparison.matches, comparison.mismatches)
        self.assertEqual(comparison.projection_pipeline_runs[0]["run_id"], run.run_id)

    def test_evidence_pipeline_is_kernel_owned(self):
        with self.assertRaises(PermissionError):
            self.si.run_evidence_pipeline(
                self_improvement_command(
                    "self_improvement.evidence_pipeline.run",
                    "agent-pipeline",
                    requested_by="agent",
                    requester_id="worker",
                    requested_authority="operator_gate",
                ),
                as_of="2026-05-16T00:00:00+00:00",
                signals=[],
            )


if __name__ == "__main__":
    unittest.main()
