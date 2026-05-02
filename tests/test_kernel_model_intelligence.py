from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from kernel import (
    Command,
    HoldoutUseRecord,
    KernelModelIntelligence,
    KernelStore,
    LocalOffloadEvalSet,
    ModelCandidate,
    ModelDemotionRecord,
    ModelEvalRun,
    ModelPromotionDecisionPacket,
    ModelRouteDecision,
    ModelTaskClassRecord,
    ShadowExecutionRecord,
    ShadowOutputArtifact,
    ShadowOutputSample,
    model_intelligence_command,
)
from kernel.records import sha256_text


class KernelModelIntelligenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = KernelStore(Path(self.tmp.name) / "kernel.db")
        self.mi = KernelModelIntelligence(self.store)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def candidate(self) -> ModelCandidate:
        return ModelCandidate(
            model_id="mlx/qwen3-small-test",
            provider="mlx",
            access_mode="local",
            source_ref="hf://example/qwen3-small-test@rev1",
            artifact_hash=sha256_text("model-artifact"),
            license="apache-2.0",
            commercial_use="allowed",
            terms_verified_at="2026-05-02T00:00:00Z",
            context_window=32_768,
            modalities=["text", "tool_use"],
            hardware_fit="good",
            sandbox_profile="mlx-readonly",
            data_residency="local_only",
            cost_profile={"marginal_usd_per_1k_tasks": "0.00"},
            latency_profile={"p50_ms": 7000, "p95_ms": 19000},
            routing_metadata={"prompt_format": "chatml", "tool_use": "disabled"},
            promotion_state="shadow",
        )

    def replacement_candidate(self) -> ModelCandidate:
        candidate = self.candidate()
        return ModelCandidate(
            model_id="mlx/qwen3-replacement-test",
            provider=candidate.provider,
            access_mode=candidate.access_mode,
            source_ref="hf://example/qwen3-replacement-test@rev2",
            artifact_hash=sha256_text("replacement-model-artifact"),
            license=candidate.license,
            commercial_use=candidate.commercial_use,
            terms_verified_at=candidate.terms_verified_at,
            context_window=candidate.context_window,
            modalities=candidate.modalities,
            hardware_fit=candidate.hardware_fit,
            sandbox_profile=candidate.sandbox_profile,
            data_residency=candidate.data_residency,
            cost_profile=candidate.cost_profile,
            latency_profile={"p50_ms": 6500, "p95_ms": 17000},
            routing_metadata={"prompt_format": "chatml", "tool_use": "disabled", "route_version": "replacement-v2"},
            promotion_state="shadow",
        )

    def eval_set(self, policy_id: str) -> LocalOffloadEvalSet:
        return LocalOffloadEvalSet(
            task_class="quick_research_summarization",
            dataset_version="seed-2026-05-02",
            artifact_ref="artifact://evals/model-intelligence/quick-research/seed-2026-05-02",
            split_counts={
                "development": 24,
                "regression": 18,
                "known_bad": 12,
                "frozen_holdout": 12,
            },
            data_classes=["public", "internal"],
            retention_policy="retain-180d-metadata-only",
            scorer_profile={
                "deterministic_checks": ["citation_coverage", "unsupported_claims"],
                "judge_calibration": "operator-labeled-seed",
            },
            holdout_policy_id=policy_id,
        )

    def eval_run(self, candidate: ModelCandidate, eval_set: LocalOffloadEvalSet) -> ModelEvalRun:
        return ModelEvalRun(
            model_id=candidate.model_id,
            task_class="quick_research_summarization",
            dataset_version=eval_set.dataset_version,
            eval_set_id=eval_set.eval_set_id,
            route_version="quick-summary@2026-05-02.1",
            route_metadata={
                "prompt_format": "chatml",
                "system_prompt_hash": sha256_text("summarize-with-citations-v1"),
                "runtime": "mlx",
                "quantization": "q4_k_m",
                "tool_use": "disabled",
            },
            sample_count=66,
            quality_score=0.84,
            reliability_score=0.97,
            latency_p50_ms=7_200,
            latency_p95_ms=18_800,
            cost_per_1k_tasks=Decimal("0.00"),
            aggregate_scores={
                "overall": 0.86,
                "quality": 0.84,
                "reliability": 0.97,
                "latency": 0.91,
                "cost": 1.0,
            },
            failure_categories=["citation_gap", "format_drift"],
            failure_modes=["1 unsupported summary sentence", "2 outputs exceeded target bullet count"],
            confidence={
                "score": 0.78,
                "method": "wilson_interval_plus_operator_labeled_seed",
                "quality_ci95": [0.74, 0.90],
                "reliability_ci95": [0.92, 0.99],
            },
            frozen_holdout_result={
                "split": "frozen_holdout",
                "sample_count": 12,
                "quality_score": 0.83,
                "reliability_score": 0.96,
                "latency_p95_ms": 19_100,
                "failure_categories": ["citation_gap"],
                "artifact_ref": "artifact://evals/model-intelligence/quick-research/seed-2026-05-02/frozen-result",
            },
            verdict="shadow",
        )

    def seed_registry(self) -> tuple[ModelCandidate, str, LocalOffloadEvalSet]:
        self.mi.register_seed_task_classes()
        policy = self.mi.seed_holdout_policy("quick_research_summarization", "seed-2026-05-02")
        policy_id = self.mi.create_holdout_policy(
            model_intelligence_command("model.holdout_policy.create", "holdout-policy"),
            policy,
        )
        eval_set = self.eval_set(policy_id)
        self.mi.register_eval_set(
            model_intelligence_command("model.eval_set.register", "eval-set"),
            eval_set,
        )
        candidate = self.candidate()
        self.mi.register_candidate(
            model_intelligence_command("model.candidate.register", "candidate"),
            candidate,
        )
        return candidate, policy_id, eval_set

    def create_promotion_decision(
        self,
        decision_id: str,
        candidate: ModelCandidate,
        eval_set: LocalOffloadEvalSet,
        *,
        recommendation: str = "promote",
        confidence: float = 0.82,
    ) -> str:
        decision = self.mi.promotion_decision(
            model_id=candidate.model_id,
            task_class=eval_set.task_class,
            proposed_routing_role="research_local",
            question=f"Promote {candidate.model_id} for {eval_set.task_class}?",
            recommendation=recommendation,
            confidence=confidence,
            evidence_refs=[f"kernel:local_offload_eval_sets/{eval_set.eval_set_id}"],
            gate_packet={
                "decision_type": "model_promotion",
                "authority_route": "operator_gate",
                "default_on_timeout": "keep_current_route",
            },
            risk_flags=["seed_pre_hermes_packet"],
        )
        object.__setattr__(decision, "decision_id", decision_id)
        return self.mi.create_decision(
            model_intelligence_command(
                "decision.record",
                f"decision-{decision_id}",
                {"decision_id": decision_id},
            ),
            decision,
        )

    def test_seed_task_eval_holdout_and_candidate_records_are_replayable(self):
        candidate, policy_id, eval_set = self.seed_registry()

        with self.store.connect() as conn:
            task_classes = conn.execute("SELECT COUNT(*) FROM model_task_classes").fetchone()[0]
            policy_row = conn.execute(
                "SELECT promotion_requires_decision FROM model_holdout_policies WHERE policy_id=?",
                (policy_id,),
            ).fetchone()
            eval_row = conn.execute(
                "SELECT task_class, status FROM local_offload_eval_sets WHERE eval_set_id=?",
                (eval_set.eval_set_id,),
            ).fetchone()
            events = [row["event_type"] for row in conn.execute("SELECT event_type FROM events ORDER BY event_seq")]

        self.assertEqual(task_classes, 3)
        self.assertEqual(policy_row["promotion_requires_decision"], 1)
        self.assertEqual(eval_row["task_class"], "quick_research_summarization")
        self.assertEqual(eval_row["status"], "active")
        self.assertEqual(
            events,
            [
                "model_task_class_registered",
                "model_task_class_registered",
                "model_task_class_registered",
                "model_holdout_policy_created",
                "local_offload_eval_set_registered",
                "model_candidate_registered",
            ],
        )
        replay = self.store.replay_critical_state()
        self.assertIn("quick_research_summarization", replay.model_task_classes)
        self.assertEqual(replay.holdout_policies[policy_id]["promotion_requires_decision"], True)
        self.assertEqual(replay.local_offload_eval_sets[eval_set.eval_set_id]["split_counts"]["frozen_holdout"], 12)
        self.assertEqual(replay.model_candidates[candidate.model_id]["promotion_state"], "shadow")

    def test_holdout_governance_blocks_development_and_self_scoring(self):
        _, policy_id, eval_set = self.seed_registry()
        blocked_dev = HoldoutUseRecord(
            policy_id=policy_id,
            eval_set_id=eval_set.eval_set_id,
            task_class="quick_research_summarization",
            dataset_version=eval_set.dataset_version,
            requester_id="worker-a",
            requester_change_ref="worker-a/route-prompt-change",
            purpose="development",
            verdict="blocked",
            reason="development work uses mutable development split only",
        )
        self.mi.record_holdout_use(
            model_intelligence_command("model.holdout_use.record", "holdout-dev-block"),
            blocked_dev,
        )

        self_scoring = HoldoutUseRecord(
            policy_id=policy_id,
            eval_set_id=eval_set.eval_set_id,
            task_class="quick_research_summarization",
            dataset_version=eval_set.dataset_version,
            requester_id="worker-a",
            requester_change_ref="worker-a/route-prompt-change",
            purpose="promotion_gate",
            verdict="allowed",
            reason="bad request",
            decision_id="decision-1",
        )
        with self.assertRaises(PermissionError):
            self.mi.record_holdout_use(
                model_intelligence_command("model.holdout_use.record", "holdout-self-score"),
                self_scoring,
            )

        no_decision = HoldoutUseRecord(
            policy_id=policy_id,
            eval_set_id=eval_set.eval_set_id,
            task_class="quick_research_summarization",
            dataset_version=eval_set.dataset_version,
            requester_id="scoring-service",
            requester_change_ref=None,
            purpose="promotion_gate",
            verdict="allowed",
            reason="missing decision",
        )
        with self.assertRaises(PermissionError):
            self.mi.record_holdout_use(
                model_intelligence_command("model.holdout_use.record", "holdout-no-decision"),
                no_decision,
            )

        replay = self.store.replay_critical_state()
        self.assertEqual(next(iter(replay.holdout_use_records.values()))["verdict"], "blocked")

    def test_eval_run_records_scores_route_version_confidence_and_holdout_without_promotion(self):
        candidate, _, eval_set = self.seed_registry()
        eval_run = self.eval_run(candidate, eval_set)

        eval_run_id = self.mi.record_eval_run(
            model_intelligence_command("model.eval_run.record", "eval-run-shadow"),
            eval_run,
        )

        with self.store.connect() as conn:
            row = conn.execute(
                """
                SELECT model_id, route_version, quality_score, reliability_score,
                       cost_per_1k_tasks, verdict, authority_effect
                FROM model_eval_runs
                WHERE eval_run_id=?
                """,
                (eval_run_id,),
            ).fetchone()
            candidate_row = conn.execute(
                "SELECT promotion_state FROM model_candidates WHERE model_id=?",
                (candidate.model_id,),
            ).fetchone()
            event_type = conn.execute(
                "SELECT event_type FROM events WHERE entity_id=?",
                (eval_run_id,),
            ).fetchone()["event_type"]

        self.assertEqual(row["model_id"], candidate.model_id)
        self.assertEqual(row["route_version"], "quick-summary@2026-05-02.1")
        self.assertEqual(row["quality_score"], 0.84)
        self.assertEqual(row["reliability_score"], 0.97)
        self.assertEqual(row["cost_per_1k_tasks"], "0.00")
        self.assertEqual(row["verdict"], "shadow")
        self.assertEqual(row["authority_effect"], "evidence_only")
        self.assertEqual(candidate_row["promotion_state"], "shadow")
        self.assertEqual(event_type, "model_eval_run_recorded")

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.model_eval_runs[eval_run_id]["aggregate_scores"]["overall"], 0.86)
        self.assertEqual(replay.model_eval_runs[eval_run_id]["confidence"]["score"], 0.78)
        self.assertEqual(replay.model_eval_runs[eval_run_id]["frozen_holdout_result"]["sample_count"], 12)
        self.assertEqual(replay.model_candidates[candidate.model_id]["promotion_state"], "shadow")

    def test_eval_run_requires_holdout_capture_and_decision_for_decision_support(self):
        candidate, _, eval_set = self.seed_registry()
        too_small_holdout = self.eval_run(candidate, eval_set)
        object.__setattr__(
            too_small_holdout,
            "frozen_holdout_result",
            {"split": "frozen_holdout", "sample_count": 6, "quality_score": 0.8, "reliability_score": 0.9},
        )
        with self.assertRaises(ValueError):
            self.mi.record_eval_run(
                model_intelligence_command("model.eval_run.record", "eval-run-small-holdout"),
                too_small_holdout,
            )

        decision_support = self.eval_run(candidate, eval_set)
        object.__setattr__(decision_support, "verdict", "supports_decision")
        with self.assertRaises(PermissionError):
            self.mi.record_eval_run(
                model_intelligence_command("model.eval_run.record", "eval-run-missing-decision"),
                decision_support,
            )

    def test_routing_decision_records_shadow_evidence_without_promotion_authority(self):
        candidate, _, eval_set = self.seed_registry()
        route = ModelRouteDecision(
            task_id="task-quick-summary-1",
            task_class="quick_research_summarization",
            data_class="internal",
            risk_level="low",
            selected_route="shadow",
            selected_model_id=None,
            candidate_model_id=candidate.model_id,
            eval_set_id=eval_set.eval_set_id,
            reasons=[
                "candidate is not promoted",
                "shadow mode records local-offload evidence without affecting production output",
            ],
            required_authority="operator_gate",
            decision_id=None,
            local_offload_estimate={"eligible": True, "estimated_savings_usd_per_1k": "4.20"},
            frontier_fallback={"provider": "openai", "reason": "production route remains frontier"},
        )
        route_id = self.mi.record_route_decision(
            model_intelligence_command("model.route_decision.record", "route-shadow"),
            route,
        )

        local_route = ModelRouteDecision(
            task_id="task-quick-summary-2",
            task_class="quick_research_summarization",
            data_class="internal",
            risk_level="low",
            selected_route="local",
            selected_model_id=candidate.model_id,
            candidate_model_id=None,
            eval_set_id=eval_set.eval_set_id,
            reasons=["should fail because promotion is external to this lane"],
            required_authority="operator_gate",
            decision_id="decision-2",
            local_offload_estimate={},
            frontier_fallback={},
        )
        with self.assertRaises(PermissionError):
            self.mi.record_route_decision(
                model_intelligence_command("model.route_decision.record", "route-local-unpromoted"),
                local_route,
            )

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.model_route_decisions[route_id]["selected_route"], "shadow")
        self.assertEqual(replay.model_route_decisions[route_id]["candidate_model_id"], candidate.model_id)

    def test_shadow_execution_adapter_records_eval_evidence_without_changing_production_route(self):
        candidate, _, eval_set = self.seed_registry()
        record = ShadowExecutionRecord(
            task_id="task-shadow-quick-summary-1",
            task_class="quick_research_summarization",
            dataset_version=eval_set.dataset_version,
            eval_set_id=eval_set.eval_set_id,
            candidate_model_id=candidate.model_id,
            data_class="internal",
            risk_level="low",
            production_route={
                "selected_route": "frontier",
                "selected_model_id": "openai/gpt-frontier-prod",
                "route_version": "frontier-summary@2026-05-02.1",
                "route_effect": "production",
                "cost_usd": "0.006",
                "artifact_ref": "artifact://prod/routes/task-shadow-quick-summary-1",
            },
            candidate_route_version="quick-summary-shadow@2026-05-02.1",
            candidate_route_metadata={
                "prompt_format": "chatml",
                "system_prompt_hash": sha256_text("shadow-summary-v1"),
                "runtime": "mlx",
                "quantization": "q4_k_m",
                "tool_use": "disabled",
            },
            samples=[
                ShadowOutputSample(
                    sample_id="sample-1",
                    input_ref="artifact://inputs/task-shadow-quick-summary-1",
                    production_output=ShadowOutputArtifact(
                        artifact_uri="artifact://prod/outputs/task-shadow-quick-summary-1/sample-1",
                        data_class="internal",
                        content_hash=sha256_text("production output 1"),
                        retention_policy="retain-90d-shadow-metadata",
                        deletion_policy="delete-with-project",
                        source_notes="production output reference only",
                    ),
                    shadow_output=ShadowOutputArtifact(
                        artifact_uri="artifact://shadow/outputs/task-shadow-quick-summary-1/sample-1",
                        data_class="internal",
                        content_hash=sha256_text("shadow output 1"),
                        retention_policy="retain-90d-shadow-metadata",
                        deletion_policy="delete-with-project",
                        source_notes="candidate output scored offline",
                    ),
                    quality_score=0.82,
                    reliability_score=0.96,
                    latency_ms=8_200,
                    cost_usd=Decimal("0.0000"),
                    failure_categories=["citation_gap"],
                    failure_modes=["missed one source qualifier"],
                    disagreement={"has_disagreement": True, "severity": "low"},
                ),
                ShadowOutputSample(
                    sample_id="sample-2",
                    input_ref="artifact://inputs/task-shadow-quick-summary-1",
                    production_output=ShadowOutputArtifact(
                        artifact_uri="artifact://prod/outputs/task-shadow-quick-summary-1/sample-2",
                        data_class="internal",
                        content_hash=sha256_text("production output 2"),
                        retention_policy="retain-90d-shadow-metadata",
                        deletion_policy="delete-with-project",
                    ),
                    shadow_output=ShadowOutputArtifact(
                        artifact_uri="artifact://shadow/outputs/task-shadow-quick-summary-1/sample-2",
                        data_class="internal",
                        content_hash=sha256_text("shadow output 2"),
                        retention_policy="retain-90d-shadow-metadata",
                        deletion_policy="delete-with-project",
                    ),
                    quality_score=0.86,
                    reliability_score=0.98,
                    latency_ms=7_600,
                    cost_usd=Decimal("0.0000"),
                    failure_categories=[],
                    failure_modes=[],
                    disagreement={"has_disagreement": False},
                ),
            ],
            execution_metadata={
                "executor": "shadow-output-adapter-test",
                "production_state_effect": "none",
                "project_state_effect": "none",
            },
        )

        result = self.mi.record_shadow_execution(
            model_intelligence_command("model.shadow_execution.record", "shadow-execution-1"),
            record,
        )

        with self.store.connect() as conn:
            route = conn.execute(
                """
                SELECT selected_route, selected_model_id, candidate_model_id, frontier_fallback_json
                FROM model_route_decisions
                WHERE route_decision_id=?
                """,
                (result.route_decision_id,),
            ).fetchone()
            eval_row = conn.execute(
                """
                SELECT model_id, baseline_model_id, route_version, route_metadata_json,
                       sample_count, quality_score, reliability_score, verdict, authority_effect
                FROM model_eval_runs
                WHERE eval_run_id=?
                """,
                (result.eval_run_id,),
            ).fetchone()
            candidate_row = conn.execute(
                "SELECT promotion_state FROM model_candidates WHERE model_id=?",
                (candidate.model_id,),
            ).fetchone()
            artifacts = conn.execute("SELECT COUNT(*) FROM artifact_refs").fetchone()[0]
            local_routes = conn.execute(
                "SELECT COUNT(*) FROM model_route_decisions WHERE selected_route='local'"
            ).fetchone()[0]

        self.assertEqual(route["selected_route"], "shadow")
        self.assertIsNone(route["selected_model_id"])
        self.assertEqual(route["candidate_model_id"], candidate.model_id)
        self.assertIn("openai/gpt-frontier-prod", route["frontier_fallback_json"])
        self.assertEqual(eval_row["model_id"], candidate.model_id)
        self.assertIsNone(eval_row["baseline_model_id"])
        self.assertEqual(eval_row["route_version"], "quick-summary-shadow@2026-05-02.1")
        self.assertEqual(eval_row["sample_count"], 2)
        self.assertAlmostEqual(eval_row["quality_score"], 0.84)
        self.assertAlmostEqual(eval_row["reliability_score"], 0.97)
        self.assertEqual(eval_row["verdict"], "shadow")
        self.assertEqual(eval_row["authority_effect"], "evidence_only")
        self.assertIn("openai/gpt-frontier-prod", eval_row["route_metadata_json"])
        self.assertEqual(candidate_row["promotion_state"], "shadow")
        self.assertEqual(artifacts, 4)
        self.assertEqual(local_routes, 0)

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.model_route_decisions[result.route_decision_id]["selected_route"], "shadow")
        self.assertEqual(replay.model_eval_runs[result.eval_run_id]["route_metadata"]["authority_effect"], "evidence_only")
        self.assertEqual(len(result.artifact_ids), 4)

    def test_shadow_execution_rejects_candidate_as_production_route(self):
        candidate, _, eval_set = self.seed_registry()
        record = ShadowExecutionRecord(
            task_id="task-shadow-bad",
            task_class="quick_research_summarization",
            dataset_version=eval_set.dataset_version,
            eval_set_id=eval_set.eval_set_id,
            candidate_model_id=candidate.model_id,
            data_class="internal",
            risk_level="low",
            production_route={"selected_model_id": candidate.model_id, "route_effect": "production"},
            candidate_route_version="quick-summary-shadow@2026-05-02.1",
            candidate_route_metadata={"runtime": "mlx"},
            samples=[
                ShadowOutputSample(
                    sample_id="sample-bad",
                    input_ref="artifact://inputs/bad",
                    production_output=ShadowOutputArtifact(
                        artifact_uri="artifact://prod/bad",
                        data_class="internal",
                        content_hash=sha256_text("prod bad"),
                        retention_policy="retain-90d",
                        deletion_policy="delete-with-project",
                    ),
                    shadow_output=ShadowOutputArtifact(
                        artifact_uri="artifact://shadow/bad",
                        data_class="internal",
                        content_hash=sha256_text("shadow bad"),
                        retention_policy="retain-90d",
                        deletion_policy="delete-with-project",
                    ),
                    quality_score=0.8,
                    reliability_score=0.9,
                    latency_ms=100,
                    cost_usd=Decimal("0"),
                    failure_categories=[],
                    failure_modes=[],
                    disagreement={"has_disagreement": False},
                )
            ],
        )
        with self.assertRaises(ValueError):
            self.mi.record_shadow_execution(
                model_intelligence_command("model.shadow_execution.record", "shadow-execution-bad"),
                record,
            )

    def test_promotion_decision_packet_is_decision_facing_and_does_not_assign_role(self):
        candidate, policy_id, eval_set = self.seed_registry()
        decision_id = "decision-model-promo-1"
        self.create_promotion_decision(decision_id, candidate, eval_set)
        holdout_use = HoldoutUseRecord(
            policy_id=policy_id,
            eval_set_id=eval_set.eval_set_id,
            task_class="quick_research_summarization",
            dataset_version=eval_set.dataset_version,
            requester_id="scoring-service",
            requester_change_ref=None,
            purpose="promotion_gate",
            verdict="allowed",
            reason="operator-gated scoring service evaluated frozen holdout",
            decision_id=decision_id,
        )
        holdout_use_id = self.mi.record_holdout_use(
            model_intelligence_command("model.holdout_use.record", "holdout-promotion-allowed"),
            holdout_use,
        )
        eval_run = self.eval_run(candidate, eval_set)
        object.__setattr__(eval_run, "verdict", "supports_decision")
        object.__setattr__(eval_run, "decision_id", decision_id)
        object.__setattr__(eval_run, "confidence", {**eval_run.confidence, "score": 0.82})
        object.__setattr__(
            eval_run,
            "frozen_holdout_result",
            {**eval_run.frozen_holdout_result, "confidence_score": 0.81},
        )
        eval_run_id = self.mi.record_eval_run(
            model_intelligence_command("model.eval_run.record", "eval-run-decision-support"),
            eval_run,
        )

        packet = self.mi.promotion_packet(
            model_id=candidate.model_id,
            task_class="quick_research_summarization",
            proposed_routing_role="research_local",
            decision_id=decision_id,
            eval_run_ids=[eval_run_id],
            holdout_use_ids=[holdout_use_id],
            evidence_refs=[
                f"kernel:model_eval_runs/{eval_run_id}",
                f"kernel:model_holdout_use_records/{holdout_use_id}",
                eval_run.frozen_holdout_result["artifact_ref"],
            ],
            frozen_holdout_confidence=0.81,
            confidence_threshold=0.80,
            gate_packet={
                "decision_type": "model_promotion",
                "authority_route": "operator_gate",
                "proposed_routing_role": "research_local",
                "role_assignment_effect": "none_until_operator_decision",
            },
            risk_flags=["seed_pre_hermes_packet"],
        )
        packet_id = self.mi.create_promotion_decision_packet(
            model_intelligence_command(
                "model.promotion_decision_packet.create",
                "promotion-packet",
                {"decision_id": decision_id},
            ),
            packet,
        )

        with self.store.connect() as conn:
            row = conn.execute(
                """
                SELECT model_id, task_class, proposed_routing_role, recommendation,
                       required_authority, frozen_holdout_confidence, confidence_threshold
                FROM model_promotion_decision_packets
                WHERE packet_id=?
                """,
                (packet_id,),
            ).fetchone()
            candidate_row = conn.execute(
                "SELECT promotion_state FROM model_candidates WHERE model_id=?",
                (candidate.model_id,),
            ).fetchone()
            local_routes = conn.execute(
                "SELECT COUNT(*) FROM model_route_decisions WHERE selected_route='local'"
            ).fetchone()[0]
            decision_row = conn.execute(
                "SELECT decision_type, required_authority, status FROM decisions WHERE decision_id=?",
                (decision_id,),
            ).fetchone()

        self.assertEqual(decision_row["decision_type"], "model_promotion")
        self.assertEqual(decision_row["required_authority"], "operator_gate")
        self.assertEqual(decision_row["status"], "proposed")
        self.assertEqual(row["model_id"], candidate.model_id)
        self.assertEqual(row["task_class"], "quick_research_summarization")
        self.assertEqual(row["proposed_routing_role"], "research_local")
        self.assertEqual(row["recommendation"], "promote")
        self.assertEqual(row["required_authority"], "operator_gate")
        self.assertEqual(row["frozen_holdout_confidence"], 0.81)
        self.assertEqual(row["confidence_threshold"], 0.80)
        self.assertEqual(candidate_row["promotion_state"], "shadow")
        self.assertEqual(local_routes, 0)

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.decisions[decision_id]["decision_type"], "model_promotion")
        self.assertEqual(
            replay.model_promotion_decision_packets[packet_id]["gate_packet"]["authority_route"],
            "operator_gate",
        )
        self.assertEqual(replay.model_candidates[candidate.model_id]["promotion_state"], "shadow")

    def test_promotion_decision_packet_enforces_authority_evidence_and_holdout_confidence(self):
        candidate, policy_id, eval_set = self.seed_registry()
        decision_id = "decision-model-promo-2"
        self.create_promotion_decision(decision_id, candidate, eval_set)
        holdout_use = HoldoutUseRecord(
            policy_id=policy_id,
            eval_set_id=eval_set.eval_set_id,
            task_class="quick_research_summarization",
            dataset_version=eval_set.dataset_version,
            requester_id="scoring-service",
            requester_change_ref=None,
            purpose="promotion_gate",
            verdict="allowed",
            reason="operator-gated scoring service evaluated frozen holdout",
            decision_id=decision_id,
        )
        holdout_use_id = self.mi.record_holdout_use(
            model_intelligence_command("model.holdout_use.record", "holdout-promotion-allowed-2"),
            holdout_use,
        )
        eval_run = self.eval_run(candidate, eval_set)
        object.__setattr__(eval_run, "verdict", "supports_decision")
        object.__setattr__(eval_run, "decision_id", decision_id)
        object.__setattr__(eval_run, "confidence", {**eval_run.confidence, "score": 0.82})
        object.__setattr__(
            eval_run,
            "frozen_holdout_result",
            {**eval_run.frozen_holdout_result, "confidence_score": 0.81},
        )
        eval_run_id = self.mi.record_eval_run(
            model_intelligence_command("model.eval_run.record", "eval-run-decision-support-2"),
            eval_run,
        )
        base_packet = ModelPromotionDecisionPacket(
            model_id=candidate.model_id,
            task_class="quick_research_summarization",
            proposed_routing_role="research_local",
            recommendation="promote",
            required_authority="operator_gate",
            decision_id=decision_id,
            eval_run_ids=[eval_run_id],
            holdout_use_ids=[holdout_use_id],
            evidence_refs=[f"kernel:model_eval_runs/{eval_run_id}"],
            frozen_holdout_confidence=0.79,
            confidence_threshold=0.80,
            gate_packet={"decision_type": "model_promotion"},
            risk_flags=[],
            default_on_timeout="keep_current_route",
        )
        with self.assertRaises(ValueError):
            self.mi.create_promotion_decision_packet(
                model_intelligence_command("model.promotion_decision_packet.create", "promotion-low-confidence"),
                base_packet,
            )

        no_evidence = ModelPromotionDecisionPacket(
            model_id=candidate.model_id,
            task_class="quick_research_summarization",
            proposed_routing_role="research_local",
            recommendation="promote",
            required_authority="operator_gate",
            decision_id=decision_id,
            eval_run_ids=[eval_run_id],
            holdout_use_ids=[holdout_use_id],
            evidence_refs=[],
            frozen_holdout_confidence=0.81,
            confidence_threshold=0.80,
            gate_packet={"decision_type": "model_promotion"},
            risk_flags=[],
            default_on_timeout="keep_current_route",
        )
        with self.assertRaises(ValueError):
            self.mi.create_promotion_decision_packet(
                model_intelligence_command("model.promotion_decision_packet.create", "promotion-no-evidence"),
                no_evidence,
            )

        model_command = Command(
            command_type="model.promotion_decision_packet.create",
            requested_by="model",
            requester_id=candidate.model_id,
            target_entity_type="decision",
            requested_authority="operator_gate",
            idempotency_key="promotion-model-requested",
            payload={"decision_id": decision_id},
        )
        ok_packet = ModelPromotionDecisionPacket(
            model_id=candidate.model_id,
            task_class="quick_research_summarization",
            proposed_routing_role="research_local",
            recommendation="promote",
            required_authority="operator_gate",
            decision_id=decision_id,
            eval_run_ids=[eval_run_id],
            holdout_use_ids=[holdout_use_id],
            evidence_refs=[f"kernel:model_eval_runs/{eval_run_id}"],
            frozen_holdout_confidence=0.81,
            confidence_threshold=0.80,
            gate_packet={"decision_type": "model_promotion"},
            risk_flags=[],
            default_on_timeout="keep_current_route",
        )
        with self.assertRaises(PermissionError):
            self.mi.create_promotion_decision_packet(model_command, ok_packet)

    def test_demotion_event_immediately_updates_routing_state_and_audit_trail(self):
        candidate, _, eval_set = self.seed_registry()
        replacement = self.replacement_candidate()
        self.mi.register_candidate(
            model_intelligence_command("model.candidate.register", "replacement-candidate"),
            replacement,
        )
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE model_candidates SET promotion_state='promoted' WHERE model_id IN (?, ?)",
                (candidate.model_id, replacement.model_id),
            )

        eval_run = self.eval_run(candidate, eval_set)
        object.__setattr__(eval_run, "quality_score", 0.72)
        object.__setattr__(eval_run, "reliability_score", 0.91)
        object.__setattr__(eval_run, "latency_p95_ms", 28_400)
        object.__setattr__(eval_run, "aggregate_scores", {**eval_run.aggregate_scores, "overall": 0.74})
        object.__setattr__(
            eval_run,
            "frozen_holdout_result",
            {**eval_run.frozen_holdout_result, "quality_score": 0.71, "latency_p95_ms": 28_400},
        )
        eval_run_id = self.mi.record_eval_run(
            model_intelligence_command("model.eval_run.record", "eval-run-demotion-quality-latency"),
            eval_run,
        )
        route = ModelRouteDecision(
            task_id="task-demote-quick-summary-1",
            task_class="quick_research_summarization",
            data_class="internal",
            risk_level="medium",
            selected_route="local",
            selected_model_id=candidate.model_id,
            candidate_model_id=None,
            eval_set_id=eval_set.eval_set_id,
            reasons=["previous operator-approved local route"],
            required_authority="operator_gate",
            decision_id=None,
            local_offload_estimate={"eligible": True},
            frontier_fallback={"provider": "openai", "reason": "fallback if local route is demoted"},
        )
        route_id = self.mi.record_route_decision(
            model_intelligence_command("model.route_decision.record", "route-before-demotion"),
            route,
        )
        demotion = ModelDemotionRecord(
            model_id=candidate.model_id,
            task_class="quick_research_summarization",
            routing_roles=["research_local"],
            reasons=["quality_regression", "latency_regression", "replacement_regression"],
            required_authority="rule",
            evidence_refs=[
                f"kernel:model_eval_runs/{eval_run_id}",
                f"kernel:model_route_decisions/{route_id}",
                "artifact://evals/model-intelligence/quick-research/demotion-regression-report",
            ],
            eval_run_ids=[eval_run_id],
            route_decision_ids=[route_id],
            metrics={
                "quality_score": 0.72,
                "quality_threshold": 0.82,
                "latency_p95_ms": 28_400,
                "latency_threshold_ms": 20_000,
                "replacement_candidate": replacement.model_id,
            },
            routing_state_update={
                "status": "active",
                "replacement_model_id": replacement.model_id,
                "route_version": "quick-summary@2026-05-02.demoted-to-replacement",
                "fallback_route": {"selected_route": "frontier", "provider": "openai"},
            },
            audit_notes="Demoted for future promotion review after frozen regression and replacement comparison.",
        )
        demotion_command = Command(
            command_type="model.demotion.record",
            requested_by="kernel",
            requester_id="kernel-model-intelligence",
            target_entity_type="model",
            target_entity_id=candidate.model_id,
            requested_authority="rule",
            idempotency_key="demotion-quality-latency-replacement",
            payload={"model_id": candidate.model_id, "reasons": demotion.reasons},
        )

        demotion_id = self.mi.record_demotion(demotion_command, demotion)

        with self.store.connect() as conn:
            candidate_row = conn.execute(
                "SELECT promotion_state FROM model_candidates WHERE model_id=?",
                (candidate.model_id,),
            ).fetchone()
            demotion_row = conn.execute(
                """
                SELECT reasons_json, required_authority, authority_effect, audit_notes
                FROM model_demotion_records
                WHERE demotion_id=?
                """,
                (demotion_id,),
            ).fetchone()
            routing_state = conn.execute(
                """
                SELECT active_model_id, status, route_version, demotion_id, reasons_json
                FROM model_routing_state
                WHERE task_class='quick_research_summarization' AND routing_role='research_local'
                """
            ).fetchone()
            event_payload = conn.execute(
                "SELECT payload_json FROM events WHERE event_type='model_demoted' AND entity_id=?",
                (demotion_id,),
            ).fetchone()["payload_json"]

        self.assertEqual(candidate_row["promotion_state"], "demoted")
        self.assertEqual(demotion_row["required_authority"], "rule")
        self.assertEqual(demotion_row["authority_effect"], "immediate_routing_update")
        self.assertIn("future promotion review", demotion_row["audit_notes"])
        self.assertEqual(routing_state["active_model_id"], replacement.model_id)
        self.assertEqual(routing_state["status"], "active")
        self.assertEqual(routing_state["demotion_id"], demotion_id)
        self.assertIn("quality_regression", routing_state["reasons_json"])
        self.assertIn(replacement.model_id, event_payload)

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.model_candidates[candidate.model_id]["promotion_state"], "demoted")
        self.assertEqual(replay.model_demotion_records[demotion_id]["reasons"][0], "quality_regression")
        routing_state_id = next(iter(replay.model_routing_state))
        self.assertEqual(replay.model_routing_state[routing_state_id]["active_model_id"], replacement.model_id)

    def test_demotion_rejects_model_self_authority_and_missing_evidence(self):
        candidate, _, _ = self.seed_registry()
        demotion = ModelDemotionRecord(
            model_id=candidate.model_id,
            task_class="quick_research_summarization",
            routing_roles=["research_local"],
            reasons=["license_tos_regression"],
            required_authority="rule",
            evidence_refs=["artifact://model-license/tos-change"],
            eval_run_ids=[],
            route_decision_ids=[],
            metrics={"commercial_use": "prohibited"},
            routing_state_update={
                "status": "blocked",
                "fallback_route": {"selected_route": "frontier", "reason": "license/toS blocked local use"},
            },
            audit_notes="License and ToS regression blocks future promotion until terms are reverified.",
        )
        model_command = Command(
            command_type="model.demotion.record",
            requested_by="model",
            requester_id=candidate.model_id,
            target_entity_type="model",
            target_entity_id=candidate.model_id,
            requested_authority="rule",
            idempotency_key="demotion-model-self-request",
            payload={"model_id": candidate.model_id},
        )
        with self.assertRaises(PermissionError):
            self.mi.record_demotion(model_command, demotion)

        no_evidence = ModelDemotionRecord(
            model_id=candidate.model_id,
            task_class="quick_research_summarization",
            routing_roles=["research_local"],
            reasons=["drift_regression"],
            required_authority="rule",
            evidence_refs=[],
            eval_run_ids=[],
            route_decision_ids=[],
            metrics={"runtime": "changed"},
            routing_state_update={"status": "blocked"},
            audit_notes="Drift suspected.",
        )
        with self.assertRaises(ValueError):
            self.mi.record_demotion(
                Command(
                    command_type="model.demotion.record",
                    requested_by="kernel",
                    requester_id="kernel-model-intelligence",
                    target_entity_type="model",
                    target_entity_id=candidate.model_id,
                    requested_authority="rule",
                    idempotency_key="demotion-missing-evidence",
                    payload={"model_id": candidate.model_id},
                ),
                no_evidence,
            )

    def test_expansion_task_classes_and_self_promoted_candidates_are_rejected(self):
        expansion = ModelTaskClassRecord(
            task_class="coding_small_patch",
            description="bad expansion flag",
            quality_threshold=0.8,
            reliability_threshold=0.9,
            latency_p95_ms=1000,
            local_offload_target=0.1,
            allowed_data_classes=["public"],
            promotion_authority="operator_gate",
            expansion_allowed=True,
        )
        with self.assertRaises(ValueError):
            self.store.register_model_task_class(
                model_intelligence_command("model.task_class.register", "bad-expansion"),
                expansion,
            )

        promoted = self.candidate()
        object.__setattr__(promoted, "promotion_state", "promoted")
        with self.assertRaises(ValueError):
            self.mi.register_candidate(
                model_intelligence_command("model.candidate.register", "bad-promoted"),
                promoted,
            )


if __name__ == "__main__":
    unittest.main()
