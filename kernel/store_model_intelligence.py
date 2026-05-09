from __future__ import annotations

import sqlite3
from decimal import Decimal
from typing import Any

from .records import (
    ArtifactGovernanceRecord,
    ArtifactLifecycleReplayProjectionComparison,
    ArtifactLifecycleTaskPacket,
    ArtifactPayloadMetadata,
    ArtifactRef,
    BackupCadenceRecord,
    Budget,
    CapabilityGrant,
    Command,
    CommercialDecisionRecommendationRecord,
    Decision,
    EncryptedStorageAccessVerificationState,
    EncryptedStorageDescriptor,
    EncryptedStorageKeyRotationRecord,
    EncryptedStorageReplayProjectionComparison,
    EvidenceBundle,
    Event,
    HoldoutPolicy,
    HoldoutUseRecord,
    LocalOffloadEvalSet,
    ModelCandidate,
    ModelDemotionRecord,
    ModelEvalRun,
    ModelPromotionDecisionPacket,
    ModelRouteDecision,
    ModelTaskClassRecord,
    OpportunityProjectDecisionPacket,
    Project,
    ProjectArtifactReceipt,
    ProjectCommercialRollup,
    ProjectCloseDecisionPacket,
    ProjectCustomerCommitment,
    ProjectCustomerCommitmentReceipt,
    ProjectCustomerFeedback,
    ProjectCustomerVisiblePacket,
    ProjectCustomerVisibleReplayProjectionComparison,
    ProjectOperatorLoadRecord,
    ProjectOutcome,
    ProjectPhaseRollup,
    ProjectPortfolioDecisionPacket,
    ProjectPortfolioReplayProjectionComparison,
    ProjectReplayProjectionComparison,
    ProjectRevenueAttribution,
    ProjectSchedulingIntent,
    ProjectSchedulingPriorityChangePacket,
    ProjectSchedulingPriorityReplayProjectionComparison,
    ProjectSchedulingReplayProjectionComparison,
    ProjectStatusRollup,
    ProjectTask,
    ProjectTaskAssignment,
    PayloadAccessReceipt,
    RecoveryChecklistReceipt,
    RecoveryReadinessPacket,
    RecoveryReadinessReplayProjectionComparison,
    RecoveryReplayProjectionComparison,
    RecoveryVerificationState,
    ResearchRequest,
    RestoreDrillPacket,
    SourceAcquisitionCheck,
    SourcePlan,
    SideEffectIntent,
    SideEffectReceipt,
    canonical_json,
    new_id,
    now_iso,
    payload_hash,
    sha256_text,
)
from .replay import ReplayState
from .store_common import (
    _loads,
    _model_task_class_payload,
    _model_candidate_payload,
    _holdout_policy_payload,
    _local_offload_eval_set_payload,
    _holdout_use_payload,
    _model_eval_run_payload,
    _model_route_decision_payload,
    _model_promotion_packet_payload,
    _model_demotion_payload,
)


class ModelIntelligenceKernelTransactionMixin:
    def register_model_task_class(self, task_class: ModelTaskClassRecord) -> str:
        if task_class.expansion_allowed:
            raise ValueError("seed Model Intelligence slice cannot enable expansion task classes")
        if task_class.promotion_authority != "operator_gate":
            raise ValueError("seed task-class promotion authority must stay operator-gated")
        payload = _model_task_class_payload(task_class)
        event_id = self.append_event("model_task_class_registered", "model", task_class.task_class, payload)
        self.conn.execute(
            """
            INSERT INTO model_task_classes (
              task_class_id, task_class, description, quality_threshold,
              reliability_threshold, latency_p95_ms, local_offload_target,
              allowed_data_classes_json, promotion_authority, expansion_allowed,
              status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_class.task_class_id,
                task_class.task_class,
                task_class.description,
                task_class.quality_threshold,
                task_class.reliability_threshold,
                task_class.latency_p95_ms,
                task_class.local_offload_target,
                canonical_json(task_class.allowed_data_classes),
                task_class.promotion_authority,
                1 if task_class.expansion_allowed else 0,
                task_class.status,
                task_class.created_at,
            ),
        )
        self.enqueue_projection(event_id, "model_task_class_projection")
        return task_class.task_class

    def register_model_candidate(self, candidate: ModelCandidate) -> str:
        if candidate.access_mode == "local" and candidate.data_residency != "local_only":
            raise ValueError("local model candidates must declare local_only data residency")
        if candidate.promotion_state == "promoted":
            raise ValueError("Model Intelligence evidence records cannot self-promote candidates")
        payload = _model_candidate_payload(candidate)
        event_id = self.append_event("model_candidate_registered", "model", candidate.model_id, payload)
        self.conn.execute(
            """
            INSERT INTO model_candidates (
              candidate_id, model_id, provider, access_mode, source_ref, artifact_hash,
              license, commercial_use, terms_verified_at, context_window,
              modalities_json, hardware_fit, sandbox_profile, data_residency,
              cost_profile_json, latency_profile_json, routing_metadata_json,
              promotion_state, last_verified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                candidate.candidate_id,
                candidate.model_id,
                candidate.provider,
                candidate.access_mode,
                candidate.source_ref,
                candidate.artifact_hash,
                candidate.license,
                candidate.commercial_use,
                candidate.terms_verified_at,
                candidate.context_window,
                canonical_json(candidate.modalities),
                candidate.hardware_fit,
                candidate.sandbox_profile,
                candidate.data_residency,
                canonical_json(candidate.cost_profile),
                canonical_json(candidate.latency_profile),
                canonical_json(candidate.routing_metadata),
                candidate.promotion_state,
                candidate.last_verified_at,
            ),
        )
        self.enqueue_projection(event_id, "model_candidate_projection")
        return candidate.model_id

    def create_holdout_policy(self, policy: HoldoutPolicy) -> str:
        task_class = self.conn.execute(
            "SELECT promotion_authority FROM model_task_classes WHERE task_class=? AND status='seed'",
            (policy.task_class,),
        ).fetchone()
        if task_class is None:
            raise ValueError("holdout policy requires a registered seed task class")
        if not policy.promotion_requires_decision:
            raise ValueError("holdout policy must require a Decision record for promotion gates")
        if policy.min_sample_count <= 0:
            raise ValueError("holdout policy min_sample_count must be positive")
        payload = _holdout_policy_payload(policy)
        event_id = self.append_event("model_holdout_policy_created", "model", policy.policy_id, payload)
        self.conn.execute(
            """
            INSERT INTO model_holdout_policies (
              policy_id, task_class, dataset_version, access, min_sample_count,
              contamination_controls_json, scorer_separation,
              promotion_requires_decision, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                policy.policy_id,
                policy.task_class,
                policy.dataset_version,
                policy.access,
                policy.min_sample_count,
                canonical_json(policy.contamination_controls),
                policy.scorer_separation,
                1 if policy.promotion_requires_decision else 0,
                policy.created_at,
            ),
        )
        self.enqueue_projection(event_id, "model_holdout_policy_projection")
        return policy.policy_id

    def register_local_offload_eval_set(self, eval_set: LocalOffloadEvalSet) -> str:
        policy = self.conn.execute(
            """
            SELECT task_class, dataset_version, min_sample_count
            FROM model_holdout_policies
            WHERE policy_id=?
            """,
            (eval_set.holdout_policy_id,),
        ).fetchone()
        if policy is None:
            raise ValueError("eval set requires a holdout policy")
        if policy["task_class"] != eval_set.task_class or policy["dataset_version"] != eval_set.dataset_version:
            raise ValueError("eval set task class/version must match holdout policy")
        required = {"development", "regression", "known_bad", "frozen_holdout"}
        split_counts = {str(key): int(value) for key, value in eval_set.split_counts.items()}
        missing = sorted(required - set(split_counts))
        if missing:
            raise ValueError(f"eval set missing required splits: {', '.join(missing)}")
        if any(count <= 0 for count in split_counts.values()):
            raise ValueError("eval split counts must be positive")
        if split_counts["frozen_holdout"] < int(policy["min_sample_count"]):
            raise ValueError("frozen holdout split is below policy minimum")
        payload = _local_offload_eval_set_payload(eval_set, split_counts)
        event_id = self.append_event("local_offload_eval_set_registered", "model", eval_set.eval_set_id, payload)
        self.conn.execute(
            """
            INSERT INTO local_offload_eval_sets (
              eval_set_id, task_class, dataset_version, artifact_ref,
              split_counts_json, data_classes_json, retention_policy,
              scorer_profile_json, holdout_policy_id, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                eval_set.eval_set_id,
                eval_set.task_class,
                eval_set.dataset_version,
                eval_set.artifact_ref,
                canonical_json(split_counts),
                canonical_json(eval_set.data_classes),
                eval_set.retention_policy,
                canonical_json(eval_set.scorer_profile),
                eval_set.holdout_policy_id,
                eval_set.status,
                eval_set.created_at,
            ),
        )
        self.enqueue_projection(event_id, "local_offload_eval_set_projection")
        return eval_set.eval_set_id

    def record_holdout_use(self, holdout_use: HoldoutUseRecord) -> str:
        row = self.conn.execute(
            """
            SELECT p.promotion_requires_decision, e.task_class, e.dataset_version
            FROM model_holdout_policies p
            JOIN local_offload_eval_sets e ON e.holdout_policy_id = p.policy_id
            WHERE p.policy_id=? AND e.eval_set_id=?
            """,
            (holdout_use.policy_id, holdout_use.eval_set_id),
        ).fetchone()
        if row is None:
            raise ValueError("holdout use requires matching policy and eval set")
        if row["task_class"] != holdout_use.task_class or row["dataset_version"] != holdout_use.dataset_version:
            raise ValueError("holdout use task class/version mismatch")
        self_scoring = holdout_use.requester_change_ref and holdout_use.requester_id in holdout_use.requester_change_ref
        if holdout_use.purpose == "development" and holdout_use.verdict != "blocked":
            raise PermissionError("development work cannot access frozen holdout")
        if self_scoring and holdout_use.verdict != "blocked":
            raise PermissionError("workers cannot score their own change on frozen holdout")
        if holdout_use.purpose == "promotion_gate" and row["promotion_requires_decision"] and not holdout_use.decision_id:
            raise PermissionError("promotion-gate holdout use requires a Decision record")
        if holdout_use.purpose == "promotion_gate" and holdout_use.verdict == "allowed":
            decision = self._get_model_promotion_decision(holdout_use.decision_id)
            if decision["required_authority"] != "operator_gate":
                raise PermissionError("promotion-gate holdout use requires an operator-gate Decision")
        payload = _holdout_use_payload(holdout_use)
        event_id = self.append_event("model_holdout_use_recorded", "model", holdout_use.holdout_use_id, payload)
        self.conn.execute(
            """
            INSERT INTO model_holdout_use_records (
              holdout_use_id, policy_id, eval_set_id, task_class, dataset_version,
              requester_id, requester_change_ref, purpose, verdict, reason,
              decision_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                holdout_use.holdout_use_id,
                holdout_use.policy_id,
                holdout_use.eval_set_id,
                holdout_use.task_class,
                holdout_use.dataset_version,
                holdout_use.requester_id,
                holdout_use.requester_change_ref,
                holdout_use.purpose,
                holdout_use.verdict,
                holdout_use.reason,
                holdout_use.decision_id,
                holdout_use.created_at,
            ),
        )
        self.enqueue_projection(event_id, "model_holdout_use_projection")
        return holdout_use.holdout_use_id

    def record_model_eval_run(self, eval_run: ModelEvalRun) -> str:
        row = self.conn.execute(
            """
            SELECT e.task_class, e.dataset_version, e.status, p.min_sample_count
            FROM local_offload_eval_sets e
            JOIN model_holdout_policies p ON p.policy_id = e.holdout_policy_id
            WHERE e.eval_set_id=?
            """,
            (eval_run.eval_set_id,),
        ).fetchone()
        if row is None:
            raise ValueError("eval run requires a registered eval set")
        if row["task_class"] != eval_run.task_class or row["dataset_version"] != eval_run.dataset_version:
            raise ValueError("eval run task class/version must match eval set")
        if row["status"] != "active":
            raise ValueError("eval run requires an active eval set")
        model = self.conn.execute(
            "SELECT promotion_state FROM model_candidates WHERE model_id=?",
            (eval_run.model_id,),
        ).fetchone()
        if model is None:
            raise ValueError("eval run model is not registered")
        if eval_run.baseline_model_id:
            baseline = self.conn.execute(
                "SELECT model_id FROM model_candidates WHERE model_id=?",
                (eval_run.baseline_model_id,),
            ).fetchone()
            if baseline is None:
                raise ValueError("eval run baseline model is not registered")
        if eval_run.authority_effect != "evidence_only":
            raise ValueError("eval run authority effect must remain evidence_only")
        if eval_run.verdict == "supports_decision" and not eval_run.decision_id:
            raise PermissionError("decision-support eval runs must cite a future Decision packet id")
        if eval_run.verdict == "supports_decision":
            decision = self._get_model_promotion_decision(eval_run.decision_id)
            if decision["required_authority"] != "operator_gate":
                raise PermissionError("decision-support eval runs require an operator-gate Decision")
        if not eval_run.route_version.strip():
            raise ValueError("eval run requires route-version metadata")
        if eval_run.sample_count <= 0:
            raise ValueError("eval run sample_count must be positive")
        if eval_run.latency_p95_ms < eval_run.latency_p50_ms:
            raise ValueError("eval run p95 latency cannot be below p50 latency")
        for score_name, score in {
            "quality_score": eval_run.quality_score,
            "reliability_score": eval_run.reliability_score,
        }.items():
            if score < 0.0 or score > 1.0:
                raise ValueError(f"eval run {score_name} must be between 0 and 1")
        confidence_score = eval_run.confidence.get("score")
        if confidence_score is None or float(confidence_score) < 0.0 or float(confidence_score) > 1.0:
            raise ValueError("eval run confidence must report a score between 0 and 1")
        frozen_sample_count = int(eval_run.frozen_holdout_result.get("sample_count", 0))
        holdout_split = eval_run.frozen_holdout_result.get("split")
        if (
            (eval_run.verdict == "supports_decision" or holdout_split == "frozen_holdout")
            and frozen_sample_count < int(row["min_sample_count"])
        ):
            raise ValueError("eval run frozen holdout result is below policy minimum")
        if "quality_score" not in eval_run.frozen_holdout_result or "reliability_score" not in eval_run.frozen_holdout_result:
            raise ValueError("eval run must capture frozen holdout quality and reliability")
        if not eval_run.aggregate_scores:
            raise ValueError("eval run requires aggregate scores")
        if "overall" not in eval_run.aggregate_scores:
            raise ValueError("eval run aggregate scores require an overall score")

        payload = _model_eval_run_payload(eval_run)
        event_id = self.append_event("model_eval_run_recorded", "model", eval_run.eval_run_id, payload)
        self.conn.execute(
            """
            INSERT INTO model_eval_runs (
              eval_run_id, model_id, task_class, dataset_version, eval_set_id,
              baseline_model_id, route_version, route_metadata_json, sample_count,
              quality_score, reliability_score, latency_p50_ms, latency_p95_ms,
              cost_per_1k_tasks, aggregate_scores_json, failure_categories_json,
              failure_modes_json, confidence_json, frozen_holdout_result_json,
              verdict, scorer_id, decision_id, authority_effect, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                eval_run.eval_run_id,
                eval_run.model_id,
                eval_run.task_class,
                eval_run.dataset_version,
                eval_run.eval_set_id,
                eval_run.baseline_model_id,
                eval_run.route_version,
                canonical_json(eval_run.route_metadata),
                eval_run.sample_count,
                eval_run.quality_score,
                eval_run.reliability_score,
                eval_run.latency_p50_ms,
                eval_run.latency_p95_ms,
                str(eval_run.cost_per_1k_tasks),
                canonical_json(eval_run.aggregate_scores),
                canonical_json(eval_run.failure_categories),
                canonical_json(eval_run.failure_modes),
                canonical_json(eval_run.confidence),
                canonical_json(eval_run.frozen_holdout_result),
                eval_run.verdict,
                eval_run.scorer_id,
                eval_run.decision_id,
                eval_run.authority_effect,
                eval_run.created_at,
            ),
        )
        self.enqueue_projection(event_id, "model_eval_run_projection")
        return eval_run.eval_run_id

    def record_model_route_decision(self, route_decision: ModelRouteDecision) -> str:
        task_class = self.conn.execute(
            "SELECT promotion_authority FROM model_task_classes WHERE task_class=? AND status='seed'",
            (route_decision.task_class,),
        ).fetchone()
        if task_class is None:
            raise ValueError("route decision requires a registered seed task class")
        if route_decision.required_authority != task_class["promotion_authority"]:
            raise ValueError("route decision authority must match task-class promotion authority")
        if route_decision.selected_model_id:
            model = self.conn.execute(
                "SELECT promotion_state FROM model_candidates WHERE model_id=?",
                (route_decision.selected_model_id,),
            ).fetchone()
            if model is None:
                raise ValueError("selected model is not registered")
            if route_decision.selected_route == "local" and model["promotion_state"] != "promoted":
                raise PermissionError("local route requires separately promoted model state")
        if route_decision.candidate_model_id:
            candidate = self.conn.execute(
                "SELECT model_id FROM model_candidates WHERE model_id=?",
                (route_decision.candidate_model_id,),
            ).fetchone()
            if candidate is None:
                raise ValueError("candidate model is not registered")
        if route_decision.eval_set_id:
            eval_set = self.conn.execute(
                "SELECT task_class FROM local_offload_eval_sets WHERE eval_set_id=?",
                (route_decision.eval_set_id,),
            ).fetchone()
            if eval_set is None or eval_set["task_class"] != route_decision.task_class:
                raise ValueError("route decision eval set mismatch")
        if route_decision.selected_route in {"local", "shadow"} and not route_decision.eval_set_id:
            raise ValueError("local or shadow routing decisions require eval-set evidence")
        payload = _model_route_decision_payload(route_decision)
        event_id = self.append_event("model_route_decision_recorded", "model", route_decision.route_decision_id, payload)
        self.conn.execute(
            """
            INSERT INTO model_route_decisions (
              route_decision_id, task_id, task_class, data_class, risk_level,
              selected_route, selected_model_id, candidate_model_id, eval_set_id,
              reasons_json, required_authority, decision_id,
              local_offload_estimate_json, frontier_fallback_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                route_decision.route_decision_id,
                route_decision.task_id,
                route_decision.task_class,
                route_decision.data_class,
                route_decision.risk_level,
                route_decision.selected_route,
                route_decision.selected_model_id,
                route_decision.candidate_model_id,
                route_decision.eval_set_id,
                canonical_json(route_decision.reasons),
                route_decision.required_authority,
                route_decision.decision_id,
                canonical_json(route_decision.local_offload_estimate),
                canonical_json(route_decision.frontier_fallback),
                route_decision.created_at,
            ),
        )
        self.enqueue_projection(event_id, "model_route_decision_projection")
        return route_decision.route_decision_id

    def create_model_promotion_decision_packet(self, packet: ModelPromotionDecisionPacket) -> str:
        task_class = self.conn.execute(
            """
            SELECT promotion_authority, quality_threshold, reliability_threshold, latency_p95_ms
            FROM model_task_classes
            WHERE task_class=? AND status='seed'
            """,
            (packet.task_class,),
        ).fetchone()
        if task_class is None:
            raise ValueError("model promotion packet requires a registered seed task class")
        if packet.required_authority != task_class["promotion_authority"]:
            raise PermissionError("kernel policy assigns promotion authority from the task class")
        if packet.required_authority != "operator_gate":
            raise PermissionError("seed model promotion packets must route through operator gate")
        if self.command.requested_by == "model":
            raise PermissionError("models cannot request or assign their own promotion authority")
        if self.command.requested_authority and self.command.requested_authority != packet.required_authority:
            raise PermissionError("command requested authority does not match kernel promotion policy")
        candidate = self.conn.execute(
            "SELECT promotion_state, commercial_use FROM model_candidates WHERE model_id=?",
            (packet.model_id,),
        ).fetchone()
        if candidate is None:
            raise ValueError("model promotion packet requires a registered model candidate")
        if candidate["promotion_state"] == "promoted":
            raise ValueError("model promotion packet cannot self-assign an already promoted state")
        if not packet.decision_id.strip():
            raise ValueError("model promotion packet requires a Decision id")
        decision = self._get_model_promotion_decision(packet.decision_id)
        if decision["required_authority"] != packet.required_authority:
            raise ValueError("model promotion packet authority must match Decision record")
        if decision["status"] != packet.status:
            raise ValueError("model promotion packet status must match Decision record")
        if decision["recommendation"] != packet.recommendation:
            raise ValueError("model promotion packet recommendation must match Decision record")
        if not packet.eval_run_ids:
            raise ValueError("model promotion packet requires eval-run evidence references")
        if not packet.holdout_use_ids:
            raise ValueError("model promotion packet requires promotion-gate holdout-use references")
        if not packet.evidence_refs:
            raise ValueError("model promotion packet requires durable evidence references")
        if packet.frozen_holdout_confidence < packet.confidence_threshold:
            raise ValueError("frozen holdout confidence is below the packet threshold")
        if packet.recommendation == "promote" and packet.frozen_holdout_confidence < packet.confidence_threshold:
            raise ValueError("promotion recommendation requires frozen holdout confidence above threshold")

        for eval_run_id in packet.eval_run_ids:
            eval_row = self.conn.execute(
                """
                SELECT model_id, task_class, verdict, decision_id, quality_score,
                       reliability_score, latency_p95_ms, confidence_json,
                       frozen_holdout_result_json
                FROM model_eval_runs
                WHERE eval_run_id=?
                """,
                (eval_run_id,),
            ).fetchone()
            if eval_row is None:
                raise ValueError("model promotion packet references unknown eval run")
            if eval_row["model_id"] != packet.model_id or eval_row["task_class"] != packet.task_class:
                raise ValueError("model promotion packet eval-run model/task mismatch")
            if eval_row["verdict"] != "supports_decision":
                raise PermissionError("promotion packet eval runs must be evidence-only decision support")
            if eval_row["decision_id"] != packet.decision_id:
                raise ValueError("promotion packet eval runs must cite the same Decision id")
            if float(eval_row["quality_score"]) < float(task_class["quality_threshold"]):
                raise ValueError("promotion packet eval quality is below task-class threshold")
            if float(eval_row["reliability_score"]) < float(task_class["reliability_threshold"]):
                raise ValueError("promotion packet eval reliability is below task-class threshold")
            if int(eval_row["latency_p95_ms"]) > int(task_class["latency_p95_ms"]):
                raise ValueError("promotion packet eval latency exceeds task-class threshold")
            confidence = _loads(eval_row["confidence_json"])
            if float(confidence.get("score", -1.0)) < packet.confidence_threshold:
                raise ValueError("promotion packet eval confidence is below threshold")
            holdout = _loads(eval_row["frozen_holdout_result_json"])
            holdout_confidence = holdout.get("confidence_score", holdout.get("confidence"))
            if holdout_confidence is None or float(holdout_confidence) < packet.confidence_threshold:
                raise ValueError("promotion packet requires frozen-holdout confidence on each eval run")
            if not holdout.get("artifact_ref"):
                raise ValueError("promotion packet eval run must cite a frozen-holdout artifact")

        for holdout_use_id in packet.holdout_use_ids:
            use_row = self.conn.execute(
                """
                SELECT task_class, purpose, verdict, decision_id
                FROM model_holdout_use_records
                WHERE holdout_use_id=?
                """,
                (holdout_use_id,),
            ).fetchone()
            if use_row is None:
                raise ValueError("model promotion packet references unknown holdout-use record")
            if use_row["task_class"] != packet.task_class:
                raise ValueError("model promotion packet holdout-use task mismatch")
            if use_row["purpose"] != "promotion_gate" or use_row["verdict"] != "allowed":
                raise PermissionError("model promotion packet requires allowed promotion-gate holdout use")
            if use_row["decision_id"] != packet.decision_id:
                raise ValueError("model promotion packet holdout-use Decision id mismatch")

        payload = _model_promotion_packet_payload(packet)
        event_id = self.append_event("model_promotion_decision_packet_created", "decision", packet.packet_id, payload)
        self.conn.execute(
            """
            INSERT INTO model_promotion_decision_packets (
              packet_id, decision_id, model_id, task_class, proposed_routing_role,
              recommendation, required_authority, eval_run_ids_json,
              holdout_use_ids_json, evidence_refs_json, frozen_holdout_confidence,
              confidence_threshold, gate_packet_json, risk_flags_json,
              default_on_timeout, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.packet_id,
                packet.decision_id,
                packet.model_id,
                packet.task_class,
                packet.proposed_routing_role,
                packet.recommendation,
                packet.required_authority,
                canonical_json(packet.eval_run_ids),
                canonical_json(packet.holdout_use_ids),
                canonical_json(packet.evidence_refs),
                packet.frozen_holdout_confidence,
                packet.confidence_threshold,
                canonical_json(packet.gate_packet),
                canonical_json(packet.risk_flags),
                packet.default_on_timeout,
                packet.status,
                packet.created_at,
            ),
        )
        self.enqueue_projection(event_id, "model_promotion_decision_packet_projection")
        return packet.packet_id

    def record_model_demotion(self, demotion: ModelDemotionRecord) -> str:
        task_class = self.conn.execute(
            """
            SELECT promotion_authority
            FROM model_task_classes
            WHERE task_class=? AND status='seed'
            """,
            (demotion.task_class,),
        ).fetchone()
        if task_class is None:
            raise ValueError("model demotion requires a registered seed task class")
        if demotion.required_authority != "rule":
            raise PermissionError("model demotion uses kernel rule authority for immediate routing safety")
        if self.command.requested_by == "model":
            raise PermissionError("models cannot request or assign their own demotion authority")
        if self.command.requested_authority and self.command.requested_authority != demotion.required_authority:
            raise PermissionError("command requested authority does not match kernel demotion policy")
        candidate = self.conn.execute(
            "SELECT promotion_state FROM model_candidates WHERE model_id=?",
            (demotion.model_id,),
        ).fetchone()
        if candidate is None:
            raise ValueError("model demotion requires a registered model candidate")
        if candidate["promotion_state"] in {"rejected", "retired"}:
            raise ValueError("model demotion cannot target rejected or retired candidates")
        if not demotion.routing_roles:
            raise ValueError("model demotion requires at least one affected routing role")
        if not demotion.reasons:
            raise ValueError("model demotion requires at least one auditable reason")
        if not demotion.evidence_refs:
            raise ValueError("model demotion requires durable evidence references")
        if not demotion.audit_notes.strip():
            raise ValueError("model demotion requires audit notes for future promotion review")
        if demotion.authority_effect != "immediate_routing_update":
            raise ValueError("model demotion must update routing immediately")

        for eval_run_id in demotion.eval_run_ids:
            eval_row = self.conn.execute(
                "SELECT model_id, task_class FROM model_eval_runs WHERE eval_run_id=?",
                (eval_run_id,),
            ).fetchone()
            if eval_row is None:
                raise ValueError("model demotion references unknown eval run")
            if eval_row["model_id"] != demotion.model_id or eval_row["task_class"] != demotion.task_class:
                raise ValueError("model demotion eval-run model/task mismatch")
        for route_decision_id in demotion.route_decision_ids:
            route_row = self.conn.execute(
                """
                SELECT task_class, selected_model_id, candidate_model_id
                FROM model_route_decisions
                WHERE route_decision_id=?
                """,
                (route_decision_id,),
            ).fetchone()
            if route_row is None:
                raise ValueError("model demotion references unknown route decision")
            if route_row["task_class"] != demotion.task_class:
                raise ValueError("model demotion route-decision task mismatch")
            if demotion.model_id not in {route_row["selected_model_id"], route_row["candidate_model_id"]}:
                raise ValueError("model demotion route-decision does not involve demoted model")

        replacement_model_id = demotion.routing_state_update.get("replacement_model_id")
        if replacement_model_id:
            replacement = self.conn.execute(
                "SELECT model_id FROM model_candidates WHERE model_id=?",
                (replacement_model_id,),
            ).fetchone()
            if replacement is None:
                raise ValueError("model demotion replacement model is not registered")
            if replacement_model_id == demotion.model_id:
                raise ValueError("model demotion replacement cannot be the demoted model")

        route_state_status = demotion.routing_state_update.get("status", "blocked")
        if route_state_status not in {"active", "demoted", "blocked"}:
            raise ValueError("model demotion routing-state status is invalid")
        if route_state_status == "active" and not replacement_model_id:
            raise ValueError("active post-demotion routing state requires a replacement model")
        if demotion.routing_state_update.get("active_model_id") == demotion.model_id:
            raise ValueError("post-demotion routing state cannot keep the demoted model active")

        route_version = str(demotion.routing_state_update.get("route_version", "")).strip()
        fallback_route = demotion.routing_state_update.get("fallback_route", {})
        routing_state_after: list[dict[str, Any]] = []
        for routing_role in demotion.routing_roles:
            existing = self.conn.execute(
                """
                SELECT state_id, active_model_id, route_version, status
                FROM model_routing_state
                WHERE task_class=? AND routing_role=?
                """,
                (demotion.task_class, routing_role),
            ).fetchone()
            state_id = existing["state_id"] if existing is not None else new_id()
            previous_state = (
                {
                    "active_model_id": existing["active_model_id"],
                    "route_version": existing["route_version"],
                    "status": existing["status"],
                }
                if existing is not None
                else None
            )
            active_model_id = replacement_model_id if route_state_status == "active" else None
            next_route_version = route_version or (
                f"demoted/{demotion.task_class}/{routing_role}/{demotion.demotion_id}"
            )
            routing_state = {
                "state_id": state_id,
                "task_class": demotion.task_class,
                "routing_role": routing_role,
                "active_model_id": active_model_id,
                "status": route_state_status,
                "route_version": next_route_version,
                "replacement_model_id": replacement_model_id,
                "demotion_id": demotion.demotion_id,
                "previous_state": previous_state,
                "fallback_route": fallback_route,
                "reasons": demotion.reasons,
                "updated_at": demotion.created_at,
            }
            routing_state_after.append(routing_state)

        payload = _model_demotion_payload(demotion, routing_state_after)
        event_id = self.append_event("model_demoted", "model", demotion.demotion_id, payload)
        self.conn.execute(
            """
            INSERT INTO model_demotion_records (
              demotion_id, model_id, task_class, routing_roles_json, reasons_json,
              required_authority, evidence_refs_json, eval_run_ids_json,
              route_decision_ids_json, metrics_json, routing_state_update_json,
              audit_notes, decision_id, authority_effect, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                demotion.demotion_id,
                demotion.model_id,
                demotion.task_class,
                canonical_json(demotion.routing_roles),
                canonical_json(demotion.reasons),
                demotion.required_authority,
                canonical_json(demotion.evidence_refs),
                canonical_json(demotion.eval_run_ids),
                canonical_json(demotion.route_decision_ids),
                canonical_json(demotion.metrics),
                canonical_json(demotion.routing_state_update),
                demotion.audit_notes,
                demotion.decision_id,
                demotion.authority_effect,
                demotion.created_at,
            ),
        )
        self.conn.execute(
            "UPDATE model_candidates SET promotion_state='demoted', last_verified_at=? WHERE model_id=?",
            (demotion.created_at, demotion.model_id),
        )
        for routing_state in routing_state_after:
            self.conn.execute(
                """
                INSERT INTO model_routing_state (
                  state_id, task_class, routing_role, active_model_id, status,
                  route_version, replacement_model_id, demotion_id,
                  previous_state_json, fallback_route_json, reasons_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_class, routing_role) DO UPDATE SET
                  active_model_id=excluded.active_model_id,
                  status=excluded.status,
                  route_version=excluded.route_version,
                  replacement_model_id=excluded.replacement_model_id,
                  demotion_id=excluded.demotion_id,
                  previous_state_json=excluded.previous_state_json,
                  fallback_route_json=excluded.fallback_route_json,
                  reasons_json=excluded.reasons_json,
                  updated_at=excluded.updated_at
                """,
                (
                    routing_state["state_id"],
                    routing_state["task_class"],
                    routing_state["routing_role"],
                    routing_state["active_model_id"],
                    routing_state["status"],
                    routing_state["route_version"],
                    routing_state["replacement_model_id"],
                    routing_state["demotion_id"],
                    canonical_json(routing_state["previous_state"]),
                    canonical_json(routing_state["fallback_route"]),
                    canonical_json(routing_state["reasons"]),
                    routing_state["updated_at"],
                ),
            )
        self.enqueue_projection(event_id, "model_demotion_projection")
        return demotion.demotion_id

    def _get_model_promotion_decision(self, decision_id: str | None) -> sqlite3.Row:
        if not decision_id:
            raise PermissionError("model promotion evidence requires a Decision record")
        decision = self.conn.execute(
            """
            SELECT decision_type, required_authority, status, recommendation
            FROM decisions
            WHERE decision_id=?
            """,
            (decision_id,),
        ).fetchone()
        if decision is None:
            raise ValueError("referenced Decision record does not exist")
        if decision["decision_type"] != "model_promotion":
            raise ValueError("referenced Decision record is not a model-promotion decision")
        return decision

