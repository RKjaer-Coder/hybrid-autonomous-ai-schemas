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
    _source_payload,
    _source_plan_payload,
    _source_acquisition_check_payload,
    _claim_payload,
    _decision_payload,
    _commercial_decision_packet_payload,
    _commercial_decision_recommendation_payload,
    _source_requires_explicit_grant,
    _validate_evidence_bundle,
    _quality_gate_result,
)


class ResearchKernelTransactionMixin:
    def create_research_request(self, request: ResearchRequest) -> str:
        if not request.question.strip():
            raise ValueError("research question is required")
        if request.max_cost_usd < 0:
            raise ValueError("research max_cost_usd must be non-negative")
        payload = {
            "request_id": request.request_id,
            "profile": request.profile,
            "question": request.question,
            "decision_target": request.decision_target,
            "freshness_horizon": request.freshness_horizon,
            "depth": request.depth,
            "source_policy": request.source_policy,
            "evidence_requirements": request.evidence_requirements,
            "max_cost_usd": str(request.max_cost_usd),
            "max_latency": request.max_latency,
            "autonomy_class": request.autonomy_class,
            "status": request.status,
            "created_at": request.created_at,
            "updated_at": request.updated_at,
        }
        event_id = self.append_event("research_request_created", "research_request", request.request_id, payload)
        self.conn.execute(
            """
            INSERT INTO research_requests (
              request_id, profile, question, decision_target, freshness_horizon, depth,
              source_policy_json, evidence_requirements_json, max_cost_usd, max_latency,
              autonomy_class, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request.request_id,
                request.profile,
                request.question,
                request.decision_target,
                request.freshness_horizon,
                request.depth,
                canonical_json(request.source_policy),
                canonical_json(request.evidence_requirements),
                str(request.max_cost_usd),
                request.max_latency,
                request.autonomy_class,
                request.status,
                request.created_at,
                request.updated_at,
            ),
        )
        self.enqueue_projection(event_id, "research_request_projection")
        return request.request_id

    def transition_research_request(self, request_id: str, status: str) -> str:
        valid_statuses = {"queued", "collecting", "synthesizing", "review_needed", "completed", "failed"}
        if status not in valid_statuses:
            raise ValueError(f"unknown research status: {status}")
        row = self.conn.execute("SELECT status FROM research_requests WHERE request_id=?", (request_id,)).fetchone()
        if row is None:
            raise ValueError("research request not found")
        valid_transitions = {
            "queued": {"collecting", "review_needed", "failed"},
            "collecting": {"synthesizing", "review_needed", "failed"},
            "synthesizing": {"completed", "review_needed", "failed"},
            "review_needed": {"collecting", "synthesizing", "failed"},
            "completed": set(),
            "failed": set(),
        }
        if status not in valid_transitions[row["status"]]:
            raise ValueError(f"invalid research transition {row['status']} -> {status}")
        updated_at = now_iso()
        payload = {
            "request_id": request_id,
            "previous_status": row["status"],
            "status": status,
            "updated_at": updated_at,
        }
        event_id = self.append_event("research_request_transitioned", "research_request", request_id, payload)
        self.conn.execute(
            "UPDATE research_requests SET status=?, updated_at=? WHERE request_id=?",
            (status, updated_at, request_id),
        )
        self.enqueue_projection(event_id, "research_request_projection")
        return request_id

    def create_source_plan(self, plan: SourcePlan) -> str:
        row = self.conn.execute(
            "SELECT status, profile, depth FROM research_requests WHERE request_id=?",
            (plan.request_id,),
        ).fetchone()
        if row is None:
            raise ValueError("research request not found")
        if row["status"] != "queued":
            raise ValueError(f"cannot create source plan from research status {row['status']}")
        if row["profile"] != plan.profile or row["depth"] != plan.depth:
            raise ValueError("source plan profile/depth must match request")
        if not plan.planned_sources:
            raise ValueError("source plan requires at least one planned source")
        payload = _source_plan_payload(plan)
        event_id = self.append_event("source_plan_created", "source_plan", plan.source_plan_id, payload)
        self.conn.execute(
            """
            INSERT INTO source_plans (
              source_plan_id, request_id, profile, depth, planned_sources_json,
              retrieval_strategy, created_by, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                plan.source_plan_id,
                plan.request_id,
                plan.profile,
                plan.depth,
                canonical_json(plan.planned_sources),
                plan.retrieval_strategy,
                plan.created_by,
                plan.status,
                plan.created_at,
            ),
        )
        self.enqueue_projection(event_id, "source_plan_projection")
        return plan.source_plan_id

    def record_source_acquisition_check(self, check: SourceAcquisitionCheck) -> str:
        row = self.conn.execute(
            """
            SELECT request_id
            FROM source_plans
            WHERE source_plan_id=?
            """,
            (check.source_plan_id,),
        ).fetchone()
        if row is None:
            raise ValueError("source plan not found")
        if row["request_id"] != check.request_id:
            raise ValueError("source acquisition check request mismatch")
        if check.result == "allowed" and _source_requires_explicit_grant(check.access_method, check.data_class):
            if not check.grant_id:
                raise PermissionError("restricted source acquisition requires a grant")
            grant = self.conn.execute(
                """
                SELECT grant_id, task_id, resource_json, scope_json
                FROM capability_grants
                WHERE grant_id=? AND status='active'
                """,
                (check.grant_id,),
            ).fetchone()
            if grant is None:
                raise PermissionError("restricted source acquisition grant is not active")
            resource = _loads(grant["resource_json"])
            scope = _loads(grant["scope_json"])
            if grant["task_id"] != check.request_id or scope.get("source_plan_id") != check.source_plan_id:
                raise PermissionError("restricted source acquisition grant scope mismatch")
            grant_ref = resource.get("source_ref")
            if grant_ref and grant_ref != check.source_ref:
                raise PermissionError("restricted source acquisition grant source mismatch")
            if resource.get("access_method") and resource.get("access_method") != check.access_method:
                raise PermissionError("restricted source acquisition grant access mismatch")
            if resource.get("data_class") and resource.get("data_class") != check.data_class:
                raise PermissionError("restricted source acquisition grant data-class mismatch")
        payload = _source_acquisition_check_payload(check)
        event_id = self.append_event(
            "source_acquisition_checked",
            "source_plan",
            check.check_id,
            payload,
            check.data_class if check.data_class != "secret_ref" else "secret_ref",
        )
        self.conn.execute(
            """
            INSERT INTO source_acquisition_checks (
              check_id, request_id, source_plan_id, source_ref, access_method,
              data_class, source_type, result, reason, grant_id, checked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                check.check_id,
                check.request_id,
                check.source_plan_id,
                check.source_ref,
                check.access_method,
                check.data_class,
                check.source_type,
                check.result,
                check.reason,
                check.grant_id,
                check.checked_at,
            ),
        )
        self.enqueue_projection(event_id, "source_acquisition_projection")
        return check.check_id

    def create_decision(self, decision: Decision) -> str:
        if not decision.question.strip():
            raise ValueError("decision question is required")
        if len(decision.options) < 2:
            raise ValueError("decision requires at least two options")
        if decision.confidence is not None and not 0.0 <= decision.confidence <= 1.0:
            raise ValueError("decision confidence must be between 0 and 1")
        if decision.status in {"decided", "gated"} and not decision.recommendation:
            raise ValueError("decided or gated decisions require a recommendation")
        if decision.status == "decided" and not decision.verdict:
            raise ValueError("decided decisions require a verdict")
        if decision.required_authority == "operator_gate" and not decision.default_on_timeout:
            raise ValueError("operator-gate decisions require a safe default_on_timeout")
        if self.command.requested_by in {"agent", "model"} and self.command.requested_authority != decision.required_authority:
            raise PermissionError("workers cannot downgrade or assign decision authority")
        if self.command.requested_authority and self.command.requested_authority != decision.required_authority:
            raise PermissionError("command requested authority does not match kernel decision policy")
        for bundle_id in decision.evidence_bundle_ids:
            row = self.conn.execute("SELECT bundle_id FROM evidence_bundles WHERE bundle_id=?", (bundle_id,)).fetchone()
            if row is None:
                raise ValueError("decision references unknown evidence bundle")
        payload = _decision_payload(decision)
        event_id = self.append_event("decision_recorded", "decision", decision.decision_id, payload)
        self.conn.execute(
            """
            INSERT INTO decisions (
              decision_id, decision_type, question, options_json, stakes,
              evidence_bundle_ids_json, evidence_refs_json, requested_by,
              required_authority, authority_policy_version, deadline, status,
              recommendation, verdict, confidence, decisive_factors_json,
              decisive_uncertainty, risk_flags_json, default_on_timeout,
              gate_packet_json, created_at, decided_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision.decision_id,
                decision.decision_type,
                decision.question,
                canonical_json(decision.options),
                decision.stakes,
                canonical_json(decision.evidence_bundle_ids),
                canonical_json(decision.evidence_refs),
                decision.requested_by,
                decision.required_authority,
                decision.authority_policy_version,
                decision.deadline,
                decision.status,
                decision.recommendation,
                decision.verdict,
                decision.confidence,
                canonical_json(decision.decisive_factors),
                decision.decisive_uncertainty,
                canonical_json(decision.risk_flags),
                decision.default_on_timeout,
                canonical_json(decision.gate_packet) if decision.gate_packet is not None else None,
                decision.created_at,
                decision.decided_at,
            ),
        )
        self.enqueue_projection(event_id, "decision_projection")
        return decision.decision_id

    def resolve_decision(
        self,
        decision_id: str,
        *,
        verdict: str,
        decided_by: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> str:
        row = self.conn.execute(
            """
            SELECT decision_id, decision_type, status, required_authority,
                   default_on_timeout, gate_packet_json
            FROM decisions
            WHERE decision_id=?
            """,
            (decision_id,),
        ).fetchone()
        if row is None:
            raise ValueError("decision not found")
        if row["status"] not in {"gated", "deliberating", "proposed"}:
            raise ValueError(f"cannot resolve decision from status {row['status']}")
        if row["required_authority"] == "operator_gate" and self.command.requested_by != "operator":
            raise PermissionError("operator-gate decisions require an operator command")
        if self.command.requested_authority and self.command.requested_authority != row["required_authority"]:
            raise PermissionError("command requested authority does not match Decision record")
        options: list[str] = []
        if row["gate_packet_json"]:
            gate_packet = _loads(row["gate_packet_json"])
            options = [str(option) for option in gate_packet.get("options", [])]
        if options and verdict not in options:
            raise ValueError("decision verdict is not one of the gate options")
        if confidence is not None and not 0.0 <= confidence <= 1.0:
            raise ValueError("decision resolution confidence must be between 0 and 1")
        decided_at = now_iso()
        payload = {
            "decision_id": decision_id,
            "decision_type": row["decision_type"],
            "previous_status": row["status"],
            "status": "decided",
            "verdict": verdict,
            "confidence": confidence,
            "decided_by": decided_by,
            "notes": notes,
            "decided_at": decided_at,
            "authority_required": row["required_authority"],
            "default_on_timeout": row["default_on_timeout"],
        }
        event_id = self.append_event("decision_resolved", "decision", decision_id, payload, actor_type="operator", actor_id=decided_by)
        self.conn.execute(
            """
            UPDATE decisions
            SET status='decided', verdict=?, confidence=COALESCE(?, confidence), decided_at=?
            WHERE decision_id=?
            """,
            (verdict, confidence, decided_at, decision_id),
        )
        self.enqueue_projection(event_id, "decision_projection")
        return decision_id

    def commit_evidence_bundle(self, bundle: EvidenceBundle) -> str:
        row = self.conn.execute(
            "SELECT status, profile, source_policy_json, evidence_requirements_json FROM research_requests WHERE request_id=?",
            (bundle.request_id,),
        ).fetchone()
        if row is None:
            raise ValueError("research request not found")
        if row["status"] not in {"synthesizing", "review_needed"}:
            raise ValueError(f"cannot commit evidence bundle from research status {row['status']}")
        plan = self.conn.execute(
            "SELECT request_id FROM source_plans WHERE source_plan_id=?",
            (bundle.source_plan_id,),
        ).fetchone()
        if plan is None or plan["request_id"] != bundle.request_id:
            raise ValueError("evidence bundle source plan does not belong to request")
        if not 0.0 <= bundle.confidence <= 1.0:
            raise ValueError("evidence bundle confidence must be between 0 and 1")
        source_ids = {source.source_id for source in bundle.sources}
        missing_sources = sorted(
            source_id for claim in bundle.claims for source_id in claim.source_ids if source_id not in source_ids
        )
        if missing_sources:
            raise ValueError(f"claim references missing source ids: {', '.join(missing_sources)}")
        sources = [_source_payload(source) for source in bundle.sources]
        claims = [_claim_payload(claim) for claim in bundle.claims]
        quality_checks = _validate_evidence_bundle(
            profile=row["profile"],
            source_policy=_loads(row["source_policy_json"]),
            evidence_requirements=_loads(row["evidence_requirements_json"]),
            bundle=bundle,
        )
        quality_result = _quality_gate_result(quality_checks, bundle.quality_gate_result)
        if quality_result == "fail" and bundle.quality_gate_result != "fail":
            raise ValueError("evidence bundle failed quality gate")
        gate_event_id = new_id()
        gate_payload = {
            "gate_event_id": gate_event_id,
            "request_id": bundle.request_id,
            "bundle_id": bundle.bundle_id,
            "source_plan_id": bundle.source_plan_id,
            "profile": row["profile"],
            "result": quality_result,
            "confidence": bundle.confidence,
            "checks": quality_checks,
            "created_at": bundle.created_at,
        }
        quality_event_id = self.append_event("quality_gate_evaluated", "gate", gate_event_id, gate_payload)
        self.conn.execute(
            """
            INSERT INTO quality_gate_events (
              gate_event_id, request_id, bundle_id, source_plan_id, profile,
              result, confidence, checks_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                gate_event_id,
                bundle.request_id,
                bundle.bundle_id,
                bundle.source_plan_id,
                row["profile"],
                quality_result,
                bundle.confidence,
                canonical_json(quality_checks),
                bundle.created_at,
            ),
        )
        self.enqueue_projection(quality_event_id, "quality_gate_projection")
        payload = {
            "bundle_id": bundle.bundle_id,
            "request_id": bundle.request_id,
            "source_plan_id": bundle.source_plan_id,
            "sources": sources,
            "claims": claims,
            "contradictions": bundle.contradictions,
            "unsupported_claims": bundle.unsupported_claims,
            "freshness_summary": bundle.freshness_summary,
            "confidence": bundle.confidence,
            "uncertainty": bundle.uncertainty,
            "counter_thesis": bundle.counter_thesis,
            "quality_gate_result": quality_result,
            "data_classes": bundle.data_classes,
            "retention_policy": bundle.retention_policy,
            "created_at": bundle.created_at,
        }
        event_id = self.append_event("evidence_bundle_committed", "evidence_bundle", bundle.bundle_id, payload)
        self.conn.execute(
            """
            INSERT INTO evidence_bundles (
              bundle_id, request_id, source_plan_id, sources_json, claims_json,
              contradictions_json, unsupported_claims_json, freshness_summary, confidence,
              uncertainty, counter_thesis, quality_gate_result, data_classes_json,
              retention_policy, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bundle.bundle_id,
                bundle.request_id,
                bundle.source_plan_id,
                canonical_json(sources),
                canonical_json(claims),
                canonical_json(bundle.contradictions),
                canonical_json(bundle.unsupported_claims),
                bundle.freshness_summary,
                bundle.confidence,
                bundle.uncertainty,
                bundle.counter_thesis,
                quality_result,
                canonical_json(bundle.data_classes),
                bundle.retention_policy,
                bundle.created_at,
            ),
        )
        self.conn.execute(
            "UPDATE research_requests SET status='completed', updated_at=? WHERE request_id=?",
            (bundle.created_at, bundle.request_id),
        )
        self.enqueue_projection(event_id, "evidence_bundle_projection")
        return bundle.bundle_id

    def create_commercial_decision_packet(self, packet: OpportunityProjectDecisionPacket) -> str:
        row = self.conn.execute(
            """
            SELECT r.profile, r.decision_target, e.quality_gate_result
            FROM evidence_bundles e
            JOIN research_requests r ON r.request_id = e.request_id
            WHERE e.bundle_id = ? AND e.request_id = ?
            """,
            (packet.evidence_bundle_id, packet.request_id),
        ).fetchone()
        if row is None:
            raise ValueError("evidence bundle not found for decision packet")
        if row["profile"] not in {"commercial", "project_support"}:
            raise ValueError("commercial decision packet requires commercial or project_support evidence")
        if row["decision_target"] and row["decision_target"] != packet.decision_target:
            raise ValueError("decision packet target does not match research request")
        if not packet.decision_target:
            raise ValueError("project-pulled commercial decision packet requires a decision target")
        decision = self.conn.execute(
            """
            SELECT decision_type, required_authority, status, recommendation
            FROM decisions
            WHERE decision_id=?
            """,
            (packet.decision_id,),
        ).fetchone()
        if decision is None:
            raise ValueError("commercial decision packet requires a first-class Decision record")
        if decision["decision_type"] not in {"project_approval", "commercial_strategy"}:
            raise ValueError("commercial decision packet Decision type mismatch")
        if decision["required_authority"] != packet.required_authority:
            raise ValueError("commercial decision packet authority must match Decision record")
        if decision["status"] != packet.status:
            raise ValueError("commercial decision packet status must match Decision record")
        if decision["recommendation"] != packet.recommendation:
            raise ValueError("commercial decision packet recommendation must match Decision record")
        payload = _commercial_decision_packet_payload(packet)
        event_id = self.append_event("commercial_decision_packet_created", "decision", packet.packet_id, payload)
        self.conn.execute(
            """
            INSERT INTO commercial_decision_packets (
              packet_id, decision_id, request_id, evidence_bundle_id, decision_target, question,
              recommendation, required_authority, opportunity_json, project_json,
              gate_packet_json, evidence_used_json, risk_flags_json,
              default_on_timeout, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.packet_id,
                packet.decision_id,
                packet.request_id,
                packet.evidence_bundle_id,
                packet.decision_target,
                packet.question,
                packet.recommendation,
                packet.required_authority,
                canonical_json(packet.opportunity),
                canonical_json(packet.project),
                canonical_json(packet.gate_packet),
                canonical_json(packet.evidence_used),
                canonical_json(packet.risk_flags),
                packet.default_on_timeout,
                packet.status,
                packet.created_at,
            ),
        )
        self.enqueue_projection(event_id, "commercial_decision_packet_projection")
        return packet.packet_id

    def create_commercial_decision_recommendation(
        self,
        recommendation: CommercialDecisionRecommendationRecord,
    ) -> str:
        if recommendation.recommendation_authority not in {"single_agent", "council"}:
            raise ValueError("commercial recommendation authority must be single_agent or council")
        if not 0.0 <= recommendation.confidence <= 1.0:
            raise ValueError("commercial recommendation confidence must be between 0 and 1")
        packet = self.conn.execute(
            """
            SELECT p.decision_id, p.request_id, p.evidence_bundle_id, p.recommendation,
                   p.risk_flags_json, p.default_on_timeout, p.status,
                   d.required_authority, d.default_on_timeout AS decision_default_on_timeout
            FROM commercial_decision_packets p
            JOIN decisions d ON d.decision_id = p.decision_id
            WHERE p.packet_id=?
            """,
            (recommendation.packet_id,),
        ).fetchone()
        if packet is None:
            raise ValueError("commercial decision packet not found for recommendation")
        if packet["decision_id"] != recommendation.decision_id:
            raise ValueError("commercial recommendation Decision id must match packet")
        if packet["request_id"] != recommendation.request_id:
            raise ValueError("commercial recommendation request id must match packet")
        if packet["evidence_bundle_id"] != recommendation.evidence_bundle_id:
            raise ValueError("commercial recommendation evidence bundle must match packet")
        if packet["recommendation"] != recommendation.recommendation:
            raise ValueError("commercial recommendation verdict must match packet recommendation")
        if packet["required_authority"] != "operator_gate":
            raise PermissionError("commercial recommendation records preserve operator-gate final authority")
        defaults = recommendation.operator_gate_defaults
        if defaults.get("required_authority") != "operator_gate":
            raise ValueError("commercial recommendation must preserve operator-gate authority default")
        if defaults.get("default_on_timeout") != packet["default_on_timeout"]:
            raise ValueError("commercial recommendation timeout default must match packet")
        if defaults.get("decision_default_on_timeout") != packet["decision_default_on_timeout"]:
            raise ValueError("commercial recommendation decision timeout default must match Decision record")
        quality_gate_context = recommendation.quality_gate_context
        if quality_gate_context.get("bundle_id") != recommendation.evidence_bundle_id:
            raise ValueError("commercial recommendation quality context must reference evidence bundle")
        if quality_gate_context.get("request_id") != recommendation.request_id:
            raise ValueError("commercial recommendation quality context must reference research request")
        if not recommendation.evidence_refs:
            raise ValueError("commercial recommendation requires durable evidence references")
        if f"kernel:evidence_bundles/{recommendation.evidence_bundle_id}" not in recommendation.evidence_refs:
            raise ValueError("commercial recommendation must preserve EvidenceBundle lineage")

        payload = _commercial_decision_recommendation_payload(recommendation)
        event_id = self.append_event(
            "commercial_decision_recommendation_recorded",
            "decision",
            recommendation.record_id,
            payload,
        )
        self.conn.execute(
            """
            INSERT INTO commercial_decision_recommendations (
              record_id, packet_id, decision_id, request_id, evidence_bundle_id,
              recommendation_authority, recommendation, confidence,
              decisive_factors_json, decisive_uncertainty, evidence_used_json,
              evidence_refs_json, quality_gate_context_json, risk_flags_json,
              operator_gate_defaults_json, rationale, model_routes_used_json,
              degraded, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                recommendation.record_id,
                recommendation.packet_id,
                recommendation.decision_id,
                recommendation.request_id,
                recommendation.evidence_bundle_id,
                recommendation.recommendation_authority,
                recommendation.recommendation,
                recommendation.confidence,
                canonical_json(recommendation.decisive_factors),
                recommendation.decisive_uncertainty,
                canonical_json(recommendation.evidence_used),
                canonical_json(recommendation.evidence_refs),
                canonical_json(recommendation.quality_gate_context),
                canonical_json(recommendation.risk_flags),
                canonical_json(recommendation.operator_gate_defaults),
                recommendation.rationale,
                canonical_json(recommendation.model_routes_used),
                1 if recommendation.degraded else 0,
                recommendation.created_at,
            ),
        )
        self.enqueue_projection(event_id, "commercial_decision_recommendation_projection")
        return recommendation.record_id

