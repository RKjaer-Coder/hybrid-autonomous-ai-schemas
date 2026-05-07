from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from .records import (
    ArtifactRef,
    Budget,
    CapabilityGrant,
    Command,
    CommercialDecisionRecommendationRecord,
    Decision,
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
    ResearchRequest,
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

KERNEL_EVENT_SCHEMA_VERSION = 1
KERNEL_POLICY_VERSION = "v3.1-foundation"

LEGACY_BOUNDARIES: dict[str, str] = {
    "immune": "adapt: safety validation and broker-bypass helper only",
    "financial_router": "adapt: route and spend helper subordinate to kernel budgets",
    "skills/local_forward_proxy.py": "adapt: network/provider proxy behind grants",
    "council": "adapt: deliberation recommendation only",
    "eval": "adapt: replay/eval substrate, not promotion authority yet",
    "harness_variants.py": "adapt: eval substrate behind kernel decisions",
    "kernel/runtime_compat.py": "wrap: CLI/proof compatibility harness, never kernel authority",
    "skills/runtime.py": "wrap: thin compatibility entrypoint for kernel runtime",
    "schemas/*.sql": "convert-to-projection: legacy domain schemas are non-authoritative",
}


def create_kernel_database(db_path: str | Path) -> None:
    root = Path(__file__).resolve().parents[1]
    schema_path = root / "schemas" / "kernel.sql"
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.executescript(schema_path.read_text(encoding="utf-8"))
        conn.commit()


@dataclass
class ReplayState:
    budgets: dict[str, dict[str, Any]] = field(default_factory=dict)
    grants: dict[str, dict[str, Any]] = field(default_factory=dict)
    side_effects: dict[str, dict[str, Any]] = field(default_factory=dict)
    artifact_refs: dict[str, dict[str, Any]] = field(default_factory=dict)
    research_requests: dict[str, dict[str, Any]] = field(default_factory=dict)
    source_plans: dict[str, dict[str, Any]] = field(default_factory=dict)
    source_acquisition_checks: dict[str, dict[str, Any]] = field(default_factory=dict)
    decisions: dict[str, dict[str, Any]] = field(default_factory=dict)
    quality_gate_events: dict[str, dict[str, Any]] = field(default_factory=dict)
    evidence_bundles: dict[str, dict[str, Any]] = field(default_factory=dict)
    commercial_decision_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    commercial_decision_recommendations: dict[str, dict[str, Any]] = field(default_factory=dict)
    projects: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_tasks: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_task_assignments: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_outcomes: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_artifact_receipts: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_customer_feedback: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_revenue_attributions: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_operator_load: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_commercial_rollups: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_status_rollups: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_close_decision_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_portfolio_decision_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_portfolio_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_scheduling_intents: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_scheduling_priority_change_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_scheduling_priority_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_scheduling_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_customer_visible_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_customer_commitments: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_customer_commitment_receipts: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_customer_visible_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    model_task_classes: dict[str, dict[str, Any]] = field(default_factory=dict)
    model_candidates: dict[str, dict[str, Any]] = field(default_factory=dict)
    local_offload_eval_sets: dict[str, dict[str, Any]] = field(default_factory=dict)
    holdout_policies: dict[str, dict[str, Any]] = field(default_factory=dict)
    holdout_use_records: dict[str, dict[str, Any]] = field(default_factory=dict)
    model_eval_runs: dict[str, dict[str, Any]] = field(default_factory=dict)
    model_route_decisions: dict[str, dict[str, Any]] = field(default_factory=dict)
    model_promotion_decision_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    model_demotion_records: dict[str, dict[str, Any]] = field(default_factory=dict)
    model_routing_state: dict[str, dict[str, Any]] = field(default_factory=dict)
    inspection_tasks: list[dict[str, Any]] = field(default_factory=list)


class KernelStore:
    """SQLite-backed v3.1 critical-state authority.

    The writer API intentionally routes every critical mutation through one
    `BEGIN IMMEDIATE` transaction: command row, event row, derived-state row,
    and projection/outbox placeholders commit or roll back together.
    """

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        create_kernel_database(self.db_path)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=5.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def execute_command(
        self,
        command: Command,
        handler: Callable[["KernelTransaction"], Any],
    ) -> Any:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                existing = self._get_existing_command(conn, command)
                if existing is not None:
                    conn.execute("COMMIT")
                    return {
                        "idempotent": True,
                        "command_id": existing["command_id"],
                        "status": existing["status"],
                        "result_event_id": existing["result_event_id"],
                    }
                self._insert_command(conn, command)
                tx = KernelTransaction(conn, command)
                result = handler(tx)
                conn.execute(
                    "UPDATE commands SET status='applied', result_event_id=COALESCE(?, result_event_id) WHERE command_id=?",
                    (tx.last_event_id, command.command_id),
                )
                conn.execute("COMMIT")
                return result
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def issue_capability_grant(self, command: Command, grant: CapabilityGrant) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.issue_capability_grant(grant)

        return self.execute_command(command, handler)

    def use_grant(
        self,
        command: Command,
        grant_id: str,
        subject_type: str,
        subject_id: str,
        capability_type: str,
        action: str,
    ) -> bool:
        def handler(tx: KernelTransaction) -> bool:
            return tx.use_grant(grant_id, subject_type, subject_id, capability_type, action)

        return self.execute_command(command, handler)

    def create_budget(self, command: Command, budget: Budget) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.create_budget(budget)

        return self.execute_command(command, handler)

    def reserve_budget(
        self,
        command: Command,
        budget_id: str,
        amount_usd: Decimal,
        reservation_id: str | None = None,
    ) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.reserve_budget(budget_id, amount_usd, reservation_id)

        return self.execute_command(command, handler)

    def prepare_side_effect(self, command: Command, intent: SideEffectIntent) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.prepare_side_effect(intent)

        return self.execute_command(command, handler)

    def record_side_effect_receipt(self, command: Command, receipt: SideEffectReceipt) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_side_effect_receipt(receipt)

        return self.execute_command(command, handler)

    def create_research_request(self, command: Command, request: ResearchRequest) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.create_research_request(request)

        return self.execute_command(command, handler)

    def transition_research_request(self, command: Command, request_id: str, status: str) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.transition_research_request(request_id, status)

        return self.execute_command(command, handler)

    def create_source_plan(self, command: Command, plan: SourcePlan) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.create_source_plan(plan)

        return self.execute_command(command, handler)

    def record_source_acquisition_check(self, command: Command, check: SourceAcquisitionCheck) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_source_acquisition_check(check)

        return self.execute_command(command, handler)

    def create_decision(self, command: Command, decision: Decision) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.create_decision(decision)

        return self.execute_command(command, handler)

    def resolve_decision(
        self,
        command: Command,
        decision_id: str,
        *,
        verdict: str,
        decided_by: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.resolve_decision(
                decision_id,
                verdict=verdict,
                decided_by=decided_by,
                notes=notes,
                confidence=confidence,
            )

        return self.execute_command(command, handler)

    def commit_evidence_bundle(self, command: Command, bundle: EvidenceBundle) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.commit_evidence_bundle(bundle)

        return self.execute_command(command, handler)

    def create_commercial_decision_packet(
        self,
        command: Command,
        packet: OpportunityProjectDecisionPacket,
    ) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.create_commercial_decision_packet(packet)

        return self.execute_command(command, handler)

    def create_commercial_decision_recommendation(
        self,
        command: Command,
        recommendation: CommercialDecisionRecommendationRecord,
    ) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.create_commercial_decision_recommendation(recommendation)

        return self.execute_command(command, handler)

    def create_project(self, command: Command, project: Project) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.create_project(project)

        return self.execute_command(command, handler)

    def create_project_task(self, command: Command, task: ProjectTask) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.create_project_task(task)

        return self.execute_command(command, handler)

    def assign_project_task(self, command: Command, assignment: ProjectTaskAssignment) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.assign_project_task(assignment)

        return self.execute_command(command, handler)

    def transition_project_task(self, command: Command, task_id: str, status: str, reason: str) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.transition_project_task(task_id, status, reason)

        return self.execute_command(command, handler)

    def record_project_followup_delivery(
        self,
        command: Command,
        task_id: str,
        *,
        artifact_ref: str,
        summary: str,
        data_class: str = "internal",
        delivery_channel: str = "local_workspace",
        side_effect_intent_id: str | None = None,
        side_effect_receipt_id: str | None = None,
        customer_visible: bool = False,
        metrics: dict[str, Any] | None = None,
        feedback: dict[str, Any] | None = None,
        revenue_impact: dict[str, Any] | None = None,
        operator_load_actual: str | None = None,
        next_recommendation: str | None = None,
    ) -> dict[str, Any]:
        def handler(tx: KernelTransaction) -> dict[str, Any]:
            return tx.record_project_followup_delivery(
                task_id,
                artifact_ref=artifact_ref,
                summary=summary,
                data_class=data_class,
                delivery_channel=delivery_channel,
                side_effect_intent_id=side_effect_intent_id,
                side_effect_receipt_id=side_effect_receipt_id,
                customer_visible=customer_visible,
                metrics=metrics,
                feedback=feedback,
                revenue_impact=revenue_impact,
                operator_load_actual=operator_load_actual,
                next_recommendation=next_recommendation,
            )

        return self.execute_command(command, handler)

    def record_project_operate_followup_outcome(
        self,
        command: Command,
        task_id: str,
        *,
        summary: str,
        internal_result_ref: str,
        operator_load_minutes: int,
        operator_load_source: str,
        operate_followup_type: str | None = None,
        metrics: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        revenue_impact: dict[str, Any] | None = None,
        side_effect_intent_id: str | None = None,
        side_effect_receipt_id: str | None = None,
        external_commitment_change: bool = False,
        operator_load_notes: str | None = None,
    ) -> dict[str, Any]:
        def handler(tx: KernelTransaction) -> dict[str, Any]:
            return tx.record_project_operate_followup_outcome(
                task_id,
                summary=summary,
                internal_result_ref=internal_result_ref,
                operator_load_minutes=operator_load_minutes,
                operator_load_source=operator_load_source,
                operate_followup_type=operate_followup_type,
                metrics=metrics,
                result=result,
                revenue_impact=revenue_impact,
                side_effect_intent_id=side_effect_intent_id,
                side_effect_receipt_id=side_effect_receipt_id,
                external_commitment_change=external_commitment_change,
                operator_load_notes=operator_load_notes,
            )

        return self.execute_command(command, handler)

    def record_project_scheduling_task_outcome(
        self,
        command: Command,
        task_id: str,
        *,
        summary: str,
        internal_result_ref: str,
        result: dict[str, Any] | None = None,
        metrics: dict[str, Any] | None = None,
        revenue_impact: dict[str, Any] | None = None,
        side_effect_intent_id: str | None = None,
        side_effect_receipt_id: str | None = None,
        external_commitment_change: bool = False,
    ) -> dict[str, Any]:
        def handler(tx: KernelTransaction) -> dict[str, Any]:
            return tx.record_project_scheduling_task_outcome(
                task_id,
                summary=summary,
                internal_result_ref=internal_result_ref,
                result=result,
                metrics=metrics,
                revenue_impact=revenue_impact,
                side_effect_intent_id=side_effect_intent_id,
                side_effect_receipt_id=side_effect_receipt_id,
                external_commitment_change=external_commitment_change,
            )

        return self.execute_command(command, handler)

    def record_project_outcome(self, command: Command, outcome: ProjectOutcome) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_project_outcome(outcome)

        return self.execute_command(command, handler)

    def record_project_artifact_receipt(self, command: Command, receipt: ProjectArtifactReceipt) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_project_artifact_receipt(receipt)

        return self.execute_command(command, handler)

    def record_project_customer_feedback(self, command: Command, feedback: ProjectCustomerFeedback) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_project_customer_feedback(feedback)

        return self.execute_command(command, handler)

    def record_project_customer_commitment_receipt(
        self,
        command: Command,
        receipt: ProjectCustomerCommitmentReceipt,
    ) -> dict[str, str | None]:
        def handler(tx: KernelTransaction) -> dict[str, str | None]:
            return tx.record_project_customer_commitment_receipt(receipt)

        return self.execute_command(command, handler)

    def record_project_revenue_attribution(self, command: Command, attribution: ProjectRevenueAttribution) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_project_revenue_attribution(attribution)

        return self.execute_command(command, handler)

    def record_project_operator_load(self, command: Command, load: ProjectOperatorLoadRecord) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_project_operator_load(load)

        return self.execute_command(command, handler)

    def record_project_post_ship_evidence(
        self,
        command: Command,
        artifact_receipt_id: str,
        *,
        feedback: ProjectCustomerFeedback,
        revenue: ProjectRevenueAttribution,
        operator_load: ProjectOperatorLoadRecord,
    ) -> dict[str, str]:
        def handler(tx: KernelTransaction) -> dict[str, str]:
            return tx.record_project_post_ship_evidence(
                artifact_receipt_id,
                feedback=feedback,
                revenue=revenue,
                operator_load=operator_load,
            )

        return self.execute_command(command, handler)

    def derive_project_status_rollup(self, command: Command, project_id: str) -> ProjectStatusRollup:
        def handler(tx: KernelTransaction) -> ProjectStatusRollup:
            return tx.derive_project_status_rollup(project_id)

        return self.execute_command(command, handler)

    def create_project_close_decision(
        self,
        command: Command,
        project_id: str,
        rollup_id: str | None = None,
    ) -> ProjectCloseDecisionPacket:
        def handler(tx: KernelTransaction) -> ProjectCloseDecisionPacket:
            return tx.create_project_close_decision(project_id, rollup_id=rollup_id)

        return self.execute_command(command, handler)

    def resolve_project_close_decision(
        self,
        command: Command,
        packet_id: str,
        *,
        verdict: str,
        decided_by: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        def handler(tx: KernelTransaction) -> dict[str, Any]:
            return tx.resolve_project_close_decision(
                packet_id,
                verdict=verdict,
                decided_by=decided_by,
                notes=notes,
                confidence=confidence,
            )

        return self.execute_command(command, handler)

    def compare_project_replay_to_projection(
        self,
        command: Command,
        project_id: str,
    ) -> ProjectReplayProjectionComparison:
        def handler(tx: KernelTransaction) -> ProjectReplayProjectionComparison:
            return tx.compare_project_replay_to_projection(project_id)

        return self.execute_command(command, handler)

    def create_project_portfolio_decision_packet(
        self,
        command: Command,
        project_ids: list[str],
        *,
        scope: str = "active_commercial_projects",
        constraints: dict[str, Any] | None = None,
    ) -> ProjectPortfolioDecisionPacket:
        def handler(tx: KernelTransaction) -> ProjectPortfolioDecisionPacket:
            return tx.create_project_portfolio_decision_packet(
                project_ids,
                scope=scope,
                constraints=constraints,
            )

        return self.execute_command(command, handler)

    def resolve_project_portfolio_decision(
        self,
        command: Command,
        packet_id: str,
        *,
        verdict: str,
        decided_by: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        def handler(tx: KernelTransaction) -> dict[str, Any]:
            return tx.resolve_project_portfolio_decision(
                packet_id,
                verdict=verdict,
                decided_by=decided_by,
                notes=notes,
                confidence=confidence,
            )

        return self.execute_command(command, handler)

    def compare_project_portfolio_replay_to_projection(
        self,
        command: Command,
        packet_id: str,
    ) -> ProjectPortfolioReplayProjectionComparison:
        def handler(tx: KernelTransaction) -> ProjectPortfolioReplayProjectionComparison:
            return tx.compare_project_portfolio_replay_to_projection(packet_id)

        return self.execute_command(command, handler)

    def create_project_scheduling_intent(
        self,
        command: Command,
        packet_id: str,
        *,
        scheduling_window: str = "next_internal_cycle",
    ) -> ProjectSchedulingIntent:
        def handler(tx: KernelTransaction) -> ProjectSchedulingIntent:
            return tx.create_project_scheduling_intent(packet_id, scheduling_window=scheduling_window)

        return self.execute_command(command, handler)

    def compare_project_scheduling_replay_to_projection(
        self,
        command: Command,
        intent_id: str,
    ) -> ProjectSchedulingReplayProjectionComparison:
        def handler(tx: KernelTransaction) -> ProjectSchedulingReplayProjectionComparison:
            return tx.compare_project_scheduling_replay_to_projection(intent_id)

        return self.execute_command(command, handler)

    def create_project_scheduling_priority_change_packet(
        self,
        command: Command,
        intent_id: str,
    ) -> ProjectSchedulingPriorityChangePacket:
        def handler(tx: KernelTransaction) -> ProjectSchedulingPriorityChangePacket:
            return tx.create_project_scheduling_priority_change_packet(intent_id)

        return self.execute_command(command, handler)

    def resolve_project_scheduling_priority_change_packet(
        self,
        command: Command,
        packet_id: str,
        *,
        verdict: str,
        decided_by: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        def handler(tx: KernelTransaction) -> dict[str, Any]:
            return tx.resolve_project_scheduling_priority_change_packet(
                packet_id,
                verdict=verdict,
                decided_by=decided_by,
                notes=notes,
                confidence=confidence,
            )

        return self.execute_command(command, handler)

    def compare_project_scheduling_priority_replay_to_projection(
        self,
        command: Command,
        packet_id: str,
    ) -> ProjectSchedulingPriorityReplayProjectionComparison:
        def handler(tx: KernelTransaction) -> ProjectSchedulingPriorityReplayProjectionComparison:
            return tx.compare_project_scheduling_priority_replay_to_projection(packet_id)

        return self.execute_command(command, handler)

    def create_project_customer_visible_packet(
        self,
        command: Command,
        outcome_id: str,
        *,
        packet_type: str,
        customer_ref: str,
        channel: str,
        subject: str,
        summary: str,
        payload_ref: str,
        side_effect_intent_id: str,
    ) -> ProjectCustomerVisiblePacket:
        def handler(tx: KernelTransaction) -> ProjectCustomerVisiblePacket:
            return tx.create_project_customer_visible_packet(
                outcome_id,
                packet_type=packet_type,
                customer_ref=customer_ref,
                channel=channel,
                subject=subject,
                summary=summary,
                payload_ref=payload_ref,
                side_effect_intent_id=side_effect_intent_id,
            )

        return self.execute_command(command, handler)

    def resolve_project_customer_visible_packet(
        self,
        command: Command,
        packet_id: str,
        *,
        verdict: str,
        side_effect_receipt_id: str | None = None,
        decided_by: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        def handler(tx: KernelTransaction) -> dict[str, Any]:
            return tx.resolve_project_customer_visible_packet(
                packet_id,
                verdict=verdict,
                side_effect_receipt_id=side_effect_receipt_id,
                decided_by=decided_by,
                notes=notes,
                confidence=confidence,
            )

        return self.execute_command(command, handler)

    def compare_project_customer_visible_replay_to_projection(
        self,
        command: Command,
        packet_id: str,
    ) -> ProjectCustomerVisibleReplayProjectionComparison:
        def handler(tx: KernelTransaction) -> ProjectCustomerVisibleReplayProjectionComparison:
            return tx.compare_project_customer_visible_replay_to_projection(packet_id)

        return self.execute_command(command, handler)

    def register_model_task_class(self, command: Command, task_class: ModelTaskClassRecord) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.register_model_task_class(task_class)

        return self.execute_command(command, handler)

    def register_model_candidate(self, command: Command, candidate: ModelCandidate) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.register_model_candidate(candidate)

        return self.execute_command(command, handler)

    def create_holdout_policy(self, command: Command, policy: HoldoutPolicy) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.create_holdout_policy(policy)

        return self.execute_command(command, handler)

    def register_local_offload_eval_set(self, command: Command, eval_set: LocalOffloadEvalSet) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.register_local_offload_eval_set(eval_set)

        return self.execute_command(command, handler)

    def record_holdout_use(self, command: Command, holdout_use: HoldoutUseRecord) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_holdout_use(holdout_use)

        return self.execute_command(command, handler)

    def record_model_eval_run(self, command: Command, eval_run: ModelEvalRun) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_model_eval_run(eval_run)

        return self.execute_command(command, handler)

    def record_model_route_decision(self, command: Command, route_decision: ModelRouteDecision) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_model_route_decision(route_decision)

        return self.execute_command(command, handler)

    def create_model_promotion_decision_packet(
        self,
        command: Command,
        packet: ModelPromotionDecisionPacket,
    ) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.create_model_promotion_decision_packet(packet)

        return self.execute_command(command, handler)

    def record_model_demotion(self, command: Command, demotion: ModelDemotionRecord) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_model_demotion(demotion)

        return self.execute_command(command, handler)

    def replay_critical_state(self) -> ReplayState:
        with self.connect() as conn:
            return self._replay_from_connection(conn)

    @staticmethod
    def _replay_from_connection(conn: sqlite3.Connection) -> ReplayState:
        state = ReplayState()
        expected_prev: str | None = None
        rows = conn.execute("SELECT * FROM events ORDER BY event_seq ASC").fetchall()
        for row in rows:
            if row["event_schema_version"] != KERNEL_EVENT_SCHEMA_VERSION:
                raise ValueError(f"unsupported event schema version: {row['event_schema_version']}")
            if row["prev_event_hash"] != expected_prev:
                raise ValueError("event hash chain mismatch")
            payload = _loads(row["payload_json"])
            expected_hash = KernelStore._event_hash(
                row["event_id"],
                row["event_seq"],
                row["event_schema_version"],
                row["event_type"],
                row["entity_type"],
                row["entity_id"],
                row["transaction_id"],
                row["command_id"],
                row["payload_hash"],
                row["prev_event_hash"],
            )
            if row["event_hash"] != expected_hash:
                raise ValueError(f"event hash mismatch for {row['event_id']}")
            expected_prev = row["event_hash"]
            KernelStore._apply_replay_event(state, row["event_type"], row["entity_id"], payload)
        return state

    def legacy_authority_status(self) -> dict[str, str]:
        return dict(LEGACY_BOUNDARIES)

    def _get_existing_command(self, conn: sqlite3.Connection, command: Command) -> sqlite3.Row | None:
        row = conn.execute(
            """
            SELECT command_id, payload_hash, status, result_event_id
            FROM commands
            WHERE command_id=? OR idempotency_key=?
            """,
            (command.command_id, command.idempotency_key),
        ).fetchone()
        if row is None:
            return None
        if row["payload_hash"] != command.payload_hash:
            raise ValueError("idempotency key or command id reused with different payload")
        return row

    def _insert_command(self, conn: sqlite3.Connection, command: Command) -> None:
        conn.execute(
            """
            INSERT INTO commands (
              command_id, command_type, requested_by, requester_id, target_entity_type,
              target_entity_id, requested_authority, payload_hash, payload_json,
              idempotency_key, submitted_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'accepted')
            """,
            (
                command.command_id,
                command.command_type,
                command.requested_by,
                command.requester_id,
                command.target_entity_type,
                command.target_entity_id,
                command.requested_authority,
                command.payload_hash,
                canonical_json(command.payload),
                command.idempotency_key,
                command.submitted_at,
            ),
        )

    @staticmethod
    def _event_hash(
        event_id: str,
        event_seq: int,
        event_schema_version: int,
        event_type: str,
        entity_type: str,
        entity_id: str,
        transaction_id: str,
        command_id: str | None,
        event_payload_hash: str,
        prev_event_hash: str | None,
    ) -> str:
        return sha256_text(
            canonical_json(
                {
                    "event_id": event_id,
                    "event_seq": event_seq,
                    "event_schema_version": event_schema_version,
                    "event_type": event_type,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "transaction_id": transaction_id,
                    "command_id": command_id,
                    "payload_hash": event_payload_hash,
                    "prev_event_hash": prev_event_hash,
                }
            )
        )

    @staticmethod
    def _apply_replay_event(state: ReplayState, event_type: str, entity_id: str, payload: dict[str, Any]) -> None:
        if event_type == "budget_created":
            state.budgets[entity_id] = {
                "cap_usd": Decimal(payload["cap_usd"]),
                "spent_usd": Decimal(payload["spent_usd"]),
                "reserved_usd": Decimal(payload["reserved_usd"]),
                "status": payload["status"],
            }
        elif event_type == "spend_reserved":
            budget = state.budgets[payload["budget_id"]]
            budget["reserved_usd"] += Decimal(payload["amount_usd"])
        elif event_type == "capability_granted":
            state.grants[entity_id] = dict(payload)
        elif event_type == "capability_used":
            state.grants[entity_id]["used_count"] += 1
        elif event_type == "artifact_ref_created":
            state.artifact_refs[entity_id] = dict(payload)
        elif event_type == "research_request_created":
            state.research_requests[entity_id] = dict(payload)
        elif event_type == "research_request_transitioned":
            request = state.research_requests[entity_id]
            request["status"] = payload["status"]
            request["updated_at"] = payload["updated_at"]
        elif event_type == "source_plan_created":
            state.source_plans[entity_id] = dict(payload)
        elif event_type == "source_acquisition_checked":
            state.source_acquisition_checks[entity_id] = dict(payload)
        elif event_type == "decision_recorded":
            state.decisions[entity_id] = dict(payload)
        elif event_type == "decision_resolved":
            decision = state.decisions[entity_id]
            decision["status"] = payload["status"]
            decision["verdict"] = payload["verdict"]
            decision["confidence"] = payload["confidence"]
            decision["decided_at"] = payload["decided_at"]
            decision["resolution"] = dict(payload)
        elif event_type == "quality_gate_evaluated":
            state.quality_gate_events[entity_id] = dict(payload)
        elif event_type == "evidence_bundle_committed":
            state.evidence_bundles[entity_id] = dict(payload)
            state.research_requests[payload["request_id"]]["status"] = "completed"
            state.research_requests[payload["request_id"]]["updated_at"] = payload["created_at"]
        elif event_type == "commercial_decision_packet_created":
            state.commercial_decision_packets[entity_id] = dict(payload)
        elif event_type == "commercial_decision_recommendation_recorded":
            state.commercial_decision_recommendations[entity_id] = dict(payload)
        elif event_type == "project_created":
            state.projects[entity_id] = dict(payload)
        elif event_type == "project_task_created":
            state.project_tasks[entity_id] = dict(payload)
        elif event_type == "project_task_assigned":
            state.project_task_assignments[entity_id] = dict(payload)
            if payload["status"] == "accepted" and payload["task_id"] in state.project_tasks:
                task = state.project_tasks[payload["task_id"]]
                if task["status"] in {"queued", "blocked"}:
                    task["status"] = "running"
                    task["updated_at"] = payload["assigned_at"]
                    task["last_assignment_id"] = entity_id
        elif event_type == "project_task_transitioned":
            task = state.project_tasks[entity_id]
            task["status"] = payload["status"]
            task["updated_at"] = payload["updated_at"]
            task["last_transition"] = dict(payload)
        elif event_type == "project_outcome_recorded":
            state.project_outcomes[entity_id] = dict(payload)
            if payload.get("task_id") in state.project_tasks:
                task = state.project_tasks[payload["task_id"]]
                if task["status"] not in {"completed", "failed", "cancelled"}:
                    task["status"] = "completed"
                    task["updated_at"] = payload["created_at"]
        elif event_type == "project_artifact_receipt_recorded":
            state.project_artifact_receipts[entity_id] = dict(payload)
        elif event_type == "project_customer_feedback_recorded":
            state.project_customer_feedback[entity_id] = dict(payload)
        elif event_type == "project_revenue_attribution_recorded":
            state.project_revenue_attributions[entity_id] = dict(payload)
        elif event_type == "project_operator_load_recorded":
            state.project_operator_load[entity_id] = dict(payload)
        elif event_type == "project_commercial_rollup_derived":
            state.project_commercial_rollups[entity_id] = dict(payload)
        elif event_type == "project_status_rollup_derived":
            state.project_status_rollups[entity_id] = dict(payload)
        elif event_type == "project_close_decision_packet_created":
            state.project_close_decision_packets[entity_id] = dict(payload)
        elif event_type == "project_close_decision_resolved":
            packet = state.project_close_decision_packets[payload["packet_id"]]
            packet["status"] = "decided"
            packet["verdict"] = payload["verdict"]
            packet["decided_by"] = payload["decided_by"]
            packet["decided_at"] = payload["decided_at"]
            packet["followup_task_id"] = payload.get("followup_task_id")
            project = state.projects[payload["project_id"]]
            project["status"] = payload["project_status"]
            project["updated_at"] = payload["updated_at"]
            project["last_close_decision_packet_id"] = payload["packet_id"]
        elif event_type == "project_replay_projection_compared":
            state.project_replay_projection_comparisons[entity_id] = dict(payload)
        elif event_type == "project_portfolio_decision_packet_created":
            state.project_portfolio_decision_packets[entity_id] = dict(payload)
        elif event_type == "project_portfolio_decision_resolved":
            packet = state.project_portfolio_decision_packets[payload["packet_id"]]
            packet["status"] = "decided"
            packet["verdict"] = payload["verdict"]
            packet["decided_by"] = payload["decided_by"]
            packet["decided_at"] = payload["decided_at"]
        elif event_type == "project_portfolio_replay_projection_compared":
            state.project_portfolio_replay_projection_comparisons[entity_id] = dict(payload)
        elif event_type == "project_scheduling_intent_recorded":
            state.project_scheduling_intents[entity_id] = dict(payload)
        elif event_type == "project_scheduling_priority_change_packet_created":
            state.project_scheduling_priority_change_packets[entity_id] = dict(payload)
        elif event_type == "project_scheduling_priority_change_packet_resolved":
            packet = state.project_scheduling_priority_change_packets[payload["packet_id"]]
            packet["status"] = "decided"
            packet["verdict"] = payload["verdict"]
            packet["decided_by"] = payload["decided_by"]
            packet["decided_at"] = payload["decided_at"]
            packet["applied_changes"] = payload["applied_changes"]
        elif event_type == "project_scheduling_priority_replay_projection_compared":
            state.project_scheduling_priority_replay_projection_comparisons[entity_id] = dict(payload)
        elif event_type == "project_scheduling_replay_projection_compared":
            state.project_scheduling_replay_projection_comparisons[entity_id] = dict(payload)
        elif event_type == "project_customer_visible_packet_created":
            state.project_customer_visible_packets[entity_id] = dict(payload)
        elif event_type == "project_customer_visible_packet_resolved":
            packet = state.project_customer_visible_packets[payload["packet_id"]]
            packet["status"] = "decided"
            packet["verdict"] = payload["verdict"]
            packet["decided_by"] = payload["decided_by"]
            packet["decided_at"] = payload["decided_at"]
        elif event_type == "project_customer_commitment_recorded":
            state.project_customer_commitments[entity_id] = dict(payload)
        elif event_type == "project_customer_commitment_receipt_recorded":
            state.project_customer_commitment_receipts[entity_id] = dict(payload)
        elif event_type == "project_customer_commitment_receipt_followup_completed":
            receipt = state.project_customer_commitment_receipts.get(payload["receipt_id"])
            if receipt is not None:
                receipt["action_required"] = False
                receipt["status"] = "accepted"
                receipt["followup_task_id"] = payload["followup_task_id"]
        elif event_type == "project_customer_visible_replay_projection_compared":
            state.project_customer_visible_replay_projection_comparisons[entity_id] = dict(payload)
        elif event_type == "model_task_class_registered":
            state.model_task_classes[entity_id] = dict(payload)
        elif event_type == "model_candidate_registered":
            state.model_candidates[entity_id] = dict(payload)
        elif event_type == "model_holdout_policy_created":
            state.holdout_policies[entity_id] = dict(payload)
        elif event_type == "local_offload_eval_set_registered":
            state.local_offload_eval_sets[entity_id] = dict(payload)
        elif event_type == "model_holdout_use_recorded":
            state.holdout_use_records[entity_id] = dict(payload)
        elif event_type == "model_eval_run_recorded":
            state.model_eval_runs[entity_id] = dict(payload)
        elif event_type == "model_route_decision_recorded":
            state.model_route_decisions[entity_id] = dict(payload)
        elif event_type == "model_promotion_decision_packet_created":
            state.model_promotion_decision_packets[entity_id] = dict(payload)
        elif event_type == "model_demoted":
            state.model_demotion_records[entity_id] = dict(payload)
            state.model_candidates[payload["model_id"]]["promotion_state"] = "demoted"
            state.model_candidates[payload["model_id"]]["last_verified_at"] = payload["created_at"]
            for routing_state in payload["routing_state_after"]:
                state.model_routing_state[routing_state["state_id"]] = dict(routing_state)
        elif event_type == "side_effect_intent_prepared":
            state.side_effects[entity_id] = {"intent": dict(payload), "receipt": None}
        elif event_type == "side_effect_receipt_recorded":
            intent_id = payload["intent_id"]
            state.side_effects.setdefault(intent_id, {"intent": None, "receipt": None})
            state.side_effects[intent_id]["receipt"] = dict(payload)
            if payload["receipt_type"] in {"failure", "timeout", "compensation_needed"}:
                state.inspection_tasks.append(
                    {
                        "intent_id": intent_id,
                        "reason": payload["receipt_type"],
                        "replay_action": "inspect_or_compensate",
                    }
                )
        elif event_type in {"projection_outbox_enqueued"}:
            return
        else:
            raise ValueError(f"unknown critical event type: {event_type}")


class KernelTransaction:
    def __init__(self, conn: sqlite3.Connection, command: Command) -> None:
        self.conn = conn
        self.command = command
        self.transaction_id = new_id()
        self.last_event_id: str | None = None

    def append_event(
        self,
        event_type: str,
        entity_type: str,
        entity_id: str,
        payload: dict[str, Any],
        data_class: str = "internal",
        actor_type: str = "kernel",
        actor_id: str = "kernel",
    ) -> str:
        event = Event(
            event_type=event_type,
            entity_type=entity_type,
            entity_id=entity_id,
            transaction_id=self.transaction_id,
            command_id=self.command.command_id,
            actor_type=actor_type,  # type: ignore[arg-type]
            actor_id=actor_id,
            policy_version=KERNEL_POLICY_VERSION,
            data_class=data_class,  # type: ignore[arg-type]
            payload=payload,
        )
        prev = self.conn.execute("SELECT event_hash FROM events ORDER BY event_seq DESC LIMIT 1").fetchone()
        prev_hash = None if prev is None else prev["event_hash"]
        cursor = self.conn.execute(
            """
            INSERT INTO events (
              event_id, event_schema_version, event_type, entity_type, entity_id,
              transaction_id, command_id, correlation_id, causation_event_id,
              actor_type, actor_id, timestamp, policy_version, data_class,
              payload_hash, payload_json, prev_event_hash, event_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '')
            """,
            (
                event.event_id,
                event.event_schema_version,
                event.event_type,
                event.entity_type,
                event.entity_id,
                event.transaction_id,
                event.command_id,
                event.correlation_id,
                event.causation_event_id,
                event.actor_type,
                event.actor_id,
                event.timestamp,
                event.policy_version,
                event.data_class,
                event.payload_hash,
                canonical_json(event.payload),
                prev_hash,
            ),
        )
        event_seq = int(cursor.lastrowid)
        event_hash = KernelStore._event_hash(
            event.event_id,
            event_seq,
            event.event_schema_version,
            event.event_type,
            event.entity_type,
            event.entity_id,
            event.transaction_id,
            event.command_id,
            event.payload_hash,
            prev_hash,
        )
        self.conn.execute("UPDATE events SET event_hash=? WHERE event_seq=?", (event_hash, event_seq))
        self.last_event_id = event.event_id
        return event.event_id

    def enqueue_projection(self, event_id: str, projection_name: str) -> None:
        self.conn.execute(
            """
            INSERT INTO projection_outbox(outbox_id, event_id, projection_name, status, created_at)
            VALUES (?, ?, ?, 'pending', ?)
            """,
            (new_id(), event_id, projection_name, now_iso()),
        )

    def issue_capability_grant(self, grant: CapabilityGrant) -> str:
        payload = {
            "grant_id": grant.grant_id,
            "task_id": grant.task_id,
            "subject_type": grant.subject_type,
            "subject_id": grant.subject_id,
            "capability_type": grant.capability_type,
            "actions": grant.actions,
            "resource": grant.resource,
            "scope": grant.scope,
            "conditions": grant.conditions,
            "issued_at": grant.issued_at,
            "expires_at": grant.expires_at,
            "max_uses": grant.max_uses,
            "used_count": grant.used_count,
            "issuer": grant.issuer,
            "policy_version": grant.policy_version,
            "revalidate_on_use": grant.revalidate_on_use,
            "status": grant.status,
        }
        event_id = self.append_event("capability_granted", "capability", grant.grant_id, payload)
        self.conn.execute(
            """
            INSERT INTO capability_grants (
              grant_id, task_id, subject_type, subject_id, capability_type, actions_json,
              resource_json, scope_json, conditions_json, issued_at, expires_at,
              max_uses, used_count, issuer, policy_version, revalidate_on_use, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                grant.grant_id,
                grant.task_id,
                grant.subject_type,
                grant.subject_id,
                grant.capability_type,
                canonical_json(grant.actions),
                canonical_json(grant.resource),
                canonical_json(grant.scope),
                canonical_json(grant.conditions),
                grant.issued_at,
                grant.expires_at,
                grant.max_uses,
                grant.used_count,
                grant.issuer,
                grant.policy_version,
                1 if grant.revalidate_on_use else 0,
                grant.status,
            ),
        )
        self.enqueue_projection(event_id, "capability_projection")
        return grant.grant_id

    def use_grant(
        self,
        grant_id: str,
        subject_type: str,
        subject_id: str,
        capability_type: str,
        action: str,
    ) -> bool:
        row = self.conn.execute(
            "SELECT * FROM capability_grants WHERE grant_id=?",
            (grant_id,),
        ).fetchone()
        if row is None:
            return False
        actions = set(_loads(row["actions_json"]))
        stale_policy = row["revalidate_on_use"] and row["policy_version"] != KERNEL_POLICY_VERSION
        exhausted = row["max_uses"] is not None and row["used_count"] >= row["max_uses"]
        expired = row["expires_at"] <= now_iso()
        valid = (
            row["status"] == "active"
            and row["subject_type"] == subject_type
            and row["subject_id"] == subject_id
            and row["capability_type"] == capability_type
            and action in actions
            and not stale_policy
            and not exhausted
            and not expired
        )
        if not valid:
            return False
        event_id = self.append_event(
            "capability_used",
            "capability",
            grant_id,
            {
                "grant_id": grant_id,
                "subject_type": subject_type,
                "subject_id": subject_id,
                "capability_type": capability_type,
                "action": action,
                "used_at": now_iso(),
            },
        )
        next_used = row["used_count"] + 1
        next_status = "exhausted" if row["max_uses"] is not None and next_used >= row["max_uses"] else "active"
        self.conn.execute(
            "UPDATE capability_grants SET used_count=?, status=? WHERE grant_id=?",
            (next_used, next_status, grant_id),
        )
        self.enqueue_projection(event_id, "grant_use_projection")
        return True

    def create_budget(self, budget: Budget) -> str:
        payload = {
            "budget_id": budget.budget_id,
            "owner_type": budget.owner_type,
            "owner_id": budget.owner_id,
            "approved_by": budget.approved_by,
            "cap_usd": str(budget.cap_usd),
            "spent_usd": str(budget.spent_usd),
            "reserved_usd": str(budget.reserved_usd),
            "expires_at": budget.expires_at,
            "status": budget.status,
        }
        event_id = self.append_event("budget_created", "budget", budget.budget_id, payload)
        self.conn.execute(
            """
            INSERT INTO budgets (
              budget_id, owner_type, owner_id, approved_by, cap_usd, spent_usd,
              reserved_usd, expires_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                budget.budget_id,
                budget.owner_type,
                budget.owner_id,
                budget.approved_by,
                str(budget.cap_usd),
                str(budget.spent_usd),
                str(budget.reserved_usd),
                budget.expires_at,
                budget.status,
            ),
        )
        self.enqueue_projection(event_id, "budget_projection")
        return budget.budget_id

    def reserve_budget(self, budget_id: str, amount_usd: Decimal, reservation_id: str | None = None) -> str:
        reservation_id = reservation_id or new_id()
        row = self.conn.execute("SELECT * FROM budgets WHERE budget_id=?", (budget_id,)).fetchone()
        if row is None:
            raise ValueError("budget not found")
        if row["status"] != "active" or row["expires_at"] <= now_iso():
            raise ValueError("budget is not active")
        cap = Decimal(row["cap_usd"])
        spent = Decimal(row["spent_usd"])
        reserved = Decimal(row["reserved_usd"])
        if amount_usd <= Decimal("0"):
            raise ValueError("reservation must be positive")
        if spent + reserved + amount_usd > cap:
            raise ValueError("budget cap exceeded")
        payload = {
            "reservation_id": reservation_id,
            "budget_id": budget_id,
            "amount_usd": str(amount_usd),
            "reserved_at": now_iso(),
            "idempotency_key": self.command.idempotency_key,
        }
        event_id = self.append_event("spend_reserved", "budget", budget_id, payload)
        self.conn.execute(
            """
            INSERT INTO budget_reservations (
              reservation_id, budget_id, command_id, amount_usd, status, created_at
            ) VALUES (?, ?, ?, ?, 'reserved', ?)
            """,
            (reservation_id, budget_id, self.command.command_id, str(amount_usd), now_iso()),
        )
        self.conn.execute(
            "UPDATE budgets SET reserved_usd=? WHERE budget_id=?",
            (str(reserved + amount_usd), budget_id),
        )
        self.enqueue_projection(event_id, "budget_projection")
        return reservation_id

    def create_artifact_ref(self, artifact: ArtifactRef) -> str:
        payload = {
            "artifact_id": artifact.artifact_id,
            "artifact_uri": artifact.artifact_uri,
            "data_class": artifact.data_class,
            "content_hash": artifact.content_hash,
            "retention_policy": artifact.retention_policy,
            "deletion_policy": artifact.deletion_policy,
            "encryption_status": artifact.encryption_status,
            "source_notes": artifact.source_notes,
            "created_at": artifact.created_at,
        }
        event_id = self.append_event("artifact_ref_created", "artifact", artifact.artifact_id, payload, artifact.data_class)
        self.conn.execute(
            """
            INSERT INTO artifact_refs (
              artifact_id, artifact_uri, data_class, content_hash, retention_policy,
              deletion_policy, encryption_status, source_notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                artifact.artifact_id,
                artifact.artifact_uri,
                artifact.data_class,
                artifact.content_hash,
                artifact.retention_policy,
                artifact.deletion_policy,
                artifact.encryption_status,
                artifact.source_notes,
                artifact.created_at,
            ),
        )
        self.enqueue_projection(event_id, "artifact_projection")
        return artifact.artifact_id

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

    def create_project(self, project: Project) -> str:
        if not project.name.strip() or not project.objective.strip():
            raise ValueError("project name and objective are required")
        if project.decision_packet_id:
            packet = self.conn.execute(
                """
                SELECT decision_id, recommendation, status, project_json
                FROM commercial_decision_packets
                WHERE packet_id=?
                """,
                (project.decision_packet_id,),
            ).fetchone()
            if packet is None:
                raise ValueError("project decision packet not found")
            if packet["status"] != "gated":
                raise ValueError("project creation requires a gated commercial decision packet")
            if packet["decision_id"] != project.decision_id:
                raise ValueError("project Decision id must match decision packet")
            decision = self.conn.execute(
                "SELECT status, verdict, required_authority FROM decisions WHERE decision_id=?",
                (project.decision_id,),
            ).fetchone()
            if decision is None:
                raise ValueError("project Decision record not found")
            if decision["required_authority"] != "operator_gate":
                raise PermissionError("G1 project creation requires operator-gate authority")
            if decision["status"] != "decided" or decision["verdict"] != "approve_validation":
                raise PermissionError("G1 project creation requires an approved validation verdict")
        if project.status not in {"proposed", "active"}:
            raise ValueError("new projects must start proposed or active")
        if not project.phases:
            raise ValueError("project requires at least one phase")
        payload = _project_payload(project)
        event_id = self.append_event("project_created", "project", project.project_id, payload)
        self.conn.execute(
            """
            INSERT INTO projects (
              project_id, opportunity_id, decision_packet_id, decision_id,
              name, objective, revenue_mechanism, operator_role,
              external_commitment_policy, budget_id, phases_json,
              success_metrics_json, kill_criteria_json, evidence_refs_json,
              status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                project.project_id,
                project.opportunity_id,
                project.decision_packet_id,
                project.decision_id,
                project.name,
                project.objective,
                project.revenue_mechanism,
                project.operator_role,
                project.external_commitment_policy,
                project.budget_id,
                canonical_json(project.phases),
                canonical_json(project.success_metrics),
                canonical_json(project.kill_criteria),
                canonical_json(project.evidence_refs),
                project.status,
                project.created_at,
                project.updated_at,
            ),
        )
        self.enqueue_projection(event_id, "project_projection")
        return project.project_id

    def create_project_task(self, task: ProjectTask) -> str:
        project = self.conn.execute(
            "SELECT project_id, status, budget_id FROM projects WHERE project_id=?",
            (task.project_id,),
        ).fetchone()
        if project is None:
            raise ValueError("project task requires an existing project")
        if project["status"] not in {"active", "paused", "blocked"}:
            raise ValueError(f"cannot create project task from project status {project['status']}")
        if task.task_type in {"build", "ship"} and task.authority_required not in {"single_agent", "council", "operator_gate"}:
            raise PermissionError("build and ship tasks require assigned non-rule authority")
        if task.task_type == "ship" and task.authority_required != "operator_gate":
            raise PermissionError("shipping tasks require operator-gate authority")
        if task.risk_level in {"high", "critical"} and task.authority_required not in {"council", "operator_gate"}:
            raise PermissionError("high-risk project tasks require council or operator-gate authority")
        if not task.objective.strip():
            raise ValueError("project task objective is required")
        if not task.required_capabilities:
            raise ValueError("project task must declare required capabilities, even when empty-by-policy")
        policy_version = task.policy_version or KERNEL_POLICY_VERSION
        command_id = task.command_id or self.command.command_id
        idempotency_key = task.idempotency_key or self.command.idempotency_key
        payload = _project_task_payload(
            task,
            command_id=command_id,
            policy_version=policy_version,
            idempotency_key=idempotency_key,
        )
        event_id = self.append_event("project_task_created", "task", task.task_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_tasks (
              task_id, project_id, phase_name, task_type, autonomy_class, objective,
              inputs_json, expected_output_schema_json, risk_level,
              required_capabilities_json, model_requirement_json, budget_id,
              deadline, status, authority_required, recovery_policy, command_id,
              policy_version, idempotency_key, evidence_refs_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task.task_id,
                task.project_id,
                task.phase_name,
                task.task_type,
                task.autonomy_class,
                task.objective,
                canonical_json(task.inputs),
                canonical_json(task.expected_output_schema) if task.expected_output_schema is not None else None,
                task.risk_level,
                canonical_json(task.required_capabilities),
                canonical_json(task.model_requirement),
                task.budget_id,
                task.deadline,
                task.status,
                task.authority_required,
                task.recovery_policy,
                command_id,
                policy_version,
                idempotency_key,
                canonical_json(task.evidence_refs),
                task.created_at,
                task.updated_at,
            ),
        )
        self.enqueue_projection(event_id, "project_task_projection")
        return task.task_id

    def assign_project_task(self, assignment: ProjectTaskAssignment) -> str:
        row = self.conn.execute(
            """
            SELECT task_id, project_id, status, required_capabilities_json,
                   inputs_json, budget_id
            FROM project_tasks
            WHERE task_id=?
            """,
            (assignment.task_id,),
        ).fetchone()
        if row is None:
            raise ValueError("project task assignment requires an existing task")
        if row["project_id"] != assignment.project_id:
            raise ValueError("project task assignment project/task mismatch")
        if not assignment.worker_id.strip():
            raise ValueError("project task assignment requires a worker id")
        existing = self.conn.execute(
            """
            SELECT assignment_id, task_id, project_id, worker_type, worker_id,
                   route_decision_id, grant_ids_json, status
            FROM project_task_assignments
            WHERE assignment_id=?
            """,
            (assignment.assignment_id,),
        ).fetchone()
        if existing is None:
            if row["status"] not in {"queued", "blocked"}:
                raise ValueError(f"cannot assign project task from status {row['status']}")
        else:
            if existing["status"] != "assigned":
                raise ValueError(f"cannot resolve project task assignment from status {existing['status']}")
            if assignment.status not in {"accepted", "rejected", "revoked"}:
                raise ValueError("existing project task assignments must resolve to accepted, rejected, or revoked")
            if existing["task_id"] != assignment.task_id or existing["project_id"] != assignment.project_id:
                raise ValueError("project task assignment resolution task/project mismatch")
            if existing["worker_type"] != assignment.worker_type or existing["worker_id"] != assignment.worker_id:
                raise PermissionError("project task assignment resolution worker mismatch")
            if _loads(existing["grant_ids_json"]) != assignment.grant_ids:
                raise PermissionError("project task assignment resolution cannot change grant evidence")
        if assignment.status == "accepted" and not assignment.accepted_capabilities:
            raise ValueError("accepted project task assignment must record accepted capabilities")
        inputs = _loads(row["inputs_json"])
        scheduling_created = bool(inputs.get("scheduling_priority_packet_id"))
        for grant_id in assignment.grant_ids:
            grant = self.conn.execute(
                """
                SELECT task_id, subject_type, subject_id, capability_type,
                       actions_json, status, policy_version
                FROM capability_grants
                WHERE grant_id=?
                """,
                (grant_id,),
            ).fetchone()
            if grant is None:
                raise PermissionError("project task assignment references unknown grant")
            if grant["task_id"] != assignment.task_id:
                raise PermissionError("project task assignment grant/task mismatch")
            if grant["status"] != "active" or grant["policy_version"] != KERNEL_POLICY_VERSION:
                raise PermissionError("project task assignment requires active current-policy grants")
            if scheduling_created and (
                grant["subject_type"] != assignment.worker_type or grant["subject_id"] != assignment.worker_id
            ):
                raise PermissionError("project task assignment grant/worker mismatch")
        self._validate_project_task_assignment_evidence(row, assignment)
        payload = _project_task_assignment_payload(assignment)
        event_id = self.append_event("project_task_assigned", "task", assignment.assignment_id, payload)
        if existing is None:
            self.conn.execute(
                """
                INSERT INTO project_task_assignments (
                  assignment_id, task_id, project_id, worker_type, worker_id,
                  route_decision_id, grant_ids_json, accepted_capabilities_json,
                  status, notes, assigned_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    assignment.assignment_id,
                    assignment.task_id,
                    assignment.project_id,
                    assignment.worker_type,
                    assignment.worker_id,
                    assignment.route_decision_id,
                    canonical_json(assignment.grant_ids),
                    canonical_json(assignment.accepted_capabilities),
                    assignment.status,
                    assignment.notes,
                    assignment.assigned_at,
                ),
            )
        else:
            self.conn.execute(
                """
                UPDATE project_task_assignments
                SET accepted_capabilities_json=?, status=?, notes=?, assigned_at=?
                WHERE assignment_id=?
                """,
                (
                    canonical_json(assignment.accepted_capabilities),
                    assignment.status,
                    assignment.notes,
                    assignment.assigned_at,
                    assignment.assignment_id,
                ),
            )
        if assignment.status == "accepted":
            self.conn.execute(
                "UPDATE project_tasks SET status='running', updated_at=? WHERE task_id=?",
                (assignment.assigned_at, assignment.task_id),
            )
        self.enqueue_projection(event_id, "project_task_assignment_projection")
        return assignment.assignment_id

    def _validate_project_task_assignment_evidence(
        self,
        task_row: sqlite3.Row,
        assignment: ProjectTaskAssignment,
    ) -> None:
        inputs = _loads(task_row["inputs_json"])
        required_capabilities = _loads(task_row["required_capabilities_json"])
        scheduling_created = bool(inputs.get("scheduling_priority_packet_id"))
        if scheduling_created:
            if inputs.get("customer_commitments_allowed") or inputs.get("customer_visible"):
                raise PermissionError("scheduling-created assignment cannot authorize customer commitments")
            if inputs.get("external_side_effects_authorized"):
                raise PermissionError("scheduling-created assignment cannot authorize side effects")
            if not task_row["budget_id"]:
                raise PermissionError("scheduling-created assignment requires durable project budget evidence")
            budget = self.conn.execute(
                """
                SELECT owner_type, owner_id, status, expires_at
                FROM budgets
                WHERE budget_id=?
                """,
                (task_row["budget_id"],),
            ).fetchone()
            if budget is None or budget["owner_type"] != "project" or budget["owner_id"] != task_row["project_id"]:
                raise PermissionError("scheduling-created assignment budget/project evidence mismatch")
            if budget["status"] != "active" or budget["expires_at"] <= now_iso():
                raise PermissionError("scheduling-created assignment requires an active budget")
        if not required_capabilities:
            return
        grant_rows = [
            self.conn.execute(
                """
                SELECT capability_type, actions_json
                FROM capability_grants
                WHERE grant_id=?
                """,
                (grant_id,),
            ).fetchone()
            for grant_id in assignment.grant_ids
        ]
        accepted = assignment.accepted_capabilities
        for required in required_capabilities:
            if not required.get("grant_required_before_run", True):
                continue
            capability_type = required.get("capability_type")
            actions = set(required.get("actions", []))
            has_grant = any(
                grant is not None
                and grant["capability_type"] == capability_type
                and actions.issubset(set(_loads(grant["actions_json"])))
                for grant in grant_rows
            )
            if not has_grant:
                raise PermissionError("project task assignment missing required capability grant evidence")
            if assignment.status == "accepted":
                has_acceptance = any(
                    item.get("capability_type") == capability_type
                    and actions.issubset(set(item.get("actions", [])))
                    for item in accepted
                )
                if not has_acceptance:
                    raise PermissionError("worker acceptance missing required capability evidence")

    def transition_project_task(self, task_id: str, status: str, reason: str) -> str:
        valid_statuses = {"queued", "running", "blocked", "completed", "failed", "cancelled"}
        if status not in valid_statuses:
            raise ValueError(f"unknown project task status: {status}")
        if not reason.strip():
            raise ValueError("project task transition requires a reason")
        row = self.conn.execute("SELECT status, authority_required FROM project_tasks WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            raise ValueError("project task not found")
        valid_transitions = {
            "queued": {"running", "blocked", "cancelled"},
            "running": {"completed", "blocked", "failed", "cancelled"},
            "blocked": {"running", "failed", "cancelled"},
            "completed": set(),
            "failed": set(),
            "cancelled": set(),
        }
        if status not in valid_transitions[row["status"]]:
            raise ValueError(f"invalid project task transition {row['status']} -> {status}")
        if status == "running":
            assignment = self.conn.execute(
                """
                SELECT assignment_id
                FROM project_task_assignments
                WHERE task_id=? AND status='accepted'
                ORDER BY assigned_at DESC
                LIMIT 1
                """,
                (task_id,),
            ).fetchone()
            if assignment is None:
                raise PermissionError("project tasks require an accepted assignment before running")
        if row["authority_required"] == "operator_gate" and status in {"running", "completed"} and self.command.requested_by != "operator":
            raise PermissionError("operator-gated project tasks require operator transition authority")
        updated_at = now_iso()
        payload = {
            "task_id": task_id,
            "previous_status": row["status"],
            "status": status,
            "reason": reason,
            "updated_at": updated_at,
            "authority_required": row["authority_required"],
        }
        event_id = self.append_event("project_task_transitioned", "task", task_id, payload)
        self.conn.execute(
            "UPDATE project_tasks SET status=?, updated_at=? WHERE task_id=?",
            (status, updated_at, task_id),
        )
        self.enqueue_projection(event_id, "project_task_projection")
        return task_id

    def record_project_followup_delivery(
        self,
        task_id: str,
        *,
        artifact_ref: str,
        summary: str,
        data_class: str = "internal",
        delivery_channel: str = "local_workspace",
        side_effect_intent_id: str | None = None,
        side_effect_receipt_id: str | None = None,
        customer_visible: bool = False,
        metrics: dict[str, Any] | None = None,
        feedback: dict[str, Any] | None = None,
        revenue_impact: dict[str, Any] | None = None,
        operator_load_actual: str | None = None,
        next_recommendation: str | None = None,
    ) -> dict[str, Any]:
        task = self.conn.execute(
            """
            SELECT task_id, project_id, phase_name, task_type, status, authority_required,
                   inputs_json, evidence_refs_json
            FROM project_tasks
            WHERE task_id=?
            """,
            (task_id,),
        ).fetchone()
        if task is None:
            raise ValueError("follow-up delivery requires an existing project task")
        if task["task_type"] not in {"build", "ship"}:
            raise ValueError("follow-up delivery only applies to build or ship tasks")
        if task["status"] != "running":
            raise ValueError("follow-up delivery requires a running assigned task")
        if not artifact_ref.strip() or not summary.strip():
            raise ValueError("follow-up delivery requires an artifact ref and summary")
        if task["task_type"] == "build" and (customer_visible or side_effect_receipt_id):
            raise PermissionError("build artifacts cannot be customer-visible or bind external side effects")
        if task["task_type"] == "ship":
            if self.command.requested_by != "operator" or self.command.requested_authority != "operator_gate":
                raise PermissionError("ship deliveries require operator-gate authority")
            if not side_effect_receipt_id:
                raise PermissionError("ship deliveries require a durable side-effect receipt")
            customer_visible = True

        artifact_kind = "shipped_artifact" if task["task_type"] == "ship" else "build_artifact"
        outcome_type = "shipped_artifact" if task["task_type"] == "ship" else "build_artifact"
        artifact = ProjectArtifactReceipt(
            project_id=task["project_id"],
            task_id=task_id,
            artifact_ref=artifact_ref,
            artifact_kind=artifact_kind,  # type: ignore[arg-type]
            summary=summary,
            data_class=data_class,  # type: ignore[arg-type]
            delivery_channel=delivery_channel,
            side_effect_intent_id=side_effect_intent_id,
            side_effect_receipt_id=side_effect_receipt_id,
            customer_visible=customer_visible,
            status="accepted",
        )
        artifact_receipt_id = self.record_project_artifact_receipt(artifact)

        outcome_feedback = dict(feedback or {})
        if next_recommendation is not None:
            outcome_feedback.setdefault("next_recommendation", next_recommendation)
        outcome = ProjectOutcome(
            project_id=task["project_id"],
            task_id=task_id,
            phase_name=task["phase_name"],
            outcome_type=outcome_type,  # type: ignore[arg-type]
            summary=summary,
            artifact_refs=[artifact_ref, f"kernel:project_artifact_receipts/{artifact_receipt_id}"],
            metrics=dict(metrics or {}),
            feedback=outcome_feedback,
            revenue_impact=dict(revenue_impact or {"amount": 0, "currency": "USD", "period": "one_time"}),
            operator_load_actual=operator_load_actual,
            status="accepted",
        )
        outcome_id = self.record_project_outcome(outcome)
        ship_task_id: str | None = None
        recommendation = (next_recommendation or outcome_feedback.get("next_recommendation") or "").lower()
        if task["task_type"] == "build" and any(term in recommendation for term in ("ship", "publish", "deploy")):
            ship_task_id = self._create_ship_task_from_build_delivery(
                project_id=task["project_id"],
                build_task_id=task_id,
                build_artifact_receipt_id=artifact_receipt_id,
                artifact_ref=artifact_ref,
                summary=summary,
                source_evidence_refs=_loads(task["evidence_refs_json"]),
            )
        return {
            "project_id": task["project_id"],
            "task_id": task_id,
            "artifact_receipt_id": artifact_receipt_id,
            "outcome_id": outcome_id,
            "ship_task_id": ship_task_id,
        }

    def record_project_operate_followup_outcome(
        self,
        task_id: str,
        *,
        summary: str,
        internal_result_ref: str,
        operator_load_minutes: int,
        operator_load_source: str,
        operate_followup_type: str | None = None,
        metrics: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        revenue_impact: dict[str, Any] | None = None,
        side_effect_intent_id: str | None = None,
        side_effect_receipt_id: str | None = None,
        external_commitment_change: bool = False,
        operator_load_notes: str | None = None,
    ) -> dict[str, Any]:
        task = self.conn.execute(
            """
            SELECT task_id, project_id, phase_name, task_type, status, inputs_json,
                   evidence_refs_json
            FROM project_tasks
            WHERE task_id=?
            """,
            (task_id,),
        ).fetchone()
        if task is None:
            raise ValueError("operate follow-up outcome requires an existing project task")
        if task["task_type"] != "operate" or task["phase_name"] != "Operate":
            raise ValueError("operate follow-up outcome only applies to Operate tasks")
        if task["status"] != "running":
            raise ValueError("operate follow-up outcome requires a running assigned task")
        if not summary.strip() or not internal_result_ref.strip():
            raise ValueError("operate follow-up outcome requires a summary and internal result ref")
        if operator_load_minutes < 0:
            raise ValueError("operate follow-up operator load minutes must be non-negative")
        if not operator_load_source.strip():
            raise ValueError("operate follow-up operator load source is required")

        inputs = _loads(task["inputs_json"])
        commitment_receipt_id = inputs.get("customer_commitment_receipt_id")
        commitment_id = inputs.get("commitment_id")
        receipt_row = None
        if commitment_receipt_id:
            receipt_row = self.conn.execute(
                """
                SELECT receipt_id, commitment_id, receipt_type, followup_task_id
                FROM project_customer_commitment_receipts
                WHERE receipt_id=?
                """,
                (commitment_receipt_id,),
            ).fetchone()
            if receipt_row is None:
                raise ValueError("operate follow-up outcome references unknown customer commitment receipt")
            if receipt_row["followup_task_id"] != task_id:
                raise PermissionError("customer commitment receipt follow-up task mismatch")
            if commitment_id and receipt_row["commitment_id"] != commitment_id:
                raise ValueError("customer commitment receipt/commitment mismatch")
        expected_followup_type = operate_followup_type or inputs.get("operate_followup_type")
        if expected_followup_type not in {
            "revenue_reconciliation",
            "retention",
            "maintenance",
            "customer_support",
        }:
            raise ValueError("operate follow-up outcome requires a known follow-up type")
        if operate_followup_type and inputs.get("operate_followup_type") and operate_followup_type != inputs["operate_followup_type"]:
            raise ValueError("operate follow-up outcome type does not match task input")

        resolved_intent_id = side_effect_intent_id
        if side_effect_receipt_id:
            if self.command.requested_by != "operator" or self.command.requested_authority != "operator_gate":
                raise PermissionError("operate follow-up side effects require operator-gate authority")
            side_effect = self._successful_task_side_effect(
                task_id=task_id,
                receipt_id=side_effect_receipt_id,
                intent_id=side_effect_intent_id,
            )
            resolved_intent_id = side_effect["intent_id"]
            external_commitment_change = True
        elif external_commitment_change:
            raise PermissionError("operate follow-up external commitments require a durable side-effect receipt")
        elif side_effect_intent_id:
            if commitment_receipt_id:
                raise PermissionError("customer commitment receipt follow-up side effects require a durable receipt")
            if self.command.requested_by != "operator" or self.command.requested_authority != "operator_gate":
                raise PermissionError("staged operate follow-up side-effect intents require operator-gate authority")
            self._require_task_side_effect_intent(task_id, side_effect_intent_id)

        output_result = dict(result or {})
        output_result.setdefault("operate_followup_type", expected_followup_type)
        output_result.setdefault("internal_result_ref", internal_result_ref)
        output_result["external_commitment_change"] = external_commitment_change
        output_result["side_effect_intent_id"] = resolved_intent_id
        output_result["side_effect_receipt_id"] = side_effect_receipt_id
        output_result["source_feedback_id"] = inputs.get("feedback_id")
        output_result["source_artifact_receipt_id"] = inputs.get("artifact_receipt_id")
        output_result["source_commitment_id"] = commitment_id
        output_result["customer_commitment_receipt_id"] = commitment_receipt_id
        output_result["receipt_type"] = receipt_row["receipt_type"] if receipt_row else inputs.get("receipt_type")
        output_result["source_outcome_id"] = inputs.get("source_outcome_id")
        output_result["evidence_refs"] = _merge_refs(
            _loads(task["evidence_refs_json"]),
            output_result.get("evidence_refs") or [],
            (
                [
                    f"kernel:project_customer_commitments/{commitment_id}",
                    f"kernel:project_customer_commitment_receipts/{commitment_receipt_id}",
                ]
                if commitment_receipt_id and commitment_id
                else []
            ),
        )

        artifact_refs = _merge_refs(
            [
                internal_result_ref,
                f"kernel:project_tasks/{task_id}",
            ],
            output_result["evidence_refs"],
        )
        if inputs.get("feedback_id"):
            artifact_refs.append(f"kernel:project_customer_feedback/{inputs['feedback_id']}")
        if resolved_intent_id:
            artifact_refs.append(f"kernel:side_effect_intents/{resolved_intent_id}")
        if side_effect_receipt_id:
            artifact_refs.append(f"kernel:side_effect_receipts/{side_effect_receipt_id}")

        outcome = ProjectOutcome(
            project_id=task["project_id"],
            task_id=task_id,
            phase_name="Operate",
            outcome_type="operate_followup",
            summary=summary,
            artifact_refs=artifact_refs,
            metrics=dict(metrics or {}),
            feedback=output_result,
            revenue_impact=dict(revenue_impact or {"amount": 0, "currency": "USD", "period": "one_time"}),
            operator_load_actual=f"{operator_load_minutes} minutes",
            side_effect_intent_id=resolved_intent_id,
            side_effect_receipt_id=side_effect_receipt_id,
            status="accepted",
        )
        outcome_id = self.record_project_outcome(outcome)
        if commitment_receipt_id:
            followup_payload = {
                "receipt_id": commitment_receipt_id,
                "commitment_id": commitment_id,
                "project_id": task["project_id"],
                "followup_task_id": task_id,
                "outcome_id": outcome_id,
                "status": "accepted",
                "action_required": False,
            }
            event_id = self.append_event(
                "project_customer_commitment_receipt_followup_completed",
                "project",
                commitment_receipt_id,
                followup_payload,
            )
            self.conn.execute(
                """
                UPDATE project_customer_commitment_receipts
                SET status='accepted', action_required=0
                WHERE receipt_id=? AND followup_task_id=?
                """,
                (commitment_receipt_id, task_id),
            )
            self.enqueue_projection(event_id, "project_customer_commitment_receipt_followup_projection")
        load_type = inputs.get("default_operator_load_type") or {
            "revenue_reconciliation": "reconciliation",
            "retention": "client_sales",
            "maintenance": "maintenance",
            "customer_support": "other",
        }[expected_followup_type]
        load_id = self.record_project_operator_load(
            ProjectOperatorLoadRecord(
                project_id=task["project_id"],
                task_id=task_id,
                outcome_id=outcome_id,
                minutes=operator_load_minutes,
                load_type=load_type,
                source=operator_load_source,
                notes=operator_load_notes or f"Operate follow-up {expected_followup_type} outcome",
            )
        )
        return {
            "project_id": task["project_id"],
            "task_id": task_id,
            "outcome_id": outcome_id,
            "operator_load_id": load_id,
            "operate_followup_type": expected_followup_type,
            "internal_result_ref": internal_result_ref,
            "external_commitment_change": external_commitment_change,
            "side_effect_intent_id": resolved_intent_id,
            "side_effect_receipt_id": side_effect_receipt_id,
        }

    def record_project_scheduling_task_outcome(
        self,
        task_id: str,
        *,
        summary: str,
        internal_result_ref: str,
        result: dict[str, Any] | None = None,
        metrics: dict[str, Any] | None = None,
        revenue_impact: dict[str, Any] | None = None,
        side_effect_intent_id: str | None = None,
        side_effect_receipt_id: str | None = None,
        external_commitment_change: bool = False,
    ) -> dict[str, Any]:
        task = self.conn.execute(
            """
            SELECT task_id, project_id, phase_name, task_type, status, budget_id,
                   inputs_json, evidence_refs_json
            FROM project_tasks
            WHERE task_id=?
            """,
            (task_id,),
        ).fetchone()
        if task is None:
            raise ValueError("scheduling task outcome requires an existing project task")
        inputs = _loads(task["inputs_json"])
        priority_packet_id = inputs.get("scheduling_priority_packet_id")
        if not priority_packet_id:
            raise ValueError("scheduling task outcome only applies to scheduling-created tasks")
        if task["status"] != "running":
            raise PermissionError("scheduling task outcome requires an accepted running assignment")
        if not summary.strip() or not internal_result_ref.strip():
            raise ValueError("scheduling task outcome requires a summary and internal result ref")
        if inputs.get("customer_commitments_allowed") or inputs.get("customer_visible"):
            raise PermissionError("scheduling-created outcomes cannot create customer-visible commitments")
        if inputs.get("external_side_effects_authorized"):
            raise PermissionError("scheduling-created outcomes cannot use autonomous side-effect authority")

        assignment = self.conn.execute(
            """
            SELECT assignment_id, grant_ids_json, accepted_capabilities_json, worker_type,
                   worker_id, assigned_at
            FROM project_task_assignments
            WHERE task_id=? AND status='accepted'
            ORDER BY assigned_at DESC, assignment_id DESC
            LIMIT 1
            """,
            (task_id,),
        ).fetchone()
        if assignment is None:
            raise PermissionError("scheduling task outcomes require accepted assignment evidence")
        grant_ids = _loads(assignment["grant_ids_json"])
        if not grant_ids:
            raise PermissionError("scheduling task outcomes require capability-grant evidence")
        if not task["budget_id"]:
            raise PermissionError("scheduling task outcomes require durable budget evidence")
        budget = self.conn.execute(
            """
            SELECT owner_type, owner_id, status
            FROM budgets
            WHERE budget_id=?
            """,
            (task["budget_id"],),
        ).fetchone()
        if budget is None or budget["owner_type"] != "project" or budget["owner_id"] != task["project_id"]:
            raise PermissionError("scheduling task outcome budget/project evidence mismatch")
        if budget["status"] != "active":
            raise PermissionError("scheduling task outcomes require an active budget")

        resolved_intent_id = side_effect_intent_id
        if side_effect_receipt_id:
            if self.command.requested_by != "operator" or self.command.requested_authority != "operator_gate":
                raise PermissionError("scheduling task side effects require operator-gate authority")
            side_effect = self._successful_task_side_effect(
                task_id=task_id,
                receipt_id=side_effect_receipt_id,
                intent_id=side_effect_intent_id,
            )
            resolved_intent_id = side_effect["intent_id"]
            external_commitment_change = True
        elif external_commitment_change:
            raise PermissionError("scheduling task customer commitments require a durable operator-gated receipt")
        elif side_effect_intent_id:
            raise PermissionError("scheduling task side effects require a durable operator-gated receipt")

        output_result = dict(result or {})
        output_result.setdefault("operate_followup_type", output_result.get("scheduling_outcome_type", "maintenance"))
        output_result.setdefault("internal_result_ref", internal_result_ref)
        output_result["external_commitment_change"] = external_commitment_change
        output_result["side_effect_intent_id"] = resolved_intent_id
        output_result["side_effect_receipt_id"] = side_effect_receipt_id
        output_result["scheduling_priority_packet_id"] = priority_packet_id
        output_result["scheduling_intent_id"] = inputs.get("scheduling_intent_id")
        output_result["portfolio_packet_id"] = inputs.get("portfolio_packet_id")
        output_result["priority_rank"] = inputs.get("priority_rank")
        output_result["queue_action"] = inputs.get("queue_action")
        output_result["assignment_id"] = assignment["assignment_id"]
        output_result["budget_id"] = task["budget_id"]
        output_result["grant_ids"] = grant_ids
        output_result["accepted_capabilities"] = _loads(assignment["accepted_capabilities_json"])
        output_result["worker_type"] = assignment["worker_type"]
        output_result["worker_id"] = assignment["worker_id"]
        output_result["evidence_refs"] = _merge_refs(
            _loads(task["evidence_refs_json"]),
            [
                f"kernel:project_task_assignments/{assignment['assignment_id']}",
                f"kernel:budgets/{task['budget_id']}",
                f"kernel:project_scheduling_priority_change_packets/{priority_packet_id}",
            ],
            [f"kernel:capability_grants/{grant_id}" for grant_id in grant_ids],
        )

        artifact_refs = _merge_refs(
            [internal_result_ref, f"kernel:project_tasks/{task_id}"],
            output_result["evidence_refs"],
        )
        if resolved_intent_id:
            artifact_refs.append(f"kernel:side_effect_intents/{resolved_intent_id}")
        if side_effect_receipt_id:
            artifact_refs.append(f"kernel:side_effect_receipts/{side_effect_receipt_id}")

        outcome = ProjectOutcome(
            project_id=task["project_id"],
            task_id=task_id,
            phase_name=task["phase_name"],
            outcome_type="operate_followup",
            summary=summary,
            artifact_refs=artifact_refs,
            metrics=dict(metrics or {}),
            feedback=output_result,
            revenue_impact=dict(revenue_impact or {"amount": 0, "currency": "USD", "period": "one_time"}),
            side_effect_intent_id=resolved_intent_id,
            side_effect_receipt_id=side_effect_receipt_id,
            status="accepted",
        )
        outcome_id = self.record_project_outcome(outcome)
        return {
            "project_id": task["project_id"],
            "task_id": task_id,
            "outcome_id": outcome_id,
            "internal_result_ref": internal_result_ref,
            "assignment_id": assignment["assignment_id"],
            "budget_id": task["budget_id"],
            "grant_ids": grant_ids,
            "scheduling_priority_packet_id": priority_packet_id,
            "scheduling_intent_id": inputs.get("scheduling_intent_id"),
            "external_commitment_change": external_commitment_change,
            "side_effect_intent_id": resolved_intent_id,
            "side_effect_receipt_id": side_effect_receipt_id,
        }

    def record_project_outcome(self, outcome: ProjectOutcome) -> str:
        project = self.conn.execute("SELECT status FROM projects WHERE project_id=?", (outcome.project_id,)).fetchone()
        if project is None:
            raise ValueError("project outcome requires an existing project")
        if outcome.task_id:
            task = self.conn.execute(
                "SELECT project_id, status FROM project_tasks WHERE task_id=?",
                (outcome.task_id,),
            ).fetchone()
            if task is None:
                raise ValueError("project outcome references unknown task")
            if task["project_id"] != outcome.project_id:
                raise ValueError("project outcome task/project mismatch")
            if task["status"] not in {"running", "completed"}:
                raise ValueError("project outcome task must be running or completed")
        if not outcome.summary.strip():
            raise ValueError("project outcome summary is required")
        payload = _project_outcome_payload(outcome)
        event_id = self.append_event("project_outcome_recorded", "project", outcome.outcome_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_outcomes (
              outcome_id, project_id, task_id, phase_name, outcome_type, summary,
              artifact_refs_json, metrics_json, feedback_json, revenue_impact_json,
              operator_load_actual, side_effect_intent_id, side_effect_receipt_id,
              status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                outcome.outcome_id,
                outcome.project_id,
                outcome.task_id,
                outcome.phase_name,
                outcome.outcome_type,
                outcome.summary,
                canonical_json(outcome.artifact_refs),
                canonical_json(outcome.metrics),
                canonical_json(outcome.feedback),
                canonical_json(outcome.revenue_impact),
                outcome.operator_load_actual,
                outcome.side_effect_intent_id,
                outcome.side_effect_receipt_id,
                outcome.status,
                outcome.created_at,
            ),
        )
        if outcome.task_id:
            self.conn.execute(
                "UPDATE project_tasks SET status='completed', updated_at=? WHERE task_id=? AND status!='completed'",
                (outcome.created_at, outcome.task_id),
            )
        self.enqueue_projection(event_id, "project_outcome_projection")
        return outcome.outcome_id

    def record_project_artifact_receipt(self, receipt: ProjectArtifactReceipt) -> str:
        self._require_project(receipt.project_id)
        if receipt.task_id:
            self._require_project_task(receipt.project_id, receipt.task_id)
        if not receipt.artifact_ref.strip() or not receipt.summary.strip():
            raise ValueError("artifact receipt requires an artifact ref and summary")
        if receipt.customer_visible and receipt.artifact_kind != "shipped_artifact":
            raise ValueError("customer-visible artifact receipts must be shipped artifacts")
        if receipt.artifact_kind == "shipped_artifact" and not receipt.side_effect_receipt_id:
            raise PermissionError("shipped artifacts require a durable side-effect receipt")
        if receipt.side_effect_receipt_id:
            side_effect = self.conn.execute(
                """
                SELECT r.receipt_id, r.intent_id, r.receipt_type, i.task_id
                FROM side_effect_receipts r
                JOIN side_effect_intents i ON i.intent_id = r.intent_id
                WHERE r.receipt_id=?
                """,
                (receipt.side_effect_receipt_id,),
            ).fetchone()
            if side_effect is None:
                raise ValueError("artifact receipt references unknown side-effect receipt")
            if side_effect["receipt_type"] != "success":
                raise PermissionError("shipped artifact receipt requires successful side-effect execution")
            if receipt.task_id and side_effect["task_id"] != receipt.task_id:
                raise ValueError("artifact side-effect task does not match project task")
            if receipt.side_effect_intent_id and side_effect["intent_id"] != receipt.side_effect_intent_id:
                raise ValueError("artifact side-effect intent/receipt mismatch")
        payload = _project_artifact_receipt_payload(receipt)
        event_id = self.append_event(
            "project_artifact_receipt_recorded",
            "artifact",
            receipt.receipt_id,
            payload,
            receipt.data_class,
        )
        self.conn.execute(
            """
            INSERT INTO project_artifact_receipts (
              receipt_id, project_id, task_id, artifact_ref, artifact_kind, summary,
              data_class, delivery_channel, side_effect_intent_id,
              side_effect_receipt_id, customer_visible, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                receipt.receipt_id,
                receipt.project_id,
                receipt.task_id,
                receipt.artifact_ref,
                receipt.artifact_kind,
                receipt.summary,
                receipt.data_class,
                receipt.delivery_channel,
                receipt.side_effect_intent_id,
                receipt.side_effect_receipt_id,
                int(receipt.customer_visible),
                receipt.status,
                receipt.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_artifact_projection")
        return receipt.receipt_id

    def _successful_task_side_effect(
        self,
        *,
        task_id: str,
        receipt_id: str,
        intent_id: str | None = None,
    ) -> sqlite3.Row:
        side_effect = self.conn.execute(
            """
            SELECT r.receipt_id, r.intent_id, r.receipt_type, i.task_id, i.required_authority
            FROM side_effect_receipts r
            JOIN side_effect_intents i ON i.intent_id = r.intent_id
            WHERE r.receipt_id=?
            """,
            (receipt_id,),
        ).fetchone()
        if side_effect is None:
            raise ValueError("operate follow-up references unknown side-effect receipt")
        if side_effect["receipt_type"] != "success":
            raise PermissionError("operate follow-up side effects require a successful durable receipt")
        if side_effect["task_id"] != task_id:
            raise ValueError("operate follow-up side-effect task does not match project task")
        if side_effect["required_authority"] != "operator_gate":
            raise PermissionError("operate follow-up side effects require operator-gate side-effect authority")
        if intent_id and side_effect["intent_id"] != intent_id:
            raise ValueError("operate follow-up side-effect intent/receipt mismatch")
        return side_effect

    def _require_task_side_effect_intent(self, task_id: str, intent_id: str) -> None:
        intent = self.conn.execute(
            """
            SELECT intent_id, task_id, required_authority
            FROM side_effect_intents
            WHERE intent_id=?
            """,
            (intent_id,),
        ).fetchone()
        if intent is None:
            raise ValueError("operate follow-up references unknown side-effect intent")
        if intent["task_id"] != task_id:
            raise ValueError("operate follow-up side-effect intent task does not match project task")
        if intent["required_authority"] != "operator_gate":
            raise PermissionError("staged operate follow-up side-effect intents require operator-gate authority")

    def record_project_customer_feedback(self, feedback: ProjectCustomerFeedback) -> str:
        self._require_project(feedback.project_id)
        if feedback.task_id:
            self._require_project_task(feedback.project_id, feedback.task_id)
        if feedback.artifact_receipt_id:
            artifact = self.conn.execute(
                "SELECT project_id FROM project_artifact_receipts WHERE receipt_id=?",
                (feedback.artifact_receipt_id,),
            ).fetchone()
            if artifact is None:
                raise ValueError("feedback references unknown artifact receipt")
            if artifact["project_id"] != feedback.project_id:
                raise ValueError("feedback artifact/project mismatch")
        if feedback.source_type == "customer" and not (feedback.customer_ref or feedback.evidence_refs):
            raise ValueError("customer feedback requires a customer ref or evidence reference")
        if not feedback.summary.strip():
            raise ValueError("feedback summary is required")
        payload = _project_customer_feedback_payload(feedback)
        event_id = self.append_event("project_customer_feedback_recorded", "project", feedback.feedback_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_customer_feedback (
              feedback_id, project_id, task_id, artifact_receipt_id, source_type,
              customer_ref, summary, sentiment, evidence_refs_json,
              action_required, operator_review_required, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                feedback.feedback_id,
                feedback.project_id,
                feedback.task_id,
                feedback.artifact_receipt_id,
                feedback.source_type,
                feedback.customer_ref,
                feedback.summary,
                feedback.sentiment,
                canonical_json(feedback.evidence_refs),
                int(feedback.action_required),
                int(feedback.operator_review_required),
                feedback.status,
                feedback.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_feedback_projection")
        return feedback.feedback_id

    def record_project_customer_commitment_receipt(
        self,
        receipt: ProjectCustomerCommitmentReceipt,
    ) -> dict[str, str | None]:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("workers cannot record customer commitment receipts")
        if self.command.payload.get("customer_commitment_requested") or self.command.payload.get("external_action_executed"):
            raise PermissionError("commitment receipt ingestion cannot create customer commitments or execute external actions")
        if not receipt.summary.strip():
            raise ValueError("customer commitment receipt summary is required")
        if receipt.receipt_type not in {"customer_response", "delivery_failure", "timeout", "compensation_needed"}:
            raise ValueError("unknown customer commitment receipt type")
        if receipt.source_type not in {"operator", "customer", "platform", "internal_signal"}:
            raise ValueError("unknown customer commitment receipt source type")
        if receipt.receipt_type in {"delivery_failure", "timeout", "compensation_needed"} and not receipt.action_required:
            raise PermissionError("failure, timeout, and compensation receipts require governed follow-up")

        commitment = self.conn.execute(
            """
            SELECT c.*, p.status AS packet_status, p.verdict AS packet_verdict
            FROM project_customer_commitments c
            JOIN project_customer_visible_packets p ON p.packet_id = c.packet_id
            WHERE c.commitment_id=?
            """,
            (receipt.commitment_id,),
        ).fetchone()
        if commitment is None:
            raise ValueError("customer commitment receipt requires an accepted commitment")
        if commitment["project_id"] != receipt.project_id:
            raise ValueError("customer commitment receipt project mismatch")
        if commitment["packet_status"] != "decided" or commitment["packet_verdict"] != "accept_customer_visible_packet":
            raise PermissionError("customer commitment receipts require an accepted customer-visible packet")

        customer_ref = receipt.customer_ref or commitment["customer_ref"]
        evidence_refs = _merge_refs(
            _loads(commitment["evidence_refs_json"]),
            receipt.evidence_refs,
            [
                f"kernel:project_customer_commitments/{receipt.commitment_id}",
                f"kernel:project_customer_visible_packets/{commitment['packet_id']}",
            ],
        )
        followup_task_id = receipt.followup_task_id
        if receipt.action_required and followup_task_id is None:
            followup_task_id = self._create_commitment_receipt_followup_task(
                commitment,
                receipt,
                customer_ref=customer_ref,
                evidence_refs=evidence_refs,
            )
        normalized = ProjectCustomerCommitmentReceipt(
            receipt_id=receipt.receipt_id,
            commitment_id=receipt.commitment_id,
            project_id=receipt.project_id,
            receipt_type=receipt.receipt_type,
            source_type=receipt.source_type,
            customer_ref=customer_ref,
            summary=receipt.summary,
            evidence_refs=evidence_refs,
            action_required=receipt.action_required,
            status=receipt.status,
            followup_task_id=followup_task_id,
            created_at=receipt.created_at,
        )
        payload = _project_customer_commitment_receipt_payload(normalized)
        event_id = self.append_event("project_customer_commitment_receipt_recorded", "project", normalized.receipt_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_customer_commitment_receipts (
              receipt_id, commitment_id, project_id, receipt_type, source_type,
              customer_ref, summary, evidence_refs_json, action_required,
              status, followup_task_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized.receipt_id,
                normalized.commitment_id,
                normalized.project_id,
                normalized.receipt_type,
                normalized.source_type,
                normalized.customer_ref,
                normalized.summary,
                canonical_json(normalized.evidence_refs),
                int(normalized.action_required),
                normalized.status,
                normalized.followup_task_id,
                normalized.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_customer_commitment_receipt_projection")
        return {
            "project_id": normalized.project_id,
            "commitment_id": normalized.commitment_id,
            "receipt_id": normalized.receipt_id,
            "followup_task_id": normalized.followup_task_id,
        }

    def record_project_revenue_attribution(self, attribution: ProjectRevenueAttribution) -> str:
        self._require_project(attribution.project_id)
        if attribution.task_id:
            self._require_project_task(attribution.project_id, attribution.task_id)
        if attribution.outcome_id:
            outcome = self.conn.execute(
                "SELECT project_id FROM project_outcomes WHERE outcome_id=?",
                (attribution.outcome_id,),
            ).fetchone()
            if outcome is None:
                raise ValueError("revenue attribution references unknown outcome")
            if outcome["project_id"] != attribution.project_id:
                raise ValueError("revenue attribution outcome/project mismatch")
        if attribution.artifact_receipt_id:
            artifact = self.conn.execute(
                "SELECT project_id FROM project_artifact_receipts WHERE receipt_id=?",
                (attribution.artifact_receipt_id,),
            ).fetchone()
            if artifact is None:
                raise ValueError("revenue attribution references unknown artifact receipt")
            if artifact["project_id"] != attribution.project_id:
                raise ValueError("revenue attribution artifact/project mismatch")
        if attribution.amount_usd < Decimal("0"):
            raise ValueError("revenue attribution amount must be non-negative")
        if not 0.0 <= attribution.confidence <= 1.0:
            raise ValueError("revenue attribution confidence must be between 0 and 1")
        if attribution.status == "reconciled" and not (attribution.external_ref or attribution.evidence_refs):
            raise ValueError("reconciled revenue attribution requires external ref or evidence")
        reconciliation_task_id = attribution.reconciliation_task_id
        if attribution.status == "needs_reconciliation" and reconciliation_task_id is None:
            task = ProjectTask(
                project_id=attribution.project_id,
                phase_name="Operate",
                task_type="operate",
                autonomy_class="A1",
                objective="Reconcile missing or low-confidence project revenue attribution evidence.",
                inputs={
                    "attribution_id": attribution.attribution_id,
                    "source": attribution.source,
                    "amount_usd": str(attribution.amount_usd),
                    "external_ref": attribution.external_ref,
                },
                expected_output_schema={
                    "type": "object",
                    "required": ["reconciliation_result", "evidence_refs", "operator_load_actual"],
                },
                risk_level="low",
                required_capabilities=[
                    {
                        "capability_type": "memory_write",
                        "actions": ["record"],
                        "scope": "project_revenue_reconciliation",
                        "grant_required_before_run": True,
                    }
                ],
                model_requirement={"task_class": "quick_research_summarization", "local_allowed_only_if_promoted": True},
                authority_required="rule",
                recovery_policy="ask_operator",
            )
            reconciliation_task_id = self.create_project_task(task)
        payload = _project_revenue_attribution_payload(attribution, reconciliation_task_id=reconciliation_task_id)
        event_id = self.append_event("project_revenue_attribution_recorded", "project", attribution.attribution_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_revenue_attributions (
              attribution_id, project_id, task_id, outcome_id, artifact_receipt_id,
              amount_usd, source, attribution_period, external_ref, evidence_refs_json,
              confidence, reconciliation_task_id, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                attribution.attribution_id,
                attribution.project_id,
                attribution.task_id,
                attribution.outcome_id,
                attribution.artifact_receipt_id,
                str(attribution.amount_usd),
                attribution.source,
                attribution.attribution_period,
                attribution.external_ref,
                canonical_json(attribution.evidence_refs),
                attribution.confidence,
                reconciliation_task_id,
                attribution.status,
                attribution.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_revenue_projection")
        return attribution.attribution_id

    def record_project_operator_load(self, load: ProjectOperatorLoadRecord) -> str:
        self._require_project(load.project_id)
        if load.task_id:
            self._require_project_task(load.project_id, load.task_id)
        if load.outcome_id:
            outcome = self.conn.execute(
                "SELECT project_id FROM project_outcomes WHERE outcome_id=?",
                (load.outcome_id,),
            ).fetchone()
            if outcome is None:
                raise ValueError("operator load references unknown outcome")
            if outcome["project_id"] != load.project_id:
                raise ValueError("operator load outcome/project mismatch")
        if load.artifact_receipt_id:
            artifact = self.conn.execute(
                "SELECT project_id FROM project_artifact_receipts WHERE receipt_id=?",
                (load.artifact_receipt_id,),
            ).fetchone()
            if artifact is None:
                raise ValueError("operator load references unknown artifact receipt")
            if artifact["project_id"] != load.project_id:
                raise ValueError("operator load artifact/project mismatch")
        if load.minutes < 0:
            raise ValueError("operator load minutes must be non-negative")
        if not load.source.strip():
            raise ValueError("operator load source is required")
        payload = _project_operator_load_payload(load)
        event_id = self.append_event("project_operator_load_recorded", "project", load.load_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_operator_load (
              load_id, project_id, task_id, outcome_id, artifact_receipt_id, minutes,
              load_type, source, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                load.load_id,
                load.project_id,
                load.task_id,
                load.outcome_id,
                load.artifact_receipt_id,
                load.minutes,
                load.load_type,
                load.source,
                load.notes,
                load.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_operator_load_projection")
        return load.load_id

    def record_project_post_ship_evidence(
        self,
        artifact_receipt_id: str,
        *,
        feedback: ProjectCustomerFeedback,
        revenue: ProjectRevenueAttribution,
        operator_load: ProjectOperatorLoadRecord,
    ) -> dict[str, str]:
        artifact = self.conn.execute(
            """
            SELECT receipt_id, project_id, task_id, artifact_kind, customer_visible,
                   side_effect_receipt_id, status
            FROM project_artifact_receipts
            WHERE receipt_id=?
            """,
            (artifact_receipt_id,),
        ).fetchone()
        if artifact is None:
            raise ValueError("post-ship evidence requires a shipped artifact receipt")
        if not artifact["side_effect_receipt_id"]:
            raise PermissionError("post-ship evidence requires a shipped artifact with side-effect authority")
        if artifact["artifact_kind"] != "shipped_artifact" or not artifact["customer_visible"]:
            raise ValueError("post-ship evidence must be tied to a customer-visible shipped artifact")
        if artifact["status"] != "accepted":
            raise ValueError("post-ship evidence requires an accepted shipped artifact")
        for label, record in {
            "feedback": feedback,
            "revenue": revenue,
            "operator_load": operator_load,
        }.items():
            if record.project_id != artifact["project_id"]:
                raise ValueError(f"post-ship {label} project mismatch")
            if record.task_id and record.task_id != artifact["task_id"]:
                raise ValueError(f"post-ship {label} task mismatch")
            if getattr(record, "artifact_receipt_id", None) and getattr(record, "artifact_receipt_id") != artifact_receipt_id:
                raise ValueError(f"post-ship {label} artifact mismatch")
        feedback_id = self.record_project_customer_feedback(
            ProjectCustomerFeedback(
                feedback_id=feedback.feedback_id,
                project_id=feedback.project_id,
                task_id=feedback.task_id or artifact["task_id"],
                artifact_receipt_id=artifact_receipt_id,
                source_type=feedback.source_type,
                customer_ref=feedback.customer_ref,
                summary=feedback.summary,
                sentiment=feedback.sentiment,
                evidence_refs=_with_ref(feedback.evidence_refs, f"kernel:project_artifact_receipts/{artifact_receipt_id}"),
                action_required=feedback.action_required,
                operator_review_required=feedback.operator_review_required,
                status=feedback.status,
                created_at=feedback.created_at,
            )
        )
        revenue_id = self.record_project_revenue_attribution(
            ProjectRevenueAttribution(
                attribution_id=revenue.attribution_id,
                project_id=revenue.project_id,
                task_id=revenue.task_id or artifact["task_id"],
                outcome_id=revenue.outcome_id,
                artifact_receipt_id=artifact_receipt_id,
                amount_usd=revenue.amount_usd,
                source=revenue.source,
                attribution_period=revenue.attribution_period,
                external_ref=revenue.external_ref,
                evidence_refs=_with_ref(revenue.evidence_refs, f"kernel:project_artifact_receipts/{artifact_receipt_id}"),
                confidence=revenue.confidence,
                reconciliation_task_id=revenue.reconciliation_task_id,
                status=revenue.status,
                created_at=revenue.created_at,
            )
        )
        load_id = self.record_project_operator_load(
            ProjectOperatorLoadRecord(
                load_id=operator_load.load_id,
                project_id=operator_load.project_id,
                task_id=operator_load.task_id or artifact["task_id"],
                outcome_id=operator_load.outcome_id,
                artifact_receipt_id=artifact_receipt_id,
                minutes=operator_load.minutes,
                load_type=operator_load.load_type,
                source=operator_load.source,
                notes=operator_load.notes,
                created_at=operator_load.created_at,
            )
        )
        return {
            "project_id": artifact["project_id"],
            "artifact_receipt_id": artifact_receipt_id,
            "feedback_id": feedback_id,
            "revenue_attribution_id": revenue_id,
            "operator_load_id": load_id,
        }

    def _derive_project_commercial_rollup(self, project_id: str) -> ProjectCommercialRollup:
        rows = self.conn.execute(
            """
            SELECT outcome_id, task_id, summary, artifact_refs_json, feedback_json,
                   revenue_impact_json, side_effect_intent_id, side_effect_receipt_id
            FROM project_outcomes
            WHERE project_id=? AND outcome_type='operate_followup' AND status='accepted'
            ORDER BY created_at, outcome_id
            """,
            (project_id,),
        ).fetchall()
        revenue_reconciled = Decimal("0")
        revenue_unreconciled = Decimal("0")
        retained = 0
        at_risk = 0
        churned = 0
        support_resolved = 0
        support_open = 0
        maintenance_resolved = 0
        maintenance_open = 0
        external_commitments = 0
        receiptless_side_effects = 0
        evidence_refs: list[str] = []
        risk_flags: list[str] = []

        for row in rows:
            feedback = _loads(row["feedback_json"])
            revenue_impact = _loads(row["revenue_impact_json"])
            followup_type = feedback.get("operate_followup_type")
            evidence_refs = _merge_refs(
                evidence_refs,
                _loads(row["artifact_refs_json"]),
                feedback.get("evidence_refs") or [],
                [f"kernel:project_outcomes/{row['outcome_id']}"],
            )
            if row["side_effect_intent_id"] and not row["side_effect_receipt_id"]:
                receiptless_side_effects += 1
                if "receiptless_operate_side_effect_intent" not in risk_flags:
                    risk_flags.append("receiptless_operate_side_effect_intent")
            if feedback.get("external_commitment_change") and row["side_effect_receipt_id"]:
                external_commitments += 1

            if followup_type == "revenue_reconciliation":
                amount = _decimal_from(revenue_impact.get("amount_usd", revenue_impact.get("amount", "0")))
                reconciled = feedback.get("reconciliation_status") == "reconciled" or feedback.get("revenue_status") == "reconciled"
                if reconciled:
                    revenue_reconciled += amount
                else:
                    revenue_unreconciled += amount
                    if amount and "unreconciled_operate_revenue" not in risk_flags:
                        risk_flags.append("unreconciled_operate_revenue")
            elif followup_type == "retention":
                status = str(feedback.get("retention_status", feedback.get("customer_retention_status", ""))).lower()
                if status in {"retained", "renewed", "expanded"}:
                    retained += 1
                elif status in {"at_risk", "risk", "needs_operator"}:
                    at_risk += 1
                elif status in {"churned", "lost"}:
                    churned += 1
            elif followup_type == "customer_support":
                status = str(feedback.get("support_status", "")).lower()
                if status in {"answered", "resolved", "closed"}:
                    support_resolved += 1
                elif status in {"open", "pending", "escalated", "needs_operator"}:
                    support_open += 1
            elif followup_type == "maintenance":
                status = str(feedback.get("maintenance_status", "")).lower()
                if status in {"resolved", "fixed", "closed"}:
                    maintenance_resolved += 1
                elif status in {"open", "pending", "regression", "needs_operator"}:
                    maintenance_open += 1

        if at_risk and "retention_at_risk" not in risk_flags:
            risk_flags.append("retention_at_risk")
        if churned and "customer_churned" not in risk_flags:
            risk_flags.append("customer_churned")
        if support_open and "support_open" not in risk_flags:
            risk_flags.append("support_open")
        if maintenance_open and "maintenance_open" not in risk_flags:
            risk_flags.append("maintenance_open")
        commitment_receipt_rows = self.conn.execute(
            """
            SELECT receipt_id, receipt_type, action_required, status
            FROM project_customer_commitment_receipts
            WHERE project_id=?
            ORDER BY created_at, receipt_id
            """,
            (project_id,),
        ).fetchall()
        for receipt in commitment_receipt_rows:
            evidence_refs = _merge_refs(evidence_refs, [f"kernel:project_customer_commitment_receipts/{receipt['receipt_id']}"])
            if receipt["action_required"] or receipt["status"] == "needs_followup":
                flag = f"customer_commitment_{receipt['receipt_type']}_needs_followup"
                if flag not in risk_flags:
                    risk_flags.append(flag)

        rollup = ProjectCommercialRollup(
            project_id=project_id,
            revenue_reconciled_usd=revenue_reconciled,
            revenue_unreconciled_usd=revenue_unreconciled,
            retained_customer_count=retained,
            at_risk_customer_count=at_risk,
            churned_customer_count=churned,
            support_resolved_count=support_resolved,
            support_open_count=support_open,
            maintenance_resolved_count=maintenance_resolved,
            maintenance_open_count=maintenance_open,
            external_commitment_count=external_commitments,
            receiptless_side_effect_count=receiptless_side_effects,
            evidence_refs=evidence_refs,
            risk_flags=risk_flags,
        )
        payload = _project_commercial_rollup_payload(rollup)
        event_id = self.append_event("project_commercial_rollup_derived", "project", rollup.rollup_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_commercial_rollups (
              rollup_id, project_id, revenue_reconciled_usd, revenue_unreconciled_usd,
              retained_customer_count, at_risk_customer_count, churned_customer_count,
              support_resolved_count, support_open_count, maintenance_resolved_count,
              maintenance_open_count, external_commitment_count, receiptless_side_effect_count,
              evidence_refs_json, risk_flags_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rollup.rollup_id,
                rollup.project_id,
                str(rollup.revenue_reconciled_usd),
                str(rollup.revenue_unreconciled_usd),
                rollup.retained_customer_count,
                rollup.at_risk_customer_count,
                rollup.churned_customer_count,
                rollup.support_resolved_count,
                rollup.support_open_count,
                rollup.maintenance_resolved_count,
                rollup.maintenance_open_count,
                rollup.external_commitment_count,
                rollup.receiptless_side_effect_count,
                canonical_json(rollup.evidence_refs),
                canonical_json(rollup.risk_flags),
                rollup.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_commercial_rollup_projection")
        return rollup

    def derive_project_status_rollup(self, project_id: str) -> ProjectStatusRollup:
        project = self._require_project(project_id)
        phases = self._project_phase_names(project_id)
        phase_rollups = [self._derive_project_phase_rollup(project_id, phase_name) for phase_name in phases]
        task_counts = self._project_task_counts(project_id)
        outcome_counts = self._project_outcome_counts(project_id)
        artifact_count = self._count_project_rows("project_artifact_receipts", project_id)
        feedback_count = self._count_project_rows("project_customer_feedback", project_id)
        revenue_total = self._project_revenue_total(project_id)
        load_minutes = self._project_operator_load_minutes(project_id)
        post_ship = self._project_post_ship_evidence_summary(project_id)
        commitment_receipts = self._project_commitment_receipt_summary(project_id)
        commercial_rollup = self._derive_project_commercial_rollup(project_id)
        commercial_payload = _project_commercial_rollup_payload(commercial_rollup)
        risk_flags: list[str] = []
        if task_counts.get("failed", 0):
            risk_flags.append("failed_tasks")
        if task_counts.get("blocked", 0):
            risk_flags.append("blocked_tasks")
        if feedback_count and not revenue_total:
            risk_flags.append("feedback_without_revenue")
        if post_ship["shipped_artifact_count"] and not post_ship["feedback_count"]:
            risk_flags.append("post_ship_feedback_missing")
        if post_ship["negative_feedback_count"]:
            risk_flags.append("negative_post_ship_feedback")
        if post_ship["action_required_count"]:
            risk_flags.append("post_ship_action_required")
        if post_ship["operator_load_minutes"] >= 60 and post_ship["revenue_attributed_usd"] == Decimal("0"):
            risk_flags.append("post_ship_operator_load_without_revenue")
        if commitment_receipts["open_followup_count"]:
            risk_flags.append("customer_commitment_receipt_followup_open")
        if commitment_receipts["delivery_failure_count"]:
            risk_flags.append("customer_delivery_failure")
        if commitment_receipts["timeout_count"]:
            risk_flags.append("customer_commitment_timeout")
        if commitment_receipts["compensation_needed_count"]:
            risk_flags.append("customer_compensation_needed")
        risk_flags.extend(flag for flag in commercial_rollup.risk_flags if flag not in risk_flags)
        recommended_status = project["status"]
        close_recommendation = "continue"
        commercial_or_post_ship_revenue = (
            commercial_rollup.revenue_reconciled_usd > Decimal("0")
            or post_ship["revenue_attributed_usd"] > Decimal("0")
        )
        if task_counts.get("failed", 0) or any(phase.status == "failed" for phase in phase_rollups):
            recommended_status = "kill_recommended"
            close_recommendation = "kill"
        elif task_counts.get("blocked", 0):
            recommended_status = "blocked"
            close_recommendation = "pause"
        elif commercial_rollup.churned_customer_count:
            recommended_status = "kill_recommended"
            close_recommendation = "kill"
        elif commercial_rollup.at_risk_customer_count or commercial_rollup.maintenance_open_count:
            recommended_status = "paused"
            close_recommendation = "pause"
        elif commercial_rollup.support_open_count:
            recommended_status = "active"
            close_recommendation = "continue"
        elif commercial_or_post_ship_revenue and (
            commercial_rollup.retained_customer_count or commercial_rollup.support_resolved_count
        ):
            recommended_status = "complete"
            close_recommendation = "complete"
        elif post_ship["negative_feedback_count"] and post_ship["revenue_attributed_usd"] == Decimal("0"):
            recommended_status = "kill_recommended"
            close_recommendation = "kill"
        elif post_ship["negative_feedback_count"]:
            recommended_status = "paused"
            close_recommendation = "pause"
        elif post_ship["action_required_count"]:
            recommended_status = "active"
            close_recommendation = "continue"
        elif post_ship["operator_load_minutes"] >= 60 and post_ship["revenue_attributed_usd"] == Decimal("0"):
            recommended_status = "kill_recommended"
            close_recommendation = "kill"
        elif post_ship["shipped_artifact_count"] and post_ship["feedback_count"] and post_ship["revenue_attributed_usd"] > Decimal("0"):
            recommended_status = "complete"
            close_recommendation = "complete"
        elif post_ship["open_followup_count"]:
            recommended_status = "active"
            close_recommendation = "continue"
        elif post_ship["shipped_artifact_count"] and post_ship["feedback_count"] and post_ship["revenue_attributed_usd"] == Decimal("0"):
            recommended_status = "paused"
            close_recommendation = "pause"
        elif task_counts.get("completed", 0) and not any(
            task_counts.get(status, 0) for status in ("queued", "running", "blocked", "failed")
        ):
            recommended_status = "complete"
            close_recommendation = "complete"
        rationale = (
            f"{task_counts.get('completed', 0)} completed tasks, "
            f"{task_counts.get('blocked', 0)} blocked tasks, "
            f"{task_counts.get('failed', 0)} failed tasks, "
            f"{revenue_total} USD attributed, {load_minutes} operator minutes. "
            f"Post-ship evidence: {post_ship['feedback_count']} feedback records, "
            f"{post_ship['revenue_attributed_usd']} USD, "
            f"{post_ship['operator_load_minutes']} operator minutes. "
            f"Operate commercial rollup: {commercial_rollup.revenue_reconciled_usd} reconciled USD, "
            f"{commercial_rollup.retained_customer_count} retained customers, "
            f"{commercial_rollup.support_open_count} open support records."
        )
        rollup = ProjectStatusRollup(
            project_id=project_id,
            project_status=project["status"],
            phase_rollups=phase_rollups,
            task_counts=task_counts,
            outcome_counts=outcome_counts,
            artifact_count=artifact_count,
            customer_feedback_count=feedback_count,
            revenue_attributed_usd=revenue_total,
            operator_load_minutes=load_minutes,
            recommended_status=recommended_status,
            close_recommendation=close_recommendation,  # type: ignore[arg-type]
            rationale=rationale,
            risk_flags=risk_flags,
            commercial_rollup_id=commercial_rollup.rollup_id,
            commercial_rollup=commercial_payload,
        )
        payload = _project_status_rollup_payload(rollup)
        event_id = self.append_event("project_status_rollup_derived", "project", rollup.rollup_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_status_rollups (
              rollup_id, project_id, project_status, phase_rollups_json,
              task_counts_json, outcome_counts_json, artifact_count,
              customer_feedback_count, revenue_attributed_usd, operator_load_minutes,
              recommended_status, close_recommendation, rationale, risk_flags_json,
              commercial_rollup_id, commercial_rollup_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rollup.rollup_id,
                rollup.project_id,
                rollup.project_status,
                canonical_json(payload["phase_rollups"]),
                canonical_json(rollup.task_counts),
                canonical_json(rollup.outcome_counts),
                rollup.artifact_count,
                rollup.customer_feedback_count,
                str(rollup.revenue_attributed_usd),
                rollup.operator_load_minutes,
                rollup.recommended_status,
                rollup.close_recommendation,
                rollup.rationale,
                canonical_json(rollup.risk_flags),
                rollup.commercial_rollup_id,
                canonical_json(rollup.commercial_rollup),
                rollup.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_status_rollup_projection")
        return rollup

    def create_project_close_decision(self, project_id: str, *, rollup_id: str | None = None) -> ProjectCloseDecisionPacket:
        self._require_project(project_id)
        if rollup_id is None:
            rollup = self.derive_project_status_rollup(project_id)
        else:
            row = self.conn.execute(
                "SELECT * FROM project_status_rollups WHERE rollup_id=? AND project_id=?",
                (rollup_id, project_id),
            ).fetchone()
            if row is None:
                raise ValueError("project close decision requires a rollup for the project")
            rollup = _rollup_from_row(row)
        evidence_refs = _merge_refs(
            [f"kernel:project_status_rollups/{rollup.rollup_id}"],
            (
                [f"kernel:project_commercial_rollups/{rollup.commercial_rollup_id}"]
                if rollup.commercial_rollup_id
                else []
            ),
            rollup.commercial_rollup.get("evidence_refs", []),
        )
        decision = Decision(
            decision_type="project_close",
            question=f"Should project {project_id} close, pause, continue, or be killed?",
            options=[
                {"verdict": "continue", "effect": "keep project active"},
                {"verdict": "complete", "effect": "mark project complete after operator approval"},
                {"verdict": "pause", "effect": "pause project without external side effects"},
                {"verdict": "kill", "effect": "recommend kill path; no customer obligations are cancelled"},
            ],
            stakes="medium",
            evidence_bundle_ids=[],
            evidence_refs=evidence_refs,
            requested_by="project",
            required_authority="operator_gate",
            authority_policy_version=KERNEL_POLICY_VERSION,
            status="gated",
            recommendation=rollup.close_recommendation,
            confidence=0.75 if rollup.close_recommendation != "continue" else 0.6,
            decisive_factors=[rollup.rationale],
            risk_flags=rollup.risk_flags,
            default_on_timeout="continue",
            gate_packet={
                "project_id": project_id,
                "rollup_id": rollup.rollup_id,
                "evidence_refs": evidence_refs,
                "side_effects_authorized": [],
                "default_on_timeout": "continue",
            },
        )
        self.create_decision(decision)
        packet = ProjectCloseDecisionPacket(
            project_id=project_id,
            decision_id=decision.decision_id,
            rollup_id=rollup.rollup_id,
            recommendation=rollup.close_recommendation,
            required_authority="operator_gate",
            rationale=rollup.rationale,
            risk_flags=rollup.risk_flags,
            evidence_refs=evidence_refs,
            default_on_timeout="continue",
        )
        payload = _project_close_decision_packet_payload(packet)
        event_id = self.append_event("project_close_decision_packet_created", "decision", packet.packet_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_close_decision_packets (
              packet_id, project_id, decision_id, rollup_id, recommendation,
              required_authority, rationale, risk_flags_json, evidence_refs_json,
              default_on_timeout, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.packet_id,
                packet.project_id,
                packet.decision_id,
                packet.rollup_id,
                packet.recommendation,
                packet.required_authority,
                packet.rationale,
                canonical_json(packet.risk_flags),
                canonical_json(packet.evidence_refs),
                packet.default_on_timeout,
                packet.status,
                packet.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_close_decision_projection")
        return packet

    def resolve_project_close_decision(
        self,
        packet_id: str,
        *,
        verdict: str,
        decided_by: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        if verdict not in {"continue", "complete", "pause", "kill"}:
            raise ValueError("project close verdict must be continue, complete, pause, or kill")
        if self.command.requested_by != "operator":
            raise PermissionError("project close decisions require an operator command")
        if self.command.requested_authority != "operator_gate":
            raise PermissionError("project close resolution requires operator-gate authority")
        row = self.conn.execute(
            """
            SELECT p.packet_id, p.project_id, p.decision_id, p.rollup_id,
                   p.recommendation, p.status AS packet_status,
                   d.status AS decision_status, pr.status AS project_status
            FROM project_close_decision_packets p
            JOIN decisions d ON d.decision_id = p.decision_id
            JOIN projects pr ON pr.project_id = p.project_id
            WHERE p.packet_id=?
            """,
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("project close decision packet not found")
        if row["packet_status"] != "gated":
            raise ValueError(f"cannot resolve project close packet from status {row['packet_status']}")
        if row["decision_status"] != "gated":
            raise ValueError(f"cannot resolve project close decision from status {row['decision_status']}")
        status_by_verdict = {
            "continue": "active",
            "complete": "complete",
            "pause": "paused",
            "kill": "killed",
        }
        previous_status = row["project_status"]
        project_status = status_by_verdict[verdict]
        if previous_status in {"complete", "killed"} and previous_status != project_status:
            raise ValueError(f"cannot resolve project close from terminal project status {previous_status}")

        self.resolve_decision(
            row["decision_id"],
            verdict=verdict,
            decided_by=decided_by,
            notes=notes,
            confidence=confidence,
        )
        decided_at = now_iso()
        followup_task_id: str | None = None
        if verdict == "continue":
            followup_task_id = self._create_post_ship_operate_followup_task(row["project_id"], packet_id, row["rollup_id"])
            if followup_task_id is None:
                followup_task_id = self._create_feedback_followup_task(row["project_id"], packet_id, row["rollup_id"])
        payload = {
            "packet_id": packet_id,
            "project_id": row["project_id"],
            "decision_id": row["decision_id"],
            "rollup_id": row["rollup_id"],
            "previous_project_status": previous_status,
            "project_status": project_status,
            "verdict": verdict,
            "recommendation": row["recommendation"],
            "decided_by": decided_by,
            "notes": notes,
            "followup_task_id": followup_task_id,
            "updated_at": decided_at,
            "decided_at": decided_at,
        }
        event_id = self.append_event("project_close_decision_resolved", "decision", packet_id, payload, actor_type="operator", actor_id=decided_by)
        self.conn.execute(
            "UPDATE project_close_decision_packets SET status='decided' WHERE packet_id=?",
            (packet_id,),
        )
        self.conn.execute(
            "UPDATE projects SET status=?, updated_at=? WHERE project_id=?",
            (project_status, decided_at, row["project_id"]),
        )
        self.enqueue_projection(event_id, "project_close_decision_projection")
        self.enqueue_projection(event_id, "project_projection")
        return {
            "packet_id": packet_id,
            "decision_id": row["decision_id"],
            "project_id": row["project_id"],
            "verdict": verdict,
            "project_status": project_status,
            "followup_task_id": followup_task_id,
        }

    def compare_project_replay_to_projection(self, project_id: str) -> ProjectReplayProjectionComparison:
        self._require_project(project_id)
        replay = KernelStore._replay_from_connection(self.conn)
        replay_project = replay.projects.get(project_id)
        replay_task_counts = _count_by_status(
            task for task in replay.project_tasks.values() if task.get("project_id") == project_id
        )
        projection_task_counts = self._project_task_counts(project_id)
        replay_revenue = sum(
            (Decimal(item["amount_usd"]) for item in replay.project_revenue_attributions.values() if item.get("project_id") == project_id),
            Decimal("0"),
        )
        projection_revenue = self._project_revenue_total(project_id)
        replay_load = sum(
            int(item["minutes"]) for item in replay.project_operator_load.values() if item.get("project_id") == project_id
        )
        projection_load = self._project_operator_load_minutes(project_id)
        replay_commercial_rollup = _latest_replay_project_commercial_rollup(replay, project_id)
        projection_commercial_rollup = self._latest_project_commercial_rollup_payload(project_id)
        projection_status = self.conn.execute(
            "SELECT status FROM projects WHERE project_id=?",
            (project_id,),
        ).fetchone()["status"]
        mismatches: list[str] = []
        if (replay_project or {}).get("status") != projection_status:
            mismatches.append("project_status")
        if replay_task_counts != projection_task_counts:
            mismatches.append("task_counts")
        if replay_revenue != projection_revenue:
            mismatches.append("revenue_attributed_usd")
        if replay_load != projection_load:
            mismatches.append("operator_load_minutes")
        if replay_commercial_rollup != projection_commercial_rollup:
            mismatches.append("commercial_rollup")
        comparison = ProjectReplayProjectionComparison(
            project_id=project_id,
            replay_project_status=(replay_project or {}).get("status"),
            projection_project_status=projection_status,
            replay_task_counts=replay_task_counts,
            projection_task_counts=projection_task_counts,
            replay_revenue_attributed_usd=replay_revenue,
            projection_revenue_attributed_usd=projection_revenue,
            replay_operator_load_minutes=replay_load,
            projection_operator_load_minutes=projection_load,
            replay_commercial_rollup=replay_commercial_rollup,
            projection_commercial_rollup=projection_commercial_rollup,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _project_replay_projection_comparison_payload(comparison)
        event_id = self.append_event("project_replay_projection_compared", "project", comparison.comparison_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_replay_projection_comparisons (
              comparison_id, project_id, replay_project_status, projection_project_status,
              replay_task_counts_json, projection_task_counts_json,
              replay_revenue_attributed_usd, projection_revenue_attributed_usd,
              replay_operator_load_minutes, projection_operator_load_minutes,
              replay_commercial_rollup_json, projection_commercial_rollup_json,
              matches, mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.project_id,
                comparison.replay_project_status,
                comparison.projection_project_status,
                canonical_json(comparison.replay_task_counts),
                canonical_json(comparison.projection_task_counts),
                str(comparison.replay_revenue_attributed_usd),
                str(comparison.projection_revenue_attributed_usd),
                comparison.replay_operator_load_minutes,
                comparison.projection_operator_load_minutes,
                canonical_json(comparison.replay_commercial_rollup),
                canonical_json(comparison.projection_commercial_rollup),
                int(comparison.matches),
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_replay_projection_comparison_projection")
        return comparison

    def create_project_portfolio_decision_packet(
        self,
        project_ids: list[str],
        *,
        scope: str = "active_commercial_projects",
        constraints: dict[str, Any] | None = None,
    ) -> ProjectPortfolioDecisionPacket:
        constraints = constraints or {}
        if self.command.requested_by in {"agent", "model"}:
            raise PermissionError("workers cannot create portfolio reprioritization packets")
        if self.command.requested_authority != "operator_gate":
            raise PermissionError("portfolio prioritization packets require operator-gate authority")
        if self.command.payload.get("autonomous_reprioritization") or self.command.payload.get("customer_commitment_requested"):
            raise PermissionError("portfolio packets cannot authorize autonomous reprioritization or customer commitments")
        if not project_ids:
            raise ValueError("portfolio packet requires at least one project")

        unique_project_ids = list(dict.fromkeys(project_ids))
        projects: list[sqlite3.Row] = [self._require_project(project_id) for project_id in unique_project_ids]
        rollups: list[ProjectStatusRollup] = []
        for project_id in unique_project_ids:
            rollups.append(self.derive_project_status_rollup(project_id))

        recommendations = [
            self._portfolio_project_recommendation(project, rollup, constraints)
            for project, rollup in zip(projects, rollups)
        ]
        recommendations.sort(key=lambda item: (-item["priority_score"], item["operator_load_minutes"], item["project_id"]))
        for index, item in enumerate(recommendations, start=1):
            item["priority_rank"] = index

        tradeoffs = _portfolio_tradeoffs(recommendations, constraints)
        risk_flags = _portfolio_risk_flags(recommendations, tradeoffs, constraints)
        recommendation = _portfolio_packet_recommendation(recommendations, risk_flags)
        rollup_ids = [rollup.rollup_id for rollup in rollups]
        evidence_refs = _merge_refs(
            *(
                _merge_refs(
                    [f"kernel:project_status_rollups/{rollup.rollup_id}"],
                    (
                        [f"kernel:project_commercial_rollups/{rollup.commercial_rollup_id}"]
                        if rollup.commercial_rollup_id
                        else []
                    ),
                    rollup.commercial_rollup.get("evidence_refs", []),
                )
                for rollup in rollups
            )
        )
        packet_body = {
            "scope": scope,
            "project_count": len(unique_project_ids),
            "recommendation": recommendation,
            "ranked_projects": recommendations,
            "tradeoffs": tradeoffs,
            "constraints": constraints,
            "authority": {
                "required_authority": "operator_gate",
                "authority_policy_version": KERNEL_POLICY_VERSION,
                "agents_may_recommend": True,
                "agents_may_reprioritize": False,
                "agents_may_commit_customer_work": False,
                "side_effects_authorized": [],
                "external_commitment_policy": "operator_only",
            },
            "default_on_timeout": "defer",
        }
        decision = Decision(
            decision_type="commercial_strategy",
            question=f"Approve portfolio priority packet for {scope}?",
            options=[
                {"verdict": "accept_prioritization", "effect": "operator accepts packet as planning guidance only"},
                {"verdict": "revise_prioritization", "effect": "operator requests a revised packet"},
                {"verdict": "defer", "effect": "no portfolio priority changes are made"},
            ],
            stakes="high",
            evidence_bundle_ids=[],
            evidence_refs=evidence_refs,
            requested_by="project",
            required_authority="operator_gate",
            authority_policy_version=KERNEL_POLICY_VERSION,
            status="gated",
            recommendation=recommendation,
            confidence=0.72 if recommendation in {"prioritize", "balance"} else 0.62,
            decisive_factors=[
                f"{item['project_id']} score={item['priority_score']} action={item['recommended_action']}"
                for item in recommendations
            ],
            risk_flags=risk_flags,
            default_on_timeout="defer",
            gate_packet={
                "portfolio_packet": packet_body,
                "options": ["accept_prioritization", "revise_prioritization", "defer"],
                "side_effects_authorized": [],
                "default_on_timeout": "defer",
            },
        )
        self.create_decision(decision)
        packet = ProjectPortfolioDecisionPacket(
            decision_id=decision.decision_id,
            scope=scope,
            project_ids=unique_project_ids,
            rollup_ids=rollup_ids,
            recommendation=recommendation,
            required_authority="operator_gate",
            packet=packet_body,
            tradeoffs=tradeoffs,
            evidence_refs=evidence_refs,
            risk_flags=risk_flags,
            default_on_timeout="defer",
        )
        payload = _project_portfolio_decision_packet_payload(packet)
        event_id = self.append_event("project_portfolio_decision_packet_created", "decision", packet.packet_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_portfolio_decision_packets (
              packet_id, decision_id, scope, project_ids_json, rollup_ids_json,
              recommendation, required_authority, packet_json, tradeoffs_json,
              evidence_refs_json, risk_flags_json, default_on_timeout, status,
              verdict, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.packet_id,
                packet.decision_id,
                packet.scope,
                canonical_json(packet.project_ids),
                canonical_json(packet.rollup_ids),
                packet.recommendation,
                packet.required_authority,
                canonical_json(packet.packet),
                canonical_json(packet.tradeoffs),
                canonical_json(packet.evidence_refs),
                canonical_json(packet.risk_flags),
                packet.default_on_timeout,
                packet.status,
                packet.verdict,
                packet.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_portfolio_decision_packet_projection")
        return packet

    def resolve_project_portfolio_decision(
        self,
        packet_id: str,
        *,
        verdict: str,
        decided_by: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        if verdict not in {"accept_prioritization", "revise_prioritization", "defer"}:
            raise ValueError("portfolio verdict must be accept_prioritization, revise_prioritization, or defer")
        if self.command.requested_by != "operator":
            raise PermissionError("portfolio decisions require an operator command")
        if self.command.requested_authority != "operator_gate":
            raise PermissionError("portfolio decision resolution requires operator-gate authority")
        row = self.conn.execute(
            """
            SELECT p.packet_id, p.decision_id, p.status AS packet_status, d.status AS decision_status
            FROM project_portfolio_decision_packets p
            JOIN decisions d ON d.decision_id = p.decision_id
            WHERE p.packet_id=?
            """,
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("portfolio decision packet not found")
        if row["packet_status"] != "gated" or row["decision_status"] != "gated":
            raise ValueError("portfolio decision packet is not gated")
        self.resolve_decision(
            row["decision_id"],
            verdict=verdict,
            decided_by=decided_by,
            notes=notes,
            confidence=confidence,
        )
        decided_at = now_iso()
        payload = {
            "packet_id": packet_id,
            "decision_id": row["decision_id"],
            "verdict": verdict,
            "decided_by": decided_by,
            "notes": notes,
            "authority_effect": "planning_guidance_only",
            "project_status_changes": [],
            "customer_commitments": [],
            "decided_at": decided_at,
        }
        event_id = self.append_event("project_portfolio_decision_resolved", "decision", packet_id, payload, actor_type="operator", actor_id=decided_by)
        self.conn.execute(
            "UPDATE project_portfolio_decision_packets SET status='decided', verdict=? WHERE packet_id=?",
            (verdict, packet_id),
        )
        self.enqueue_projection(event_id, "project_portfolio_decision_packet_projection")
        return {
            "packet_id": packet_id,
            "decision_id": row["decision_id"],
            "verdict": verdict,
            "authority_effect": "planning_guidance_only",
            "project_status_changes": [],
            "customer_commitments": [],
        }

    def compare_project_portfolio_replay_to_projection(self, packet_id: str) -> ProjectPortfolioReplayProjectionComparison:
        replay = KernelStore._replay_from_connection(self.conn)
        replay_packet = replay.project_portfolio_decision_packets.get(packet_id)
        row = self.conn.execute(
            "SELECT * FROM project_portfolio_decision_packets WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("portfolio decision packet not found")
        projection_packet = _portfolio_packet_from_row(row)
        mismatches: list[str] = []
        if replay_packet != projection_packet:
            mismatches.append("portfolio_packet")
        comparison = ProjectPortfolioReplayProjectionComparison(
            packet_id=packet_id,
            replay_packet=replay_packet or {},
            projection_packet=projection_packet,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _project_portfolio_replay_projection_comparison_payload(comparison)
        event_id = self.append_event("project_portfolio_replay_projection_compared", "project", comparison.comparison_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_portfolio_replay_projection_comparisons (
              comparison_id, packet_id, replay_packet_json, projection_packet_json,
              matches, mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.packet_id,
                canonical_json(comparison.replay_packet),
                canonical_json(comparison.projection_packet),
                int(comparison.matches),
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_portfolio_replay_projection_comparison_projection")
        return comparison

    def create_project_scheduling_intent(
        self,
        packet_id: str,
        *,
        scheduling_window: str = "next_internal_cycle",
    ) -> ProjectSchedulingIntent:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("workers cannot create project scheduling intents")
        if self.command.requested_authority not in {None, "rule"}:
            raise PermissionError("scheduling intents are internal rule-governed records")
        blocked_flags = {
            "autonomous_reprioritization",
            "autonomous_cancellation",
            "customer_commitment_requested",
            "priority_change_requested",
        }
        if any(self.command.payload.get(flag) for flag in blocked_flags):
            raise PermissionError("scheduling intents cannot reprioritize, cancel, or commit customer-facing work")
        if not scheduling_window.strip():
            raise ValueError("scheduling window is required")
        row = self.conn.execute(
            "SELECT * FROM project_portfolio_decision_packets WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("portfolio decision packet not found")
        if row["status"] != "decided" or row["verdict"] != "accept_prioritization":
            raise PermissionError("scheduling intents require an accepted operator portfolio packet")

        packet = _portfolio_packet_from_row(row)
        packet_body = packet["packet"]
        ranked = packet_body.get("ranked_projects", [])
        queue_adjustments = [
            _bounded_queue_adjustment(item, rank=index, constraints=packet_body.get("constraints", {}))
            for index, item in enumerate(ranked, start=1)
        ]
        intent_body = {
            "portfolio_packet_id": packet_id,
            "source_decision_id": row["decision_id"],
            "scheduling_window": scheduling_window,
            "scope": row["scope"],
            "authority": {
                "required_authority": "rule",
                "authority_effect": "internal_scheduling_recommendations_only",
                "priority_changes_require_operator_gate": True,
                "cancellations_require_operator_gate": True,
                "customer_commitments_allowed": False,
                "side_effects_authorized": [],
            },
            "bounds": {
                "max_queue_delta_tasks_per_project": 1,
                "allowed_task_types": ["operate", "feedback"],
                "customer_visible_work": False,
                "mutates_project_status": False,
                "mutates_task_priority": False,
                "cancels_tasks": False,
            },
            "tradeoffs": packet["tradeoffs"],
            "queue_adjustment_count": len(queue_adjustments),
        }
        risk_flags = list(dict.fromkeys([*packet["risk_flags"], *(_scheduling_risk_flags(queue_adjustments))]))
        evidence_refs = list(dict.fromkeys([f"kernel:project_portfolio_decision_packets/{packet_id}", *packet["evidence_refs"]]))
        intent = ProjectSchedulingIntent(
            portfolio_packet_id=packet_id,
            source_decision_id=row["decision_id"],
            scope=row["scope"],
            project_ids=packet["project_ids"],
            scheduling_window=scheduling_window,
            intent=intent_body,
            queue_adjustments=queue_adjustments,
            evidence_refs=evidence_refs,
            risk_flags=risk_flags,
            required_authority="rule",
            authority_effect="internal_scheduling_recommendations_only",
        )
        payload = _project_scheduling_intent_payload(intent)
        event_id = self.append_event("project_scheduling_intent_recorded", "task", intent.intent_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_scheduling_intents (
              intent_id, portfolio_packet_id, source_decision_id, scope,
              project_ids_json, scheduling_window, intent_json,
              queue_adjustments_json, evidence_refs_json, risk_flags_json,
              required_authority, authority_effect, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                intent.intent_id,
                intent.portfolio_packet_id,
                intent.source_decision_id,
                intent.scope,
                canonical_json(intent.project_ids),
                intent.scheduling_window,
                canonical_json(intent.intent),
                canonical_json(intent.queue_adjustments),
                canonical_json(intent.evidence_refs),
                canonical_json(intent.risk_flags),
                intent.required_authority,
                intent.authority_effect,
                intent.status,
                intent.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_scheduling_intent_projection")
        return intent

    def compare_project_scheduling_replay_to_projection(
        self,
        intent_id: str,
    ) -> ProjectSchedulingReplayProjectionComparison:
        replay = KernelStore._replay_from_connection(self.conn)
        replay_intent = replay.project_scheduling_intents.get(intent_id)
        row = self.conn.execute(
            "SELECT * FROM project_scheduling_intents WHERE intent_id=?",
            (intent_id,),
        ).fetchone()
        if row is None:
            raise ValueError("project scheduling intent not found")
        projection_intent = _project_scheduling_intent_from_row(row)
        mismatches: list[str] = []
        if replay_intent != projection_intent:
            mismatches.append("project_scheduling_intent")
        comparison = ProjectSchedulingReplayProjectionComparison(
            intent_id=intent_id,
            replay_intent=replay_intent or {},
            projection_intent=projection_intent,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _project_scheduling_replay_projection_comparison_payload(comparison)
        event_id = self.append_event("project_scheduling_replay_projection_compared", "task", comparison.comparison_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_scheduling_replay_projection_comparisons (
              comparison_id, intent_id, replay_intent_json, projection_intent_json,
              matches, mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.intent_id,
                canonical_json(comparison.replay_intent),
                canonical_json(comparison.projection_intent),
                int(comparison.matches),
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_scheduling_replay_projection_comparison_projection")
        return comparison

    def create_project_scheduling_priority_change_packet(
        self,
        intent_id: str,
    ) -> ProjectSchedulingPriorityChangePacket:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("workers cannot create scheduling priority-change packets")
        if self.command.requested_authority not in {None, "operator_gate"}:
            raise PermissionError("priority-change packets must be prepared as operator-gated decisions")
        blocked_flags = {
            "autonomous_queue_mutation",
            "autonomous_reprioritization",
            "autonomous_cancellation",
            "customer_commitment_requested",
            "priority_change_apply_requested",
        }
        if any(self.command.payload.get(flag) for flag in blocked_flags):
            raise PermissionError("priority-change packet creation cannot mutate queues, cancel work, or commit customers")
        row = self.conn.execute(
            "SELECT * FROM project_scheduling_intents WHERE intent_id=?",
            (intent_id,),
        ).fetchone()
        if row is None:
            raise ValueError("project scheduling intent not found")
        if row["status"] != "recorded":
            raise ValueError("project scheduling intent is not active")
        existing = self.conn.execute(
            """
            SELECT packet_id FROM project_scheduling_priority_change_packets
            WHERE intent_id=? AND status='gated'
            """,
            (intent_id,),
        ).fetchone()
        if existing is not None:
            raise ValueError("scheduling intent already has a gated priority-change packet")

        intent = _project_scheduling_intent_from_row(row)
        proposed_changes = [
            _priority_change_from_adjustment(adjustment, scheduling_window=row["scheduling_window"])
            for adjustment in intent["queue_adjustments"]
        ]
        evidence_refs = list(
            dict.fromkeys(
                [
                    f"kernel:project_scheduling_intents/{intent_id}",
                    f"kernel:project_portfolio_decision_packets/{row['portfolio_packet_id']}",
                    *intent["evidence_refs"],
                ]
            )
        )
        risk_flags = list(dict.fromkeys([*intent["risk_flags"], *(_priority_change_risk_flags(proposed_changes))]))
        decision = Decision(
            decision_type="commercial_strategy",
            question=f"Apply bounded internal queue priority changes for {row['scheduling_window']}?",
            options=[
                {"verdict": "accept_priority_changes", "effect": "operator applies bounded internal queue changes only"},
                {"verdict": "reject_priority_changes", "effect": "no queue changes are made"},
                {"verdict": "defer", "effect": "no queue changes are made before another operator review"},
            ],
            stakes="high",
            evidence_bundle_ids=[],
            evidence_refs=evidence_refs,
            requested_by="scheduler",
            required_authority="operator_gate",
            authority_policy_version=KERNEL_POLICY_VERSION,
            status="gated",
            recommendation="accept_priority_changes" if proposed_changes else "defer",
            confidence=0.7,
            decisive_factors=[
                f"{change['project_id']} rank={change['priority_rank']} action={change['queue_action']}"
                for change in proposed_changes
            ],
            risk_flags=risk_flags,
            default_on_timeout="defer",
            gate_packet={
                "scheduling_priority_change_packet": {
                    "intent_id": intent_id,
                    "scheduling_window": row["scheduling_window"],
                    "proposed_changes": proposed_changes,
                    "authority": {
                        "required_authority": "operator_gate",
                        "mutates_queue_on_packet_creation": False,
                        "applies_only_on_accept": True,
                        "customer_commitments_allowed": False,
                        "cancellations_allowed": False,
                        "side_effects_authorized": [],
                    },
                },
                "options": ["accept_priority_changes", "reject_priority_changes", "defer"],
                "side_effects_authorized": [],
                "default_on_timeout": "defer",
            },
        )
        self.create_decision(decision)
        packet = ProjectSchedulingPriorityChangePacket(
            intent_id=intent_id,
            portfolio_packet_id=row["portfolio_packet_id"],
            source_decision_id=row["source_decision_id"],
            decision_id=decision.decision_id,
            scope=row["scope"],
            project_ids=intent["project_ids"],
            scheduling_window=row["scheduling_window"],
            proposed_changes=proposed_changes,
            evidence_refs=evidence_refs,
            risk_flags=risk_flags,
            required_authority="operator_gate",
            default_on_timeout="defer",
        )
        payload = _project_scheduling_priority_change_packet_payload(packet)
        event_id = self.append_event("project_scheduling_priority_change_packet_created", "decision", packet.packet_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_scheduling_priority_change_packets (
              packet_id, intent_id, portfolio_packet_id, source_decision_id,
              decision_id, scope, project_ids_json, scheduling_window,
              proposed_changes_json, evidence_refs_json, risk_flags_json,
              required_authority, default_on_timeout, status, verdict,
              applied_changes_json, created_at, decided_by, decided_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.packet_id,
                packet.intent_id,
                packet.portfolio_packet_id,
                packet.source_decision_id,
                packet.decision_id,
                packet.scope,
                canonical_json(packet.project_ids),
                packet.scheduling_window,
                canonical_json(packet.proposed_changes),
                canonical_json(packet.evidence_refs),
                canonical_json(packet.risk_flags),
                packet.required_authority,
                packet.default_on_timeout,
                packet.status,
                packet.verdict,
                canonical_json(packet.applied_changes),
                packet.created_at,
                packet.decided_by,
                packet.decided_at,
            ),
        )
        self.enqueue_projection(event_id, "project_scheduling_priority_change_packet_projection")
        return packet

    def resolve_project_scheduling_priority_change_packet(
        self,
        packet_id: str,
        *,
        verdict: str,
        decided_by: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        if verdict not in {"accept_priority_changes", "reject_priority_changes", "defer"}:
            raise ValueError("priority-change verdict must be accept_priority_changes, reject_priority_changes, or defer")
        if self.command.requested_by != "operator":
            raise PermissionError("priority-change packet resolution requires an operator command")
        if self.command.requested_authority != "operator_gate":
            raise PermissionError("priority-change packet resolution requires operator-gate authority")
        blocked_flags = {
            "autonomous_queue_mutation",
            "autonomous_reprioritization",
            "autonomous_cancellation",
            "customer_commitment_requested",
        }
        if any(self.command.payload.get(flag) for flag in blocked_flags):
            raise PermissionError("priority-change resolution cannot authorize autonomous mutation, cancellation, or customer commitments")
        row = self.conn.execute(
            """
            SELECT p.*, d.status AS decision_status
            FROM project_scheduling_priority_change_packets p
            JOIN decisions d ON d.decision_id = p.decision_id
            WHERE p.packet_id=?
            """,
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("scheduling priority-change packet not found")
        if row["status"] != "gated" or row["decision_status"] != "gated":
            raise ValueError("scheduling priority-change packet is not gated")

        self.resolve_decision(
            row["decision_id"],
            verdict=verdict,
            decided_by=decided_by,
            notes=notes,
            confidence=confidence,
        )
        proposed_changes = _loads(row["proposed_changes_json"])
        applied_changes: list[dict[str, Any]] = []
        if verdict == "accept_priority_changes":
            for change in proposed_changes:
                if change["queue_action"] != "recommend_next_internal_task":
                    applied_changes.append(_not_applied_priority_change(change, reason="operator_accepted_no_queue_delta_for_action"))
                    continue
                if int(change["max_queue_delta_tasks"]) > 1:
                    raise PermissionError("priority-change packets may apply at most one queued task per project")
                task_id = self._create_scheduling_priority_task(row, change)
                applied_changes.append(
                    {
                        "project_id": change["project_id"],
                        "priority_rank": change["priority_rank"],
                        "queue_action": change["queue_action"],
                        "task_id": task_id,
                        "task_type": change["task_type"],
                        "status": "queued",
                        "customer_visible": False,
                        "external_side_effects_authorized": [],
                        "cancellation_applied": False,
                        "customer_commitment_applied": False,
                    }
                )
        else:
            applied_changes = [
                _not_applied_priority_change(change, reason=f"operator_{verdict}")
                for change in proposed_changes
            ]

        decided_at = now_iso()
        payload = {
            "packet_id": packet_id,
            "intent_id": row["intent_id"],
            "decision_id": row["decision_id"],
            "verdict": verdict,
            "decided_by": decided_by,
            "notes": notes,
            "authority_effect": "bounded_internal_queue_changes" if verdict == "accept_priority_changes" else "no_queue_changes",
            "applied_changes": applied_changes,
            "customer_commitments": [],
            "cancellations": [],
            "side_effects_authorized": [],
            "decided_at": decided_at,
        }
        event_id = self.append_event(
            "project_scheduling_priority_change_packet_resolved",
            "decision",
            packet_id,
            payload,
            actor_type="operator",
            actor_id=decided_by,
        )
        self.conn.execute(
            """
            UPDATE project_scheduling_priority_change_packets
            SET status='decided', verdict=?, applied_changes_json=?, decided_by=?, decided_at=?
            WHERE packet_id=?
            """,
            (verdict, canonical_json(applied_changes), decided_by, decided_at, packet_id),
        )
        self.enqueue_projection(event_id, "project_scheduling_priority_change_packet_projection")
        return {
            "packet_id": packet_id,
            "decision_id": row["decision_id"],
            "verdict": verdict,
            "authority_effect": payload["authority_effect"],
            "applied_changes": applied_changes,
            "customer_commitments": [],
            "cancellations": [],
        }

    def compare_project_scheduling_priority_replay_to_projection(
        self,
        packet_id: str,
    ) -> ProjectSchedulingPriorityReplayProjectionComparison:
        replay = KernelStore._replay_from_connection(self.conn)
        replay_packet = replay.project_scheduling_priority_change_packets.get(packet_id)
        row = self.conn.execute(
            "SELECT * FROM project_scheduling_priority_change_packets WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("scheduling priority-change packet not found")
        projection_packet = _project_scheduling_priority_change_packet_from_row(row)
        mismatches: list[str] = []
        if replay_packet != projection_packet:
            mismatches.append("project_scheduling_priority_change_packet")
        comparison = ProjectSchedulingPriorityReplayProjectionComparison(
            packet_id=packet_id,
            replay_packet=replay_packet or {},
            projection_packet=projection_packet,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _project_scheduling_priority_replay_projection_comparison_payload(comparison)
        event_id = self.append_event("project_scheduling_priority_replay_projection_compared", "task", comparison.comparison_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_scheduling_priority_replay_projection_comparisons (
              comparison_id, packet_id, replay_packet_json, projection_packet_json,
              matches, mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.packet_id,
                canonical_json(comparison.replay_packet),
                canonical_json(comparison.projection_packet),
                int(comparison.matches),
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_scheduling_priority_replay_projection_comparison_projection")
        return comparison

    def create_project_customer_visible_packet(
        self,
        outcome_id: str,
        *,
        packet_type: str,
        customer_ref: str,
        channel: str,
        subject: str,
        summary: str,
        payload_ref: str,
        side_effect_intent_id: str,
    ) -> ProjectCustomerVisiblePacket:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("workers cannot create customer-visible packets")
        if self.command.requested_authority != "operator_gate":
            raise PermissionError("customer-visible packets require operator-gate authority")
        if self.command.payload.get("autonomous_customer_visible") or self.command.payload.get("external_action_executed"):
            raise PermissionError("customer-visible packets cannot be autonomous or record executed external actions")
        if packet_type not in {"customer_message", "customer_delivery"}:
            raise ValueError("customer-visible packet type must be customer_message or customer_delivery")
        required = [customer_ref, channel, subject, summary, payload_ref, side_effect_intent_id]
        if any(not value.strip() for value in required):
            raise ValueError("customer-visible packet requires customer, channel, subject, summary, payload ref, and intent")

        outcome = self.conn.execute(
            """
            SELECT outcome_id, project_id, task_id, status, outcome_type, summary,
                   artifact_refs_json, side_effect_intent_id, side_effect_receipt_id
            FROM project_outcomes
            WHERE outcome_id=?
            """,
            (outcome_id,),
        ).fetchone()
        if outcome is None:
            raise ValueError("customer-visible packet requires an existing internal outcome")
        if outcome["status"] != "accepted":
            raise PermissionError("customer-visible packets require accepted internal outcome evidence")
        if outcome["side_effect_receipt_id"]:
            raise PermissionError("customer-visible packets must be prepared from internal outcomes before external commitment")
        if not outcome["task_id"]:
            raise PermissionError("customer-visible packets require task-linked internal outcome evidence")

        intent = self.conn.execute(
            """
            SELECT intent_id, task_id, side_effect_type, required_authority, status
            FROM side_effect_intents
            WHERE intent_id=?
            """,
            (side_effect_intent_id,),
        ).fetchone()
        if intent is None:
            raise ValueError("customer-visible packet requires a durable side-effect intent")
        if intent["task_id"] != outcome["task_id"]:
            raise ValueError("customer-visible side-effect intent task does not match outcome task")
        if intent["required_authority"] != "operator_gate":
            raise PermissionError("customer-visible side-effect intent must require operator gate")
        if intent["status"] != "prepared":
            raise PermissionError("customer-visible packet requires a prepared side-effect intent")
        if packet_type == "customer_message" and intent["side_effect_type"] != "message":
            raise ValueError("customer message packets require a message side-effect intent")
        if packet_type == "customer_delivery" and intent["side_effect_type"] not in {"publish", "deploy", "message"}:
            raise ValueError("customer delivery packets require publish, deploy, or message side-effect intent")

        existing = self.conn.execute(
            """
            SELECT packet_id FROM project_customer_visible_packets
            WHERE outcome_id=? AND status='gated'
            """,
            (outcome_id,),
        ).fetchone()
        if existing is not None:
            raise ValueError("internal outcome already has a gated customer-visible packet")

        evidence_refs = _merge_refs(
            [f"kernel:project_outcomes/{outcome_id}", f"kernel:project_tasks/{outcome['task_id']}"],
            _loads(outcome["artifact_refs_json"]),
            [f"kernel:side_effect_intents/{side_effect_intent_id}"],
        )
        risk_flags = ["customer_visible_commitment_requires_receipt"]
        decision = Decision(
            decision_type="commercial_strategy",
            question=f"Approve {packet_type.replace('_', ' ')} for {customer_ref}?",
            options=[
                {"verdict": "accept_customer_visible_packet", "effect": "record commitment only with successful durable receipt"},
                {"verdict": "reject_customer_visible_packet", "effect": "no customer commitment or side effect is recorded"},
                {"verdict": "defer", "effect": "keep packet gated; no customer commitment or side effect is recorded"},
            ],
            stakes="high",
            evidence_bundle_ids=[],
            evidence_refs=evidence_refs,
            requested_by="project",
            required_authority="operator_gate",
            authority_policy_version=KERNEL_POLICY_VERSION,
            status="gated",
            recommendation="accept_customer_visible_packet",
            confidence=0.72,
            decisive_factors=[
                f"internal_outcome={outcome_id}",
                f"side_effect_intent={side_effect_intent_id}",
                "durable_receipt_required_before_customer_commitment",
            ],
            risk_flags=risk_flags,
            default_on_timeout="defer",
            gate_packet={
                "customer_visible_packet": {
                    "outcome_id": outcome_id,
                    "packet_type": packet_type,
                    "customer_ref": customer_ref,
                    "channel": channel,
                    "subject": subject,
                    "payload_ref": payload_ref,
                    "side_effect_intent_id": side_effect_intent_id,
                    "authority": {
                        "required_authority": "operator_gate",
                        "records_commitment_on_creation": False,
                        "receipt_required_before_commitment": True,
                        "replay_executes_external_effects": False,
                    },
                },
                "options": ["accept_customer_visible_packet", "reject_customer_visible_packet", "defer"],
                "side_effects_authorized": [side_effect_intent_id],
                "default_on_timeout": "defer",
            },
        )
        self.create_decision(decision)
        packet = ProjectCustomerVisiblePacket(
            project_id=outcome["project_id"],
            outcome_id=outcome_id,
            decision_id=decision.decision_id,
            packet_type=packet_type,  # type: ignore[arg-type]
            customer_ref=customer_ref,
            channel=channel,
            subject=subject,
            summary=summary,
            payload_ref=payload_ref,
            side_effect_intent_id=side_effect_intent_id,
            evidence_refs=evidence_refs,
            risk_flags=risk_flags,
            required_authority="operator_gate",
            default_on_timeout="defer",
        )
        payload = _project_customer_visible_packet_payload(packet)
        event_id = self.append_event("project_customer_visible_packet_created", "decision", packet.packet_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_customer_visible_packets (
              packet_id, project_id, outcome_id, decision_id, packet_type,
              customer_ref, channel, subject, summary, payload_ref,
              side_effect_intent_id, evidence_refs_json, risk_flags_json,
              required_authority, default_on_timeout, status, verdict,
              created_at, decided_by, decided_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.packet_id,
                packet.project_id,
                packet.outcome_id,
                packet.decision_id,
                packet.packet_type,
                packet.customer_ref,
                packet.channel,
                packet.subject,
                packet.summary,
                packet.payload_ref,
                packet.side_effect_intent_id,
                canonical_json(packet.evidence_refs),
                canonical_json(packet.risk_flags),
                packet.required_authority,
                packet.default_on_timeout,
                packet.status,
                packet.verdict,
                packet.created_at,
                packet.decided_by,
                packet.decided_at,
            ),
        )
        self.enqueue_projection(event_id, "project_customer_visible_packet_projection")
        return packet

    def resolve_project_customer_visible_packet(
        self,
        packet_id: str,
        *,
        verdict: str,
        side_effect_receipt_id: str | None = None,
        decided_by: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        if verdict not in {"accept_customer_visible_packet", "reject_customer_visible_packet", "defer"}:
            raise ValueError("customer-visible verdict must be accept_customer_visible_packet, reject_customer_visible_packet, or defer")
        if self.command.requested_by != "operator":
            raise PermissionError("customer-visible packet resolution requires an operator command")
        if self.command.requested_authority != "operator_gate":
            raise PermissionError("customer-visible packet resolution requires operator-gate authority")
        if self.command.payload.get("autonomous_customer_visible") or self.command.payload.get("external_action_executed"):
            raise PermissionError("customer-visible packet resolution cannot record autonomous external actions")
        row = self.conn.execute(
            """
            SELECT p.*, d.status AS decision_status
            FROM project_customer_visible_packets p
            JOIN decisions d ON d.decision_id = p.decision_id
            WHERE p.packet_id=?
            """,
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("customer-visible packet not found")
        if row["status"] != "gated" or row["decision_status"] != "gated":
            raise ValueError("customer-visible packet is not gated")
        if verdict == "accept_customer_visible_packet" and not side_effect_receipt_id:
            raise PermissionError("accepted customer-visible packets require a durable side-effect receipt")
        if verdict != "accept_customer_visible_packet" and side_effect_receipt_id:
            raise PermissionError("rejected or deferred customer-visible packets cannot record side-effect receipts")

        self.resolve_decision(
            row["decision_id"],
            verdict=verdict,
            decided_by=decided_by,
            notes=notes,
            confidence=confidence,
        )
        decided_at = now_iso()
        commitment_id: str | None = None
        commitment_payload: dict[str, Any] | None = None
        if verdict == "accept_customer_visible_packet":
            side_effect = self._successful_task_side_effect(
                task_id=self._outcome_task_id(row["outcome_id"]),
                receipt_id=side_effect_receipt_id or "",
                intent_id=row["side_effect_intent_id"],
            )
            evidence_refs = _merge_refs(
                _loads(row["evidence_refs_json"]),
                [
                    f"kernel:project_customer_visible_packets/{packet_id}",
                    f"kernel:side_effect_receipts/{side_effect_receipt_id}",
                ],
            )
            commitment = ProjectCustomerCommitment(
                packet_id=packet_id,
                project_id=row["project_id"],
                outcome_id=row["outcome_id"],
                side_effect_intent_id=side_effect["intent_id"],
                side_effect_receipt_id=side_effect_receipt_id or "",
                customer_ref=row["customer_ref"],
                channel=row["channel"],
                commitment_type="message_sent" if row["packet_type"] == "customer_message" else "delivery_made",
                payload_ref=row["payload_ref"],
                summary=row["summary"],
                evidence_refs=evidence_refs,
            )
            commitment_payload = _project_customer_commitment_payload(commitment)
            commitment_event_id = self.append_event(
                "project_customer_commitment_recorded",
                "project",
                commitment.commitment_id,
                commitment_payload,
                actor_type="operator",
                actor_id=decided_by,
            )
            self.conn.execute(
                """
                INSERT INTO project_customer_commitments (
                  commitment_id, packet_id, project_id, outcome_id,
                  side_effect_intent_id, side_effect_receipt_id, customer_ref,
                  channel, commitment_type, payload_ref, summary,
                  evidence_refs_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    commitment.commitment_id,
                    commitment.packet_id,
                    commitment.project_id,
                    commitment.outcome_id,
                    commitment.side_effect_intent_id,
                    commitment.side_effect_receipt_id,
                    commitment.customer_ref,
                    commitment.channel,
                    commitment.commitment_type,
                    commitment.payload_ref,
                    commitment.summary,
                    canonical_json(commitment.evidence_refs),
                    commitment.created_at,
                ),
            )
            self.enqueue_projection(commitment_event_id, "project_customer_commitment_projection")
            commitment_id = commitment.commitment_id

        payload = {
            "packet_id": packet_id,
            "decision_id": row["decision_id"],
            "project_id": row["project_id"],
            "outcome_id": row["outcome_id"],
            "verdict": verdict,
            "decided_by": decided_by,
            "notes": notes,
            "side_effect_intent_id": row["side_effect_intent_id"],
            "side_effect_receipt_id": side_effect_receipt_id,
            "customer_commitment_id": commitment_id,
            "customer_commitments": [commitment_payload] if commitment_payload else [],
            "external_effects_executed_by_replay": False,
            "decided_at": decided_at,
        }
        event_id = self.append_event(
            "project_customer_visible_packet_resolved",
            "decision",
            packet_id,
            payload,
            actor_type="operator",
            actor_id=decided_by,
        )
        self.conn.execute(
            """
            UPDATE project_customer_visible_packets
            SET status='decided', verdict=?, decided_by=?, decided_at=?
            WHERE packet_id=?
            """,
            (verdict, decided_by, decided_at, packet_id),
        )
        self.enqueue_projection(event_id, "project_customer_visible_packet_projection")
        return {
            "packet_id": packet_id,
            "decision_id": row["decision_id"],
            "verdict": verdict,
            "customer_commitment_id": commitment_id,
            "customer_commitments": [commitment_payload] if commitment_payload else [],
            "side_effect_intent_id": row["side_effect_intent_id"],
            "side_effect_receipt_id": side_effect_receipt_id,
        }

    def compare_project_customer_visible_replay_to_projection(
        self,
        packet_id: str,
    ) -> ProjectCustomerVisibleReplayProjectionComparison:
        replay = KernelStore._replay_from_connection(self.conn)
        replay_packet = replay.project_customer_visible_packets.get(packet_id)
        row = self.conn.execute(
            "SELECT * FROM project_customer_visible_packets WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("customer-visible packet not found")
        projection_packet = _project_customer_visible_packet_from_row(row)
        replay_commitments = sorted(
            (
                commitment
                for commitment in replay.project_customer_commitments.values()
                if commitment.get("packet_id") == packet_id
            ),
            key=lambda item: item["commitment_id"],
        )
        projection_commitments = self._project_customer_commitments_for_packet(packet_id)
        replay_commitment_ids = {commitment["commitment_id"] for commitment in replay_commitments}
        projection_commitment_ids = {commitment["commitment_id"] for commitment in projection_commitments}
        replay_commitment_receipts = sorted(
            (
                receipt
                for receipt in replay.project_customer_commitment_receipts.values()
                if receipt.get("commitment_id") in replay_commitment_ids
            ),
            key=lambda item: item["receipt_id"],
        )
        projection_commitment_receipts = self._project_customer_commitment_receipts_for_commitments(
            projection_commitment_ids
        )
        mismatches: list[str] = []
        if replay_packet != projection_packet:
            mismatches.append("project_customer_visible_packet")
        if replay_commitments != projection_commitments:
            mismatches.append("project_customer_commitments")
        if replay_commitment_receipts != projection_commitment_receipts:
            mismatches.append("project_customer_commitment_receipts")
        comparison = ProjectCustomerVisibleReplayProjectionComparison(
            packet_id=packet_id,
            replay_packet=replay_packet or {},
            projection_packet=projection_packet,
            replay_commitments=replay_commitments,
            projection_commitments=projection_commitments,
            replay_commitment_receipts=replay_commitment_receipts,
            projection_commitment_receipts=projection_commitment_receipts,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _project_customer_visible_replay_projection_comparison_payload(comparison)
        event_id = self.append_event("project_customer_visible_replay_projection_compared", "project", comparison.comparison_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_customer_visible_replay_projection_comparisons (
              comparison_id, packet_id, replay_packet_json, projection_packet_json,
              replay_commitments_json, projection_commitments_json,
              replay_commitment_receipts_json, projection_commitment_receipts_json,
              matches, mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.packet_id,
                canonical_json(comparison.replay_packet),
                canonical_json(comparison.projection_packet),
                canonical_json(comparison.replay_commitments),
                canonical_json(comparison.projection_commitments),
                canonical_json(comparison.replay_commitment_receipts),
                canonical_json(comparison.projection_commitment_receipts),
                int(comparison.matches),
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_customer_visible_replay_projection_comparison_projection")
        return comparison

    def _outcome_task_id(self, outcome_id: str) -> str:
        row = self.conn.execute("SELECT task_id FROM project_outcomes WHERE outcome_id=?", (outcome_id,)).fetchone()
        if row is None or not row["task_id"]:
            raise ValueError("customer-visible packet outcome lacks task evidence")
        return row["task_id"]

    def _project_customer_commitments_for_packet(self, packet_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM project_customer_commitments
            WHERE packet_id=?
            ORDER BY commitment_id
            """,
            (packet_id,),
        ).fetchall()
        return [_project_customer_commitment_from_row(row) for row in rows]

    def _project_customer_commitment_receipts_for_commitments(self, commitment_ids: set[str]) -> list[dict[str, Any]]:
        if not commitment_ids:
            return []
        placeholders = ",".join("?" for _ in commitment_ids)
        rows = self.conn.execute(
            f"""
            SELECT * FROM project_customer_commitment_receipts
            WHERE commitment_id IN ({placeholders})
            ORDER BY receipt_id
            """,
            tuple(sorted(commitment_ids)),
        ).fetchall()
        return [_project_customer_commitment_receipt_from_row(row) for row in rows]

    def _create_scheduling_priority_task(self, packet_row: sqlite3.Row, change: dict[str, Any]) -> str:
        if change.get("customer_visible") or change.get("external_side_effects_authorized"):
            raise PermissionError("scheduling priority changes cannot create customer-visible or side-effecting work")
        task = ProjectTask(
            project_id=change["project_id"],
            phase_name="Operate",
            task_type=change["task_type"],
            autonomy_class="A1",
            objective=f"Execute internal scheduling priority rank {change['priority_rank']} for {packet_row['scheduling_window']}.",
            inputs={
                "scheduling_priority_packet_id": packet_row["packet_id"],
                "scheduling_intent_id": packet_row["intent_id"],
                "portfolio_packet_id": packet_row["portfolio_packet_id"],
                "priority_rank": change["priority_rank"],
                "queue_action": change["queue_action"],
                "tradeoff_drivers": change["tradeoff_drivers"],
                "customer_visible": False,
                "external_side_effects_authorized": [],
                "customer_commitments_allowed": False,
                "cancellation_allowed": False,
            },
            expected_output_schema={
                "type": "object",
                "required": ["internal_result_ref", "external_commitment_change"],
                "properties": {
                    "internal_result_ref": {"type": "string"},
                    "external_commitment_change": {"const": False},
                },
            },
            risk_level="low",
            required_capabilities=[
                {
                    "capability_type": "memory_write",
                    "actions": ["record"],
                    "scope": "project_internal_scheduling",
                    "grant_required_before_run": True,
                    "external_side_effects": "blocked_without_operator_gate_and_receipt",
                }
            ],
            model_requirement={
                "task_class": "quick_research_summarization",
                "local_allowed_only_if_promoted": True,
                "frontier_fallback_allowed_with_budget": False,
            },
            authority_required="rule",
            recovery_policy="ask_operator",
            budget_id=self._project_budget_id(change["project_id"]),
            idempotency_key=f"scheduling-priority:{packet_row['packet_id']}:{change['project_id']}:{change['priority_rank']}",
            evidence_refs=[
                f"kernel:project_scheduling_priority_change_packets/{packet_row['packet_id']}",
                f"kernel:project_scheduling_intents/{packet_row['intent_id']}",
                *change.get("evidence_refs", []),
            ],
        )
        return self.create_project_task(task)

    def _portfolio_project_recommendation(
        self,
        project: sqlite3.Row,
        rollup: ProjectStatusRollup,
        constraints: dict[str, Any],
    ) -> dict[str, Any]:
        commercial = rollup.commercial_rollup
        budget = self._project_budget_summary(self._project_budget_id(project["project_id"]))
        revenue = Decimal(commercial.get("revenue_reconciled_usd", "0"))
        unreconciled = Decimal(commercial.get("revenue_unreconciled_usd", "0"))
        retained = int(commercial.get("retained_customer_count", 0))
        at_risk = int(commercial.get("at_risk_customer_count", 0))
        churned = int(commercial.get("churned_customer_count", 0))
        support_open = int(commercial.get("support_open_count", 0))
        maintenance_open = int(commercial.get("maintenance_open_count", 0))
        receiptless = int(commercial.get("receiptless_side_effect_count", 0))
        load_minutes = int(rollup.operator_load_minutes)
        score = int(revenue) + retained * 150 - load_minutes * 2 - at_risk * 120 - churned * 300
        score -= support_open * 40 + maintenance_open * 80 + receiptless * 200 + int(unreconciled / Decimal("2"))
        min_remaining = Decimal(str(constraints.get("min_budget_remaining_usd", "0")))
        if budget["remaining_usd"] < min_remaining:
            score -= 100
        if rollup.close_recommendation == "complete":
            action = "harvest_or_complete"
        elif rollup.close_recommendation == "kill":
            action = "kill_or_stop_investment"
        elif rollup.close_recommendation == "pause":
            action = "pause_until_operator_review"
        elif score >= int(constraints.get("accelerate_score_threshold", 250)):
            action = "prioritize_next"
        else:
            action = "continue_bounded"
        return {
            "project_id": project["project_id"],
            "project_name": self._project_name(project["project_id"]),
            "project_status": project["status"],
            "rollup_id": rollup.rollup_id,
            "commercial_rollup_id": rollup.commercial_rollup_id,
            "recommended_action": action,
            "priority_score": score,
            "close_recommendation": rollup.close_recommendation,
            "budget": {
                **budget,
                "cap_usd": str(budget["cap_usd"]),
                "spent_usd": str(budget["spent_usd"]),
                "reserved_usd": str(budget["reserved_usd"]),
                "remaining_usd": str(budget["remaining_usd"]),
            },
            "operator_load_minutes": load_minutes,
            "retention": {
                "retained": retained,
                "at_risk": at_risk,
                "churned": churned,
            },
            "revenue": {
                "reconciled_usd": str(revenue),
                "unreconciled_usd": str(unreconciled),
            },
            "support_open_count": support_open,
            "maintenance_open_count": maintenance_open,
            "risk_flags": rollup.risk_flags,
            "evidence_refs": _merge_refs(
                [f"kernel:project_status_rollups/{rollup.rollup_id}"],
                (
                    [f"kernel:project_commercial_rollups/{rollup.commercial_rollup_id}"]
                    if rollup.commercial_rollup_id
                    else []
                ),
                rollup.commercial_rollup.get("evidence_refs", []),
            ),
        }

    def _project_budget_summary(self, budget_id: str | None) -> dict[str, Any]:
        if budget_id is None:
            return {
                "budget_id": None,
                "cap_usd": Decimal("0"),
                "spent_usd": Decimal("0"),
                "reserved_usd": Decimal("0"),
                "remaining_usd": Decimal("0"),
                "status": "none",
            }
        row = self.conn.execute(
            "SELECT budget_id, cap_usd, spent_usd, reserved_usd, status FROM budgets WHERE budget_id=?",
            (budget_id,),
        ).fetchone()
        if row is None:
            return {
                "budget_id": budget_id,
                "cap_usd": Decimal("0"),
                "spent_usd": Decimal("0"),
                "reserved_usd": Decimal("0"),
                "remaining_usd": Decimal("0"),
                "status": "missing",
            }
        cap = Decimal(row["cap_usd"])
        spent = Decimal(row["spent_usd"])
        reserved = Decimal(row["reserved_usd"])
        return {
            "budget_id": row["budget_id"],
            "cap_usd": cap,
            "spent_usd": spent,
            "reserved_usd": reserved,
            "remaining_usd": cap - spent - reserved,
            "status": row["status"],
        }

    def _project_budget_id(self, project_id: str) -> str | None:
        row = self.conn.execute("SELECT budget_id FROM projects WHERE project_id=?", (project_id,)).fetchone()
        return row["budget_id"] if row else None

    def _project_name(self, project_id: str) -> str:
        row = self.conn.execute("SELECT name FROM projects WHERE project_id=?", (project_id,)).fetchone()
        return row["name"] if row else project_id

    def _latest_project_commercial_rollup_payload(self, project_id: str) -> dict[str, Any]:
        row = self.conn.execute(
            """
            SELECT *
            FROM project_commercial_rollups
            WHERE project_id=?
            ORDER BY created_at DESC, rollup_id DESC
            LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        if row is None:
            return {}
        return _project_commercial_rollup_payload(_commercial_rollup_from_row(row))

    def _project_phase_names(self, project_id: str) -> list[str]:
        row = self.conn.execute("SELECT phases_json FROM projects WHERE project_id=?", (project_id,)).fetchone()
        phases = _loads(row["phases_json"]) if row else []
        names = [phase.get("name") or phase.get("phase_name") for phase in phases if phase.get("name") or phase.get("phase_name")]
        task_rows = self.conn.execute(
            "SELECT DISTINCT phase_name FROM project_tasks WHERE project_id=? AND phase_name IS NOT NULL",
            (project_id,),
        ).fetchall()
        for task_row in task_rows:
            if task_row["phase_name"] not in names:
                names.append(task_row["phase_name"])
        return names or ["Unphased"]

    def _create_feedback_followup_task(self, project_id: str, packet_id: str, rollup_id: str) -> str | None:
        feedback = self.conn.execute(
            """
            SELECT f.feedback_id, f.task_id, f.artifact_receipt_id, f.source_type,
                   f.customer_ref, f.summary, f.sentiment, f.evidence_refs_json,
                   f.created_at, t.task_type AS source_task_type
            FROM project_customer_feedback f
            LEFT JOIN project_tasks t ON t.task_id = f.task_id
            WHERE f.project_id=?
              AND f.action_required=1
              AND f.status IN ('needs_followup', 'accepted')
              AND (t.task_type='validate' OR f.task_id IS NULL)
            ORDER BY f.created_at DESC, f.feedback_id DESC
            LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        if feedback is None:
            return None
        task_key = f"commercial-feedback-followup:{project_id}:{feedback['feedback_id']}"
        existing = self.conn.execute(
            "SELECT task_id FROM project_tasks WHERE project_id=? AND idempotency_key=?",
            (project_id, task_key),
        ).fetchone()
        if existing is not None:
            return existing["task_id"]

        summary = feedback["summary"].strip()
        lower_summary = summary.lower()
        if any(term in lower_summary for term in ("build", "change", "revise", "iteration", "follow-up", "follow up")):
            task_type = "build"
        elif any(term in lower_summary for term in ("ship", "publish", "deploy", "send to customer")):
            task_type = "ship"
        else:
            task_type = "build"
        phase_name = "Ship" if task_type == "ship" else "Build"
        authority = "operator_gate" if task_type == "ship" else "single_agent"
        required_capabilities = [
            {
                "capability_type": "side_effect" if task_type == "ship" else "file",
                "actions": ["prepare"] if task_type == "ship" else ["read", "write"],
                "scope": "project_delivery" if task_type == "ship" else "project_workspace",
                "grant_required_before_run": True,
            }
        ]
        evidence_refs = [f"kernel:project_customer_feedback/{feedback['feedback_id']}", f"kernel:project_status_rollups/{rollup_id}"]
        evidence_refs.extend(_loads(feedback["evidence_refs_json"]))
        task = ProjectTask(
            project_id=project_id,
            phase_name=phase_name,
            task_type=task_type,  # type: ignore[arg-type]
            autonomy_class="A2",
            objective=f"Address accepted validation feedback: {summary}",
            inputs={
                "close_decision_packet_id": packet_id,
                "rollup_id": rollup_id,
                "feedback_id": feedback["feedback_id"],
                "source_task_id": feedback["task_id"],
                "artifact_receipt_id": feedback["artifact_receipt_id"],
                "source_type": feedback["source_type"],
                "customer_ref": feedback["customer_ref"],
                "sentiment": feedback["sentiment"],
                "summary": summary,
            },
            expected_output_schema={
                "type": "object",
                "required": ["artifact_ref", "change_summary", "operator_load_actual", "next_recommendation"],
            },
            risk_level="medium",
            required_capabilities=required_capabilities,
            model_requirement={
                "task_class": "coding_small_patch",
                "local_allowed_only_if_promoted": True,
                "frontier_fallback_allowed_with_budget": False,
            },
            authority_required=authority,  # type: ignore[arg-type]
            recovery_policy="ask_operator",
            idempotency_key=task_key,
            evidence_refs=evidence_refs,
        )
        return self.create_project_task(task)

    def _create_post_ship_operate_followup_task(self, project_id: str, packet_id: str, rollup_id: str) -> str | None:
        feedback = self.conn.execute(
            """
            SELECT f.feedback_id, f.task_id, f.artifact_receipt_id, f.source_type,
                   f.customer_ref, f.summary, f.sentiment, f.evidence_refs_json,
                   f.created_at
            FROM project_customer_feedback f
            JOIN project_artifact_receipts a ON a.receipt_id = f.artifact_receipt_id
            WHERE f.project_id=?
              AND f.action_required=1
              AND f.status='accepted'
              AND a.artifact_kind='shipped_artifact'
              AND a.customer_visible=1
              AND a.side_effect_receipt_id IS NOT NULL
              AND a.status='accepted'
            ORDER BY f.created_at DESC, f.feedback_id DESC
            LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        if feedback is None:
            return None
        task_key = f"commercial-operate-followup:{project_id}:{feedback['feedback_id']}"
        existing = self.conn.execute(
            "SELECT task_id FROM project_tasks WHERE project_id=? AND idempotency_key=?",
            (project_id, task_key),
        ).fetchone()
        if existing is not None:
            return existing["task_id"]

        followup_type = _operate_followup_type(feedback["summary"])
        capability_scope_by_type = {
            "revenue_reconciliation": "project_revenue_reconciliation",
            "retention": "project_retention_analysis",
            "maintenance": "project_maintenance_triage",
            "customer_support": "project_customer_support_draft",
        }
        load_type_by_followup = {
            "revenue_reconciliation": "reconciliation",
            "retention": "client_sales",
            "maintenance": "maintenance",
            "customer_support": "other",
        }
        summary = feedback["summary"].strip()
        evidence_refs = [
            f"kernel:project_customer_feedback/{feedback['feedback_id']}",
            f"kernel:project_artifact_receipts/{feedback['artifact_receipt_id']}",
            f"kernel:project_status_rollups/{rollup_id}",
        ]
        evidence_refs.extend(_loads(feedback["evidence_refs_json"]))
        task = ProjectTask(
            project_id=project_id,
            phase_name="Operate",
            task_type="operate",
            autonomy_class="A1",
            objective=f"Prepare governed Operate follow-up for accepted post-ship feedback: {summary}",
            inputs={
                "close_decision_packet_id": packet_id,
                "rollup_id": rollup_id,
                "feedback_id": feedback["feedback_id"],
                "source_task_id": feedback["task_id"],
                "artifact_receipt_id": feedback["artifact_receipt_id"],
                "source_type": feedback["source_type"],
                "customer_ref": feedback["customer_ref"],
                "sentiment": feedback["sentiment"],
                "summary": summary,
                "operate_followup_type": followup_type,
                "external_commitment_policy": "draft_or_internal_only_without_side_effect_receipt",
                "default_operator_load_type": load_type_by_followup[followup_type],
            },
            expected_output_schema={
                "type": "object",
                "required": [
                    "operate_followup_type",
                    "internal_result_ref",
                    "evidence_refs",
                    "operator_load_actual",
                    "external_commitment_change",
                    "side_effect_receipt_id",
                ],
                "properties": {
                    "external_commitment_change": {"const": False},
                    "side_effect_receipt_id": {"type": ["string", "null"]},
                },
            },
            risk_level="low",
            required_capabilities=[
                {
                    "capability_type": "memory_write",
                    "actions": ["record"],
                    "scope": capability_scope_by_type[followup_type],
                    "grant_required_before_run": True,
                    "external_side_effects": "blocked_without_operator_gate_and_receipt",
                }
            ],
            model_requirement={
                "task_class": "quick_research_summarization",
                "local_allowed_only_if_promoted": True,
                "frontier_fallback_allowed_with_budget": False,
            },
            authority_required="rule",
            recovery_policy="ask_operator",
            idempotency_key=task_key,
            evidence_refs=evidence_refs,
        )
        return self.create_project_task(task)

    def _create_commitment_receipt_followup_task(
        self,
        commitment: sqlite3.Row,
        receipt: ProjectCustomerCommitmentReceipt,
        *,
        customer_ref: str,
        evidence_refs: list[str],
    ) -> str:
        task_key = f"commercial-commitment-receipt-followup:{receipt.project_id}:{receipt.receipt_id}"
        existing = self.conn.execute(
            "SELECT task_id FROM project_tasks WHERE project_id=? AND idempotency_key=?",
            (receipt.project_id, task_key),
        ).fetchone()
        if existing is not None:
            return existing["task_id"]
        followup_type = _commitment_receipt_followup_type(receipt.receipt_type, receipt.summary)
        load_type_by_followup = {
            "revenue_reconciliation": "reconciliation",
            "retention": "client_sales",
            "maintenance": "maintenance",
            "customer_support": "other",
        }
        capability_scope_by_type = {
            "revenue_reconciliation": "project_revenue_reconciliation",
            "retention": "project_retention_analysis",
            "maintenance": "project_maintenance_triage",
            "customer_support": "project_customer_support_draft",
        }
        task = ProjectTask(
            project_id=receipt.project_id,
            phase_name="Operate",
            task_type="operate",
            autonomy_class="A1",
            objective=f"Prepare governed Operate follow-up for customer commitment receipt: {receipt.summary.strip()}",
            inputs={
                "commitment_id": receipt.commitment_id,
                "customer_commitment_receipt_id": receipt.receipt_id,
                "source_outcome_id": commitment["outcome_id"],
                "customer_ref": customer_ref,
                "receipt_type": receipt.receipt_type,
                "source_type": receipt.source_type,
                "summary": receipt.summary,
                "operate_followup_type": followup_type,
                "external_commitment_policy": "draft_or_internal_only_without_side_effect_receipt",
                "default_operator_load_type": load_type_by_followup[followup_type],
            },
            expected_output_schema={
                "type": "object",
                "required": [
                    "operate_followup_type",
                    "internal_result_ref",
                    "evidence_refs",
                    "operator_load_actual",
                    "external_commitment_change",
                    "side_effect_receipt_id",
                ],
                "properties": {
                    "operate_followup_type": {"const": followup_type},
                    "external_commitment_change": {"const": False},
                    "side_effect_receipt_id": {"type": ["string", "null"]},
                },
            },
            risk_level="low",
            required_capabilities=[
                {
                    "capability_type": "memory_write",
                    "actions": ["record"],
                    "scope": capability_scope_by_type[followup_type],
                    "grant_required_before_run": True,
                    "external_side_effects": "blocked_without_operator_gate_and_receipt",
                }
            ],
            model_requirement={
                "task_class": "quick_research_summarization",
                "local_allowed_only_if_promoted": True,
                "frontier_fallback_allowed_with_budget": False,
            },
            authority_required="rule",
            recovery_policy="ask_operator",
            idempotency_key=task_key,
            evidence_refs=_merge_refs(
                evidence_refs,
                [
                    f"kernel:project_customer_commitment_receipts/{receipt.receipt_id}",
                    f"kernel:project_outcomes/{commitment['outcome_id']}",
                ],
            ),
        )
        return self.create_project_task(task)

    def _create_ship_task_from_build_delivery(
        self,
        *,
        project_id: str,
        build_task_id: str,
        build_artifact_receipt_id: str,
        artifact_ref: str,
        summary: str,
        source_evidence_refs: list[str],
    ) -> str:
        task_key = f"commercial-build-ship:{project_id}:{build_artifact_receipt_id}"
        existing = self.conn.execute(
            "SELECT task_id FROM project_tasks WHERE project_id=? AND idempotency_key=?",
            (project_id, task_key),
        ).fetchone()
        if existing is not None:
            return existing["task_id"]
        evidence_refs = [
            f"kernel:project_tasks/{build_task_id}",
            f"kernel:project_artifact_receipts/{build_artifact_receipt_id}",
        ]
        evidence_refs.extend(source_evidence_refs)
        task = ProjectTask(
            project_id=project_id,
            phase_name="Ship",
            task_type="ship",
            autonomy_class="A2",
            objective=f"Prepare operator-gated delivery for build artifact: {summary}",
            inputs={
                "build_task_id": build_task_id,
                "build_artifact_receipt_id": build_artifact_receipt_id,
                "artifact_ref": artifact_ref,
                "summary": summary,
            },
            expected_output_schema={
                "type": "object",
                "required": ["side_effect_receipt_id", "artifact_ref", "delivery_channel", "operator_load_actual"],
            },
            risk_level="medium",
            required_capabilities=[
                {
                    "capability_type": "side_effect",
                    "actions": ["prepare"],
                    "scope": "project_delivery",
                    "grant_required_before_run": True,
                }
            ],
            model_requirement={
                "task_class": "coding_small_patch",
                "local_allowed_only_if_promoted": True,
                "frontier_fallback_allowed_with_budget": False,
            },
            authority_required="operator_gate",
            recovery_policy="ask_operator",
            idempotency_key=task_key,
            evidence_refs=evidence_refs,
        )
        return self.create_project_task(task)

    def _derive_project_phase_rollup(self, project_id: str, phase_name: str) -> ProjectPhaseRollup:
        task_counts = self._project_task_counts(project_id, phase_name=phase_name)
        outcome_counts = self._project_outcome_counts(project_id, phase_name=phase_name)
        artifact_count = self._count_project_rows("project_artifact_receipts", project_id, phase_name=phase_name)
        feedback_count = self._count_project_rows("project_customer_feedback", project_id, phase_name=phase_name)
        revenue_total = self._project_revenue_total(project_id, phase_name=phase_name)
        load_minutes = self._project_operator_load_minutes(project_id, phase_name=phase_name)
        last_activity_at = self._project_phase_last_activity(project_id, phase_name)
        if not sum(task_counts.values()):
            status = "not_started"
        elif task_counts.get("failed", 0):
            status = "failed"
        elif task_counts.get("blocked", 0):
            status = "blocked"
        elif task_counts.get("running", 0) or task_counts.get("queued", 0):
            status = "active"
        elif task_counts.get("completed", 0):
            status = "complete"
        else:
            status = "at_risk"
        return ProjectPhaseRollup(
            phase_name=phase_name,
            task_counts=task_counts,
            outcome_counts=outcome_counts,
            artifact_count=artifact_count,
            customer_feedback_count=feedback_count,
            revenue_attributed_usd=revenue_total,
            operator_load_minutes=load_minutes,
            status=status,  # type: ignore[arg-type]
            last_activity_at=last_activity_at,
        )

    def _project_task_counts(self, project_id: str, *, phase_name: str | None = None) -> dict[str, int]:
        params: list[Any] = [project_id]
        clause = "project_id=?"
        if phase_name is not None:
            clause += " AND COALESCE(phase_name, 'Unphased')=?"
            params.append(phase_name)
        rows = self.conn.execute(
            f"SELECT status, COUNT(*) AS count FROM project_tasks WHERE {clause} GROUP BY status",
            params,
        ).fetchall()
        return {row["status"]: int(row["count"]) for row in rows}

    def _project_outcome_counts(self, project_id: str, *, phase_name: str | None = None) -> dict[str, int]:
        params: list[Any] = [project_id]
        clause = "project_id=?"
        if phase_name is not None:
            clause += " AND COALESCE(phase_name, 'Unphased')=?"
            params.append(phase_name)
        rows = self.conn.execute(
            f"SELECT outcome_type, COUNT(*) AS count FROM project_outcomes WHERE {clause} GROUP BY outcome_type",
            params,
        ).fetchall()
        return {row["outcome_type"]: int(row["count"]) for row in rows}

    def _count_project_rows(self, table: str, project_id: str, *, phase_name: str | None = None) -> int:
        allowed = {"project_artifact_receipts", "project_customer_feedback"}
        if table not in allowed:
            raise ValueError("unsupported project count table")
        if phase_name is None:
            return int(self.conn.execute(f"SELECT COUNT(*) AS count FROM {table} WHERE project_id=?", (project_id,)).fetchone()["count"])
        return int(
            self.conn.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM {table} r
                LEFT JOIN project_tasks t ON t.task_id = r.task_id
                WHERE r.project_id=? AND COALESCE(t.phase_name, 'Unphased')=?
                """,
                (project_id, phase_name),
            ).fetchone()["count"]
        )

    def _project_revenue_total(self, project_id: str, *, phase_name: str | None = None) -> Decimal:
        if phase_name is None:
            rows = self.conn.execute(
                "SELECT amount_usd FROM project_revenue_attributions WHERE project_id=?",
                (project_id,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT r.amount_usd
                FROM project_revenue_attributions r
                LEFT JOIN project_tasks t ON t.task_id = r.task_id
                WHERE r.project_id=? AND COALESCE(t.phase_name, 'Unphased')=?
                """,
                (project_id, phase_name),
            ).fetchall()
        return sum((Decimal(row["amount_usd"]) for row in rows), Decimal("0"))

    def _project_operator_load_minutes(self, project_id: str, *, phase_name: str | None = None) -> int:
        if phase_name is None:
            row = self.conn.execute(
                "SELECT COALESCE(SUM(minutes), 0) AS minutes FROM project_operator_load WHERE project_id=?",
                (project_id,),
            ).fetchone()
        else:
            row = self.conn.execute(
                """
                SELECT COALESCE(SUM(l.minutes), 0) AS minutes
                FROM project_operator_load l
                LEFT JOIN project_tasks t ON t.task_id = l.task_id
                WHERE l.project_id=? AND COALESCE(t.phase_name, 'Unphased')=?
                """,
                (project_id, phase_name),
            ).fetchone()
        return int(row["minutes"])

    def _project_post_ship_evidence_summary(self, project_id: str) -> dict[str, Any]:
        shipped = self.conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM project_artifact_receipts
            WHERE project_id=? AND artifact_kind='shipped_artifact' AND customer_visible=1
            """,
            (project_id,),
        ).fetchone()
        feedback = self.conn.execute(
            """
            SELECT
              COUNT(*) AS count,
              COALESCE(SUM(CASE WHEN sentiment IN ('negative', 'mixed') THEN 1 ELSE 0 END), 0) AS negative_count,
              COALESCE(SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END), 0) AS positive_count,
              COALESCE(SUM(CASE WHEN action_required = 1 AND status = 'accepted' THEN 1 ELSE 0 END), 0) AS action_count,
              COALESCE(SUM(CASE WHEN action_required = 1 OR status = 'needs_followup' THEN 1 ELSE 0 END), 0) AS open_followup_count
            FROM project_customer_feedback
            WHERE project_id=? AND artifact_receipt_id IN (
              SELECT receipt_id
              FROM project_artifact_receipts
              WHERE project_id=? AND artifact_kind='shipped_artifact' AND customer_visible=1
            )
            """,
            (project_id, project_id),
        ).fetchone()
        revenue_rows = self.conn.execute(
            """
            SELECT amount_usd
            FROM project_revenue_attributions
            WHERE project_id=? AND artifact_receipt_id IN (
              SELECT receipt_id
              FROM project_artifact_receipts
              WHERE project_id=? AND artifact_kind='shipped_artifact' AND customer_visible=1
            )
            """,
            (project_id, project_id),
        ).fetchall()
        load = self.conn.execute(
            """
            SELECT COALESCE(SUM(minutes), 0) AS minutes
            FROM project_operator_load
            WHERE project_id=? AND artifact_receipt_id IN (
              SELECT receipt_id
              FROM project_artifact_receipts
              WHERE project_id=? AND artifact_kind='shipped_artifact' AND customer_visible=1
            )
            """,
            (project_id, project_id),
        ).fetchone()
        return {
            "shipped_artifact_count": int(shipped["count"]),
            "feedback_count": int(feedback["count"]),
            "positive_feedback_count": int(feedback["positive_count"]),
            "negative_feedback_count": int(feedback["negative_count"]),
            "action_required_count": int(feedback["action_count"]),
            "open_followup_count": int(feedback["open_followup_count"]),
            "revenue_attributed_usd": sum((Decimal(row["amount_usd"]) for row in revenue_rows), Decimal("0")),
            "operator_load_minutes": int(load["minutes"]),
        }

    def _project_commitment_receipt_summary(self, project_id: str) -> dict[str, Any]:
        row = self.conn.execute(
            """
            SELECT
              COUNT(*) AS count,
              COALESCE(SUM(CASE WHEN action_required = 1 OR status = 'needs_followup' THEN 1 ELSE 0 END), 0) AS open_followup_count,
              COALESCE(SUM(CASE WHEN receipt_type = 'delivery_failure' THEN 1 ELSE 0 END), 0) AS delivery_failure_count,
              COALESCE(SUM(CASE WHEN receipt_type = 'timeout' THEN 1 ELSE 0 END), 0) AS timeout_count,
              COALESCE(SUM(CASE WHEN receipt_type = 'compensation_needed' THEN 1 ELSE 0 END), 0) AS compensation_needed_count
            FROM project_customer_commitment_receipts
            WHERE project_id=?
            """,
            (project_id,),
        ).fetchone()
        return {
            "count": int(row["count"]),
            "open_followup_count": int(row["open_followup_count"]),
            "delivery_failure_count": int(row["delivery_failure_count"]),
            "timeout_count": int(row["timeout_count"]),
            "compensation_needed_count": int(row["compensation_needed_count"]),
        }

    def _project_phase_last_activity(self, project_id: str, phase_name: str) -> str | None:
        rows = self.conn.execute(
            """
            SELECT MAX(created_at) AS last_activity_at FROM (
              SELECT created_at FROM project_tasks WHERE project_id=? AND COALESCE(phase_name, 'Unphased')=?
              UNION ALL
              SELECT created_at FROM project_outcomes WHERE project_id=? AND COALESCE(phase_name, 'Unphased')=?
            )
            """,
            (project_id, phase_name, project_id, phase_name),
        ).fetchone()
        return rows["last_activity_at"] if rows else None

    def _require_project(self, project_id: str) -> sqlite3.Row:
        project = self.conn.execute("SELECT project_id, status FROM projects WHERE project_id=?", (project_id,)).fetchone()
        if project is None:
            raise ValueError("project record requires an existing project")
        return project

    def _require_project_task(self, project_id: str, task_id: str) -> sqlite3.Row:
        task = self.conn.execute(
            "SELECT task_id, project_id FROM project_tasks WHERE task_id=?",
            (task_id,),
        ).fetchone()
        if task is None:
            raise ValueError("project record references unknown task")
        if task["project_id"] != project_id:
            raise ValueError("project record task/project mismatch")
        return task

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

    def prepare_side_effect(self, intent: SideEffectIntent) -> str:
        if not self.use_grant(intent.grant_id, "adapter", "side_effect_broker", "side_effect", "prepare"):
            raise PermissionError("side-effect grant denied")
        payload = {
            "intent_id": intent.intent_id,
            "task_id": intent.task_id,
            "side_effect_type": intent.side_effect_type,
            "target": intent.target,
            "payload_hash": intent.payload_hash,
            "required_authority": intent.required_authority,
            "grant_id": intent.grant_id,
            "timeout_policy": intent.timeout_policy,
            "status": intent.status,
        }
        event_id = self.append_event("side_effect_intent_prepared", "side_effect", intent.intent_id, payload)
        self.conn.execute(
            """
            INSERT INTO side_effect_intents (
              intent_id, task_id, side_effect_type, target_json, payload_hash,
              required_authority, grant_id, timeout_policy, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                intent.intent_id,
                intent.task_id,
                intent.side_effect_type,
                canonical_json(intent.target),
                intent.payload_hash,
                intent.required_authority,
                intent.grant_id,
                intent.timeout_policy,
                intent.status,
            ),
        )
        self.enqueue_projection(event_id, "side_effect_projection")
        return intent.intent_id

    def record_side_effect_receipt(self, receipt: SideEffectReceipt) -> str:
        row = self.conn.execute(
            "SELECT status FROM side_effect_intents WHERE intent_id=?",
            (receipt.intent_id,),
        ).fetchone()
        if row is None:
            raise ValueError("side-effect intent not found")
        payload = {
            "receipt_id": receipt.receipt_id,
            "intent_id": receipt.intent_id,
            "receipt_type": receipt.receipt_type,
            "receipt_hash": receipt.receipt_hash,
            "details": receipt.details,
            "recorded_at": receipt.recorded_at,
        }
        event_id = self.append_event("side_effect_receipt_recorded", "side_effect", receipt.intent_id, payload)
        self.conn.execute(
            """
            INSERT INTO side_effect_receipts (
              receipt_id, intent_id, receipt_type, receipt_hash, details_json, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                receipt.receipt_id,
                receipt.intent_id,
                receipt.receipt_type,
                receipt.receipt_hash,
                canonical_json(receipt.details),
                receipt.recorded_at,
            ),
        )
        next_status = {
            "success": "executed",
            "failure": "failed",
            "timeout": "compensation_needed",
            "cancellation": "cancelled",
            "compensation_needed": "compensation_needed",
        }[receipt.receipt_type]
        self.conn.execute(
            "UPDATE side_effect_intents SET status=? WHERE intent_id=?",
            (next_status, receipt.intent_id),
        )
        self.enqueue_projection(event_id, "side_effect_projection")
        return receipt.receipt_id


def _loads(value: str) -> Any:
    import json

    return json.loads(value)


def payload_hash_for(value: dict[str, Any]) -> str:
    return payload_hash(value)


def _source_payload(source: Any) -> dict[str, Any]:
    return {
        "source_id": source.source_id,
        "url_or_ref": source.url_or_ref,
        "source_type": source.source_type,
        "retrieved_at": source.retrieved_at,
        "source_date": source.source_date,
        "relevance": source.relevance,
        "reliability": source.reliability,
        "license_or_tos_notes": source.license_or_tos_notes,
        "content_hash": source.content_hash,
        "artifact_ref": source.artifact_ref,
        "access_method": source.access_method,
        "data_class": source.data_class,
    }


def _source_plan_payload(plan: Any) -> dict[str, Any]:
    return {
        "source_plan_id": plan.source_plan_id,
        "request_id": plan.request_id,
        "profile": plan.profile,
        "depth": plan.depth,
        "planned_sources": plan.planned_sources,
        "retrieval_strategy": plan.retrieval_strategy,
        "created_by": plan.created_by,
        "status": plan.status,
        "created_at": plan.created_at,
    }


def _source_acquisition_check_payload(check: Any) -> dict[str, Any]:
    return {
        "check_id": check.check_id,
        "request_id": check.request_id,
        "source_plan_id": check.source_plan_id,
        "source_ref": check.source_ref,
        "access_method": check.access_method,
        "data_class": check.data_class,
        "source_type": check.source_type,
        "result": check.result,
        "reason": check.reason,
        "grant_id": check.grant_id,
        "checked_at": check.checked_at,
    }


def _claim_payload(claim: Any) -> dict[str, Any]:
    return {
        "claim_id": claim.claim_id,
        "text": claim.text,
        "claim_type": claim.claim_type,
        "source_ids": claim.source_ids,
        "confidence": claim.confidence,
        "freshness": claim.freshness,
        "importance": claim.importance,
    }


def _decision_payload(decision: Any) -> dict[str, Any]:
    return {
        "decision_id": decision.decision_id,
        "decision_type": decision.decision_type,
        "question": decision.question,
        "options": decision.options,
        "stakes": decision.stakes,
        "evidence_bundle_ids": decision.evidence_bundle_ids,
        "evidence_refs": decision.evidence_refs,
        "requested_by": decision.requested_by,
        "required_authority": decision.required_authority,
        "authority_policy_version": decision.authority_policy_version,
        "deadline": decision.deadline,
        "status": decision.status,
        "recommendation": decision.recommendation,
        "verdict": decision.verdict,
        "confidence": decision.confidence,
        "decisive_factors": decision.decisive_factors,
        "decisive_uncertainty": decision.decisive_uncertainty,
        "risk_flags": decision.risk_flags,
        "default_on_timeout": decision.default_on_timeout,
        "gate_packet": decision.gate_packet,
        "created_at": decision.created_at,
        "decided_at": decision.decided_at,
    }


def _commercial_decision_packet_payload(packet: Any) -> dict[str, Any]:
    return {
        "packet_id": packet.packet_id,
        "decision_id": packet.decision_id,
        "request_id": packet.request_id,
        "evidence_bundle_id": packet.evidence_bundle_id,
        "decision_target": packet.decision_target,
        "question": packet.question,
        "recommendation": packet.recommendation,
        "required_authority": packet.required_authority,
        "opportunity": packet.opportunity,
        "project": packet.project,
        "gate_packet": packet.gate_packet,
        "evidence_used": packet.evidence_used,
        "risk_flags": packet.risk_flags,
        "default_on_timeout": packet.default_on_timeout,
        "status": packet.status,
        "created_at": packet.created_at,
    }


def _commercial_decision_recommendation_payload(recommendation: Any) -> dict[str, Any]:
    return {
        "record_id": recommendation.record_id,
        "packet_id": recommendation.packet_id,
        "decision_id": recommendation.decision_id,
        "request_id": recommendation.request_id,
        "evidence_bundle_id": recommendation.evidence_bundle_id,
        "recommendation_authority": recommendation.recommendation_authority,
        "recommendation": recommendation.recommendation,
        "confidence": recommendation.confidence,
        "decisive_factors": recommendation.decisive_factors,
        "decisive_uncertainty": recommendation.decisive_uncertainty,
        "evidence_used": recommendation.evidence_used,
        "evidence_refs": recommendation.evidence_refs,
        "quality_gate_context": recommendation.quality_gate_context,
        "risk_flags": recommendation.risk_flags,
        "operator_gate_defaults": recommendation.operator_gate_defaults,
        "rationale": recommendation.rationale,
        "model_routes_used": recommendation.model_routes_used,
        "degraded": recommendation.degraded,
        "created_at": recommendation.created_at,
    }


def _project_payload(project: Any) -> dict[str, Any]:
    return {
        "project_id": project.project_id,
        "opportunity_id": project.opportunity_id,
        "decision_packet_id": project.decision_packet_id,
        "decision_id": project.decision_id,
        "name": project.name,
        "objective": project.objective,
        "revenue_mechanism": project.revenue_mechanism,
        "operator_role": project.operator_role,
        "external_commitment_policy": project.external_commitment_policy,
        "budget_id": project.budget_id,
        "phases": project.phases,
        "success_metrics": project.success_metrics,
        "kill_criteria": project.kill_criteria,
        "evidence_refs": project.evidence_refs,
        "status": project.status,
        "created_at": project.created_at,
        "updated_at": project.updated_at,
    }


def _project_task_payload(
    task: Any,
    *,
    command_id: str,
    policy_version: str,
    idempotency_key: str,
) -> dict[str, Any]:
    return {
        "task_id": task.task_id,
        "project_id": task.project_id,
        "phase_name": task.phase_name,
        "task_type": task.task_type,
        "autonomy_class": task.autonomy_class,
        "objective": task.objective,
        "inputs": task.inputs,
        "expected_output_schema": task.expected_output_schema,
        "risk_level": task.risk_level,
        "required_capabilities": task.required_capabilities,
        "model_requirement": task.model_requirement,
        "budget_id": task.budget_id,
        "deadline": task.deadline,
        "status": task.status,
        "authority_required": task.authority_required,
        "recovery_policy": task.recovery_policy,
        "command_id": command_id,
        "policy_version": policy_version,
        "idempotency_key": idempotency_key,
        "evidence_refs": task.evidence_refs,
        "created_at": task.created_at,
        "updated_at": task.updated_at,
    }


def _project_task_assignment_payload(assignment: Any) -> dict[str, Any]:
    return {
        "assignment_id": assignment.assignment_id,
        "task_id": assignment.task_id,
        "project_id": assignment.project_id,
        "worker_type": assignment.worker_type,
        "worker_id": assignment.worker_id,
        "route_decision_id": assignment.route_decision_id,
        "grant_ids": assignment.grant_ids,
        "accepted_capabilities": assignment.accepted_capabilities,
        "status": assignment.status,
        "notes": assignment.notes,
        "assigned_at": assignment.assigned_at,
    }


def _project_outcome_payload(outcome: Any) -> dict[str, Any]:
    return {
        "outcome_id": outcome.outcome_id,
        "project_id": outcome.project_id,
        "task_id": outcome.task_id,
        "phase_name": outcome.phase_name,
        "outcome_type": outcome.outcome_type,
        "summary": outcome.summary,
        "artifact_refs": outcome.artifact_refs,
        "metrics": outcome.metrics,
        "feedback": outcome.feedback,
        "revenue_impact": outcome.revenue_impact,
        "operator_load_actual": outcome.operator_load_actual,
        "side_effect_intent_id": outcome.side_effect_intent_id,
        "side_effect_receipt_id": outcome.side_effect_receipt_id,
        "status": outcome.status,
        "created_at": outcome.created_at,
    }


def _project_artifact_receipt_payload(receipt: Any) -> dict[str, Any]:
    return {
        "receipt_id": receipt.receipt_id,
        "project_id": receipt.project_id,
        "task_id": receipt.task_id,
        "artifact_ref": receipt.artifact_ref,
        "artifact_kind": receipt.artifact_kind,
        "summary": receipt.summary,
        "data_class": receipt.data_class,
        "delivery_channel": receipt.delivery_channel,
        "side_effect_intent_id": receipt.side_effect_intent_id,
        "side_effect_receipt_id": receipt.side_effect_receipt_id,
        "customer_visible": receipt.customer_visible,
        "status": receipt.status,
        "created_at": receipt.created_at,
    }


def _project_customer_feedback_payload(feedback: Any) -> dict[str, Any]:
    return {
        "feedback_id": feedback.feedback_id,
        "project_id": feedback.project_id,
        "task_id": feedback.task_id,
        "artifact_receipt_id": feedback.artifact_receipt_id,
        "source_type": feedback.source_type,
        "customer_ref": feedback.customer_ref,
        "summary": feedback.summary,
        "sentiment": feedback.sentiment,
        "evidence_refs": feedback.evidence_refs,
        "action_required": feedback.action_required,
        "operator_review_required": feedback.operator_review_required,
        "status": feedback.status,
        "created_at": feedback.created_at,
    }


def _project_revenue_attribution_payload(
    attribution: Any,
    *,
    reconciliation_task_id: str | None,
) -> dict[str, Any]:
    return {
        "attribution_id": attribution.attribution_id,
        "project_id": attribution.project_id,
        "task_id": attribution.task_id,
        "outcome_id": attribution.outcome_id,
        "artifact_receipt_id": attribution.artifact_receipt_id,
        "amount_usd": str(attribution.amount_usd),
        "source": attribution.source,
        "attribution_period": attribution.attribution_period,
        "external_ref": attribution.external_ref,
        "evidence_refs": attribution.evidence_refs,
        "confidence": attribution.confidence,
        "reconciliation_task_id": reconciliation_task_id,
        "status": attribution.status,
        "created_at": attribution.created_at,
    }


def _project_operator_load_payload(load: Any) -> dict[str, Any]:
    return {
        "load_id": load.load_id,
        "project_id": load.project_id,
        "task_id": load.task_id,
        "outcome_id": load.outcome_id,
        "artifact_receipt_id": load.artifact_receipt_id,
        "minutes": load.minutes,
        "load_type": load.load_type,
        "source": load.source,
        "notes": load.notes,
        "created_at": load.created_at,
    }


def _operate_followup_type(summary: str) -> str:
    text = summary.lower()
    if any(term in text for term in ("reconcile", "invoice", "payment", "billing", "stripe", "revenue", "receipt", "paid", "charge")):
        return "revenue_reconciliation"
    if any(term in text for term in ("renew", "renewal", "retain", "retention", "churn", "expand", "upsell", "adoption", "continue using")):
        return "retention"
    if any(term in text for term in ("bug", "broken", "fix", "maintenance", "latency", "slow", "error", "regression", "outage")):
        return "maintenance"
    return "customer_support"


def _commitment_receipt_followup_type(receipt_type: str, summary: str) -> str:
    if receipt_type == "delivery_failure":
        return "maintenance"
    if receipt_type in {"timeout", "compensation_needed"}:
        return "customer_support"
    return _operate_followup_type(summary)


def _project_phase_rollup_payload(phase: ProjectPhaseRollup) -> dict[str, Any]:
    return {
        "phase_name": phase.phase_name,
        "task_counts": phase.task_counts,
        "outcome_counts": phase.outcome_counts,
        "artifact_count": phase.artifact_count,
        "customer_feedback_count": phase.customer_feedback_count,
        "revenue_attributed_usd": str(phase.revenue_attributed_usd),
        "operator_load_minutes": phase.operator_load_minutes,
        "status": phase.status,
        "last_activity_at": phase.last_activity_at,
    }


def _project_commercial_rollup_payload(rollup: ProjectCommercialRollup) -> dict[str, Any]:
    return {
        "rollup_id": rollup.rollup_id,
        "project_id": rollup.project_id,
        "revenue_reconciled_usd": str(rollup.revenue_reconciled_usd),
        "revenue_unreconciled_usd": str(rollup.revenue_unreconciled_usd),
        "retained_customer_count": rollup.retained_customer_count,
        "at_risk_customer_count": rollup.at_risk_customer_count,
        "churned_customer_count": rollup.churned_customer_count,
        "support_resolved_count": rollup.support_resolved_count,
        "support_open_count": rollup.support_open_count,
        "maintenance_resolved_count": rollup.maintenance_resolved_count,
        "maintenance_open_count": rollup.maintenance_open_count,
        "external_commitment_count": rollup.external_commitment_count,
        "receiptless_side_effect_count": rollup.receiptless_side_effect_count,
        "evidence_refs": rollup.evidence_refs,
        "risk_flags": rollup.risk_flags,
        "created_at": rollup.created_at,
    }


def _project_status_rollup_payload(rollup: ProjectStatusRollup) -> dict[str, Any]:
    return {
        "rollup_id": rollup.rollup_id,
        "project_id": rollup.project_id,
        "project_status": rollup.project_status,
        "phase_rollups": [_project_phase_rollup_payload(phase) for phase in rollup.phase_rollups],
        "task_counts": rollup.task_counts,
        "outcome_counts": rollup.outcome_counts,
        "artifact_count": rollup.artifact_count,
        "customer_feedback_count": rollup.customer_feedback_count,
        "revenue_attributed_usd": str(rollup.revenue_attributed_usd),
        "operator_load_minutes": rollup.operator_load_minutes,
        "recommended_status": rollup.recommended_status,
        "close_recommendation": rollup.close_recommendation,
        "rationale": rollup.rationale,
        "risk_flags": rollup.risk_flags,
        "commercial_rollup_id": rollup.commercial_rollup_id,
        "commercial_rollup": rollup.commercial_rollup,
        "created_at": rollup.created_at,
    }


def _project_close_decision_packet_payload(packet: ProjectCloseDecisionPacket) -> dict[str, Any]:
    return {
        "packet_id": packet.packet_id,
        "project_id": packet.project_id,
        "decision_id": packet.decision_id,
        "rollup_id": packet.rollup_id,
        "recommendation": packet.recommendation,
        "required_authority": packet.required_authority,
        "rationale": packet.rationale,
        "risk_flags": packet.risk_flags,
        "evidence_refs": packet.evidence_refs,
        "default_on_timeout": packet.default_on_timeout,
        "status": packet.status,
        "created_at": packet.created_at,
    }


def _project_replay_projection_comparison_payload(comparison: ProjectReplayProjectionComparison) -> dict[str, Any]:
    return {
        "comparison_id": comparison.comparison_id,
        "project_id": comparison.project_id,
        "replay_project_status": comparison.replay_project_status,
        "projection_project_status": comparison.projection_project_status,
        "replay_task_counts": comparison.replay_task_counts,
        "projection_task_counts": comparison.projection_task_counts,
        "replay_revenue_attributed_usd": str(comparison.replay_revenue_attributed_usd),
        "projection_revenue_attributed_usd": str(comparison.projection_revenue_attributed_usd),
        "replay_operator_load_minutes": comparison.replay_operator_load_minutes,
        "projection_operator_load_minutes": comparison.projection_operator_load_minutes,
        "replay_commercial_rollup": comparison.replay_commercial_rollup,
        "projection_commercial_rollup": comparison.projection_commercial_rollup,
        "matches": comparison.matches,
        "mismatches": comparison.mismatches,
        "created_at": comparison.created_at,
    }


def _project_portfolio_decision_packet_payload(packet: ProjectPortfolioDecisionPacket) -> dict[str, Any]:
    return {
        "packet_id": packet.packet_id,
        "decision_id": packet.decision_id,
        "scope": packet.scope,
        "project_ids": packet.project_ids,
        "rollup_ids": packet.rollup_ids,
        "recommendation": packet.recommendation,
        "required_authority": packet.required_authority,
        "packet": packet.packet,
        "tradeoffs": packet.tradeoffs,
        "evidence_refs": packet.evidence_refs,
        "risk_flags": packet.risk_flags,
        "default_on_timeout": packet.default_on_timeout,
        "status": packet.status,
        "verdict": packet.verdict,
        "created_at": packet.created_at,
    }


def _project_portfolio_replay_projection_comparison_payload(
    comparison: ProjectPortfolioReplayProjectionComparison,
) -> dict[str, Any]:
    return {
        "comparison_id": comparison.comparison_id,
        "packet_id": comparison.packet_id,
        "replay_packet": comparison.replay_packet,
        "projection_packet": comparison.projection_packet,
        "matches": comparison.matches,
        "mismatches": comparison.mismatches,
        "created_at": comparison.created_at,
    }


def _project_scheduling_intent_payload(intent: ProjectSchedulingIntent) -> dict[str, Any]:
    return {
        "intent_id": intent.intent_id,
        "portfolio_packet_id": intent.portfolio_packet_id,
        "source_decision_id": intent.source_decision_id,
        "scope": intent.scope,
        "project_ids": intent.project_ids,
        "scheduling_window": intent.scheduling_window,
        "intent": intent.intent,
        "queue_adjustments": intent.queue_adjustments,
        "evidence_refs": intent.evidence_refs,
        "risk_flags": intent.risk_flags,
        "required_authority": intent.required_authority,
        "authority_effect": intent.authority_effect,
        "status": intent.status,
        "created_at": intent.created_at,
    }


def _project_scheduling_priority_change_packet_payload(packet: ProjectSchedulingPriorityChangePacket) -> dict[str, Any]:
    return {
        "packet_id": packet.packet_id,
        "intent_id": packet.intent_id,
        "portfolio_packet_id": packet.portfolio_packet_id,
        "source_decision_id": packet.source_decision_id,
        "decision_id": packet.decision_id,
        "scope": packet.scope,
        "project_ids": packet.project_ids,
        "scheduling_window": packet.scheduling_window,
        "proposed_changes": packet.proposed_changes,
        "evidence_refs": packet.evidence_refs,
        "risk_flags": packet.risk_flags,
        "required_authority": packet.required_authority,
        "default_on_timeout": packet.default_on_timeout,
        "status": packet.status,
        "verdict": packet.verdict,
        "applied_changes": packet.applied_changes,
        "created_at": packet.created_at,
        "decided_by": packet.decided_by,
        "decided_at": packet.decided_at,
    }


def _project_scheduling_priority_replay_projection_comparison_payload(
    comparison: ProjectSchedulingPriorityReplayProjectionComparison,
) -> dict[str, Any]:
    return {
        "comparison_id": comparison.comparison_id,
        "packet_id": comparison.packet_id,
        "replay_packet": comparison.replay_packet,
        "projection_packet": comparison.projection_packet,
        "matches": comparison.matches,
        "mismatches": comparison.mismatches,
        "created_at": comparison.created_at,
    }


def _project_scheduling_replay_projection_comparison_payload(
    comparison: ProjectSchedulingReplayProjectionComparison,
) -> dict[str, Any]:
    return {
        "comparison_id": comparison.comparison_id,
        "intent_id": comparison.intent_id,
        "replay_intent": comparison.replay_intent,
        "projection_intent": comparison.projection_intent,
        "matches": comparison.matches,
        "mismatches": comparison.mismatches,
        "created_at": comparison.created_at,
    }


def _project_customer_visible_packet_payload(packet: ProjectCustomerVisiblePacket) -> dict[str, Any]:
    return {
        "packet_id": packet.packet_id,
        "project_id": packet.project_id,
        "outcome_id": packet.outcome_id,
        "decision_id": packet.decision_id,
        "packet_type": packet.packet_type,
        "customer_ref": packet.customer_ref,
        "channel": packet.channel,
        "subject": packet.subject,
        "summary": packet.summary,
        "payload_ref": packet.payload_ref,
        "side_effect_intent_id": packet.side_effect_intent_id,
        "evidence_refs": packet.evidence_refs,
        "risk_flags": packet.risk_flags,
        "required_authority": packet.required_authority,
        "default_on_timeout": packet.default_on_timeout,
        "status": packet.status,
        "verdict": packet.verdict,
        "created_at": packet.created_at,
        "decided_by": packet.decided_by,
        "decided_at": packet.decided_at,
    }


def _project_customer_commitment_payload(commitment: ProjectCustomerCommitment) -> dict[str, Any]:
    return {
        "commitment_id": commitment.commitment_id,
        "packet_id": commitment.packet_id,
        "project_id": commitment.project_id,
        "outcome_id": commitment.outcome_id,
        "side_effect_intent_id": commitment.side_effect_intent_id,
        "side_effect_receipt_id": commitment.side_effect_receipt_id,
        "customer_ref": commitment.customer_ref,
        "channel": commitment.channel,
        "commitment_type": commitment.commitment_type,
        "payload_ref": commitment.payload_ref,
        "summary": commitment.summary,
        "evidence_refs": commitment.evidence_refs,
        "created_at": commitment.created_at,
    }


def _project_customer_commitment_receipt_payload(receipt: ProjectCustomerCommitmentReceipt) -> dict[str, Any]:
    return {
        "receipt_id": receipt.receipt_id,
        "commitment_id": receipt.commitment_id,
        "project_id": receipt.project_id,
        "receipt_type": receipt.receipt_type,
        "source_type": receipt.source_type,
        "customer_ref": receipt.customer_ref,
        "summary": receipt.summary,
        "evidence_refs": receipt.evidence_refs,
        "action_required": receipt.action_required,
        "status": receipt.status,
        "followup_task_id": receipt.followup_task_id,
        "created_at": receipt.created_at,
    }


def _project_customer_visible_replay_projection_comparison_payload(
    comparison: ProjectCustomerVisibleReplayProjectionComparison,
) -> dict[str, Any]:
    return {
        "comparison_id": comparison.comparison_id,
        "packet_id": comparison.packet_id,
        "replay_packet": comparison.replay_packet,
        "projection_packet": comparison.projection_packet,
        "replay_commitments": comparison.replay_commitments,
        "projection_commitments": comparison.projection_commitments,
        "replay_commitment_receipts": comparison.replay_commitment_receipts,
        "projection_commitment_receipts": comparison.projection_commitment_receipts,
        "matches": comparison.matches,
        "mismatches": comparison.mismatches,
        "created_at": comparison.created_at,
    }


def _portfolio_packet_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "packet_id": row["packet_id"],
        "decision_id": row["decision_id"],
        "scope": row["scope"],
        "project_ids": _loads(row["project_ids_json"]),
        "rollup_ids": _loads(row["rollup_ids_json"]),
        "recommendation": row["recommendation"],
        "required_authority": row["required_authority"],
        "packet": _loads(row["packet_json"]),
        "tradeoffs": _loads(row["tradeoffs_json"]),
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "risk_flags": _loads(row["risk_flags_json"]),
        "default_on_timeout": row["default_on_timeout"],
        "status": row["status"],
        "verdict": row["verdict"],
        "created_at": row["created_at"],
    }


def _project_scheduling_intent_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "intent_id": row["intent_id"],
        "portfolio_packet_id": row["portfolio_packet_id"],
        "source_decision_id": row["source_decision_id"],
        "scope": row["scope"],
        "project_ids": _loads(row["project_ids_json"]),
        "scheduling_window": row["scheduling_window"],
        "intent": _loads(row["intent_json"]),
        "queue_adjustments": _loads(row["queue_adjustments_json"]),
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "risk_flags": _loads(row["risk_flags_json"]),
        "required_authority": row["required_authority"],
        "authority_effect": row["authority_effect"],
        "status": row["status"],
        "created_at": row["created_at"],
    }


def _project_scheduling_priority_change_packet_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "packet_id": row["packet_id"],
        "intent_id": row["intent_id"],
        "portfolio_packet_id": row["portfolio_packet_id"],
        "source_decision_id": row["source_decision_id"],
        "decision_id": row["decision_id"],
        "scope": row["scope"],
        "project_ids": _loads(row["project_ids_json"]),
        "scheduling_window": row["scheduling_window"],
        "proposed_changes": _loads(row["proposed_changes_json"]),
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "risk_flags": _loads(row["risk_flags_json"]),
        "required_authority": row["required_authority"],
        "default_on_timeout": row["default_on_timeout"],
        "status": row["status"],
        "verdict": row["verdict"],
        "applied_changes": _loads(row["applied_changes_json"]),
        "created_at": row["created_at"],
        "decided_by": row["decided_by"],
        "decided_at": row["decided_at"],
    }


def _project_customer_visible_packet_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "packet_id": row["packet_id"],
        "project_id": row["project_id"],
        "outcome_id": row["outcome_id"],
        "decision_id": row["decision_id"],
        "packet_type": row["packet_type"],
        "customer_ref": row["customer_ref"],
        "channel": row["channel"],
        "subject": row["subject"],
        "summary": row["summary"],
        "payload_ref": row["payload_ref"],
        "side_effect_intent_id": row["side_effect_intent_id"],
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "risk_flags": _loads(row["risk_flags_json"]),
        "required_authority": row["required_authority"],
        "default_on_timeout": row["default_on_timeout"],
        "status": row["status"],
        "verdict": row["verdict"],
        "created_at": row["created_at"],
        "decided_by": row["decided_by"],
        "decided_at": row["decided_at"],
    }


def _project_customer_commitment_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "commitment_id": row["commitment_id"],
        "packet_id": row["packet_id"],
        "project_id": row["project_id"],
        "outcome_id": row["outcome_id"],
        "side_effect_intent_id": row["side_effect_intent_id"],
        "side_effect_receipt_id": row["side_effect_receipt_id"],
        "customer_ref": row["customer_ref"],
        "channel": row["channel"],
        "commitment_type": row["commitment_type"],
        "payload_ref": row["payload_ref"],
        "summary": row["summary"],
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "created_at": row["created_at"],
    }


def _project_customer_commitment_receipt_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "receipt_id": row["receipt_id"],
        "commitment_id": row["commitment_id"],
        "project_id": row["project_id"],
        "receipt_type": row["receipt_type"],
        "source_type": row["source_type"],
        "customer_ref": row["customer_ref"],
        "summary": row["summary"],
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "action_required": bool(row["action_required"]),
        "status": row["status"],
        "followup_task_id": row["followup_task_id"],
        "created_at": row["created_at"],
    }


def _portfolio_tradeoffs(recommendations: list[dict[str, Any]], constraints: dict[str, Any]) -> dict[str, Any]:
    total_revenue = sum((Decimal(item["revenue"]["reconciled_usd"]) for item in recommendations), Decimal("0"))
    total_unreconciled = sum((Decimal(item["revenue"]["unreconciled_usd"]) for item in recommendations), Decimal("0"))
    total_load = sum(int(item["operator_load_minutes"]) for item in recommendations)
    retained = sum(int(item["retention"]["retained"]) for item in recommendations)
    at_risk = sum(int(item["retention"]["at_risk"]) for item in recommendations)
    churned = sum(int(item["retention"]["churned"]) for item in recommendations)
    remaining_budget = sum((Decimal(item["budget"]["remaining_usd"]) for item in recommendations), Decimal("0"))
    return {
        "budget": {
            "remaining_usd": str(remaining_budget),
            "max_budget_usd": str(constraints.get("max_budget_usd", "")),
            "min_remaining_usd": str(constraints.get("min_budget_remaining_usd", "")),
        },
        "operator_load": {
            "minutes": total_load,
            "max_minutes": constraints.get("max_operator_load_minutes"),
        },
        "retention": {
            "retained": retained,
            "at_risk": at_risk,
            "churned": churned,
        },
        "revenue": {
            "reconciled_usd": str(total_revenue),
            "unreconciled_usd": str(total_unreconciled),
        },
    }


def _bounded_queue_adjustment(item: dict[str, Any], *, rank: int, constraints: dict[str, Any]) -> dict[str, Any]:
    action = item["recommended_action"]
    budget_remaining = Decimal(item["budget"]["remaining_usd"])
    operator_load = int(item["operator_load_minutes"])
    revenue = Decimal(item["revenue"]["reconciled_usd"])
    at_risk = int(item["retention"]["at_risk"])
    churned = int(item["retention"]["churned"])
    drivers: list[str] = []
    if revenue >= Decimal(str(constraints.get("high_revenue_usd", "250"))):
        drivers.append("revenue_high")
    if budget_remaining < Decimal(str(constraints.get("min_budget_remaining_usd", "0"))):
        drivers.append("budget_low")
    if constraints.get("max_operator_load_minutes") is not None and operator_load > int(constraints["max_operator_load_minutes"]):
        drivers.append("operator_load_high")
    if at_risk:
        drivers.append("retention_at_risk")
    if churned:
        drivers.append("customer_churned")

    if action in {"prioritize_next", "harvest_or_complete"}:
        queue_action = "recommend_next_internal_task"
        allowed_delta = 1
    elif action == "pause_until_operator_review":
        queue_action = "recommend_hold_new_internal_work"
        allowed_delta = 0
    elif action == "kill_or_stop_investment":
        queue_action = "recommend_operator_cancellation_gate"
        allowed_delta = 0
    else:
        queue_action = "maintain_current_internal_queue"
        allowed_delta = 0

    return {
        "project_id": item["project_id"],
        "priority_rank": rank,
        "source_recommended_action": action,
        "queue_action": queue_action,
        "max_queue_delta_tasks": allowed_delta,
        "allowed_task_types": ["operate", "feedback"],
        "customer_visible": False,
        "external_side_effects_authorized": [],
        "priority_change": {
            "applied": False,
            "requires_operator_gate": True,
        },
        "cancellation": {
            "applied": False,
            "requires_operator_gate": True,
        },
        "customer_commitment": {
            "applied": False,
            "allowed": False,
        },
        "tradeoff_drivers": drivers or ["steady_state"],
        "evidence_refs": item.get("evidence_refs", []),
    }


def _priority_change_from_adjustment(adjustment: dict[str, Any], *, scheduling_window: str) -> dict[str, Any]:
    task_type = "feedback" if "retention_at_risk" in adjustment["tradeoff_drivers"] else "operate"
    return {
        "project_id": adjustment["project_id"],
        "priority_rank": adjustment["priority_rank"],
        "scheduling_window": scheduling_window,
        "source_queue_action": adjustment["queue_action"],
        "queue_action": adjustment["queue_action"],
        "max_queue_delta_tasks": min(int(adjustment["max_queue_delta_tasks"]), 1),
        "task_type": task_type,
        "allowed_task_types": adjustment["allowed_task_types"],
        "customer_visible": False,
        "external_side_effects_authorized": [],
        "requires_operator_gate": True,
        "mutates_queue_on_packet_creation": False,
        "applies_only_on_accept": True,
        "cancellation": {
            "applied": False,
            "allowed": False,
            "requires_operator_gate": True,
        },
        "customer_commitment": {
            "applied": False,
            "allowed": False,
        },
        "tradeoff_drivers": adjustment["tradeoff_drivers"],
        "evidence_refs": adjustment.get("evidence_refs", []),
    }


def _not_applied_priority_change(change: dict[str, Any], *, reason: str) -> dict[str, Any]:
    return {
        "project_id": change["project_id"],
        "priority_rank": change["priority_rank"],
        "queue_action": change["queue_action"],
        "task_id": None,
        "task_type": change["task_type"],
        "status": "not_applied",
        "reason": reason,
        "customer_visible": False,
        "external_side_effects_authorized": [],
        "cancellation_applied": False,
        "customer_commitment_applied": False,
    }


def _priority_change_risk_flags(proposed_changes: list[dict[str, Any]]) -> list[str]:
    flags: list[str] = []
    if any(change["queue_action"] == "recommend_operator_cancellation_gate" for change in proposed_changes):
        flags.append("priority_packet_contains_cancellation_recommendation_without_cancellation_authority")
    if any(change["customer_visible"] for change in proposed_changes):
        flags.append("priority_packet_customer_visible_work_blocked")
    return flags


def _scheduling_risk_flags(queue_adjustments: list[dict[str, Any]]) -> list[str]:
    flags: list[str] = []
    for adjustment in queue_adjustments:
        drivers = set(adjustment["tradeoff_drivers"])
        if "budget_low" in drivers:
            flags.append("scheduling_budget_low")
        if "operator_load_high" in drivers:
            flags.append("scheduling_operator_load_high")
        if "retention_at_risk" in drivers:
            flags.append("scheduling_retention_at_risk")
        if "customer_churned" in drivers:
            flags.append("scheduling_customer_churned")
    return list(dict.fromkeys(flags))


def _portfolio_risk_flags(
    recommendations: list[dict[str, Any]],
    tradeoffs: dict[str, Any],
    constraints: dict[str, Any],
) -> list[str]:
    flags: list[str] = []
    if constraints.get("max_operator_load_minutes") is not None:
        if tradeoffs["operator_load"]["minutes"] > int(constraints["max_operator_load_minutes"]):
            flags.append("operator_load_over_constraint")
    if constraints.get("max_budget_usd") is not None:
        remaining = Decimal(tradeoffs["budget"]["remaining_usd"])
        if remaining < -Decimal(str(constraints["max_budget_usd"])):
            flags.append("budget_over_constraint")
    if constraints.get("min_budget_remaining_usd") is not None:
        lowest_remaining = min(
            (Decimal(item["budget"]["remaining_usd"]) for item in recommendations),
            default=Decimal("0"),
        )
        if lowest_remaining < Decimal(str(constraints["min_budget_remaining_usd"])):
            flags.append("budget_under_required_remaining")
    if tradeoffs["retention"]["at_risk"]:
        flags.append("retention_at_risk")
    if tradeoffs["retention"]["churned"]:
        flags.append("customer_churned")
    if Decimal(tradeoffs["revenue"]["unreconciled_usd"]) > Decimal("0"):
        flags.append("unreconciled_revenue")
    for item in recommendations:
        for flag in item["risk_flags"]:
            if flag not in flags:
                flags.append(flag)
    return flags


def _portfolio_packet_recommendation(recommendations: list[dict[str, Any]], risk_flags: list[str]) -> str:
    if not recommendations:
        return "defer"
    if "customer_churned" in risk_flags or all(item["recommended_action"] == "kill_or_stop_investment" for item in recommendations):
        return "pause"
    if {"operator_load_over_constraint", "budget_over_constraint"} & set(risk_flags):
        return "balance"
    if recommendations[0]["recommended_action"] in {"prioritize_next", "harvest_or_complete"}:
        return "prioritize"
    return "balance"


def _rollup_from_row(row: sqlite3.Row) -> ProjectStatusRollup:
    phase_rollups = [
        ProjectPhaseRollup(
            phase_name=phase["phase_name"],
            task_counts=phase["task_counts"],
            outcome_counts=phase["outcome_counts"],
            artifact_count=phase["artifact_count"],
            customer_feedback_count=phase["customer_feedback_count"],
            revenue_attributed_usd=Decimal(phase["revenue_attributed_usd"]),
            operator_load_minutes=phase["operator_load_minutes"],
            status=phase["status"],
            last_activity_at=phase.get("last_activity_at"),
        )
        for phase in _loads(row["phase_rollups_json"])
    ]
    return ProjectStatusRollup(
        rollup_id=row["rollup_id"],
        project_id=row["project_id"],
        project_status=row["project_status"],
        phase_rollups=phase_rollups,
        task_counts=_loads(row["task_counts_json"]),
        outcome_counts=_loads(row["outcome_counts_json"]),
        artifact_count=row["artifact_count"],
        customer_feedback_count=row["customer_feedback_count"],
        revenue_attributed_usd=Decimal(row["revenue_attributed_usd"]),
        operator_load_minutes=row["operator_load_minutes"],
        recommended_status=row["recommended_status"],
        close_recommendation=row["close_recommendation"],
        rationale=row["rationale"],
        risk_flags=_loads(row["risk_flags_json"]),
        commercial_rollup_id=row["commercial_rollup_id"],
        commercial_rollup=_loads(row["commercial_rollup_json"]),
        created_at=row["created_at"],
    )


def _commercial_rollup_from_row(row: sqlite3.Row) -> ProjectCommercialRollup:
    return ProjectCommercialRollup(
        rollup_id=row["rollup_id"],
        project_id=row["project_id"],
        revenue_reconciled_usd=Decimal(row["revenue_reconciled_usd"]),
        revenue_unreconciled_usd=Decimal(row["revenue_unreconciled_usd"]),
        retained_customer_count=row["retained_customer_count"],
        at_risk_customer_count=row["at_risk_customer_count"],
        churned_customer_count=row["churned_customer_count"],
        support_resolved_count=row["support_resolved_count"],
        support_open_count=row["support_open_count"],
        maintenance_resolved_count=row["maintenance_resolved_count"],
        maintenance_open_count=row["maintenance_open_count"],
        external_commitment_count=row["external_commitment_count"],
        receiptless_side_effect_count=row["receiptless_side_effect_count"],
        evidence_refs=_loads(row["evidence_refs_json"]),
        risk_flags=_loads(row["risk_flags_json"]),
        created_at=row["created_at"],
    )


def _latest_replay_project_commercial_rollup(replay: ReplayState, project_id: str) -> dict[str, Any]:
    rows = [
        row
        for row in replay.project_commercial_rollups.values()
        if row.get("project_id") == project_id
    ]
    if not rows:
        return {}
    return sorted(rows, key=lambda row: (row.get("created_at") or "", row.get("rollup_id") or ""))[-1]


def _count_by_status(items: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        status = item.get("status")
        if status is not None:
            counts[status] = counts.get(status, 0) + 1
    return counts


def _with_ref(refs: list[str], ref: str) -> list[str]:
    result = list(refs)
    if ref not in result:
        result.append(ref)
    return result


def _merge_refs(*groups: Any) -> list[str]:
    result: list[str] = []
    for group in groups:
        if group is None:
            continue
        if isinstance(group, str):
            candidates = [group]
        else:
            candidates = list(group)
        for ref in candidates:
            ref_text = str(ref)
            if ref_text and ref_text not in result:
                result.append(ref_text)
    return result


def _decimal_from(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def _model_task_class_payload(task_class: Any) -> dict[str, Any]:
    return {
        "task_class_id": task_class.task_class_id,
        "task_class": task_class.task_class,
        "description": task_class.description,
        "quality_threshold": task_class.quality_threshold,
        "reliability_threshold": task_class.reliability_threshold,
        "latency_p95_ms": task_class.latency_p95_ms,
        "local_offload_target": task_class.local_offload_target,
        "allowed_data_classes": task_class.allowed_data_classes,
        "promotion_authority": task_class.promotion_authority,
        "expansion_allowed": task_class.expansion_allowed,
        "status": task_class.status,
        "created_at": task_class.created_at,
    }


def _model_candidate_payload(candidate: Any) -> dict[str, Any]:
    return {
        "candidate_id": candidate.candidate_id,
        "model_id": candidate.model_id,
        "provider": candidate.provider,
        "access_mode": candidate.access_mode,
        "source_ref": candidate.source_ref,
        "artifact_hash": candidate.artifact_hash,
        "license": candidate.license,
        "commercial_use": candidate.commercial_use,
        "terms_verified_at": candidate.terms_verified_at,
        "context_window": candidate.context_window,
        "modalities": candidate.modalities,
        "hardware_fit": candidate.hardware_fit,
        "sandbox_profile": candidate.sandbox_profile,
        "data_residency": candidate.data_residency,
        "cost_profile": candidate.cost_profile,
        "latency_profile": candidate.latency_profile,
        "routing_metadata": candidate.routing_metadata,
        "promotion_state": candidate.promotion_state,
        "last_verified_at": candidate.last_verified_at,
    }


def _holdout_policy_payload(policy: Any) -> dict[str, Any]:
    return {
        "policy_id": policy.policy_id,
        "task_class": policy.task_class,
        "dataset_version": policy.dataset_version,
        "access": policy.access,
        "min_sample_count": policy.min_sample_count,
        "contamination_controls": policy.contamination_controls,
        "scorer_separation": policy.scorer_separation,
        "promotion_requires_decision": policy.promotion_requires_decision,
        "created_at": policy.created_at,
    }


def _local_offload_eval_set_payload(eval_set: Any, split_counts: dict[str, int] | None = None) -> dict[str, Any]:
    return {
        "eval_set_id": eval_set.eval_set_id,
        "task_class": eval_set.task_class,
        "dataset_version": eval_set.dataset_version,
        "artifact_ref": eval_set.artifact_ref,
        "split_counts": split_counts or eval_set.split_counts,
        "data_classes": eval_set.data_classes,
        "retention_policy": eval_set.retention_policy,
        "scorer_profile": eval_set.scorer_profile,
        "holdout_policy_id": eval_set.holdout_policy_id,
        "status": eval_set.status,
        "created_at": eval_set.created_at,
    }


def _holdout_use_payload(holdout_use: Any) -> dict[str, Any]:
    return {
        "holdout_use_id": holdout_use.holdout_use_id,
        "policy_id": holdout_use.policy_id,
        "eval_set_id": holdout_use.eval_set_id,
        "task_class": holdout_use.task_class,
        "dataset_version": holdout_use.dataset_version,
        "requester_id": holdout_use.requester_id,
        "requester_change_ref": holdout_use.requester_change_ref,
        "purpose": holdout_use.purpose,
        "verdict": holdout_use.verdict,
        "reason": holdout_use.reason,
        "decision_id": holdout_use.decision_id,
        "created_at": holdout_use.created_at,
    }


def _model_eval_run_payload(eval_run: Any) -> dict[str, Any]:
    return {
        "eval_run_id": eval_run.eval_run_id,
        "model_id": eval_run.model_id,
        "task_class": eval_run.task_class,
        "dataset_version": eval_run.dataset_version,
        "eval_set_id": eval_run.eval_set_id,
        "baseline_model_id": eval_run.baseline_model_id,
        "route_version": eval_run.route_version,
        "route_metadata": eval_run.route_metadata,
        "sample_count": eval_run.sample_count,
        "quality_score": eval_run.quality_score,
        "reliability_score": eval_run.reliability_score,
        "latency_p50_ms": eval_run.latency_p50_ms,
        "latency_p95_ms": eval_run.latency_p95_ms,
        "cost_per_1k_tasks": str(eval_run.cost_per_1k_tasks),
        "aggregate_scores": eval_run.aggregate_scores,
        "failure_categories": eval_run.failure_categories,
        "failure_modes": eval_run.failure_modes,
        "confidence": eval_run.confidence,
        "frozen_holdout_result": eval_run.frozen_holdout_result,
        "verdict": eval_run.verdict,
        "scorer_id": eval_run.scorer_id,
        "decision_id": eval_run.decision_id,
        "authority_effect": eval_run.authority_effect,
        "created_at": eval_run.created_at,
    }


def _model_route_decision_payload(route_decision: Any) -> dict[str, Any]:
    return {
        "route_decision_id": route_decision.route_decision_id,
        "task_id": route_decision.task_id,
        "task_class": route_decision.task_class,
        "data_class": route_decision.data_class,
        "risk_level": route_decision.risk_level,
        "selected_route": route_decision.selected_route,
        "selected_model_id": route_decision.selected_model_id,
        "candidate_model_id": route_decision.candidate_model_id,
        "eval_set_id": route_decision.eval_set_id,
        "reasons": route_decision.reasons,
        "required_authority": route_decision.required_authority,
        "decision_id": route_decision.decision_id,
        "local_offload_estimate": route_decision.local_offload_estimate,
        "frontier_fallback": route_decision.frontier_fallback,
        "created_at": route_decision.created_at,
    }


def _model_promotion_packet_payload(packet: Any) -> dict[str, Any]:
    return {
        "packet_id": packet.packet_id,
        "decision_id": packet.decision_id,
        "model_id": packet.model_id,
        "task_class": packet.task_class,
        "proposed_routing_role": packet.proposed_routing_role,
        "recommendation": packet.recommendation,
        "required_authority": packet.required_authority,
        "eval_run_ids": packet.eval_run_ids,
        "holdout_use_ids": packet.holdout_use_ids,
        "evidence_refs": packet.evidence_refs,
        "frozen_holdout_confidence": packet.frozen_holdout_confidence,
        "confidence_threshold": packet.confidence_threshold,
        "gate_packet": packet.gate_packet,
        "risk_flags": packet.risk_flags,
        "default_on_timeout": packet.default_on_timeout,
        "status": packet.status,
        "created_at": packet.created_at,
    }


def _model_demotion_payload(demotion: Any, routing_state_after: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "demotion_id": demotion.demotion_id,
        "model_id": demotion.model_id,
        "task_class": demotion.task_class,
        "routing_roles": demotion.routing_roles,
        "reasons": demotion.reasons,
        "required_authority": demotion.required_authority,
        "evidence_refs": demotion.evidence_refs,
        "eval_run_ids": demotion.eval_run_ids,
        "route_decision_ids": demotion.route_decision_ids,
        "metrics": demotion.metrics,
        "routing_state_update": demotion.routing_state_update,
        "routing_state_after": routing_state_after,
        "audit_notes": demotion.audit_notes,
        "decision_id": demotion.decision_id,
        "authority_effect": demotion.authority_effect,
        "created_at": demotion.created_at,
    }


def _source_requires_explicit_grant(access_method: str, data_class: str) -> bool:
    return access_method in {"operator_provided", "paid_source", "local_file", "internal_record", "api"} or data_class in {
        "internal",
        "sensitive",
        "secret_ref",
        "regulated",
        "client_confidential",
    }


def _validate_evidence_bundle(
    *,
    profile: str,
    source_policy: dict[str, Any],
    evidence_requirements: dict[str, Any],
    bundle: EvidenceBundle,
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    sources = [_source_payload(source) for source in bundle.sources]
    claims = [_claim_payload(claim) for claim in bundle.claims]

    minimum_sources = int(evidence_requirements.get("minimum_sources", 1))
    checks.append(
        {
            "name": "minimum_sources",
            "result": "pass" if len(sources) >= minimum_sources else "fail",
            "detail": f"{len(sources)} sources present; {minimum_sources} required",
        }
    )
    checks.append(
        {
            "name": "uncertainty_required",
            "result": "pass" if bundle.uncertainty.strip() else "fail",
            "detail": "uncertainty is recorded" if bundle.uncertainty.strip() else "uncertainty is missing",
        }
    )

    allowed = set(source_policy.get("allowed_source_types") or [])
    blocked = set(source_policy.get("blocked_source_types") or [])
    source_types = {source["source_type"] for source in sources}
    if allowed:
        outside_allowed = sorted(source_types - allowed)
        checks.append(
            {
                "name": "allowed_source_types",
                "result": "pass" if not outside_allowed else "fail",
                "detail": ",".join(outside_allowed) if outside_allowed else "all source types allowed",
            }
        )
    blocked_present = sorted(source_types & blocked)
    checks.append(
        {
            "name": "blocked_source_types",
            "result": "pass" if not blocked_present else "fail",
            "detail": ",".join(blocked_present) if blocked_present else "no blocked source types present",
        }
    )

    if evidence_requirements.get("high_stakes_claims_require_independent_sources", False):
        official_or_primary = {"official", "primary_data"}
        type_by_id = {source["source_id"]: source["source_type"] for source in sources}
        weak_claims = [
            claim["claim_id"]
            for claim in claims
            if claim["importance"] in {"high", "critical"}
            and len(set(claim["source_ids"])) < 2
            and not any(type_by_id.get(source_id) in official_or_primary for source_id in claim["source_ids"])
        ]
        checks.append(
            {
                "name": "high_stakes_claim_support",
                "result": "pass" if not weak_claims else "fail",
                "detail": ",".join(weak_claims) if weak_claims else "high-stakes claims are sufficiently sourced",
            }
        )

    checks.extend(_profile_quality_checks(profile, sources, claims, bundle))
    return checks


def _profile_quality_checks(
    profile: str,
    sources: list[dict[str, Any]],
    claims: list[dict[str, Any]],
    bundle: EvidenceBundle,
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    source_types = {source["source_type"] for source in sources}
    claim_text = "\n".join(claim["text"].lower() for claim in claims)

    if profile == "commercial":
        wtp_terms = ("willingness-to-pay", "willingness to pay", "pricing", "buyer", "transaction", "market")
        has_wtp_claim = any(term in claim_text for term in wtp_terms)
        has_wtp_evidence = bool(source_types & {"official", "primary_data", "market_data", "internal_record"})
        checks.append(
            {
                "name": "commercial_willingness_to_pay_evidence",
                "result": "pass" if not has_wtp_claim or has_wtp_evidence else "fail",
                "detail": "buyer/pricing evidence present" if has_wtp_evidence else "willingness-to-pay claim lacks buyer/pricing evidence",
            }
        )
        has_operator_load = "operator load" in claim_text or "operator-load" in claim_text or "operator_load" in claim_text
        checks.append(
            {
                "name": "commercial_operator_load_estimate",
                "result": "pass" if has_operator_load or bundle.quality_gate_result == "degraded" else "degraded",
                "detail": "operator-load estimate recorded" if has_operator_load else "operator-load estimate not explicit",
            }
        )
    elif profile == "ai_models":
        license_known = "license" in claim_text or any(source["source_type"] == "model_card" for source in sources)
        checks.append(
            {
                "name": "ai_models_license_status",
                "result": "pass" if license_known else "degraded",
                "detail": "license/commercial-use status addressed" if license_known else "license/commercial-use status unknown",
            }
        )
        if any(claim["claim_type"] == "recommendation" and "promote" in claim["text"].lower() for claim in claims):
            checks.append(
                {
                    "name": "ai_models_no_autonomous_promotion",
                    "result": "fail",
                    "detail": "model radar may recommend evals but cannot promote models",
                }
            )
    elif profile == "financial_markets":
        forecasts = [claim["claim_id"] for claim in claims if claim["claim_type"] == "forecast"]
        unlabeled = [claim["claim_id"] for claim in claims if "will " in claim["text"].lower() and claim["claim_type"] != "forecast"]
        checks.append(
            {
                "name": "financial_forecast_labeling",
                "result": "pass" if forecasts or not unlabeled else "fail",
                "detail": ",".join(unlabeled) if unlabeled else "forecasts are labeled or absent",
            }
        )
    elif profile == "system_improvement":
        has_eval_plan = "eval" in claim_text or "replay" in claim_text
        checks.append(
            {
                "name": "system_improvement_eval_plan",
                "result": "pass" if has_eval_plan else "fail",
                "detail": "eval/replay plan present" if has_eval_plan else "improvement lacks eval or replay plan",
            }
        )
    elif profile == "security":
        has_component = "component" in claim_text or "version" in claim_text
        has_mitigation = "mitigation" in claim_text or "patch" in claim_text
        checks.append(
            {
                "name": "security_component_and_mitigation",
                "result": "pass" if has_component and has_mitigation else "fail",
                "detail": "component/version and mitigation covered" if has_component and has_mitigation else "security finding lacks component/version or mitigation",
            }
        )
    elif profile == "regulatory":
        has_jurisdiction = "jurisdiction" in claim_text
        has_effective_date = "effective date" in claim_text
        checks.append(
            {
                "name": "regulatory_authority_context",
                "result": "pass" if has_jurisdiction and has_effective_date else "fail",
                "detail": "jurisdiction and effective date covered" if has_jurisdiction and has_effective_date else "regulatory claim lacks jurisdiction or effective date",
            }
        )
    return checks


def _quality_gate_result(checks: list[dict[str, Any]], requested_result: str) -> str:
    results = {check["result"] for check in checks}
    if "fail" in results:
        return "fail"
    if "degraded" in results or requested_result == "degraded":
        return "degraded"
    return "pass"
