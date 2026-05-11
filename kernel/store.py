from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

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
    HermesAdapterReadinessPacket,
    HermesAdapterReadinessReplayProjectionComparison,
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

from .replay import (
    KERNEL_EVENT_SCHEMA_VERSION,
    KERNEL_POLICY_VERSION,
    LEGACY_BOUNDARIES,
    ReplayState,
    apply_replay_event,
    create_kernel_database,
)
from .store_artifacts import ArtifactKernelTransactionMixin
from .store_commercial import CommercialKernelTransactionMixin
from .store_model_intelligence import ModelIntelligenceKernelTransactionMixin
from .store_recovery import RecoveryKernelTransactionMixin
from .store_research import ResearchKernelTransactionMixin
from .store_common import (
    _loads,
)


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

    def record_artifact_governance(self, command: Command, record: ArtifactGovernanceRecord) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_artifact_governance(record)

        return self.execute_command(command, handler)

    def record_artifact_payload_metadata(self, command: Command, metadata: ArtifactPayloadMetadata) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_artifact_payload_metadata(metadata)

        return self.execute_command(command, handler)

    def scan_artifact_retention_due(self, command: Command, as_of: str) -> list[ArtifactLifecycleTaskPacket]:
        def handler(tx: KernelTransaction) -> list[ArtifactLifecycleTaskPacket]:
            return tx.scan_artifact_retention_due(as_of)

        return self.execute_command(command, handler)

    def complete_artifact_lifecycle_task(
        self,
        command: Command,
        packet_id: str,
        *,
        receipt_ref: str,
        receipt_hash: str,
        status: str = "completed",
        reason: str | None = None,
    ) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.complete_artifact_lifecycle_task(
                packet_id,
                receipt_ref=receipt_ref,
                receipt_hash=receipt_hash,
                status=status,
                reason=reason,
            )

        return self.execute_command(command, handler)

    def compare_artifact_lifecycle_replay_to_projection(
        self,
        command: Command,
        artifact_id: str,
    ) -> ArtifactLifecycleReplayProjectionComparison:
        def handler(tx: KernelTransaction) -> ArtifactLifecycleReplayProjectionComparison:
            return tx.compare_artifact_lifecycle_replay_to_projection(artifact_id)

        return self.execute_command(command, handler)

    def record_encrypted_storage_descriptor(self, command: Command, descriptor: EncryptedStorageDescriptor) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_encrypted_storage_descriptor(descriptor)

        return self.execute_command(command, handler)

    def record_encrypted_storage_key_rotation(
        self,
        command: Command,
        rotation: EncryptedStorageKeyRotationRecord,
    ) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_encrypted_storage_key_rotation(rotation)

        return self.execute_command(command, handler)

    def record_payload_access_receipt(self, command: Command, receipt: PayloadAccessReceipt) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_payload_access_receipt(receipt)

        return self.execute_command(command, handler)

    def record_encrypted_storage_access_verification(
        self,
        command: Command,
        verification: EncryptedStorageAccessVerificationState,
    ) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_encrypted_storage_access_verification(verification)

        return self.execute_command(command, handler)

    def compare_encrypted_storage_replay_to_projection(
        self,
        command: Command,
        descriptor_id: str,
    ) -> EncryptedStorageReplayProjectionComparison:
        def handler(tx: KernelTransaction) -> EncryptedStorageReplayProjectionComparison:
            return tx.compare_encrypted_storage_replay_to_projection(descriptor_id)

        return self.execute_command(command, handler)

    def record_backup_cadence(self, command: Command, record: BackupCadenceRecord) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_backup_cadence(record)

        return self.execute_command(command, handler)

    def create_restore_drill_packet(self, command: Command, packet: RestoreDrillPacket) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.create_restore_drill_packet(packet)

        return self.execute_command(command, handler)

    def record_recovery_checklist_receipt(self, command: Command, receipt: RecoveryChecklistReceipt) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_recovery_checklist_receipt(receipt)

        return self.execute_command(command, handler)

    def record_recovery_verification_state(
        self,
        command: Command,
        verification: RecoveryVerificationState,
    ) -> str:
        def handler(tx: KernelTransaction) -> str:
            return tx.record_recovery_verification_state(verification)

        return self.execute_command(command, handler)

    def compare_recovery_replay_to_projection(
        self,
        command: Command,
        drill_id: str,
    ) -> RecoveryReplayProjectionComparison:
        def handler(tx: KernelTransaction) -> RecoveryReplayProjectionComparison:
            return tx.compare_recovery_replay_to_projection(drill_id)

        return self.execute_command(command, handler)

    def create_recovery_readiness_packet(
        self,
        command: Command,
        *,
        scope: str,
        as_of: str,
    ) -> RecoveryReadinessPacket:
        def handler(tx: KernelTransaction) -> RecoveryReadinessPacket:
            return tx.create_recovery_readiness_packet(scope=scope, as_of=as_of)

        return self.execute_command(command, handler)

    def compare_recovery_readiness_replay_to_projection(
        self,
        command: Command,
        packet_id: str,
    ) -> RecoveryReadinessReplayProjectionComparison:
        def handler(tx: KernelTransaction) -> RecoveryReadinessReplayProjectionComparison:
            return tx.compare_recovery_readiness_replay_to_projection(packet_id)

        return self.execute_command(command, handler)

    def create_hermes_adapter_readiness_packet(
        self,
        command: Command,
        *,
        adapter_name: str,
        hermes_version: str,
        as_of: str,
        surface_checks: list[dict[str, Any]],
        reconciliation_checks: list[dict[str, Any]],
        recovery_readiness_packet_id: str | None = None,
    ) -> HermesAdapterReadinessPacket:
        def handler(tx: KernelTransaction) -> HermesAdapterReadinessPacket:
            return tx.create_hermes_adapter_readiness_packet(
                adapter_name=adapter_name,
                hermes_version=hermes_version,
                as_of=as_of,
                surface_checks=surface_checks,
                reconciliation_checks=reconciliation_checks,
                recovery_readiness_packet_id=recovery_readiness_packet_id,
            )

        return self.execute_command(command, handler)

    def compare_hermes_adapter_readiness_replay_to_projection(
        self,
        command: Command,
        packet_id: str,
    ) -> HermesAdapterReadinessReplayProjectionComparison:
        def handler(tx: KernelTransaction) -> HermesAdapterReadinessReplayProjectionComparison:
            return tx.compare_hermes_adapter_readiness_replay_to_projection(packet_id)

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
            apply_replay_event(state, row["event_type"], row["entity_id"], payload)
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




class KernelTransaction(
    ArtifactKernelTransactionMixin,
    RecoveryKernelTransactionMixin,
    ResearchKernelTransactionMixin,
    CommercialKernelTransactionMixin,
    ModelIntelligenceKernelTransactionMixin,
):
    def __init__(self, conn: sqlite3.Connection, command: Command) -> None:
        self.conn = conn
        self.command = command
        self.transaction_id = new_id()
        self.last_event_id: str | None = None

    @staticmethod
    def _replay_from_connection(conn: sqlite3.Connection) -> ReplayState:
        return KernelStore._replay_from_connection(conn)

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
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("workers cannot mint capability grants")
        if grant.issuer != "kernel":
            raise PermissionError("capability grants must be issued by the kernel")
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

    def prepare_side_effect(self, intent: SideEffectIntent) -> str:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("workers cannot prepare side effects directly")
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
