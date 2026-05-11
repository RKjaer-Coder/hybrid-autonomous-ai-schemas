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
from .replay import ReplayState
from .store_common import (
    _backup_cadence_record_payload,
    _restore_drill_packet_payload,
    _recovery_checklist_receipt_payload,
    _recovery_verification_state_payload,
    _backup_cadence_record_row_payload,
    _restore_drill_packet_row_payload,
    _recovery_checklist_receipt_row_payload,
    _recovery_verification_state_row_payload,
    _recovery_replay_projection_comparison_payload,
    _recovery_readiness_packet_payload,
    _recovery_readiness_packet_row_payload,
    _recovery_readiness_replay_projection_comparison_payload,
    _recovery_readiness_actions,
    _recovery_readiness_evidence_refs,
    _hermes_adapter_readiness_packet_payload,
    _hermes_adapter_readiness_packet_row_payload,
    _hermes_adapter_readiness_replay_projection_comparison_payload,
)


class RecoveryKernelTransactionMixin:
    HERMES_V013_REQUIRED_SURFACES = (
        "kanban_worker_lifecycle",
        "dashboard_profile_provider_controls",
        "provider_plugin_calls",
        "mcp_sse_oauth_forwarding",
        "no_agent_cron_watchdog",
        "gateway_goal_checkpoint_resume",
        "platform_allowlists_redaction",
    )
    HERMES_V013_REQUIRED_RECONCILIATION_CHECKS = (
        "kernel_task_status",
        "assignment_ownership",
        "grant_status_scope_expiry_use_count",
        "budget_reservation_status",
        "side_effect_intent_idempotency_receipt",
        "policy_version",
        "operator_halt_quarantine_state",
    )

    def record_backup_cadence(self, record: BackupCadenceRecord) -> str:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("backup cadence records are kernel-owned")
        if not record.scope.strip() or not record.backup_target.strip():
            raise ValueError("backup cadence scope and target are required")
        if record.status == "active" and not record.encryption_required:
            raise PermissionError("active backup cadence records must require encryption")
        payload = _backup_cadence_record_payload(record)
        event_id = self.append_event("backup_cadence_recorded", "policy", record.cadence_id, payload, "internal")
        self.conn.execute(
            """
            INSERT INTO backup_cadence_records (
              cadence_id, scope, cadence, backup_target, encryption_required,
              retention_policy, recovery_point_objective, next_due_at, status,
              evidence_refs_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.cadence_id,
                record.scope,
                record.cadence,
                record.backup_target,
                1 if record.encryption_required else 0,
                record.retention_policy,
                record.recovery_point_objective,
                record.next_due_at,
                record.status,
                canonical_json(record.evidence_refs),
                record.created_at,
                record.updated_at,
            ),
        )
        self.enqueue_projection(event_id, "backup_cadence_projection")
        return record.cadence_id

    def create_restore_drill_packet(self, packet: RestoreDrillPacket) -> str:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("restore drill packets are kernel-owned")
        cadence = self.conn.execute(
            "SELECT * FROM backup_cadence_records WHERE cadence_id=?",
            (packet.cadence_id,),
        ).fetchone()
        if cadence is None:
            raise ValueError("restore drill packet requires an existing backup cadence")
        if cadence["status"] != "active":
            raise PermissionError("restore drill packets require an active backup cadence")
        if not packet.backup_ref.strip() or not packet.backup_manifest_hash.strip():
            raise ValueError("restore drill packet requires backup reference and manifest hash")
        if not packet.checklist_items:
            raise ValueError("restore drill packet requires checklist items")
        payload = _restore_drill_packet_payload(packet)
        event_id = self.append_event("restore_drill_packet_created", "policy", packet.drill_id, payload, "internal")
        self.conn.execute(
            """
            INSERT INTO restore_drill_packets (
              drill_id, cadence_id, backup_ref, backup_manifest_hash, drill_scope,
              scheduled_for, required_authority, checklist_items_json,
              evidence_refs_json, status, created_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.drill_id,
                packet.cadence_id,
                packet.backup_ref,
                packet.backup_manifest_hash,
                packet.drill_scope,
                packet.scheduled_for,
                packet.required_authority,
                canonical_json(packet.checklist_items),
                canonical_json(packet.evidence_refs),
                packet.status,
                packet.created_at,
                packet.completed_at,
            ),
        )
        self.enqueue_projection(event_id, "restore_drill_packet_projection")
        return packet.drill_id

    def record_recovery_checklist_receipt(self, receipt: RecoveryChecklistReceipt) -> str:
        if self.command.requested_by != "operator" or self.command.requested_authority != "operator_gate":
            raise PermissionError("recovery checklist receipts require operator-gate authority")
        packet = self.conn.execute(
            "SELECT * FROM restore_drill_packets WHERE drill_id=?",
            (receipt.drill_id,),
        ).fetchone()
        if packet is None:
            raise ValueError("recovery checklist receipt requires an existing restore drill packet")
        if packet["status"] != "queued":
            raise ValueError(f"cannot receipt recovery checklist from drill status {packet['status']}")
        if not receipt.checklist_results:
            raise ValueError("recovery checklist receipt requires checklist results")
        if not receipt.receipt_ref.strip() or not receipt.receipt_hash.strip():
            raise ValueError("recovery checklist receipt requires durable receipt references")
        payload = _recovery_checklist_receipt_payload(receipt)
        event_id = self.append_event(
            "recovery_checklist_receipt_recorded",
            "policy",
            receipt.receipt_id,
            payload,
            "internal",
            actor_type="operator",
            actor_id=receipt.operator_id,
        )
        self.conn.execute(
            """
            INSERT INTO recovery_checklist_receipts (
              receipt_id, drill_id, operator_id, checklist_results_json,
              receipt_ref, receipt_hash, status, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                receipt.receipt_id,
                receipt.drill_id,
                receipt.operator_id,
                canonical_json(receipt.checklist_results),
                receipt.receipt_ref,
                receipt.receipt_hash,
                receipt.status,
                receipt.notes,
                receipt.created_at,
            ),
        )
        self.enqueue_projection(event_id, "recovery_checklist_receipt_projection")
        return receipt.receipt_id

    def record_recovery_verification_state(self, verification: RecoveryVerificationState) -> str:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("recovery verification state is kernel-owned")
        packet = self.conn.execute(
            "SELECT * FROM restore_drill_packets WHERE drill_id=?",
            (verification.drill_id,),
        ).fetchone()
        if packet is None:
            raise ValueError("recovery verification requires an existing restore drill packet")
        if packet["status"] != "queued":
            raise ValueError(f"cannot verify recovery drill from status {packet['status']}")
        if verification.cadence_id != packet["cadence_id"]:
            raise ValueError("recovery verification cadence does not match restore drill packet")
        if verification.backup_manifest_hash != packet["backup_manifest_hash"]:
            raise ValueError("recovery verification manifest hash does not match restore drill packet")
        all_checks_pass = all(bool(value) for value in verification.verification_checks.values())
        if verification.status == "verified":
            if verification.fail_closed or not all_checks_pass or verification.mismatch_summary:
                raise PermissionError("verified recovery state requires passing checks and open fail-closed gate")
            if verification.receipt_id is None:
                raise PermissionError("verified recovery state requires an accepted operator checklist receipt")
            receipt = self.conn.execute(
                "SELECT * FROM recovery_checklist_receipts WHERE receipt_id=? AND drill_id=?",
                (verification.receipt_id, verification.drill_id),
            ).fetchone()
            if receipt is None or receipt["status"] != "accepted":
                raise PermissionError("verified recovery state requires an accepted operator checklist receipt")
        elif not verification.fail_closed:
            raise PermissionError("failed or blocked recovery verification must fail closed")
        payload = _recovery_verification_state_payload(verification)
        event_id = self.append_event(
            "recovery_verification_state_recorded",
            "policy",
            verification.verification_id,
            payload,
            "internal",
        )
        self.conn.execute(
            """
            INSERT INTO recovery_verification_states (
              verification_id, drill_id, cadence_id, receipt_id,
              backup_manifest_hash, status, fail_closed, verification_checks_json,
              mismatch_summary_json, evidence_refs_json, verified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verification.verification_id,
                verification.drill_id,
                verification.cadence_id,
                verification.receipt_id,
                verification.backup_manifest_hash,
                verification.status,
                1 if verification.fail_closed else 0,
                canonical_json(verification.verification_checks),
                canonical_json(verification.mismatch_summary),
                canonical_json(verification.evidence_refs),
                verification.verified_at,
            ),
        )
        self.conn.execute(
            "UPDATE restore_drill_packets SET status=?, completed_at=? WHERE drill_id=?",
            (verification.status, verification.verified_at, verification.drill_id),
        )
        self.enqueue_projection(event_id, "recovery_verification_state_projection")
        return verification.verification_id

    def compare_recovery_replay_to_projection(self, drill_id: str) -> RecoveryReplayProjectionComparison:
        projection_drill = self.conn.execute(
            "SELECT * FROM restore_drill_packets WHERE drill_id=?",
            (drill_id,),
        ).fetchone()
        if projection_drill is None:
            raise ValueError("restore drill packet not found")
        projection_cadence = self.conn.execute(
            "SELECT * FROM backup_cadence_records WHERE cadence_id=?",
            (projection_drill["cadence_id"],),
        ).fetchone()
        replay = self.__class__._replay_from_connection(self.conn)
        replay_cadence = replay.backup_cadence_records.get(projection_drill["cadence_id"], {})
        replay_drill_packet = replay.restore_drill_packets.get(drill_id, {})
        replay_checklist_receipts = sorted(
            (
                dict(value)
                for value in replay.recovery_checklist_receipts.values()
                if value["drill_id"] == drill_id
            ),
            key=lambda item: item["receipt_id"],
        )
        replay_verification_state = next(
            (dict(value) for value in replay.recovery_verification_states.values() if value["drill_id"] == drill_id),
            {},
        )
        projection_receipts = [
            _recovery_checklist_receipt_row_payload(row)
            for row in self.conn.execute(
                "SELECT * FROM recovery_checklist_receipts WHERE drill_id=? ORDER BY receipt_id",
                (drill_id,),
            ).fetchall()
        ]
        projection_verification = self.conn.execute(
            "SELECT * FROM recovery_verification_states WHERE drill_id=?",
            (drill_id,),
        ).fetchone()
        projection_cadence_payload = {} if projection_cadence is None else _backup_cadence_record_row_payload(projection_cadence)
        projection_drill_payload = _restore_drill_packet_row_payload(projection_drill)
        projection_verification_payload = (
            {} if projection_verification is None else _recovery_verification_state_row_payload(projection_verification)
        )
        mismatches: list[str] = []
        if replay_cadence != projection_cadence_payload:
            mismatches.append("backup_cadence_records")
        if replay_drill_packet != projection_drill_payload:
            mismatches.append("restore_drill_packets")
        if replay_checklist_receipts != projection_receipts:
            mismatches.append("recovery_checklist_receipts")
        if replay_verification_state != projection_verification_payload:
            mismatches.append("recovery_verification_states")
        comparison = RecoveryReplayProjectionComparison(
            drill_id=drill_id,
            replay_cadence=replay_cadence,
            projection_cadence=projection_cadence_payload,
            replay_drill_packet=replay_drill_packet,
            projection_drill_packet=projection_drill_payload,
            replay_checklist_receipts=replay_checklist_receipts,
            projection_checklist_receipts=projection_receipts,
            replay_verification_state=replay_verification_state,
            projection_verification_state=projection_verification_payload,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _recovery_replay_projection_comparison_payload(comparison)
        event_id = self.append_event(
            "recovery_replay_projection_compared",
            "policy",
            comparison.comparison_id,
            payload,
            "internal",
        )
        self.conn.execute(
            """
            INSERT INTO recovery_replay_projection_comparisons (
              comparison_id, drill_id, replay_cadence_json, projection_cadence_json,
              replay_drill_packet_json, projection_drill_packet_json,
              replay_checklist_receipts_json, projection_checklist_receipts_json,
              replay_verification_state_json, projection_verification_state_json,
              matches, mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.drill_id,
                canonical_json(comparison.replay_cadence),
                canonical_json(comparison.projection_cadence),
                canonical_json(comparison.replay_drill_packet),
                canonical_json(comparison.projection_drill_packet),
                canonical_json(comparison.replay_checklist_receipts),
                canonical_json(comparison.projection_checklist_receipts),
                canonical_json(comparison.replay_verification_state),
                canonical_json(comparison.projection_verification_state),
                1 if comparison.matches else 0,
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "recovery_replay_projection_comparison_projection")
        return comparison

    def create_recovery_readiness_packet(self, *, scope: str, as_of: str) -> RecoveryReadinessPacket:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("recovery readiness packets are kernel-owned read-only state")
        if not scope.strip() or not as_of.strip():
            raise ValueError("recovery readiness packets require scope and as_of")
        packet = self._build_recovery_readiness_packet(scope=scope, as_of=as_of)
        if packet.live_controls_enabled:
            raise PermissionError("recovery readiness packets cannot enable live controls")
        payload = _recovery_readiness_packet_payload(packet)
        event_id = self.append_event(
            "recovery_readiness_packet_created",
            "policy",
            packet.packet_id,
            payload,
            "internal",
        )
        self.conn.execute(
            """
            INSERT INTO recovery_readiness_packets (
              packet_id, scope, as_of, backup_cadence_summary_json,
              restore_drill_summary_json, encrypted_payload_descriptor_summary_json,
              payload_access_failure_summary_json, fail_closed_state_json,
              next_operator_actions_json, readiness_status, evidence_refs_json,
              live_controls_enabled, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.packet_id,
                packet.scope,
                packet.as_of,
                canonical_json(packet.backup_cadence_summary),
                canonical_json(packet.restore_drill_summary),
                canonical_json(packet.encrypted_payload_descriptor_summary),
                canonical_json(packet.payload_access_failure_summary),
                canonical_json(packet.fail_closed_state),
                canonical_json(packet.next_operator_actions),
                packet.readiness_status,
                canonical_json(packet.evidence_refs),
                0,
                packet.created_at,
            ),
        )
        self.enqueue_projection(event_id, "recovery_readiness_packet_projection")
        return packet

    def compare_recovery_readiness_replay_to_projection(
        self,
        packet_id: str,
    ) -> RecoveryReadinessReplayProjectionComparison:
        projection_row = self.conn.execute(
            "SELECT * FROM recovery_readiness_packets WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
        if projection_row is None:
            raise ValueError("recovery readiness packet not found")
        replay = self.__class__._replay_from_connection(self.conn)
        replay_packet = replay.recovery_readiness_packets.get(packet_id, {})
        projection_packet = _recovery_readiness_packet_row_payload(projection_row)
        mismatches: list[str] = []
        if replay_packet != projection_packet:
            mismatches.append("recovery_readiness_packets")
        comparison = RecoveryReadinessReplayProjectionComparison(
            packet_id=packet_id,
            replay_packet=replay_packet,
            projection_packet=projection_packet,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _recovery_readiness_replay_projection_comparison_payload(comparison)
        event_id = self.append_event(
            "recovery_readiness_replay_projection_compared",
            "policy",
            comparison.comparison_id,
            payload,
            "internal",
        )
        self.conn.execute(
            """
            INSERT INTO recovery_readiness_replay_projection_comparisons (
              comparison_id, packet_id, replay_packet_json, projection_packet_json,
              matches, mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.packet_id,
                canonical_json(comparison.replay_packet),
                canonical_json(comparison.projection_packet),
                1 if comparison.matches else 0,
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "recovery_readiness_replay_projection_comparison_projection")
        return comparison

    def create_hermes_adapter_readiness_packet(
        self,
        *,
        adapter_name: str,
        hermes_version: str,
        as_of: str,
        surface_checks: list[dict[str, Any]],
        reconciliation_checks: list[dict[str, Any]],
        recovery_readiness_packet_id: str | None = None,
    ) -> HermesAdapterReadinessPacket:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("Hermes adapter readiness packets are kernel-owned read-only state")
        if not adapter_name.strip() or not hermes_version.strip() or not as_of.strip():
            raise ValueError("Hermes adapter readiness packets require adapter_name, hermes_version, and as_of")
        if not surface_checks or not reconciliation_checks:
            raise ValueError("Hermes adapter readiness packets require surface and reconciliation checks")
        recovery_row = None
        if recovery_readiness_packet_id is not None:
            recovery_row = self.conn.execute(
                "SELECT * FROM recovery_readiness_packets WHERE packet_id=?",
                (recovery_readiness_packet_id,),
            ).fetchone()
            if recovery_row is None:
                raise ValueError("Hermes adapter readiness requires an existing recovery readiness packet")

        packet = self._build_hermes_adapter_readiness_packet(
            adapter_name=adapter_name,
            hermes_version=hermes_version,
            as_of=as_of,
            surface_checks=surface_checks,
            reconciliation_checks=reconciliation_checks,
            recovery_readiness_packet_id=recovery_readiness_packet_id,
            recovery_readiness_status=None if recovery_row is None else recovery_row["readiness_status"],
        )
        if packet.live_controls_enabled:
            raise PermissionError("Hermes adapter readiness packets cannot enable live controls")
        payload = _hermes_adapter_readiness_packet_payload(packet)
        event_id = self.append_event(
            "hermes_adapter_readiness_packet_created",
            "policy",
            packet.packet_id,
            payload,
            "internal",
        )
        self.conn.execute(
            """
            INSERT INTO hermes_adapter_readiness_packets (
              packet_id, adapter_name, hermes_version, as_of, surface_checks_json,
              reconciliation_checks_json, recovery_readiness_packet_id,
              next_operator_actions_json, readiness_status, evidence_refs_json,
              live_controls_enabled, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.packet_id,
                packet.adapter_name,
                packet.hermes_version,
                packet.as_of,
                canonical_json(packet.surface_checks),
                canonical_json(packet.reconciliation_checks),
                packet.recovery_readiness_packet_id,
                canonical_json(packet.next_operator_actions),
                packet.readiness_status,
                canonical_json(packet.evidence_refs),
                0,
                packet.created_at,
            ),
        )
        self.enqueue_projection(event_id, "hermes_adapter_readiness_packet_projection")
        return packet

    def compare_hermes_adapter_readiness_replay_to_projection(
        self,
        packet_id: str,
    ) -> HermesAdapterReadinessReplayProjectionComparison:
        projection_row = self.conn.execute(
            "SELECT * FROM hermes_adapter_readiness_packets WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
        if projection_row is None:
            raise ValueError("Hermes adapter readiness packet not found")
        replay = self.__class__._replay_from_connection(self.conn)
        replay_packet = replay.hermes_adapter_readiness_packets.get(packet_id, {})
        projection_packet = _hermes_adapter_readiness_packet_row_payload(projection_row)
        mismatches: list[str] = []
        if replay_packet != projection_packet:
            mismatches.append("hermes_adapter_readiness_packets")
        comparison = HermesAdapterReadinessReplayProjectionComparison(
            packet_id=packet_id,
            replay_packet=replay_packet,
            projection_packet=projection_packet,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _hermes_adapter_readiness_replay_projection_comparison_payload(comparison)
        event_id = self.append_event(
            "hermes_adapter_readiness_replay_projection_compared",
            "policy",
            comparison.comparison_id,
            payload,
            "internal",
        )
        self.conn.execute(
            """
            INSERT INTO hermes_adapter_readiness_replay_projection_comparisons (
              comparison_id, packet_id, replay_packet_json, projection_packet_json,
              matches, mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.packet_id,
                canonical_json(comparison.replay_packet),
                canonical_json(comparison.projection_packet),
                1 if comparison.matches else 0,
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "hermes_adapter_readiness_replay_projection_comparison_projection")
        return comparison

    def _build_hermes_adapter_readiness_packet(
        self,
        *,
        adapter_name: str,
        hermes_version: str,
        as_of: str,
        surface_checks: list[dict[str, Any]],
        reconciliation_checks: list[dict[str, Any]],
        recovery_readiness_packet_id: str | None,
        recovery_readiness_status: str | None,
    ) -> HermesAdapterReadinessPacket:
        surface_by_name = {str(check.get("surface", "")): check for check in surface_checks}
        reconciliation_by_name = {str(check.get("check", "")): check for check in reconciliation_checks}
        missing_surfaces = [
            surface for surface in self.HERMES_V013_REQUIRED_SURFACES if surface not in surface_by_name
        ]
        missing_reconciliation = [
            check for check in self.HERMES_V013_REQUIRED_RECONCILIATION_CHECKS if check not in reconciliation_by_name
        ]
        failing_surfaces = [
            name for name, check in surface_by_name.items() if check.get("status") in {"failed", "blocked"}
        ]
        failing_reconciliation = [
            name for name, check in reconciliation_by_name.items() if check.get("status") in {"failed", "blocked"}
        ]
        pending_surfaces = [
            name for name, check in surface_by_name.items() if check.get("status") != "passed"
        ]
        pending_reconciliation = [
            name for name, check in reconciliation_by_name.items() if check.get("status") != "passed"
        ]

        actions: list[dict[str, Any]] = []
        if recovery_readiness_packet_id is None:
            actions.append(
                {
                    "action": "create_recovery_readiness_packet",
                    "required_authority": "operator_gate",
                    "reason": "Hermes adapter readiness must be tied to current recovery readiness evidence.",
                    "refs": [],
                }
            )
        elif recovery_readiness_status != "ready":
            actions.append(
                {
                    "action": "resolve_recovery_readiness",
                    "required_authority": "operator_gate",
                    "reason": "Hermes attachment remains blocked until recovery readiness is ready.",
                    "refs": [f"kernel:recovery_readiness_packets/{recovery_readiness_packet_id}"],
                }
            )
        if missing_surfaces:
            actions.append(
                {
                    "action": "prove_hermes_adapter_surfaces",
                    "required_authority": "operator_gate",
                    "reason": "One or more Hermes v0.13 adapter surfaces have no proof record.",
                    "refs": missing_surfaces,
                }
            )
        if missing_reconciliation:
            actions.append(
                {
                    "action": "prove_resume_reconciliation_checks",
                    "required_authority": "operator_gate",
                    "reason": "One or more resume/checkpoint reconciliation checks have no proof record.",
                    "refs": missing_reconciliation,
                }
            )
        if failing_surfaces or failing_reconciliation:
            actions.append(
                {
                    "action": "block_live_hermes_attachment",
                    "required_authority": "operator_gate",
                    "reason": "Hermes adapter proof contains failed or blocked checks.",
                    "refs": failing_surfaces + failing_reconciliation,
                }
            )
        elif pending_surfaces or pending_reconciliation:
            actions.append(
                {
                    "action": "complete_adapter_proof_checks",
                    "required_authority": "operator_gate",
                    "reason": "Hermes adapter proof has pending checks before live controls can be considered.",
                    "refs": pending_surfaces + pending_reconciliation,
                }
            )

        fail_closed = (
            recovery_readiness_status == "fail_closed"
            or bool(failing_surfaces)
            or bool(failing_reconciliation)
        )
        readiness_status = "fail_closed" if fail_closed else ("action_required" if actions else "ready")
        evidence_refs = [
            "spec:s08_operator_deployment#Hermes v0.13 Adapter Proofs",
            *[
                str(ref)
                for check in surface_checks + reconciliation_checks
                for ref in check.get("evidence_refs", [])
            ],
        ]
        if recovery_readiness_packet_id is not None:
            evidence_refs.append(f"kernel:recovery_readiness_packets/{recovery_readiness_packet_id}")

        return HermesAdapterReadinessPacket(
            adapter_name=adapter_name,
            hermes_version=hermes_version,
            as_of=as_of,
            surface_checks=surface_checks,
            reconciliation_checks=reconciliation_checks,
            recovery_readiness_packet_id=recovery_readiness_packet_id,
            next_operator_actions=actions,
            readiness_status=readiness_status,  # type: ignore[arg-type]
            evidence_refs=sorted(set(evidence_refs)),
            live_controls_enabled=False,
        )

    def _build_recovery_readiness_packet(self, *, scope: str, as_of: str) -> RecoveryReadinessPacket:
        cadence_rows = self.conn.execute(
            "SELECT * FROM backup_cadence_records WHERE scope=? ORDER BY cadence_id",
            (scope,),
        ).fetchall()
        cadence_ids = {row["cadence_id"] for row in cadence_rows}
        all_drill_rows = self.conn.execute(
            "SELECT * FROM restore_drill_packets ORDER BY scheduled_for, drill_id"
        ).fetchall()
        drill_rows = [row for row in all_drill_rows if row["cadence_id"] in cadence_ids]
        descriptor_rows = self.conn.execute("SELECT * FROM encrypted_storage_descriptors ORDER BY descriptor_id").fetchall()
        receipt_rows = self.conn.execute("SELECT * FROM payload_access_receipts ORDER BY created_at, receipt_id").fetchall()
        recovery_verification_rows = self.conn.execute(
            "SELECT * FROM recovery_verification_states ORDER BY verified_at, verification_id"
        ).fetchall()
        storage_verification_rows = self.conn.execute(
            "SELECT * FROM encrypted_storage_access_verification_states ORDER BY verified_at, verification_id"
        ).fetchall()

        active_cadences = [row for row in cadence_rows if row["status"] == "active"]
        overdue_cadence_ids = sorted(row["cadence_id"] for row in active_cadences if row["next_due_at"] <= as_of)
        cadence_summary = {
            "total": len(cadence_rows),
            "active": len(active_cadences),
            "paused": sum(1 for row in cadence_rows if row["status"] == "paused"),
            "retired": sum(1 for row in cadence_rows if row["status"] == "retired"),
            "overdue_active_cadence_ids": overdue_cadence_ids,
            "next_due_at": min((row["next_due_at"] for row in active_cadences), default=None),
            "encryption_required_for_active": all(bool(row["encryption_required"]) for row in active_cadences),
        }

        latest_drill = drill_rows[-1] if drill_rows else None
        queued_overdue_drill_ids = sorted(
            row["drill_id"] for row in drill_rows if row["status"] == "queued" and row["scheduled_for"] <= as_of
        )
        failed_or_blocked_drill_ids = sorted(
            row["drill_id"] for row in drill_rows if row["status"] in {"failed", "blocked"}
        )
        drill_summary = {
            "total": len(drill_rows),
            "verified": sum(1 for row in drill_rows if row["status"] == "verified"),
            "queued": sum(1 for row in drill_rows if row["status"] == "queued"),
            "failed_or_blocked_drill_ids": failed_or_blocked_drill_ids,
            "queued_overdue_drill_ids": queued_overdue_drill_ids,
            "latest_drill_id": None if latest_drill is None else latest_drill["drill_id"],
            "latest_drill_status": None if latest_drill is None else latest_drill["status"],
        }

        descriptor_status_counts: dict[str, int] = {}
        for row in descriptor_rows:
            descriptor_status_counts[row["status"]] = descriptor_status_counts.get(row["status"], 0) + 1
        backup_payload_descriptor_ids = sorted(
            row["descriptor_id"] for row in descriptor_rows if row["storage_scope"] == "backup_payload"
        )
        inaccessible_descriptor_ids = sorted(
            row["descriptor_id"] for row in descriptor_rows if row["status"] == "inaccessible"
        )
        descriptor_summary = {
            "total": len(descriptor_rows),
            "backup_payload_descriptor_count": len(backup_payload_descriptor_ids),
            "artifact_payload_descriptor_count": sum(
                1 for row in descriptor_rows if row["storage_scope"] == "artifact_payload"
            ),
            "status_counts": descriptor_status_counts,
            "inaccessible_descriptor_ids": inaccessible_descriptor_ids,
            "backup_payload_descriptor_ids": backup_payload_descriptor_ids,
        }

        failed_receipts = [
            row for row in receipt_rows if row["access_result"] != "allowed" or row["verification_status"] in {"failed", "blocked"}
        ]
        access_failure_summary = {
            "failure_count": len(failed_receipts),
            "failed_receipt_ids": [row["receipt_id"] for row in failed_receipts],
            "latest_failure_at": max((row["created_at"] for row in failed_receipts), default=None),
        }

        recovery_fail_closed_ids = sorted(row["verification_id"] for row in recovery_verification_rows if row["fail_closed"])
        storage_fail_closed_ids = sorted(row["verification_id"] for row in storage_verification_rows if row["fail_closed"])
        fail_closed_state = {
            "fail_closed": bool(
                recovery_fail_closed_ids
                or storage_fail_closed_ids
                or inaccessible_descriptor_ids
                or failed_or_blocked_drill_ids
            ),
            "recovery_verification_ids": recovery_fail_closed_ids,
            "encrypted_storage_verification_ids": storage_fail_closed_ids,
            "inaccessible_descriptor_ids": inaccessible_descriptor_ids,
            "failed_or_blocked_drill_ids": failed_or_blocked_drill_ids,
        }

        actions = _recovery_readiness_actions(
            active_cadence_ids=sorted(row["cadence_id"] for row in active_cadences),
            drill_count=len(drill_rows),
            overdue_cadence_ids=overdue_cadence_ids,
            queued_overdue_drill_ids=queued_overdue_drill_ids,
            failed_or_blocked_drill_ids=failed_or_blocked_drill_ids,
            backup_payload_descriptor_ids=backup_payload_descriptor_ids,
            inaccessible_descriptor_ids=inaccessible_descriptor_ids,
            failed_receipt_ids=[row["receipt_id"] for row in failed_receipts],
            recovery_fail_closed_ids=recovery_fail_closed_ids,
            storage_fail_closed_ids=storage_fail_closed_ids,
        )
        readiness_status = "fail_closed" if fail_closed_state["fail_closed"] else ("action_required" if actions else "ready")
        evidence_refs = _recovery_readiness_evidence_refs(
            cadence_rows,
            drill_rows,
            descriptor_rows,
            failed_receipts,
            recovery_verification_rows,
            storage_verification_rows,
        )
        return RecoveryReadinessPacket(
            scope=scope,
            as_of=as_of,
            backup_cadence_summary=cadence_summary,
            restore_drill_summary=drill_summary,
            encrypted_payload_descriptor_summary=descriptor_summary,
            payload_access_failure_summary=access_failure_summary,
            fail_closed_state=fail_closed_state,
            next_operator_actions=actions,
            readiness_status=readiness_status,  # type: ignore[arg-type]
            evidence_refs=evidence_refs,
            live_controls_enabled=False,
        )
