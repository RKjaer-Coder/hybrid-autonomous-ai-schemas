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


def _artifact_governance_payload(record: Any) -> dict[str, Any]:
    return {
        "record_id": record.record_id,
        "artifact_id": record.artifact_id,
        "action": record.action,
        "reason": record.reason,
        "required_authority": record.required_authority,
        "evidence_refs": record.evidence_refs,
        "receipt_ref": record.receipt_ref,
        "receipt_hash": record.receipt_hash,
        "status": record.status,
        "created_at": record.created_at,
    }


def _artifact_payload_metadata_payload(metadata: ArtifactPayloadMetadata) -> dict[str, Any]:
    return {
        "metadata_id": metadata.metadata_id,
        "artifact_id": metadata.artifact_id,
        "payload_uri": metadata.payload_uri,
        "storage_backend": metadata.storage_backend,
        "data_class": metadata.data_class,
        "content_hash": metadata.content_hash,
        "payload_hash": metadata.payload_hash,
        "size_bytes": metadata.size_bytes,
        "retention_policy": metadata.retention_policy,
        "retention_due_at": metadata.retention_due_at,
        "deletion_policy": metadata.deletion_policy,
        "encryption_status": metadata.encryption_status,
        "encryption_key_ref": metadata.encryption_key_ref,
        "access_policy": metadata.access_policy,
        "legal_hold": metadata.legal_hold,
        "status": metadata.status,
        "created_at": metadata.created_at,
        "updated_at": metadata.updated_at,
    }


def _artifact_lifecycle_task_packet_payload(packet: ArtifactLifecycleTaskPacket) -> dict[str, Any]:
    return {
        "packet_id": packet.packet_id,
        "artifact_id": packet.artifact_id,
        "metadata_id": packet.metadata_id,
        "action": packet.action,
        "reason": packet.reason,
        "due_at": packet.due_at,
        "required_authority": packet.required_authority,
        "evidence_refs": packet.evidence_refs,
        "receipt_required": packet.receipt_required,
        "receipt_ref": packet.receipt_ref,
        "receipt_hash": packet.receipt_hash,
        "status": packet.status,
        "created_at": packet.created_at,
        "completed_at": packet.completed_at,
    }


def _artifact_ref_row_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "artifact_id": row["artifact_id"],
        "artifact_uri": row["artifact_uri"],
        "data_class": row["data_class"],
        "content_hash": row["content_hash"],
        "retention_policy": row["retention_policy"],
        "deletion_policy": row["deletion_policy"],
        "encryption_status": row["encryption_status"],
        "source_notes": row["source_notes"],
        "created_at": row["created_at"],
    }


def _artifact_payload_metadata_row_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "metadata_id": row["metadata_id"],
        "artifact_id": row["artifact_id"],
        "payload_uri": row["payload_uri"],
        "storage_backend": row["storage_backend"],
        "data_class": row["data_class"],
        "content_hash": row["content_hash"],
        "payload_hash": row["payload_hash"],
        "size_bytes": row["size_bytes"],
        "retention_policy": row["retention_policy"],
        "retention_due_at": row["retention_due_at"],
        "deletion_policy": row["deletion_policy"],
        "encryption_status": row["encryption_status"],
        "encryption_key_ref": row["encryption_key_ref"],
        "access_policy": _loads(row["access_policy_json"]),
        "legal_hold": bool(row["legal_hold"]),
        "status": row["status"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _artifact_lifecycle_task_packet_row_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "packet_id": row["packet_id"],
        "artifact_id": row["artifact_id"],
        "metadata_id": row["metadata_id"],
        "action": row["action"],
        "reason": row["reason"],
        "due_at": row["due_at"],
        "required_authority": row["required_authority"],
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "receipt_required": bool(row["receipt_required"]),
        "receipt_ref": row["receipt_ref"],
        "receipt_hash": row["receipt_hash"],
        "status": row["status"],
        "created_at": row["created_at"],
        "completed_at": row["completed_at"],
    }


def _artifact_lifecycle_replay_projection_comparison_payload(
    comparison: ArtifactLifecycleReplayProjectionComparison,
) -> dict[str, Any]:
    return {
        "comparison_id": comparison.comparison_id,
        "artifact_id": comparison.artifact_id,
        "replay_artifact_state": comparison.replay_artifact_state,
        "projection_artifact_state": comparison.projection_artifact_state,
        "replay_payload_metadata": comparison.replay_payload_metadata,
        "projection_payload_metadata": comparison.projection_payload_metadata,
        "replay_task_packets": comparison.replay_task_packets,
        "projection_task_packets": comparison.projection_task_packets,
        "matches": comparison.matches,
        "mismatches": comparison.mismatches,
        "created_at": comparison.created_at,
    }


def _encrypted_storage_descriptor_payload(descriptor: EncryptedStorageDescriptor) -> dict[str, Any]:
    return {
        "descriptor_id": descriptor.descriptor_id,
        "storage_scope": descriptor.storage_scope,
        "owner_ref": descriptor.owner_ref,
        "descriptor_uri": descriptor.descriptor_uri,
        "storage_backend": descriptor.storage_backend,
        "local_path_ref": descriptor.local_path_ref,
        "data_class": descriptor.data_class,
        "ciphertext_hash": descriptor.ciphertext_hash,
        "plaintext_hash": descriptor.plaintext_hash,
        "size_bytes": descriptor.size_bytes,
        "encryption_algorithm": descriptor.encryption_algorithm,
        "key_ref": descriptor.key_ref,
        "key_version": descriptor.key_version,
        "key_status": descriptor.key_status,
        "access_policy": descriptor.access_policy,
        "retention_policy": descriptor.retention_policy,
        "deletion_policy": descriptor.deletion_policy,
        "evidence_refs": descriptor.evidence_refs,
        "status": descriptor.status,
        "created_at": descriptor.created_at,
        "updated_at": descriptor.updated_at,
    }


def _encrypted_storage_key_rotation_payload(rotation: EncryptedStorageKeyRotationRecord) -> dict[str, Any]:
    return {
        "rotation_id": rotation.rotation_id,
        "descriptor_id": rotation.descriptor_id,
        "old_key_ref": rotation.old_key_ref,
        "new_key_ref": rotation.new_key_ref,
        "old_key_version": rotation.old_key_version,
        "new_key_version": rotation.new_key_version,
        "rotation_reason": rotation.rotation_reason,
        "required_authority": rotation.required_authority,
        "evidence_refs": rotation.evidence_refs,
        "receipt_ref": rotation.receipt_ref,
        "receipt_hash": rotation.receipt_hash,
        "status": rotation.status,
        "created_at": rotation.created_at,
    }


def _payload_access_receipt_payload(receipt: PayloadAccessReceipt) -> dict[str, Any]:
    return {
        "receipt_id": receipt.receipt_id,
        "descriptor_id": receipt.descriptor_id,
        "operation": receipt.operation,
        "subject_type": receipt.subject_type,
        "subject_id": receipt.subject_id,
        "grant_id": receipt.grant_id,
        "access_result": receipt.access_result,
        "verification_status": receipt.verification_status,
        "payload_hash": receipt.payload_hash,
        "receipt_ref": receipt.receipt_ref,
        "receipt_hash": receipt.receipt_hash,
        "evidence_refs": receipt.evidence_refs,
        "details": receipt.details,
        "created_at": receipt.created_at,
    }


def _encrypted_storage_access_verification_payload(
    verification: EncryptedStorageAccessVerificationState,
) -> dict[str, Any]:
    return {
        "verification_id": verification.verification_id,
        "descriptor_id": verification.descriptor_id,
        "last_receipt_id": verification.last_receipt_id,
        "status": verification.status,
        "fail_closed": verification.fail_closed,
        "verification_checks": verification.verification_checks,
        "mismatch_summary": verification.mismatch_summary,
        "evidence_refs": verification.evidence_refs,
        "verified_at": verification.verified_at,
    }


def _encrypted_storage_descriptor_row_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "descriptor_id": row["descriptor_id"],
        "storage_scope": row["storage_scope"],
        "owner_ref": row["owner_ref"],
        "descriptor_uri": row["descriptor_uri"],
        "storage_backend": row["storage_backend"],
        "local_path_ref": row["local_path_ref"],
        "data_class": row["data_class"],
        "ciphertext_hash": row["ciphertext_hash"],
        "plaintext_hash": row["plaintext_hash"],
        "size_bytes": row["size_bytes"],
        "encryption_algorithm": row["encryption_algorithm"],
        "key_ref": row["key_ref"],
        "key_version": row["key_version"],
        "key_status": row["key_status"],
        "access_policy": _loads(row["access_policy_json"]),
        "retention_policy": row["retention_policy"],
        "deletion_policy": row["deletion_policy"],
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "status": row["status"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _encrypted_storage_key_rotation_row_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "rotation_id": row["rotation_id"],
        "descriptor_id": row["descriptor_id"],
        "old_key_ref": row["old_key_ref"],
        "new_key_ref": row["new_key_ref"],
        "old_key_version": row["old_key_version"],
        "new_key_version": row["new_key_version"],
        "rotation_reason": row["rotation_reason"],
        "required_authority": row["required_authority"],
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "receipt_ref": row["receipt_ref"],
        "receipt_hash": row["receipt_hash"],
        "status": row["status"],
        "created_at": row["created_at"],
    }


def _payload_access_receipt_row_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "receipt_id": row["receipt_id"],
        "descriptor_id": row["descriptor_id"],
        "operation": row["operation"],
        "subject_type": row["subject_type"],
        "subject_id": row["subject_id"],
        "grant_id": row["grant_id"],
        "access_result": row["access_result"],
        "verification_status": row["verification_status"],
        "payload_hash": row["payload_hash"],
        "receipt_ref": row["receipt_ref"],
        "receipt_hash": row["receipt_hash"],
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "details": _loads(row["details_json"]),
        "created_at": row["created_at"],
    }


def _encrypted_storage_access_verification_row_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "verification_id": row["verification_id"],
        "descriptor_id": row["descriptor_id"],
        "last_receipt_id": row["last_receipt_id"],
        "status": row["status"],
        "fail_closed": bool(row["fail_closed"]),
        "verification_checks": _loads(row["verification_checks_json"]),
        "mismatch_summary": _loads(row["mismatch_summary_json"]),
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "verified_at": row["verified_at"],
    }


def _encrypted_storage_replay_projection_comparison_payload(
    comparison: EncryptedStorageReplayProjectionComparison,
) -> dict[str, Any]:
    return {
        "comparison_id": comparison.comparison_id,
        "descriptor_id": comparison.descriptor_id,
        "replay_descriptor": comparison.replay_descriptor,
        "projection_descriptor": comparison.projection_descriptor,
        "replay_key_rotations": comparison.replay_key_rotations,
        "projection_key_rotations": comparison.projection_key_rotations,
        "replay_access_receipts": comparison.replay_access_receipts,
        "projection_access_receipts": comparison.projection_access_receipts,
        "replay_verification_state": comparison.replay_verification_state,
        "projection_verification_state": comparison.projection_verification_state,
        "matches": comparison.matches,
        "mismatches": comparison.mismatches,
        "created_at": comparison.created_at,
    }


def _backup_cadence_record_payload(record: BackupCadenceRecord) -> dict[str, Any]:
    return {
        "cadence_id": record.cadence_id,
        "scope": record.scope,
        "cadence": record.cadence,
        "backup_target": record.backup_target,
        "encryption_required": record.encryption_required,
        "retention_policy": record.retention_policy,
        "recovery_point_objective": record.recovery_point_objective,
        "next_due_at": record.next_due_at,
        "status": record.status,
        "evidence_refs": record.evidence_refs,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


def _restore_drill_packet_payload(packet: RestoreDrillPacket) -> dict[str, Any]:
    return {
        "drill_id": packet.drill_id,
        "cadence_id": packet.cadence_id,
        "backup_ref": packet.backup_ref,
        "backup_manifest_hash": packet.backup_manifest_hash,
        "drill_scope": packet.drill_scope,
        "scheduled_for": packet.scheduled_for,
        "required_authority": packet.required_authority,
        "checklist_items": packet.checklist_items,
        "evidence_refs": packet.evidence_refs,
        "status": packet.status,
        "created_at": packet.created_at,
        "completed_at": packet.completed_at,
    }


def _recovery_checklist_receipt_payload(receipt: RecoveryChecklistReceipt) -> dict[str, Any]:
    return {
        "receipt_id": receipt.receipt_id,
        "drill_id": receipt.drill_id,
        "operator_id": receipt.operator_id,
        "checklist_results": receipt.checklist_results,
        "receipt_ref": receipt.receipt_ref,
        "receipt_hash": receipt.receipt_hash,
        "status": receipt.status,
        "notes": receipt.notes,
        "created_at": receipt.created_at,
    }


def _recovery_verification_state_payload(verification: RecoveryVerificationState) -> dict[str, Any]:
    return {
        "verification_id": verification.verification_id,
        "drill_id": verification.drill_id,
        "cadence_id": verification.cadence_id,
        "receipt_id": verification.receipt_id,
        "backup_manifest_hash": verification.backup_manifest_hash,
        "status": verification.status,
        "fail_closed": verification.fail_closed,
        "verification_checks": verification.verification_checks,
        "mismatch_summary": verification.mismatch_summary,
        "evidence_refs": verification.evidence_refs,
        "verified_at": verification.verified_at,
    }


def _backup_cadence_record_row_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "cadence_id": row["cadence_id"],
        "scope": row["scope"],
        "cadence": row["cadence"],
        "backup_target": row["backup_target"],
        "encryption_required": bool(row["encryption_required"]),
        "retention_policy": row["retention_policy"],
        "recovery_point_objective": row["recovery_point_objective"],
        "next_due_at": row["next_due_at"],
        "status": row["status"],
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _restore_drill_packet_row_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "drill_id": row["drill_id"],
        "cadence_id": row["cadence_id"],
        "backup_ref": row["backup_ref"],
        "backup_manifest_hash": row["backup_manifest_hash"],
        "drill_scope": row["drill_scope"],
        "scheduled_for": row["scheduled_for"],
        "required_authority": row["required_authority"],
        "checklist_items": _loads(row["checklist_items_json"]),
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "status": row["status"],
        "created_at": row["created_at"],
        "completed_at": row["completed_at"],
    }


def _recovery_checklist_receipt_row_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "receipt_id": row["receipt_id"],
        "drill_id": row["drill_id"],
        "operator_id": row["operator_id"],
        "checklist_results": _loads(row["checklist_results_json"]),
        "receipt_ref": row["receipt_ref"],
        "receipt_hash": row["receipt_hash"],
        "status": row["status"],
        "notes": row["notes"],
        "created_at": row["created_at"],
    }


def _recovery_verification_state_row_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "verification_id": row["verification_id"],
        "drill_id": row["drill_id"],
        "cadence_id": row["cadence_id"],
        "receipt_id": row["receipt_id"],
        "backup_manifest_hash": row["backup_manifest_hash"],
        "status": row["status"],
        "fail_closed": bool(row["fail_closed"]),
        "verification_checks": _loads(row["verification_checks_json"]),
        "mismatch_summary": _loads(row["mismatch_summary_json"]),
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "verified_at": row["verified_at"],
    }


def _recovery_replay_projection_comparison_payload(
    comparison: RecoveryReplayProjectionComparison,
) -> dict[str, Any]:
    return {
        "comparison_id": comparison.comparison_id,
        "drill_id": comparison.drill_id,
        "replay_cadence": comparison.replay_cadence,
        "projection_cadence": comparison.projection_cadence,
        "replay_drill_packet": comparison.replay_drill_packet,
        "projection_drill_packet": comparison.projection_drill_packet,
        "replay_checklist_receipts": comparison.replay_checklist_receipts,
        "projection_checklist_receipts": comparison.projection_checklist_receipts,
        "replay_verification_state": comparison.replay_verification_state,
        "projection_verification_state": comparison.projection_verification_state,
        "matches": comparison.matches,
        "mismatches": comparison.mismatches,
        "created_at": comparison.created_at,
    }


def _recovery_readiness_packet_payload(packet: RecoveryReadinessPacket) -> dict[str, Any]:
    return {
        "packet_id": packet.packet_id,
        "scope": packet.scope,
        "as_of": packet.as_of,
        "backup_cadence_summary": packet.backup_cadence_summary,
        "restore_drill_summary": packet.restore_drill_summary,
        "encrypted_payload_descriptor_summary": packet.encrypted_payload_descriptor_summary,
        "payload_access_failure_summary": packet.payload_access_failure_summary,
        "fail_closed_state": packet.fail_closed_state,
        "next_operator_actions": packet.next_operator_actions,
        "readiness_status": packet.readiness_status,
        "evidence_refs": packet.evidence_refs,
        "live_controls_enabled": packet.live_controls_enabled,
        "created_at": packet.created_at,
    }


def _recovery_readiness_packet_row_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "packet_id": row["packet_id"],
        "scope": row["scope"],
        "as_of": row["as_of"],
        "backup_cadence_summary": _loads(row["backup_cadence_summary_json"]),
        "restore_drill_summary": _loads(row["restore_drill_summary_json"]),
        "encrypted_payload_descriptor_summary": _loads(row["encrypted_payload_descriptor_summary_json"]),
        "payload_access_failure_summary": _loads(row["payload_access_failure_summary_json"]),
        "fail_closed_state": _loads(row["fail_closed_state_json"]),
        "next_operator_actions": _loads(row["next_operator_actions_json"]),
        "readiness_status": row["readiness_status"],
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "live_controls_enabled": bool(row["live_controls_enabled"]),
        "created_at": row["created_at"],
    }


def _recovery_readiness_replay_projection_comparison_payload(
    comparison: RecoveryReadinessReplayProjectionComparison,
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


def _recovery_readiness_actions(
    *,
    active_cadence_ids: list[str],
    drill_count: int,
    overdue_cadence_ids: list[str],
    queued_overdue_drill_ids: list[str],
    failed_or_blocked_drill_ids: list[str],
    backup_payload_descriptor_ids: list[str],
    inaccessible_descriptor_ids: list[str],
    failed_receipt_ids: list[str],
    recovery_fail_closed_ids: list[str],
    storage_fail_closed_ids: list[str],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if not active_cadence_ids:
        actions.append(
            {
                "action": "record_active_backup_cadence",
                "required_authority": "operator_gate",
                "reason": "No active encrypted backup cadence exists for the readiness scope.",
                "refs": [],
            }
        )
    if overdue_cadence_ids:
        actions.append(
            {
                "action": "run_due_backup",
                "required_authority": "operator_gate",
                "reason": "One or more active backup cadences are due or overdue.",
                "refs": [f"kernel:backup_cadence_records/{cadence_id}" for cadence_id in overdue_cadence_ids],
            }
        )
    if drill_count == 0 and active_cadence_ids:
        actions.append(
            {
                "action": "schedule_restore_drill_if_none_current",
                "required_authority": "operator_gate",
                "reason": "Operator should keep a current restore drill attached to the active cadence.",
                "refs": [f"kernel:backup_cadence_records/{cadence_id}" for cadence_id in active_cadence_ids],
            }
        )
    if queued_overdue_drill_ids:
        actions.append(
            {
                "action": "complete_restore_drill_checklist",
                "required_authority": "operator_gate",
                "reason": "Queued restore drill packets are scheduled at or before the readiness timestamp.",
                "refs": [f"kernel:restore_drill_packets/{drill_id}" for drill_id in queued_overdue_drill_ids],
            }
        )
    if failed_or_blocked_drill_ids or recovery_fail_closed_ids:
        actions.append(
            {
                "action": "investigate_recovery_verification",
                "required_authority": "operator_gate",
                "reason": "Recovery verification is failed, blocked, or fail-closed.",
                "refs": [
                    *[f"kernel:restore_drill_packets/{drill_id}" for drill_id in failed_or_blocked_drill_ids],
                    *[
                        f"kernel:recovery_verification_states/{verification_id}"
                        for verification_id in recovery_fail_closed_ids
                    ],
                ],
            }
        )
    if not backup_payload_descriptor_ids:
        actions.append(
            {
                "action": "register_backup_payload_descriptor",
                "required_authority": "operator_gate",
                "reason": "No encrypted storage descriptor is registered for backup payloads.",
                "refs": [],
            }
        )
    if inaccessible_descriptor_ids or failed_receipt_ids or storage_fail_closed_ids:
        actions.append(
            {
                "action": "investigate_encrypted_payload_access",
                "required_authority": "operator_gate",
                "reason": "Encrypted payload descriptors or access receipts indicate inaccessible or failed state.",
                "refs": [
                    *[
                        f"kernel:encrypted_storage_descriptors/{descriptor_id}"
                        for descriptor_id in inaccessible_descriptor_ids
                    ],
                    *[f"kernel:payload_access_receipts/{receipt_id}" for receipt_id in failed_receipt_ids],
                    *[
                        f"kernel:encrypted_storage_access_verification_states/{verification_id}"
                        for verification_id in storage_fail_closed_ids
                    ],
                ],
            }
        )
    return actions


def _recovery_readiness_evidence_refs(
    cadence_rows: list[sqlite3.Row],
    drill_rows: list[sqlite3.Row],
    descriptor_rows: list[sqlite3.Row],
    failed_receipts: list[sqlite3.Row],
    recovery_verification_rows: list[sqlite3.Row],
    storage_verification_rows: list[sqlite3.Row],
) -> list[str]:
    refs = [
        *[f"kernel:backup_cadence_records/{row['cadence_id']}" for row in cadence_rows],
        *[f"kernel:restore_drill_packets/{row['drill_id']}" for row in drill_rows],
        *[f"kernel:encrypted_storage_descriptors/{row['descriptor_id']}" for row in descriptor_rows],
        *[f"kernel:payload_access_receipts/{row['receipt_id']}" for row in failed_receipts],
        *[f"kernel:recovery_verification_states/{row['verification_id']}" for row in recovery_verification_rows],
        *[
            f"kernel:encrypted_storage_access_verification_states/{row['verification_id']}"
            for row in storage_verification_rows
        ],
    ]
    return sorted(set(refs))


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
