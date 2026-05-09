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
from .replay import KERNEL_POLICY_VERSION, ReplayState
from .store_common import (
    _loads,
    _artifact_governance_payload,
    _artifact_payload_metadata_payload,
    _artifact_lifecycle_task_packet_payload,
    _artifact_ref_row_payload,
    _artifact_payload_metadata_row_payload,
    _artifact_lifecycle_task_packet_row_payload,
    _artifact_lifecycle_replay_projection_comparison_payload,
    _encrypted_storage_descriptor_payload,
    _encrypted_storage_key_rotation_payload,
    _payload_access_receipt_payload,
    _encrypted_storage_access_verification_payload,
    _encrypted_storage_descriptor_row_payload,
    _encrypted_storage_key_rotation_row_payload,
    _payload_access_receipt_row_payload,
    _encrypted_storage_access_verification_row_payload,
    _encrypted_storage_replay_projection_comparison_payload,
)


class ArtifactKernelTransactionMixin:
    def create_artifact_ref(self, artifact: ArtifactRef) -> str:
        if not artifact.artifact_uri.strip():
            raise ValueError("artifact URI is required")
        if not artifact.content_hash.strip():
            raise ValueError("artifact content hash is required")
        if artifact.data_class in {"sensitive", "secret_ref", "regulated", "client_confidential"} and artifact.encryption_status == "unencrypted":
            raise PermissionError("sensitive artifact refs must not be recorded as unencrypted")
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

    def record_artifact_governance(self, record: ArtifactGovernanceRecord) -> str:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("workers cannot record artifact governance outcomes")
        if record.action not in {"retain", "quarantine", "redact", "delete", "crypto_shred"}:
            raise ValueError("unknown artifact governance action")
        if record.status not in {"recorded", "applied", "blocked"}:
            raise ValueError("unknown artifact governance status")
        if not record.reason.strip():
            raise ValueError("artifact governance reason is required")
        if not record.evidence_refs:
            raise ValueError("artifact governance requires evidence references")
        artifact = self.conn.execute(
            """
            SELECT artifact_id, data_class, deletion_policy, encryption_status
            FROM artifact_refs
            WHERE artifact_id=?
            """,
            (record.artifact_id,),
        ).fetchone()
        if artifact is None:
            raise ValueError("artifact governance requires an existing artifact ref")
        sensitive = artifact["data_class"] in {"sensitive", "secret_ref", "regulated", "client_confidential"}
        destructive = record.action in {"redact", "delete", "crypto_shred"}
        if sensitive and destructive and (
            self.command.requested_by != "operator"
            or self.command.requested_authority != "operator_gate"
            or record.required_authority != "operator_gate"
        ):
            raise PermissionError("sensitive artifact redaction or deletion requires operator-gate authority")
        if record.action == "crypto_shred" and artifact["deletion_policy"] != "crypto-shred":
            raise PermissionError("crypto-shred action requires a crypto-shred deletion policy")
        if record.action in {"redact", "delete", "crypto_shred"} and (not record.receipt_ref or not record.receipt_hash):
            raise ValueError("redaction and deletion actions require durable receipt references")
        if artifact["encryption_status"] == "deleted" and record.action not in {"retain", "delete", "crypto_shred"}:
            raise PermissionError("deleted artifact refs cannot receive non-retention governance actions")

        payload = _artifact_governance_payload(record)
        event_id = self.append_event("artifact_governance_recorded", "artifact", record.record_id, payload, artifact["data_class"])
        self.conn.execute(
            """
            INSERT INTO artifact_governance_records (
              record_id, artifact_id, action, reason, required_authority,
              evidence_refs_json, receipt_ref, receipt_hash, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.record_id,
                record.artifact_id,
                record.action,
                record.reason,
                record.required_authority,
                canonical_json(record.evidence_refs),
                record.receipt_ref,
                record.receipt_hash,
                record.status,
                record.created_at,
            ),
        )
        if record.status == "applied" and record.action == "quarantine":
            self.conn.execute("UPDATE artifact_refs SET encryption_status='quarantined' WHERE artifact_id=?", (record.artifact_id,))
        elif record.status == "applied" and record.action in {"delete", "crypto_shred"}:
            self.conn.execute("UPDATE artifact_refs SET encryption_status='deleted' WHERE artifact_id=?", (record.artifact_id,))
        self.enqueue_projection(event_id, "artifact_governance_projection")
        return record.record_id

    def record_artifact_payload_metadata(self, metadata: ArtifactPayloadMetadata) -> str:
        artifact = self.conn.execute(
            """
            SELECT artifact_id, data_class, content_hash, retention_policy,
                   deletion_policy, encryption_status
            FROM artifact_refs
            WHERE artifact_id=?
            """,
            (metadata.artifact_id,),
        ).fetchone()
        if artifact is None:
            raise ValueError("artifact payload metadata requires an existing artifact ref")
        if not metadata.payload_uri.strip():
            raise ValueError("artifact payload URI is required")
        if metadata.size_bytes < 0:
            raise ValueError("artifact payload size must be non-negative")
        if metadata.data_class != artifact["data_class"]:
            raise ValueError("artifact payload data class must match ArtifactRef")
        if metadata.content_hash != artifact["content_hash"]:
            raise ValueError("artifact payload content hash must match ArtifactRef")
        if metadata.retention_policy != artifact["retention_policy"]:
            raise ValueError("artifact payload retention policy must match ArtifactRef")
        if metadata.deletion_policy != artifact["deletion_policy"]:
            raise ValueError("artifact payload deletion policy must match ArtifactRef")
        sensitive = metadata.data_class in {"sensitive", "secret_ref", "regulated", "client_confidential"}
        if sensitive and metadata.encryption_status == "unencrypted":
            raise PermissionError("sensitive artifact payloads must be encrypted")
        if metadata.encryption_status == "encrypted" and not metadata.encryption_key_ref:
            raise ValueError("encrypted artifact payloads require an encryption key reference")
        payload = _artifact_payload_metadata_payload(metadata)
        event_id = self.append_event(
            "artifact_payload_metadata_recorded",
            "artifact",
            metadata.metadata_id,
            payload,
            metadata.data_class,
        )
        self.conn.execute(
            """
            INSERT INTO artifact_payload_metadata (
              metadata_id, artifact_id, payload_uri, storage_backend, data_class,
              content_hash, payload_hash, size_bytes, retention_policy,
              retention_due_at, deletion_policy, encryption_status,
              encryption_key_ref, access_policy_json, legal_hold, status,
              created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                metadata.metadata_id,
                metadata.artifact_id,
                metadata.payload_uri,
                metadata.storage_backend,
                metadata.data_class,
                metadata.content_hash,
                metadata.payload_hash,
                metadata.size_bytes,
                metadata.retention_policy,
                metadata.retention_due_at,
                metadata.deletion_policy,
                metadata.encryption_status,
                metadata.encryption_key_ref,
                canonical_json(metadata.access_policy),
                1 if metadata.legal_hold else 0,
                metadata.status,
                metadata.created_at,
                metadata.updated_at,
            ),
        )
        self.enqueue_projection(event_id, "artifact_payload_metadata_projection")
        return metadata.metadata_id

    def scan_artifact_retention_due(self, as_of: str) -> list[ArtifactLifecycleTaskPacket]:
        if self.command.requested_by not in {"operator", "kernel", "scheduler"}:
            raise PermissionError("artifact retention scans are kernel-owned")
        rows = self.conn.execute(
            """
            SELECT m.*, a.encryption_status AS artifact_encryption_status
            FROM artifact_payload_metadata m
            JOIN artifact_refs a ON a.artifact_id = m.artifact_id
            WHERE m.retention_due_at <= ?
              AND m.legal_hold = 0
              AND m.status IN ('active','quarantined','deletion_due')
              AND NOT EXISTS (
                SELECT 1
                FROM artifact_lifecycle_task_packets p
                WHERE p.metadata_id = m.metadata_id
                  AND p.status = 'queued'
              )
            ORDER BY m.retention_due_at, m.metadata_id
            """,
            (as_of,),
        ).fetchall()
        packets: list[ArtifactLifecycleTaskPacket] = []
        for row in rows:
            data_class = row["data_class"]
            sensitive = data_class in {"sensitive", "secret_ref", "regulated", "client_confidential"}
            if sensitive and row["status"] == "active":
                action = "quarantine"
                required_authority = "rule"
                reason = "Retention due scan quarantined sensitive artifact payload before deletion."
            else:
                action = "crypto_shred" if row["deletion_policy"] == "crypto-shred" else "delete"
                required_authority = "operator_gate" if sensitive else "rule"
                reason = "Retention period ended; artifact payload requires governed lifecycle completion."
            packet = ArtifactLifecycleTaskPacket(
                artifact_id=row["artifact_id"],
                metadata_id=row["metadata_id"],
                action=action,  # type: ignore[arg-type]
                reason=reason,
                due_at=as_of,
                required_authority=required_authority,  # type: ignore[arg-type]
                evidence_refs=[
                    f"kernel:artifact_refs/{row['artifact_id']}",
                    f"kernel:artifact_payload_metadata/{row['metadata_id']}",
                ],
            )
            payload = _artifact_lifecycle_task_packet_payload(packet)
            event_id = self.append_event(
                "artifact_lifecycle_task_packet_created",
                "artifact",
                packet.packet_id,
                payload,
                data_class,
            )
            self.conn.execute(
                """
                INSERT INTO artifact_lifecycle_task_packets (
                  packet_id, artifact_id, metadata_id, action, reason, due_at,
                  required_authority, evidence_refs_json, receipt_required,
                  receipt_ref, receipt_hash, status, created_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    packet.packet_id,
                    packet.artifact_id,
                    packet.metadata_id,
                    packet.action,
                    packet.reason,
                    packet.due_at,
                    packet.required_authority,
                    canonical_json(packet.evidence_refs),
                    1 if packet.receipt_required else 0,
                    packet.receipt_ref,
                    packet.receipt_hash,
                    packet.status,
                    packet.created_at,
                    packet.completed_at,
                ),
            )
            if action in {"delete", "crypto_shred"}:
                self.conn.execute(
                    "UPDATE artifact_payload_metadata SET status='deletion_due', updated_at=? WHERE metadata_id=?",
                    (packet.created_at, packet.metadata_id),
                )
            self.enqueue_projection(event_id, "artifact_lifecycle_task_packet_projection")
            packets.append(packet)
        return packets

    def complete_artifact_lifecycle_task(
        self,
        packet_id: str,
        *,
        receipt_ref: str,
        receipt_hash: str,
        status: str = "completed",
        reason: str | None = None,
    ) -> str:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("workers cannot complete artifact lifecycle task packets")
        if status not in {"completed", "blocked"}:
            raise ValueError("artifact lifecycle completion status must be completed or blocked")
        if not receipt_ref.strip() or not receipt_hash.strip():
            raise ValueError("artifact lifecycle completion requires durable receipt references")
        row = self.conn.execute(
            """
            SELECT p.*, m.data_class, m.deletion_policy
            FROM artifact_lifecycle_task_packets p
            JOIN artifact_payload_metadata m ON m.metadata_id = p.metadata_id
            WHERE p.packet_id=?
            """,
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("artifact lifecycle task packet not found")
        if row["status"] != "queued":
            raise ValueError(f"cannot complete artifact lifecycle task from status {row['status']}")
        sensitive = row["data_class"] in {"sensitive", "secret_ref", "regulated", "client_confidential"}
        destructive = row["action"] in {"delete", "crypto_shred"}
        if sensitive and destructive and (
            self.command.requested_by != "operator"
            or self.command.requested_authority != "operator_gate"
            or row["required_authority"] != "operator_gate"
        ):
            raise PermissionError("sensitive artifact payload deletion requires operator-gate authority")
        if row["action"] == "crypto_shred" and row["deletion_policy"] != "crypto-shred":
            raise PermissionError("crypto-shred lifecycle task requires a crypto-shred deletion policy")
        completed_at = now_iso()
        completion_payload = {
            "packet_id": packet_id,
            "artifact_id": row["artifact_id"],
            "metadata_id": row["metadata_id"],
            "action": row["action"],
            "status": status,
            "reason": reason or row["reason"],
            "receipt_ref": receipt_ref,
            "receipt_hash": receipt_hash,
            "completed_at": completed_at,
        }
        event_id = self.append_event(
            "artifact_lifecycle_task_completed",
            "artifact",
            packet_id,
            completion_payload,
            row["data_class"],
        )
        self.conn.execute(
            """
            UPDATE artifact_lifecycle_task_packets
            SET status=?, receipt_ref=?, receipt_hash=?, completed_at=?
            WHERE packet_id=?
            """,
            (status, receipt_ref, receipt_hash, completed_at, packet_id),
        )
        if status == "completed":
            if row["action"] == "quarantine":
                metadata_status = "quarantined"
                encryption_status = "quarantined"
            elif row["action"] == "delete":
                metadata_status = "deleted"
                encryption_status = "deleted"
            else:
                metadata_status = "crypto_shredded"
                encryption_status = "deleted"
            self.conn.execute(
                """
                UPDATE artifact_payload_metadata
                SET status=?, encryption_status=?, updated_at=?
                WHERE metadata_id=?
                """,
                (metadata_status, encryption_status, completed_at, row["metadata_id"]),
            )
            governance = ArtifactGovernanceRecord(
                artifact_id=row["artifact_id"],
                action=row["action"],
                reason=reason or row["reason"],
                required_authority=row["required_authority"],
                evidence_refs=[
                    *_loads(row["evidence_refs_json"]),
                    f"kernel:artifact_lifecycle_task_packets/{packet_id}",
                ],
                receipt_ref=receipt_ref,
                receipt_hash=receipt_hash,
                status="applied",
                created_at=completed_at,
            )
            self.record_artifact_governance(governance)
        self.enqueue_projection(event_id, "artifact_lifecycle_task_packet_projection")
        return packet_id

    def compare_artifact_lifecycle_replay_to_projection(
        self,
        artifact_id: str,
    ) -> ArtifactLifecycleReplayProjectionComparison:
        projection_artifact = self.conn.execute(
            "SELECT * FROM artifact_refs WHERE artifact_id=?",
            (artifact_id,),
        ).fetchone()
        if projection_artifact is None:
            raise ValueError("artifact ref not found")
        replay = self.__class__._replay_from_connection(self.conn)
        replay_artifact_state = replay.artifact_refs.get(artifact_id, {})
        projection_artifact_state = _artifact_ref_row_payload(projection_artifact)
        projection_metadata_rows = self.conn.execute(
            "SELECT * FROM artifact_payload_metadata WHERE artifact_id=? ORDER BY metadata_id",
            (artifact_id,),
        ).fetchall()
        projection_packet_rows = self.conn.execute(
            "SELECT * FROM artifact_lifecycle_task_packets WHERE artifact_id=? ORDER BY packet_id",
            (artifact_id,),
        ).fetchall()
        replay_payload_metadata = sorted(
            (
                dict(value)
                for value in replay.artifact_payload_metadata.values()
                if value["artifact_id"] == artifact_id
            ),
            key=lambda item: item["metadata_id"],
        )
        projection_payload_metadata = [_artifact_payload_metadata_row_payload(row) for row in projection_metadata_rows]
        replay_task_packets = sorted(
            (
                dict(value)
                for value in replay.artifact_lifecycle_task_packets.values()
                if value["artifact_id"] == artifact_id
            ),
            key=lambda item: item["packet_id"],
        )
        projection_task_packets = [_artifact_lifecycle_task_packet_row_payload(row) for row in projection_packet_rows]
        mismatches: list[str] = []
        if replay_artifact_state != projection_artifact_state:
            mismatches.append("artifact_ref_state")
        if replay_payload_metadata != projection_payload_metadata:
            mismatches.append("artifact_payload_metadata")
        if replay_task_packets != projection_task_packets:
            mismatches.append("artifact_lifecycle_task_packets")
        comparison = ArtifactLifecycleReplayProjectionComparison(
            artifact_id=artifact_id,
            replay_artifact_state=replay_artifact_state,
            projection_artifact_state=projection_artifact_state,
            replay_payload_metadata=replay_payload_metadata,
            projection_payload_metadata=projection_payload_metadata,
            replay_task_packets=replay_task_packets,
            projection_task_packets=projection_task_packets,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _artifact_lifecycle_replay_projection_comparison_payload(comparison)
        event_id = self.append_event(
            "artifact_lifecycle_replay_projection_compared",
            "artifact",
            comparison.comparison_id,
            payload,
            projection_artifact["data_class"],
        )
        self.conn.execute(
            """
            INSERT INTO artifact_lifecycle_replay_projection_comparisons (
              comparison_id, artifact_id, replay_artifact_state_json,
              projection_artifact_state_json, replay_payload_metadata_json,
              projection_payload_metadata_json, replay_task_packets_json,
              projection_task_packets_json, matches, mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.artifact_id,
                canonical_json(comparison.replay_artifact_state),
                canonical_json(comparison.projection_artifact_state),
                canonical_json(comparison.replay_payload_metadata),
                canonical_json(comparison.projection_payload_metadata),
                canonical_json(comparison.replay_task_packets),
                canonical_json(comparison.projection_task_packets),
                1 if comparison.matches else 0,
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "artifact_lifecycle_replay_projection_comparison_projection")
        return comparison

    def record_encrypted_storage_descriptor(self, descriptor: EncryptedStorageDescriptor) -> str:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("encrypted storage descriptors are kernel-owned")
        if descriptor.storage_scope not in {"artifact_payload", "backup_payload"}:
            raise ValueError("unknown encrypted storage scope")
        if not descriptor.descriptor_uri.strip() or not descriptor.local_path_ref.strip():
            raise ValueError("encrypted storage descriptors require URI and local path references")
        if not descriptor.ciphertext_hash.strip() or not descriptor.plaintext_hash.strip():
            raise ValueError("encrypted storage descriptors require ciphertext and plaintext hashes")
        if descriptor.size_bytes < 0:
            raise ValueError("encrypted storage descriptor size must be non-negative")
        if not descriptor.key_ref.strip() or not descriptor.key_version.strip():
            raise ValueError("encrypted storage descriptors require key references")
        if descriptor.storage_scope == "artifact_payload":
            artifact = self.conn.execute(
                "SELECT data_class, retention_policy, deletion_policy FROM artifact_refs WHERE artifact_id=?",
                (descriptor.owner_ref,),
            ).fetchone()
            if artifact is None:
                raise ValueError("artifact payload storage descriptors require an existing artifact ref owner")
            if descriptor.data_class != artifact["data_class"]:
                raise ValueError("encrypted storage descriptor data class must match ArtifactRef")
            if descriptor.retention_policy != artifact["retention_policy"]:
                raise ValueError("encrypted storage descriptor retention policy must match ArtifactRef")
            if descriptor.deletion_policy != artifact["deletion_policy"]:
                raise ValueError("encrypted storage descriptor deletion policy must match ArtifactRef")
        sensitive = descriptor.data_class in {"sensitive", "secret_ref", "regulated", "client_confidential"}
        if sensitive and not descriptor.encryption_algorithm.strip():
            raise PermissionError("sensitive encrypted storage descriptors require an encryption algorithm")
        if not descriptor.evidence_refs:
            raise ValueError("encrypted storage descriptors require evidence references")
        payload = _encrypted_storage_descriptor_payload(descriptor)
        event_id = self.append_event(
            "encrypted_storage_descriptor_recorded",
            "artifact",
            descriptor.descriptor_id,
            payload,
            descriptor.data_class,
        )
        self.conn.execute(
            """
            INSERT INTO encrypted_storage_descriptors (
              descriptor_id, storage_scope, owner_ref, descriptor_uri, storage_backend,
              local_path_ref, data_class, ciphertext_hash, plaintext_hash, size_bytes,
              encryption_algorithm, key_ref, key_version, key_status, access_policy_json,
              retention_policy, deletion_policy, evidence_refs_json, status, created_at,
              updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                descriptor.descriptor_id,
                descriptor.storage_scope,
                descriptor.owner_ref,
                descriptor.descriptor_uri,
                descriptor.storage_backend,
                descriptor.local_path_ref,
                descriptor.data_class,
                descriptor.ciphertext_hash,
                descriptor.plaintext_hash,
                descriptor.size_bytes,
                descriptor.encryption_algorithm,
                descriptor.key_ref,
                descriptor.key_version,
                descriptor.key_status,
                canonical_json(descriptor.access_policy),
                descriptor.retention_policy,
                descriptor.deletion_policy,
                canonical_json(descriptor.evidence_refs),
                descriptor.status,
                descriptor.created_at,
                descriptor.updated_at,
            ),
        )
        self.enqueue_projection(event_id, "encrypted_storage_descriptor_projection")
        return descriptor.descriptor_id

    def record_encrypted_storage_key_rotation(self, rotation: EncryptedStorageKeyRotationRecord) -> str:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("workers cannot rotate encrypted storage key references")
        if not rotation.receipt_ref.strip() or not rotation.receipt_hash.strip():
            raise ValueError("encrypted storage key rotations require durable receipt references")
        if not rotation.evidence_refs:
            raise ValueError("encrypted storage key rotations require evidence references")
        descriptor = self.conn.execute(
            "SELECT * FROM encrypted_storage_descriptors WHERE descriptor_id=?",
            (rotation.descriptor_id,),
        ).fetchone()
        if descriptor is None:
            raise ValueError("encrypted storage key rotation requires an existing descriptor")
        if descriptor["key_ref"] != rotation.old_key_ref or descriptor["key_version"] != rotation.old_key_version:
            raise ValueError("encrypted storage key rotation must start from the current key reference")
        sensitive = descriptor["data_class"] in {"sensitive", "secret_ref", "regulated", "client_confidential"}
        if sensitive and (
            self.command.requested_by != "operator"
            or self.command.requested_authority != "operator_gate"
            or rotation.required_authority != "operator_gate"
        ):
            raise PermissionError("sensitive encrypted storage key rotation requires operator-gate authority")
        payload = _encrypted_storage_key_rotation_payload(rotation)
        event_id = self.append_event(
            "encrypted_storage_key_rotated",
            "artifact",
            rotation.rotation_id,
            payload,
            descriptor["data_class"],
        )
        self.conn.execute(
            """
            INSERT INTO encrypted_storage_key_rotations (
              rotation_id, descriptor_id, old_key_ref, new_key_ref, old_key_version,
              new_key_version, rotation_reason, required_authority, evidence_refs_json,
              receipt_ref, receipt_hash, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rotation.rotation_id,
                rotation.descriptor_id,
                rotation.old_key_ref,
                rotation.new_key_ref,
                rotation.old_key_version,
                rotation.new_key_version,
                rotation.rotation_reason,
                rotation.required_authority,
                canonical_json(rotation.evidence_refs),
                rotation.receipt_ref,
                rotation.receipt_hash,
                rotation.status,
                rotation.created_at,
            ),
        )
        if rotation.status == "applied":
            self.conn.execute(
                """
                UPDATE encrypted_storage_descriptors
                SET key_ref=?, key_version=?, key_status='rotated', status='rotated', updated_at=?
                WHERE descriptor_id=?
                """,
                (rotation.new_key_ref, rotation.new_key_version, rotation.created_at, rotation.descriptor_id),
            )
        self.enqueue_projection(event_id, "encrypted_storage_key_rotation_projection")
        return rotation.rotation_id

    def record_payload_access_receipt(self, receipt: PayloadAccessReceipt) -> str:
        descriptor = self.conn.execute(
            "SELECT * FROM encrypted_storage_descriptors WHERE descriptor_id=?",
            (receipt.descriptor_id,),
        ).fetchone()
        if descriptor is None:
            raise ValueError("payload access receipts require an existing encrypted storage descriptor")
        if not receipt.receipt_ref.strip() or not receipt.receipt_hash.strip():
            raise ValueError("payload access receipts require durable receipt references")
        if not receipt.evidence_refs:
            raise ValueError("payload access receipts require evidence references")
        if receipt.access_result == "allowed":
            if receipt.verification_status != "verified":
                raise PermissionError("payload access fails closed unless verification is verified")
            if descriptor["status"] not in {"active", "rotated"}:
                raise PermissionError("payload access fails closed for inactive encrypted storage descriptors")
            self._assert_payload_access_grant(receipt, descriptor)
        payload = _payload_access_receipt_payload(receipt)
        event_id = self.append_event(
            "payload_access_receipt_recorded",
            "artifact",
            receipt.receipt_id,
            payload,
            descriptor["data_class"],
        )
        self.conn.execute(
            """
            INSERT INTO payload_access_receipts (
              receipt_id, descriptor_id, operation, subject_type, subject_id,
              grant_id, access_result, verification_status, payload_hash,
              receipt_ref, receipt_hash, evidence_refs_json, details_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                receipt.receipt_id,
                receipt.descriptor_id,
                receipt.operation,
                receipt.subject_type,
                receipt.subject_id,
                receipt.grant_id,
                receipt.access_result,
                receipt.verification_status,
                receipt.payload_hash,
                receipt.receipt_ref,
                receipt.receipt_hash,
                canonical_json(receipt.evidence_refs),
                canonical_json(receipt.details),
                receipt.created_at,
            ),
        )
        if receipt.verification_status in {"failed", "blocked"}:
            self.conn.execute(
                "UPDATE encrypted_storage_descriptors SET status='inaccessible', updated_at=? WHERE descriptor_id=?",
                (receipt.created_at, receipt.descriptor_id),
            )
        if receipt.access_result == "allowed" and receipt.grant_id and receipt.subject_type not in {"kernel", "operator"}:
            self._record_payload_access_grant_use(receipt)
        self.enqueue_projection(event_id, "payload_access_receipt_projection")
        return receipt.receipt_id

    def _assert_payload_access_grant(self, receipt: PayloadAccessReceipt, descriptor: sqlite3.Row) -> None:
        access_policy = _loads(descriptor["access_policy_json"])
        allowed_subjects = set(access_policy.get(receipt.operation, []))
        subject_refs = {receipt.subject_type, f"{receipt.subject_type}:{receipt.subject_id}"}
        policy_allows = "*" in allowed_subjects or bool(allowed_subjects & subject_refs)
        if not policy_allows:
            raise PermissionError("payload access subject is not allowed by encrypted storage policy")
        if receipt.subject_type in {"kernel", "operator"}:
            return
        if not receipt.grant_id:
            raise PermissionError("worker payload access requires a live file capability grant")
        grant = self.conn.execute(
            "SELECT * FROM capability_grants WHERE grant_id=?",
            (receipt.grant_id,),
        ).fetchone()
        if grant is None:
            raise PermissionError("payload access grant not found")
        actions = set(_loads(grant["actions_json"]))
        resource = _loads(grant["resource_json"])
        stale_policy = grant["revalidate_on_use"] and grant["policy_version"] != KERNEL_POLICY_VERSION
        exhausted = grant["max_uses"] is not None and grant["used_count"] >= grant["max_uses"]
        expired = grant["expires_at"] <= now_iso()
        valid = (
            grant["status"] == "active"
            and grant["subject_type"] == receipt.subject_type
            and grant["subject_id"] == receipt.subject_id
            and grant["capability_type"] == "file"
            and receipt.operation in actions
            and resource.get("descriptor_id") == receipt.descriptor_id
            and not stale_policy
            and not exhausted
            and not expired
        )
        if not valid:
            raise PermissionError("payload access fails closed without a valid file capability grant")

    def _record_payload_access_grant_use(self, receipt: PayloadAccessReceipt) -> None:
        grant = self.conn.execute(
            "SELECT * FROM capability_grants WHERE grant_id=?",
            (receipt.grant_id,),
        ).fetchone()
        if grant is None:
            raise PermissionError("payload access grant not found")
        event_id = self.append_event(
            "capability_used",
            "capability",
            receipt.grant_id or "",
            {
                "grant_id": receipt.grant_id,
                "subject_type": receipt.subject_type,
                "subject_id": receipt.subject_id,
                "capability_type": "file",
                "action": receipt.operation,
                "used_at": now_iso(),
                "receipt_id": receipt.receipt_id,
                "descriptor_id": receipt.descriptor_id,
            },
        )
        next_used = grant["used_count"] + 1
        next_status = "exhausted" if grant["max_uses"] is not None and next_used >= grant["max_uses"] else "active"
        self.conn.execute(
            "UPDATE capability_grants SET used_count=?, status=? WHERE grant_id=?",
            (next_used, next_status, receipt.grant_id),
        )
        self.enqueue_projection(event_id, "grant_use_projection")

    def record_encrypted_storage_access_verification(
        self,
        verification: EncryptedStorageAccessVerificationState,
    ) -> str:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("encrypted storage access verification is kernel-owned")
        descriptor = self.conn.execute(
            "SELECT data_class FROM encrypted_storage_descriptors WHERE descriptor_id=?",
            (verification.descriptor_id,),
        ).fetchone()
        if descriptor is None:
            raise ValueError("encrypted storage access verification requires an existing descriptor")
        if verification.last_receipt_id:
            receipt = self.conn.execute(
                "SELECT descriptor_id FROM payload_access_receipts WHERE receipt_id=?",
                (verification.last_receipt_id,),
            ).fetchone()
            if receipt is None or receipt["descriptor_id"] != verification.descriptor_id:
                raise ValueError("encrypted storage access verification receipt lineage is invalid")
        all_checks_pass = all(bool(value) for value in verification.verification_checks.values())
        if verification.status == "verified" and (verification.fail_closed or not all_checks_pass or verification.mismatch_summary):
            raise PermissionError("verified encrypted storage access state requires passing checks and no fail-closed flag")
        if verification.status in {"failed", "blocked"} and not verification.fail_closed:
            raise PermissionError("failed encrypted storage access verification must fail closed")
        payload = _encrypted_storage_access_verification_payload(verification)
        event_id = self.append_event(
            "encrypted_storage_access_verification_recorded",
            "artifact",
            verification.verification_id,
            payload,
            descriptor["data_class"],
        )
        self.conn.execute(
            """
            INSERT INTO encrypted_storage_access_verification_states (
              verification_id, descriptor_id, last_receipt_id, status, fail_closed,
              verification_checks_json, mismatch_summary_json, evidence_refs_json,
              verified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verification.verification_id,
                verification.descriptor_id,
                verification.last_receipt_id,
                verification.status,
                1 if verification.fail_closed else 0,
                canonical_json(verification.verification_checks),
                canonical_json(verification.mismatch_summary),
                canonical_json(verification.evidence_refs),
                verification.verified_at,
            ),
        )
        if verification.fail_closed:
            self.conn.execute(
                "UPDATE encrypted_storage_descriptors SET status='inaccessible', updated_at=? WHERE descriptor_id=?",
                (verification.verified_at, verification.descriptor_id),
            )
        self.enqueue_projection(event_id, "encrypted_storage_access_verification_projection")
        return verification.verification_id

    def compare_encrypted_storage_replay_to_projection(
        self,
        descriptor_id: str,
    ) -> EncryptedStorageReplayProjectionComparison:
        projection_descriptor = self.conn.execute(
            "SELECT * FROM encrypted_storage_descriptors WHERE descriptor_id=?",
            (descriptor_id,),
        ).fetchone()
        if projection_descriptor is None:
            raise ValueError("encrypted storage descriptor not found")
        replay = self.__class__._replay_from_connection(self.conn)
        replay_descriptor = replay.encrypted_storage_descriptors.get(descriptor_id, {})
        projection_descriptor_payload = _encrypted_storage_descriptor_row_payload(projection_descriptor)
        projection_rotation_rows = self.conn.execute(
            "SELECT * FROM encrypted_storage_key_rotations WHERE descriptor_id=? ORDER BY rotation_id",
            (descriptor_id,),
        ).fetchall()
        projection_receipt_rows = self.conn.execute(
            "SELECT * FROM payload_access_receipts WHERE descriptor_id=? ORDER BY receipt_id",
            (descriptor_id,),
        ).fetchall()
        projection_verification = self.conn.execute(
            """
            SELECT * FROM encrypted_storage_access_verification_states
            WHERE descriptor_id=?
            ORDER BY verified_at DESC, verification_id DESC
            LIMIT 1
            """,
            (descriptor_id,),
        ).fetchone()
        replay_key_rotations = sorted(
            (
                dict(value)
                for value in replay.encrypted_storage_key_rotations.values()
                if value["descriptor_id"] == descriptor_id
            ),
            key=lambda item: item["rotation_id"],
        )
        replay_access_receipts = sorted(
            (
                dict(value)
                for value in replay.payload_access_receipts.values()
                if value["descriptor_id"] == descriptor_id
            ),
            key=lambda item: item["receipt_id"],
        )
        replay_verification_state = sorted(
            (
                dict(value)
                for value in replay.encrypted_storage_access_verification_states.values()
                if value["descriptor_id"] == descriptor_id
            ),
            key=lambda item: (item["verified_at"], item["verification_id"]),
        )
        replay_verification_payload = replay_verification_state[-1] if replay_verification_state else {}
        projection_key_rotations = [_encrypted_storage_key_rotation_row_payload(row) for row in projection_rotation_rows]
        projection_access_receipts = [_payload_access_receipt_row_payload(row) for row in projection_receipt_rows]
        projection_verification_payload = (
            {}
            if projection_verification is None
            else _encrypted_storage_access_verification_row_payload(projection_verification)
        )
        mismatches: list[str] = []
        if replay_descriptor != projection_descriptor_payload:
            mismatches.append("encrypted_storage_descriptors")
        if replay_key_rotations != projection_key_rotations:
            mismatches.append("encrypted_storage_key_rotations")
        if replay_access_receipts != projection_access_receipts:
            mismatches.append("payload_access_receipts")
        if replay_verification_payload != projection_verification_payload:
            mismatches.append("encrypted_storage_access_verification_states")
        comparison = EncryptedStorageReplayProjectionComparison(
            descriptor_id=descriptor_id,
            replay_descriptor=replay_descriptor,
            projection_descriptor=projection_descriptor_payload,
            replay_key_rotations=replay_key_rotations,
            projection_key_rotations=projection_key_rotations,
            replay_access_receipts=replay_access_receipts,
            projection_access_receipts=projection_access_receipts,
            replay_verification_state=replay_verification_payload,
            projection_verification_state=projection_verification_payload,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _encrypted_storage_replay_projection_comparison_payload(comparison)
        event_id = self.append_event(
            "encrypted_storage_replay_projection_compared",
            "artifact",
            comparison.comparison_id,
            payload,
            projection_descriptor["data_class"],
        )
        self.conn.execute(
            """
            INSERT INTO encrypted_storage_replay_projection_comparisons (
              comparison_id, descriptor_id, replay_descriptor_json,
              projection_descriptor_json, replay_key_rotations_json,
              projection_key_rotations_json, replay_access_receipts_json,
              projection_access_receipts_json, replay_verification_state_json,
              projection_verification_state_json, matches, mismatches_json,
              created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.descriptor_id,
                canonical_json(comparison.replay_descriptor),
                canonical_json(comparison.projection_descriptor),
                canonical_json(comparison.replay_key_rotations),
                canonical_json(comparison.projection_key_rotations),
                canonical_json(comparison.replay_access_receipts),
                canonical_json(comparison.projection_access_receipts),
                canonical_json(comparison.replay_verification_state),
                canonical_json(comparison.projection_verification_state),
                1 if comparison.matches else 0,
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "encrypted_storage_replay_projection_comparison_projection")
        return comparison

