from __future__ import annotations

import sqlite3
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from kernel import (
    ArtifactGovernanceRecord,
    ArtifactPayloadMetadata,
    ArtifactRef,
    BackupCadenceRecord,
    Budget,
    CapabilityGrant,
    Command,
    EncryptedStorageAccessVerificationState,
    EncryptedStorageDescriptor,
    EncryptedStorageKeyRotationRecord,
    KernelStore,
    PayloadAccessReceipt,
    RecoveryChecklistReceipt,
    RecoveryVerificationState,
    RestoreDrillPacket,
    SideEffectIntent,
    SideEffectReceipt,
    create_kernel_backup,
    restore_kernel_backup,
    verify_kernel_backup,
)
from kernel.records import new_id, payload_hash, sha256_text
from kernel.store import KERNEL_POLICY_VERSION


def command(
    command_type: str,
    key: str,
    payload: dict | None = None,
    *,
    requested_by: str = "operator",
    requested_authority: str | None = None,
) -> Command:
    return Command(
        command_type=command_type,
        requested_by=requested_by,  # type: ignore[arg-type]
        requester_id=requested_by,
        target_entity_type="kernel",
        idempotency_key=key,
        payload=payload or {"key": key},
        requested_authority=requested_authority,  # type: ignore[arg-type]
    )


def future() -> str:
    return "2999-01-01T00:00:00Z"


class KernelFoundationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "kernel.db"
        self.store = KernelStore(self.db_path)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def count(self, table: str) -> int:
        with self.store.connect() as conn:
            return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def create_ready_recovery_readiness_packet(self):
        cadence = BackupCadenceRecord(
            scope="kernel.db",
            cadence="daily",
            backup_target="artifact://local/encrypted-kernel-backups",
            encryption_required=True,
            retention_policy="retain-30d",
            recovery_point_objective="24h",
            next_due_at="2999-01-01T00:00:00Z",
            evidence_refs=["spec:s08_operator_deployment"],
        )
        self.store.record_backup_cadence(command("backup.cadence", f"cadence-{cadence.cadence_id}", requested_by="kernel"), cadence)
        drill = RestoreDrillPacket(
            cadence_id=cadence.cadence_id,
            backup_ref="artifact://local/encrypted-kernel-backups/kernel-ready",
            backup_manifest_hash=sha256_text("ready manifest"),
            drill_scope="kernel.db restore into isolated verification path",
            scheduled_for="2026-05-09T01:00:00Z",
            checklist_items=[{"id": "schema", "label": "Verify schema fidelity"}],
            evidence_refs=[f"kernel:backup_cadence_records/{cadence.cadence_id}"],
        )
        self.store.create_restore_drill_packet(command("backup.drill", f"drill-{drill.drill_id}", requested_by="scheduler"), drill)
        checklist = RecoveryChecklistReceipt(
            drill_id=drill.drill_id,
            operator_id="operator",
            checklist_results=[{"id": "schema", "status": "pass"}],
            receipt_ref="artifact://local/recovery-drills/ready-receipt",
            receipt_hash=sha256_text("ready receipt"),
            status="accepted",
        )
        self.store.record_recovery_checklist_receipt(
            command("backup.recovery_receipt", f"checklist-{checklist.receipt_id}", requested_authority="operator_gate"),
            checklist,
        )
        self.store.record_recovery_verification_state(
            command("backup.recovery_verify", f"verified-{drill.drill_id}", requested_by="kernel"),
            RecoveryVerificationState(
                drill_id=drill.drill_id,
                cadence_id=cadence.cadence_id,
                receipt_id=checklist.receipt_id,
                backup_manifest_hash=drill.backup_manifest_hash,
                status="verified",
                fail_closed=False,
                verification_checks={"schema_fidelity": True, "event_hash_chain": True},
                mismatch_summary=[],
                evidence_refs=[f"kernel:restore_drill_packets/{drill.drill_id}"],
            ),
        )
        self.store.record_encrypted_storage_descriptor(
            command("storage.descriptor", f"descriptor-{drill.drill_id}", requested_by="kernel"),
            EncryptedStorageDescriptor(
                storage_scope="backup_payload",
                owner_ref=drill.backup_ref,
                descriptor_uri="storage://local/backups/kernel-ready",
                storage_backend="local_encrypted_store",
                local_path_ref="/var/lib/hai/backups/kernel-ready.ciphertext",
                data_class="internal",
                ciphertext_hash=sha256_text("ready ciphertext"),
                plaintext_hash=sha256_text("ready plaintext"),
                size_bytes=4096,
                encryption_algorithm="xchacha20-poly1305",
                key_ref="kms://local/backups/kernel-ready/key",
                key_version="v1",
                access_policy={"read": ["kernel"], "write": ["kernel"]},
                retention_policy="retain-30d",
                deletion_policy="crypto-shred",
                evidence_refs=[f"kernel:restore_drill_packets/{drill.drill_id}"],
            ),
        )
        return self.store.create_recovery_readiness_packet(
            command("recovery.readiness", f"ready-{drill.drill_id}", requested_by="kernel"),
            scope="kernel.db",
            as_of="2026-05-10T00:00:00Z",
        )

    def test_event_and_state_commit_atomically(self):
        budget = Budget(
            budget_id=new_id(),
            owner_type="project",
            owner_id=new_id(),
            approved_by="operator",
            cap_usd=Decimal("10"),
            expires_at=future(),
        )

        with self.assertRaises(RuntimeError):
            self.store.execute_command(
                command("budget.create", "atomic-fail"),
                lambda tx: (tx.create_budget(budget), (_ for _ in ()).throw(RuntimeError("boom"))),
            )

        self.assertEqual(self.count("events"), 0)
        self.assertEqual(self.count("budgets"), 0)

        self.store.create_budget(command("budget.create", "atomic-ok"), budget)
        self.assertEqual(self.count("events"), 1)
        self.assertEqual(self.count("budgets"), 1)
        self.assertEqual(self.count("projection_outbox"), 1)

    def test_command_idempotency_returns_existing_result_and_rejects_payload_drift(self):
        budget = Budget(
            budget_id=new_id(),
            owner_type="project",
            owner_id=new_id(),
            approved_by="operator",
            cap_usd=Decimal("10"),
            expires_at=future(),
        )
        first = command("budget.create", "same-command", {"amount": 10})
        self.store.create_budget(first, budget)
        second = command("budget.create", "same-command", {"amount": 10})
        result = self.store.create_budget(second, budget)
        self.assertEqual(result["idempotent"], True)
        self.assertEqual(self.count("events"), 1)
        self.assertEqual(self.count("budgets"), 1)

        with self.assertRaises(ValueError):
            self.store.create_budget(command("budget.create", "same-command", {"amount": 11}), budget)

    def test_grants_fail_closed_for_absent_stale_and_exhausted_grants(self):
        denied = self.store.use_grant(
            command("grant.use", "missing-grant"),
            "missing",
            "agent",
            "worker-1",
            "tool",
            "run",
        )
        self.assertFalse(denied)
        self.assertEqual(self.count("events"), 0)

        grant = CapabilityGrant(
            grant_id=new_id(),
            task_id=new_id(),
            subject_type="agent",
            subject_id="worker-1",
            capability_type="tool",
            actions=["run"],
            resource={"tool": "pytest"},
            scope={},
            conditions={},
            expires_at=future(),
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        self.store.issue_capability_grant(command("grant.issue", "grant-ok"), grant)
        self.assertTrue(
            self.store.use_grant(
                command("grant.use", "grant-use-1"),
                grant.grant_id,
                "agent",
                "worker-1",
                "tool",
                "run",
            )
        )
        self.assertFalse(
            self.store.use_grant(
                command("grant.use", "grant-use-2"),
                grant.grant_id,
                "agent",
                "worker-1",
                "tool",
                "run",
            )
        )

        stale = CapabilityGrant(
            grant_id=new_id(),
            task_id=new_id(),
            subject_type="agent",
            subject_id="worker-2",
            capability_type="network",
            actions=["fetch"],
            resource={"domain": "example.com"},
            scope={},
            conditions={},
            expires_at=future(),
            policy_version="old-policy",
        )
        self.store.issue_capability_grant(command("grant.issue", "grant-stale"), stale)
        self.assertFalse(
            self.store.use_grant(
                command("grant.use", "stale-use"),
                stale.grant_id,
                "agent",
                "worker-2",
                "network",
                "fetch",
            )
        )

    def test_workers_cannot_mint_capability_grants(self):
        for worker in ("agent", "model", "tool"):
            with self.subTest(worker=worker):
                grant = CapabilityGrant(
                    grant_id=new_id(),
                    task_id=new_id(),
                    subject_type="agent",
                    subject_id=f"{worker}-worker",
                    capability_type="network",
                    actions=["fetch"],
                    resource={"domain": "example.com"},
                    scope={},
                    conditions={},
                    expires_at=future(),
                    policy_version=KERNEL_POLICY_VERSION,
                )
                with self.assertRaisesRegex(PermissionError, "workers cannot mint capability grants"):
                    self.store.issue_capability_grant(
                        command("grant.issue", f"{worker}-grant-bypass", requested_by=worker),
                        grant,
                    )

        self.assertEqual(self.count("events"), 0)
        self.assertEqual(self.count("commands"), 0)
        self.assertEqual(self.count("capability_grants"), 0)

    def test_budget_reservation_updates_event_and_state_once(self):
        budget = Budget(
            budget_id=new_id(),
            owner_type="project",
            owner_id=new_id(),
            approved_by="operator",
            cap_usd=Decimal("5.00"),
            expires_at=future(),
        )
        self.store.create_budget(command("budget.create", "budget-for-reserve"), budget)
        reservation_id = self.store.reserve_budget(
            command("budget.reserve", "reserve-1"),
            budget.budget_id,
            Decimal("2.25"),
        )
        self.assertTrue(reservation_id)
        with self.store.connect() as conn:
            row = conn.execute("SELECT reserved_usd FROM budgets WHERE budget_id=?", (budget.budget_id,)).fetchone()
            self.assertEqual(row["reserved_usd"], "2.25")
        replay = self.store.replay_critical_state()
        self.assertEqual(replay.budgets[budget.budget_id]["reserved_usd"], Decimal("2.25"))
        with self.assertRaises(ValueError):
            self.store.reserve_budget(
                command("budget.reserve", "reserve-too-much"),
                budget.budget_id,
                Decimal("3.00"),
            )

    def test_side_effect_intent_receipt_replay_never_reexecutes(self):
        grant = CapabilityGrant(
            grant_id=new_id(),
            task_id=new_id(),
            subject_type="adapter",
            subject_id="side_effect_broker",
            capability_type="side_effect",
            actions=["prepare"],
            resource={"kind": "message"},
            scope={},
            conditions={},
            expires_at=future(),
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=2,
        )
        self.store.issue_capability_grant(command("grant.issue", "side-grant"), grant)
        intent = SideEffectIntent(
            intent_id=new_id(),
            task_id=grant.task_id,
            side_effect_type="message",
            target={"channel": "test"},
            payload_hash=payload_hash({"body": "hello"}),
            required_authority="operator_gate",
            grant_id=grant.grant_id,
            timeout_policy="compensate",
        )
        self.store.prepare_side_effect(command("side.prepare", "side-prepare"), intent)
        receipt = SideEffectReceipt(
            intent_id=intent.intent_id,
            receipt_type="timeout",
            receipt_hash=sha256_text("timeout-receipt"),
            details={"broker": "test", "executed_by_replay": False},
        )
        self.store.record_side_effect_receipt(command("side.receipt", "side-receipt"), receipt)

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.side_effects[intent.intent_id]["intent"]["status"], "prepared")
        self.assertEqual(replay.side_effects[intent.intent_id]["receipt"]["receipt_type"], "timeout")
        self.assertEqual(
            replay.inspection_tasks,
            [{"intent_id": intent.intent_id, "reason": "timeout", "replay_action": "inspect_or_compensate"}],
        )

    def test_workers_cannot_prepare_side_effects_directly(self):
        grant = CapabilityGrant(
            grant_id=new_id(),
            task_id=new_id(),
            subject_type="adapter",
            subject_id="side_effect_broker",
            capability_type="side_effect",
            actions=["prepare"],
            resource={"kind": "message"},
            scope={},
            conditions={},
            expires_at=future(),
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        self.store.issue_capability_grant(command("grant.issue", "worker-side-grant"), grant)
        intent = SideEffectIntent(
            intent_id=new_id(),
            task_id=grant.task_id,
            side_effect_type="message",
            target={"channel": "customer"},
            payload_hash=payload_hash({"body": "hello"}),
            required_authority="operator_gate",
            grant_id=grant.grant_id,
            timeout_policy="ask_operator",
        )

        for worker in ("agent", "model", "tool"):
            with self.subTest(worker=worker):
                with self.assertRaisesRegex(PermissionError, "workers cannot prepare side effects directly"):
                    self.store.prepare_side_effect(
                        command("side.prepare", f"{worker}-side-effect-bypass", requested_by=worker),
                        intent,
                    )

        with self.store.connect() as conn:
            grant_row = conn.execute(
                "SELECT used_count, status FROM capability_grants WHERE grant_id=?",
                (grant.grant_id,),
            ).fetchone()
            commands = [
                row["idempotency_key"]
                for row in conn.execute("SELECT idempotency_key FROM commands ORDER BY submitted_at").fetchall()
            ]
        self.assertEqual(grant_row["used_count"], 0)
        self.assertEqual(grant_row["status"], "active")
        self.assertEqual(commands, ["worker-side-grant"])
        self.assertEqual(self.count("side_effect_intents"), 0)

    def test_artifact_refs_are_governed_records(self):
        artifact = ArtifactRef(
            artifact_uri="artifact://local/test",
            data_class="client_confidential",
            content_hash=sha256_text("content"),
            retention_policy="retain-30d",
            deletion_policy="crypto-shred",
            encryption_status="encrypted",
            source_notes="unit test",
        )
        artifact_id = self.store.execute_command(
            command("artifact.ref", "artifact-ref"),
            lambda tx: tx.create_artifact_ref(artifact),
        )
        self.assertEqual(artifact_id, artifact.artifact_id)
        replay = self.store.replay_critical_state()
        self.assertEqual(replay.artifact_refs[artifact.artifact_id]["data_class"], "client_confidential")

    def test_artifact_governance_records_redaction_and_crypto_shred_receipts(self):
        artifact = ArtifactRef(
            artifact_uri="artifact://local/client-dossier",
            data_class="client_confidential",
            content_hash=sha256_text("client content"),
            retention_policy="retain-30d",
            deletion_policy="crypto-shred",
            encryption_status="encrypted",
            source_notes="client artifact",
        )
        artifact_id = self.store.execute_command(
            command("artifact.ref", "artifact-client-dossier"),
            lambda tx: tx.create_artifact_ref(artifact),
        )
        redaction = ArtifactGovernanceRecord(
            artifact_id=artifact_id,
            action="redact",
            reason="Remove client identifying details before evidence sharing.",
            required_authority="operator_gate",
            evidence_refs=[f"kernel:artifact_refs/{artifact_id}", "operator://redaction-review/1"],
            receipt_ref="artifact://local/client-dossier/redaction-receipt",
            receipt_hash=sha256_text("redacted"),
        )
        redaction_id = self.store.record_artifact_governance(
            command("artifact.governance", "artifact-redaction", requested_authority="operator_gate"),
            redaction,
        )
        shred = ArtifactGovernanceRecord(
            artifact_id=artifact_id,
            action="crypto_shred",
            reason="Retention period ended and deletion policy requires crypto-shredding.",
            required_authority="operator_gate",
            evidence_refs=[f"kernel:artifact_governance_records/{redaction_id}"],
            receipt_ref="kms://local/key/client-dossier/shredded",
            receipt_hash=sha256_text("key destroyed"),
        )
        shred_id = self.store.record_artifact_governance(
            command("artifact.governance", "artifact-shred", requested_authority="operator_gate"),
            shred,
        )

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.artifact_governance_records[redaction_id]["action"], "redact")
        self.assertEqual(replay.artifact_governance_records[shred_id]["action"], "crypto_shred")
        self.assertEqual(replay.artifact_refs[artifact_id]["encryption_status"], "deleted")
        with self.store.connect() as conn:
            row = conn.execute("SELECT encryption_status FROM artifact_refs WHERE artifact_id=?", (artifact_id,)).fetchone()
        self.assertEqual(row["encryption_status"], "deleted")

    def test_artifact_governance_fails_closed_for_sensitive_worker_or_missing_receipts(self):
        artifact = ArtifactRef(
            artifact_uri="artifact://local/secret",
            data_class="secret_ref",
            content_hash=sha256_text("secret pointer"),
            retention_policy="retain-7d",
            deletion_policy="crypto-shred",
            encryption_status="encrypted",
        )
        artifact_id = self.store.execute_command(
            command("artifact.ref", "artifact-secret"),
            lambda tx: tx.create_artifact_ref(artifact),
        )
        record = ArtifactGovernanceRecord(
            artifact_id=artifact_id,
            action="delete",
            reason="Delete secret pointer.",
            required_authority="operator_gate",
            evidence_refs=[f"kernel:artifact_refs/{artifact_id}"],
            receipt_ref="kms://receipt",
            receipt_hash=sha256_text("deleted"),
        )
        with self.assertRaises(PermissionError):
            self.store.record_artifact_governance(
                command(
                    "artifact.governance",
                    "artifact-worker-delete",
                    requested_by="agent",
                    requested_authority="operator_gate",
                ),
                record,
            )
        missing_receipt = ArtifactGovernanceRecord(
            artifact_id=artifact_id,
            action="delete",
            reason="Delete secret pointer.",
            required_authority="operator_gate",
            evidence_refs=[f"kernel:artifact_refs/{artifact_id}"],
        )
        with self.assertRaises(ValueError):
            self.store.record_artifact_governance(
                command("artifact.governance", "artifact-missing-receipt", requested_authority="operator_gate"),
                missing_receipt,
            )

    def test_artifact_payload_retention_scan_and_receipt_required_crypto_shred(self):
        artifact = ArtifactRef(
            artifact_uri="artifact://local/payload-client-dossier",
            data_class="client_confidential",
            content_hash=sha256_text("payload content"),
            retention_policy="retain-until-2026-05-01",
            deletion_policy="crypto-shred",
            encryption_status="encrypted",
        )
        artifact_id = self.store.execute_command(
            command("artifact.ref", "payload-lifecycle-artifact"),
            lambda tx: tx.create_artifact_ref(artifact),
        )
        metadata = ArtifactPayloadMetadata(
            artifact_id=artifact_id,
            payload_uri="artifact://local/payload-client-dossier/blob",
            storage_backend="local_encrypted_store",
            data_class="client_confidential",
            content_hash=artifact.content_hash,
            payload_hash=payload_hash({"blob": "payload content"}),
            size_bytes=128,
            retention_policy=artifact.retention_policy,
            retention_due_at="2026-05-01T00:00:00Z",
            deletion_policy=artifact.deletion_policy,
            encryption_status="encrypted",
            encryption_key_ref="kms://local/client-dossier/key",
            access_policy={"read": ["kernel"], "external_side_effects": False},
        )
        metadata_id = self.store.record_artifact_payload_metadata(
            command("artifact.payload", "payload-lifecycle-metadata"),
            metadata,
        )

        packets = self.store.scan_artifact_retention_due(
            command("artifact.retention_scan", "payload-lifecycle-scan", requested_by="scheduler"),
            "2026-05-09T00:00:00Z",
        )
        self.assertEqual(len(packets), 1)
        self.assertEqual(packets[0].action, "quarantine")
        self.assertEqual(packets[0].required_authority, "rule")
        self.assertEqual(
            self.store.scan_artifact_retention_due(
                command("artifact.retention_scan", "payload-lifecycle-scan-idempotent", requested_by="scheduler"),
                "2026-05-09T00:00:00Z",
            ),
            [],
        )

        with self.assertRaises(ValueError):
            self.store.complete_artifact_lifecycle_task(
                command("artifact.lifecycle_complete", "payload-lifecycle-missing-receipt"),
                packets[0].packet_id,
                receipt_ref="",
                receipt_hash="",
            )
        self.store.complete_artifact_lifecycle_task(
            command("artifact.lifecycle_complete", "payload-lifecycle-quarantine"),
            packets[0].packet_id,
            receipt_ref="artifact://local/payload-client-dossier/quarantine-receipt",
            receipt_hash=sha256_text("quarantined"),
        )
        delete_packets = self.store.scan_artifact_retention_due(
            command("artifact.retention_scan", "payload-lifecycle-delete-scan", requested_by="scheduler"),
            "2026-05-10T00:00:00Z",
        )
        self.assertEqual(len(delete_packets), 1)
        self.assertEqual(delete_packets[0].action, "crypto_shred")
        self.assertEqual(delete_packets[0].required_authority, "operator_gate")
        with self.assertRaises(PermissionError):
            self.store.complete_artifact_lifecycle_task(
                command("artifact.lifecycle_complete", "payload-lifecycle-worker-delete", requested_by="scheduler"),
                delete_packets[0].packet_id,
                receipt_ref="kms://local/client-dossier/key/shred-receipt",
                receipt_hash=sha256_text("key destroyed"),
            )
        self.store.complete_artifact_lifecycle_task(
            command(
                "artifact.lifecycle_complete",
                "payload-lifecycle-crypto-shred",
                requested_authority="operator_gate",
            ),
            delete_packets[0].packet_id,
            receipt_ref="kms://local/client-dossier/key/shred-receipt",
            receipt_hash=sha256_text("key destroyed"),
        )

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.artifact_refs[artifact_id]["encryption_status"], "deleted")
        self.assertEqual(replay.artifact_payload_metadata[metadata_id]["status"], "crypto_shredded")
        self.assertEqual(
            replay.artifact_lifecycle_task_packets[delete_packets[0].packet_id]["receipt_hash"],
            sha256_text("key destroyed"),
        )

    def test_artifact_payload_metadata_fails_closed_for_unencrypted_sensitive_payloads_and_legal_hold(self):
        artifact = ArtifactRef(
            artifact_uri="artifact://local/legal-hold-secret",
            data_class="secret_ref",
            content_hash=sha256_text("secret payload"),
            retention_policy="retain-until-2026-05-01",
            deletion_policy="crypto-shred",
            encryption_status="encrypted",
        )
        artifact_id = self.store.execute_command(
            command("artifact.ref", "legal-hold-artifact"),
            lambda tx: tx.create_artifact_ref(artifact),
        )
        with self.assertRaises(PermissionError):
            self.store.record_artifact_payload_metadata(
                command("artifact.payload", "legal-hold-unencrypted"),
                ArtifactPayloadMetadata(
                    artifact_id=artifact_id,
                    payload_uri="artifact://local/legal-hold-secret/blob",
                    storage_backend="local_store",
                    data_class="secret_ref",
                    content_hash=artifact.content_hash,
                    payload_hash=payload_hash({"blob": "secret"}),
                    size_bytes=12,
                    retention_policy=artifact.retention_policy,
                    retention_due_at="2026-05-01T00:00:00Z",
                    deletion_policy=artifact.deletion_policy,
                    encryption_status="unencrypted",
                    encryption_key_ref=None,
                    access_policy={"read": ["kernel"]},
                ),
            )
        metadata = ArtifactPayloadMetadata(
            artifact_id=artifact_id,
            payload_uri="artifact://local/legal-hold-secret/blob",
            storage_backend="local_encrypted_store",
            data_class="secret_ref",
            content_hash=artifact.content_hash,
            payload_hash=payload_hash({"blob": "secret"}),
            size_bytes=12,
            retention_policy=artifact.retention_policy,
            retention_due_at="2026-05-01T00:00:00Z",
            deletion_policy=artifact.deletion_policy,
            encryption_status="encrypted",
            encryption_key_ref="kms://local/legal-hold/key",
            access_policy={"read": ["kernel"]},
            legal_hold=True,
        )
        self.store.record_artifact_payload_metadata(command("artifact.payload", "legal-hold-metadata"), metadata)
        packets = self.store.scan_artifact_retention_due(
            command("artifact.retention_scan", "legal-hold-scan", requested_by="kernel"),
            "2026-05-09T00:00:00Z",
        )
        self.assertEqual(packets, [])

    def test_artifact_lifecycle_replay_projection_comparison_detects_drift(self):
        artifact = ArtifactRef(
            artifact_uri="artifact://local/replay-drift",
            data_class="internal",
            content_hash=sha256_text("internal payload"),
            retention_policy="retain-until-2026-05-01",
            deletion_policy="delete",
            encryption_status="encrypted",
        )
        artifact_id = self.store.execute_command(
            command("artifact.ref", "lifecycle-replay-artifact"),
            lambda tx: tx.create_artifact_ref(artifact),
        )
        metadata = ArtifactPayloadMetadata(
            artifact_id=artifact_id,
            payload_uri="artifact://local/replay-drift/blob",
            storage_backend="local_encrypted_store",
            data_class="internal",
            content_hash=artifact.content_hash,
            payload_hash=payload_hash({"blob": "internal"}),
            size_bytes=64,
            retention_policy=artifact.retention_policy,
            retention_due_at="2026-05-01T00:00:00Z",
            deletion_policy=artifact.deletion_policy,
            encryption_status="encrypted",
            encryption_key_ref="kms://local/internal/key",
            access_policy={"read": ["kernel"]},
        )
        self.store.record_artifact_payload_metadata(command("artifact.payload", "lifecycle-replay-metadata"), metadata)
        packet = self.store.scan_artifact_retention_due(
            command("artifact.retention_scan", "lifecycle-replay-scan", requested_by="scheduler"),
            "2026-05-09T00:00:00Z",
        )[0]
        self.store.complete_artifact_lifecycle_task(
            command("artifact.lifecycle_complete", "lifecycle-replay-delete"),
            packet.packet_id,
            receipt_ref="artifact://local/replay-drift/delete-receipt",
            receipt_hash=sha256_text("deleted"),
        )

        comparison = self.store.compare_artifact_lifecycle_replay_to_projection(
            command("artifact.lifecycle_compare", "lifecycle-replay-compare"),
            artifact_id,
        )
        self.assertTrue(comparison.matches)
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE artifact_payload_metadata SET status='active' WHERE metadata_id=?",
                (metadata.metadata_id,),
            )
        drift = self.store.compare_artifact_lifecycle_replay_to_projection(
            command("artifact.lifecycle_compare", "lifecycle-replay-drift"),
            artifact_id,
        )
        self.assertFalse(drift.matches)
        self.assertIn("artifact_payload_metadata", drift.mismatches)

    def test_encrypted_storage_descriptors_key_rotation_and_payload_access_are_replayable(self):
        artifact = ArtifactRef(
            artifact_uri="artifact://local/encrypted-payload",
            data_class="client_confidential",
            content_hash=sha256_text("plaintext"),
            retention_policy="retain-30d",
            deletion_policy="crypto-shred",
            encryption_status="encrypted",
        )
        artifact_id = self.store.execute_command(
            command("artifact.ref", "encrypted-storage-artifact"),
            lambda tx: tx.create_artifact_ref(artifact),
        )
        descriptor = EncryptedStorageDescriptor(
            storage_scope="artifact_payload",
            owner_ref=artifact_id,
            descriptor_uri="storage://local/artifacts/encrypted-payload",
            storage_backend="local_encrypted_store",
            local_path_ref="/var/lib/hai/artifacts/encrypted-payload.ciphertext",
            data_class="client_confidential",
            ciphertext_hash=sha256_text("ciphertext-v1"),
            plaintext_hash=artifact.content_hash,
            size_bytes=256,
            encryption_algorithm="xchacha20-poly1305",
            key_ref="kms://local/artifacts/encrypted-payload/key",
            key_version="v1",
            access_policy={"read": ["kernel", "agent:reader"], "write": ["kernel"]},
            retention_policy=artifact.retention_policy,
            deletion_policy=artifact.deletion_policy,
            evidence_refs=[f"kernel:artifact_refs/{artifact_id}"],
        )
        descriptor_id = self.store.record_encrypted_storage_descriptor(
            command("storage.descriptor", "encrypted-storage-descriptor", requested_by="kernel"),
            descriptor,
        )
        rotation = EncryptedStorageKeyRotationRecord(
            descriptor_id=descriptor_id,
            old_key_ref=descriptor.key_ref,
            new_key_ref="kms://local/artifacts/encrypted-payload/key-2",
            old_key_version="v1",
            new_key_version="v2",
            rotation_reason="Scheduled local key-reference rotation.",
            required_authority="operator_gate",
            evidence_refs=[f"kernel:encrypted_storage_descriptors/{descriptor_id}"],
            receipt_ref="kms://local/artifacts/encrypted-payload/key-2/receipt",
            receipt_hash=sha256_text("rotated"),
        )
        self.store.record_encrypted_storage_key_rotation(
            command("storage.rotate", "encrypted-storage-rotation", requested_authority="operator_gate"),
            rotation,
        )
        grant = CapabilityGrant(
            task_id=new_id(),
            subject_type="agent",
            subject_id="reader",
            capability_type="file",
            actions=["read"],
            resource={"descriptor_id": descriptor_id},
            scope={"local_path_ref": descriptor.local_path_ref},
            conditions={"receipt_required": True},
            expires_at=future(),
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        self.store.issue_capability_grant(command("grant.issue", "encrypted-storage-read-grant"), grant)
        receipt = PayloadAccessReceipt(
            descriptor_id=descriptor_id,
            operation="read",
            subject_type="agent",
            subject_id="reader",
            grant_id=grant.grant_id,
            access_result="allowed",
            verification_status="verified",
            payload_hash=descriptor.plaintext_hash,
            receipt_ref="storage://local/artifacts/encrypted-payload/read-receipt",
            receipt_hash=sha256_text("read receipt"),
            evidence_refs=[
                f"kernel:encrypted_storage_descriptors/{descriptor_id}",
                f"kernel:capability_grants/{grant.grant_id}",
            ],
        )
        receipt_id = self.store.record_payload_access_receipt(
            command("storage.access", "encrypted-storage-read", requested_by="agent"),
            receipt,
        )
        verification = EncryptedStorageAccessVerificationState(
            descriptor_id=descriptor_id,
            last_receipt_id=receipt_id,
            status="verified",
            fail_closed=False,
            verification_checks={
                "descriptor_exists": True,
                "grant_verified": True,
                "receipt_hash_present": True,
                "payload_hash_matches": True,
            },
            mismatch_summary=[],
            evidence_refs=[f"kernel:payload_access_receipts/{receipt_id}"],
        )
        self.store.record_encrypted_storage_access_verification(
            command("storage.verify", "encrypted-storage-verify", requested_by="kernel"),
            verification,
        )

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.encrypted_storage_descriptors[descriptor_id]["key_ref"], rotation.new_key_ref)
        self.assertEqual(replay.encrypted_storage_descriptors[descriptor_id]["status"], "rotated")
        self.assertEqual(replay.payload_access_receipts[receipt_id]["access_result"], "allowed")
        self.assertFalse(
            replay.encrypted_storage_access_verification_states[verification.verification_id]["fail_closed"]
        )
        comparison = self.store.compare_encrypted_storage_replay_to_projection(
            command("storage.compare", "encrypted-storage-compare", requested_by="kernel"),
            descriptor_id,
        )
        self.assertTrue(comparison.matches)

    def test_encrypted_storage_access_fails_closed_without_grant_or_after_failed_verification(self):
        descriptor = EncryptedStorageDescriptor(
            storage_scope="backup_payload",
            owner_ref="backup://kernel.db/2026-05-09",
            descriptor_uri="storage://local/backups/kernel-2026-05-09",
            storage_backend="local_encrypted_store",
            local_path_ref="/var/lib/hai/backups/kernel-2026-05-09.ciphertext",
            data_class="internal",
            ciphertext_hash=sha256_text("backup ciphertext"),
            plaintext_hash=sha256_text("backup plaintext"),
            size_bytes=4096,
            encryption_algorithm="xchacha20-poly1305",
            key_ref="kms://local/backups/kernel/key",
            key_version="v1",
            access_policy={"read": ["agent:backup-reader"], "write": ["kernel"]},
            retention_policy="retain-30d",
            deletion_policy="crypto-shred",
            evidence_refs=["kernel:backup_manifest/kernel-2026-05-09"],
        )
        descriptor_id = self.store.record_encrypted_storage_descriptor(
            command("storage.descriptor", "backup-storage-descriptor", requested_by="kernel"),
            descriptor,
        )
        with self.assertRaises(PermissionError):
            self.store.record_payload_access_receipt(
                command("storage.access", "backup-read-without-grant", requested_by="agent"),
                PayloadAccessReceipt(
                    descriptor_id=descriptor_id,
                    operation="read",
                    subject_type="agent",
                    subject_id="backup-reader",
                    access_result="allowed",
                    verification_status="verified",
                    payload_hash=descriptor.plaintext_hash,
                    receipt_ref="storage://local/backups/read-receipt",
                    receipt_hash=sha256_text("read"),
                    evidence_refs=[f"kernel:encrypted_storage_descriptors/{descriptor_id}"],
                ),
            )
        failed_receipt = PayloadAccessReceipt(
            descriptor_id=descriptor_id,
            operation="read",
            subject_type="agent",
            subject_id="backup-reader",
            access_result="blocked",
            verification_status="failed",
            payload_hash=descriptor.plaintext_hash,
            receipt_ref="storage://local/backups/blocked-read",
            receipt_hash=sha256_text("blocked"),
            evidence_refs=[f"kernel:encrypted_storage_descriptors/{descriptor_id}"],
            details={"reason": "ciphertext hash mismatch"},
        )
        failed_receipt_id = self.store.record_payload_access_receipt(
            command("storage.access", "backup-blocked-read", requested_by="agent"),
            failed_receipt,
        )
        failed_state = EncryptedStorageAccessVerificationState(
            descriptor_id=descriptor_id,
            last_receipt_id=failed_receipt_id,
            status="failed",
            fail_closed=True,
            verification_checks={"ciphertext_hash_matches": False, "receipt_hash_present": True},
            mismatch_summary=["ciphertext_hash_matches"],
            evidence_refs=[f"kernel:payload_access_receipts/{failed_receipt_id}"],
        )
        self.store.record_encrypted_storage_access_verification(
            command("storage.verify", "backup-failed-verify", requested_by="kernel"),
            failed_state,
        )
        replay = self.store.replay_critical_state()
        self.assertEqual(replay.encrypted_storage_descriptors[descriptor_id]["status"], "inaccessible")
        with self.assertRaises(PermissionError):
            self.store.record_payload_access_receipt(
                command("storage.access", "backup-read-after-fail", requested_by="operator"),
                PayloadAccessReceipt(
                    descriptor_id=descriptor_id,
                    operation="read",
                    subject_type="operator",
                    subject_id="operator",
                    access_result="allowed",
                    verification_status="verified",
                    payload_hash=descriptor.plaintext_hash,
                    receipt_ref="storage://local/backups/operator-read",
                    receipt_hash=sha256_text("operator read"),
                    evidence_refs=[f"kernel:encrypted_storage_descriptors/{descriptor_id}"],
                ),
            )

    def test_encrypted_storage_replay_projection_comparison_detects_drift(self):
        descriptor = EncryptedStorageDescriptor(
            storage_scope="backup_payload",
            owner_ref="backup://kernel.db/drift",
            descriptor_uri="storage://local/backups/drift",
            storage_backend="local_encrypted_store",
            local_path_ref="/var/lib/hai/backups/drift.ciphertext",
            data_class="internal",
            ciphertext_hash=sha256_text("drift ciphertext"),
            plaintext_hash=sha256_text("drift plaintext"),
            size_bytes=128,
            encryption_algorithm="xchacha20-poly1305",
            key_ref="kms://local/backups/drift/key",
            key_version="v1",
            access_policy={"read": ["kernel"], "write": ["kernel"]},
            retention_policy="retain-7d",
            deletion_policy="delete",
            evidence_refs=["kernel:backup_manifest/drift"],
        )
        descriptor_id = self.store.record_encrypted_storage_descriptor(
            command("storage.descriptor", "storage-drift-descriptor", requested_by="kernel"),
            descriptor,
        )
        comparison = self.store.compare_encrypted_storage_replay_to_projection(
            command("storage.compare", "storage-drift-clean", requested_by="kernel"),
            descriptor_id,
        )
        self.assertTrue(comparison.matches)
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE encrypted_storage_descriptors SET status='inaccessible' WHERE descriptor_id=?",
                (descriptor_id,),
            )
        drift = self.store.compare_encrypted_storage_replay_to_projection(
            command("storage.compare", "storage-drift-detected", requested_by="kernel"),
            descriptor_id,
        )
        self.assertFalse(drift.matches)
        self.assertIn("encrypted_storage_descriptors", drift.mismatches)

    def test_backup_cadence_restore_drill_and_recovery_verification_are_replayable(self):
        cadence = BackupCadenceRecord(
            scope="kernel.db",
            cadence="daily",
            backup_target="artifact://local/encrypted-kernel-backups",
            encryption_required=True,
            retention_policy="retain-30d",
            recovery_point_objective="24h",
            next_due_at="2026-05-10T00:00:00Z",
            evidence_refs=["spec:s08_operator_deployment"],
        )
        self.store.record_backup_cadence(command("backup.cadence", "recovery-cadence", requested_by="kernel"), cadence)
        packet = RestoreDrillPacket(
            cadence_id=cadence.cadence_id,
            backup_ref="artifact://local/encrypted-kernel-backups/kernel-2026-05-09",
            backup_manifest_hash=sha256_text("manifest"),
            drill_scope="kernel.db restore into isolated verification path",
            scheduled_for="2026-05-10T01:00:00Z",
            checklist_items=[
                {"id": "schema", "label": "Verify schema fidelity"},
                {"id": "hash_chain", "label": "Verify event hash chain"},
                {"id": "receipts", "label": "Confirm governed receipts survived"},
            ],
            evidence_refs=[f"kernel:backup_cadence_records/{cadence.cadence_id}"],
        )
        self.store.create_restore_drill_packet(command("backup.drill", "recovery-drill", requested_by="scheduler"), packet)
        receipt = RecoveryChecklistReceipt(
            drill_id=packet.drill_id,
            operator_id="operator",
            checklist_results=[
                {"id": "schema", "status": "pass"},
                {"id": "hash_chain", "status": "pass"},
                {"id": "receipts", "status": "pass"},
            ],
            receipt_ref="artifact://local/recovery-drills/receipt",
            receipt_hash=sha256_text("operator checklist receipt"),
            status="accepted",
        )
        self.store.record_recovery_checklist_receipt(
            command(
                "backup.recovery_receipt",
                "recovery-checklist-receipt",
                requested_authority="operator_gate",
            ),
            receipt,
        )
        verification = RecoveryVerificationState(
            drill_id=packet.drill_id,
            cadence_id=cadence.cadence_id,
            receipt_id=receipt.receipt_id,
            backup_manifest_hash=packet.backup_manifest_hash,
            status="verified",
            fail_closed=False,
            verification_checks={
                "schema_fidelity": True,
                "event_hash_chain": True,
                "governed_receipts": True,
                "restore_copy_verified": True,
            },
            mismatch_summary=[],
            evidence_refs=[
                f"kernel:restore_drill_packets/{packet.drill_id}",
                f"kernel:recovery_checklist_receipts/{receipt.receipt_id}",
            ],
        )
        self.store.record_recovery_verification_state(
            command("backup.recovery_verify", "recovery-verification", requested_by="kernel"),
            verification,
        )

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.backup_cadence_records[cadence.cadence_id]["cadence"], "daily")
        self.assertEqual(replay.restore_drill_packets[packet.drill_id]["status"], "verified")
        self.assertEqual(replay.recovery_checklist_receipts[receipt.receipt_id]["receipt_hash"], receipt.receipt_hash)
        self.assertFalse(replay.recovery_verification_states[verification.verification_id]["fail_closed"])
        comparison = self.store.compare_recovery_replay_to_projection(
            command("backup.recovery_compare", "recovery-replay-compare", requested_by="kernel"),
            packet.drill_id,
        )
        self.assertTrue(comparison.matches)

        with self.store.connect() as conn:
            conn.execute(
                "UPDATE recovery_verification_states SET fail_closed=1 WHERE verification_id=?",
                (verification.verification_id,),
            )
        drift = self.store.compare_recovery_replay_to_projection(
            command("backup.recovery_compare", "recovery-replay-drift", requested_by="kernel"),
            packet.drill_id,
        )
        self.assertFalse(drift.matches)
        self.assertIn("recovery_verification_states", drift.mismatches)

    def test_recovery_verification_fails_closed_without_receipt_or_passing_checks(self):
        cadence = BackupCadenceRecord(
            scope="kernel.db",
            cadence="weekly",
            backup_target="artifact://local/encrypted-kernel-backups",
            encryption_required=True,
            retention_policy="retain-90d",
            recovery_point_objective="24h",
            next_due_at="2026-05-16T00:00:00Z",
            evidence_refs=[],
        )
        self.store.record_backup_cadence(command("backup.cadence", "failed-recovery-cadence", requested_by="kernel"), cadence)
        packet = RestoreDrillPacket(
            cadence_id=cadence.cadence_id,
            backup_ref="artifact://local/encrypted-kernel-backups/kernel-2026-05-09",
            backup_manifest_hash=sha256_text("failed manifest"),
            drill_scope="isolated restore",
            scheduled_for="2026-05-16T01:00:00Z",
            checklist_items=[{"id": "schema", "label": "Verify schema fidelity"}],
            evidence_refs=[],
        )
        self.store.create_restore_drill_packet(command("backup.drill", "failed-recovery-drill", requested_by="scheduler"), packet)
        with self.assertRaises(PermissionError):
            self.store.record_recovery_verification_state(
                command("backup.recovery_verify", "receiptless-verified-recovery", requested_by="kernel"),
                RecoveryVerificationState(
                    drill_id=packet.drill_id,
                    cadence_id=cadence.cadence_id,
                    backup_manifest_hash=packet.backup_manifest_hash,
                    status="verified",
                    fail_closed=False,
                    verification_checks={"schema_fidelity": True},
                    mismatch_summary=[],
                    evidence_refs=[],
                ),
            )

        failed = RecoveryVerificationState(
            drill_id=packet.drill_id,
            cadence_id=cadence.cadence_id,
            backup_manifest_hash=packet.backup_manifest_hash,
            status="failed",
            fail_closed=True,
            verification_checks={"schema_fidelity": False},
            mismatch_summary=["schema_fidelity"],
            evidence_refs=[f"kernel:restore_drill_packets/{packet.drill_id}"],
        )
        self.store.record_recovery_verification_state(
            command("backup.recovery_verify", "failed-closed-recovery", requested_by="kernel"),
            failed,
        )
        replay = self.store.replay_critical_state()
        self.assertTrue(replay.recovery_verification_states[failed.verification_id]["fail_closed"])
        self.assertEqual(replay.restore_drill_packets[packet.drill_id]["status"], "failed")

    def test_recovery_readiness_packets_summarize_storage_health_and_compare_replay(self):
        cadence = BackupCadenceRecord(
            scope="kernel.db",
            cadence="daily",
            backup_target="artifact://local/encrypted-kernel-backups",
            encryption_required=True,
            retention_policy="retain-30d",
            recovery_point_objective="24h",
            next_due_at="2026-05-11T00:00:00Z",
            evidence_refs=["spec:s08_operator_deployment"],
        )
        self.store.record_backup_cadence(command("backup.cadence", "readiness-cadence", requested_by="kernel"), cadence)
        drill = RestoreDrillPacket(
            cadence_id=cadence.cadence_id,
            backup_ref="artifact://local/encrypted-kernel-backups/kernel-2026-05-09",
            backup_manifest_hash=sha256_text("readiness manifest"),
            drill_scope="kernel.db restore into isolated verification path",
            scheduled_for="2026-05-09T01:00:00Z",
            checklist_items=[{"id": "schema", "label": "Verify schema fidelity"}],
            evidence_refs=[f"kernel:backup_cadence_records/{cadence.cadence_id}"],
        )
        self.store.create_restore_drill_packet(command("backup.drill", "readiness-drill", requested_by="scheduler"), drill)
        checklist = RecoveryChecklistReceipt(
            drill_id=drill.drill_id,
            operator_id="operator",
            checklist_results=[{"id": "schema", "status": "pass"}],
            receipt_ref="artifact://local/recovery-drills/readiness-receipt",
            receipt_hash=sha256_text("readiness receipt"),
            status="accepted",
        )
        self.store.record_recovery_checklist_receipt(
            command(
                "backup.recovery_receipt",
                "readiness-checklist",
                requested_authority="operator_gate",
            ),
            checklist,
        )
        self.store.record_recovery_verification_state(
            command("backup.recovery_verify", "readiness-recovery-verified", requested_by="kernel"),
            RecoveryVerificationState(
                drill_id=drill.drill_id,
                cadence_id=cadence.cadence_id,
                receipt_id=checklist.receipt_id,
                backup_manifest_hash=drill.backup_manifest_hash,
                status="verified",
                fail_closed=False,
                verification_checks={"schema_fidelity": True, "event_hash_chain": True},
                mismatch_summary=[],
                evidence_refs=[
                    f"kernel:restore_drill_packets/{drill.drill_id}",
                    f"kernel:recovery_checklist_receipts/{checklist.receipt_id}",
                ],
            ),
        )
        descriptor = EncryptedStorageDescriptor(
            storage_scope="backup_payload",
            owner_ref=drill.backup_ref,
            descriptor_uri="storage://local/backups/kernel-readiness",
            storage_backend="local_encrypted_store",
            local_path_ref="/var/lib/hai/backups/kernel-readiness.ciphertext",
            data_class="internal",
            ciphertext_hash=sha256_text("readiness ciphertext"),
            plaintext_hash=sha256_text("readiness plaintext"),
            size_bytes=4096,
            encryption_algorithm="xchacha20-poly1305",
            key_ref="kms://local/backups/kernel-readiness/key",
            key_version="v1",
            access_policy={"read": ["kernel"], "write": ["kernel"]},
            retention_policy="retain-30d",
            deletion_policy="crypto-shred",
            evidence_refs=[f"kernel:restore_drill_packets/{drill.drill_id}"],
        )
        descriptor_id = self.store.record_encrypted_storage_descriptor(
            command("storage.descriptor", "readiness-backup-descriptor", requested_by="kernel"),
            descriptor,
        )

        ready = self.store.create_recovery_readiness_packet(
            command("recovery.readiness", "readiness-ready", requested_by="kernel"),
            scope="kernel.db",
            as_of="2026-05-10T00:00:00Z",
        )
        self.assertEqual(ready.readiness_status, "ready")
        self.assertFalse(ready.live_controls_enabled)
        self.assertEqual(ready.backup_cadence_summary["active"], 1)
        self.assertEqual(ready.restore_drill_summary["latest_drill_status"], "verified")
        self.assertEqual(ready.encrypted_payload_descriptor_summary["backup_payload_descriptor_count"], 1)
        self.assertEqual(ready.next_operator_actions, [])
        comparison = self.store.compare_recovery_readiness_replay_to_projection(
            command("recovery.readiness_compare", "readiness-compare", requested_by="kernel"),
            ready.packet_id,
        )
        self.assertTrue(comparison.matches)

        failed_receipt = PayloadAccessReceipt(
            descriptor_id=descriptor_id,
            operation="read",
            subject_type="agent",
            subject_id="backup-reader",
            access_result="blocked",
            verification_status="failed",
            payload_hash=descriptor.plaintext_hash,
            receipt_ref="storage://local/backups/kernel-readiness/blocked-read",
            receipt_hash=sha256_text("readiness blocked read"),
            evidence_refs=[f"kernel:encrypted_storage_descriptors/{descriptor_id}"],
            details={"reason": "ciphertext hash mismatch"},
        )
        failed_receipt_id = self.store.record_payload_access_receipt(
            command("storage.access", "readiness-blocked-read", requested_by="agent"),
            failed_receipt,
        )
        self.store.record_encrypted_storage_access_verification(
            command("storage.verify", "readiness-storage-failed", requested_by="kernel"),
            EncryptedStorageAccessVerificationState(
                descriptor_id=descriptor_id,
                last_receipt_id=failed_receipt_id,
                status="failed",
                fail_closed=True,
                verification_checks={"ciphertext_hash_matches": False},
                mismatch_summary=["ciphertext_hash_matches"],
                evidence_refs=[f"kernel:payload_access_receipts/{failed_receipt_id}"],
            ),
        )
        fail_closed = self.store.create_recovery_readiness_packet(
            command("recovery.readiness", "readiness-fail-closed", requested_by="kernel"),
            scope="kernel.db",
            as_of="2026-05-10T00:00:00Z",
        )
        self.assertEqual(fail_closed.readiness_status, "fail_closed")
        self.assertTrue(fail_closed.fail_closed_state["fail_closed"])
        self.assertEqual(fail_closed.payload_access_failure_summary["failed_receipt_ids"], [failed_receipt_id])
        self.assertIn("investigate_encrypted_payload_access", [item["action"] for item in fail_closed.next_operator_actions])
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE recovery_readiness_packets SET readiness_status='ready' WHERE packet_id=?",
                (fail_closed.packet_id,),
            )
        drift = self.store.compare_recovery_readiness_replay_to_projection(
            command("recovery.readiness_compare", "readiness-drift", requested_by="kernel"),
            fail_closed.packet_id,
        )
        self.assertFalse(drift.matches)
        self.assertIn("recovery_readiness_packets", drift.mismatches)

    def test_recovery_readiness_packets_are_kernel_owned_and_read_only(self):
        with self.assertRaises(PermissionError):
            self.store.create_recovery_readiness_packet(
                command("recovery.readiness", "agent-readiness", requested_by="agent"),
                scope="kernel.db",
                as_of="2026-05-10T00:00:00Z",
            )

    def test_hermes_adapter_readiness_packet_is_read_only_and_replayable(self):
        recovery = self.create_ready_recovery_readiness_packet()
        surface_checks = [
            {"surface": surface, "status": "passed", "evidence_refs": [f"proof:{surface}"]}
            for surface in (
                "kanban_worker_lifecycle",
                "dashboard_profile_provider_controls",
                "provider_plugin_calls",
                "mcp_sse_oauth_forwarding",
                "no_agent_cron_watchdog",
                "gateway_goal_checkpoint_resume",
                "platform_allowlists_redaction",
            )
        ]
        reconciliation_checks = [
            {"check": check, "status": "passed", "evidence_refs": [f"proof:{check}"]}
            for check in (
                "kernel_task_status",
                "assignment_ownership",
                "grant_status_scope_expiry_use_count",
                "budget_reservation_status",
                "side_effect_intent_idempotency_receipt",
                "policy_version",
                "operator_halt_quarantine_state",
            )
        ]

        packet = self.store.create_hermes_adapter_readiness_packet(
            command("hermes.adapter_readiness", "hermes-ready", requested_by="kernel"),
            adapter_name="hermes-v0.13",
            hermes_version="0.13.0",
            as_of="2026-05-10T00:00:00Z",
            surface_checks=surface_checks,
            reconciliation_checks=reconciliation_checks,
            recovery_readiness_packet_id=recovery.packet_id,
        )
        self.assertEqual(packet.readiness_status, "ready")
        self.assertFalse(packet.live_controls_enabled)
        self.assertEqual(packet.next_operator_actions, [])
        self.assertIn(f"kernel:recovery_readiness_packets/{recovery.packet_id}", packet.evidence_refs)
        replay = self.store.replay_critical_state()
        self.assertEqual(replay.hermes_adapter_readiness_packets[packet.packet_id]["readiness_status"], "ready")
        comparison = self.store.compare_hermes_adapter_readiness_replay_to_projection(
            command("hermes.adapter_readiness_compare", "hermes-ready-compare", requested_by="kernel"),
            packet.packet_id,
        )
        self.assertTrue(comparison.matches)

        with self.store.connect() as conn:
            conn.execute(
                "UPDATE hermes_adapter_readiness_packets SET readiness_status='fail_closed' WHERE packet_id=?",
                (packet.packet_id,),
            )
        drift = self.store.compare_hermes_adapter_readiness_replay_to_projection(
            command("hermes.adapter_readiness_compare", "hermes-ready-drift", requested_by="kernel"),
            packet.packet_id,
        )
        self.assertFalse(drift.matches)
        self.assertIn("hermes_adapter_readiness_packets", drift.mismatches)

    def test_hermes_adapter_readiness_blocks_missing_and_failed_proofs(self):
        recovery = self.create_ready_recovery_readiness_packet()
        packet = self.store.create_hermes_adapter_readiness_packet(
            command("hermes.adapter_readiness", "hermes-blocked", requested_by="kernel"),
            adapter_name="hermes-v0.13",
            hermes_version="0.13.0",
            as_of="2026-05-10T00:00:00Z",
            surface_checks=[
                {
                    "surface": "gateway_goal_checkpoint_resume",
                    "status": "failed",
                    "evidence_refs": ["proof:resume-side-effect-replay-failed"],
                }
            ],
            reconciliation_checks=[
                {
                    "check": "side_effect_intent_idempotency_receipt",
                    "status": "blocked",
                    "evidence_refs": ["proof:receipt-revalidation-missing"],
                }
            ],
            recovery_readiness_packet_id=recovery.packet_id,
        )
        self.assertEqual(packet.readiness_status, "fail_closed")
        actions = [item["action"] for item in packet.next_operator_actions]
        self.assertIn("prove_hermes_adapter_surfaces", actions)
        self.assertIn("prove_resume_reconciliation_checks", actions)
        self.assertIn("block_live_hermes_attachment", actions)
        self.assertFalse(packet.live_controls_enabled)

        with self.assertRaises(PermissionError):
            self.store.create_hermes_adapter_readiness_packet(
                command("hermes.adapter_readiness", "agent-hermes-readiness", requested_by="agent"),
                adapter_name="hermes-v0.13",
                hermes_version="0.13.0",
                as_of="2026-05-10T00:00:00Z",
                surface_checks=[{"surface": "kanban_worker_lifecycle", "status": "passed"}],
                reconciliation_checks=[{"check": "kernel_task_status", "status": "passed"}],
                recovery_readiness_packet_id=recovery.packet_id,
            )

    def test_kernel_backup_manifest_and_restore_preserve_audit_and_governed_records(self):
        grant = CapabilityGrant(
            grant_id=new_id(),
            task_id=new_id(),
            subject_type="adapter",
            subject_id="side_effect_broker",
            capability_type="side_effect",
            actions=["prepare"],
            resource={"kind": "message"},
            scope={},
            conditions={},
            expires_at=future(),
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=2,
        )
        self.store.issue_capability_grant(command("grant.issue", "backup-side-grant"), grant)
        intent = SideEffectIntent(
            intent_id=new_id(),
            task_id=grant.task_id,
            side_effect_type="message",
            target={"channel": "customer"},
            payload_hash=payload_hash({"body": "governed delivery"}),
            required_authority="operator_gate",
            grant_id=grant.grant_id,
            timeout_policy="compensate",
        )
        self.store.prepare_side_effect(command("side.prepare", "backup-side-prepare"), intent)
        receipt = SideEffectReceipt(
            intent_id=intent.intent_id,
            receipt_type="success",
            receipt_hash=sha256_text("side-effect-success"),
            details={"broker": "test", "executed_by_replay": False},
        )
        self.store.record_side_effect_receipt(command("side.receipt", "backup-side-receipt"), receipt)
        artifact = ArtifactRef(
            artifact_uri="artifact://local/backup-client-dossier",
            data_class="client_confidential",
            content_hash=sha256_text("client dossier"),
            retention_policy="retain-30d",
            deletion_policy="crypto-shred",
            encryption_status="encrypted",
        )
        artifact_id = self.store.execute_command(
            command("artifact.ref", "backup-artifact"),
            lambda tx: tx.create_artifact_ref(artifact),
        )
        governance = ArtifactGovernanceRecord(
            artifact_id=artifact_id,
            action="redact",
            reason="Preserve redaction receipt in backup manifest.",
            required_authority="operator_gate",
            evidence_refs=[f"kernel:artifact_refs/{artifact_id}", f"kernel:side_effect_receipts/{receipt.receipt_id}"],
            receipt_ref="artifact://local/backup-client-dossier/redaction-receipt",
            receipt_hash=sha256_text("backup redaction receipt"),
        )
        self.store.record_artifact_governance(
            command("artifact.governance", "backup-artifact-governance", requested_authority="operator_gate"),
            governance,
        )

        backup_dir = Path(self.tmp.name) / "backup"
        manifest = create_kernel_backup(self.db_path, backup_dir)
        self.assertEqual(manifest["schema"]["name"], "kernel.sql")
        self.assertEqual(manifest["kernel"]["event_schema_version"], 1)
        self.assertEqual(manifest["governed_records"]["tables"]["side_effect_receipts"]["row_count"], 1)
        self.assertEqual(manifest["governed_records"]["tables"]["artifact_governance_records"]["row_count"], 1)
        self.assertEqual(verify_kernel_backup(backup_dir / "kernel.db", backup_dir / "kernel.backup.manifest.json"), manifest)

        restore_path = Path(self.tmp.name) / "restore" / "kernel.db"
        restore_kernel_backup(backup_dir / "kernel.db", manifest, restore_path)
        restored = KernelStore(restore_path).replay_critical_state()
        self.assertEqual(restored.side_effects[intent.intent_id]["receipt"]["receipt_hash"], receipt.receipt_hash)
        self.assertEqual(restored.artifact_governance_records[governance.record_id]["receipt_hash"], governance.receipt_hash)

    def test_kernel_backup_restore_fails_closed_on_hash_chain_or_governed_record_drift(self):
        budget = Budget(
            budget_id=new_id(),
            owner_type="project",
            owner_id=new_id(),
            approved_by="operator",
            cap_usd=Decimal("1"),
            expires_at=future(),
        )
        self.store.create_budget(command("budget.create", "backup-drift-budget"), budget)
        backup_dir = Path(self.tmp.name) / "drift-backup"
        manifest = create_kernel_backup(self.db_path, backup_dir)
        with sqlite3.connect(backup_dir / "kernel.db") as conn:
            conn.execute("UPDATE events SET prev_event_hash='drift' WHERE event_seq=1")
            conn.commit()
        with self.assertRaises(ValueError):
            restore_kernel_backup(backup_dir / "kernel.db", manifest, Path(self.tmp.name) / "drift-restore.db")

        artifact = ArtifactRef(
            artifact_uri="artifact://local/missing-governance",
            data_class="client_confidential",
            content_hash=sha256_text("client data"),
            retention_policy="retain-30d",
            deletion_policy="crypto-shred",
            encryption_status="encrypted",
        )
        artifact_id = self.store.execute_command(
            command("artifact.ref", "missing-governed-artifact"),
            lambda tx: tx.create_artifact_ref(artifact),
        )
        governance = ArtifactGovernanceRecord(
            artifact_id=artifact_id,
            action="delete",
            reason="Deletion receipt must survive restore.",
            required_authority="operator_gate",
            evidence_refs=[f"kernel:artifact_refs/{artifact_id}"],
            receipt_ref="kms://local/delete-receipt",
            receipt_hash=sha256_text("delete receipt"),
        )
        self.store.record_artifact_governance(
            command("artifact.governance", "missing-governed-delete", requested_authority="operator_gate"),
            governance,
        )
        missing_dir = Path(self.tmp.name) / "missing-governed-backup"
        create_kernel_backup(self.db_path, missing_dir)
        with sqlite3.connect(missing_dir / "kernel.db") as conn:
            conn.execute(
                "UPDATE artifact_governance_records SET receipt_hash=NULL WHERE record_id=?",
                (governance.record_id,),
            )
            conn.commit()
        with self.assertRaises(ValueError):
            verify_kernel_backup(missing_dir / "kernel.db", missing_dir / "kernel.backup.manifest.json")

    def test_unknown_event_schema_version_fails_closed_for_replay(self):
        budget = Budget(
            budget_id=new_id(),
            owner_type="project",
            owner_id=new_id(),
            approved_by="operator",
            cap_usd=Decimal("1"),
            expires_at=future(),
        )
        self.store.create_budget(command("budget.create", "bad-version-budget"), budget)
        with self.store.connect() as conn:
            conn.execute("UPDATE events SET event_schema_version=99")
        with self.assertRaises(ValueError):
            self.store.replay_critical_state()

    def test_legacy_schemas_remain_non_authoritative(self):
        boundaries = self.store.legacy_authority_status()
        self.assertIn("schemas/*.sql", boundaries)
        self.assertIn("non-authoritative", boundaries["schemas/*.sql"])
        self.assertIn("subordinate to kernel budgets", boundaries["financial_router"])

        with self.store.connect() as conn:
            tables = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
        self.assertIn("commands", tables)
        self.assertIn("projects", tables)
        self.assertIn("project_tasks", tables)
        self.assertIn("project_task_assignments", tables)
        self.assertIn("project_outcomes", tables)
        self.assertIn("project_artifact_receipts", tables)
        self.assertIn("project_customer_feedback", tables)
        self.assertIn("project_revenue_attributions", tables)
        self.assertIn("project_operator_load", tables)
        self.assertIn("project_commercial_rollups", tables)
        self.assertIn("project_status_rollups", tables)
        self.assertIn("project_close_decision_packets", tables)
        self.assertIn("project_replay_projection_comparisons", tables)
        self.assertIn("project_portfolio_decision_packets", tables)
        self.assertIn("project_portfolio_replay_projection_comparisons", tables)
        self.assertIn("project_scheduling_intents", tables)
        self.assertIn("project_scheduling_priority_change_packets", tables)
        self.assertIn("project_scheduling_priority_replay_projection_comparisons", tables)
        self.assertIn("project_scheduling_replay_projection_comparisons", tables)
        self.assertIn("project_customer_visible_packets", tables)
        self.assertIn("project_customer_commitments", tables)
        self.assertIn("project_customer_commitment_receipts", tables)
        self.assertIn("project_customer_visible_replay_projection_comparisons", tables)
        self.assertIn("artifact_governance_records", tables)
        self.assertIn("artifact_payload_metadata", tables)
        self.assertIn("artifact_lifecycle_task_packets", tables)
        self.assertIn("artifact_lifecycle_replay_projection_comparisons", tables)
        self.assertIn("backup_cadence_records", tables)
        self.assertIn("restore_drill_packets", tables)
        self.assertIn("recovery_checklist_receipts", tables)
        self.assertIn("recovery_verification_states", tables)
        self.assertIn("recovery_replay_projection_comparisons", tables)
        self.assertIn("recovery_readiness_packets", tables)
        self.assertIn("recovery_readiness_replay_projection_comparisons", tables)
        self.assertNotIn("research_tasks", tables)

    def test_schema_contains_authoritative_placeholders(self):
        with self.store.connect() as conn:
            outbox_cols = {row[1] for row in conn.execute("PRAGMA table_info('projection_outbox')").fetchall()}
            receipt_cols = {row[1] for row in conn.execute("PRAGMA table_info('side_effect_receipts')").fetchall()}
        self.assertIn("projection_name", outbox_cols)
        self.assertIn("receipt_hash", receipt_cols)


if __name__ == "__main__":
    unittest.main()
