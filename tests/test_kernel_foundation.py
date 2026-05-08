from __future__ import annotations

import sqlite3
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from kernel import (
    ArtifactGovernanceRecord,
    ArtifactRef,
    Budget,
    CapabilityGrant,
    Command,
    KernelStore,
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
        self.assertNotIn("research_tasks", tables)

    def test_schema_contains_authoritative_placeholders(self):
        with self.store.connect() as conn:
            outbox_cols = {row[1] for row in conn.execute("PRAGMA table_info('projection_outbox')").fetchall()}
            receipt_cols = {row[1] for row in conn.execute("PRAGMA table_info('side_effect_receipts')").fetchall()}
        self.assertIn("projection_name", outbox_cols)
        self.assertIn("receipt_hash", receipt_cols)


if __name__ == "__main__":
    unittest.main()
