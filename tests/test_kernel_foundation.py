from __future__ import annotations

import sqlite3
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from kernel import (
    ArtifactRef,
    Budget,
    CapabilityGrant,
    Command,
    KernelStore,
    SideEffectIntent,
    SideEffectReceipt,
)
from kernel.records import new_id, payload_hash, sha256_text
from kernel.store import KERNEL_POLICY_VERSION


def command(command_type: str, key: str, payload: dict | None = None) -> Command:
    return Command(
        command_type=command_type,
        requested_by="operator",
        requester_id="operator",
        target_entity_type="kernel",
        idempotency_key=key,
        payload=payload or {"key": key},
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
        self.assertNotIn("projects", tables)
        self.assertNotIn("research_tasks", tables)

    def test_schema_contains_authoritative_placeholders(self):
        with self.store.connect() as conn:
            outbox_cols = {row[1] for row in conn.execute("PRAGMA table_info('projection_outbox')").fetchall()}
            receipt_cols = {row[1] for row in conn.execute("PRAGMA table_info('side_effect_receipts')").fetchall()}
        self.assertIn("projection_name", outbox_cols)
        self.assertIn("receipt_hash", receipt_cols)


if __name__ == "__main__":
    unittest.main()
