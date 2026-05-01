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
    Event,
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
    "skills/runtime.py": "wrap: runtime harness, never kernel authority",
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

    def replay_critical_state(self) -> ReplayState:
        state = ReplayState()
        expected_prev: str | None = None
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM events ORDER BY event_seq ASC").fetchall()
        for row in rows:
            if row["event_schema_version"] != KERNEL_EVENT_SCHEMA_VERSION:
                raise ValueError(f"unsupported event schema version: {row['event_schema_version']}")
            if row["prev_event_hash"] != expected_prev:
                raise ValueError("event hash chain mismatch")
            payload = _loads(row["payload_json"])
            expected_hash = self._event_hash(
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
            self._apply_replay_event(state, row["event_type"], row["entity_id"], payload)
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
