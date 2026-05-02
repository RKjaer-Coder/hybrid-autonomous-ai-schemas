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
        elif event_type == "quality_gate_evaluated":
            state.quality_gate_events[entity_id] = dict(payload)
        elif event_type == "evidence_bundle_committed":
            state.evidence_bundles[entity_id] = dict(payload)
            state.research_requests[payload["request_id"]]["status"] = "completed"
            state.research_requests[payload["request_id"]]["updated_at"] = payload["created_at"]
        elif event_type == "commercial_decision_packet_created":
            state.commercial_decision_packets[entity_id] = dict(payload)
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
        if row["quality_gate_result"] == "fail":
            raise ValueError("failed evidence bundle cannot produce a commercial decision packet")
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
