from __future__ import annotations

import datetime
import json
import logging
import sqlite3
import uuid
from typing import Optional

from financial_router.router import route_task
from financial_router.types import BudgetState, CostStatus, JWTClaims, ModelInfo, RoutingDecision, RoutingTier, TaskMetadata
from skills.db_manager import DatabaseManager

LOGGER = logging.getLogger(__name__)


class FinancialRouterSkill:
    def __init__(self, db_manager: DatabaseManager):
        self._db = db_manager

    def route(self, task: TaskMetadata, models: list[ModelInfo], budget: BudgetState, jwt: JWTClaims) -> RoutingDecision:
        decision = route_task(task, models, budget, jwt)
        correlation_id = decision.reservation_id or task.idempotency_key or task.task_id
        g3_status = None
        if decision.requires_operator_approval:
            g3_status = "PENDING"
        elif decision.tier == RoutingTier.PAID_CLOUD:
            g3_status = "APPROVED"
        if decision.tier == RoutingTier.PAID_CLOUD and g3_status == "APPROVED":
            cost_status = CostStatus.ESTIMATED.value
        else:
            cost_status = CostStatus.NOT_APPLICABLE.value
        conn = self._db.get_connection("financial_ledger")
        conn.execute(
            """
            INSERT INTO routing_decisions (
                decision_id, task_id, project_id, session_id, chain_id, correlation_id,
                role, route_selected, model_used, commercial_use_ok, quality_warning,
                cost_usd, cost_status, justification, g3_required, g3_status,
                reservation_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                task.task_id,
                task.project_id,
                jwt.session_id,
                task.idempotency_key or task.task_id,
                correlation_id,
                "Primary Reasoning",
                decision.tier.value,
                decision.model_id,
                1,
                1 if decision.quality_warning else 0,
                decision.estimated_cost_usd,
                cost_status,
                decision.justification,
                1 if decision.requires_operator_approval else 0,
                g3_status,
                decision.reservation_id,
                datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
            ),
        )
        conn.commit()
        return decision

    def quarantine_inflight_paid_response(
        self,
        *,
        correlation_id: str,
        response_payload: dict | list | str,
        received_at: str | None = None,
    ) -> dict:
        financial = self._db.get_connection("financial_ledger")
        immune = self._db.get_connection("immune")
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
        received_ts = received_at or now
        route_row = financial.execute(
            """
            SELECT decision_id, task_id, project_id, session_id, correlation_id, route_selected,
                   model_used, cost_usd, cost_status, g3_status, reservation_id
            FROM routing_decisions
            WHERE correlation_id = ?
            ORDER BY created_at DESC, decision_id DESC
            LIMIT 1
            """,
            (correlation_id,),
        ).fetchone()
        if route_row is None:
            raise KeyError(correlation_id)
        if route_row["route_selected"] != RoutingTier.PAID_CLOUD.value:
            raise ValueError("Only paid_cloud decisions can be quarantined as disputed in-flight calls.")
        if route_row["g3_status"] != "APPROVED":
            raise ValueError("Only approved paid_cloud decisions can be quarantined as disputed in-flight calls.")

        amount_usd = float(route_row["cost_usd"] or 0.0)
        project_id = self._project_id_for_cost(financial, route_row["project_id"])
        cost_row = financial.execute(
            """
            SELECT record_id
            FROM cost_records
            WHERE correlation_id = ?
            ORDER BY created_at DESC, record_id DESC
            LIMIT 1
            """,
            (correlation_id,),
        ).fetchone()
        if cost_row is None:
            cost_record_id = str(uuid.uuid4())
            financial.execute(
                """
                INSERT INTO cost_records (
                    record_id, project_id, cost_category, amount_usd, description,
                    provider, task_id, correlation_id, route_decision_id, cost_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cost_record_id,
                    project_id,
                    "cloud_api",
                    amount_usd,
                    "Interrupted paid cloud response quarantined during SECURITY_CASCADE",
                    route_row["model_used"],
                    route_row["task_id"],
                    correlation_id,
                    route_row["decision_id"],
                    CostStatus.DISPUTED.value,
                    now,
                ),
            )
        else:
            cost_record_id = str(cost_row["record_id"])
            financial.execute(
                """
                UPDATE cost_records
                SET cost_status = ?, amount_usd = ?, route_decision_id = COALESCE(route_decision_id, ?)
                WHERE record_id = ?
                """,
                (CostStatus.DISPUTED.value, amount_usd, route_row["decision_id"], cost_record_id),
            )

        financial.execute(
            """
            UPDATE routing_decisions
            SET cost_status = ?
            WHERE decision_id = ?
            """,
            (CostStatus.DISPUTED.value, route_row["decision_id"]),
        )
        financial.commit()

        payload_format = "text" if isinstance(response_payload, str) else "json"
        payload_text = response_payload if isinstance(response_payload, str) else json.dumps(response_payload, sort_keys=True, separators=(",", ":"))
        quarantine_id: str | None = None
        quarantine_persisted = False
        try:
            quarantine_id = str(uuid.uuid4())
            immune.execute(
                """
                INSERT INTO quarantined_responses (
                    quarantine_id, correlation_id, session_id, project_id, task_id,
                    route_decision_id, cost_record_id, reservation_id, source_breaker,
                    provider, model_used, payload_format, payload_text, received_at,
                    quarantined_at, review_status, operator_decision, review_notes,
                    review_digest_id, reviewed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    quarantine_id,
                    correlation_id,
                    route_row["session_id"],
                    route_row["project_id"],
                    route_row["task_id"],
                    route_row["decision_id"],
                    cost_record_id,
                    route_row["reservation_id"],
                    "SECURITY_CASCADE",
                    route_row["model_used"],
                    route_row["model_used"],
                    payload_format,
                    payload_text,
                    received_ts,
                    now,
                    "PENDING",
                    None,
                    None,
                    None,
                    None,
                ),
            )
            immune.commit()
            quarantine_persisted = True
        except sqlite3.DatabaseError:
            LOGGER.warning(
                "quarantine_persistence_failed",
                extra={"correlation_id": correlation_id, "route_decision_id": route_row["decision_id"]},
                exc_info=True,
            )

        return {
            "correlation_id": correlation_id,
            "route_decision_id": route_row["decision_id"],
            "cost_record_id": cost_record_id,
            "quarantine_id": quarantine_id,
            "quarantine_persisted": quarantine_persisted,
            "cost_status": CostStatus.DISPUTED.value,
            "amount_usd": amount_usd,
            "received_at": received_ts,
            "quarantined_at": now if quarantine_persisted else None,
        }

    @staticmethod
    def _project_id_for_cost(conn, project_id: str | None) -> str | None:
        if not project_id:
            return None
        row = conn.execute(
            "SELECT 1 FROM projects WHERE project_id = ?",
            (project_id,),
        ).fetchone()
        return project_id if row is not None else None


_SKILL: Optional[FinancialRouterSkill] = None


def configure_skill(db_manager: DatabaseManager):
    global _SKILL
    _SKILL = FinancialRouterSkill(db_manager)


def financial_router_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("financial router skill not configured")
    if action == "route":
        return _SKILL.route(kwargs["task"], kwargs["models"], kwargs["budget"], kwargs["jwt"])
    if action == "quarantine_inflight_paid_response":
        return _SKILL.quarantine_inflight_paid_response(
            correlation_id=kwargs["correlation_id"],
            response_payload=kwargs["response_payload"],
            received_at=kwargs.get("received_at"),
        )
    raise ValueError(f"Unknown action: {action}")
