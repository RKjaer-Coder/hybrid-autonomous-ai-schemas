from __future__ import annotations

import datetime
import uuid
from typing import Optional

from financial_router.router import route_task
from financial_router.types import BudgetState, JWTClaims, ModelInfo, RoutingDecision, RoutingTier, TaskMetadata
from skills.db_manager import DatabaseManager


class FinancialRouterSkill:
    def __init__(self, db_manager: DatabaseManager):
        self._db = db_manager

    def route(self, task: TaskMetadata, models: list[ModelInfo], budget: BudgetState, jwt: JWTClaims) -> RoutingDecision:
        decision = route_task(task, models, budget, jwt)
        g3_status = None
        if decision.requires_operator_approval:
            g3_status = "PENDING"
        elif decision.tier == RoutingTier.PAID_CLOUD:
            g3_status = "APPROVED"
        conn = self._db.get_connection("financial_ledger")
        conn.execute(
            "INSERT INTO routing_decisions (decision_id, task_id, chain_id, role, route_selected, model_used, commercial_use_ok, quality_warning, cost_usd, justification, g3_required, g3_status, reservation_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                str(uuid.uuid4()),
                task.task_id,
                task.idempotency_key or task.task_id,
                "Primary Reasoning",
                decision.tier.value,
                decision.model_id,
                1,
                1 if decision.quality_warning else 0,
                decision.estimated_cost_usd,
                decision.justification,
                1 if decision.requires_operator_approval else 0,
                g3_status,
                decision.reservation_id,
                datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
            ),
        )
        conn.commit()
        return decision


_SKILL: Optional[FinancialRouterSkill] = None


def configure_skill(db_manager: DatabaseManager):
    global _SKILL
    _SKILL = FinancialRouterSkill(db_manager)


def financial_router_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("financial router skill not configured")
    if action == "route":
        return _SKILL.route(kwargs["task"], kwargs["models"], kwargs["budget"], kwargs["jwt"])
    raise ValueError(f"Unknown action: {action}")
