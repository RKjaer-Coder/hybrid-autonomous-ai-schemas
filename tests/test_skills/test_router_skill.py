from __future__ import annotations

from financial_router.types import BudgetState, JWTClaims, ModelInfo, TaskMetadata
from skills.db_manager import DatabaseManager
from skills.financial_router.skill import FinancialRouterSkill


def test_router_wraps_and_logs(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    skill = FinancialRouterSkill(db)
    decision = skill.route(
        TaskMetadata(task_id="t1", task_type="x", required_capability="y", quality_threshold=0.1),
        [ModelInfo("m-local", "local", True, 0.9, 0.0)],
        BudgetState(),
        JWTClaims(session_id="s1"),
    )
    assert decision.tier.value == "local"
    conn = db.get_connection("financial_ledger")
    n = conn.execute("SELECT COUNT(*) FROM routing_decisions").fetchone()[0]
    assert n == 1


def test_router_persists_default_fallback_route(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    skill = FinancialRouterSkill(db)
    decision = skill.route(
        TaskMetadata(task_id="t2", task_type="x", required_capability="y", quality_threshold=0.95, idempotency_key="req-2"),
        [ModelInfo("m-sub", "subscription", True, 0.6, 0.0, rate_limit_remaining=10)],
        BudgetState(),
        JWTClaims(session_id="s2"),
    )
    assert decision.tier.value == "default_fallback"
    conn = db.get_connection("financial_ledger")
    row = conn.execute(
        "SELECT route_selected, quality_warning FROM routing_decisions WHERE task_id = ?",
        ("t2",),
    ).fetchone()
    assert row["route_selected"] == "default_fallback"
    assert row["quality_warning"] == 1
