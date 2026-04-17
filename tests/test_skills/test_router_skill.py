from __future__ import annotations

import json

from financial_router.types import BudgetState, JWTClaims, ModelInfo, SystemPhase, TaskMetadata
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


def test_router_quarantines_interrupted_paid_call_as_disputed(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    skill = FinancialRouterSkill(db)
    ledger = db.get_connection("financial_ledger")
    ledger.execute(
        """
        INSERT INTO projects (
            project_id, opportunity_id, name, income_mechanism, thesis,
            success_criteria, compute_budget, portfolio_weight, status,
            kill_score_watch, cashflow_actual_usd, council_verdict_id,
            pivot_log, created_at, closed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "project-paid",
            "opp-paid",
            "Paid project",
            "software_product",
            "Thesis",
            json.dumps({"ok": True}),
            json.dumps({"max_executor_hours": 10}),
            0.2,
            "ACTIVE",
            0,
            0.0,
            None,
            "[]",
            "2026-04-15T10:00:00+00:00",
            None,
        ),
    )
    ledger.commit()

    decision = skill.route(
        TaskMetadata(
            task_id="t-paid",
            task_type="x",
            required_capability="y",
            quality_threshold=0.9,
            estimated_task_value_usd=100.0,
            project_id="project-paid",
            idempotency_key="corr-paid-1",
            is_operating_phase=True,
        ),
        [
            ModelInfo("m-paid", "paid", True, 0.96, 0.005),
        ],
        BudgetState(
            system_phase=SystemPhase.OPERATING,
            project_cloud_spend_cap_usd=50.0,
            project_cloud_spend_current_usd=1.0,
            project_cashflow_target_usd=5000.0,
        ),
        JWTClaims(session_id="session-paid", max_api_spend_usd=10.0),
    )
    assert decision.tier.value == "paid_cloud"

    route_row = ledger.execute(
        """
        SELECT correlation_id, cost_status, g3_status
        FROM routing_decisions
        WHERE task_id = ?
        """,
        ("t-paid",),
    ).fetchone()
    assert route_row["correlation_id"] == "corr-paid-1"
    assert route_row["cost_status"] == "ESTIMATED"
    assert route_row["g3_status"] == "APPROVED"

    result = skill.quarantine_inflight_paid_response(
        correlation_id="corr-paid-1",
        response_payload={"ok": True, "answer": "late"},
    )

    route_row = ledger.execute(
        """
        SELECT cost_status
        FROM routing_decisions
        WHERE task_id = ?
        """,
        ("t-paid",),
    ).fetchone()
    cost_row = ledger.execute(
        """
        SELECT project_id, amount_usd, correlation_id, cost_status
        FROM cost_records
        WHERE correlation_id = ?
        """,
        ("corr-paid-1",),
    ).fetchone()
    immune = db.get_connection("immune")
    quarantine_row = immune.execute(
        """
        SELECT correlation_id, review_status, source_breaker, session_id, task_id
        FROM quarantined_responses
        WHERE correlation_id = ?
        """,
        ("corr-paid-1",),
    ).fetchone()

    assert result["quarantine_persisted"] is True
    assert result["cost_status"] == "DISPUTED"
    assert route_row["cost_status"] == "DISPUTED"
    assert cost_row["project_id"] == "project-paid"
    assert cost_row["correlation_id"] == "corr-paid-1"
    assert cost_row["cost_status"] == "DISPUTED"
    assert cost_row["amount_usd"] == decision.estimated_cost_usd
    assert quarantine_row["correlation_id"] == "corr-paid-1"
    assert quarantine_row["review_status"] == "PENDING"
    assert quarantine_row["source_breaker"] == "SECURITY_CASCADE"
    assert quarantine_row["session_id"] == "session-paid"
    assert quarantine_row["task_id"] == "t-paid"
