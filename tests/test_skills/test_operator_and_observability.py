from __future__ import annotations

import datetime
import json
import uuid

from harness_variants import ExecutionTrace, ExecutionTraceStep, HarnessVariantManager
from financial_router.types import BudgetState, JWTClaims, ModelInfo, SystemPhase, TaskMetadata
from immune.circuit_breakers import CircuitBreakerLogger
from immune.config import load_config
from immune.judge import judge_check
from immune.judge_lifecycle import JudgeLifecycleManager
from immune.types import JudgePayload
from skills.db_manager import DatabaseManager
from skills.financial_router.skill import FinancialRouterSkill
from skills.observability.skill import ObservabilitySkill
from skills.operator_interface.skill import OperatorInterfaceSkill
from skills.research_domain.skill import ResearchDomainSkill
from skills.strategic_memory.skill import StrategicMemorySkill


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)


def _ts(value: datetime.datetime) -> str:
    return value.isoformat()


def _insert_normal_judge_verdict(
    immune,
    *,
    skill_name: str,
    task_type: str | None = None,
    result: str,
    timestamp: str,
) -> None:
    immune.execute(
        """
        INSERT INTO immune_verdicts (
            verdict_id, verdict_type, scan_tier, session_id, skill_name,
            task_type, result, match_pattern, latency_ms, timestamp, judge_mode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            "judge_output",
            "fast_path",
            f"session-{skill_name}-{result}-{timestamp}",
            skill_name,
            task_type or skill_name,
            result,
            "seeded",
            5,
            timestamp,
            "NORMAL",
        ),
    )


def test_operator_interface_generates_idempotent_digest_with_urgent_pending_first(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    memory = StrategicMemorySkill(db)
    operator = OperatorInterfaceSkill(db)
    financial = db.get_connection("financial_ledger")
    strategic = db.get_connection("strategic_memory")
    operator_db = db.get_connection("operator_digest")
    now = _now()

    financial.execute(
        """
        INSERT INTO projects (
            project_id, opportunity_id, name, income_mechanism, thesis,
            success_criteria, compute_budget, portfolio_weight, status,
            kill_score_watch, cashflow_actual_usd, council_verdict_id,
            pivot_log, created_at, closed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "project-1",
            "opp-1",
            "Alpha",
            "client_work",
            "Thesis",
            json.dumps(["close first client"]),
            json.dumps({"max_executor_hours": 10}),
            0.60,
            "ACTIVE",
            1,
            0.0,
            None,
            "[]",
            _ts(now - datetime.timedelta(hours=2)),
            None,
        ),
    )
    financial.execute(
        "INSERT INTO treasury VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("treasury-1", "injection", 50.0, 50.0, None, "Seeded treasury", _ts(now - datetime.timedelta(hours=1))),
    )
    financial.execute(
        "INSERT INTO revenue_records VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("rev-1", "project-1", 25.0, "client_invoice", "operator_reported", _ts(now - datetime.timedelta(hours=8)), _ts(now), _ts(now)),
    )
    financial.execute(
        """
        INSERT INTO cost_records (
            record_id, project_id, cost_category, amount_usd,
            description, provider, task_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("cost-1", "project-1", "cloud_api", 5.0, "Inference", "OpenAI", "task-1", _ts(now - datetime.timedelta(hours=6))),
    )
    strategic.execute(
        """
        INSERT INTO opportunity_records (
            opportunity_id, income_mechanism, title, thesis, detected_by,
            council_verdict_id, validation_spend, validation_report,
            cashflow_estimate, status, project_id, learning_record,
            provenance_links, provenance_degraded, trust_tier, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "opp-1",
            "client_work",
            "Alpha work",
            "Thesis",
            "operator",
            None,
            0.0,
            None,
            json.dumps({"low": 100, "mid": 200, "high": 300, "currency": "USD", "period": "month"}),
            "GO_NO_GO",
            None,
            None,
            "[]",
            0,
            2,
            _ts(now - datetime.timedelta(hours=3)),
            _ts(now - datetime.timedelta(hours=1)),
        ),
    )
    operator_db.execute(
        """
        INSERT INTO gate_log (
            gate_id, gate_type, trigger_description, context_packet, project_id,
            status, timeout_hours, operator_response, created_at, responded_at, expires_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "gate-1",
            "G1",
            "Operator review",
            json.dumps({"project": "Alpha"}),
            "project-1",
            "PENDING",
            24.0,
            None,
            _ts(now - datetime.timedelta(minutes=10)),
            None,
            _ts(now + datetime.timedelta(hours=2)),
        ),
    )
    operator_db.execute(
        """
        INSERT INTO harvest_requests (
            harvest_id, task_id, prompt_text, target_interface, context_summary,
            priority, status, expires_at, operator_result, relevance_score,
            clarification_sent, created_at, delivered_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "harvest-1",
            "task-1",
            "Check source",
            "ChatGPT Plus",
            "Need one data point",
            "P2_NORMAL",
            "PENDING",
            _ts(now + datetime.timedelta(hours=12)),
            None,
            None,
            0,
            _ts(now - datetime.timedelta(minutes=5)),
            None,
        ),
    )
    operator_db.commit()

    memory.write_brief(
        "task-1",
        "Fresh Brief",
        "Summary",
        actionability="ACTION_RECOMMENDED",
        depth_tier="FULL",
        source_urls=["https://example.com/a", "https://api.example.com/b"],
        source_assessments=[
            {"url": "https://example.com/a", "relevance": 0.8, "freshness": "2026-04-14", "source_type": "tier2_web"},
            {"url": "https://api.example.com/b", "relevance": 0.9, "freshness": "2026-04-14", "source_type": "tier1_api"},
        ],
        uncertainty_statement="We still do not know whether the observed demand is representative across the full addressable market.",
        counter_thesis="The strongest reason this could be wrong is that the second source may overstate short-term momentum.",
    )
    operator.record_heartbeat("command")

    digest_a = operator.generate_digest()
    digest_b = operator.generate_digest()

    assert digest_a["digest_id"] == digest_b["digest_id"]
    assert digest_a["digest_type"] == "daily"
    assert digest_a["sections_included"][0] == "PENDING DECISIONS"
    assert "Fresh Brief (ACTION_RECOMMENDED)" in digest_a["content"]
    assert "OLR=" in digest_a["content"]


def test_operator_interface_enforces_alert_suppression_and_acknowledgement(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    operator = OperatorInterfaceSkill(db)
    base = _now()

    first = operator.alert(
        "T2",
        "EXECUTOR_SATURATION",
        "Initial saturation.",
        reference_time=_ts(base),
    )
    operator.alert(
        "T2",
        "EXECUTOR_SATURATION",
        "Duplicate within suppression window.",
        reference_time=_ts(base + datetime.timedelta(minutes=5)),
    )
    for idx in range(2, 7):
        operator.alert(
            "T2",
            f"T2_TYPE_{idx}",
            f"T2 alert {idx}",
            reference_time=_ts(base + datetime.timedelta(minutes=15 + idx)),
        )
    t3_a = operator.alert(
        "T3",
        "SECURITY_CASCADE",
        "Critical security cascade.",
        reference_time=_ts(base + datetime.timedelta(minutes=30)),
    )
    operator.alert(
        "T3",
        "SECURITY_CASCADE",
        "Second critical security cascade.",
        reference_time=_ts(base + datetime.timedelta(minutes=31)),
    )
    ack = operator.acknowledge_alert(t3_a, reference_time=_ts(base + datetime.timedelta(minutes=32)))

    alerts = operator.list_alerts(limit=10, include_suppressed=True)
    delivered_t2 = [row for row in alerts if row["tier"] == "T2" and not row["suppressed"]]
    suppressed_t2 = [row for row in alerts if row["tier"] == "T2" and row["suppressed"]]
    delivered_t3 = [row for row in alerts if row["tier"] == "T3" and not row["suppressed"]]

    assert first
    assert len(delivered_t2) == 5
    assert len(suppressed_t2) == 2
    assert len(delivered_t3) == 2
    assert ack["alert_id"] == t3_a
    assert operator.list_alerts(limit=5, tier="T3", unacknowledged_only=True)[0]["acknowledged"] is False


def test_observability_queries_filters_breakers_and_health_modes(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    operator = OperatorInterfaceSkill(db)
    observability = ObservabilitySkill(db, telemetry_buffer=None, immune_buffer=None)
    breaker_logger = CircuitBreakerLogger(str(test_data_dir / "immune_system.db"))
    telemetry = db.get_connection("telemetry")
    strategic = db.get_connection("strategic_memory")
    operator_db = db.get_connection("operator_digest")
    now = _now()

    operator_db.execute(
        "INSERT INTO operator_heartbeat VALUES (?, ?, ?, ?)",
        (str(uuid.uuid4()), "message", "CLI", _ts(now - datetime.timedelta(hours=80))),
    )
    for idx in range(30):
        operator_db.execute(
            """
            INSERT INTO gate_log (
                gate_id, gate_type, trigger_description, context_packet, project_id,
                status, timeout_hours, operator_response, created_at, responded_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"gate-{idx}",
                "G1",
                "Pending gate",
                json.dumps({"idx": idx}),
                None,
                "PENDING",
                24.0,
                None,
                _ts(now - datetime.timedelta(hours=1)),
                None,
                _ts(now + datetime.timedelta(hours=12)),
            ),
        )
    operator_db.execute(
        """
        INSERT INTO operator_load_tracking VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "load-1",
            (now - datetime.timedelta(days=now.weekday())).date().isoformat(),
            json.dumps({"G1": 30, "G2": 0, "G3": 0, "G4": 0}),
            0,
            0,
            0,
            17.33,
            1,
            _ts(now),
        ),
    )
    operator_db.commit()
    operator.alert("T2", "EXECUTOR_SATURATION", "Heads up.", channel_delivered="CLI")
    operator.alert("T3", "SECURITY_CASCADE", "Critical.", channel_delivered="CLI")
    breaker_logger.log_breaker(
        "TOOL_FAILURE_STORM",
        "TRIPPED",
        "30% failures",
        "HALT_EXECUTION",
        False,
        timestamp=_ts(now - datetime.timedelta(seconds=30)),
    )
    breaker_logger.log_breaker(
        "SECURITY_CASCADE",
        "TRIPPED",
        "3 security alerts",
        "FULL_SYSTEM_HALT",
        True,
        timestamp=_ts(now - datetime.timedelta(seconds=5)),
    )

    telemetry.execute(
        "INSERT INTO chain_definitions (chain_type, steps, created_at, updated_at) VALUES (?, ?, ?, ?)",
        (
            "operator_workflow",
            '[{"step_type":"alert","skill":"operator_interface"},{"step_type":"digest","skill":"operator_interface"}]',
            _ts(now),
            _ts(now),
        ),
    )
    telemetry.execute(
        "INSERT INTO step_outcomes VALUES (?,?,?,?,?,?,?,?,?)",
        ("evt-pass", "alert", "operator_interface", "operator-workflow", "PASS", 12, 0, None, _ts(now)),
    )
    telemetry.execute(
        "INSERT INTO step_outcomes VALUES (?,?,?,?,?,?,?,?,?)",
        ("evt-fail", "digest", "operator_interface", "operator-workflow", "FAIL", 40, 0, None, _ts(now)),
    )
    strategic.execute(
        """
        INSERT INTO research_tasks (
            task_id, domain, source, title, brief, priority, status,
            max_spend_usd, actual_spend_usd, output_brief_id, follow_up_tasks,
            stale_after, tags, depth_upgrade, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "task-stale",
            2,
            "operator",
            "Stale task",
            "Brief",
            "P2_NORMAL",
            "STALE",
            0.0,
            0.0,
            None,
            "[]",
            None,
            "[]",
            0,
            _ts(now - datetime.timedelta(days=1)),
            _ts(now),
        ),
    )
    strategic.execute(
        """
        INSERT INTO council_verdicts (
            verdict_id, tier_used, decision_type, recommendation, confidence,
            reasoning_summary, dissenting_views, minority_positions,
            full_debate_record, cost_usd, project_id, outcome_record,
            da_quality_score, da_assessment, tie_break, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "verdict-1",
            1,
            "phase_gate",
            "PURSUE",
            0.67,
            "Continue.",
            "Watch the pending harvest.",
            "[]",
            None,
            0.0,
            "project-1",
            None,
            0.5,
            "[]",
            0,
            _ts(now),
        ),
    )
    telemetry.commit()
    strategic.commit()

    critical_digest = operator.generate_digest(digest_type="critical_only", operator_state="ACTIVE")
    alerts = observability.query_alert_history(limit=5, tier="T2", alert_type="EXECUTOR_SATURATION", include_suppressed=False)
    breaker_rows = observability.query_circuit_breakers(limit=5, state="TRIPPED")
    compound_breakers = observability.recent_compound_breakers(limit=5, unresolved_only=True)
    council = observability.query_council_verdicts(limit=5, decision_type="phase_gate")
    reliability = observability.reliability_dashboard()
    breakers = observability.circuit_breaker_status()
    health = observability.system_health()
    digests = observability.recent_digests(limit=5, digest_type="critical_only")

    assert alerts[0]["alert_type"] == "EXECUTOR_SATURATION"
    assert breaker_rows[0]["breaker_name"] == "SECURITY_CASCADE"
    assert compound_breakers[0]["breaker_names"] == ["SECURITY_CASCADE", "TOOL_FAILURE_STORM"]
    assert compound_breakers[0]["winning_action"] == "FULL_SYSTEM_HALT"
    assert council[0]["verdict_id"] == "verdict-1"
    assert reliability["chains"][0]["chain_type"] == "operator_workflow"
    assert reliability["critical_steps"][0]["step_type"] == "digest"
    assert breakers["critical"] == ["digest/operator_interface"]
    assert breakers["logged_active"] == ["SECURITY_CASCADE", "TOOL_FAILURE_STORM"]
    assert breakers["recent_compound_events"][0]["winning_action"] == "FULL_SYSTEM_HALT"
    assert health["heartbeat_state"] == "CONSERVATIVE"
    assert health["recommended_digest_type"] == "catch_up"
    assert health["operator_load"]["critical_only_recommended"] is True
    assert health["circuit_breakers"]["logged_active"] == ["SECURITY_CASCADE", "TOOL_FAILURE_STORM"]
    assert health["circuit_breakers"]["recent_compound_events"][0]["winning_action"] == "FULL_SYSTEM_HALT"
    assert health["research_health"]["stale_tasks"] == 1
    assert digests[0]["digest_id"] == critical_digest["digest_id"]
    assert "compound=SECURITY_CASCADE+TOOL_FAILURE_STORM->FULL_SYSTEM_HALT" in critical_digest["content"]


def test_quarantined_responses_and_disputed_costs_surface_for_operator_review(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    operator = OperatorInterfaceSkill(db)
    observability = ObservabilitySkill(db, telemetry_buffer=None, immune_buffer=None)
    router = FinancialRouterSkill(db)
    financial = db.get_connection("financial_ledger")
    now = _now()

    financial.execute(
        """
        INSERT INTO projects (
            project_id, opportunity_id, name, income_mechanism, thesis,
            success_criteria, compute_budget, portfolio_weight, status,
            kill_score_watch, cashflow_actual_usd, council_verdict_id,
            pivot_log, created_at, closed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "project-quarantine",
            "opp-quarantine",
            "Quarantine Project",
            "client_work",
            "Thesis",
            json.dumps({"ok": True}),
            json.dumps({"max_executor_hours": 10}),
            0.25,
            "ACTIVE",
            0,
            0.0,
            None,
            "[]",
            _ts(now - datetime.timedelta(hours=2)),
            None,
        ),
    )
    financial.execute(
        "INSERT INTO treasury VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("treasury-q", "injection", 25.0, 25.0, None, "Seeded treasury", _ts(now - datetime.timedelta(hours=1))),
    )
    financial.commit()

    operator.record_heartbeat("command")
    router.route(
        task=TaskMetadata(
            task_id="task-quarantine",
            task_type="reasoning",
            required_capability="analysis",
            quality_threshold=0.9,
            estimated_task_value_usd=100.0,
            project_id="project-quarantine",
            idempotency_key="corr-quarantine-1",
            is_operating_phase=True,
        ),
        models=[ModelInfo("paid-frontier", "paid", True, 0.96, 0.005)],
        budget=BudgetState(
            system_phase=SystemPhase.OPERATING,
            project_cloud_spend_cap_usd=25.0,
            project_cloud_spend_current_usd=1.0,
            project_cashflow_target_usd=5000.0,
        ),
        jwt=JWTClaims(
            session_id="session-quarantine",
            max_api_spend_usd=10.0,
        ),
    )
    operator.dispatch_approved_paid_route(
        correlation_id="corr-quarantine-1",
        jwt_claims={"session_id": "session-quarantine", "max_api_spend_usd": 10.0, "current_session_spend_usd": 0.0},
    )
    router.quarantine_inflight_paid_response(
        correlation_id="corr-quarantine-1",
        response_payload={"ok": True, "output": "late paid response"},
        received_at=_ts(now),
    )

    pending = operator.list_quarantined_responses(limit=5, pending_review_only=True)
    disputed = observability.recent_disputed_costs(limit=5)
    quarantines = observability.recent_quarantined_responses(limit=5, pending_review_only=True)
    health = observability.system_health()
    digest = operator.generate_digest(digest_type="critical_only", operator_state="ACTIVE")

    assert pending[0]["correlation_id"] == "corr-quarantine-1"
    assert pending[0]["review_status"] == "PENDING"
    assert disputed[0]["correlation_id"] == "corr-quarantine-1"
    assert disputed[0]["cost_status"] == "DISPUTED"
    assert quarantines[0]["correlation_id"] == "corr-quarantine-1"
    assert health["quarantined_responses"]["pending_review_count"] == 1
    assert health["disputed_costs"]["count"] == 1
    assert "quarantine pending=1" in digest["content"]
    assert "disputed_spend=$0.01/1" in digest["content"]

    reviewed = operator.review_quarantined_response(
        pending[0]["quarantine_id"],
        "DISCARD",
        review_notes="Wasted spend during incident triage.",
        reference_time=_ts(now + datetime.timedelta(minutes=5)),
    )

    pending_after = operator.list_quarantined_responses(limit=5, pending_review_only=True)

    assert reviewed["review_status"] == "DISCARDED"
    assert reviewed["operator_decision"] == "DISCARD"
    assert pending_after == []


def test_g3_requests_surface_for_operator_digest_and_observability(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    operator = OperatorInterfaceSkill(db)
    observability = ObservabilitySkill(db, telemetry_buffer=None, immune_buffer=None)
    router = FinancialRouterSkill(db)
    financial = db.get_connection("financial_ledger")
    now = _now()

    financial.execute(
        """
        INSERT INTO treasury VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("treasury-g3", "injection", 40.0, 40.0, None, "Seeded treasury", _ts(now - datetime.timedelta(hours=1))),
    )
    financial.commit()
    operator.record_heartbeat("command")

    router.route(
        task=TaskMetadata(
            task_id="task-g3",
            task_type="reasoning",
            required_capability="analysis",
            quality_threshold=0.9,
            estimated_task_value_usd=100.0,
            project_id="project-g3",
            idempotency_key="corr-g3-pending",
            is_operating_phase=True,
        ),
        models=[ModelInfo("paid-frontier", "paid", True, 0.96, 0.005)],
        budget=BudgetState(
            system_phase=SystemPhase.OPERATING,
            project_cloud_spend_cap_usd=None,
            project_cashflow_target_usd=5000.0,
        ),
        jwt=JWTClaims(session_id="session-g3", max_api_spend_usd=10.0),
    )
    pending_request = operator.list_g3_approval_requests(limit=5, status="PENDING")[0]

    approved = operator.review_g3_approval_request(
        pending_request["request_id"],
        "APPROVE",
        operator_notes="Frontier model justified outside the project budget.",
        reference_time=_ts(now + datetime.timedelta(minutes=5)),
    )
    denied_request = router.route(
        task=TaskMetadata(
            task_id="task-g3-denied",
            task_type="reasoning",
            required_capability="analysis",
            quality_threshold=0.9,
            estimated_task_value_usd=100.0,
            project_id="project-g3",
            idempotency_key="corr-g3-denied",
            is_operating_phase=True,
        ),
        models=[ModelInfo("paid-frontier", "paid", True, 0.96, 0.005)],
        budget=BudgetState(
            system_phase=SystemPhase.OPERATING,
            project_cloud_spend_cap_usd=None,
            project_cashflow_target_usd=5000.0,
        ),
        jwt=JWTClaims(session_id="session-g3-denied", max_api_spend_usd=10.0),
    )
    assert denied_request.requires_operator_approval is True
    denied_request_row = operator.list_g3_approval_requests(limit=5, status="PENDING")[0]
    denied = operator.review_g3_approval_request(
        denied_request_row["request_id"],
        "DENY",
        operator_notes="Use a non-paid fallback instead.",
        reference_time=_ts(now + datetime.timedelta(minutes=10)),
    )

    router.route(
        task=TaskMetadata(
            task_id="task-g3-expired",
            task_type="reasoning",
            required_capability="analysis",
            quality_threshold=0.9,
            estimated_task_value_usd=100.0,
            project_id="project-g3",
            idempotency_key="corr-g3-expired",
            is_operating_phase=True,
        ),
        models=[ModelInfo("paid-frontier", "paid", True, 0.96, 0.005)],
        budget=BudgetState(
            system_phase=SystemPhase.OPERATING,
            project_cloud_spend_cap_usd=None,
            project_cashflow_target_usd=5000.0,
        ),
        jwt=JWTClaims(session_id="session-g3-expired", max_api_spend_usd=10.0),
    )
    expired_request_row = operator.list_g3_approval_requests(limit=5, status="PENDING")[0]
    expired = operator.review_g3_approval_request(
        expired_request_row["request_id"],
        "EXPIRE",
        operator_notes="Timed out in operator testing.",
        reference_time=_ts(now + datetime.timedelta(minutes=15)),
    )

    pending_rows = operator.list_g3_approval_requests(limit=5, status="PENDING")
    recent_requests = observability.recent_g3_approval_requests(limit=10)
    health = observability.system_health()
    digest = operator.generate_digest(digest_type="critical_only", operator_state="ACTIVE")

    assert approved["status"] == "APPROVED"
    assert denied["status"] == "DENIED"
    assert expired["status"] == "EXPIRED"
    assert pending_rows == []
    assert {row["status"] for row in recent_requests[:3]} == {"APPROVED", "DENIED", "EXPIRED"}
    assert health["g3_requests"]["pending_count"] == 0
    assert health["g3_requests"]["approved_24h"] == 1
    assert health["g3_requests"]["denied_24h"] == 1
    assert health["g3_requests"]["expired_24h"] == 1
    assert "g3=pending:0/approved24h:1/denied24h:1/expired24h:1" in digest["content"]


def test_judge_fallback_incidents_surface_in_observability_and_digest(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    operator = OperatorInterfaceSkill(db)
    observability = ObservabilitySkill(db, telemetry_buffer=None, immune_buffer=None)
    immune = db.get_connection("immune")
    now = _now()

    operator.record_heartbeat("command")
    immune.execute(
        """
        INSERT INTO immune_verdicts (
            verdict_id, verdict_type, scan_tier, session_id, skill_name,
            task_type, result, match_pattern, latency_ms, judge_mode, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            "judge_output",
            "fast_path",
            "session-fallback-1",
            "operator_interface",
            "operator_interface",
            "PASS",
            "Judge structural fallback: judge_degraded",
            3,
            "FALLBACK",
            _ts(now),
        ),
    )
    immune.execute(
        """
        INSERT INTO immune_verdicts (
            verdict_id, verdict_type, scan_tier, session_id, skill_name,
            task_type, result, match_pattern, latency_ms, judge_mode, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            "judge_output",
            "fast_path",
            "session-fallback-2",
            "observability",
            "observability",
            "BLOCK",
            "Structural fallback blocked sensitive output",
            4,
            "FALLBACK",
            _ts(now + datetime.timedelta(minutes=1)),
        ),
    )
    immune.commit()

    fallback_rows = observability.recent_fallback_judge_verdicts(limit=5)
    filtered_rows = observability.query_immune_verdicts(limit=5, judge_mode="FALLBACK")
    health = observability.system_health()
    digest = operator.generate_digest(digest_type="critical_only", operator_state="ACTIVE")

    assert fallback_rows[0]["judge_mode"] == "FALLBACK"
    assert filtered_rows[0]["judge_mode"] == "FALLBACK"
    assert health["judge_fallback"]["count"] == 2
    assert health["judge_fallback"]["blocked_count"] == 1
    assert health["recommended_digest_type"] == "critical_only"
    assert "judge_fallback recent=2 blocked=1" in digest["content"]


def test_judge_deadlock_restart_is_explicit_and_retro_reviews_fallback_passes(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    operator = OperatorInterfaceSkill(db)
    observability = ObservabilitySkill(db, telemetry_buffer=None, immune_buffer=None)
    immune = db.get_connection("immune")
    manager = JudgeLifecycleManager(str(test_data_dir / "immune_system.db"), load_config())
    now = _now()

    for skill_name, result, minutes_ago in [
        ("planner", "BLOCK", 5),
        ("research_domain", "BLOCK", 4),
        ("operator_interface", "BLOCK", 3),
        ("observability", "PASS", 2),
        ("council", "BLOCK", 1),
        ("financial_router", "BLOCK", 0),
    ]:
        _insert_normal_judge_verdict(
            immune,
            skill_name=skill_name,
            result=result,
            timestamp=_ts(now - datetime.timedelta(minutes=minutes_ago)),
        )
    immune.commit()

    trigger_payload = JudgePayload(
        session_id="deadlock-trigger",
        skill_name="financial_router",
        tool_name="route",
        output={"secret": "sk-trigger"},
        expected_schema=None,
    )
    trigger_verdict = judge_check(trigger_payload, load_config())
    activation = manager.record_verdict(trigger_payload, trigger_verdict, reference_time=_ts(now))

    status = observability.judge_deadlock_status()
    digest = operator.generate_digest(digest_type="critical_only", operator_state="ACTIVE")

    assert activation is not None
    assert activation["status"] == "ACTIVE"
    assert status["mode"] == "FALLBACK"
    assert status["active_event"]["event_id"] == activation["event_id"]
    assert "judge_deadlock fallback until=" in digest["content"]

    fallback_payload = JudgePayload(
        session_id="deadlock-fallback-pass",
        skill_name="operator_interface",
        tool_name="digest",
        output={"command": "echo safe"},
        expected_schema={
            "type": "object",
            "required": ["ok"],
            "properties": {"ok": {"type": "boolean"}},
        },
    )
    prepared, active = manager.prepare_payload(
        fallback_payload,
        reference_time=_ts(now + datetime.timedelta(minutes=1)),
    )
    fallback_verdict = judge_check(prepared, load_config())
    manager.record_verdict(
        prepared,
        fallback_verdict,
        reference_time=_ts(now + datetime.timedelta(minutes=1)),
    )

    pending_queue = observability.judge_fallback_review_queue(limit=5, review_status="PENDING")
    pending_digest = operator.generate_digest(digest_type="critical_only", operator_state="ACTIVE")

    assert active is not None
    assert active["status"] == "ACTIVE"
    assert fallback_verdict.outcome.value == "PASS"
    assert fallback_verdict.judge_mode.value == "FALLBACK"
    assert pending_queue[0]["review_status"] == "PENDING"
    assert "judge_review pending=1" in pending_digest["content"]

    restarted = operator.restart_judge_after_deadlock(
        event_id=activation["event_id"],
        reference_time=_ts(now + datetime.timedelta(minutes=11)),
    )
    status_after = manager.status(reference_time=_ts(now + datetime.timedelta(minutes=11)))
    blocked_reviews = observability.judge_fallback_review_queue(limit=5, review_status="BLOCK")
    blocked_digest = operator.generate_digest(digest_type="critical_only", operator_state="ACTIVE")

    assert restarted["status"] == "CLEARED"
    assert restarted["review_queue"]["pending"] == 0
    assert restarted["review_queue"]["blocked"] == 1
    assert status_after["mode"] == "NORMAL"
    assert blocked_reviews[0]["review_status"] == "BLOCK"
    assert "judge_review blocked=1" in blocked_digest["content"]


def test_judge_deadlock_fallback_auto_expiry_halts_when_blocks_persist(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    operator = OperatorInterfaceSkill(db)
    immune = db.get_connection("immune")
    manager = JudgeLifecycleManager(str(test_data_dir / "immune_system.db"), load_config())
    now = _now()

    for skill_name, result, minutes_ago in [
        ("planner", "BLOCK", 5),
        ("research_domain", "BLOCK", 4),
        ("operator_interface", "BLOCK", 3),
        ("observability", "PASS", 2),
        ("council", "BLOCK", 1),
        ("financial_router", "BLOCK", 0),
    ]:
        _insert_normal_judge_verdict(
            immune,
            skill_name=skill_name,
            result=result,
            timestamp=_ts(now - datetime.timedelta(minutes=minutes_ago)),
        )
    immune.commit()

    payload = JudgePayload(
        session_id="deadlock-expiry-trigger",
        skill_name="financial_router",
        tool_name="route",
        output={"secret": "sk-trigger"},
        expected_schema=None,
    )
    activation = manager.record_verdict(payload, judge_check(payload, load_config()), reference_time=_ts(now))

    later = now + datetime.timedelta(minutes=31)
    for skill_name, result, minutes_ago in [
        ("planner", "BLOCK", 5),
        ("research_domain", "BLOCK", 4),
        ("operator_interface", "BLOCK", 3),
        ("observability", "PASS", 2),
        ("council", "BLOCK", 1),
        ("financial_router", "BLOCK", 0),
    ]:
        _insert_normal_judge_verdict(
            immune,
            skill_name=skill_name,
            result=result,
            timestamp=_ts(later - datetime.timedelta(minutes=minutes_ago)),
        )
    immune.commit()

    status = manager.status(reference_time=_ts(later))
    events = operator.list_judge_fallback_events(limit=5)

    assert activation is not None
    assert status["mode"] == "HALTED"
    assert events[0]["status"] == "HALTED"
    assert events[0]["end_reason"] == "judge_deadlock_fallback_expired_with_persistent_blocks"


def test_runtime_halt_contract_surfaces_and_allows_audited_restart_after_deadlock_clears(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    operator = OperatorInterfaceSkill(db)
    observability = ObservabilitySkill(db, telemetry_buffer=None, immune_buffer=None)
    immune = db.get_connection("immune")
    manager = JudgeLifecycleManager(str(test_data_dir / "immune_system.db"), load_config())
    now = _now()

    for skill_name, result, minutes_ago in [
        ("planner", "BLOCK", 5),
        ("research_domain", "BLOCK", 4),
        ("operator_interface", "BLOCK", 3),
        ("observability", "PASS", 2),
        ("council", "BLOCK", 1),
        ("financial_router", "BLOCK", 0),
    ]:
        _insert_normal_judge_verdict(
            immune,
            skill_name=skill_name,
            result=result,
            timestamp=_ts(now - datetime.timedelta(minutes=minutes_ago)),
        )
    immune.commit()

    payload = JudgePayload(
        session_id="deadlock-expiry-runtime-trigger",
        skill_name="financial_router",
        tool_name="route",
        output={"secret": "sk-trigger"},
        expected_schema=None,
    )
    activation = manager.record_verdict(payload, judge_check(payload, load_config()), reference_time=_ts(now))

    later = now + datetime.timedelta(minutes=31)
    for skill_name, result, minutes_ago in [
        ("planner", "BLOCK", 5),
        ("research_domain", "BLOCK", 4),
        ("operator_interface", "BLOCK", 3),
        ("observability", "PASS", 2),
        ("council", "BLOCK", 1),
        ("financial_router", "BLOCK", 0),
    ]:
        _insert_normal_judge_verdict(
            immune,
            skill_name=skill_name,
            result=result,
            timestamp=_ts(later - datetime.timedelta(minutes=minutes_ago)),
        )
    immune.commit()

    halted_status = manager.status(reference_time=_ts(later))
    runtime_status = operator.runtime_status()
    health = observability.system_health()
    digest = operator.generate_digest(digest_type="critical_only", operator_state="ACTIVE")

    assert activation is not None
    assert halted_status["mode"] == "HALTED"
    assert runtime_status["lifecycle_state"] == "HALTED"
    assert runtime_status["active_halt"]["source"] == "JUDGE_DEADLOCK"
    assert health["runtime_control"]["lifecycle_state"] == "HALTED"
    assert "runtime HALTED source=JUDGE_DEADLOCK" in digest["content"]

    restarted = operator.restart_runtime_after_halt(
        restart_reason="operator_runtime_restart",
        notes="judge drift corrected",
        reference_time=_ts(now + datetime.timedelta(minutes=40)),
    )
    final_runtime_status = operator.runtime_status()
    restart_history = operator.list_runtime_restart_history(limit=5)

    assert restarted["status"] == "COMPLETED"
    assert restarted["judge_restart"]["status"] == "CLEARED"
    assert restarted["runtime_restart"]["status"] == "COMPLETED"
    assert final_runtime_status["lifecycle_state"] == "ACTIVE"
    assert restart_history[0]["status"] == "COMPLETED"


def test_runtime_restart_attempt_is_blocked_while_deadlock_persists(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    operator = OperatorInterfaceSkill(db)
    immune = db.get_connection("immune")
    manager = JudgeLifecycleManager(str(test_data_dir / "immune_system.db"), load_config())
    now = _now()

    for skill_name, result, minutes_ago in [
        ("planner", "BLOCK", 5),
        ("research_domain", "BLOCK", 4),
        ("operator_interface", "BLOCK", 3),
        ("observability", "PASS", 2),
        ("council", "BLOCK", 1),
        ("financial_router", "BLOCK", 0),
    ]:
        _insert_normal_judge_verdict(
            immune,
            skill_name=skill_name,
            result=result,
            timestamp=_ts(now - datetime.timedelta(minutes=minutes_ago)),
        )
    immune.commit()

    payload = JudgePayload(
        session_id="deadlock-blocked-runtime-trigger",
        skill_name="financial_router",
        tool_name="route",
        output={"secret": "sk-trigger"},
        expected_schema=None,
    )
    manager.record_verdict(payload, judge_check(payload, load_config()), reference_time=_ts(now))

    later = now + datetime.timedelta(minutes=31)
    for skill_name, result, minutes_ago in [
        ("planner", "BLOCK", 5),
        ("research_domain", "BLOCK", 4),
        ("operator_interface", "BLOCK", 3),
        ("observability", "PASS", 2),
        ("council", "BLOCK", 1),
        ("financial_router", "BLOCK", 0),
    ]:
        _insert_normal_judge_verdict(
            immune,
            skill_name=skill_name,
            result=result,
            timestamp=_ts(later - datetime.timedelta(minutes=minutes_ago)),
        )
    immune.commit()
    manager.status(reference_time=_ts(later))

    blocked = operator.restart_runtime_after_halt(
        restart_reason="operator_runtime_restart",
        notes="too early",
        reference_time=_ts(later),
    )
    runtime_status = operator.runtime_status()
    restart_history = operator.list_runtime_restart_history(limit=5)

    assert blocked["status"] == "BLOCKED"
    assert blocked["judge_restart"]["status"] == "HALTED"
    assert blocked["runtime_restart"]["status"] == "BLOCKED"
    assert runtime_status["lifecycle_state"] == "HALTED"
    assert restart_history[0]["status"] == "BLOCKED"


def test_second_judge_deadlock_trigger_inside_guard_halts_and_keeps_fail_closed_posture(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    operator = OperatorInterfaceSkill(db)
    immune = db.get_connection("immune")
    manager = JudgeLifecycleManager(str(test_data_dir / "immune_system.db"), load_config())
    now = _now()

    for skill_name, result, minutes_ago in [
        ("planner", "BLOCK", 5),
        ("research_domain", "BLOCK", 4),
        ("operator_interface", "BLOCK", 3),
        ("observability", "PASS", 2),
        ("council", "BLOCK", 1),
    ]:
        _insert_normal_judge_verdict(
            immune,
            skill_name=skill_name,
            result=result,
            timestamp=_ts(now - datetime.timedelta(minutes=minutes_ago)),
        )
    immune.commit()

    first_payload = JudgePayload(
        session_id="deadlock-first",
        skill_name="financial_router",
        tool_name="route",
        output={"secret": "sk-trigger"},
        expected_schema=None,
    )
    first = manager.record_verdict(first_payload, judge_check(first_payload, load_config()), reference_time=_ts(now))
    operator.restart_judge_after_deadlock(
        event_id=first["event_id"],
        reference_time=_ts(now + datetime.timedelta(minutes=11)),
    )

    second_time = now + datetime.timedelta(hours=12)
    for skill_name, result, minutes_ago in [
        ("planner", "BLOCK", 5),
        ("research_domain", "BLOCK", 4),
        ("operator_interface", "BLOCK", 3),
        ("observability", "PASS", 2),
        ("council", "BLOCK", 1),
    ]:
        _insert_normal_judge_verdict(
            immune,
            skill_name=skill_name,
            result=result,
            timestamp=_ts(second_time - datetime.timedelta(minutes=minutes_ago)),
        )
    immune.commit()

    second_payload = JudgePayload(
        session_id="deadlock-second",
        skill_name="research_domain",
        tool_name="write_brief",
        output={"secret": "sk-trigger-2"},
        expected_schema=None,
    )
    second = manager.record_verdict(
        second_payload,
        judge_check(second_payload, load_config()),
        reference_time=_ts(second_time),
    )
    prepared, active = manager.prepare_payload(
        JudgePayload(
            session_id="deadlock-post-halt",
            skill_name="operator_interface",
            tool_name="digest",
            output={"ok": True},
            expected_schema=None,
        ),
        reference_time=_ts(second_time),
    )
    halted_verdict = manager.halted_verdict(prepared)
    breaker_row = immune.execute(
        """
        SELECT breaker_name, action_taken
        FROM circuit_breaker_log
        WHERE breaker_name = 'JUDGE_DEADLOCK'
        ORDER BY timestamp DESC, event_id DESC
        LIMIT 1
        """
    ).fetchone()

    assert second is not None
    assert second["status"] == "HALTED"
    assert active is not None
    assert active["status"] == "HALTED"
    assert halted_verdict.outcome.value == "BLOCK"
    assert breaker_row["action_taken"] == "FULL_SYSTEM_HALT"
    assert operator.list_judge_fallback_events(limit=5)[0]["status"] == "HALTED"


def test_judge_deadlock_uses_persisted_task_types_not_skill_names(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    immune = db.get_connection("immune")
    manager = JudgeLifecycleManager(str(test_data_dir / "immune_system.db"), load_config())
    now = _now()

    for task_type, result, minutes_ago in [
        ("planning", "BLOCK", 5),
        ("research", "BLOCK", 4),
        ("operations", "BLOCK", 3),
        ("planning", "PASS", 2),
        ("research", "BLOCK", 1),
    ]:
        _insert_normal_judge_verdict(
            immune,
            skill_name="shared_skill",
            task_type=task_type,
            result=result,
            timestamp=_ts(now - datetime.timedelta(minutes=minutes_ago)),
        )
    immune.commit()

    payload = JudgePayload(
        session_id="deadlock-task-type-trigger",
        skill_name="shared_skill",
        tool_name="shared_tool",
        task_type="finance",
        output={"secret": "sk-trigger"},
        expected_schema=None,
    )

    activation = manager.record_verdict(payload, judge_check(payload, load_config()), reference_time=_ts(now))

    assert activation is not None
    assert activation["status"] == "ACTIVE"
    assert activation["distinct_task_types"] == ["finance", "operations", "planning", "research"]


def test_research_domain_can_list_and_complete_tasks(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    research = ResearchDomainSkill(db)

    task_id = research.create_task("Market scan", "Check competitors", tags=["market"])
    pending = research.list_tasks(status="PENDING")
    completed = research.complete_task(task_id, actual_spend_usd=0.0)

    assert pending[0]["task_id"] == task_id
    assert pending[0]["tags"] == ["market"]
    assert completed["status"] == "COMPLETE"


def test_operator_and_observability_surface_harness_variants_and_traces(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    operator = OperatorInterfaceSkill(db)
    observability = ObservabilitySkill(db, None, None)
    manager = HarnessVariantManager(str(test_data_dir / "telemetry.db"))

    manager.log_execution_trace(
        ExecutionTrace(
            trace_id="trace-runtime-1",
            task_id="task-runtime-1",
            role="runtime_contract",
            skill_name="runtime",
            harness_version="v1",
            intent_goal="prove runtime contract",
            steps=[
                ExecutionTraceStep(
                    step_index=1,
                    tool_call="financial_router.route",
                    tool_result='{"tier":"paid_cloud"}',
                    tool_result_file=None,
                    tokens_in=0,
                    tokens_out=0,
                    latency_ms=3,
                    model_used="repo-contract",
                )
            ],
            prompt_template="contract harness",
            context_assembled="runtime+operator",
            retrieval_queries=[],
            judge_verdict="PASS",
            judge_reasoning="ok",
            outcome_score=1.0,
            cost_usd=0.0,
            duration_ms=20,
            training_eligible=True,
            retention_class="STANDARD",
            source_chain_id="chain-runtime-1",
            source_session_id="session-runtime-1",
            source_trace_id=None,
            created_at="2026-04-21T12:00:00+00:00",
        )
    )

    proposed = operator.propose_harness_variant(
        skill_name="research_domain",
        parent_version="abc123",
        diff="@@ -1 +1 @@\n-old\n+new\n",
        reference_time="2026-04-21T12:01:00+00:00",
    )
    shadow = operator.start_harness_variant_shadow_eval(
        variant_id=proposed["variant_id"],
        reference_time="2026-04-21T12:02:00+00:00",
    )
    promoted = operator.record_harness_variant_eval(
        variant_id=proposed["variant_id"],
        benchmark_name="shadow_replay_research_domain",
        baseline_outcome_scores=[0.7, 0.8],
        variant_outcome_scores=[0.8, 0.85],
        regression_rate=0.0,
        gate_0_pass=True,
        known_bad_block_rate=1.0,
        gate_1_pass=True,
        baseline_mean_score=0.75,
        variant_mean_score=0.825,
        quality_delta=0.075,
        gate_2_pass=True,
        baseline_std=0.05,
        variant_std=0.03,
        gate_3_pass=True,
        regressed_trace_count=0,
        improved_trace_count=2,
        net_trace_gain=2,
        traces_evaluated=2,
        compute_cost_cu=1.0,
        eval_duration_ms=120,
        reference_time="2026-04-21T12:03:00+00:00",
    )

    assert shadow["status"] == "SHADOW_EVAL"
    assert promoted["status"] == "PROMOTED"

    traces = operator.list_execution_traces(limit=5, skill_name="runtime")
    variants = operator.list_harness_variants(limit=5, skill_name="research_domain")
    frontier = observability.harness_frontier(limit=5, skill_name="research_domain")
    health = observability.system_health()

    assert traces[0]["trace_id"] == "trace-runtime-1"
    assert variants[0]["variant_id"] == proposed["variant_id"]
    assert frontier[0]["variant_id"] == proposed["variant_id"]
    assert health["harness_variants"]["execution_traces"]["total_count"] == 1
    assert health["harness_variants"]["execution_traces"]["training_eligible_count"] == 1
    assert health["harness_variants"]["variants"]["promoted_count"] == 1


def test_operator_can_run_replay_eval_from_execution_traces(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    operator = OperatorInterfaceSkill(db)
    manager = HarnessVariantManager(str(test_data_dir / "telemetry.db"))

    for idx, score in enumerate((0.71, 0.76, 0.8), start=1):
        manager.log_execution_trace(
            ExecutionTrace(
                trace_id=f"research-trace-{idx}",
                task_id=f"research-task-{idx}",
                role="runtime",
                skill_name="research_domain",
                harness_version="baseline-v1",
                intent_goal="replay baseline",
                steps=[
                    ExecutionTraceStep(
                        step_index=1,
                        tool_call="research_domain_2.create_task",
                        tool_result='{"ok":true}',
                        tool_result_file=None,
                        tokens_in=0,
                        tokens_out=0,
                        latency_ms=4,
                        model_used="local-default",
                    )
                ],
                prompt_template="baseline prompt",
                context_assembled="evidence " * 35,
                retrieval_queries=["query-a", "query-b"],
                judge_verdict="PASS",
                judge_reasoning="ok",
                outcome_score=score,
                cost_usd=0.0,
                duration_ms=22,
                training_eligible=True,
                retention_class="STANDARD",
                source_chain_id=f"research-chain-{idx}",
                source_session_id=f"research-session-{idx}",
                source_trace_id=None,
                created_at=f"2026-04-21T13:0{idx}:00+00:00",
            )
        )

    manager.log_execution_trace(
        ExecutionTrace(
            trace_id="research-known-bad",
            task_id="research-task-bad",
            role="runtime",
            skill_name="research_domain",
            harness_version="baseline-v1",
            intent_goal="replay bad set",
            steps=[],
            prompt_template="baseline prompt",
            context_assembled="evidence " * 10,
            retrieval_queries=["unsafe"],
            judge_verdict="FAIL",
            judge_reasoning="bad",
            outcome_score=0.08,
            cost_usd=0.0,
            duration_ms=8,
            training_eligible=False,
            retention_class="FAILURE_AUDIT",
            source_chain_id="research-chain-bad",
            source_session_id="research-session-bad",
            source_trace_id=None,
            created_at="2026-04-21T13:09:00+00:00",
        )
    )

    proposed = operator.propose_harness_variant(
        skill_name="research_domain",
        parent_version="baseline-v1",
        diff="@@ -1 +1 @@\n-old\n+new\n",
        prompt_prelude="Tighten evidence grounding and clarify the rubric.",
        retrieval_strategy_diff="Use multi-query retrieval and rerank the strongest evidence.",
        scoring_formula_diff="Calibrate thresholds and reward grounded evidence.",
        context_assembly_diff="Compress context and prioritize the highest-signal snippets.",
        reference_time="2026-04-21T13:10:00+00:00",
    )

    evaluated = operator.evaluate_harness_variant_from_traces(
        variant_id=proposed["variant_id"],
        sample_size=10,
        minimum_trace_count=3,
        minimum_known_bad_traces=1,
        reference_time="2026-04-21T13:11:00+00:00",
    )

    assert evaluated["status"] == "PROMOTED"
    assert evaluated["eval_result"]["traces_evaluated"] == 3
    assert evaluated["eval_result"]["known_bad_block_rate"] == 1.0
    assert evaluated["eval_result"]["gate_1_pass"] is True


def test_replay_readiness_summary_surfaces_threshold_gap(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    observability = ObservabilitySkill(db, None, None)
    manager = HarnessVariantManager(str(test_data_dir / "telemetry.db"))

    manager.log_execution_trace(
        ExecutionTrace(
            trace_id="runtime-ready-gap",
            task_id="task-gap-1",
            role="operator_workflow",
            skill_name="runtime",
            harness_version="operator_workflow_v1",
            intent_goal="baseline trace",
            steps=[],
            prompt_template="operator_workflow",
            context_assembled="runtime workflow",
            retrieval_queries=[],
            judge_verdict="PASS",
            judge_reasoning="ok",
            outcome_score=1.0,
            cost_usd=0.0,
            duration_ms=10,
            training_eligible=True,
            retention_class="STANDARD",
            source_chain_id="chain-gap-1",
            source_session_id="session-gap-1",
            source_trace_id=None,
            created_at="2026-04-21T14:00:00+00:00",
        )
    )

    health = observability.system_health()
    replay = health["harness_variants"]["execution_traces"]["replay_readiness"]

    assert replay["status"] == "IMPLEMENTED_BELOW_ACTIVATION_THRESHOLD"
    assert replay["eligible_source_traces"] == 1
    assert replay["known_bad_source_traces"] == 0
    assert replay["distinct_skill_count"] == 1
    assert replay["minimum_eligible_traces"] == 500
    assert replay["minimum_known_bad_traces"] == 25
    assert replay["minimum_distinct_skills"] == 3
    assert replay["blockers"]


def test_operator_and_observability_surface_workspace_overview_and_milestone_health(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    operator = OperatorInterfaceSkill(db)
    observability = ObservabilitySkill(db, None, None)
    manager = HarnessVariantManager(str(test_data_dir / "telemetry.db"))

    manager.log_execution_trace(
        ExecutionTrace(
            trace_id="workspace-trace-1",
            task_id="workspace-task-1",
            role="runtime_contract",
            skill_name="runtime",
            harness_version="contract-v1",
            intent_goal="workspace proof",
            steps=[],
            prompt_template="workspace proof",
            context_assembled="workspace proof",
            retrieval_queries=[],
            judge_verdict="PASS",
            judge_reasoning="ok",
            outcome_score=1.0,
            cost_usd=0.0,
            duration_ms=10,
            training_eligible=True,
            retention_class="STANDARD",
            source_chain_id="workspace-chain-1",
            source_session_id="workspace-session-1",
            source_trace_id=None,
            created_at="2026-04-22T10:00:00+00:00",
        )
    )

    operator_view = operator.workspace_overview()
    observability_view = observability.workspace_overview()

    assert "milestone_health" in operator_view
    assert "runtime_status" in operator_view
    assert "replay_readiness" in operator_view
    assert "milestone_health" in observability_view
    assert "system_health" in observability_view
    assert observability.milestone_health()["milestones"]["M1"]["implemented"] is True


def test_council_tier2_health_surfaces_in_observability_and_digest(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    operator = OperatorInterfaceSkill(db)
    observability = ObservabilitySkill(db, telemetry_buffer=None, immune_buffer=None)
    strategic = db.get_connection("strategic_memory")
    operator_db = db.get_connection("operator_digest")
    now = _now()
    now_ts = _ts(now)

    strategic.execute(
        """
        INSERT INTO council_verdicts (
            verdict_id, tier_used, decision_type, recommendation, confidence,
            reasoning_summary, dissenting_views, minority_positions,
            full_debate_record, cost_usd, project_id, outcome_record,
            da_quality_score, da_assessment, tie_break, degraded,
            confidence_cap, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()), 2, "opportunity_screen", "PURSUE", 0.82,
            "tier2 ok", "risk", json.dumps(["minority"]), "record", 0.0, None, None,
            0.5, json.dumps([{"objection": "risk", "tag": "acknowledged", "reasoning": "tracked"}]), 0, 0, None, now_ts,
        ),
    )
    strategic.execute(
        """
        INSERT INTO council_verdicts (
            verdict_id, tier_used, decision_type, recommendation, confidence,
            reasoning_summary, dissenting_views, minority_positions,
            full_debate_record, cost_usd, project_id, outcome_record,
            da_quality_score, da_assessment, tie_break, degraded,
            confidence_cap, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()), 1, "opportunity_screen", "PURSUE", 0.68,
            "degraded tier1", "risk", None, None, 0.0, None, None,
            0.5, json.dumps([{"objection": "risk", "tag": "acknowledged", "reasoning": "tracked"}]), 0, 1, 0.70, now_ts,
        ),
    )
    operator_db.execute(
        """
        INSERT INTO gate_log (
            gate_id, gate_type, trigger_description, context_packet, project_id,
            status, timeout_hours, operator_response, created_at, responded_at, expires_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()), "G3", "council_tier2:opportunity_screen:subject-1", json.dumps({"x": 1}), None,
            "PENDING", 6.0, None, now_ts, None, now_ts,
        ),
    )
    operator_db.execute(
        """
        INSERT INTO alert_log (
            alert_id, tier, alert_type, content, channel_delivered,
            suppressed, acknowledged, acknowledged_at, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()), "T2", "COUNCIL_BACKLOG", "Tier 2 backlog depth reached 4.", "CLI",
            0, 0, None, now_ts,
        ),
    )
    strategic.commit()
    operator_db.commit()

    health = observability.system_health()
    digest = operator.generate_digest(digest_type="critical_only", operator_state="ACTIVE")

    assert health["council_health"]["tier2_24h"] == 1
    assert health["council_health"]["degraded_24h"] == 1
    assert health["council_health"]["pending_tier2_g3"] == 1
    assert health["council_health"]["backlog_alerts_24h"] == 1
    assert "council=tier2_24h:1/degraded24h:1" in digest["content"]
    assert "council_g3 pending=1" in digest["content"]
    assert "council_backlog alerts24h=1" in digest["content"]
