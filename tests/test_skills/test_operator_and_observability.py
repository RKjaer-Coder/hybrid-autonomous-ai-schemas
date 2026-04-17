from __future__ import annotations

import datetime
import json
import uuid

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
    result: str,
    timestamp: str,
) -> None:
    immune.execute(
        """
        INSERT INTO immune_verdicts (
            verdict_id, verdict_type, scan_tier, session_id, skill_name,
            result, match_pattern, latency_ms, timestamp, judge_mode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            "judge_output",
            "fast_path",
            f"session-{skill_name}-{result}-{timestamp}",
            skill_name,
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
            result, match_pattern, latency_ms, judge_mode, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            "judge_output",
            "fast_path",
            "session-fallback-1",
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
            result, match_pattern, latency_ms, judge_mode, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            "judge_output",
            "fast_path",
            "session-fallback-2",
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


def test_research_domain_can_list_and_complete_tasks(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    research = ResearchDomainSkill(db)

    task_id = research.create_task("Market scan", "Check competitors", tags=["market"])
    pending = research.list_tasks(status="PENDING")
    completed = research.complete_task(task_id, actual_spend_usd=0.0)

    assert pending[0]["task_id"] == task_id
    assert pending[0]["tags"] == ["market"]
    assert completed["status"] == "COMPLETE"
