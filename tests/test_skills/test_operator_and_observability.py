from __future__ import annotations

import datetime
import json
import uuid

from skills.db_manager import DatabaseManager
from skills.observability.skill import ObservabilitySkill
from skills.operator_interface.skill import OperatorInterfaceSkill
from skills.research_domain.skill import ResearchDomainSkill
from skills.strategic_memory.skill import StrategicMemorySkill


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)


def _ts(value: datetime.datetime) -> str:
    return value.isoformat()


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
    council = observability.query_council_verdicts(limit=5, decision_type="phase_gate")
    reliability = observability.reliability_dashboard()
    breakers = observability.circuit_breaker_status()
    health = observability.system_health()
    digests = observability.recent_digests(limit=5, digest_type="critical_only")

    assert alerts[0]["alert_type"] == "EXECUTOR_SATURATION"
    assert council[0]["verdict_id"] == "verdict-1"
    assert reliability["chains"][0]["chain_type"] == "operator_workflow"
    assert reliability["critical_steps"][0]["step_type"] == "digest"
    assert breakers["critical"] == ["digest/operator_interface"]
    assert health["heartbeat_state"] == "CONSERVATIVE"
    assert health["recommended_digest_type"] == "catch_up"
    assert health["operator_load"]["critical_only_recommended"] is True
    assert health["research_health"]["stale_tasks"] == 1
    assert digests[0]["digest_id"] == critical_digest["digest_id"]


def test_research_domain_can_list_and_complete_tasks(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    research = ResearchDomainSkill(db)

    task_id = research.create_task("Market scan", "Check competitors", tags=["market"])
    pending = research.list_tasks(status="PENDING")
    completed = research.complete_task(task_id, actual_spend_usd=0.0)

    assert pending[0]["task_id"] == task_id
    assert pending[0]["tags"] == ["market"]
    assert completed["status"] == "COMPLETE"
