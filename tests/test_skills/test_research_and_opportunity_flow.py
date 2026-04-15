from __future__ import annotations

import datetime
import json

from skills.council.skill import configure_skill as configure_council_skill
from skills.db_manager import DatabaseManager
from skills.hermes_interfaces import MockHermesRuntime
from skills.opportunity_pipeline.skill import OpportunityPipelineSkill
from skills.research_domain.skill import ResearchDomainSkill
from skills.strategic_memory.skill import StrategicMemorySkill


def _ts(value: datetime.datetime) -> str:
    return value.replace(microsecond=0).isoformat()


def _seed_council(runtime: MockHermesRuntime) -> None:
    runtime.set_mock_response("delegate:strategist", '{"role":"strategist","case_for":"x","market_fit_score":0.7,"timing_assessment":"x","strategic_alignment":"x","key_assumption":"x"}')
    runtime.set_mock_response("delegate:critic", '{"role":"critic","case_against":"x","execution_risk":"x","market_risk":"x","fatal_dependency":"x","risk_severity":0.6}')
    runtime.set_mock_response("delegate:realist", '{"role":"realist","execution_requirements":"x","compute_needs":"x","time_to_revenue_days":30,"capital_required_usd":0,"blocking_prerequisite":"x","feasibility_score":0.7}')
    runtime.set_mock_response("delegate:devils_advocate", '{"role":"devils_advocate","shared_assumption":"x","novel_risk":"x","material_disagreement":"x","alternative_interpretation":"x"}')
    runtime.set_mock_response("delegate:synthesis", '{"tier_used":1,"decision_type":"opportunity_screen","recommendation":"PURSUE","confidence":0.74,"reasoning_summary":"ok","dissenting_views":"watch drift","da_assessment":[{"objection":"x","tag":"acknowledged","reasoning":"x"}],"tie_break":false,"risk_watch":["brief drift"]}')


def test_research_task_lifecycle_and_brief_completion(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    research = ResearchDomainSkill(db)
    memory = StrategicMemorySkill(db)

    task_id = research.create_task(
        "Market scan",
        "Check competitors",
        priority="P1_HIGH",
        tags=["market", "priority"],
        max_spend_usd=3.0,
        stale_after="2026-04-20T00:00:00+00:00",
    )
    started = research.start_task(task_id)
    stale = research.mark_stale(task_id)
    restarted = research.start_task(task_id)
    brief_id = memory.write_brief(
        task_id,
        "Competitor Brief",
        "Summary",
        actionability="ACTION_REQUIRED",
        urgency="CRITICAL",
        depth_tier="FULL",
        source_urls=["https://example.com/a", "https://api.example.com/b"],
        source_assessments=[
            {"url": "https://example.com/a", "relevance": 0.8, "freshness": "2026-04-14", "source_type": "tier2_web"},
            {"url": "https://api.example.com/b", "relevance": 0.9, "freshness": "2026-04-14", "source_type": "tier1_api"},
        ],
        uncertainty_statement="We still do not know whether the observed demand is broad enough to persist after launch.",
        counter_thesis="The strongest reason this could fail is that the detected need may be a niche artifact of one customer segment.",
        spawned_tasks=["follow-up-1"],
        provenance_links=["signal-1"],
    )
    completed = research.complete_task(
        task_id,
        output_brief_id=brief_id,
        actual_spend_usd=1.25,
        follow_up_tasks=["follow-up-1"],
    )
    fetched = research.get_task(task_id)

    assert started["status"] == "ACTIVE"
    assert stale["status"] == "STALE"
    assert restarted["status"] == "ACTIVE"
    assert completed["status"] == "COMPLETE"
    assert completed["output_brief_id"] == brief_id
    assert completed["actual_spend_usd"] == 1.25
    assert completed["follow_up_tasks"] == ["follow-up-1"]
    assert fetched["depth_upgrade"] is True


def test_research_task_rejects_invalid_completion_brief(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    research = ResearchDomainSkill(db)
    memory = StrategicMemorySkill(db)

    task_id = research.create_task("Task A", "Brief A")
    other_task_id = research.create_task("Task B", "Brief B")
    other_brief_id = memory.write_brief(other_task_id, "Other Brief", "Summary")

    research.start_task(task_id)

    try:
        research.complete_task(task_id, output_brief_id=other_brief_id)
    except ValueError as exc:
        assert "does not belong" in str(exc)
    else:
        raise AssertionError("expected mismatched brief validation failure")


def test_opportunity_pipeline_handoff_and_project_backpropagation(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    pipeline = OpportunityPipelineSkill(db)
    financial = db.get_connection("financial_ledger")

    opportunity_id = pipeline.create_opportunity(
        "Client automation offer",
        "Build a reusable client-work package",
        income_mechanism="client_work",
        detected_by="research_prompted",
        cashflow_estimate={"low": 500, "mid": 1200, "high": 2000, "currency": "USD", "period": "month"},
        provenance_links=["brief-1", "brief-2"],
    )
    pipeline.transition_opportunity(opportunity_id, "SCREENED")
    pipeline.transition_opportunity(opportunity_id, "QUALIFIED")
    pipeline.transition_opportunity(
        opportunity_id,
        "IN_VALIDATION",
        validation_spend=0.0,
        validation_report="Local validation underway.",
    )
    pipeline.transition_opportunity(
        opportunity_id,
        "GO_NO_GO",
        validation_report="Validation complete.",
    )
    handoff = pipeline.handoff_to_project(opportunity_id, project_name="Automation Package")
    closed = pipeline.close_from_project(
        handoff["project_id"],
        project_status="COMPLETE",
        learning_record={"result": "positive", "note": "Reusable package confirmed"},
    )

    project = financial.execute(
        "SELECT status, opportunity_id, name FROM projects WHERE project_id = ?",
        (handoff["project_id"],),
    ).fetchone()
    phases = financial.execute(
        "SELECT name, status FROM phases WHERE project_id = ? ORDER BY sequence ASC",
        (handoff["project_id"],),
    ).fetchall()

    assert handoff["opportunity"]["status"] == "ACTIVE"
    assert handoff["opportunity"]["project_id"] == handoff["project_id"]
    assert project["status"] == "COMPLETE"
    assert project["opportunity_id"] == opportunity_id
    assert project["name"] == "Automation Package"
    assert [row["name"] for row in phases] == ["VALIDATE", "BUILD", "DEPLOY", "OPERATE"]
    assert phases[0]["status"] == "ACTIVE"
    assert closed["status"] == "CLOSED"
    assert closed["learning_record"]["result"] == "positive"


def test_phase_gate_context_packet_continue_and_outcome_backpropagation(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    pipeline = OpportunityPipelineSkill(db)
    financial = db.get_connection("financial_ledger")
    strategic = db.get_connection("strategic_memory")
    now = datetime.datetime.now(datetime.timezone.utc)

    verdict_id = "verdict-go"
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
            verdict_id,
            1,
            "go_no_go",
            "PURSUE",
            0.74,
            "Proceed.",
            None,
            "[]",
            None,
            0.0,
            "project-1",
            None,
            None,
            "[]",
            0,
            _ts(now),
        ),
    )
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
            "software_product",
            "Test thesis",
            json.dumps({"cashflow_target_usd": 1000, "primary": "cashflow_target"}),
            json.dumps({"max_executor_hours": 40, "max_cloud_spend_usd": 20}),
            0.2,
            "ACTIVE",
            0,
            900.0,
            verdict_id,
            "[]",
            _ts(now - datetime.timedelta(days=2)),
            None,
        ),
    )
    financial.execute(
        """
        INSERT INTO phases (
            phase_id, project_id, name, status, sequence, scope,
            success_criteria, compute_budget, compute_consumed, outputs,
            gate_result, started_at, gate_triggered_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "phase-validate",
            "project-1",
            "VALIDATE",
            "ACTIVE",
            0,
            "Validate the thesis with customer evidence.",
            json.dumps(["customer evidence"]),
            json.dumps({"executor_hours_cap": 10, "cloud_spend_cap_usd": 0}),
            json.dumps({"executor_hours": 9.8, "cloud_spend_usd": 0}),
            json.dumps(["interviews", "notes"]),
            None,
            _ts(now - datetime.timedelta(days=1)),
            None,
            None,
        ),
    )
    financial.execute(
        """
        INSERT INTO phases (
            phase_id, project_id, name, status, sequence, scope,
            success_criteria, compute_budget, compute_consumed, outputs,
            gate_result, started_at, gate_triggered_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "phase-build",
            "project-1",
            "BUILD",
            "PENDING",
            1,
            "Build the first artifact.",
            json.dumps(["artifact"]),
            json.dumps({"executor_hours_cap": 10, "cloud_spend_cap_usd": 0}),
            json.dumps({"executor_hours": 0, "cloud_spend_usd": 0}),
            json.dumps([]),
            None,
            None,
            None,
            None,
        ),
    )
    financial.execute(
        """
        INSERT INTO assets (
            asset_id, project_id, asset_type, name, description, reusable, location, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("asset-1", "project-1", "template", "Interview Script", "Reusable interview script", 1, "/tmp/script.md", _ts(now)),
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
            "software_product",
            "Alpha",
            "Test thesis",
            "operator",
            verdict_id,
            0.0,
            None,
            json.dumps({"low": 500, "mid": 1000, "high": 1500, "currency": "USD", "period": "month"}),
            "ACTIVE",
            "project-1",
            None,
            "[]",
            0,
            2,
            _ts(now - datetime.timedelta(days=2)),
            _ts(now),
        ),
    )
    financial.commit()
    strategic.commit()

    triggered = pipeline.trigger_phase_gate(
        "project-1",
        "BUDGET_EXHAUSTED",
        outputs_summary="Strong interview signal and one reusable asset created.",
        cashflow_forecast_usd=1000.0,
        kill_score_current=0.46,
        kill_signals=[{"signal_type": "cashflow_vs_forecast", "raw_score": 0.5}, {"signal_type": "asset_creation", "raw_score": 0.0}],
    )
    applied = pipeline.apply_phase_gate_verdict(
        "project-1",
        "CONTINUE",
        0.61,
        "Validation passed, but stay in watch mode for the next phase.",
        gate_id=triggered["gate_id"],
    )
    closed = pipeline.close_from_project(
        "project-1",
        project_status="COMPLETE",
        learning_record={"result": "positive"},
    )

    validate_phase = financial.execute(
        "SELECT status, gate_result FROM phases WHERE phase_id = ?",
        ("phase-validate",),
    ).fetchone()
    build_phase = financial.execute(
        "SELECT status FROM phases WHERE phase_id = ?",
        ("phase-build",),
    ).fetchone()
    project = financial.execute(
        "SELECT status, kill_score_watch FROM projects WHERE project_id = ?",
        ("project-1",),
    ).fetchone()
    gate_log = db.get_connection("operator_digest").execute(
        "SELECT gate_type, status FROM gate_log WHERE gate_id = ?",
        (triggered["gate_id"],),
    ).fetchone()
    verdict = strategic.execute(
        "SELECT outcome_record FROM council_verdicts WHERE verdict_id = ?",
        (verdict_id,),
    ).fetchone()
    calibration = strategic.execute(
        "SELECT predicted_outcome, actual_outcome, prediction_correct FROM calibration_records WHERE verdict_id = ?",
        (verdict_id,),
    ).fetchone()

    assert triggered["council_tier"] == "TIER_2"
    assert "outputs_summary" in triggered["context_packet"]
    assert validate_phase["status"] == "COMPLETE"
    assert json.loads(validate_phase["gate_result"])["verdict"] == "CONTINUE"
    assert build_phase["status"] == "ACTIVE"
    assert applied["project"]["status"] == "ACTIVE"
    assert project["status"] == "COMPLETE"
    assert project["kill_score_watch"] == 1
    assert gate_log["gate_type"] == "G1"
    assert gate_log["status"] == "APPROVED"
    assert json.loads(verdict["outcome_record"])["actual_outcome"] == 1.0
    assert calibration["predicted_outcome"] == "PURSUE"
    assert calibration["prediction_correct"] == 1.0
    assert closed["learning_record"]["operator_learning_record"]["result"] == "positive"


def test_phase_gate_pause_kill_and_resume_flow(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    pipeline = OpportunityPipelineSkill(db)
    financial = db.get_connection("financial_ledger")
    strategic = db.get_connection("strategic_memory")
    operator = db.get_connection("operator_digest")
    now = datetime.datetime.now(datetime.timezone.utc)

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
            "project-2",
            "opp-2",
            "Beta",
            "client_work",
            "Service thesis",
            json.dumps({"cashflow_target_usd": 500}),
            json.dumps({"max_executor_hours": 20}),
            0.3,
            "ACTIVE",
            0,
            50.0,
            None,
            "[]",
            _ts(now - datetime.timedelta(days=3)),
            None,
        ),
    )
    financial.execute(
        """
        INSERT INTO phases (
            phase_id, project_id, name, status, sequence, scope,
            success_criteria, compute_budget, compute_consumed, outputs,
            gate_result, started_at, gate_triggered_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "phase-ops",
            "project-2",
            "OPERATE",
            "ACTIVE",
            3,
            "Operate the client system.",
            json.dumps(["revenue"]),
            json.dumps({"executor_hours_cap": 20, "cloud_spend_cap_usd": 5}),
            json.dumps({"executor_hours": 18, "cloud_spend_usd": 4.5}),
            json.dumps(["one invoice"]),
            None,
            _ts(now - datetime.timedelta(days=1)),
            None,
            None,
        ),
    )
    financial.execute(
        """
        INSERT INTO assets (
            asset_id, project_id, asset_type, name, description, reusable, location, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("asset-beta", "project-2", "tool", "Client Tooling", "Reusable tool", 1, "/tmp/tool.py", _ts(now)),
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
            "opp-2",
            "client_work",
            "Beta",
            "Service thesis",
            "research_prompted",
            None,
            0.0,
            None,
            json.dumps({"low": 200, "mid": 500, "high": 800, "currency": "USD", "period": "month"}),
            "ACTIVE",
            "project-2",
            None,
            "[]",
            0,
            2,
            _ts(now - datetime.timedelta(days=3)),
            _ts(now),
        ),
    )
    financial.commit()
    strategic.commit()

    pause_gate = pipeline.trigger_phase_gate(
        "project-2",
        "BLOCKER",
        outputs_summary="A blocker prevents further movement.",
        kill_score_current=0.20,
    )
    paused = pipeline.apply_phase_gate_verdict(
        "project-2",
        "PAUSE",
        0.72,
        "Pause until the blocker is resolved.",
        gate_id=pause_gate["gate_id"],
    )
    resumed = pipeline.resume_project("project-2", reason="blocker_resolved")
    kill_gate = pipeline.trigger_phase_gate(
        "project-2",
        "BUDGET_EXHAUSTED",
        outputs_summary="Runway is nearly exhausted and performance is weak.",
        kill_score_current=0.71,
        kill_signals=[{"signal_type": "cashflow_vs_forecast", "raw_score": 1.0}, {"signal_type": "technical_blocker", "raw_score": 1.0}],
    )
    kill_result = pipeline.apply_phase_gate_verdict(
        "project-2",
        "KILL_RECOMMEND",
        0.82,
        "The project is no longer economically justified.",
        gate_id=kill_gate["gate_id"],
        failure_analysis="Weak cashflow and unresolved blockers.",
    )

    project = financial.execute(
        "SELECT status FROM projects WHERE project_id = ?",
        ("project-2",),
    ).fetchone()
    g2_gate = operator.execute(
        "SELECT gate_type, status FROM gate_log WHERE gate_id = ?",
        (kill_result["g2_gate_id"],),
    ).fetchone()
    recommendation = financial.execute(
        "SELECT g2_status, thesis_summary FROM kill_recommendations WHERE recommendation_id = ?",
        (kill_result["recommendation_id"],),
    ).fetchone()

    assert paused["project"]["status"] == "PAUSED"
    assert resumed["project"]["status"] == "ACTIVE"
    assert project["status"] == "KILL_RECOMMENDED"
    assert g2_gate["gate_type"] == "G2"
    assert g2_gate["status"] == "PENDING"
    assert recommendation["g2_status"] == "PENDING"
    assert "Service thesis" in recommendation["thesis_summary"]


def test_route_brief_creates_harvest_and_qualified_opportunity_actions(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    research = ResearchDomainSkill(db)
    memory = StrategicMemorySkill(db)
    operator = db.get_connection("operator_digest")
    now = datetime.datetime.now(datetime.timezone.utc)
    runtime = MockHermesRuntime(data_dir=str(test_data_dir))
    _seed_council(runtime)
    configure_council_skill(runtime, db)

    operator.execute(
        "INSERT INTO operator_heartbeat VALUES (?, ?, ?, ?)",
        ("hb-1", "message", "CLI", _ts(now - datetime.timedelta(hours=2))),
    )
    operator.commit()

    harvest_task = research.create_task("Find hidden info", "Need subscription-only data", priority="P1_HIGH")
    harvest_brief_id = memory.write_brief(
        harvest_task,
        "Subscription-only gap",
        "We need one missing input from a subscription interface.",
        actionability="HARVEST_NEEDED",
        action_type="operator_surface",
    )
    harvest_route = research.route_task_output(
        harvest_task,
        target_interface="Claude Pro web",
        harvest_prompt="Ask Claude Pro for the hidden data.",
    )

    opp_task = research.create_task("Spot opportunity", "Look for a product angle", priority="P1_HIGH", tags=["market"])
    opp_brief_id = memory.write_brief(
        opp_task,
        "Qualified opportunity",
        "A reusable software product looks viable.",
        actionability="ACTION_RECOMMENDED",
        action_type="opportunity_feed",
        depth_tier="FULL",
        tags=["market", "product"],
        source_urls=["https://example.com/a", "https://api.example.com/b"],
        source_assessments=[
            {"url": "https://example.com/a", "relevance": 0.8, "freshness": "2026-04-14", "source_type": "tier2_web"},
            {"url": "https://api.example.com/b", "relevance": 0.9, "freshness": "2026-04-14", "source_type": "tier1_api"},
        ],
        uncertainty_statement="There is still uncertainty around conversion quality and time to first sale across segments.",
        counter_thesis="The strongest reason this could fail is that a credible alternative may already own the narrowest high-intent segment.",
    )
    opp_route = research.route_task_output(opp_task, include_council_review=True)

    held_task = research.create_task("Weakly sourced idea", "Need corroboration", priority="P2_NORMAL")
    held_brief_id = memory.write_brief(
        held_task,
        "Held opportunity",
        "Interesting idea, but only one source so far.",
        confidence=0.91,
        actionability="ACTION_RECOMMENDED",
        action_type="opportunity_feed",
        depth_tier="FULL",
        source_urls=["https://example.com/solo"],
        source_assessments=[
            {"url": "https://example.com/solo", "relevance": 0.9, "freshness": "2026-04-14", "source_type": "tier2_web"},
        ],
        uncertainty_statement="This still lacks market breadth evidence even though the single source is recent and strong.",
        counter_thesis="The clearest reason to doubt this idea is that one source can easily overstate localized demand.",
    )
    held_route = research.route_task_output(held_task)

    harvest_rows = operator.execute(
        "SELECT target_interface, status FROM harvest_requests WHERE task_id = ?",
        (harvest_task,),
    ).fetchall()
    opportunity = db.get_connection("strategic_memory").execute(
        "SELECT status, council_verdict_id FROM opportunity_records WHERE opportunity_id = ?",
        (opp_route["actions"][0]["opportunity_id"],),
    ).fetchone()
    held_task_row = db.get_connection("strategic_memory").execute(
        "SELECT follow_up_tasks FROM research_tasks WHERE task_id = ?",
        (held_task,),
    ).fetchone()

    assert harvest_route["brief_id"] == harvest_brief_id
    assert harvest_route["actions"][0]["type"] == "harvest_request_created"
    assert harvest_rows[0]["target_interface"] == "Claude Pro web"
    assert harvest_rows[0]["status"] == "PENDING"
    assert opp_route["brief_id"] == opp_brief_id
    assert opp_route["actions"][0]["type"] == "opportunity_created"
    assert opp_route["actions"][1]["type"] == "council_review_created"
    assert opportunity["status"] == "QUALIFIED"
    assert opportunity["council_verdict_id"] == opp_route["actions"][1]["verdict_id"]
    assert held_route["brief_id"] == held_brief_id
    assert held_route["actions"][0]["type"] == "opportunity_deferred"
    assert json.loads(held_task_row["follow_up_tasks"]) == [held_route["actions"][0]["follow_up_task_id"]]
