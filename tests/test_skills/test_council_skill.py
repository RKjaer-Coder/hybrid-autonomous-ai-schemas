from __future__ import annotations

import pytest

from harness_variants import HarnessVariantManager
from skills.council.skill import CouncilSkill
from skills.db_manager import DatabaseManager
from skills.hermes_interfaces import MockHermesRuntime


def _seed_delegate(rt: MockHermesRuntime):
    rt.set_mock_response("delegate:strategist", '{"role":"strategist","case_for":"x","market_fit_score":0.7,"timing_assessment":"x","strategic_alignment":"x","key_assumption":"x"}')
    rt.set_mock_response("delegate:critic", '{"role":"critic","case_against":"x","execution_risk":"x","market_risk":"x","fatal_dependency":"x","risk_severity":0.6}')
    rt.set_mock_response("delegate:realist", '{"role":"realist","execution_requirements":"x","compute_needs":"x","time_to_revenue_days":30,"capital_required_usd":1000,"blocking_prerequisite":"x","feasibility_score":0.7}')
    rt.set_mock_response("delegate:devils_advocate", '{"role":"devils_advocate","shared_assumption":"x","novel_risk":"x","material_disagreement":"x","alternative_interpretation":"x"}')
    rt.set_mock_response("delegate:synthesis", '{"tier_used":1,"decision_type":"opportunity_screen","recommendation":"PURSUE","confidence":0.75,"reasoning_summary":"ok","dissenting_views":"risk","da_assessment":[{"objection":"x","tag":"acknowledged","reasoning":"x"}],"tie_break":false,"risk_watch":[]}')


def _seed_mixture(rt: MockHermesRuntime):
    rt.set_mock_response(
        "mixture",
        {
            "tier_used": 2,
            "decision_type": "opportunity_screen",
            "recommendation": "PURSUE",
            "confidence": 0.81,
            "reasoning_summary": "Tier 2 confirmed the opportunity while keeping a strong distribution-risk dissent.",
            "dissenting_views": "Distribution dependence could compress margin before owned channels mature.",
            "minority_positions": ["Reject until owned distribution is proven."],
            "full_debate_record": "Round1 split 2/1. Round2 focused on channel dependency. Round3 synthesized pursue with explicit risk watch.",
            "cost_usd": 0.0,
            "da_assessment": [{"objection": "Distribution dependence", "tag": "acknowledged", "reasoning": "Retained as core dissent."}],
            "tie_break": False,
            "risk_watch": ["Owned distribution progress"],
        },
    )


def test_tier1_deliberation_runs_and_persists(test_data_dir):
    rt = MockHermesRuntime(data_dir=str(test_data_dir))
    _seed_delegate(rt)
    db = DatabaseManager(str(test_data_dir))
    s = CouncilSkill(rt, db)
    verdict = s.deliberate("opportunity_screen", "subj-1", "small context")
    assert verdict.tier_used == 1
    conn = db.get_connection("strategic_memory")
    n = conn.execute("SELECT COUNT(*) FROM council_verdicts").fetchone()[0]
    assert n == 1
    traces = HarnessVariantManager(str(test_data_dir / "telemetry.db")).list_execution_traces(limit=5, skill_name="council")
    assert traces[0]["role"] == "council_deliberation"
    assert traces[0]["judge_verdict"] == "PASS"


def test_context_budget_enforced(test_data_dir):
    rt = MockHermesRuntime(data_dir=str(test_data_dir))
    _seed_delegate(rt)
    db = DatabaseManager(str(test_data_dir))
    s = CouncilSkill(rt, db)
    with pytest.raises(ValueError):
        s.deliberate("opportunity_screen", "subj-1", "word " * 5000)
    traces = HarnessVariantManager(str(test_data_dir / "telemetry.db")).list_execution_traces(limit=5, skill_name="council")
    assert traces[0]["judge_verdict"] == "FAIL"
    assert traces[0]["retention_class"] == "FAILURE_AUDIT"


def test_auto_escalation_confidence_lt_point_60(test_data_dir):
    rt = MockHermesRuntime(data_dir=str(test_data_dir))
    _seed_delegate(rt)
    rt.set_mock_response("delegate:synthesis", '{"tier_used":1,"decision_type":"opportunity_screen","recommendation":"PURSUE","confidence":0.55,"reasoning_summary":"ok","dissenting_views":"risk","da_assessment":[{"objection":"x","tag":"acknowledged","reasoning":"x"}],"tie_break":false,"risk_watch":[]}')
    db = DatabaseManager(str(test_data_dir))
    s = CouncilSkill(rt, db)
    verdict = s.deliberate("opportunity_screen", "subj-1", "small context")
    assert verdict.recommendation.value == "ESCALATE"


def test_tier2_runs_when_requested_and_persists_extended_fields(test_data_dir):
    rt = MockHermesRuntime(data_dir=str(test_data_dir))
    _seed_delegate(rt)
    _seed_mixture(rt)
    db = DatabaseManager(str(test_data_dir))
    s = CouncilSkill(rt, db)
    verdict = s.deliberate(
        "opportunity_screen",
        "subj-2",
        "small context",
        deliberation_config={
            "requested_tier": 2,
            "tier2_models": ["local-a", "free-b", "frontier-c"],
        },
    )
    assert verdict.tier_used == 2
    row = db.get_connection("strategic_memory").execute(
        "SELECT tier_used, degraded, confidence_cap, full_debate_record FROM council_verdicts WHERE verdict_id = ?",
        (verdict.verdict_id,),
    ).fetchone()
    assert row["tier_used"] == 2
    assert row["degraded"] == 0
    assert row["confidence_cap"] is None
    assert row["full_debate_record"]


def test_tier2_pending_g3_creates_gate_and_returns_escalate(test_data_dir):
    rt = MockHermesRuntime(data_dir=str(test_data_dir))
    _seed_delegate(rt)
    _seed_mixture(rt)
    db = DatabaseManager(str(test_data_dir))
    s = CouncilSkill(rt, db)
    verdict = s.deliberate(
        "opportunity_screen",
        "subj-3",
        "small context",
        deliberation_config={
            "requested_tier": 2,
            "tier2_models": ["local-a", "paid-b"],
            "tier2_estimated_cost_usd": 3.25,
        },
    )
    assert verdict.recommendation.value == "ESCALATE"
    operator = db.get_connection("operator_digest")
    pending_gate = operator.execute(
        "SELECT gate_type, status FROM gate_log WHERE trigger_description = ?",
        ("council_tier2:opportunity_screen:subj-3",),
    ).fetchone()
    alert = operator.execute(
        "SELECT alert_type FROM alert_log WHERE alert_type = 'COUNCIL_TIER2_G3_PENDING'",
    ).fetchone()
    assert pending_gate["gate_type"] == "G3"
    assert pending_gate["status"] == "PENDING"
    assert alert["alert_type"] == "COUNCIL_TIER2_G3_PENDING"


def test_tier2_denied_g3_degrades_tier1_and_persists_flags(test_data_dir):
    rt = MockHermesRuntime(data_dir=str(test_data_dir))
    _seed_delegate(rt)
    _seed_mixture(rt)
    db = DatabaseManager(str(test_data_dir))
    s = CouncilSkill(rt, db)
    verdict = s.deliberate(
        "opportunity_screen",
        "subj-4",
        "small context",
        deliberation_config={
            "requested_tier": 2,
            "tier2_models": ["local-a", "paid-b"],
            "tier2_estimated_cost_usd": 3.25,
            "g3_status": "DENIED",
        },
    )
    assert verdict.tier_used == 1
    assert verdict.degraded is True
    assert verdict.confidence <= 0.70
    row = db.get_connection("strategic_memory").execute(
        "SELECT degraded, confidence_cap FROM council_verdicts WHERE verdict_id = ?",
        (verdict.verdict_id,),
    ).fetchone()
    assert row["degraded"] == 1
    assert row["confidence_cap"] == 0.70
