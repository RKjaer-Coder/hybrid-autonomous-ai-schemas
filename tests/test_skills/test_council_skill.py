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
