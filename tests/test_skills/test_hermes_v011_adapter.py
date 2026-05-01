from __future__ import annotations

from immune.types import AlertSeverity, BlockReason, CheckType, ImmuneConfig, ImmuneVerdict, Outcome, Tier, generate_uuid_v7
from skills.hermes_v011_adapter import ApprovalRequest, ApprovalResponse, HermesV011PreToolCallAdapter, PreToolCallRequest


def test_pre_tool_adapter_vetoes_paid_call_without_g3_budget_before_dispatch():
    adapter = HermesV011PreToolCallAdapter(
        config=ImmuneConfig(known_tool_registry=frozenset({"paid_model_call"}))
    )

    decision = adapter.pre_tool_call(
        PreToolCallRequest(
            session_id=generate_uuid_v7(),
            skill_name="financial_router",
            tool_name="paid_model_call",
            arguments={"billing_tier": "paid_cloud"},
            jwt_claims={"max_api_spend_usd": 0.0, "current_session_spend_usd": 0.0},
            estimated_cost_usd=0.01,
        )
    )

    assert decision.allow is False
    assert decision.reason == "g3_veto:no_session_budget"


def test_pre_tool_adapter_allows_paid_call_inside_session_and_project_budget():
    adapter = HermesV011PreToolCallAdapter(
        config=ImmuneConfig(known_tool_registry=frozenset({"paid_model_call"}))
    )

    decision = adapter.pre_tool_call(
        PreToolCallRequest(
            session_id=generate_uuid_v7(),
            skill_name="financial_router",
            tool_name="paid_model_call",
            arguments={"billing_tier": "paid_cloud"},
            jwt_claims={"max_api_spend_usd": 1.0, "current_session_spend_usd": 0.1},
            estimated_cost_usd=0.01,
            project_budget_cap_usd=0.5,
            project_spend_usd=0.1,
        )
    )

    assert decision.allow is True
    assert decision.check_path == ("sheriff", "g3")


def test_pre_tool_adapter_fails_closed_on_policy_error():
    def broken_sheriff(_payload, _config):  # noqa: ANN001
        raise RuntimeError("policy store unavailable")

    adapter = HermesV011PreToolCallAdapter(sheriff_fn=broken_sheriff)

    decision = adapter.pre_tool_call(PreToolCallRequest(session_id=generate_uuid_v7(), skill_name="x", tool_name="y"))

    assert decision.allow is False
    assert decision.reason.startswith("adapter_error_fail_closed:RuntimeError")


def test_pre_tool_adapter_blocks_sheriff_verdicts():
    def blocking_sheriff(payload, _config):  # noqa: ANN001
        return ImmuneVerdict(
            verdict_id="v",
            check_type=CheckType.SHERIFF,
            tier=Tier.FAST_PATH,
            skill_name=payload.skill_name,
            session_id=payload.session_id,
            outcome=Outcome.BLOCK,
            block_reason=BlockReason.POLICY_VIOLATION,
            block_detail="blocked",
            alert_severity=AlertSeverity.IMMUNE_BLOCK_FAST,
        )

    adapter = HermesV011PreToolCallAdapter(sheriff_fn=blocking_sheriff)
    decision = adapter.pre_tool_call(
        PreToolCallRequest(session_id=generate_uuid_v7(), skill_name="x", tool_name="y")
    )

    assert decision.allow is False
    assert decision.reason == "sheriff_block:POLICY_VIOLATION"


def test_v012_pre_approval_request_fails_closed_for_g3_without_budget():
    adapter = HermesV011PreToolCallAdapter()

    decision = adapter.pre_approval_request(
        ApprovalRequest(
            session_id=generate_uuid_v7(),
            approval_type="g3_paid_spend",
            payload={"tool_name": "paid_model_call"},
            jwt_claims={"max_api_spend_usd": 0.0},
            estimated_cost_usd=0.02,
        )
    )

    assert decision.allow is False
    assert decision.reason == "g3_veto:no_session_budget"
    assert decision.check_path[:2] == ("pre_approval_request", "sheriff")


def test_v012_post_approval_response_blocks_unapproved_g3_dispatch():
    adapter = HermesV011PreToolCallAdapter()

    decision = adapter.post_approval_response(
        ApprovalResponse(
            session_id=generate_uuid_v7(),
            approval_type="g3_paid_spend",
            decision="DENIED",
        )
    )

    assert decision.allow is False
    assert decision.reason == "g3_veto:approval_not_granted"
