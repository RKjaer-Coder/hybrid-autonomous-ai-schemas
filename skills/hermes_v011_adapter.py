from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable

from immune.config import load_config
from immune.sheriff import sheriff_check
from immune.types import ImmuneConfig, Outcome, SheriffPayload


@dataclass(frozen=True)
class PreToolCallRequest:
    session_id: str
    skill_name: str
    tool_name: str
    arguments: dict[str, Any] = field(default_factory=dict)
    raw_prompt: str | None = None
    source_trust_tier: int = 4
    jwt_claims: dict[str, Any] = field(default_factory=dict)
    estimated_cost_usd: float = 0.0
    project_budget_cap_usd: float | None = None
    project_spend_usd: float = 0.0


@dataclass(frozen=True)
class PreToolCallDecision:
    allow: bool
    reason: str
    check_path: tuple[str, ...]


@dataclass(frozen=True)
class ApprovalRequest:
    session_id: str
    approval_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    jwt_claims: dict[str, Any] = field(default_factory=dict)
    estimated_cost_usd: float = 0.0
    project_budget_cap_usd: float | None = None
    project_spend_usd: float = 0.0


@dataclass(frozen=True)
class ApprovalResponse:
    session_id: str
    approval_type: str
    decision: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ApprovalDecision:
    allow: bool
    reason: str
    check_path: tuple[str, ...]


class HermesV012ApprovalHookAdapter:
    """Blocking Hermes v0.12 approval-hook adapter for repo policy checks."""

    def __init__(
        self,
        *,
        config: ImmuneConfig | None = None,
        sheriff_fn: Callable[[SheriffPayload, ImmuneConfig], Any] = sheriff_check,
        timeout_ms: float = 200.0,
    ) -> None:
        self._config = config or load_config()
        self._sheriff_fn = sheriff_fn
        self._timeout_ms = timeout_ms

    def pre_tool_call(self, request: PreToolCallRequest) -> PreToolCallDecision:
        start = time.monotonic_ns()
        try:
            verdict = self._sheriff_fn(
                SheriffPayload(
                    session_id=request.session_id,
                    skill_name=request.skill_name,
                    tool_name=request.tool_name,
                    arguments=request.arguments,
                    raw_prompt=request.raw_prompt,
                    source_trust_tier=request.source_trust_tier,
                    jwt_claims=request.jwt_claims,
                ),
                self._config,
            )
            if getattr(verdict, "outcome", None) == Outcome.BLOCK:
                return PreToolCallDecision(
                    allow=False,
                    reason=f"sheriff_block:{verdict.block_reason.value if verdict.block_reason else 'unknown'}",
                    check_path=("sheriff",),
                )

            g3_decision = self._g3_decision(request)
            if g3_decision is not None:
                return g3_decision

            elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
            if elapsed_ms > self._timeout_ms:
                return PreToolCallDecision(False, "adapter_timeout_fail_closed", ("sheriff", "g3", "timeout"))
            return PreToolCallDecision(True, "allowed", ("sheriff", "g3"))
        except Exception as exc:  # noqa: BLE001
            return PreToolCallDecision(False, f"adapter_error_fail_closed:{type(exc).__name__}", ("fail_closed",))

    def pre_approval_request(self, request: ApprovalRequest) -> ApprovalDecision:
        try:
            if request.approval_type == "g3_paid_spend":
                decision = self._g3_decision(
                    PreToolCallRequest(
                        session_id=request.session_id,
                        skill_name="financial_router",
                        tool_name=str(request.payload.get("tool_name") or "paid_model_call"),
                        arguments={
                            **request.payload,
                            "billing_tier": request.payload.get("billing_tier") or "paid_cloud",
                        },
                        jwt_claims=request.jwt_claims,
                        estimated_cost_usd=request.estimated_cost_usd,
                        project_budget_cap_usd=request.project_budget_cap_usd,
                        project_spend_usd=request.project_spend_usd,
                    )
                )
                if decision is not None:
                    return ApprovalDecision(False, decision.reason, ("pre_approval_request", *decision.check_path))
            return ApprovalDecision(True, "approval_request_allowed", ("pre_approval_request",))
        except Exception as exc:  # noqa: BLE001
            return ApprovalDecision(False, f"adapter_error_fail_closed:{type(exc).__name__}", ("fail_closed",))

    def post_approval_response(self, response: ApprovalResponse) -> ApprovalDecision:
        try:
            if response.approval_type == "g3_paid_spend" and response.decision != "APPROVED":
                return ApprovalDecision(False, "g3_veto:approval_not_granted", ("post_approval_response", "g3"))
            return ApprovalDecision(True, "approval_response_allowed", ("post_approval_response",))
        except Exception as exc:  # noqa: BLE001
            return ApprovalDecision(False, f"adapter_error_fail_closed:{type(exc).__name__}", ("fail_closed",))

    @staticmethod
    def _g3_decision(request: PreToolCallRequest) -> PreToolCallDecision | None:
        billing_tier = str(request.arguments.get("billing_tier") or request.arguments.get("route_selected") or "")
        paid_requested = (
            request.estimated_cost_usd > 0
            or billing_tier == "paid_cloud"
            or bool(request.arguments.get("metered_api_call"))
        )
        if not paid_requested:
            return None

        max_api_spend = float(request.jwt_claims.get("max_api_spend_usd", 0.0) or 0.0)
        current_session_spend = float(request.jwt_claims.get("current_session_spend_usd", 0.0) or 0.0)
        if max_api_spend <= 0:
            return PreToolCallDecision(False, "g3_veto:no_session_budget", ("sheriff", "g3"))
        if current_session_spend + request.estimated_cost_usd > max_api_spend:
            return PreToolCallDecision(False, "g3_veto:session_cap_exceeded", ("sheriff", "g3"))
        if request.project_budget_cap_usd is None:
            return PreToolCallDecision(False, "g3_veto:no_project_budget", ("sheriff", "g3"))
        if request.project_spend_usd + request.estimated_cost_usd > request.project_budget_cap_usd:
            return PreToolCallDecision(False, "g3_veto:project_cap_exceeded", ("sheriff", "g3"))
        return None


HermesV011PreToolCallAdapter = HermesV012ApprovalHookAdapter
