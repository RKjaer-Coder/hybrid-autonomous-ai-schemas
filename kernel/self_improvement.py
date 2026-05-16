from __future__ import annotations

from .records import (
    Command,
    Decision,
    SelfImprovementEvalRecord,
    SelfImprovementPromotionPacket,
    SelfImprovementProposal,
    SelfImprovementRollbackRecord,
)
from .store import KERNEL_POLICY_VERSION, KernelStore


def self_improvement_command(
    command_type: str,
    key: str,
    *,
    requested_by: str = "operator",
    requester_id: str = "operator",
    requested_authority: str = "operator_gate",
    payload: dict | None = None,
) -> Command:
    return Command(
        command_type=command_type,
        requested_by=requested_by,  # type: ignore[arg-type]
        requester_id=requester_id,
        target_entity_type="self_improvement",
        requested_authority=requested_authority,  # type: ignore[arg-type]
        idempotency_key=key,
        payload=payload or {"key": key},
    )


class KernelSelfImprovement:
    """Governed proposal/eval/promotion lane for pre-Hermes self-improvement."""

    def __init__(self, store: KernelStore) -> None:
        self.store = store

    def record_proposal(self, command: Command, proposal: SelfImprovementProposal) -> str:
        return self.store.record_self_improvement_proposal(command, proposal)

    def record_eval(self, command: Command, record: SelfImprovementEvalRecord) -> str:
        return self.store.record_self_improvement_eval(command, record)

    def create_promotion_packet(self, command: Command, packet: SelfImprovementPromotionPacket) -> str:
        return self.store.create_self_improvement_promotion_packet(command, packet)

    def record_rollback(self, command: Command, record: SelfImprovementRollbackRecord) -> str:
        return self.store.record_self_improvement_rollback(command, record)

    def compare_replay_to_projection(self, command: Command, scope: str = "self_improvement"):
        return self.store.compare_self_improvement_replay_to_projection(command, scope)

    def run_evidence_pipeline(
        self,
        command: Command,
        *,
        signals: list[dict],
        as_of: str,
        scope: str = "pre_hermes_self_improvement",
        run_id: str | None = None,
    ):
        return self.store.run_self_improvement_evidence_pipeline(
            command,
            signals=signals,
            as_of=as_of,
            scope=scope,
            run_id=run_id,
        )

    def promotion_decision(
        self,
        *,
        proposal: SelfImprovementProposal,
        question: str,
        recommendation: str = "approve",
        confidence: float | None = None,
        evidence_refs: list[str] | None = None,
        risk_flags: list[str] | None = None,
        status: str = "proposed",
        default_on_timeout: str = "keep_current_behavior",
    ) -> Decision:
        return Decision(
            decision_type="system_improvement",
            question=question,
            options=[
                {"option_id": "approve", "label": f"Approve change for {proposal.target_id}"},
                {"option_id": "reject", "label": "Reject change"},
                {"option_id": "needs_more_data", "label": "Require more evaluation"},
                {"option_id": "rollback", "label": "Rollback if already promoted"},
            ],
            stakes="high" if proposal.target_type in {"workflow", "policy", "model"} else "medium",
            evidence_bundle_ids=[],
            evidence_refs=evidence_refs or proposal.problem_evidence,
            requested_by="kernel",
            required_authority=proposal.authority_required,
            authority_policy_version=KERNEL_POLICY_VERSION,
            status=status,  # type: ignore[arg-type]
            recommendation=recommendation,
            confidence=confidence,
            decisive_factors=[
                f"proposal_id={proposal.proposal_id}",
                f"target_type={proposal.target_type}",
                f"target_id={proposal.target_id}",
            ],
            risk_flags=risk_flags or [],
            default_on_timeout=default_on_timeout,
            gate_packet={
                "decision_type": "system_improvement",
                "proposal_id": proposal.proposal_id,
                "authority_route": proposal.authority_required,
                "default_on_timeout": default_on_timeout,
            },
        )
