from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .records import (
    CapabilityGrant,
    ClaimRecord,
    Command,
    EvidenceBundle,
    ResearchRequest,
    SourceAcquisitionCheck,
    SourcePlan,
    SourceRecord,
    new_id,
)
from .store import KERNEL_POLICY_VERSION
from .store import KernelStore


@dataclass(frozen=True)
class LegacyResearchProjection:
    """Compatibility pointer from a kernel ResearchRequest to legacy research_tasks."""

    request_id: str
    task_id: str
    projection: str = "strategic_memory.research_tasks"


class KernelResearchEngine:
    """Authoritative v3.1 research request and evidence-bundle lane.

    Legacy research skills may still render, route, and display projections.
    The request and evidence authority starts here: command, event, derived
    kernel state, and replay.
    """

    def __init__(self, store: KernelStore) -> None:
        self.store = store

    def create_request(self, command: Command, request: ResearchRequest) -> str:
        return self.store.create_research_request(command, request)

    def start_collection(self, command: Command, request_id: str) -> str:
        return self.store.transition_research_request(command, request_id, "collecting")

    def create_source_plan(self, command: Command, plan: SourcePlan) -> str:
        return self.store.create_source_plan(command, plan)

    def issue_retrieval_grants(
        self,
        command_factory: Any,
        plan: SourcePlan,
        *,
        subject_id: str = "research_retrieval_broker",
        expires_at: str = "9999-12-31T23:59:59Z",
    ) -> list[str]:
        grant_ids: list[str] = []
        for idx, planned in enumerate(plan.planned_sources):
            if not _planned_source_requires_grant(planned):
                continue
            capability_type = _capability_for_access_method(planned.get("access_method", "public_web"))
            grant = CapabilityGrant(
                task_id=plan.request_id,
                subject_type="adapter",
                subject_id=subject_id,
                capability_type=capability_type,
                actions=["retrieve"],
                resource={
                    "source_ref": planned.get("url_or_ref") or planned.get("source_ref"),
                    "access_method": planned.get("access_method"),
                    "data_class": planned.get("data_class"),
                    "source_type": planned.get("source_type"),
                },
                scope={"request_id": plan.request_id, "source_plan_id": plan.source_plan_id, "planned_source_index": idx},
                conditions={
                    "metadata_only_when_raw_cache_disallowed": True,
                    "prompt_injection_scan_required": True,
                    "side_effects_require_separate_grant": True,
                },
                expires_at=expires_at,
                policy_version=KERNEL_POLICY_VERSION,
                max_uses=1,
            )
            command = command_factory(grant, idx) if callable(command_factory) else command_factory
            grant_ids.append(self.store.issue_capability_grant(command, grant))
        return grant_ids

    def record_source_acquisition_check(self, command: Command, check: SourceAcquisitionCheck) -> str:
        return self.store.record_source_acquisition_check(command, check)

    def start_synthesis(self, command: Command, request_id: str) -> str:
        return self.store.transition_research_request(command, request_id, "synthesizing")

    def require_review(self, command: Command, request_id: str) -> str:
        return self.store.transition_research_request(command, request_id, "review_needed")

    def fail_request(self, command: Command, request_id: str) -> str:
        return self.store.transition_research_request(command, request_id, "failed")

    def commit_evidence_bundle(self, command: Command, bundle: EvidenceBundle) -> str:
        return self.store.commit_evidence_bundle(command, bundle)

    def project_request_to_legacy_task(self, request_id: str, db_manager: Any) -> LegacyResearchProjection:
        """Create a non-authoritative research_domain task for existing UI/flow compatibility."""
        with self.store.connect() as conn:
            row = conn.execute(
                """
                SELECT profile, question, depth, max_cost_usd
                FROM research_requests
                WHERE request_id = ?
                """,
                (request_id,),
            ).fetchone()
        if row is None:
            raise ValueError("research request not found")

        from skills.research_domain.skill import ResearchDomainSkill

        research = ResearchDomainSkill(db_manager)
        task_id = research.create_task(
            row["question"],
            f"Kernel ResearchRequest {request_id} ({row['profile']}, {row['depth']}).",
            priority=_legacy_priority_for_depth(row["depth"]),
            domain=_legacy_domain_for_profile(row["profile"]),
            source="operator",
            tags=["kernel_research_request", request_id, row["profile"]],
            max_spend_usd=float(Decimal(row["max_cost_usd"])),
        )
        return LegacyResearchProjection(request_id=request_id, task_id=task_id)


def research_request_command(
    *,
    key: str,
    payload: dict[str, Any] | None = None,
    requester_id: str = "operator",
) -> Command:
    return Command(
        command_type="research.request",
        requested_by="operator",
        requester_id=requester_id,
        target_entity_type="research_request",
        idempotency_key=key,
        payload=payload or {"key": key},
    )


def evidence_bundle_command(
    *,
    request_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "operator",
) -> Command:
    return Command(
        command_type="research.evidence_bundle",
        requested_by="operator",
        requester_id=requester_id,
        target_entity_type="evidence_bundle",
        target_entity_id=request_id,
        idempotency_key=key or f"evidence-bundle:{request_id}:{new_id()}",
        payload=payload or {"request_id": request_id},
    )


def source_plan_command(
    *,
    request_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "operator",
) -> Command:
    return Command(
        command_type="research.source_plan",
        requested_by="operator",
        requester_id=requester_id,
        target_entity_type="source_plan",
        target_entity_id=request_id,
        idempotency_key=key or f"source-plan:{request_id}:{new_id()}",
        payload=payload or {"request_id": request_id},
    )


def source_acquisition_command(
    *,
    source_plan_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "operator",
) -> Command:
    return Command(
        command_type="research.source_acquisition_check",
        requested_by="operator",
        requester_id=requester_id,
        target_entity_type="source_plan",
        target_entity_id=source_plan_id,
        idempotency_key=key or f"source-acquisition:{source_plan_id}:{new_id()}",
        payload=payload or {"source_plan_id": source_plan_id},
    )


def retrieval_grant_command(
    *,
    grant_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "kernel",
) -> Command:
    return Command(
        command_type="research.retrieval_grant",
        requested_by="kernel",
        requester_id=requester_id,
        target_entity_type="capability",
        target_entity_id=grant_id,
        idempotency_key=key or f"research-retrieval-grant:{grant_id}",
        payload=payload or {"grant_id": grant_id},
    )


def _legacy_priority_for_depth(depth: str) -> str:
    return {"quick": "P2_NORMAL", "standard": "P1_HIGH", "deep": "P1_HIGH"}[depth]


def _legacy_domain_for_profile(profile: str) -> int:
    return {
        "commercial": 2,
        "project_support": 2,
        "ai_models": 5,
        "financial_markets": 4,
        "system_improvement": 1,
        "security": 1,
        "regulatory": 4,
        "general": 2,
    }[profile]


__all__ = [
    "ClaimRecord",
    "EvidenceBundle",
    "KernelResearchEngine",
    "LegacyResearchProjection",
    "ResearchRequest",
    "SourceAcquisitionCheck",
    "SourcePlan",
    "SourceRecord",
    "evidence_bundle_command",
    "research_request_command",
    "retrieval_grant_command",
    "source_acquisition_command",
    "source_plan_command",
]


def _planned_source_requires_grant(planned: dict[str, Any]) -> bool:
    access_method = planned.get("access_method", "public_web")
    data_class = planned.get("data_class", "public")
    return access_method in {"operator_provided", "paid_source", "local_file", "internal_record", "api"} or data_class in {
        "internal",
        "sensitive",
        "secret_ref",
        "regulated",
        "client_confidential",
    }


def _capability_for_access_method(access_method: str) -> str:
    if access_method in {"local_file", "internal_record", "operator_provided"}:
        return "file"
    return "network"
