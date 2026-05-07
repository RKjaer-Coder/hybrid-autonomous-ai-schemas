from __future__ import annotations

import re
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
    sha256_text,
)
from .store import KERNEL_POLICY_VERSION
from .store import KernelStore


@dataclass(frozen=True)
class LegacyResearchProjection:
    """Compatibility pointer from a kernel ResearchRequest to legacy research_tasks."""

    request_id: str
    task_id: str
    projection: str = "strategic_memory.research_tasks"


@dataclass(frozen=True)
class ProjectResearchInput:
    """Operator/project-pulled source text already acquired inside policy."""

    url_or_ref: str
    text: str
    source_type: str
    source_date: str
    access_method: str = "operator_provided"
    data_class: str = "internal"
    retrieved_at: str | None = None
    relevance: float = 0.75
    reliability: float = 0.75
    license_or_tos_notes: str | None = None
    artifact_ref: str | None = None


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

    def synthesize_project_commercial_evidence_bundle(
        self,
        command: Command,
        request_id: str,
        source_plan_id: str,
        research_inputs: list[ProjectResearchInput | dict[str, Any]],
        *,
        retention_policy: str = "retain-90d",
    ) -> EvidenceBundle:
        """Extract deterministic claims from already acquired project research inputs.

        This deliberately does not retrieve live sources. Callers pass project-
        pulled inputs that have already crossed source acquisition policy; this
        helper turns them into SourceRecord/ClaimRecord lineage and commits the
        resulting EvidenceBundle through the normal quality gate.
        """

        with self.store.connect() as conn:
            row = conn.execute(
                """
                SELECT profile, source_policy_json, evidence_requirements_json
                FROM research_requests
                WHERE request_id=?
                """,
                (request_id,),
            ).fetchone()
            plan = conn.execute(
                "SELECT request_id FROM source_plans WHERE source_plan_id=?",
                (source_plan_id,),
            ).fetchone()
        if row is None:
            raise ValueError("research request not found")
        if row["profile"] not in {"commercial", "project_support"}:
            raise ValueError("project commercial synthesis requires commercial or project_support profile")
        if plan is None or plan["request_id"] != request_id:
            raise ValueError("source plan does not belong to research request")

        inputs = [_normalize_project_research_input(item) for item in research_inputs]
        sources = [_source_from_project_input(item) for item in inputs]
        claims = _extract_claims_from_project_inputs(inputs, sources)
        unsupported_claims = _commercial_unsupported_claims(claims, sources)
        contradictions = _commercial_contradictions(claims)
        confidence = _bundle_confidence(claims, sources, unsupported_claims, contradictions)
        quality_gate_result = _requested_quality_gate_result(
            profile=row["profile"],
            evidence_requirements=row["evidence_requirements_json"],
            sources=sources,
            claims=claims,
            unsupported_claims=unsupported_claims,
        )
        bundle = EvidenceBundle(
            request_id=request_id,
            source_plan_id=source_plan_id,
            sources=sources,
            claims=claims,
            contradictions=contradictions,
            unsupported_claims=unsupported_claims,
            freshness_summary=_freshness_summary(sources),
            confidence=confidence,
            uncertainty=_commercial_uncertainty(unsupported_claims, contradictions),
            counter_thesis=_commercial_counter_thesis(unsupported_claims, contradictions),
            quality_gate_result=quality_gate_result,
            data_classes=_ordered_unique(source.data_class for source in sources) or ["internal"],
            retention_policy=retention_policy,
        )
        self.commit_evidence_bundle(command, bundle)
        return bundle

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
    "ProjectResearchInput",
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


def _normalize_project_research_input(item: ProjectResearchInput | dict[str, Any]) -> ProjectResearchInput:
    if isinstance(item, ProjectResearchInput):
        return item
    return ProjectResearchInput(
        url_or_ref=str(item["url_or_ref"]),
        text=str(item["text"]),
        source_type=str(item.get("source_type", "internal_record")),
        source_date=str(item.get("source_date", "unknown")),
        access_method=str(item.get("access_method", "operator_provided")),
        data_class=str(item.get("data_class", "internal")),
        retrieved_at=item.get("retrieved_at"),
        relevance=float(item.get("relevance", 0.75)),
        reliability=float(item.get("reliability", 0.75)),
        license_or_tos_notes=item.get("license_or_tos_notes"),
        artifact_ref=item.get("artifact_ref"),
    )


def _source_from_project_input(item: ProjectResearchInput) -> SourceRecord:
    return SourceRecord(
        url_or_ref=item.url_or_ref,
        source_type=item.source_type,  # type: ignore[arg-type]
        retrieved_at=item.retrieved_at or "1970-01-01T00:00:00Z",
        source_date=item.source_date,
        relevance=_clamp(item.relevance),
        reliability=_clamp(item.reliability),
        content_hash=sha256_text(item.text),
        access_method=item.access_method,  # type: ignore[arg-type]
        data_class=item.data_class,  # type: ignore[arg-type]
        license_or_tos_notes=item.license_or_tos_notes,
        artifact_ref=item.artifact_ref,
    )


def _extract_claims_from_project_inputs(
    inputs: list[ProjectResearchInput],
    sources: list[SourceRecord],
) -> list[ClaimRecord]:
    claims: list[ClaimRecord] = []
    seen: set[tuple[str, str]] = set()
    for item, source in zip(inputs, sources):
        for sentence in _claim_sentences(item.text):
            key = (source.source_id, sentence.lower())
            if key in seen:
                continue
            seen.add(key)
            claims.append(
                ClaimRecord(
                    text=sentence,
                    claim_type=_claim_type(sentence),
                    source_ids=[source.source_id],
                    confidence=round(_clamp((source.relevance + source.reliability) / 2), 2),
                    freshness=_claim_freshness(source.source_date),
                    importance=_claim_importance(sentence),
                )
            )
    return claims


def _claim_sentences(text: str) -> list[str]:
    cleaned = re.sub(r"\s+", " ", text.replace("\r", "\n")).strip()
    candidates = re.split(r"(?<=[.!?])\s+|(?:^|\n)\s*[-*]\s+", cleaned)
    sentences = [candidate.strip(" -\t") for candidate in candidates if candidate.strip(" -\t")]
    return [sentence[:500] for sentence in sentences]


def _claim_type(text: str) -> str:
    lower = text.lower()
    if any(term in lower for term in ("recommend", "should ", "next step", "validation plan", "kill criteria")):
        return "recommendation"
    if any(term in lower for term in ("forecast", "expected to", "likely to", "will ")):
        return "forecast"
    if any(term in lower for term in ("estimate", "estimated", "pricing", "price", "usd", "$", "%", "hours")):
        return "estimate"
    if any(term in lower for term in ("suggests", "indicates", "plausible", "appears", "risk", "complexity")):
        return "interpretation"
    return "fact"


def _claim_importance(text: str) -> str:
    lower = text.lower()
    if any(term in lower for term in ("legal", "regulated", "public claim", "customer commitment")):
        return "critical"
    if any(
        term in lower
        for term in (
            "buyer",
            "customer",
            "willingness-to-pay",
            "willingness to pay",
            "pricing",
            "operator load",
            "validation",
            "competition",
            "distribution",
            "build complexity",
            "kill criteria",
        )
    ):
        return "high"
    return "medium"


def _claim_freshness(source_date: str) -> str:
    if not source_date or source_date == "unknown":
        return "unknown"
    if source_date[:4].isdigit() and int(source_date[:4]) < 2025:
        return "aging"
    return "current"


def _commercial_unsupported_claims(claims: list[ClaimRecord], sources: list[SourceRecord]) -> list[str]:
    text = "\n".join(claim.text.lower() for claim in claims)
    unsupported: list[str] = []
    if not claims:
        unsupported.append("No extractable commercial claims were present in the project-pulled inputs.")
    if not any(term in text for term in ("buyer", "customer", "client", "problem")):
        unsupported.append("Customer/problem evidence is missing.")
    if not any(term in text for term in ("willingness-to-pay", "willingness to pay", "pricing", "price", "transaction")):
        unsupported.append("Willingness-to-pay or pricing evidence is missing.")
    if not any(term in text for term in ("operator load", "operator-load", "operator_load", "hours")):
        unsupported.append("Operator-load estimate is missing.")
    if not any(term in text for term in ("validation", "experiment", "pilot")):
        unsupported.append("Validation experiment is missing.")
    if not sources:
        unsupported.append("No source lineage is available.")
    return unsupported


def _commercial_contradictions(claims: list[ClaimRecord]) -> list[dict[str, Any]]:
    text_by_claim = [(claim.claim_id, claim.text.lower()) for claim in claims]
    contradictions: list[dict[str, Any]] = []
    for positive, negative, label in (
        ("strong demand", "weak demand", "demand_strength"),
        ("low operator load", "high operator load", "operator_load"),
        ("low complexity", "high complexity", "build_complexity"),
    ):
        positive_ids = [claim_id for claim_id, text in text_by_claim if positive in text]
        negative_ids = [claim_id for claim_id, text in text_by_claim if negative in text]
        if positive_ids and negative_ids:
            contradictions.append({"topic": label, "claim_ids": positive_ids + negative_ids})
    return contradictions


def _bundle_confidence(
    claims: list[ClaimRecord],
    sources: list[SourceRecord],
    unsupported_claims: list[str],
    contradictions: list[dict[str, Any]],
) -> float:
    if not sources or not claims:
        return 0.0
    source_score = sum((source.relevance + source.reliability) / 2 for source in sources) / len(sources)
    claim_score = sum(claim.confidence for claim in claims) / len(claims)
    penalty = min(0.35, 0.06 * len(unsupported_claims) + 0.08 * len(contradictions))
    return round(_clamp(((source_score + claim_score) / 2) - penalty), 2)


def _requested_quality_gate_result(
    *,
    profile: str,
    evidence_requirements: str,
    sources: list[SourceRecord],
    claims: list[ClaimRecord],
    unsupported_claims: list[str],
) -> str:
    import json

    requirements = json.loads(evidence_requirements)
    minimum_sources = int(requirements.get("minimum_sources", 1))
    claim_text = "\n".join(claim.text.lower() for claim in claims)
    source_types = {source.source_type for source in sources}
    if len(sources) < minimum_sources or not claims:
        return "fail"
    if profile == "commercial" and any(
        term in claim_text for term in ("willingness-to-pay", "willingness to pay", "pricing", "buyer", "transaction", "market")
    ) and not source_types & {"official", "primary_data", "market_data", "internal_record"}:
        return "fail"
    return "degraded" if unsupported_claims else "pass"


def _freshness_summary(sources: list[SourceRecord]) -> str:
    if not sources:
        return "No source freshness available."
    unknown = sum(1 for source in sources if source.source_date == "unknown")
    if unknown:
        return f"{len(sources) - unknown} of {len(sources)} sources have dated evidence; {unknown} are unknown."
    return f"{len(sources)} project-pulled sources include source dates."


def _commercial_uncertainty(unsupported_claims: list[str], contradictions: list[dict[str, Any]]) -> str:
    if contradictions:
        return "Commercial evidence contains contradictions that require operator review before validation budget."
    if unsupported_claims:
        return "Commercial evidence is incomplete: " + " ".join(unsupported_claims[:2])
    return "Commercial evidence is bounded to the supplied project-pulled inputs; live retrieval was not performed."


def _commercial_counter_thesis(unsupported_claims: list[str], contradictions: list[dict[str, Any]]) -> str | None:
    if contradictions:
        return "The project may not merit validation until contradictory evidence is resolved."
    if unsupported_claims:
        return "The opportunity may be an unsupported internal hypothesis rather than a validated commercial pull."
    return "Demand may still be narrow or non-repeatable despite the supplied commercial evidence."


def _ordered_unique(values: Any) -> list[Any]:
    ordered: list[Any] = []
    for value in values:
        if value not in ordered:
            ordered.append(value)
    return ordered


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))
