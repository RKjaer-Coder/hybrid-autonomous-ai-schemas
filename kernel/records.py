from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Literal

JsonObject = dict[str, Any]

ActorType = Literal["kernel", "operator", "agent", "tool", "model", "scheduler"]
Authority = Literal["rule", "single_agent", "council", "operator_gate"]
DataClass = Literal["public", "internal", "sensitive", "secret_ref", "regulated", "client_confidential"]
DecisionType = Literal[
    "project_approval",
    "kill",
    "pivot",
    "spend",
    "architecture",
    "model_promotion",
    "security",
    "commercial_strategy",
    "system_improvement",
    "model_demotion",
    "routing",
    "other",
]
DecisionStakes = Literal["low", "medium", "high", "critical"]
DecisionRequester = Literal["operator", "kernel", "project", "research", "model_intelligence", "scheduler"]
DecisionStatus = Literal["proposed", "deliberating", "decided", "gated", "expired", "cancelled"]
ResearchProfile = Literal[
    "commercial",
    "ai_models",
    "financial_markets",
    "system_improvement",
    "security",
    "regulatory",
    "project_support",
    "general",
]
ResearchDepth = Literal["quick", "standard", "deep"]
ResearchStatus = Literal["queued", "collecting", "synthesizing", "review_needed", "completed", "failed"]
ResearchAutonomyClass = Literal["A2", "A3", "A4"]
SourceType = Literal[
    "official",
    "primary_data",
    "reputable_media",
    "community",
    "model_card",
    "paper",
    "market_data",
    "internal_record",
    "other",
]
SourceAccessMethod = Literal["public_web", "operator_provided", "paid_source", "local_file", "internal_record", "api"]
ClaimType = Literal["fact", "estimate", "forecast", "interpretation", "recommendation"]
ClaimFreshness = Literal["current", "aging", "stale", "unknown"]
ClaimImportance = Literal["low", "medium", "high", "critical"]
QualityGateResult = Literal["pass", "fail", "degraded"]
SourcePlanStatus = Literal["planned", "collecting", "completed", "blocked"]
AcquisitionBoundaryResult = Literal["allowed", "blocked", "requires_grant"]
CommercialDecisionRecommendation = Literal["pursue", "pause", "reject", "insufficient_evidence"]
CommercialRevenueMechanism = Literal[
    "client_work",
    "software",
    "service",
    "data_product",
    "ip_asset",
    "marketplace",
    "other",
]
ModelProvider = Literal[
    "local",
    "lm_studio",
    "ollama",
    "mlx",
    "openrouter",
    "nous",
    "openai",
    "anthropic",
    "google",
    "xai",
    "other",
]
ModelAccessMode = Literal["local", "free_api", "subscription_tool", "paid_api", "operator_prompted"]
CommercialUseStatus = Literal["allowed", "restricted", "prohibited", "unknown"]
HardwareFit = Literal["excellent", "good", "marginal", "not_local"]
DataResidency = Literal["local_only", "provider_retained", "provider_no_train", "unknown"]
ModelPromotionState = Literal["discovered", "queued_for_eval", "shadow", "promoted", "demoted", "rejected", "retired"]
ModelTaskClass = Literal[
    "quick_research_summarization",
    "source_claim_extraction",
    "coding_small_patch",
]
EvalSplit = Literal["development", "regression", "known_bad", "frozen_holdout"]
EvalSetStatus = Literal["draft", "active", "retired"]
HoldoutAccess = Literal["sealed", "operator_only", "scoring_service"]
HoldoutUseVerdict = Literal["allowed", "blocked"]
RouteDecisionVerdict = Literal["local", "shadow", "fallback", "frontier", "operator_prompted", "blocked"]
EvalRunVerdict = Literal["supports_decision", "shadow", "reject", "needs_more_data"]
ModelRoutingRole = Literal[
    "primary_local",
    "research_local",
    "coding_local",
    "validation_local",
    "embeddings_local",
    "frontier_escalation",
    "cheap_cloud",
]
PromotionRecommendation = Literal["promote", "keep_shadow", "reject", "needs_more_data"]
DemotionReason = Literal[
    "quality_regression",
    "latency_regression",
    "license_tos_regression",
    "drift_regression",
    "replacement_regression",
]
RoutingStateStatus = Literal["active", "demoted", "blocked"]


def new_id() -> str:
    generator = getattr(uuid, "uuid7", None)
    return str(generator() if callable(generator) else uuid.uuid4())


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def payload_hash(payload: JsonObject) -> str:
    return sha256_text(canonical_json(payload))


@dataclass(frozen=True)
class Command:
    command_type: str
    requested_by: Literal["operator", "kernel", "scheduler", "agent", "tool", "model"]
    requester_id: str
    target_entity_type: str
    idempotency_key: str
    payload: JsonObject = field(default_factory=dict)
    command_id: str = field(default_factory=new_id)
    target_entity_id: str | None = None
    requested_authority: Authority | None = None
    submitted_at: str = field(default_factory=now_iso)

    @property
    def payload_hash(self) -> str:
        return payload_hash(self.payload)


@dataclass(frozen=True)
class Event:
    event_type: str
    entity_type: str
    entity_id: str
    transaction_id: str
    actor_type: ActorType
    actor_id: str
    policy_version: str
    data_class: DataClass
    payload: JsonObject
    command_id: str | None = None
    correlation_id: str | None = None
    causation_event_id: str | None = None
    event_schema_version: int = 1
    event_id: str = field(default_factory=new_id)
    timestamp: str = field(default_factory=now_iso)

    @property
    def payload_hash(self) -> str:
        return payload_hash(self.payload)


@dataclass(frozen=True)
class CapabilityGrant:
    task_id: str
    subject_type: Literal["agent", "tool", "model", "adapter"]
    subject_id: str
    capability_type: Literal["model", "tool", "file", "network", "spend", "memory_write", "side_effect"]
    actions: list[str]
    resource: JsonObject
    scope: JsonObject
    conditions: JsonObject
    expires_at: str
    policy_version: str
    grant_id: str = field(default_factory=new_id)
    issued_at: str = field(default_factory=now_iso)
    max_uses: int | None = None
    used_count: int = 0
    issuer: Literal["kernel"] = "kernel"
    revalidate_on_use: bool = True
    status: Literal["active", "exhausted", "revoked", "expired"] = "active"


@dataclass(frozen=True)
class Budget:
    owner_type: Literal["project", "research_profile", "system_maintenance"]
    owner_id: str
    approved_by: Literal["operator"]
    cap_usd: Decimal
    expires_at: str
    budget_id: str = field(default_factory=new_id)
    spent_usd: Decimal = Decimal("0")
    reserved_usd: Decimal = Decimal("0")
    status: Literal["active", "exhausted", "expired", "revoked"] = "active"


@dataclass(frozen=True)
class ArtifactRef:
    artifact_uri: str
    data_class: DataClass
    content_hash: str
    retention_policy: str
    deletion_policy: str
    encryption_status: Literal["unencrypted", "encrypted", "quarantined", "deleted"]
    source_notes: str | None = None
    artifact_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ResearchRequest:
    profile: ResearchProfile
    question: str
    freshness_horizon: str
    depth: ResearchDepth
    source_policy: JsonObject
    evidence_requirements: JsonObject
    max_cost_usd: Decimal
    autonomy_class: ResearchAutonomyClass
    request_id: str = field(default_factory=new_id)
    decision_target: str | None = None
    max_latency: str | None = None
    status: ResearchStatus = "queued"
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class Decision:
    decision_type: DecisionType
    question: str
    options: list[JsonObject]
    stakes: DecisionStakes
    evidence_bundle_ids: list[str]
    requested_by: DecisionRequester
    required_authority: Authority
    authority_policy_version: str
    status: DecisionStatus
    decision_id: str = field(default_factory=new_id)
    evidence_refs: list[str] = field(default_factory=list)
    deadline: str | None = None
    recommendation: str | None = None
    verdict: str | None = None
    confidence: float | None = None
    decisive_factors: list[str] = field(default_factory=list)
    decisive_uncertainty: str | None = None
    risk_flags: list[str] = field(default_factory=list)
    default_on_timeout: str | None = None
    gate_packet: JsonObject | None = None
    created_at: str = field(default_factory=now_iso)
    decided_at: str | None = None


@dataclass(frozen=True)
class SourceRecord:
    url_or_ref: str
    source_type: SourceType
    retrieved_at: str
    source_date: str
    relevance: float
    reliability: float
    content_hash: str
    access_method: SourceAccessMethod
    data_class: DataClass
    source_id: str = field(default_factory=new_id)
    license_or_tos_notes: str | None = None
    artifact_ref: str | None = None


@dataclass(frozen=True)
class SourcePlan:
    request_id: str
    profile: ResearchProfile
    depth: ResearchDepth
    planned_sources: list[JsonObject]
    retrieval_strategy: str
    created_by: Literal["kernel", "operator", "agent", "scheduler"]
    source_plan_id: str = field(default_factory=new_id)
    status: SourcePlanStatus = "planned"
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class SourceAcquisitionCheck:
    request_id: str
    source_plan_id: str
    source_ref: str
    access_method: SourceAccessMethod
    data_class: DataClass
    source_type: SourceType
    result: AcquisitionBoundaryResult
    reason: str
    grant_id: str | None = None
    check_id: str = field(default_factory=new_id)
    checked_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ClaimRecord:
    text: str
    claim_type: ClaimType
    source_ids: list[str]
    confidence: float
    freshness: ClaimFreshness
    importance: ClaimImportance
    claim_id: str = field(default_factory=new_id)


@dataclass(frozen=True)
class EvidenceBundle:
    request_id: str
    source_plan_id: str
    sources: list[SourceRecord]
    claims: list[ClaimRecord]
    contradictions: list[JsonObject]
    unsupported_claims: list[str]
    freshness_summary: str
    confidence: float
    uncertainty: str
    counter_thesis: str | None
    quality_gate_result: QualityGateResult
    data_classes: list[DataClass]
    retention_policy: str
    bundle_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class OpportunityProjectDecisionPacket:
    request_id: str
    evidence_bundle_id: str
    decision_id: str
    decision_target: str
    question: str
    recommendation: CommercialDecisionRecommendation
    required_authority: Authority
    opportunity: JsonObject
    project: JsonObject
    gate_packet: JsonObject
    evidence_used: list[str]
    risk_flags: list[str]
    default_on_timeout: str
    status: Literal["proposed", "gated", "decided", "cancelled"] = "proposed"
    packet_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ModelTaskClassRecord:
    task_class: ModelTaskClass
    description: str
    quality_threshold: float
    reliability_threshold: float
    latency_p95_ms: int
    local_offload_target: float
    allowed_data_classes: list[DataClass]
    promotion_authority: Authority
    expansion_allowed: bool = False
    task_class_id: str = field(default_factory=new_id)
    status: Literal["seed", "retired"] = "seed"
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ModelCandidate:
    model_id: str
    provider: ModelProvider
    access_mode: ModelAccessMode
    source_ref: str
    artifact_hash: str | None
    license: str
    commercial_use: CommercialUseStatus
    terms_verified_at: str | None
    context_window: int | None
    modalities: list[str]
    hardware_fit: HardwareFit
    sandbox_profile: str | None
    data_residency: DataResidency
    cost_profile: JsonObject
    latency_profile: JsonObject
    routing_metadata: JsonObject
    promotion_state: ModelPromotionState = "discovered"
    candidate_id: str = field(default_factory=new_id)
    last_verified_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class LocalOffloadEvalSet:
    task_class: ModelTaskClass
    dataset_version: str
    artifact_ref: str
    split_counts: dict[EvalSplit, int]
    data_classes: list[DataClass]
    retention_policy: str
    scorer_profile: JsonObject
    holdout_policy_id: str
    eval_set_id: str = field(default_factory=new_id)
    status: EvalSetStatus = "active"
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class HoldoutPolicy:
    task_class: ModelTaskClass
    dataset_version: str
    access: HoldoutAccess
    min_sample_count: int
    contamination_controls: list[str]
    scorer_separation: str
    promotion_requires_decision: bool
    policy_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class HoldoutUseRecord:
    policy_id: str
    eval_set_id: str
    task_class: ModelTaskClass
    dataset_version: str
    requester_id: str
    requester_change_ref: str | None
    purpose: Literal["development", "regression", "promotion_gate", "audit"]
    verdict: HoldoutUseVerdict
    reason: str
    decision_id: str | None = None
    holdout_use_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ModelEvalRun:
    model_id: str
    task_class: ModelTaskClass
    dataset_version: str
    eval_set_id: str
    route_version: str
    route_metadata: JsonObject
    sample_count: int
    quality_score: float
    reliability_score: float
    latency_p50_ms: int
    latency_p95_ms: int
    cost_per_1k_tasks: Decimal
    aggregate_scores: JsonObject
    failure_categories: list[str]
    failure_modes: list[str]
    confidence: JsonObject
    frozen_holdout_result: JsonObject
    verdict: EvalRunVerdict
    baseline_model_id: str | None = None
    scorer_id: str = "kernel-model-intelligence-scorer"
    decision_id: str | None = None
    authority_effect: Literal["evidence_only"] = "evidence_only"
    eval_run_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ModelRouteDecision:
    task_id: str
    task_class: ModelTaskClass
    data_class: DataClass
    risk_level: Literal["low", "medium", "high", "critical"]
    selected_route: RouteDecisionVerdict
    selected_model_id: str | None
    candidate_model_id: str | None
    eval_set_id: str | None
    reasons: list[str]
    required_authority: Authority
    decision_id: str | None
    local_offload_estimate: JsonObject
    frontier_fallback: JsonObject
    route_decision_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ModelPromotionDecisionPacket:
    model_id: str
    task_class: ModelTaskClass
    proposed_routing_role: ModelRoutingRole
    recommendation: PromotionRecommendation
    required_authority: Authority
    decision_id: str
    eval_run_ids: list[str]
    holdout_use_ids: list[str]
    evidence_refs: list[str]
    frozen_holdout_confidence: float
    confidence_threshold: float
    gate_packet: JsonObject
    risk_flags: list[str]
    default_on_timeout: str
    status: Literal["proposed", "gated", "decided", "cancelled"] = "proposed"
    packet_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ModelDemotionRecord:
    model_id: str
    task_class: ModelTaskClass
    routing_roles: list[ModelRoutingRole]
    reasons: list[DemotionReason]
    required_authority: Authority
    evidence_refs: list[str]
    eval_run_ids: list[str]
    route_decision_ids: list[str]
    metrics: JsonObject
    routing_state_update: JsonObject
    audit_notes: str
    decision_id: str | None = None
    authority_effect: Literal["immediate_routing_update"] = "immediate_routing_update"
    demotion_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class SideEffectIntent:
    task_id: str
    side_effect_type: Literal[
        "message",
        "publish",
        "deploy",
        "purchase",
        "provider_call",
        "account_change",
        "financial",
        "legal",
        "other",
    ]
    target: JsonObject
    payload_hash: str
    required_authority: Authority
    grant_id: str
    timeout_policy: Literal["deny", "pause", "compensate", "ask_operator"]
    intent_id: str = field(default_factory=new_id)
    status: Literal["prepared", "executed", "failed", "cancelled", "compensation_needed"] = "prepared"


@dataclass(frozen=True)
class SideEffectReceipt:
    intent_id: str
    receipt_type: Literal["success", "failure", "timeout", "cancellation", "compensation_needed"]
    receipt_hash: str
    details: JsonObject
    receipt_id: str = field(default_factory=new_id)
    recorded_at: str = field(default_factory=now_iso)
