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
RiskLevel = Literal["low", "medium", "high", "critical"]
AutonomyClass = Literal["A0", "A1", "A2", "A3", "A4", "A5"]
DecisionType = Literal[
    "project_approval",
    "project_close",
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
ProjectStatus = Literal["proposed", "active", "paused", "blocked", "kill_recommended", "complete", "killed"]
ProjectOperatorRole = Literal["sales", "reviewer", "client_owner", "none", "mixed"]
ProjectCommitmentPolicy = Literal["operator_only", "preapproved_templates", "project_grants"]
ProjectTaskStatus = Literal["queued", "running", "blocked", "completed", "failed", "cancelled"]
ProjectTaskType = Literal["validate", "build", "ship", "operate", "feedback"]
ProjectTaskAssignmentStatus = Literal["assigned", "accepted", "rejected", "revoked"]
ProjectTaskWorkerType = Literal["agent", "operator", "tool", "model", "scheduler"]
RecoveryPolicy = Literal[
    "retry_same",
    "retry_with_smaller_context",
    "reroute_model",
    "reroute_tool",
    "degrade_output",
    "ask_operator",
    "fail_closed",
]
ProjectOutcomeType = Literal[
    "validation",
    "build_artifact",
    "shipped_artifact",
    "feedback",
    "project_close",
    "operate_followup",
]
ProjectOutcomeStatus = Literal["recorded", "accepted", "needs_followup"]
ProjectArtifactKind = Literal["validation_artifact", "build_artifact", "shipped_artifact"]
ProjectArtifactStatus = Literal["recorded", "accepted", "quarantined"]
ProjectFeedbackSourceType = Literal["operator", "customer", "platform", "internal_signal"]
ProjectFeedbackSentiment = Literal["positive", "neutral", "negative", "mixed", "unknown"]
ProjectFeedbackStatus = Literal["recorded", "accepted", "needs_followup"]
ProjectCustomerCommitmentReceiptType = Literal[
    "customer_response",
    "delivery_failure",
    "timeout",
    "compensation_needed",
]
ProjectCustomerCommitmentReceiptStatus = Literal["recorded", "accepted", "needs_followup"]
ProjectRevenueSource = Literal["operator_reported", "invoice", "stripe", "app_store", "marketplace", "platform", "other"]
ProjectRevenueStatus = Literal["recorded", "reconciled", "needs_reconciliation"]
ProjectOperatorLoadType = Literal[
    "gate_review",
    "client_sales",
    "build_review",
    "maintenance",
    "reconciliation",
    "other",
]
ProjectPhaseRollupStatus = Literal["not_started", "active", "blocked", "complete", "failed", "at_risk"]
ProjectCloseRecommendation = Literal["continue", "complete", "kill", "pause"]
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
CommercialDeliberationAuthority = Literal["single_agent", "council"]
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
class CommercialDecisionRecommendationRecord:
    packet_id: str
    decision_id: str
    request_id: str
    evidence_bundle_id: str
    recommendation_authority: CommercialDeliberationAuthority
    recommendation: CommercialDecisionRecommendation
    confidence: float
    decisive_factors: list[str]
    decisive_uncertainty: str
    evidence_used: list[str]
    evidence_refs: list[str]
    quality_gate_context: JsonObject
    risk_flags: list[str]
    operator_gate_defaults: JsonObject
    rationale: str
    model_routes_used: list[str]
    degraded: bool
    record_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class Project:
    name: str
    objective: str
    revenue_mechanism: CommercialRevenueMechanism
    operator_role: ProjectOperatorRole
    external_commitment_policy: ProjectCommitmentPolicy
    phases: list[JsonObject]
    success_metrics: list[str]
    kill_criteria: list[str]
    project_id: str = field(default_factory=new_id)
    opportunity_id: str | None = None
    decision_packet_id: str | None = None
    decision_id: str | None = None
    budget_id: str | None = None
    evidence_refs: list[str] = field(default_factory=list)
    status: ProjectStatus = "proposed"
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectTask:
    project_id: str
    task_type: ProjectTaskType
    autonomy_class: AutonomyClass
    objective: str
    inputs: JsonObject
    risk_level: RiskLevel
    required_capabilities: list[JsonObject]
    model_requirement: JsonObject
    authority_required: Authority
    recovery_policy: RecoveryPolicy
    task_id: str = field(default_factory=new_id)
    phase_name: str | None = None
    expected_output_schema: JsonObject | None = None
    budget_id: str | None = None
    deadline: str | None = None
    status: ProjectTaskStatus = "queued"
    command_id: str | None = None
    policy_version: str | None = None
    idempotency_key: str | None = None
    evidence_refs: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectTaskAssignment:
    task_id: str
    project_id: str
    worker_type: ProjectTaskWorkerType
    worker_id: str
    grant_ids: list[str]
    assignment_id: str = field(default_factory=new_id)
    route_decision_id: str | None = None
    accepted_capabilities: list[JsonObject] = field(default_factory=list)
    status: ProjectTaskAssignmentStatus = "accepted"
    notes: str | None = None
    assigned_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectOutcome:
    project_id: str
    outcome_type: ProjectOutcomeType
    summary: str
    artifact_refs: list[str]
    metrics: JsonObject
    feedback: JsonObject
    revenue_impact: JsonObject
    outcome_id: str = field(default_factory=new_id)
    task_id: str | None = None
    phase_name: str | None = None
    operator_load_actual: str | None = None
    side_effect_intent_id: str | None = None
    side_effect_receipt_id: str | None = None
    status: ProjectOutcomeStatus = "recorded"
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectArtifactReceipt:
    project_id: str
    artifact_ref: str
    artifact_kind: ProjectArtifactKind
    summary: str
    data_class: DataClass
    delivery_channel: str
    receipt_id: str = field(default_factory=new_id)
    task_id: str | None = None
    side_effect_intent_id: str | None = None
    side_effect_receipt_id: str | None = None
    customer_visible: bool = False
    status: ProjectArtifactStatus = "recorded"
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectCustomerFeedback:
    project_id: str
    source_type: ProjectFeedbackSourceType
    summary: str
    sentiment: ProjectFeedbackSentiment
    feedback_id: str = field(default_factory=new_id)
    task_id: str | None = None
    artifact_receipt_id: str | None = None
    customer_ref: str | None = None
    evidence_refs: list[str] = field(default_factory=list)
    action_required: bool = False
    operator_review_required: bool = True
    status: ProjectFeedbackStatus = "recorded"
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectRevenueAttribution:
    project_id: str
    amount_usd: Decimal
    source: ProjectRevenueSource
    attribution_period: str
    confidence: float
    attribution_id: str = field(default_factory=new_id)
    task_id: str | None = None
    outcome_id: str | None = None
    artifact_receipt_id: str | None = None
    external_ref: str | None = None
    evidence_refs: list[str] = field(default_factory=list)
    reconciliation_task_id: str | None = None
    status: ProjectRevenueStatus = "recorded"
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectOperatorLoadRecord:
    project_id: str
    minutes: int
    load_type: ProjectOperatorLoadType
    source: str
    load_id: str = field(default_factory=new_id)
    task_id: str | None = None
    outcome_id: str | None = None
    artifact_receipt_id: str | None = None
    notes: str | None = None
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectPhaseRollup:
    phase_name: str
    task_counts: JsonObject
    outcome_counts: JsonObject
    artifact_count: int
    customer_feedback_count: int
    revenue_attributed_usd: Decimal
    operator_load_minutes: int
    status: ProjectPhaseRollupStatus
    last_activity_at: str | None = None


@dataclass(frozen=True)
class ProjectCommercialRollup:
    project_id: str
    revenue_reconciled_usd: Decimal
    revenue_unreconciled_usd: Decimal
    retained_customer_count: int
    at_risk_customer_count: int
    churned_customer_count: int
    support_resolved_count: int
    support_open_count: int
    maintenance_resolved_count: int
    maintenance_open_count: int
    external_commitment_count: int
    receiptless_side_effect_count: int
    evidence_refs: list[str]
    risk_flags: list[str]
    rollup_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectStatusRollup:
    project_id: str
    project_status: ProjectStatus
    phase_rollups: list[ProjectPhaseRollup]
    task_counts: JsonObject
    outcome_counts: JsonObject
    artifact_count: int
    customer_feedback_count: int
    revenue_attributed_usd: Decimal
    operator_load_minutes: int
    recommended_status: ProjectStatus
    close_recommendation: ProjectCloseRecommendation
    rationale: str
    risk_flags: list[str]
    commercial_rollup_id: str | None = None
    commercial_rollup: JsonObject = field(default_factory=dict)
    rollup_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectCloseDecisionPacket:
    project_id: str
    decision_id: str
    rollup_id: str
    recommendation: ProjectCloseRecommendation
    required_authority: Authority
    rationale: str
    risk_flags: list[str]
    evidence_refs: list[str]
    default_on_timeout: str
    status: Literal["gated", "decided", "cancelled"] = "gated"
    packet_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectReplayProjectionComparison:
    project_id: str
    replay_project_status: str | None
    projection_project_status: str | None
    replay_task_counts: JsonObject
    projection_task_counts: JsonObject
    replay_revenue_attributed_usd: Decimal
    projection_revenue_attributed_usd: Decimal
    replay_operator_load_minutes: int
    projection_operator_load_minutes: int
    replay_commercial_rollup: JsonObject
    projection_commercial_rollup: JsonObject
    matches: bool
    mismatches: list[str]
    comparison_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectPortfolioDecisionPacket:
    decision_id: str
    scope: str
    project_ids: list[str]
    rollup_ids: list[str]
    recommendation: str
    required_authority: Authority
    packet: JsonObject
    tradeoffs: JsonObject
    evidence_refs: list[str]
    risk_flags: list[str]
    default_on_timeout: str
    status: Literal["gated", "decided", "cancelled"] = "gated"
    packet_id: str = field(default_factory=new_id)
    verdict: str | None = None
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectPortfolioReplayProjectionComparison:
    packet_id: str
    replay_packet: JsonObject
    projection_packet: JsonObject
    matches: bool
    mismatches: list[str]
    comparison_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectSchedulingIntent:
    portfolio_packet_id: str
    source_decision_id: str
    scope: str
    project_ids: list[str]
    scheduling_window: str
    intent: JsonObject
    queue_adjustments: list[JsonObject]
    evidence_refs: list[str]
    risk_flags: list[str]
    required_authority: Authority
    authority_effect: str
    intent_id: str = field(default_factory=new_id)
    status: Literal["recorded", "superseded", "cancelled"] = "recorded"
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectSchedulingPriorityChangePacket:
    intent_id: str
    portfolio_packet_id: str
    source_decision_id: str
    decision_id: str
    scope: str
    project_ids: list[str]
    scheduling_window: str
    proposed_changes: list[JsonObject]
    evidence_refs: list[str]
    risk_flags: list[str]
    required_authority: Authority
    default_on_timeout: str
    packet_id: str = field(default_factory=new_id)
    status: Literal["gated", "decided", "cancelled"] = "gated"
    verdict: str | None = None
    applied_changes: list[JsonObject] = field(default_factory=list)
    created_at: str = field(default_factory=now_iso)
    decided_by: str | None = None
    decided_at: str | None = None


@dataclass(frozen=True)
class ProjectSchedulingPriorityReplayProjectionComparison:
    packet_id: str
    replay_packet: JsonObject
    projection_packet: JsonObject
    matches: bool
    mismatches: list[str]
    comparison_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectSchedulingReplayProjectionComparison:
    intent_id: str
    replay_intent: JsonObject
    projection_intent: JsonObject
    matches: bool
    mismatches: list[str]
    comparison_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectCustomerVisiblePacket:
    project_id: str
    outcome_id: str
    decision_id: str
    packet_type: Literal["customer_message", "customer_delivery"]
    customer_ref: str
    channel: str
    subject: str
    summary: str
    payload_ref: str
    side_effect_intent_id: str
    evidence_refs: list[str]
    required_authority: Authority
    default_on_timeout: str
    packet_id: str = field(default_factory=new_id)
    risk_flags: list[str] = field(default_factory=list)
    status: Literal["gated", "decided", "cancelled"] = "gated"
    verdict: str | None = None
    created_at: str = field(default_factory=now_iso)
    decided_by: str | None = None
    decided_at: str | None = None


@dataclass(frozen=True)
class ProjectCustomerCommitment:
    packet_id: str
    project_id: str
    outcome_id: str
    side_effect_intent_id: str
    side_effect_receipt_id: str
    customer_ref: str
    channel: str
    commitment_type: Literal["message_sent", "delivery_made"]
    payload_ref: str
    summary: str
    evidence_refs: list[str]
    commitment_id: str = field(default_factory=new_id)
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectCustomerCommitmentReceipt:
    commitment_id: str
    project_id: str
    receipt_type: ProjectCustomerCommitmentReceiptType
    summary: str
    receipt_id: str = field(default_factory=new_id)
    source_type: ProjectFeedbackSourceType = "customer"
    customer_ref: str | None = None
    evidence_refs: list[str] = field(default_factory=list)
    action_required: bool = True
    status: ProjectCustomerCommitmentReceiptStatus = "needs_followup"
    followup_task_id: str | None = None
    created_at: str = field(default_factory=now_iso)


@dataclass(frozen=True)
class ProjectCustomerVisibleReplayProjectionComparison:
    packet_id: str
    replay_packet: JsonObject
    projection_packet: JsonObject
    replay_commitments: list[JsonObject]
    projection_commitments: list[JsonObject]
    replay_commitment_receipts: list[JsonObject]
    projection_commitment_receipts: list[JsonObject]
    matches: bool
    mismatches: list[str]
    comparison_id: str = field(default_factory=new_id)
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
