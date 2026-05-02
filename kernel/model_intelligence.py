from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from .records import (
    ArtifactRef,
    Command,
    DataClass,
    Decision,
    HoldoutPolicy,
    HoldoutUseRecord,
    JsonObject,
    LocalOffloadEvalSet,
    ModelCandidate,
    ModelDemotionRecord,
    ModelEvalRun,
    ModelPromotionDecisionPacket,
    ModelRouteDecision,
    ModelTaskClass,
    ModelTaskClassRecord,
    new_id,
)
from .store import KERNEL_POLICY_VERSION, KernelStore, KernelTransaction


SEED_TASK_CLASSES: tuple[str, ...] = (
    "quick_research_summarization",
    "source_claim_extraction",
    "coding_small_patch",
)


def model_intelligence_command(command_type: str, key: str, payload: dict | None = None) -> Command:
    return Command(
        command_type=command_type,
        requested_by="operator",
        requester_id="operator",
        target_entity_type="model",
        idempotency_key=key,
        requested_authority="operator_gate",
        payload=payload or {"key": key},
    )


def default_seed_task_classes() -> list[ModelTaskClassRecord]:
    return [
        ModelTaskClassRecord(
            task_class="quick_research_summarization",
            description="Summarize a small evidence bundle into decision-useful bullets without adding uncited claims.",
            quality_threshold=0.82,
            reliability_threshold=0.95,
            latency_p95_ms=20_000,
            local_offload_target=0.35,
            allowed_data_classes=["public", "internal"],
            promotion_authority="operator_gate",
        ),
        ModelTaskClassRecord(
            task_class="source_claim_extraction",
            description="Extract cited atomic claims from source records with freshness, confidence, and uncertainty labels.",
            quality_threshold=0.86,
            reliability_threshold=0.96,
            latency_p95_ms=25_000,
            local_offload_target=0.30,
            allowed_data_classes=["public", "internal"],
            promotion_authority="operator_gate",
        ),
        ModelTaskClassRecord(
            task_class="coding_small_patch",
            description="Produce a bounded single-purpose code patch with tests and no authority over release or deployment.",
            quality_threshold=0.80,
            reliability_threshold=0.94,
            latency_p95_ms=60_000,
            local_offload_target=0.20,
            allowed_data_classes=["public", "internal", "sensitive"],
            promotion_authority="operator_gate",
        ),
    ]


@dataclass(frozen=True)
class ShadowOutputArtifact:
    """Replay pointer for one production or shadow output artifact."""

    artifact_uri: str
    data_class: DataClass
    content_hash: str
    retention_policy: str
    deletion_policy: str
    encryption_status: str = "unencrypted"
    source_notes: str | None = None
    artifact_id: str = field(default_factory=new_id)

    def to_artifact_ref(self) -> ArtifactRef:
        return ArtifactRef(
            artifact_id=self.artifact_id,
            artifact_uri=self.artifact_uri,
            data_class=self.data_class,
            content_hash=self.content_hash,
            retention_policy=self.retention_policy,
            deletion_policy=self.deletion_policy,
            encryption_status=self.encryption_status,  # type: ignore[arg-type]
            source_notes=self.source_notes,
        )


@dataclass(frozen=True)
class ShadowOutputSample:
    """One candidate output scored beside a production output."""

    sample_id: str
    input_ref: str
    production_output: ShadowOutputArtifact
    shadow_output: ShadowOutputArtifact
    quality_score: float
    reliability_score: float
    latency_ms: int
    cost_usd: Decimal
    failure_categories: list[str]
    failure_modes: list[str]
    disagreement: JsonObject
    scorer_notes: str | None = None


@dataclass(frozen=True)
class ShadowExecutionRecord:
    task_id: str
    task_class: ModelTaskClass
    dataset_version: str
    eval_set_id: str
    candidate_model_id: str
    data_class: DataClass
    risk_level: str
    production_route: JsonObject
    candidate_route_version: str
    candidate_route_metadata: JsonObject
    samples: list[ShadowOutputSample]
    scorer_id: str = "kernel-shadow-output-scorer"
    execution_metadata: JsonObject = field(default_factory=dict)


@dataclass(frozen=True)
class ShadowExecutionResult:
    route_decision_id: str
    eval_run_id: str
    artifact_ids: list[str]


class KernelModelIntelligence:
    """Seed Model Intelligence kernel lane.

    This class records registry, eval metadata, holdout governance, and routing
    evidence. It intentionally has no API that promotes a model.
    """

    def __init__(self, store: KernelStore) -> None:
        self.store = store

    def register_seed_task_classes(self, command_key_prefix: str = "model-task-class") -> list[str]:
        registered: list[str] = []
        for idx, task_class in enumerate(default_seed_task_classes(), start=1):
            command = model_intelligence_command(
                "model.task_class.register",
                f"{command_key_prefix}-{idx}-{task_class.task_class}",
                {"task_class": task_class.task_class},
            )
            registered.append(self.store.register_model_task_class(command, task_class))
        return registered

    def register_candidate(self, command: Command, candidate: ModelCandidate) -> str:
        return self.store.register_model_candidate(command, candidate)

    def create_holdout_policy(self, command: Command, policy: HoldoutPolicy) -> str:
        return self.store.create_holdout_policy(command, policy)

    def register_eval_set(self, command: Command, eval_set: LocalOffloadEvalSet) -> str:
        return self.store.register_local_offload_eval_set(command, eval_set)

    def record_holdout_use(self, command: Command, holdout_use: HoldoutUseRecord) -> str:
        return self.store.record_holdout_use(command, holdout_use)

    def record_eval_run(self, command: Command, eval_run: ModelEvalRun) -> str:
        return self.store.record_model_eval_run(command, eval_run)

    def record_route_decision(self, command: Command, route_decision: ModelRouteDecision) -> str:
        return self.store.record_model_route_decision(command, route_decision)

    def create_promotion_decision_packet(self, command: Command, packet: ModelPromotionDecisionPacket) -> str:
        return self.store.create_model_promotion_decision_packet(command, packet)

    def create_decision(self, command: Command, decision: Decision) -> str:
        return self.store.create_decision(command, decision)

    def record_demotion(self, command: Command, demotion: ModelDemotionRecord) -> str:
        return self.store.record_model_demotion(command, demotion)

    def record_shadow_execution(self, command: Command, record: ShadowExecutionRecord) -> ShadowExecutionResult:
        """Record shadow output evidence without changing production route state."""

        if record.task_class not in SEED_TASK_CLASSES:
            raise ValueError("only seed task classes can receive pre-Hermes shadow execution records")
        if not record.samples:
            raise ValueError("shadow execution requires at least one scored output sample")
        if record.production_route.get("selected_model_id") == record.candidate_model_id:
            raise ValueError("shadow execution candidate cannot be the production selected model")
        if record.production_route.get("route_effect") not in {None, "production"}:
            raise ValueError("production route metadata cannot declare shadow authority")

        def handler(tx: KernelTransaction) -> ShadowExecutionResult:
            artifact_ids: list[str] = []
            for sample in record.samples:
                artifact_ids.append(tx.create_artifact_ref(sample.production_output.to_artifact_ref()))
                artifact_ids.append(tx.create_artifact_ref(sample.shadow_output.to_artifact_ref()))

            route_decision = ModelRouteDecision(
                task_id=record.task_id,
                task_class=record.task_class,
                data_class=record.data_class,
                risk_level=record.risk_level,  # type: ignore[arg-type]
                selected_route="shadow",
                selected_model_id=None,
                candidate_model_id=record.candidate_model_id,
                eval_set_id=record.eval_set_id,
                reasons=[
                    "production route remains authoritative",
                    "candidate output is recorded as evidence_only shadow scoring",
                ],
                required_authority="operator_gate",
                decision_id=None,
                local_offload_estimate=_shadow_local_offload_estimate(record),
                frontier_fallback={
                    "production_route": record.production_route,
                    "route_effect": "unchanged",
                },
            )
            route_decision_id = tx.record_model_route_decision(route_decision)

            eval_run = _shadow_eval_run(record, route_decision_id, artifact_ids)
            eval_run_id = tx.record_model_eval_run(eval_run)
            return ShadowExecutionResult(
                route_decision_id=route_decision_id,
                eval_run_id=eval_run_id,
                artifact_ids=artifact_ids,
            )

        return self.store.execute_command(command, handler)

    def promotion_packet(
        self,
        *,
        model_id: str,
        task_class: str,
        proposed_routing_role: str,
        decision_id: str,
        eval_run_ids: list[str],
        holdout_use_ids: list[str],
        evidence_refs: list[str],
        frozen_holdout_confidence: float,
        confidence_threshold: float,
        gate_packet: dict,
        risk_flags: list[str] | None = None,
        recommendation: str = "promote",
        default_on_timeout: str = "keep_current_route",
    ) -> ModelPromotionDecisionPacket:
        if task_class not in SEED_TASK_CLASSES:
            raise ValueError("only seed task classes can receive pre-Hermes promotion packets")
        return ModelPromotionDecisionPacket(
            model_id=model_id,
            task_class=task_class,  # type: ignore[arg-type]
            proposed_routing_role=proposed_routing_role,  # type: ignore[arg-type]
            recommendation=recommendation,  # type: ignore[arg-type]
            required_authority="operator_gate",
            decision_id=decision_id,
            eval_run_ids=eval_run_ids,
            holdout_use_ids=holdout_use_ids,
            evidence_refs=evidence_refs,
            frozen_holdout_confidence=frozen_holdout_confidence,
            confidence_threshold=confidence_threshold,
            gate_packet=gate_packet,
            risk_flags=risk_flags or [],
            default_on_timeout=default_on_timeout,
        )

    def promotion_decision(
        self,
        *,
        model_id: str,
        task_class: str,
        proposed_routing_role: str,
        question: str,
        recommendation: str = "promote",
        confidence: float | None = None,
        evidence_bundle_ids: list[str] | None = None,
        evidence_refs: list[str] | None = None,
        risk_flags: list[str] | None = None,
        gate_packet: dict | None = None,
        stakes: str = "high",
        status: str = "proposed",
        default_on_timeout: str = "keep_current_route",
    ) -> Decision:
        if task_class not in SEED_TASK_CLASSES:
            raise ValueError("only seed task classes can receive pre-Hermes promotion decisions")
        return Decision(
            decision_type="model_promotion",
            question=question,
            options=[
                {"option_id": "promote", "label": f"Promote {model_id} for {task_class}"},
                {"option_id": "keep_shadow", "label": "Keep candidate in shadow mode"},
                {"option_id": "reject", "label": "Reject promotion"},
                {"option_id": "needs_more_data", "label": "Require more evidence"},
            ],
            stakes=stakes,  # type: ignore[arg-type]
            evidence_bundle_ids=evidence_bundle_ids or [],
            evidence_refs=evidence_refs or [],
            requested_by="model_intelligence",
            required_authority="operator_gate",
            authority_policy_version=KERNEL_POLICY_VERSION,
            status=status,  # type: ignore[arg-type]
            recommendation=recommendation,
            confidence=confidence,
            decisive_factors=[
                f"model_id={model_id}",
                f"task_class={task_class}",
                f"proposed_routing_role={proposed_routing_role}",
            ],
            risk_flags=risk_flags or [],
            default_on_timeout=default_on_timeout,
            gate_packet=gate_packet,
        )

    def seed_holdout_policy(
        self,
        task_class: str,
        dataset_version: str,
        *,
        min_sample_count: int = 12,
    ) -> HoldoutPolicy:
        if task_class not in SEED_TASK_CLASSES:
            raise ValueError("only seed task classes can receive pre-Hermes holdout policies")
        return HoldoutPolicy(
            policy_id=new_id(),
            task_class=task_class,  # type: ignore[arg-type]
            dataset_version=dataset_version,
            access="scoring_service",
            min_sample_count=min_sample_count,
            contamination_controls=[
                "frozen_holdout_artifact_ref_only",
                "no_development_access",
                "requester_change_ref_screening",
                "promotion_gate_requires_decision_id",
            ],
            scorer_separation="worker changes may use development/regression only; holdout scoring is separate",
            promotion_requires_decision=True,
        )


def _shadow_eval_run(
    record: ShadowExecutionRecord,
    route_decision_id: str,
    artifact_ids: list[str],
) -> ModelEvalRun:
    quality_scores = [sample.quality_score for sample in record.samples]
    reliability_scores = [sample.reliability_score for sample in record.samples]
    latencies = [sample.latency_ms for sample in record.samples]
    cost_total = sum((sample.cost_usd for sample in record.samples), Decimal("0"))
    sample_count = len(record.samples)
    quality_score = sum(quality_scores) / sample_count
    reliability_score = sum(reliability_scores) / sample_count
    latency_p50_ms = _percentile(latencies, 50)
    latency_p95_ms = _percentile(latencies, 95)
    cost_per_1k_tasks = (cost_total / Decimal(sample_count)) * Decimal("1000")
    failure_categories = sorted({category for sample in record.samples for category in sample.failure_categories})
    failure_modes = [mode for sample in record.samples for mode in sample.failure_modes]
    disagreement_count = sum(1 for sample in record.samples if sample.disagreement.get("has_disagreement"))
    artifact_refs = [
        {
            "sample_id": sample.sample_id,
            "input_ref": sample.input_ref,
            "production_output_artifact_id": sample.production_output.artifact_id,
            "production_output_artifact_uri": sample.production_output.artifact_uri,
            "shadow_output_artifact_id": sample.shadow_output.artifact_id,
            "shadow_output_artifact_uri": sample.shadow_output.artifact_uri,
        }
        for sample in record.samples
    ]
    route_metadata = {
        "adapter": "shadow_output_execution_scoring",
        "authority_effect": "evidence_only",
        "production_route": record.production_route,
        "candidate_route": {
            "model_id": record.candidate_model_id,
            "route_version": record.candidate_route_version,
            "metadata": record.candidate_route_metadata,
        },
        "route_decision_id": route_decision_id,
        "artifact_ids": artifact_ids,
        "artifact_refs": artifact_refs,
        "execution_metadata": record.execution_metadata,
    }
    return ModelEvalRun(
        model_id=record.candidate_model_id,
        task_class=record.task_class,
        dataset_version=record.dataset_version,
        eval_set_id=record.eval_set_id,
        route_version=record.candidate_route_version,
        route_metadata=route_metadata,
        sample_count=sample_count,
        quality_score=quality_score,
        reliability_score=reliability_score,
        latency_p50_ms=latency_p50_ms,
        latency_p95_ms=latency_p95_ms,
        cost_per_1k_tasks=cost_per_1k_tasks.quantize(Decimal("0.0001")),
        aggregate_scores={
            "overall": round((quality_score + reliability_score) / 2, 6),
            "quality": round(quality_score, 6),
            "reliability": round(reliability_score, 6),
            "latency_p50_ms": latency_p50_ms,
            "latency_p95_ms": latency_p95_ms,
            "disagreement_rate": round(disagreement_count / sample_count, 6),
        },
        failure_categories=failure_categories,
        failure_modes=failure_modes,
        confidence={
            "score": min(0.99, max(0.0, reliability_score * min(1.0, sample_count / 25))),
            "method": "shadow_production_trace_scoring",
            "sample_count": sample_count,
        },
        frozen_holdout_result={
            "split": "shadow_production_trace",
            "sample_count": sample_count,
            "quality_score": round(quality_score, 6),
            "reliability_score": round(reliability_score, 6),
            "artifact_ref": f"kernel:model_eval_runs/shadow/{record.task_id}",
            "artifact_ids": artifact_ids,
        },
        verdict="shadow",
        baseline_model_id=None,
        scorer_id=record.scorer_id,
    )


def _shadow_local_offload_estimate(record: ShadowExecutionRecord) -> JsonObject:
    sample_count = len(record.samples)
    cost_total = sum((sample.cost_usd for sample in record.samples), Decimal("0"))
    production_cost = record.production_route.get("cost_usd")
    estimate: JsonObject = {
        "eligible": True,
        "sample_count": sample_count,
        "shadow_cost_usd": str(cost_total),
        "candidate_model_id": record.candidate_model_id,
    }
    if production_cost is not None:
        estimate["production_cost_usd"] = str(production_cost)
        estimate["estimated_savings_usd_per_1k"] = str(
            (Decimal(str(production_cost)) / Decimal(sample_count) - cost_total / Decimal(sample_count))
            * Decimal("1000")
        )
    return estimate


def _percentile(values: list[int], percentile: int) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    rank = max(1, int(round((percentile / 100) * len(ordered))))
    return ordered[min(rank, len(ordered)) - 1]
