from __future__ import annotations

import datetime
from typing import Optional

from .types import (
    BudgetState,
    G3Path,
    G3Status,
    JWTClaims,
    ModelInfo,
    RoutingDecision,
    RoutingTier,
    SystemPhase,
    TaskMetadata,
)

_DEFAULT_ESTIMATED_TOKENS = 2000
_ROI_EPSILON = 1e-12


def _filter_commercial(models: list[ModelInfo]) -> tuple[list[ModelInfo], dict[str, str]]:
    """Apply commercial use gate. Returns (permitted_models, {model_id: skip_reason})."""
    permitted: list[ModelInfo] = []
    skipped: dict[str, str] = {}
    for model in models:
        if model.tier == "local" or model.commercial_use_permitted:
            permitted.append(model)
        else:
            skipped[model.model_id] = (
                f"Model {model.model_id} excluded by commercial use gate for tier {model.tier}."
            )
    return permitted, skipped


def _best_model_for_tier(
    models: list[ModelInfo],
    tier: str,
    min_quality: float,
    check_quota: bool = False,
    check_rate_limit: bool = False,
) -> tuple[Optional[ModelInfo], str]:
    """Find best model at a given tier meeting quality + availability constraints."""
    tier_models = [m for m in models if m.tier == tier]
    if not tier_models:
        return None, f"no {tier} model available"

    candidates = []
    reasons = []
    for model in sorted(tier_models, key=lambda m: (m.model_id,)):
        if model.quality_score < min_quality:
            reasons.append(
                f"{model.model_id} quality {model.quality_score:.4f} below threshold {min_quality:.4f}"
            )
            continue
        if check_quota and model.quota_remaining is not None and model.quota_remaining <= 0:
            reasons.append(f"{model.model_id} quota exhausted")
            continue
        if check_rate_limit and model.rate_limit_remaining is not None and model.rate_limit_remaining <= 0:
            reasons.append(f"{model.model_id} rate limit exhausted")
            continue
        candidates.append(model)

    if not candidates:
        return None, "; ".join(reasons) if reasons else f"no {tier} model qualified"

    return sorted(candidates, key=lambda m: (-m.quality_score, m.model_id))[0], ""


def _compute_roi(task: TaskMetadata, budget: BudgetState, estimated_cost: float) -> float:
    """Compute ROI for a paid routing decision. Returns -1.0 if not computable."""
    if estimated_cost < 0:
        return -1.0

    if task.estimated_task_value_usd is not None:
        estimated_value = task.estimated_task_value_usd
    elif budget.project_cashflow_target_usd is not None:
        estimated_value = budget.project_cashflow_target_usd * budget.task_contribution_pct
    else:
        return -1.0

    if estimated_cost == 0:
        return float("inf") if estimated_value > 0 else -1.0

    return (estimated_value - estimated_cost) / estimated_cost


def _check_g3_timeout(budget: BudgetState, current_time: datetime.datetime) -> bool:
    """Returns True if G3 has timed out (>=6h since request)."""
    if budget.g3_status != G3Status.PENDING or budget.g3_requested_at is None:
        return False
    return (current_time - budget.g3_requested_at) >= datetime.timedelta(hours=budget.g3_timeout_hours)


def _build_justification(selected_tier: RoutingTier, skipped: dict[str, str]) -> str:
    """Build human-readable justification from skipped reasons."""
    ordered = ["local", "free_cloud", "subscription", "paid_cloud"]
    priority_parts = [f"{k}: {skipped[k]}" for k in ordered if k in skipped]
    model_parts = [f"{k}: {v}" for k, v in sorted(skipped.items()) if k not in ordered]
    pieces = priority_parts + model_parts
    return f"Selected {selected_tier.value}. {' | '.join(pieces) if pieces else 'no tiers skipped'}."


def route_task(
    task: TaskMetadata,
    available_models: list[ModelInfo],
    budget: BudgetState,
    jwt: JWTClaims,
    current_time: Optional[datetime.datetime] = None,
) -> RoutingDecision:
    """Pure decision logic for financial routing waterfall."""
    now = current_time or datetime.datetime.now(datetime.timezone.utc)
    skipped: dict[str, str] = {}

    models = sorted(available_models, key=lambda m: (m.tier, m.model_id))
    permitted_models, commercial_skips = _filter_commercial(models)
    skipped.update(commercial_skips)

    g3_expired = _check_g3_timeout(budget, now)
    if g3_expired:
        skipped["paid_cloud"] = "G3 request expired (timeout reached), paid routing skipped and fallback applied."

    local_model, local_reason = _best_model_for_tier(permitted_models, "local", task.quality_threshold)
    if local_model is not None:
        return RoutingDecision(RoutingTier.LOCAL, local_model.model_id, G3Path.NOT_APPLICABLE, 0.0, False, _build_justification(RoutingTier.LOCAL, skipped), skipped, False, False)
    skipped["local"] = local_reason

    free_model, free_reason = _best_model_for_tier(permitted_models, "free_cloud", task.quality_threshold, check_quota=True)
    if free_model is not None:
        return RoutingDecision(RoutingTier.FREE_CLOUD, free_model.model_id, G3Path.NOT_APPLICABLE, 0.0, False, _build_justification(RoutingTier.FREE_CLOUD, skipped), skipped, False, False)
    skipped["free_cloud"] = free_reason

    sub_model, sub_reason = _best_model_for_tier(permitted_models, "subscription", task.quality_threshold, check_rate_limit=True)
    if sub_model is not None:
        return RoutingDecision(RoutingTier.SUBSCRIPTION, sub_model.model_id, G3Path.NOT_APPLICABLE, 0.0, False, _build_justification(RoutingTier.SUBSCRIPTION, skipped), skipped, False, False)
    skipped["subscription"] = sub_reason

    if not g3_expired:
        if budget.system_phase == SystemPhase.CONSTRUCTION:
            skipped["paid_cloud"] = "construction phase — paid routing prohibited."
        elif task.is_council_tier1_preassessment:
            skipped["paid_cloud"] = "tier 1 council pre-assessment is always free, never gated."
        else:
            paid_candidates = [m for m in permitted_models if m.tier == "paid"]
            if not paid_candidates:
                skipped["paid_cloud"] = "no paid model available"
            else:
                qualified_paid: list[tuple[ModelInfo, float, G3Path, bool]] = []
                paid_fail_reasons: list[str] = []
                for model in sorted(paid_candidates, key=lambda m: (m.model_id,)):
                    if model.quality_score < task.quality_threshold:
                        paid_fail_reasons.append(f"{model.model_id} quality {model.quality_score:.4f} below threshold {task.quality_threshold:.4f}")
                        continue
                    estimated_cost = model.cost_per_1k_tokens * _DEFAULT_ESTIMATED_TOKENS / 1000.0
                    if jwt.current_session_spend_usd + estimated_cost >= jwt.max_api_spend_usd:
                        paid_fail_reasons.append(
                            f"{model.model_id} blocked: JWT session spend cap would be exceeded ({jwt.current_session_spend_usd:.4f} + {estimated_cost:.4f} >= {jwt.max_api_spend_usd:.4f})."
                        )
                        continue
                    roi = _compute_roi(task, budget, estimated_cost)
                    if roi + _ROI_EPSILON < 5.0:
                        paid_fail_reasons.append(f"{model.model_id} rejected: ROI {roi:.4f} below 5.0")
                        continue

                    if budget.project_cloud_spend_cap_usd is not None:
                        headroom = budget.project_cloud_spend_cap_usd - budget.project_cloud_spend_current_usd
                        if estimated_cost > headroom:
                            paid_fail_reasons.append(f"{model.model_id} rejected: insufficient budget headroom ({estimated_cost:.4f} > {headroom:.4f})")
                            continue
                        g3_path, requires_approval = G3Path.WITHIN_BUDGET, False
                    else:
                        g3_path, requires_approval = G3Path.OUTSIDE_BUDGET, True
                    qualified_paid.append((model, estimated_cost, g3_path, requires_approval))

                if qualified_paid:
                    selected_model, estimated_cost, g3_path, requires_approval = sorted(
                        qualified_paid, key=lambda entry: (-entry[0].quality_score, entry[0].model_id)
                    )[0]
                    return RoutingDecision(RoutingTier.PAID_CLOUD, selected_model.model_id, g3_path, estimated_cost, False, _build_justification(RoutingTier.PAID_CLOUD, skipped), skipped, requires_approval, False)
                skipped["paid_cloud"] = "; ".join(paid_fail_reasons)

    sub_fallback_models = [
        m for m in permitted_models
        if m.tier == "subscription" and (m.rate_limit_remaining is None or m.rate_limit_remaining > 0)
    ]
    if sub_fallback_models:
        fallback = sorted(sub_fallback_models, key=lambda m: (-m.quality_score, m.model_id))[0]
        return RoutingDecision(RoutingTier.DEFAULT_FALLBACK, fallback.model_id, G3Path.NOT_APPLICABLE, 0.0, True, _build_justification(RoutingTier.DEFAULT_FALLBACK, skipped), skipped, False, False)

    if "subscription" not in skipped:
        skipped["subscription"] = "no subscription model available for default fallback"

    return RoutingDecision(RoutingTier.COMPUTE_STARVED, None, G3Path.NOT_APPLICABLE, 0.0, False, _build_justification(RoutingTier.COMPUTE_STARVED, skipped), skipped, False, True)
