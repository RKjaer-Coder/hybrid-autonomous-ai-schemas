"""M4 golden set — financial routing and compliance scenarios."""

from __future__ import annotations

from eval.fixtures.common import DeterministicFactory


def generate_m4_test_set() -> dict:
    factory = DeterministicFactory(seed=404)

    def task(
        *,
        quality_threshold: float,
        estimated_task_value_usd: float | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        return {
            "task_id": factory.uuid_v7(),
            "task_type": "reasoning",
            "required_capability": "analysis",
            "quality_threshold": quality_threshold,
            "estimated_task_value_usd": estimated_task_value_usd,
            "project_id": factory.uuid_v7(),
            "idempotency_key": idempotency_key,
            "is_operating_phase": estimated_task_value_usd is not None,
            "is_council_tier1_preassessment": False,
        }

    def budget(
        *,
        system_phase: str,
        project_cloud_spend_cap_usd: float | None = None,
        project_cloud_spend_current_usd: float = 0.0,
        project_cashflow_target_usd: float | None = None,
        g3_status: str = "not_required",
    ) -> dict:
        return {
            "system_phase": system_phase,
            "project_cloud_spend_cap_usd": project_cloud_spend_cap_usd,
            "project_cloud_spend_current_usd": project_cloud_spend_current_usd,
            "project_cashflow_target_usd": project_cashflow_target_usd,
            "task_contribution_pct": 0.01,
            "g3_status": g3_status,
            "g3_requested_at": None,
            "g3_timeout_hours": 6.0,
        }

    def jwt(*, max_api_spend_usd: float, current_session_spend_usd: float = 0.0) -> dict:
        return {
            "session_id": factory.uuid_v7(),
            "max_api_spend_usd": max_api_spend_usd,
            "current_session_spend_usd": current_session_spend_usd,
        }

    scenarios = [
        {
            "scenario_id": "m4-local",
            "task": task(quality_threshold=0.70),
            "models": [
                {"model_id": "local-coder", "tier": "local", "commercial_use_permitted": True, "quality_score": 0.92, "cost_per_1k_tokens": 0.0},
                {"model_id": "free-backup", "tier": "free_cloud", "commercial_use_permitted": True, "quality_score": 0.99, "cost_per_1k_tokens": 0.0, "quota_remaining": 50},
            ],
            "budget": budget(system_phase="construction"),
            "jwt": jwt(max_api_spend_usd=0.0),
            "expected": {
                "path_label": "local",
                "tier": "local",
                "model_id": "local-coder",
                "g3_path": "not_applicable",
                "requires_operator_approval": False,
                "quality_warning": False,
                "compute_starved": False,
                "reservation_required": False,
                "ledger": {"route_selected": "local", "g3_required": 0, "g3_status": None, "quality_warning": 0},
            },
        },
        {
            "scenario_id": "m4-free-cloud",
            "task": task(quality_threshold=0.75),
            "models": [
                {"model_id": "local-weak", "tier": "local", "commercial_use_permitted": True, "quality_score": 0.40, "cost_per_1k_tokens": 0.0},
                {"model_id": "free-gemini", "tier": "free_cloud", "commercial_use_permitted": True, "quality_score": 0.82, "cost_per_1k_tokens": 0.0, "quota_remaining": 12},
                {"model_id": "sub-strong", "tier": "subscription", "commercial_use_permitted": True, "quality_score": 0.95, "cost_per_1k_tokens": 0.0, "rate_limit_remaining": 10},
            ],
            "budget": budget(system_phase="construction"),
            "jwt": jwt(max_api_spend_usd=0.0),
            "expected": {
                "path_label": "free_cloud",
                "tier": "free_cloud",
                "model_id": "free-gemini",
                "g3_path": "not_applicable",
                "requires_operator_approval": False,
                "quality_warning": False,
                "compute_starved": False,
                "reservation_required": False,
                "ledger": {"route_selected": "free_cloud", "g3_required": 0, "g3_status": None, "quality_warning": 0},
            },
        },
        {
            "scenario_id": "m4-subscription",
            "task": task(quality_threshold=0.80),
            "models": [
                {"model_id": "local-weak", "tier": "local", "commercial_use_permitted": True, "quality_score": 0.30, "cost_per_1k_tokens": 0.0},
                {"model_id": "free-exhausted", "tier": "free_cloud", "commercial_use_permitted": True, "quality_score": 0.90, "cost_per_1k_tokens": 0.0, "quota_remaining": 0},
                {"model_id": "sub-primary", "tier": "subscription", "commercial_use_permitted": True, "quality_score": 0.84, "cost_per_1k_tokens": 0.0, "rate_limit_remaining": 8},
            ],
            "budget": budget(system_phase="construction"),
            "jwt": jwt(max_api_spend_usd=0.0),
            "expected": {
                "path_label": "subscription",
                "tier": "subscription",
                "model_id": "sub-primary",
                "g3_path": "not_applicable",
                "requires_operator_approval": False,
                "quality_warning": False,
                "compute_starved": False,
                "reservation_required": False,
                "ledger": {"route_selected": "subscription", "g3_required": 0, "g3_status": None, "quality_warning": 0},
            },
        },
        {
            "scenario_id": "m4-paid-within-budget",
            "task": task(
                quality_threshold=0.85,
                estimated_task_value_usd=10.0,
                idempotency_key=factory.uuid_v7(),
            ),
            "models": [
                {"model_id": "local-weak", "tier": "local", "commercial_use_permitted": True, "quality_score": 0.40, "cost_per_1k_tokens": 0.0},
                {"model_id": "free-exhausted", "tier": "free_cloud", "commercial_use_permitted": True, "quality_score": 0.80, "cost_per_1k_tokens": 0.0, "quota_remaining": 0},
                {"model_id": "sub-throttled", "tier": "subscription", "commercial_use_permitted": True, "quality_score": 0.83, "cost_per_1k_tokens": 0.0, "rate_limit_remaining": 0},
                {"model_id": "paid-frontier", "tier": "paid", "commercial_use_permitted": True, "quality_score": 0.95, "cost_per_1k_tokens": 0.005},
            ],
            "budget": budget(
                system_phase="operating",
                project_cloud_spend_cap_usd=20.0,
                project_cloud_spend_current_usd=1.0,
                project_cashflow_target_usd=1000.0,
            ),
            "jwt": jwt(max_api_spend_usd=20.0),
            "expected": {
                "path_label": "paid_cloud_within_budget",
                "tier": "paid_cloud",
                "model_id": "paid-frontier",
                "g3_path": "within_budget",
                "requires_operator_approval": False,
                "quality_warning": False,
                "compute_starved": False,
                "reservation_required": True,
                "ledger": {"route_selected": "paid_cloud", "g3_required": 0, "g3_status": "APPROVED", "quality_warning": 0},
            },
        },
        {
            "scenario_id": "m4-paid-pending-g3",
            "task": task(
                quality_threshold=0.85,
                estimated_task_value_usd=10.0,
                idempotency_key=factory.uuid_v7(),
            ),
            "models": [
                {"model_id": "local-weak", "tier": "local", "commercial_use_permitted": True, "quality_score": 0.40, "cost_per_1k_tokens": 0.0},
                {"model_id": "free-exhausted", "tier": "free_cloud", "commercial_use_permitted": True, "quality_score": 0.75, "cost_per_1k_tokens": 0.0, "quota_remaining": 0},
                {"model_id": "sub-throttled", "tier": "subscription", "commercial_use_permitted": True, "quality_score": 0.82, "cost_per_1k_tokens": 0.0, "rate_limit_remaining": 0},
                {"model_id": "paid-frontier", "tier": "paid", "commercial_use_permitted": True, "quality_score": 0.94, "cost_per_1k_tokens": 0.005},
            ],
            "budget": budget(system_phase="operating", project_cloud_spend_cap_usd=None, project_cashflow_target_usd=1000.0),
            "jwt": jwt(max_api_spend_usd=20.0),
            "g3_enforcement": "pending_approval",
            "autonomous_paid_forbidden": True,
            "expected": {
                "path_label": "paid_cloud_pending_g3",
                "tier": "paid_cloud",
                "model_id": "paid-frontier",
                "g3_path": "outside_budget",
                "requires_operator_approval": True,
                "quality_warning": False,
                "compute_starved": False,
                "reservation_required": True,
                "ledger": {"route_selected": "paid_cloud", "g3_required": 1, "g3_status": "PENDING", "quality_warning": 0},
            },
        },
        {
            "scenario_id": "m4-default-fallback",
            "task": task(quality_threshold=0.95),
            "models": [
                {"model_id": "local-weak", "tier": "local", "commercial_use_permitted": True, "quality_score": 0.55, "cost_per_1k_tokens": 0.0},
                {"model_id": "free-exhausted", "tier": "free_cloud", "commercial_use_permitted": True, "quality_score": 0.80, "cost_per_1k_tokens": 0.0, "quota_remaining": 0},
                {"model_id": "sub-fallback", "tier": "subscription", "commercial_use_permitted": True, "quality_score": 0.62, "cost_per_1k_tokens": 0.0, "rate_limit_remaining": 4},
            ],
            "budget": budget(system_phase="construction"),
            "jwt": jwt(max_api_spend_usd=0.0),
            "expected": {
                "path_label": "default_fallback",
                "tier": "default_fallback",
                "model_id": "sub-fallback",
                "g3_path": "not_applicable",
                "requires_operator_approval": False,
                "quality_warning": True,
                "compute_starved": False,
                "reservation_required": False,
                "ledger": {"route_selected": "default_fallback", "g3_required": 0, "g3_status": None, "quality_warning": 1},
            },
        },
        {
            "scenario_id": "m4-compute-starved",
            "task": task(
                quality_threshold=0.90,
                estimated_task_value_usd=10.0,
                idempotency_key=factory.uuid_v7(),
            ),
            "models": [
                {"model_id": "blocked-free", "tier": "free_cloud", "commercial_use_permitted": False, "quality_score": 0.95, "cost_per_1k_tokens": 0.0, "quota_remaining": 5},
                {"model_id": "paid-frontier", "tier": "paid", "commercial_use_permitted": True, "quality_score": 0.97, "cost_per_1k_tokens": 0.005},
            ],
            "budget": budget(system_phase="construction"),
            "jwt": jwt(max_api_spend_usd=0.0),
            "g3_enforcement": "non_paid_block",
            "autonomous_paid_forbidden": True,
            "expected": {
                "path_label": "compute_starved",
                "tier": "compute_starved",
                "model_id": None,
                "g3_path": "not_applicable",
                "requires_operator_approval": False,
                "quality_warning": False,
                "compute_starved": True,
                "reservation_required": False,
                "ledger": {"route_selected": "compute_starved", "g3_required": 0, "g3_status": None, "quality_warning": 0},
            },
        },
    ]

    return {
        "scenarios": scenarios,
        "evaluation_criteria": {
            "min_routing_match_rate": 1.0,
            "min_ledger_persistence_rate": 1.0,
            "min_g3_enforcement_rate": 1.0,
            "min_routing_path_coverage": 7,
            "max_false_autonomous_spend": 0,
        },
    }
