from __future__ import annotations

import datetime
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class RoutingTier(Enum):
    """The tier the router selected."""

    LOCAL = "local"
    FREE_CLOUD = "free_cloud"
    SUBSCRIPTION = "subscription"
    PAID_CLOUD = "paid_cloud"
    DEFAULT_FALLBACK = "default_fallback"
    COMPUTE_STARVED = "compute_starved"


class G3Path(Enum):
    """Which G3 gate path applies for paid_cloud routing."""

    NOT_APPLICABLE = "not_applicable"
    WITHIN_BUDGET = "within_budget"
    OUTSIDE_BUDGET = "outside_budget"


class SystemPhase(Enum):
    """System lifecycle phase."""

    CONSTRUCTION = "construction"
    OPERATING = "operating"


class G3Status(Enum):
    """Status of a pending G3 gate request."""

    NOT_REQUIRED = "not_required"
    PENDING = "pending"
    APPROVED = "approved"
    EXPIRED = "expired"


@dataclass(frozen=True)
class TaskMetadata:
    """Metadata about the task requesting model routing."""

    task_id: str
    task_type: str
    required_capability: str
    quality_threshold: float
    estimated_task_value_usd: Optional[float] = None
    project_id: Optional[str] = None
    is_operating_phase: bool = False
    is_council_tier1_preassessment: bool = False


@dataclass(frozen=True)
class ModelInfo:
    """A model available for routing."""

    model_id: str
    tier: str
    commercial_use_permitted: bool
    quality_score: float
    cost_per_1k_tokens: float
    rate_limit_remaining: Optional[int] = None
    quota_remaining: Optional[int] = None


@dataclass(frozen=True)
class BudgetState:
    """Current financial state for routing decisions."""

    project_cloud_spend_cap_usd: Optional[float] = None
    project_cloud_spend_current_usd: float = 0.0
    system_phase: SystemPhase = SystemPhase.CONSTRUCTION
    project_cashflow_target_usd: Optional[float] = None
    task_contribution_pct: float = 0.01
    g3_status: G3Status = G3Status.NOT_REQUIRED
    g3_requested_at: Optional[datetime.datetime] = None
    g3_timeout_hours: float = 6.0


@dataclass(frozen=True)
class JWTClaims:
    """Session-level JWT claims relevant to financial routing."""

    session_id: str
    max_api_spend_usd: float = 0.00
    current_session_spend_usd: float = 0.0


@dataclass(frozen=True)
class RoutingDecision:
    """The router's output: which model to use and why."""

    tier: RoutingTier
    model_id: Optional[str]
    g3_path: G3Path
    estimated_cost_usd: float
    quality_warning: bool
    justification: str
    skipped_reasons: dict
    requires_operator_approval: bool
    compute_starved: bool
