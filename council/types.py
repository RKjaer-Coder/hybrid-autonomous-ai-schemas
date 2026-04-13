from __future__ import annotations

from dataclasses import dataclass
import datetime
from enum import Enum, unique
from typing import List, Optional


@unique
class DecisionType(Enum):
    OPPORTUNITY_SCREEN = "opportunity_screen"
    GO_NO_GO = "go_no_go"
    KILL_REC = "kill_rec"
    PHASE_GATE = "phase_gate"
    OPERATOR_STRATEGIC = "operator_strategic"
    SYSTEM_CRITICAL = "system_critical"


@unique
class Recommendation(Enum):
    PURSUE = "PURSUE"
    REJECT = "REJECT"
    PAUSE = "PAUSE"
    ESCALATE = "ESCALATE"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


@unique
class CouncilTier(Enum):
    TIER_1 = 1
    TIER_2 = 2


@unique
class RoleName(Enum):
    STRATEGIST = "strategist"
    CRITIC = "critic"
    REALIST = "realist"
    DEVILS_ADVOCATE = "devils_advocate"


@unique
class DATag(Enum):
    INCORPORATED = "incorporated"
    ACKNOWLEDGED = "acknowledged"
    DISMISSED = "dismissed"


@unique
class BriefSignal(Enum):
    SUFFICIENT = "sufficient"
    INCOMPLETE = "incomplete"
    MISLEADING = "misleading"


@dataclass(frozen=True)
class RoleOutput:
    role: RoleName
    content: str
    token_count: int
    max_tokens: int


@dataclass(frozen=True)
class DAAssessment:
    objection: str
    tag: DATag
    reasoning: str


@dataclass(frozen=True)
class CouncilVerdict:
    verdict_id: str
    tier_used: int
    decision_type: DecisionType
    recommendation: Recommendation
    confidence: float
    reasoning_summary: str
    dissenting_views: str
    minority_positions: Optional[List[str]]
    full_debate_record: Optional[str]
    cost_usd: float
    project_id: Optional[str]
    outcome_record: None = None
    brief_quality: Optional[List["BriefQualitySignal"]] = None
    da_assessment: Optional[List[DAAssessment]] = None
    da_quality_score: Optional[float] = None
    tie_break: bool = False
    degraded: bool = False
    confidence_cap: Optional[float] = None
    created_at: str = ""

    def __post_init__(self) -> None:
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Confidence must be 0.0–1.0, got {self.confidence}")
        if self.tier_used not in (1, 2):
            raise ValueError(f"Tier must be 1 or 2, got {self.tier_used}")
        if self.degraded and self.confidence > 0.70:  # 0.70 is allowed; above 0.70 is rejected.
            raise ValueError(f"Degraded verdict confidence cannot exceed 0.70, got {self.confidence}")


@dataclass(frozen=True)
class BriefQualitySignal:
    verdict_id: str
    brief_id: str
    signal: BriefSignal
    missing_dimension: Optional[str]
    created_at: str = ""


@dataclass(frozen=True)
class CalibrationRecord:
    calibration_id: str
    verdict_id: str
    decision_type: DecisionType
    predicted_outcome: float
    actual_outcome: Optional[float]
    prediction_correct: Optional[float]
    role_weights_used: dict
    which_role_was_right: Optional[RoleName]
    da_quality_score: Optional[float]
    tie_break: bool = False
    created_at: str = ""


@dataclass(frozen=True)
class ContextPacket:
    decision_type: DecisionType
    subject_id: str
    context_text: str
    token_count: int
    max_tokens: int
    source_briefs: Optional[List[str]] = None


DEFAULT_ROLE_WEIGHTS: dict = {
    RoleName.STRATEGIST: 0.30,
    RoleName.CRITIC: 0.35,
    RoleName.REALIST: 0.25,
    RoleName.DEVILS_ADVOCATE: 0.10,
}

WEIGHT_FLOOR: float = 0.10
WEIGHT_CAP: float = 0.45
WEIGHT_DRIFT_PER_CYCLE: float = 0.10
WEIGHT_DRIFT_ABSOLUTE: float = 0.30


def iso_utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
