from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, unique
import os
import time


@unique
class CheckType(Enum):
    SHERIFF = "sheriff"
    JUDGE = "judge"


@unique
class Tier(Enum):
    FAST_PATH = "fast_path"
    DEEP_SCAN = "deep_scan"


@unique
class Outcome(Enum):
    PASS = "PASS"
    BLOCK = "BLOCK"


@unique
class BlockReason(Enum):
    IPI_DETECTED = "IPI_DETECTED"
    POLICY_VIOLATION = "POLICY_VIOLATION"
    STRUCTURAL_MALFORMATION = "STRUCTURAL_MALFORMATION"
    CONTENT_SAFETY = "CONTENT_SAFETY"
    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"
    TRUST_TIER_VIOLATION = "TRUST_TIER_VIOLATION"
    TIMEOUT = "TIMEOUT"
    INTERNAL_ERROR = "INTERNAL_ERROR"


@unique
class CircuitBreakerState(Enum):
    ARMED = "ARMED"
    TRIPPED = "TRIPPED"
    COOLDOWN = "COOLDOWN"


@unique
class AlertSeverity(Enum):
    IMMUNE_BLOCK_FAST = "IMMUNE_BLOCK_FAST"
    IMMUNE_BLOCK_DEEP = "IMMUNE_BLOCK_DEEP"
    IMMUNE_TIMEOUT = "IMMUNE_TIMEOUT"
    DEEP_SCAN_TIMEOUT = "DEEP_SCAN_TIMEOUT"
    SECURITY_ALERT = "SECURITY_ALERT"
    SECURITY_CASCADE = "SECURITY_CASCADE"


@dataclass(frozen=True)
class SheriffPayload:
    session_id: str
    skill_name: str
    tool_name: str
    arguments: dict
    raw_prompt: str | None = None
    source_trust_tier: int = 4
    jwt_claims: dict | None = None


@dataclass(frozen=True)
class JudgePayload:
    session_id: str
    skill_name: str
    tool_name: str
    output: dict
    expected_schema: dict | None = None
    max_trust_tier: int = 4
    memory_write_target: str | None = None


@dataclass(frozen=True)
class ImmuneVerdict:
    verdict_id: str
    check_type: CheckType
    tier: Tier
    skill_name: str
    session_id: str
    outcome: Outcome
    block_reason: BlockReason | None = None
    block_detail: str | None = None
    latency_ms: float = 0.0
    alert_severity: AlertSeverity | None = None

    def __post_init__(self) -> None:
        if self.outcome == Outcome.BLOCK and self.block_reason is None:
            raise ValueError("BLOCK outcome requires block_reason")
        if self.outcome == Outcome.PASS and self.block_reason is not None:
            raise ValueError("PASS outcome must not set block_reason")
        if self.latency_ms < 0:
            raise ValueError("latency_ms must be >= 0")
        if self.block_detail is not None and len(self.block_detail) > 200:
            object.__setattr__(self, "block_detail", self.block_detail[:200])


@dataclass(frozen=True)
class ImmuneConfig:
    sheriff_fast_path_timeout_ms: float = 50.0
    deep_scan_timeout_ms: float = 500.0
    judge_timeout_ms: float = 50.0
    tool_quarantine_block_rate: float = 0.20
    tool_quarantine_window_seconds: int = 900
    security_cascade_count: int = 3
    security_cascade_window_seconds: int = 60
    verdict_buffer_size: int = 64
    verdict_flush_interval_ms: int = 100
    deep_scan_enabled: bool = True
    deep_scan_timeout_rate_threshold: float = 0.10
    bootstrap_patch_enabled: bool = True
    context_params_enabled: bool = False
    permitted_endpoints: frozenset[str] = field(
        default_factory=lambda: frozenset({"localhost", "127.0.0.1", "::1"})
    )
    known_tool_registry: frozenset[str] = field(default_factory=frozenset)


class ImmuneBlockError(Exception):
    """Raised when Sheriff/Judge blocks tool dispatch."""

    def __init__(self, verdict: ImmuneVerdict):
        self.verdict = verdict
        reason = verdict.block_reason.value if verdict.block_reason else "unknown"
        super().__init__(f"Immune BLOCK: {reason} — {verdict.block_detail or 'no detail'}")


def generate_uuid_v7() -> str:
    """Generate UUIDv7 from current unix epoch milliseconds and random bits."""
    ts_ms = int(time.time_ns() // 1_000_000)
    rand = bytearray(os.urandom(10))
    b = bytearray(16)
    b[0:6] = ts_ms.to_bytes(6, "big")
    b[6] = (0x70 | (rand[0] & 0x0F))
    b[7] = rand[1]
    b[8] = (0x80 | (rand[2] & 0x3F))
    b[9:] = rand[3:10]
    h = b.hex()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


if __name__ == "__main__":
    print(generate_uuid_v7())
