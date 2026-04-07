"""Shared deterministic utilities and realistic data pools for eval fixtures."""

from __future__ import annotations

import datetime as dt
import random
import uuid
from typing import Any

INCOME_MECHANISMS = ["software_product", "client_work", "market_opportunity", "ip_asset"]
RESEARCH_DOMAINS = {
    1: "Security",
    2: "Model Ecosystem",
    3: "Business & Market",
    4: "Regulatory & Compliance",
    5: "Intelligence Opportunity Scanning",
}

REALISTIC_THESES = [
    f"Thesis {i}: Build a focused automation asset with constrained scope, explicit buyer profile, and measurable first-month ROI. Validate by interviewing operators, shipping a narrow MVP, and collecting paid pilot feedback."
    for i in range(1, 36)
]
REALISTIC_BRIEFS = [
    f"Brief {i}: Recent signals indicate changing procurement behavior, with improved traction for lower-cost, compliance-aware tooling and clear migration pathways."
    for i in range(1, 26)
]
REALISTIC_TASK_BRIEFS = [
    f"Task brief {i}: Produce a practical deliverable with constraints, acceptance criteria, and rollback guidance while preserving auditability and reproducibility."
    for i in range(1, 31)
]
RESEARCH_SEED_QUERIES = {
    d: [f"Domain {d} query {i}: summarize notable developments and implications." for i in range(1, 6)]
    for d in RESEARCH_DOMAINS
}


class DeterministicFactory:
    """Factory for deterministic UUIDv7-like IDs and ISO8601 timestamps."""

    def __init__(self, seed: int = 42) -> None:
        self.seed = seed
        self.rng = random.Random(seed)
        self.counter = 0
        self.base_ms = int(dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1000) + seed * 100

    def uuid_v7(self) -> str:
        """Generate deterministic RFC 9562 UUIDv7 textual ID."""
        ts_ms = (self.base_ms + self.counter) & ((1 << 48) - 1)
        self.counter += 1
        rand_a = self.rng.getrandbits(12)
        rand_b = self.rng.getrandbits(62)
        value = 0
        value |= ts_ms << 80
        value |= 0x7 << 76
        value |= rand_a << 64
        value |= 0b10 << 62
        value |= rand_b
        return str(uuid.UUID(int=value))

    def now(self) -> str:
        return dt.datetime.fromtimestamp(self.base_ms / 1000, tz=dt.timezone.utc).replace(microsecond=0).isoformat()

    def offset(self, days: int = 0, hours: int = 0, minutes: int = 0) -> str:
        base = dt.datetime.fromtimestamp(self.base_ms / 1000, tz=dt.timezone.utc)
        shifted = base + dt.timedelta(days=days, hours=hours, minutes=minutes)
        return shifted.replace(microsecond=0).isoformat()

    def random_past(self, max_days_ago: int = 90) -> str:
        mins = self.rng.randint(1, max_days_ago * 24 * 60)
        base = dt.datetime.fromtimestamp(self.base_ms / 1000, tz=dt.timezone.utc)
        return (base - dt.timedelta(minutes=mins)).replace(microsecond=0).isoformat()


def generate_uuid_v7(seed: int = 42) -> str:
    return DeterministicFactory(seed).uuid_v7()


def now_iso8601(seed: int = 42) -> str:
    return DeterministicFactory(seed).now()


def offset_iso8601(days: int = 0, hours: int = 0, minutes: int = 0, seed: int = 42) -> str:
    return DeterministicFactory(seed).offset(days=days, hours=hours, minutes=minutes)


def random_past_timestamp(max_days_ago: int = 90, seed: int = 42) -> str:
    return DeterministicFactory(seed).random_past(max_days_ago=max_days_ago)


def weighted_sum(signals: list[dict[str, Any]]) -> float:
    return round(sum(float(s["weight"]) * float(s["raw_score"]) for s in signals), 4)
