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
