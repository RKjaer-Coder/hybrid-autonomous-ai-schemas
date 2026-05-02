from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from financial_router.router import route_task
from financial_router.types import BudgetState, JWTClaims, ModelInfo, RoutingDecision, TaskMetadata
from immune.sheriff import sheriff_check
from immune.types import ImmuneConfig, Outcome, SheriffPayload, generate_uuid_v7
from migrate import LEGACY_SCHEMAS, apply_schema, verify_database
from skills.config import IntegrationConfig
from skills.bootstrap import BootstrapOrchestrator
from skills.hermes_interfaces import HermesSessionContext, HermesToolRegistry
from skills.local_forward_proxy import ProxyServerConfig, _default_port, _host_allowed

from .records import CapabilityGrant, Command, SideEffectIntent, new_id, payload_hash
from .store import KERNEL_POLICY_VERSION, KernelStore, KernelTransaction


@dataclass(frozen=True)
class ProviderCallRequest:
    """A v3.1 runtime request to prepare, not execute, an external provider call."""

    task: TaskMetadata
    available_models: list[ModelInfo]
    budget: BudgetState
    jwt: JWTClaims
    provider_endpoint: str
    provider_payload: dict[str, Any]
    proxy_config: dict[str, Any]
    budget_id: str | None = None
    method: str = "POST"
    session_id: str = field(default_factory=generate_uuid_v7)
    source_trust_tier: int = 1
    grant_expires_at: str = "2999-01-01T00:00:00Z"
    timeout_policy: str = "deny"


@dataclass(frozen=True)
class PreparedProviderCall:
    task_id: str
    model_id: str
    routing_tier: str
    estimated_cost_usd: Decimal
    budget_reservation_id: str | None
    network_grant_id: str
    model_grant_id: str
    side_effect_grant_id: str
    side_effect_intent_id: str
    proxy_audit_log_path: str
    provider_endpoint: str
    side_effect_payload_hash: str


@dataclass(frozen=True)
class RuntimeBootstrapState:
    ok: bool
    config: IntegrationConfig
    session_context: HermesSessionContext
    database_status: dict[str, bool]
    registered_tools: list[str]


def normalize_runtime_layout(config: IntegrationConfig) -> IntegrationConfig:
    defaults = IntegrationConfig()
    if config.data_dir == defaults.data_dir:
        return config

    base_dir = Path(config.data_dir).expanduser().resolve().parent
    skills_dir = config.skills_dir
    checkpoints_dir = config.checkpoints_dir
    alerts_dir = config.alerts_dir

    if skills_dir == defaults.skills_dir:
        skills_dir = str(base_dir / "skills" / "hybrid-autonomous-ai")
    if checkpoints_dir == defaults.checkpoints_dir:
        checkpoints_dir = str(Path(skills_dir) / "checkpoints")
    if alerts_dir == defaults.alerts_dir:
        alerts_dir = str(base_dir / "alerts")

    return IntegrationConfig(
        data_dir=config.data_dir,
        skills_dir=skills_dir,
        checkpoints_dir=checkpoints_dir,
        alerts_dir=alerts_dir,
        max_api_spend_usd=config.max_api_spend_usd,
        construction_phase=config.construction_phase,
        profile_name=config.profile_name,
        proxy_bind_url=config.proxy_bind_url,
        outbound_allowlist_domains=tuple(config.outbound_allowlist_domains),
        outbound_allowlist_ports=tuple(config.outbound_allowlist_ports),
        hermes_gateway_url=config.hermes_gateway_url,
        hermes_workspace_url=config.hermes_workspace_url,
    )


def runtime_logs_dir(config: IntegrationConfig) -> Path:
    return Path(config.data_dir).expanduser().resolve().parent / "logs"


def prepare_runtime_directories(config: IntegrationConfig) -> IntegrationConfig:
    """Resolve and create the filesystem layout expected by the runtime."""
    resolved = normalize_runtime_layout(config).resolve_paths()
    path_hints = (
        (resolved.data_dir, "--data-dir"),
        (resolved.skills_dir, "--skills-dir"),
        (resolved.checkpoints_dir, "--checkpoints-dir"),
        (resolved.alerts_dir, "--alerts-dir"),
        (str(runtime_logs_dir(resolved)), "--data-dir"),
    )
    for raw_path, flag in path_hints:
        try:
            Path(raw_path).mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise RuntimeError(
                f"cannot create runtime directory '{raw_path}' ({exc.strerror or exc}); "
                f"choose a writable path with {flag}"
            ) from exc
    return resolved


def migrate_runtime_databases(config: IntegrationConfig) -> dict[str, bool]:
    """Apply legacy projection schemas under the v3.1 runtime bootstrap lane."""
    resolved = prepare_runtime_directories(config)
    root = Path(__file__).resolve().parents[1]
    data_dir = Path(resolved.data_dir)
    status: dict[str, bool] = {}
    for db_name, schema_rel in LEGACY_SCHEMAS.items():
        db_path = data_dir / f"{db_name}.db"
        schema_path = root / schema_rel
        apply_schema(db_path, schema_path)
        ok, _errors = verify_database(db_path, db_name, schema_path)
        status[db_name] = ok
    return status


def verify_runtime_databases(config: IntegrationConfig) -> tuple[dict[str, bool], dict[str, list[str]]]:
    """Verify deployed legacy projection databases against current schema contracts."""
    resolved = normalize_runtime_layout(config).resolve_paths()
    root = Path(__file__).resolve().parents[1]
    data_dir = Path(resolved.data_dir)
    status: dict[str, bool] = {}
    errors: dict[str, list[str]] = {}
    for db_name, schema_rel in LEGACY_SCHEMAS.items():
        db_path = data_dir / f"{db_name}.db"
        schema_path = root / schema_rel
        if not db_path.is_file():
            status[db_name] = False
            errors[db_name] = [f"missing database: {db_path}"]
            continue
        ok, db_errors = verify_database(db_path, db_name, schema_path)
        status[db_name] = ok
        if db_errors:
            errors[db_name] = db_errors
    return status, errors


def require_runtime_databases(config: IntegrationConfig) -> dict[str, bool]:
    """Apply schemas and fail closed unless every projection DB verifies cleanly."""
    resolved = normalize_runtime_layout(config).resolve_paths()
    status = migrate_runtime_databases(resolved)
    _, errors = verify_runtime_databases(resolved)
    if not all(status.values()) or errors:
        details = []
        for db_name in sorted(LEGACY_SCHEMAS):
            db_errors = errors.get(db_name, [])
            if not db_errors and not status.get(db_name, False):
                db_errors = ["verification returned false"]
            details.extend(f"{db_name}: {error}" for error in db_errors)
        raise RuntimeError("runtime database schema verification failed: " + "; ".join(details))
    return status


def make_session_context(
    config: IntegrationConfig,
    *,
    model_name: str = "local-default",
    session_id: str | None = None,
    jwt_claims: dict[str, Any] | None = None,
) -> HermesSessionContext:
    resolved = config.resolve_paths()
    return HermesSessionContext(
        session_id=session_id or generate_uuid_v7(),
        profile_name=resolved.profile_name,
        model_name=model_name,
        jwt_claims=jwt_claims or {},
        data_dir=resolved.data_dir,
    )


def bootstrap_runtime_state(
    tool_registry: HermesToolRegistry,
    *,
    config: IntegrationConfig | None = None,
    session_context: HermesSessionContext | None = None,
    model_name: str = "local-default",
    jwt_claims: dict[str, Any] | None = None,
) -> RuntimeBootstrapState:
    """Prepare runtime state, migrate projection DBs, and register integration skills."""
    resolved = prepare_runtime_directories(config or IntegrationConfig())
    db_status = require_runtime_databases(resolved)
    ctx = session_context or make_session_context(resolved, model_name=model_name, jwt_claims=jwt_claims)
    orchestrator = BootstrapOrchestrator(resolved, tool_registry, ctx)
    ok = orchestrator.run()
    return RuntimeBootstrapState(
        ok=ok,
        config=resolved,
        session_context=ctx,
        database_status=db_status,
        registered_tools=tool_registry.list_tools(),
    )


class KernelRuntime:
    """First v3.1 runtime replacement surface.

    This class is intentionally narrow. It proves the new direction by making
    one real lane authoritative under the kernel: prepare a provider call after
    immune validation, router selection, budget reservation, grants, and durable
    side-effect intent recording.
    """

    def __init__(self, store: KernelStore, immune_config: ImmuneConfig | None = None) -> None:
        self.store = store
        self.immune_config = immune_config or ImmuneConfig()

    def prepare_provider_call(self, command: Command, request: ProviderCallRequest) -> PreparedProviderCall:
        def handler(tx: KernelTransaction) -> PreparedProviderCall:
            return self._prepare_provider_call(tx, request)

        return self.store.execute_command(command, handler)

    def _prepare_provider_call(self, tx: KernelTransaction, request: ProviderCallRequest) -> PreparedProviderCall:
        proxy_config = ProxyServerConfig.from_payload(request.proxy_config)
        endpoint = _validated_proxy_endpoint(request.provider_endpoint, proxy_config)
        immune_config = _immune_config_for_endpoint(self.immune_config, endpoint["host"])
        verdict = sheriff_check(
            SheriffPayload(
                session_id=request.session_id,
                skill_name="kernel_runtime",
                tool_name="api_call",
                arguments={
                    "endpoint": request.provider_endpoint,
                    "method": request.method,
                    "task_id": request.task.task_id,
                },
                source_trust_tier=request.source_trust_tier,
                jwt_claims={
                    "max_api_spend_usd": request.jwt.max_api_spend_usd,
                    "current_spend_usd": request.jwt.current_session_spend_usd,
                },
            ),
            immune_config,
        )
        if verdict.outcome != Outcome.PASS:
            detail = verdict.block_detail or (verdict.block_reason.value if verdict.block_reason else "blocked")
            raise PermissionError(f"immune validation blocked provider call: {detail}")

        decision = route_task(
            task=request.task,
            available_models=request.available_models,
            budget=request.budget,
            jwt=request.jwt,
            request_id=tx.command.idempotency_key,
        )
        if decision.compute_starved or decision.model_id is None:
            raise RuntimeError(f"no eligible model route: {decision.justification}")
        if decision.requires_operator_approval:
            raise PermissionError("provider call requires an operator gate before preparation")

        reservation_id = self._reserve_paid_budget(tx, request, decision)
        network_grant = self._issue_network_grant(tx, request, proxy_config, endpoint)
        model_grant = self._issue_model_grant(tx, request, decision)
        side_effect_grant = self._issue_side_effect_grant(tx, request, decision)
        if not tx.use_grant(
            network_grant.grant_id,
            "adapter",
            "local_forward_proxy",
            "network",
            "connect",
        ):
            raise PermissionError("kernel network grant denied")
        if not tx.use_grant(
            model_grant.grant_id,
            "model",
            decision.model_id,
            "model",
            "invoke",
        ):
            raise PermissionError("kernel model grant denied")

        payload_digest = payload_hash(request.provider_payload)
        intent = SideEffectIntent(
            task_id=request.task.task_id,
            side_effect_type="provider_call",
            target={
                "endpoint": request.provider_endpoint,
                "method": request.method,
                "model_id": decision.model_id,
                "routing_tier": decision.tier.value,
                "proxy": proxy_config.to_payload(),
                "budget_reservation_id": reservation_id,
            },
            payload_hash=payload_digest,
            required_authority="rule",
            grant_id=side_effect_grant.grant_id,
            timeout_policy=request.timeout_policy,  # type: ignore[arg-type]
        )
        intent_id = tx.prepare_side_effect(intent)
        return PreparedProviderCall(
            task_id=request.task.task_id,
            model_id=decision.model_id,
            routing_tier=decision.tier.value,
            estimated_cost_usd=Decimal(str(decision.estimated_cost_usd)),
            budget_reservation_id=reservation_id,
            network_grant_id=network_grant.grant_id,
            model_grant_id=model_grant.grant_id,
            side_effect_grant_id=side_effect_grant.grant_id,
            side_effect_intent_id=intent_id,
            proxy_audit_log_path=proxy_config.audit_log_path,
            provider_endpoint=request.provider_endpoint,
            side_effect_payload_hash=payload_digest,
        )

    @staticmethod
    def _reserve_paid_budget(
        tx: KernelTransaction,
        request: ProviderCallRequest,
        decision: RoutingDecision,
    ) -> str | None:
        if decision.estimated_cost_usd <= 0:
            return None
        if request.budget_id is None:
            raise PermissionError("paid provider call requires a kernel budget id")
        return tx.reserve_budget(
            request.budget_id,
            Decimal(str(decision.estimated_cost_usd)),
            reservation_id=tx.command.idempotency_key,
        )

    @staticmethod
    def _issue_network_grant(
        tx: KernelTransaction,
        request: ProviderCallRequest,
        proxy_config: ProxyServerConfig,
        endpoint: dict[str, Any],
    ) -> CapabilityGrant:
        grant = CapabilityGrant(
            task_id=request.task.task_id,
            subject_type="adapter",
            subject_id="local_forward_proxy",
            capability_type="network",
            actions=["connect"],
            resource={
                "scheme": endpoint["scheme"],
                "host": endpoint["host"],
                "port": endpoint["port"],
            },
            scope={"proxy": proxy_config.to_payload()},
            conditions={"prepared_by": "kernel_runtime.prepare_provider_call"},
            expires_at=request.grant_expires_at,
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        tx.issue_capability_grant(grant)
        return grant

    @staticmethod
    def _issue_model_grant(
        tx: KernelTransaction,
        request: ProviderCallRequest,
        decision: RoutingDecision,
    ) -> CapabilityGrant:
        grant = CapabilityGrant(
            task_id=request.task.task_id,
            subject_type="model",
            subject_id=decision.model_id or "",
            capability_type="model",
            actions=["invoke"],
            resource={"model_id": decision.model_id, "routing_tier": decision.tier.value},
            scope={"task_type": request.task.task_type, "required_capability": request.task.required_capability},
            conditions={"routing_justification": decision.justification},
            expires_at=request.grant_expires_at,
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        tx.issue_capability_grant(grant)
        return grant

    @staticmethod
    def _issue_side_effect_grant(
        tx: KernelTransaction,
        request: ProviderCallRequest,
        decision: RoutingDecision,
    ) -> CapabilityGrant:
        grant = CapabilityGrant(
            task_id=request.task.task_id,
            subject_type="adapter",
            subject_id="side_effect_broker",
            capability_type="side_effect",
            actions=["prepare"],
            resource={"side_effect_type": "provider_call", "model_id": decision.model_id},
            scope={"idempotency_key": tx.command.idempotency_key},
            conditions={"external_action": "prepare_only"},
            expires_at=request.grant_expires_at,
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        tx.issue_capability_grant(grant)
        return grant


def _validated_proxy_endpoint(provider_endpoint: str, proxy_config: ProxyServerConfig) -> dict[str, Any]:
    split = urlsplit(provider_endpoint)
    if not split.scheme or not split.hostname:
        raise PermissionError("provider endpoint must be an absolute URL")
    scheme = split.scheme.lower()
    host = split.hostname.lower()
    port = split.port or _default_port(scheme)
    if scheme not in proxy_config.allowed_schemes:
        raise PermissionError(f"provider endpoint scheme not allowed: {scheme}")
    if port not in proxy_config.allowed_ports:
        raise PermissionError(f"provider endpoint port not allowed: {port}")
    if not _host_allowed(host, proxy_config.allowed_domains):
        raise PermissionError(f"provider endpoint host not allowed: {host}")
    return {"scheme": scheme, "host": host, "port": port}


def _immune_config_for_endpoint(base: ImmuneConfig, host: str) -> ImmuneConfig:
    permitted = set(base.permitted_endpoints)
    permitted.add(host)
    return dataclasses.replace(base, permitted_endpoints=frozenset(permitted))
