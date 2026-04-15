from __future__ import annotations

import argparse
import json
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from financial_router.types import BudgetState, JWTClaims, ModelInfo, TaskMetadata
from immune.types import JudgePayload, Outcome, SheriffPayload, generate_uuid_v7
from migrate import SCHEMAS, apply_schema, verify_database
from skills.bootstrap import BootstrapOrchestrator
from skills.config import IntegrationConfig
from skills.db_manager import CANONICAL_DATABASES, DatabaseManager
from skills.hermes_interfaces import HermesSessionContext, HermesToolRegistry, MockHermesRuntime

EXPECTED_CORE_TOOLS = (
    "immune_system",
    "financial_router",
    "strategic_memory",
    "council",
    "research_domain_2",
    "opportunity_pipeline",
    "operator_interface",
    "observability",
)


@dataclass(frozen=True)
class RuntimeBootstrapResult:
    """Structured result for a Hermes integration bootstrap attempt."""

    ok: bool
    config: IntegrationConfig
    session_context: HermesSessionContext
    database_status: dict[str, bool]
    registered_tools: list[str]


@dataclass(frozen=True)
class RuntimeProfileInstallResult:
    """Filesystem bundle describing how Hermes should bootstrap this runtime."""

    config: IntegrationConfig
    repo_root: str
    profile_manifest_path: str
    launcher_paths: dict[str, str]
    linked_skill_paths: list[str]


@dataclass(frozen=True)
class RuntimeDoctorResult:
    """Health report for the prepared Hermes runtime layout."""

    ok: bool
    config: IntegrationConfig
    path_status: dict[str, bool]
    database_status: dict[str, bool]
    registered_tools: list[str]
    missing_items: list[str]
    profile_manifest_path: str


@dataclass(frozen=True)
class WorkflowObservabilitySnapshot:
    """Queryable runtime evidence produced by the operator workflow proof."""

    alert_history: list[dict[str, Any]]
    council_verdicts: list[dict[str, Any]]
    digest_history: list[dict[str, Any]]
    immune_verdicts: list[dict[str, Any]]
    telemetry_events: list[dict[str, Any]]
    reliability_dashboard: dict[str, Any]
    system_health: dict[str, Any]


@dataclass(frozen=True)
class OperatorWorkflowResult:
    """End-to-end operator workflow and council-backed project proof result."""

    ok: bool
    bootstrap: RuntimeBootstrapResult
    sheriff_outcome: str
    routing_tier: str | None
    brief_id: str | None
    readback: dict[str, Any] | None
    opportunity_id: str | None
    harvest_id: str | None
    project_id: str | None
    phase_gate_id: str | None
    phase_gate_verdict: str | None
    council_verdict_ids: list[str]
    alert_id: str | None
    digest_id: str | None
    digest: dict[str, Any] | None
    observability: WorkflowObservabilitySnapshot | None
    doctor: RuntimeDoctorResult
    error: str | None = None


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _mock_council_synthesis(
    decision_type: str,
    *,
    recommendation: str,
    confidence: float,
    reasoning_summary: str,
    dissenting_views: str,
    risk_watch: list[str] | None = None,
) -> str:
    return json.dumps(
        {
            "tier_used": 1,
            "decision_type": decision_type,
            "recommendation": recommendation,
            "confidence": confidence,
            "reasoning_summary": reasoning_summary,
            "dissenting_views": dissenting_views,
            "da_assessment": [
                {
                    "objection": "Execution variance remains possible",
                    "tag": "acknowledged",
                    "reasoning": "Captured in dissent and risk watch.",
                }
            ],
            "tie_break": False,
            "risk_watch": risk_watch or [],
        }
    )


def _seed_mock_council_roles(tool_registry: HermesToolRegistry) -> None:
    if not isinstance(tool_registry, MockHermesRuntime):
        return
    tool_registry.set_mock_response(
        "delegate:strategist",
        json.dumps(
            {
                "role": "strategist",
                "case_for": "The opportunity has a credible route from validated signal to execution.",
                "market_fit_score": 0.77,
                "timing_assessment": "Current operator workflow evidence supports moving forward.",
                "strategic_alignment": "Fits the local-first commercial engine.",
                "key_assumption": "The initial operator proof generalises to the next phase.",
            }
        ),
    )
    tool_registry.set_mock_response(
        "delegate:critic",
        json.dumps(
            {
                "role": "critic",
                "case_against": "The current evidence is still narrow and could hide delivery risk.",
                "execution_risk": "Phase handoff could amplify coordination mistakes.",
                "market_risk": "Signal may reflect a narrow segment.",
                "fatal_dependency": "Reliable project gate execution.",
                "risk_severity": 0.58,
            }
        ),
    )
    tool_registry.set_mock_response(
        "delegate:realist",
        json.dumps(
            {
                "role": "realist",
                "execution_requirements": "Maintain deterministic gate handling and operator visibility.",
                "compute_needs": "Local execution with occasional council deliberation.",
                "time_to_revenue_days": 45,
                "capital_required_usd": 0,
                "blocking_prerequisite": "Qualified opportunity must pass phase gate.",
                "feasibility_score": 0.71,
            }
        ),
    )
    tool_registry.set_mock_response(
        "delegate:devils_advocate",
        json.dumps(
            {
                "role": "devils_advocate",
                "shared_assumption": "The current workflow proof is representative of future load.",
                "novel_risk": "Brief quality can degrade if corroboration lags.",
                "material_disagreement": "The gate may be too optimistic about downstream execution.",
                "alternative_interpretation": "Proceeding now may just defer a necessary pause by one phase.",
            }
        ),
    )


def _seed_mock_council_synthesis(
    tool_registry: HermesToolRegistry,
    *,
    decision_type: str,
    recommendation: str,
    confidence: float,
    reasoning_summary: str,
    dissenting_views: str,
    risk_watch: list[str] | None = None,
) -> None:
    if not isinstance(tool_registry, MockHermesRuntime):
        return
    tool_registry.set_mock_response(
        "delegate:synthesis",
        _mock_council_synthesis(
            decision_type,
            recommendation=recommendation,
            confidence=confidence,
            reasoning_summary=reasoning_summary,
            dissenting_views=dissenting_views,
            risk_watch=risk_watch,
        ),
    )


def _phase_gate_verdict_from_council(recommendation: str) -> str:
    if recommendation == "PURSUE":
        return "CONTINUE"
    if recommendation == "REJECT":
        return "KILL_RECOMMEND"
    return "PAUSE"


def _ensure_operator_workflow_chain_definition(config: IntegrationConfig, chain_type: str = "operator_workflow") -> None:
    db_manager = DatabaseManager(config.data_dir)
    try:
        conn = db_manager.get_connection("telemetry")
        now = _utc_now()
        steps = json.dumps(
            [
                {"step_type": "heartbeat", "skill": "operator_interface"},
                {"step_type": "sheriff", "skill": "immune_system"},
                {"step_type": "route", "skill": "financial_router"},
                {"step_type": "create_opportunity_task", "skill": "research_domain_2"},
                {"step_type": "write_brief", "skill": "strategic_memory"},
                {"step_type": "read_brief", "skill": "strategic_memory"},
                {"step_type": "route_opportunity_brief", "skill": "research_domain_2"},
                {"step_type": "create_harvest_task", "skill": "research_domain_2"},
                {"step_type": "route_harvest_brief", "skill": "research_domain_2"},
                {"step_type": "phase_gate_trigger", "skill": "opportunity_pipeline"},
                {"step_type": "council_phase_gate", "skill": "council"},
                {"step_type": "phase_gate_apply", "skill": "opportunity_pipeline"},
                {"step_type": "judge", "skill": "immune_system"},
                {"step_type": "alert", "skill": "operator_interface"},
                {"step_type": "digest", "skill": "operator_interface"},
            ]
        )
        conn.execute(
            """
            INSERT INTO chain_definitions (chain_type, steps, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chain_type) DO UPDATE SET
                steps=excluded.steps,
                updated_at=excluded.updated_at
            """,
            (chain_type, steps, now, now),
        )
        conn.commit()
    finally:
        db_manager.close_all()


def _persist_step_outcome(
    config: IntegrationConfig,
    *,
    chain_id: str,
    step_type: str,
    skill: str,
    outcome: str,
    latency_ms: float,
    quality_warning: bool = False,
    recovery_tier: int | None = None,
) -> None:
    db_manager = DatabaseManager(config.data_dir)
    try:
        conn = db_manager.get_connection("telemetry")
        conn.execute(
            """
            INSERT OR IGNORE INTO step_outcomes (
                event_id, step_type, skill, chain_id, outcome, latency_ms,
                quality_warning, recovery_tier, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                generate_uuid_v7(),
                step_type,
                skill,
                chain_id,
                outcome,
                int(latency_ms),
                1 if quality_warning else 0,
                recovery_tier,
                _utc_now(),
            ),
        )
        conn.commit()
    finally:
        db_manager.close_all()


def _persist_immune_verdict(config: IntegrationConfig, verdict: Any, latency_ms: float) -> None:
    db_manager = DatabaseManager(config.data_dir)
    try:
        conn = db_manager.get_connection("immune")
        conn.execute(
            """
            INSERT OR IGNORE INTO immune_verdicts (
                verdict_id, verdict_type, scan_tier, session_id, skill_name,
                result, match_pattern, latency_ms, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verdict.verdict_id,
                "sheriff_input" if verdict.check_type.value == "sheriff" else "judge_output",
                verdict.tier.value,
                verdict.session_id,
                verdict.skill_name,
                verdict.outcome.value,
                verdict.block_reason.value if verdict.block_reason else None,
                int(latency_ms),
                _utc_now(),
            ),
        )
        conn.commit()
    finally:
        db_manager.close_all()


def _record_tool_step(
    config: IntegrationConfig,
    *,
    chain_id: str,
    step_type: str,
    skill: str,
    result: Any,
    quality_warning: bool = False,
) -> None:
    _persist_step_outcome(
        config,
        chain_id=chain_id,
        step_type=step_type,
        skill=skill,
        outcome="PASS" if result.success else "FAIL",
        latency_ms=result.duration_ms,
        quality_warning=quality_warning,
    )


def _normalize_runtime_layout(config: IntegrationConfig) -> IntegrationConfig:
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
    )


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _runtime_bundle_dir(config: IntegrationConfig) -> Path:
    return Path(config.skills_dir) / "runtime"


def _runtime_profile_manifest_path(config: IntegrationConfig) -> Path:
    return _runtime_bundle_dir(config) / "profile_manifest.json"


def _runtime_launcher_paths(config: IntegrationConfig) -> dict[str, Path]:
    bin_dir = _runtime_bundle_dir(config) / "bin"
    return {
        "bootstrap": bin_dir / "bootstrap_runtime.sh",
        "doctor": bin_dir / "doctor_runtime.sh",
        "operator_workflow": bin_dir / "run_operator_workflow.sh",
    }


def _linked_skills_dir(config: IntegrationConfig) -> Path:
    return _runtime_bundle_dir(config) / "linked_skills"


def _command_args(config: IntegrationConfig) -> list[str]:
    return [
        "--data-dir",
        config.data_dir,
        "--skills-dir",
        config.skills_dir,
        "--checkpoints-dir",
        config.checkpoints_dir,
        "--alerts-dir",
        config.alerts_dir,
        "--profile-name",
        config.profile_name,
    ]


def _command_string(config: IntegrationConfig, action_flag: str, repo_root: Path) -> str:
    parts = [
        f"PYTHONPATH={shlex.quote(str(repo_root))}${{PYTHONPATH:+:${{PYTHONPATH}}}}",
        "python3",
        "-m",
        "skills.runtime",
        action_flag,
        *_command_args(config),
    ]
    return " ".join(shlex.quote(part) for part in parts)


def _write_launcher(path: Path, config: IntegrationConfig, repo_root: Path, action_flag: str) -> None:
    args = " ".join(shlex.quote(part) for part in [action_flag, *_command_args(config)])
    path.write_text(
        "\n".join(
            [
                "#!/bin/sh",
                "set -eu",
                f"REPO_ROOT={shlex.quote(str(repo_root))}",
                'if [ -n "${PYTHONPATH:-}" ]; then',
                '  export PYTHONPATH="$REPO_ROOT:$PYTHONPATH"',
                "else",
                '  export PYTHONPATH="$REPO_ROOT"',
                "fi",
                f"exec python3 -m skills.runtime {args} \"$@\"",
                "",
            ]
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)


def _symlink_skill_directory(source: Path, dest: Path) -> None:
    if dest.is_symlink():
        if dest.resolve() == source.resolve():
            return
        dest.unlink()
    elif dest.exists():
        raise FileExistsError(f"Refusing to replace non-symlink path: {dest}")
    dest.symlink_to(source, target_is_directory=True)


def prepare_runtime_directories(config: IntegrationConfig) -> IntegrationConfig:
    """Resolve and create the filesystem layout expected by the integration layer."""
    resolved = _normalize_runtime_layout(config).resolve_paths()
    for raw_path in (
        resolved.data_dir,
        resolved.skills_dir,
        resolved.checkpoints_dir,
        resolved.alerts_dir,
    ):
        Path(raw_path).mkdir(parents=True, exist_ok=True)
    return resolved


def install_runtime_profile(config: IntegrationConfig, *, repo_root: str | None = None) -> RuntimeProfileInstallResult:
    """Install a filesystem bundle that a Hermes profile can bootstrap from."""
    resolved = prepare_runtime_directories(config)
    root = Path(repo_root).expanduser().resolve() if repo_root else _repo_root()
    runtime_dir = _runtime_bundle_dir(resolved)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    linked_skills_dir = _linked_skills_dir(resolved)
    linked_skills_dir.mkdir(parents=True, exist_ok=True)
    launcher_paths = _runtime_launcher_paths(resolved)
    for launcher in launcher_paths.values():
        launcher.parent.mkdir(parents=True, exist_ok=True)

    linked_skill_paths: list[str] = []
    for manifest_path in sorted(root.glob("skills/*/manifest.yaml")):
        skill_dir = manifest_path.parent.resolve()
        link_path = linked_skills_dir / skill_dir.name
        _symlink_skill_directory(skill_dir, link_path)
        linked_skill_paths.append(str(link_path))

    manifest = {
        "profile_name": resolved.profile_name,
        "repo_root": str(root),
        "data_dir": resolved.data_dir,
        "skills_dir": resolved.skills_dir,
        "checkpoints_dir": resolved.checkpoints_dir,
        "alerts_dir": resolved.alerts_dir,
        "linked_skills": linked_skill_paths,
        "commands": {
            "bootstrap": _command_string(resolved, "--bootstrap-live", root),
            "doctor": _command_string(resolved, "--doctor", root),
            "operator_workflow": _command_string(resolved, "--operator-workflow", root),
        },
    }
    manifest_path = _runtime_profile_manifest_path(resolved)
    manifest_path.write_text(f"{json.dumps(manifest, indent=2, sort_keys=True)}\n", encoding="utf-8")

    _write_launcher(launcher_paths["bootstrap"], resolved, root, "--bootstrap-live")
    _write_launcher(launcher_paths["doctor"], resolved, root, "--doctor")
    _write_launcher(launcher_paths["operator_workflow"], resolved, root, "--operator-workflow")

    return RuntimeProfileInstallResult(
        config=resolved,
        repo_root=str(root),
        profile_manifest_path=str(manifest_path),
        launcher_paths={name: str(path) for name, path in launcher_paths.items()},
        linked_skill_paths=linked_skill_paths,
    )


def migrate_runtime_databases(config: IntegrationConfig) -> dict[str, bool]:
    """Apply all schema files into the configured data directory and verify them."""
    resolved = prepare_runtime_directories(config)
    root = Path(__file__).resolve().parents[1]
    data_dir = Path(resolved.data_dir)
    status: dict[str, bool] = {}
    for db_name, schema_rel in SCHEMAS.items():
        db_path = data_dir / f"{db_name}.db"
        schema_path = root / schema_rel
        apply_schema(db_path, schema_path)
        ok, _errors = verify_database(db_path, db_name, schema_path)
        status[db_name] = ok
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


def bootstrap_runtime(
    tool_registry: HermesToolRegistry,
    *,
    config: IntegrationConfig | None = None,
    session_context: HermesSessionContext | None = None,
    model_name: str = "local-default",
    jwt_claims: dict[str, Any] | None = None,
) -> RuntimeBootstrapResult:
    """Prepare runtime state, migrate databases, and register integration skills."""
    resolved = prepare_runtime_directories(config or IntegrationConfig())
    db_status = migrate_runtime_databases(resolved)
    ctx = session_context or make_session_context(resolved, model_name=model_name, jwt_claims=jwt_claims)
    orchestrator = BootstrapOrchestrator(resolved, tool_registry, ctx)
    ok = orchestrator.run()
    return RuntimeBootstrapResult(
        ok=ok,
        config=resolved,
        session_context=ctx,
        database_status=db_status,
        registered_tools=tool_registry.list_tools(),
    )


def doctor_runtime(
    tool_registry: HermesToolRegistry | None = None,
    *,
    config: IntegrationConfig | None = None,
    bootstrap_if_needed: bool = True,
) -> RuntimeDoctorResult:
    """Verify the prepared runtime layout, database health, and core skill registration."""
    resolved = _normalize_runtime_layout(config or IntegrationConfig()).resolve_paths()
    launcher_paths = _runtime_launcher_paths(resolved)
    path_status = {
        "data_dir": Path(resolved.data_dir).is_dir(),
        "skills_dir": Path(resolved.skills_dir).is_dir(),
        "checkpoints_dir": Path(resolved.checkpoints_dir).is_dir(),
        "alerts_dir": Path(resolved.alerts_dir).is_dir(),
        "profile_manifest": _runtime_profile_manifest_path(resolved).is_file(),
        "bootstrap_launcher": launcher_paths["bootstrap"].is_file(),
        "doctor_launcher": launcher_paths["doctor"].is_file(),
        "operator_workflow_launcher": launcher_paths["operator_workflow"].is_file(),
    }

    database_status = {db_name: False for db_name in CANONICAL_DATABASES}
    if path_status["data_dir"]:
        db_manager = DatabaseManager(resolved.data_dir)
        try:
            database_status.update(db_manager.verify_all_databases())
        finally:
            db_manager.close_all()

    registered_tools: list[str] = []
    if tool_registry is not None:
        if bootstrap_if_needed and not tool_registry.list_tools():
            bootstrap_runtime(tool_registry, config=resolved)
        registered_tools = tool_registry.list_tools()

    missing_items = [name for name, ok in path_status.items() if not ok]
    missing_items.extend(f"db:{db_name}" for db_name, ok in database_status.items() if not ok)
    if tool_registry is not None:
        missing_items.extend(f"tool:{tool_name}" for tool_name in EXPECTED_CORE_TOOLS if tool_name not in registered_tools)

    return RuntimeDoctorResult(
        ok=not missing_items,
        config=resolved,
        path_status=path_status,
        database_status=database_status,
        registered_tools=registered_tools,
        missing_items=missing_items,
        profile_manifest_path=str(_runtime_profile_manifest_path(resolved)),
    )


def run_operator_workflow(
    tool_registry: HermesToolRegistry,
    *,
    config: IntegrationConfig | None = None,
    session_context: HermesSessionContext | None = None,
    model_name: str = "local-default",
    task_id: str = "stage0-operator-workflow",
    title: str = "Operator workflow smoke test",
    summary: str = "Validated bootstrap, routing, council-backed opportunity review, and operator alert smoke test.",
) -> OperatorWorkflowResult:
    """Run one deterministic operator workflow plus council-backed project flow."""
    resolved_config = config or IntegrationConfig()
    install_runtime_profile(resolved_config)
    bootstrap = bootstrap_runtime(
        tool_registry,
        config=resolved_config,
        session_context=session_context,
        model_name=model_name,
    )
    resolved = bootstrap.config
    session_id = bootstrap.session_context.session_id
    chain_id = task_id
    _ensure_operator_workflow_chain_definition(resolved)
    _seed_mock_council_roles(tool_registry)

    sheriff_outcome = "error"
    routing_tier: str | None = None
    brief_id: str | None = None
    readback: dict[str, Any] | None = None
    opportunity_id: str | None = None
    harvest_id: str | None = None
    project_id: str | None = None
    phase_gate_id: str | None = None
    phase_gate_verdict: str | None = None
    council_verdict_ids: list[str] = []
    alert_id: str | None = None
    digest_payload: dict[str, Any] | None = None

    def _fail(error: str | None, *, observability: WorkflowObservabilitySnapshot | None = None) -> OperatorWorkflowResult:
        doctor = doctor_runtime(tool_registry, config=resolved, bootstrap_if_needed=False)
        return OperatorWorkflowResult(
            ok=False,
            bootstrap=bootstrap,
            sheriff_outcome=sheriff_outcome,
            routing_tier=routing_tier,
            brief_id=brief_id,
            readback=readback,
            opportunity_id=opportunity_id,
            harvest_id=harvest_id,
            project_id=project_id,
            phase_gate_id=phase_gate_id,
            phase_gate_verdict=phase_gate_verdict,
            council_verdict_ids=list(council_verdict_ids),
            alert_id=alert_id,
            digest_id=None if digest_payload is None else digest_payload.get("digest_id"),
            digest=digest_payload,
            observability=observability,
            doctor=doctor,
            error=error,
        )

    heartbeat = tool_registry.invoke_tool(
        "operator_interface",
        {
            "action": "record_heartbeat",
            "interaction_type": "command",
            "channel": "CLI",
        },
    )
    _record_tool_step(resolved, chain_id=chain_id, step_type="heartbeat", skill="operator_interface", result=heartbeat)
    if not heartbeat.success:
        return _fail(heartbeat.error)

    sheriff = tool_registry.invoke_tool(
        "immune_system",
        {
            "action": "sheriff",
            "payload": SheriffPayload(
                session_id=session_id,
                skill_name="operator_workflow",
                tool_name="strategic_memory",
                arguments={"action": "write_brief", "task_id": task_id, "title": title},
                raw_prompt=summary,
                source_trust_tier=4,
                jwt_claims=bootstrap.session_context.jwt_claims,
            ),
        },
    )
    _record_tool_step(resolved, chain_id=chain_id, step_type="sheriff", skill="immune_system", result=sheriff)
    if not sheriff.success:
        return _fail(sheriff.error)
    sheriff_verdict = sheriff.output
    sheriff_outcome = sheriff_verdict.outcome.value
    _persist_immune_verdict(resolved, sheriff_verdict, sheriff.duration_ms)
    if sheriff_verdict.outcome == Outcome.BLOCK:
        return _fail("workflow blocked by sheriff")

    route = tool_registry.invoke_tool(
        "financial_router",
        {
            "action": "route",
            "task": TaskMetadata(
                task_id=task_id,
                task_type="operator_workflow",
                required_capability="strategic_memory",
                quality_threshold=0.4,
            ),
            "models": [ModelInfo("m-local-primary", "local", True, 0.95, 0.0)],
            "budget": BudgetState(),
            "jwt": JWTClaims(session_id=session_id),
        },
    )
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="route",
        skill="financial_router",
        result=route,
        quality_warning=bool(route.success and getattr(route.output, "quality_warning", False)),
    )
    if not route.success:
        return _fail(route.error)
    routing_decision = route.output
    routing_tier = routing_decision.tier.value

    opportunity_task = tool_registry.invoke_tool(
        "research_domain_2",
        {
            "action": "create_task",
            "title": title,
            "brief": summary,
            "priority": "P1_HIGH",
            "tags": ["runtime", "council", "opportunity"],
        },
    )
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="create_opportunity_task",
        skill="research_domain_2",
        result=opportunity_task,
    )
    if not opportunity_task.success:
        return _fail(opportunity_task.error)
    opportunity_task_id = opportunity_task.output

    write_result = tool_registry.invoke_tool(
        "strategic_memory",
        {
            "action": "write_brief",
            "task_id": opportunity_task_id,
            "title": title,
            "summary": summary,
            "confidence": 0.82,
            "actionability": "ACTION_RECOMMENDED",
            "action_type": "opportunity_feed",
            "depth_tier": "FULL",
            "tags": ["runtime", "council", "project-gate"],
            "source_urls": ["https://example.com/runtime", "https://api.example.com/runtime"],
            "source_assessments": [
                {
                    "url": "https://example.com/runtime",
                    "relevance": 0.86,
                    "freshness": "2026-04-15",
                    "source_type": "tier2_web",
                },
                {
                    "url": "https://api.example.com/runtime",
                    "relevance": 0.91,
                    "freshness": "2026-04-15",
                    "source_type": "tier1_api",
                },
            ],
            "uncertainty_statement": (
                "We still need more real production repetitions to know whether the validated workflow "
                "holds under broader operator load."
            ),
            "counter_thesis": (
                "The strongest reason this could fail is that the current proof overfits the happy path "
                "and underestimates downstream delivery friction."
            ),
            "provenance_links": ["runtime-proof", "operator-workflow"],
        },
    )
    _record_tool_step(resolved, chain_id=chain_id, step_type="write_brief", skill="strategic_memory", result=write_result)
    if not write_result.success:
        return _fail(write_result.error)
    brief_id = write_result.output

    read_result = tool_registry.invoke_tool(
        "strategic_memory",
        {
            "action": "read_brief",
            "brief_id": brief_id,
        },
    )
    _record_tool_step(resolved, chain_id=chain_id, step_type="read_brief", skill="strategic_memory", result=read_result)
    if not read_result.success:
        return _fail(read_result.error)
    readback = read_result.output

    complete_opportunity_task = tool_registry.invoke_tool(
        "research_domain_2",
        {
            "action": "complete_task",
            "task_id": opportunity_task_id,
            "output_brief_id": brief_id,
            "actual_spend_usd": 0.0,
        },
    )
    if not complete_opportunity_task.success:
        return _fail(complete_opportunity_task.error)

    _seed_mock_council_synthesis(
        tool_registry,
        decision_type="opportunity_screen",
        recommendation="PURSUE",
        confidence=0.74,
        reasoning_summary="The brief is sufficiently corroborated to advance into validation.",
        dissenting_views="The evidence is promising but still drawn from a narrow slice of execution data.",
        risk_watch=["brief quality drift", "phase gate throughput"],
    )
    route_opportunity_brief = tool_registry.invoke_tool(
        "research_domain_2",
        {
            "action": "route_task_output",
            "task_id": opportunity_task_id,
            "include_council_review": True,
        },
    )
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="route_opportunity_brief",
        skill="research_domain_2",
        result=route_opportunity_brief,
    )
    if not route_opportunity_brief.success:
        return _fail(route_opportunity_brief.error)
    for action in route_opportunity_brief.output["actions"]:
        if action["type"] in {"opportunity_created", "opportunity_existing"}:
            opportunity_id = action["opportunity_id"]
        if action["type"] == "council_review_created":
            council_verdict_ids.append(action["verdict_id"])
    if opportunity_id is None:
        return _fail("opportunity routing did not produce an opportunity id")

    harvest_task = tool_registry.invoke_tool(
        "research_domain_2",
        {
            "action": "create_task",
            "title": f"{title} harvest",
            "brief": "Gather one subscription-only corroboration input for the runtime proof.",
            "priority": "P1_HIGH",
            "tags": ["runtime", "harvest"],
        },
    )
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="create_harvest_task",
        skill="research_domain_2",
        result=harvest_task,
    )
    if not harvest_task.success:
        return _fail(harvest_task.error)
    harvest_task_id = harvest_task.output

    harvest_brief = tool_registry.invoke_tool(
        "strategic_memory",
        {
            "action": "write_brief",
            "task_id": harvest_task_id,
            "title": f"{title} harvest",
            "summary": "The workflow needs one subscription-only corroboration step.",
            "confidence": 0.66,
            "actionability": "HARVEST_NEEDED",
            "action_type": "operator_surface",
        },
    )
    if not harvest_brief.success:
        return _fail(harvest_brief.error)

    complete_harvest_task = tool_registry.invoke_tool(
        "research_domain_2",
        {
            "action": "complete_task",
            "task_id": harvest_task_id,
            "output_brief_id": harvest_brief.output,
            "actual_spend_usd": 0.0,
        },
    )
    if not complete_harvest_task.success:
        return _fail(complete_harvest_task.error)

    route_harvest_brief = tool_registry.invoke_tool(
        "research_domain_2",
        {
            "action": "route_task_output",
            "task_id": harvest_task_id,
            "target_interface": "Claude Pro web",
            "harvest_prompt": "Review the workflow proof and extract the missing corroboration data point.",
        },
    )
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="route_harvest_brief",
        skill="research_domain_2",
        result=route_harvest_brief,
    )
    if not route_harvest_brief.success:
        return _fail(route_harvest_brief.error)
    for action in route_harvest_brief.output["actions"]:
        if action["type"] in {"harvest_request_created", "harvest_request_existing"}:
            harvest_id = action["harvest_id"]
            break
    if harvest_id is None:
        return _fail("harvest routing did not produce a harvest request id")

    transition_validation = tool_registry.invoke_tool(
        "opportunity_pipeline",
        {
            "action": "transition_opportunity",
            "opportunity_id": opportunity_id,
            "new_status": "IN_VALIDATION",
            "validation_report": f"Council-reviewed runtime opportunity from brief {brief_id}.",
            "validation_spend": 0.0,
            "council_verdict_id": council_verdict_ids[0] if council_verdict_ids else None,
        },
    )
    if not transition_validation.success:
        return _fail(transition_validation.error)
    transition_go_no_go = tool_registry.invoke_tool(
        "opportunity_pipeline",
        {
            "action": "transition_opportunity",
            "opportunity_id": opportunity_id,
            "new_status": "GO_NO_GO",
            "validation_report": "Validation complete. Ready for deterministic handoff into project flow.",
            "council_verdict_id": council_verdict_ids[0] if council_verdict_ids else None,
        },
    )
    if not transition_go_no_go.success:
        return _fail(transition_go_no_go.error)

    project_handoff = tool_registry.invoke_tool(
        "opportunity_pipeline",
        {
            "action": "handoff_to_project",
            "opportunity_id": opportunity_id,
            "project_name": f"{title} Project",
        },
    )
    if not project_handoff.success:
        return _fail(project_handoff.error)
    project_id = project_handoff.output["project_id"]

    phase_gate_trigger = tool_registry.invoke_tool(
        "opportunity_pipeline",
        {
            "action": "trigger_phase_gate",
            "project_id": project_id,
            "trigger": "BUDGET_EXHAUSTED",
            "outputs_summary": "Validated workflow, one qualified opportunity, and one pending harvest request created.",
            "cashflow_forecast_usd": 1000.0,
            "kill_score_current": 0.46,
            "kill_signals": [
                {"signal_type": "cashflow_vs_forecast", "raw_score": 0.5},
                {"signal_type": "operator_load", "raw_score": 0.4},
            ],
        },
    )
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="phase_gate_trigger",
        skill="opportunity_pipeline",
        result=phase_gate_trigger,
    )
    if not phase_gate_trigger.success:
        return _fail(phase_gate_trigger.error)
    phase_gate_id = phase_gate_trigger.output["gate_id"]

    _seed_mock_council_synthesis(
        tool_registry,
        decision_type="phase_gate",
        recommendation="PURSUE",
        confidence=0.67,
        reasoning_summary="The gate context supports continuing into the next phase with a focused scope amendment.",
        dissenting_views="The pending harvest request still leaves one evidence gap to monitor closely.",
        risk_watch=["pending harvest", "cashflow tracking"],
    )
    phase_gate_council = tool_registry.invoke_tool(
        "council",
        {
            "action": "deliberate",
            "decision_type": "phase_gate",
            "subject_id": project_id,
            "context": json.dumps(phase_gate_trigger.output["context_packet"], sort_keys=True),
            "source_briefs": [brief_id],
        },
    )
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="council_phase_gate",
        skill="council",
        result=phase_gate_council,
    )
    if not phase_gate_council.success:
        return _fail(phase_gate_council.error)
    phase_gate_council_verdict = phase_gate_council.output
    council_verdict_ids.append(phase_gate_council_verdict.verdict_id)
    phase_gate_verdict = _phase_gate_verdict_from_council(phase_gate_council_verdict.recommendation.value)

    apply_phase_gate = tool_registry.invoke_tool(
        "opportunity_pipeline",
        {
            "action": "apply_phase_gate_verdict",
            "project_id": project_id,
            "verdict": phase_gate_verdict,
            "confidence": phase_gate_council_verdict.confidence,
            "rationale": phase_gate_council_verdict.reasoning_summary,
            "dissent_log": [phase_gate_council_verdict.dissenting_views],
            "gate_id": phase_gate_id,
            "next_phase_amendments": {
                "scope_delta": "Prioritise the validated deterministic workflow slice before expanding.",
            },
        },
    )
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="phase_gate_apply",
        skill="opportunity_pipeline",
        result=apply_phase_gate,
    )
    if not apply_phase_gate.success:
        return _fail(apply_phase_gate.error)

    judge = tool_registry.invoke_tool(
        "immune_system",
        {
            "action": "judge",
            "payload": JudgePayload(
                session_id=session_id,
                skill_name="operator_workflow",
                tool_name="opportunity_pipeline",
                output=apply_phase_gate.output,
            ),
        },
    )
    _record_tool_step(resolved, chain_id=chain_id, step_type="judge", skill="immune_system", result=judge)
    if not judge.success:
        return _fail(judge.error)
    judge_verdict = judge.output
    _persist_immune_verdict(resolved, judge_verdict, judge.duration_ms)
    if judge_verdict.outcome == Outcome.BLOCK:
        return _fail("workflow blocked by judge")

    alert_result = tool_registry.invoke_tool(
        "operator_interface",
        {
            "action": "alert",
            "tier": "T1",
            "alert_type": "WORKFLOW_SMOKE_TEST",
            "content": (
                f"Workflow {task_id} advanced opportunity {opportunity_id} into project {project_id}, "
                f"recorded harvest {harvest_id}, and applied {phase_gate_verdict}."
            ),
        },
    )
    _record_tool_step(resolved, chain_id=chain_id, step_type="alert", skill="operator_interface", result=alert_result)
    if not alert_result.success:
        return _fail(alert_result.error)
    alert_id = alert_result.output

    digest_result = tool_registry.invoke_tool(
        "operator_interface",
        {
            "action": "generate_digest",
            "digest_type": "daily",
            "operator_state": "ACTIVE",
        },
    )
    _record_tool_step(resolved, chain_id=chain_id, step_type="digest", skill="operator_interface", result=digest_result)
    if not digest_result.success:
        return _fail(digest_result.error)
    digest_payload = digest_result.output

    observability_alerts = tool_registry.invoke_tool("observability", {"action": "query_alert_history", "limit": 5})
    observability_council = tool_registry.invoke_tool(
        "observability",
        {"action": "query_council_verdicts", "limit": 10, "project_id": project_id},
    )
    observability_digests = tool_registry.invoke_tool("observability", {"action": "recent_digests", "limit": 3})
    observability_immune = tool_registry.invoke_tool("observability", {"action": "query_immune_verdicts", "limit": 5})
    observability_telemetry = tool_registry.invoke_tool(
        "observability",
        {"action": "query_telemetry", "chain_id": chain_id, "limit": 30},
    )
    observability_reliability = tool_registry.invoke_tool(
        "observability",
        {"action": "reliability_dashboard", "limit": 10},
    )
    observability_health = tool_registry.invoke_tool("observability", {"action": "system_health"})
    observability_results = [
        observability_alerts,
        observability_council,
        observability_digests,
        observability_immune,
        observability_telemetry,
        observability_reliability,
        observability_health,
    ]
    first_failure = next((item for item in observability_results if not item.success), None)
    if first_failure is not None:
        return _fail(first_failure.error)

    observability = WorkflowObservabilitySnapshot(
        alert_history=observability_alerts.output,
        council_verdicts=observability_council.output,
        digest_history=observability_digests.output,
        immune_verdicts=observability_immune.output,
        telemetry_events=observability_telemetry.output,
        reliability_dashboard=observability_reliability.output,
        system_health=observability_health.output,
    )
    doctor = doctor_runtime(tool_registry, config=resolved, bootstrap_if_needed=False)
    return OperatorWorkflowResult(
        ok=doctor.ok,
        bootstrap=bootstrap,
        sheriff_outcome=sheriff_outcome,
        routing_tier=routing_tier,
        brief_id=brief_id,
        readback=readback,
        opportunity_id=opportunity_id,
        harvest_id=harvest_id,
        project_id=project_id,
        phase_gate_id=phase_gate_id,
        phase_gate_verdict=phase_gate_verdict,
        council_verdict_ids=list(council_verdict_ids),
        alert_id=alert_id,
        digest_id=digest_payload["digest_id"],
        digest=digest_payload,
        observability=observability,
        doctor=doctor,
        error=None if doctor.ok else "doctor reported missing runtime components",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and smoke-test the Hermes integration bootstrap")
    parser.add_argument("--bootstrap-live", action="store_true", help="Bootstrap the runtime against the selected registry")
    parser.add_argument("--install-profile", action="store_true", help="Install a local Hermes runtime profile bundle")
    parser.add_argument("--doctor", action="store_true", help="Verify runtime layout, databases, and skill registration")
    parser.add_argument("--operator-workflow", action="store_true", help="Run the operator workflow plus council-backed project smoke test")
    parser.add_argument("--data-dir", default="~/.hermes/data/")
    parser.add_argument("--skills-dir", default="~/.hermes/skills/hybrid-autonomous-ai/")
    parser.add_argument("--checkpoints-dir", default="~/.hermes/skills/hybrid-autonomous-ai/checkpoints/")
    parser.add_argument("--alerts-dir", default="~/.hermes/alerts/")
    parser.add_argument("--profile-name", default="hybrid-autonomous-ai")
    parser.add_argument("--model-name", default="local-default")
    parser.add_argument("--repo-root", default=None, help="Override the repository root used for profile installation")
    parser.add_argument("--task-id", default="stage0-operator-workflow")
    parser.add_argument("--title", default="Operator workflow smoke test")
    parser.add_argument(
        "--summary",
        default="Validated bootstrap, routing, council-backed opportunity review, and operator alert smoke test.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    config = IntegrationConfig(
        data_dir=args.data_dir,
        skills_dir=args.skills_dir,
        checkpoints_dir=args.checkpoints_dir,
        alerts_dir=args.alerts_dir,
        profile_name=args.profile_name,
    )
    runtime = MockHermesRuntime(data_dir=str(Path(args.data_dir).expanduser()))

    if args.install_profile:
        result = install_runtime_profile(config, repo_root=args.repo_root)
        print(f"profile manifest={result.profile_manifest_path}")
        print(f"launchers={','.join(sorted(result.launcher_paths.values()))}")
        print(f"linked_skills={len(result.linked_skill_paths)}")
        return 0

    if args.doctor:
        result = doctor_runtime(runtime, config=config)
        print("doctor ok" if result.ok else "doctor failed")
        print(f"missing={','.join(result.missing_items) if result.missing_items else 'none'}")
        print(f"tools={','.join(result.registered_tools)}")
        return 0 if result.ok else 1

    if args.operator_workflow:
        result = run_operator_workflow(
            runtime,
            config=config,
            model_name=args.model_name,
            task_id=args.task_id,
            title=args.title,
            summary=args.summary,
        )
        print("workflow ok" if result.ok else "workflow failed")
        print(f"sheriff={result.sheriff_outcome}")
        print(f"route={result.routing_tier or 'none'}")
        print(f"brief_id={result.brief_id or 'none'}")
        print(f"alert_id={result.alert_id or 'none'}")
        if result.error:
            print(f"error={result.error}")
        return 0 if result.ok else 1

    result = bootstrap_runtime(runtime, config=config, model_name=args.model_name)
    print("bootstrap ok" if result.ok else "bootstrap failed")
    print(f"session_id={result.session_context.session_id}")
    print(f"tools={','.join(result.registered_tools)}")
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
