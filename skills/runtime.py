from __future__ import annotations

import argparse
import contextlib
import datetime
import json
import math
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import time
import threading
import types
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import urlsplit

from financial_router.types import BudgetState, JWTClaims, ModelInfo, SystemPhase, TaskMetadata
from harness_variants import ExecutionTrace, ExecutionTraceStep, HarnessVariantManager
from hermes_profile_contract import HermesProfileContract
from immune.bootstrap_patch import apply_immune_patch
from immune.config import load_config
from immune.judge_lifecycle import JudgeLifecycleManager
from immune.types import (
    AlertSeverity,
    BlockReason,
    CheckType,
    ImmuneBlockError,
    ImmuneVerdict,
    JudgeMode,
    JudgePayload,
    Outcome,
    SheriffPayload,
    Tier,
    generate_uuid_v7,
)
from immune.verdict_logger import VerdictLogger
from migrate import SCHEMAS, apply_schema, verify_database
from runtime_control import RuntimeControlManager
from skills.bootstrap import BootstrapOrchestrator
from skills.config import IntegrationConfig
from skills.db_manager import CANONICAL_DATABASES, DatabaseManager
from skills.hermes_interfaces import HermesSessionContext, HermesToolRegistry, MockHermesRuntime
from skills.local_forward_proxy import ProxyServerConfig, start_proxy_server
from skills.milestone_status import evaluate_milestone_status, runtime_support_artifact_paths

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
EXPECTED_SEED_TOOLS = (
    "code_execution",
    "file_operations",
    "web_search",
    "web_fetch",
    "shell_command",
)
LEGACY_SPLIT_DATABASES = ("opportunity.db", "project.db", "treasury.db")
MANIFEST_HERMES_VERSION_FLOOR = (0, 10, 0)
CHECKLIST_HERMES_VERSION_FLOOR = (0, 10, 0)
VERSION_DRIFT_NOTE = (
    "spec drift: repo runtime now targets Hermes v0.10.0+ and treats "
    "config.yaml as the primary upstream surface. Any remaining v0.8/v0.9 "
    "language should be considered stale."
)
PROFILE_DRIFT_NOTE = (
    "spec/doc drift: upstream Hermes docs center config.yaml inside the profile "
    "directory. The repo still generates a spec-compat profile.yaml projection, "
    "but config.yaml is the authoritative runtime surface."
)
CONFIG_SURFACE_UNCERTAINTY_NOTE = (
    "upstream uncertainty: current Hermes public docs clearly describe "
    "config.yaml and approvals.mode, but they do not clearly document a "
    "first-class dangerous_commands config schema. The repo still projects and "
    "validates the §7.5c dangerous-command set as a repo-owned contract until "
    "live Hermes proves the exact upstream key shape."
)
DEFAULT_EVIDENCE_CYCLES = 5
DEFAULT_REPLAY_REPORT_LIMIT = 10


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
    profile_dir: str
    profile_config_path: str
    spec_profile_path: str
    profile_manifest_path: str
    launcher_paths: dict[str, str]
    linked_skill_paths: list[str]


@dataclass(frozen=True)
class HermesProfileValidationResult:
    """Structured validation result for the repo-owned Hermes profile artifacts."""

    ok: bool
    profile_dir: str
    profile_config_path: str
    spec_profile_path: str
    checks: dict[str, bool]
    issues: list[str]


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
    profile_validation: HermesProfileValidationResult


@dataclass(frozen=True)
class ExternalCommandResult:
    """Captured result from a Hermes CLI probe."""

    ok: bool
    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    error: str | None = None


@dataclass(frozen=True)
class HermesReadinessResult:
    """Readiness report for attaching the repo runtime to a real Hermes install."""

    ok: bool
    config: IntegrationConfig
    hermes_installed: bool
    hermes_version: str | None
    hermes_version_ok: bool
    profile_listed: bool
    live_tools: list[str]
    seed_tool_status: dict[str, bool]
    config_status: dict[str, bool]
    profile_validation: HermesProfileValidationResult
    path_status: dict[str, bool]
    database_status: dict[str, bool]
    legacy_database_files: list[str]
    cli_smoke_attempted: bool
    cli_smoke_ok: bool
    cli_smoke_marker: str | None
    cli_smoke_step_outcomes_delta: int
    cli_smoke_log_trace: bool
    cli_smoke_output: str | None
    checkpoint_backup_path: str | None
    blocking_items: list[str]
    drift_items: list[str]
    install: RuntimeProfileInstallResult
    doctor: RuntimeDoctorResult
    contract_harness: HermesContractHarnessResult


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
    trace_id: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class HermesContractHarnessResult:
    """Repo-local Hermes-parity lifecycle proof without requiring a live Hermes install."""

    ok: bool
    config: IntegrationConfig
    bootstrap: RuntimeBootstrapResult
    doctor: RuntimeDoctorResult
    contract_checks: dict[str, bool]
    route_decision: dict[str, Any] | None
    approval_request: dict[str, Any] | None
    approval_review: dict[str, Any] | None
    dispatch_result: dict[str, Any] | None
    judge_deadlock_event: dict[str, Any] | None
    runtime_halt: dict[str, Any] | None
    blocked_dispatch_pre_side_effect: bool
    blocked_dispatch_reason: str | None
    restart_result: dict[str, Any] | None
    final_runtime_status: dict[str, Any] | None
    trace_id: str | None
    issues: list[str]


@dataclass(frozen=True)
class TaskLoopProofResult:
    ok: bool
    config: IntegrationConfig
    bootstrap: RuntimeBootstrapResult
    doctor: RuntimeDoctorResult
    task_id: str | None
    brief_id: str | None
    route_summary: dict[str, Any] | None
    trace_id: str | None
    issues: list[str]


@dataclass(frozen=True)
class ResearchCronProofResult:
    ok: bool
    config: IntegrationConfig
    bootstrap: RuntimeBootstrapResult
    doctor: RuntimeDoctorResult
    standing_brief_id: str | None
    scheduled_job_id: str | None
    queued_task_id: str | None
    trace_id: str | None
    issues: list[str]


@dataclass(frozen=True)
class ProxySelfTestResult:
    ok: bool
    config: IntegrationConfig
    proxy_url: str | None
    allowed_request_count: int
    blocked_request_count: int
    audit_log_path: str
    trace_id: str | None
    issues: list[str]


@dataclass(frozen=True)
class BootstrapStackResult:
    ok: bool
    install: RuntimeProfileInstallResult
    doctor: RuntimeDoctorResult
    operator_workflow: OperatorWorkflowResult
    contract_harness: HermesContractHarnessResult
    task_loop_proof: TaskLoopProofResult
    research_cron_proof: ResearchCronProofResult
    proxy_self_test: ProxySelfTestResult
    milestone_status: dict[str, Any]


@dataclass(frozen=True)
class EvidenceScenarioResult:
    scenario_id: str
    cycle_index: int
    classification: str
    ok: bool
    trace_id: str | None
    produced_skill_families: list[str]
    issues: list[str]
    details: dict[str, Any]


@dataclass(frozen=True)
class EvidenceBatchResult:
    ok: bool
    config: IntegrationConfig
    bootstrap: RuntimeBootstrapResult
    doctor: RuntimeDoctorResult
    requested_cycles: int
    cycles: int
    until_replay_ready: bool
    stopped_reason: str
    scenario_results: list[EvidenceScenarioResult]
    generated_trace_count: int
    generated_source_trace_count: int
    generated_activation_trace_count: int
    generated_known_bad_trace_count: int
    before_replay_report: dict[str, Any]
    replay_report: dict[str, Any]
    progress_projection: dict[str, Any]
    report_path: str


@dataclass(frozen=True)
class MacStudioDayOneResult:
    ok: bool
    install: RuntimeProfileInstallResult
    doctor: RuntimeDoctorResult
    bootstrap_stack: BootstrapStackResult
    evidence_batch: EvidenceBatchResult
    replay_report: dict[str, Any]
    handoff_path: str
    issues: list[str]


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


def _ensure_task_loop_chain_definition(config: IntegrationConfig, chain_type: str = "task_loop_proof") -> None:
    db_manager = DatabaseManager(config.data_dir)
    try:
        conn = db_manager.get_connection("telemetry")
        now = _utc_now()
        steps = json.dumps(
            [
                {"step_type": "create_task", "skill": "research_domain_2"},
                {"step_type": "start_task", "skill": "research_domain_2"},
                {"step_type": "write_brief", "skill": "strategic_memory"},
                {"step_type": "complete_task", "skill": "research_domain_2"},
                {"step_type": "route_task_output", "skill": "research_domain_2"},
                {"step_type": "judge", "skill": "immune_system"},
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


def _ensure_research_cron_chain_definition(config: IntegrationConfig, chain_type: str = "research_cron_proof") -> None:
    db_manager = DatabaseManager(config.data_dir)
    try:
        conn = db_manager.get_connection("telemetry")
        now = _utc_now()
        steps = json.dumps(
            [
                {"step_type": "create_standing_brief", "skill": "research_domain_2"},
                {"step_type": "schedule_standing_brief", "skill": "research_domain_2"},
                {"step_type": "queue_standing_brief_run", "skill": "research_domain_2"},
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
                task_type, result, match_pattern, latency_ms, judge_mode, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verdict.verdict_id,
                "sheriff_input" if verdict.check_type.value == "sheriff" else "judge_output",
                verdict.tier.value,
                verdict.session_id,
                verdict.skill_name,
                verdict.task_type,
                verdict.outcome.value,
                verdict.block_reason.value if verdict.block_reason else verdict.block_detail,
                int(latency_ms),
                verdict.judge_mode.value,
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
        proxy_bind_url=config.proxy_bind_url,
        outbound_allowlist_domains=tuple(config.outbound_allowlist_domains),
        outbound_allowlist_ports=tuple(config.outbound_allowlist_ports),
        hermes_gateway_url=config.hermes_gateway_url,
        hermes_workspace_url=config.hermes_workspace_url,
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
        "bootstrap_stack": bin_dir / "bootstrap_stack.sh",
        "doctor": bin_dir / "doctor_runtime.sh",
        "readiness": bin_dir / "readiness_runtime.sh",
        "start_proxy": bin_dir / "start_local_forward_proxy.sh",
        "proxy_self_test": bin_dir / "proxy_self_test.sh",
        "operator_workflow": bin_dir / "run_operator_workflow.sh",
        "contract_harness": bin_dir / "contract_harness_runtime.sh",
        "task_loop_proof": bin_dir / "task_loop_proof.sh",
        "research_cron_proof": bin_dir / "research_cron_proof.sh",
        "evidence_factory": bin_dir / "evidence_factory.sh",
        "replay_readiness_report": bin_dir / "replay_readiness_report.sh",
        "mac_studio_day_one": bin_dir / "mac_studio_day_one.sh",
        "gateway": bin_dir / "start_gateway.sh",
        "workspace": bin_dir / "start_workspace.sh",
        "operator_checklist": bin_dir / "operator_validation_checklist.sh",
        "milestone_status": bin_dir / "milestone_status.sh",
        "workspace_overview": bin_dir / "workspace_overview.sh",
    }


def _linked_skills_dir(config: IntegrationConfig) -> Path:
    return _runtime_bundle_dir(config) / "linked_skills"


def _runtime_root(config: IntegrationConfig) -> Path:
    return Path(config.data_dir).expanduser().resolve().parent


def _runtime_profile_dir(config: IntegrationConfig) -> Path:
    return _runtime_root(config) / "profiles" / config.profile_name


def _runtime_logs_dir(config: IntegrationConfig) -> Path:
    return _runtime_root(config) / "logs"


def _runtime_profile_config_path(config: IntegrationConfig) -> Path:
    return _runtime_profile_dir(config) / "config.yaml"


def _runtime_spec_profile_path(config: IntegrationConfig) -> Path:
    return _runtime_profile_dir(config) / "profile.yaml"


def _runtime_profile_validation_repo_root(config: IntegrationConfig) -> Path:
    manifest = _read_json_yaml(_runtime_profile_manifest_path(config))
    if manifest is not None:
        repo_root = manifest.get("repo_root")
        if isinstance(repo_root, str) and repo_root.strip():
            return Path(repo_root).expanduser().resolve()
    return _repo_root()


def _runtime_operator_validation_checklist_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["operator_validation_checklist"]


def _runtime_network_controls_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["network_controls"]


def _runtime_proxy_allowlist_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["proxy_allowlist"]


def _runtime_gateway_manifest_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["gateway_manifest"]


def _runtime_workspace_manifest_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["workspace_manifest"]


def _runtime_evidence_factory_manifest_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["evidence_factory_manifest"]


def _runtime_replay_readiness_report_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["replay_readiness_report"]


def _runtime_mac_studio_day_one_handoff_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["mac_studio_day_one_handoff"]


def _runtime_proxy_audit_log_path(config: IntegrationConfig) -> Path:
    return _runtime_logs_dir(config) / "local_forward_proxy_audit.jsonl"


def _proxy_bind_host_port(config: IntegrationConfig) -> tuple[str, int]:
    split = urlsplit(config.proxy_bind_url)
    return split.hostname or "127.0.0.1", split.port or 18080


def _proxy_config_payload(
    config: IntegrationConfig,
    *,
    bind_host: str | None = None,
    bind_port: int | None = None,
    audit_log_path: str | None = None,
    allowed_domains: Sequence[str] | None = None,
    allowed_ports: Sequence[int] | None = None,
) -> dict[str, Any]:
    default_host, default_port = _proxy_bind_host_port(config)
    return {
        "bind_host": bind_host or default_host,
        "bind_port": bind_port if bind_port is not None else default_port,
        "audit_log_path": audit_log_path or str(_runtime_proxy_audit_log_path(config)),
        "outbound_allowlist": {
            "domains": list(allowed_domains or config.outbound_allowlist_domains),
            "ports": [int(port) for port in (allowed_ports or config.outbound_allowlist_ports)],
            "schemes": ["http", "https"],
        },
    }


def _evidence_factory_scenario_catalog() -> list[dict[str, str]]:
    return [
        {
            "scenario_id": "operator_workflow",
            "classification": "activation_positive",
            "description": "Runs the council-backed operator workflow smoke test.",
        },
        {
            "scenario_id": "task_loop_proof",
            "classification": "activation_positive",
            "description": "Exercises deterministic research task creation, brief writeback, routing, and judge review.",
        },
        {
            "scenario_id": "research_cron_proof",
            "classification": "activation_positive",
            "description": "Creates, schedules, and queues a standing brief run.",
        },
        {
            "scenario_id": "research_to_opportunity_flow",
            "classification": "activation_positive",
            "description": "Routes a high-quality brief through opportunity creation and council review.",
        },
        {
            "scenario_id": "opportunity_project_flow",
            "classification": "activation_positive",
            "description": "Walks an opportunity through qualification, project handoff, and learning backpropagation.",
        },
        {
            "scenario_id": "invalid_brief_completion",
            "classification": "known_bad",
            "description": "Confirms a task cannot complete with a brief belonging to another task.",
        },
        {
            "scenario_id": "archived_standing_brief_queue",
            "classification": "known_bad",
            "description": "Confirms archived standing briefs cannot queue fresh runs.",
        },
        {
            "scenario_id": "invalid_opportunity_transition",
            "classification": "known_bad",
            "description": "Confirms the opportunity state machine rejects invalid jumps.",
        },
    ]


@contextlib.contextmanager
def _temporary_env(overrides: dict[str, str]):
    previous = {key: os.environ.get(key) for key in overrides}
    try:
        for key, value in overrides.items():
            os.environ[key] = value
        yield
    finally:
        for key, old_value in previous.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


def _run_external_command(argv: Sequence[str]) -> ExternalCommandResult:
    command = tuple(str(part) for part in argv)
    try:
        completed = subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        return ExternalCommandResult(
            ok=False,
            command=command,
            returncode=-1,
            stdout="",
            stderr="",
            error=str(exc),
        )
    return ExternalCommandResult(
        ok=completed.returncode == 0,
        command=command,
        returncode=completed.returncode,
        stdout=completed.stdout.strip(),
        stderr=completed.stderr.strip(),
    )


def _parse_semver(text: str) -> tuple[int, int, int] | None:
    match = re.search(r"(\d+)\.(\d+)\.(\d+)", text)
    if match is None:
        return None
    return tuple(int(part) for part in match.groups())


def _extract_named_entries(text: str) -> list[str]:
    names: set[str] = set()
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        token = re.split(r"\s+", stripped.lstrip("-*"), maxsplit=1)[0].rstrip(":,")
        if re.fullmatch(r"[A-Za-z0-9_.-]+", token):
            names.add(token)
        for expected in EXPECTED_SEED_TOOLS:
            if expected in stripped:
                names.add(expected)
    return sorted(names)


def _format_probe_failure(result: ExternalCommandResult) -> str:
    if result.error:
        return result.error
    stderr = result.stderr or "no stderr"
    return f"exit {result.returncode}: {stderr}"


def _run_command_candidates(
    runner: Callable[[Sequence[str]], ExternalCommandResult],
    candidates: Sequence[Sequence[str]],
) -> ExternalCommandResult:
    last_result: ExternalCommandResult | None = None
    for candidate in candidates:
        result = runner(candidate)
        last_result = result
        if result.ok:
            return result
    if last_result is None:
        raise ValueError("at least one command candidate is required")
    return last_result


def _write_json_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(f"{json.dumps(payload, indent=2, sort_keys=True)}\n", encoding="utf-8")


def _read_json_yaml(path: Path) -> dict[str, Any] | None:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _validate_profile_artifacts(config: IntegrationConfig, repo_root: Path) -> HermesProfileValidationResult:
    config_path = _runtime_profile_config_path(config)
    spec_profile_path = _runtime_spec_profile_path(config)
    contract = HermesProfileContract(config=config, repo_root=str(repo_root))
    actual_config_doc = _read_json_yaml(config_path) if config_path.is_file() else None
    actual_spec_profile_doc = _read_json_yaml(spec_profile_path) if spec_profile_path.is_file() else None

    checks = contract.generated_checks(actual_config_doc, actual_spec_profile_doc)
    issues = [name for name, ok in checks.items() if not ok]
    return HermesProfileValidationResult(
        ok=not issues,
        profile_dir=str(_runtime_profile_dir(config)),
        profile_config_path=str(config_path),
        spec_profile_path=str(spec_profile_path),
        checks=checks,
        issues=issues,
    )


def _step_outcome_count(config: IntegrationConfig) -> int:
    db_path = Path(config.data_dir) / "telemetry.db"
    if not db_path.is_file():
        return 0
    with sqlite3.connect(db_path) as conn:
        return int(conn.execute("SELECT COUNT(*) FROM step_outcomes").fetchone()[0])


def _snapshot_runtime_data(config: IntegrationConfig) -> str:
    checkpoint_dir = Path(config.checkpoints_dir)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    stamp = _utc_now().replace(":", "").replace("-", "")
    snapshot_path = checkpoint_dir / f"readiness-data-snapshot-{stamp}.tar.gz"
    with tarfile.open(snapshot_path, "w:gz") as archive:
        archive.add(Path(config.data_dir), arcname="data")
    return str(snapshot_path)


def _log_execution_trace(config: IntegrationConfig, trace: ExecutionTrace) -> dict[str, Any]:
    telemetry_db_path = Path(config.data_dir) / "telemetry.db"
    manager = HarnessVariantManager(str(telemetry_db_path))
    return manager.log_execution_trace(trace)


def _install_fake_dispatch_module(dispatch: Callable[..., Any]) -> dict[str, types.ModuleType | None]:
    previous = {
        "hermes": sys.modules.get("hermes"),
        "hermes.tools": sys.modules.get("hermes.tools"),
        "hermes.tools.base": sys.modules.get("hermes.tools.base"),
    }
    hermes = types.ModuleType("hermes")
    tools = types.ModuleType("hermes.tools")
    base = types.ModuleType("hermes.tools.base")
    base.execute_tool = dispatch
    sys.modules["hermes"] = hermes
    sys.modules["hermes.tools"] = tools
    sys.modules["hermes.tools.base"] = base
    return previous


def _restore_fake_dispatch_module(previous: dict[str, types.ModuleType | None]) -> None:
    for key, module in previous.items():
        if module is None:
            sys.modules.pop(key, None)
        else:
            sys.modules[key] = module


def _capture_log_state(log_dir: Path) -> dict[Path, tuple[float, int]]:
    if not log_dir.is_dir():
        return {}
    return {
        path: (path.stat().st_mtime, path.stat().st_size)
        for path in log_dir.rglob("*")
        if path.is_file()
    }


def _log_trace_present(log_dir: Path, previous_state: dict[Path, tuple[float, int]], marker: str) -> bool:
    if not log_dir.is_dir():
        return False
    for path in sorted(log_dir.rglob("*")):
        if not path.is_file():
            continue
        stat = path.stat()
        previous = previous_state.get(path)
        if previous is not None and previous == (stat.st_mtime, stat.st_size):
            continue
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        lower = content.lower()
        if marker.lower() in lower or "shell_command" in lower or "step_outcome" in lower:
            return True
    return False


def _default_cli_smoke_query(marker: str) -> str:
    return (
        "Readiness smoke test. Use the shell_command tool exactly once to run "
        f"`echo {marker}` and then reply with the echoed marker only."
    )


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


def _write_env_launcher(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(["#!/bin/sh", "set -eu", *lines, ""]), encoding="utf-8")
    path.chmod(0o755)


def _write_replay_readiness_report_artifact(config: IntegrationConfig, payload: dict[str, Any]) -> None:
    artifact = dict(payload)
    artifact.setdefault("generated_at", _utc_now())
    artifact.setdefault("artifact_path", str(_runtime_replay_readiness_report_path(config)))
    _write_json_yaml(_runtime_replay_readiness_report_path(config), artifact)


def _write_mac_studio_day_one_handoff(
    config: IntegrationConfig,
    repo_root: Path,
    *,
    install: RuntimeProfileInstallResult | None = None,
    evidence_batch: EvidenceBatchResult | None = None,
    bootstrap_stack_result: BootstrapStackResult | None = None,
    replay_report: dict[str, Any] | None = None,
) -> None:
    manifest = _read_json_yaml(_runtime_profile_manifest_path(config)) or {}
    commands = manifest.get("commands", {})
    lines = [
        "# Mac Studio Day-One Handoff",
        "",
        f"Generated: {_utc_now()}",
        "",
        "## Goal",
        "",
        "Use this package to rehearse the repo-local substrate now and to cut over quickly once Hermes is available on the Mac Studio.",
        "",
        "## Day-One Sequence",
        "",
        f"1. Start the local forward proxy: `{commands.get('start_proxy', 'not-generated')}`",
        f"2. Rehearse the repo-local launch bundle: `{commands.get('bootstrap_stack', _command_string(config, '--bootstrap-stack', repo_root))}`",
        f"3. Grow replay corpus: `{commands.get('evidence_factory', _command_string(config, '--evidence-factory', repo_root))}`",
        f"   If you want one bounded run toward readiness, use: `{commands.get('evidence_factory', _command_string(config, '--evidence-factory', repo_root))} --until-replay-ready --evidence-cycles {DEFAULT_EVIDENCE_CYCLES}`",
        f"4. Inspect replay coverage: `{commands.get('replay_readiness_report', _command_string(config, '--replay-readiness-report', repo_root))}`",
        f"5. When Hermes is installed, run live readiness: `{commands.get('readiness', _command_string(config, '--readiness', repo_root))}`",
        f"6. Open the workspace/operator view: `{commands.get('workspace_overview', _command_string(config, '--workspace-overview', repo_root))}`",
        "",
        "## Runtime Artifacts",
        "",
        f"- Profile manifest: `{_runtime_profile_manifest_path(config)}`",
        f"- Operator checklist: `{_runtime_operator_validation_checklist_path(config)}`",
        f"- Proxy allowlist: `{_runtime_proxy_allowlist_path(config)}`",
        f"- Proxy audit log: `{_runtime_proxy_audit_log_path(config)}`",
        f"- Evidence manifest: `{_runtime_evidence_factory_manifest_path(config)}`",
        f"- Replay readiness report: `{_runtime_replay_readiness_report_path(config)}`",
        "",
    ]
    if install is not None:
        lines.extend(
            [
                "## Installed Bundle",
                "",
                f"- Profile dir: `{install.profile_dir}`",
                f"- Config path: `{install.profile_config_path}`",
                f"- Linked skills: `{len(install.linked_skill_paths)}`",
                "",
            ]
        )
    if bootstrap_stack_result is not None:
        lines.extend(
            [
                "## Rehearsal Status",
                "",
                f"- Bootstrap stack: `{'PASS' if bootstrap_stack_result.ok else 'FAIL'}`",
                f"- Proxy self-test: `{'PASS' if bootstrap_stack_result.proxy_self_test.ok else 'FAIL'}`",
                f"- Operator workflow: `{'PASS' if bootstrap_stack_result.operator_workflow.ok else 'FAIL'}`",
                f"- Contract harness: `{'PASS' if bootstrap_stack_result.contract_harness.ok else 'FAIL'}`",
                f"- Task loop proof: `{'PASS' if bootstrap_stack_result.task_loop_proof.ok else 'FAIL'}`",
                f"- Research cron proof: `{'PASS' if bootstrap_stack_result.research_cron_proof.ok else 'FAIL'}`",
                "",
            ]
        )
    if evidence_batch is not None:
        lines.extend(
            [
                "## Evidence Batch",
                "",
                f"- Cycles: `{evidence_batch.cycles}`",
                f"- Stop reason: `{evidence_batch.stopped_reason}`",
                f"- Scenarios passed: `{sum(1 for item in evidence_batch.scenario_results if item.ok)}/{len(evidence_batch.scenario_results)}`",
                f"- Generated traces: `{evidence_batch.generated_trace_count}`",
                f"- Generated activation traces: `{evidence_batch.generated_activation_trace_count}`",
                f"- Generated known-bad traces: `{evidence_batch.generated_known_bad_trace_count}`",
                "",
            ]
        )
    if replay_report is not None:
        lines.extend(
            [
                "## Replay Readiness Snapshot",
                "",
                f"- Status: `{replay_report.get('status', 'UNKNOWN')}`",
                f"- Eligible traces: `{replay_report.get('eligible_source_traces', 0)}/{replay_report.get('minimum_eligible_traces', 0)}`",
                f"- Known-bad traces: `{replay_report.get('known_bad_source_traces', 0)}/{replay_report.get('minimum_known_bad_traces', 0)}`",
                f"- Distinct skills: `{replay_report.get('distinct_skill_count', 0)}/{replay_report.get('minimum_distinct_skills', 0)}`",
                "",
            ]
        )
    _runtime_mac_studio_day_one_handoff_path(config).write_text("\n".join(lines), encoding="utf-8")


def _write_runtime_support_artifacts(config: IntegrationConfig, repo_root: Path) -> None:
    contract = HermesProfileContract(config=config, repo_root=str(repo_root))
    artifacts = runtime_support_artifact_paths(config)
    for path in artifacts.values():
        path.parent.mkdir(parents=True, exist_ok=True)

    network_doc = {
        **contract.network_controls(),
        "proxy_allowlist_path": str(_runtime_proxy_allowlist_path(config)),
        "proxy_audit_log_path": str(_runtime_proxy_audit_log_path(config)),
        "proxy_environment": {
            "HTTP_PROXY": config.proxy_bind_url,
            "HTTPS_PROXY": config.proxy_bind_url,
            "ALL_PROXY": config.proxy_bind_url,
            "NO_PROXY": ",".join(config.outbound_allowlist_domains),
        },
        "gateway_url": config.hermes_gateway_url,
    }
    proxy_doc = _proxy_config_payload(config)
    gateway_doc = {
        **contract.gateway_mapping(),
        "workspace_url": config.hermes_workspace_url,
        "network_controls_path": str(_runtime_network_controls_path(config)),
        "startup_hint": (
            "Set HERMES_GATEWAY_CMD to your preferred Hermes gateway command to let the "
            "generated launcher execute it directly."
        ),
    }
    workspace_doc = {
        **contract.workspace_mapping(),
        "gateway_url": config.hermes_gateway_url,
        "workspace_snapshot_command": _command_string(config, "--workspace-overview", repo_root),
        "milestone_status_command": _command_string(config, "--milestone-status", repo_root),
    }
    evidence_doc = {
        "generated_at": _utc_now(),
        "command": _command_string(config, "--evidence-factory", repo_root),
        "until_replay_ready_command": (
            _command_string(config, "--evidence-factory", repo_root)
            + " --until-replay-ready"
            + f" --evidence-cycles {DEFAULT_EVIDENCE_CYCLES}"
        ),
        "recommended_cycles": DEFAULT_EVIDENCE_CYCLES,
        "report_command": _command_string(config, "--replay-readiness-report", repo_root),
        "scenarios": _evidence_factory_scenario_catalog(),
    }
    checklist_lines = [
        "# Operator Validation Checklist",
        "",
        "1. Start the generated local forward proxy launcher and confirm it binds cleanly.",
        "2. Run the generated bootstrap stack command.",
        "3. Confirm `doctor` passes and all canonical databases are in WAL mode.",
        "4. Confirm the repo-local contract harness and proxy self-test pass.",
        "5. Run the evidence factory and inspect the replay readiness report.",
        "6. Confirm the task-loop and research-cron proofs pass.",
        "7. If Hermes is installed, run readiness and verify the live profile/config surface.",
        "8. Open the Hermes Workspace and confirm gates, traces, quarantine review, replay readiness, runtime halt state, and milestone health are visible.",
    ]
    _write_json_yaml(_runtime_network_controls_path(config), network_doc)
    _write_json_yaml(_runtime_proxy_allowlist_path(config), proxy_doc)
    _write_json_yaml(_runtime_gateway_manifest_path(config), gateway_doc)
    _write_json_yaml(_runtime_workspace_manifest_path(config), workspace_doc)
    _write_json_yaml(_runtime_evidence_factory_manifest_path(config), evidence_doc)
    _write_replay_readiness_report_artifact(
        config,
        {
            "available": False,
            "status": "UNAVAILABLE",
            "operator_ack_required_below_threshold": True,
            "minimum_eligible_traces": 500,
            "minimum_known_bad_traces": 25,
            "minimum_distinct_skills": 3,
            "eligible_source_traces": 0,
            "known_bad_source_traces": 0,
            "distinct_skill_count": 0,
            "blockers": ["telemetry_unavailable"],
        },
    )
    _runtime_operator_validation_checklist_path(config).write_text(
        "\n".join(checklist_lines) + "\n",
        encoding="utf-8",
    )
    _write_mac_studio_day_one_handoff(config, repo_root)


def _symlink_skill_directory(source: Path, dest: Path) -> None:
    if dest.is_symlink():
        if dest.resolve() == source.resolve():
            return
        dest.unlink()
    elif dest.exists():
        raise FileExistsError(f"Refusing to replace non-symlink path: {dest}")
    try:
        dest.symlink_to(source, target_is_directory=True)
    except FileExistsError:
        if dest.is_symlink() and dest.resolve() == source.resolve():
            return
        raise


def _next_contract_harness_base_time(immune_db_path: Path, guard_hours: int) -> datetime.datetime:
    base_time = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    if not immune_db_path.is_file():
        return base_time
    with sqlite3.connect(immune_db_path) as conn:
        row = conn.execute("SELECT MAX(started_at) FROM judge_fallback_events").fetchone()
    latest = row[0] if row is not None else None
    if not latest:
        return base_time
    latest_started = datetime.datetime.fromisoformat(str(latest))
    if latest_started.tzinfo is None:
        latest_started = latest_started.replace(tzinfo=datetime.timezone.utc)
    else:
        latest_started = latest_started.astimezone(datetime.timezone.utc)
    candidate = latest_started + datetime.timedelta(hours=guard_hours, seconds=5)
    return candidate if candidate > base_time else base_time


def prepare_runtime_directories(config: IntegrationConfig) -> IntegrationConfig:
    """Resolve and create the filesystem layout expected by the integration layer."""
    resolved = _normalize_runtime_layout(config).resolve_paths()
    path_hints = (
        (resolved.data_dir, "--data-dir"),
        (resolved.skills_dir, "--skills-dir"),
        (resolved.checkpoints_dir, "--checkpoints-dir"),
        (resolved.alerts_dir, "--alerts-dir"),
        (str(_runtime_logs_dir(resolved)), "--data-dir"),
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
        "profile_dir": str(_runtime_profile_dir(resolved)),
        "profile_config_path": str(_runtime_profile_config_path(resolved)),
        "spec_profile_path": str(_runtime_spec_profile_path(resolved)),
        "network_controls_path": str(_runtime_network_controls_path(resolved)),
        "proxy_allowlist_path": str(_runtime_proxy_allowlist_path(resolved)),
        "proxy_audit_log_path": str(_runtime_proxy_audit_log_path(resolved)),
        "gateway_manifest_path": str(_runtime_gateway_manifest_path(resolved)),
        "workspace_manifest_path": str(_runtime_workspace_manifest_path(resolved)),
        "operator_validation_checklist_path": str(_runtime_operator_validation_checklist_path(resolved)),
        "evidence_factory_manifest_path": str(_runtime_evidence_factory_manifest_path(resolved)),
        "replay_readiness_report_path": str(_runtime_replay_readiness_report_path(resolved)),
        "mac_studio_day_one_handoff_path": str(_runtime_mac_studio_day_one_handoff_path(resolved)),
        "data_dir": resolved.data_dir,
        "skills_dir": resolved.skills_dir,
        "checkpoints_dir": resolved.checkpoints_dir,
        "alerts_dir": resolved.alerts_dir,
        "logs_dir": str(_runtime_logs_dir(resolved)),
        "linked_skills": linked_skill_paths,
        "commands": {
            "bootstrap": _command_string(resolved, "--bootstrap-live", root),
            "bootstrap_stack": _command_string(resolved, "--bootstrap-stack", root),
            "doctor": _command_string(resolved, "--doctor", root),
            "readiness": _command_string(resolved, "--readiness", root),
            "start_proxy": str(launcher_paths["start_proxy"]),
            "proxy_self_test": _command_string(resolved, "--proxy-self-test", root),
            "operator_workflow": _command_string(resolved, "--operator-workflow", root),
            "contract_harness": _command_string(resolved, "--contract-harness", root),
            "task_loop_proof": _command_string(resolved, "--task-loop-proof", root),
            "research_cron_proof": _command_string(resolved, "--research-cron-proof", root),
            "evidence_factory": _command_string(resolved, "--evidence-factory", root),
            "replay_readiness_report": _command_string(resolved, "--replay-readiness-report", root),
            "mac_studio_day_one": _command_string(resolved, "--mac-studio-day-one", root),
            "milestone_status": _command_string(resolved, "--milestone-status", root),
            "workspace_overview": _command_string(resolved, "--workspace-overview", root),
        },
    }
    manifest_path = _runtime_profile_manifest_path(resolved)
    manifest_path.write_text(f"{json.dumps(manifest, indent=2, sort_keys=True)}\n", encoding="utf-8")

    profile_dir = _runtime_profile_dir(resolved)
    profile_dir.mkdir(parents=True, exist_ok=True)
    contract = HermesProfileContract(config=resolved, repo_root=str(root))
    config_doc = contract.config_document()
    spec_profile_doc = contract.spec_profile_document()
    _write_json_yaml(_runtime_profile_config_path(resolved), config_doc)
    _write_json_yaml(_runtime_spec_profile_path(resolved), spec_profile_doc)
    _write_runtime_support_artifacts(resolved, root)

    _write_launcher(launcher_paths["bootstrap"], resolved, root, "--bootstrap-live")
    _write_launcher(launcher_paths["bootstrap_stack"], resolved, root, "--bootstrap-stack")
    _write_launcher(launcher_paths["doctor"], resolved, root, "--doctor")
    _write_launcher(launcher_paths["readiness"], resolved, root, "--readiness")
    _write_launcher(launcher_paths["proxy_self_test"], resolved, root, "--proxy-self-test")
    _write_launcher(launcher_paths["operator_workflow"], resolved, root, "--operator-workflow")
    _write_launcher(launcher_paths["contract_harness"], resolved, root, "--contract-harness")
    _write_launcher(launcher_paths["task_loop_proof"], resolved, root, "--task-loop-proof")
    _write_launcher(launcher_paths["research_cron_proof"], resolved, root, "--research-cron-proof")
    _write_launcher(launcher_paths["evidence_factory"], resolved, root, "--evidence-factory")
    _write_launcher(launcher_paths["replay_readiness_report"], resolved, root, "--replay-readiness-report")
    _write_launcher(launcher_paths["mac_studio_day_one"], resolved, root, "--mac-studio-day-one")
    _write_launcher(launcher_paths["milestone_status"], resolved, root, "--milestone-status")
    _write_launcher(launcher_paths["workspace_overview"], resolved, root, "--workspace-overview")
    _write_launcher(launcher_paths["operator_checklist"], resolved, root, "--operator-checklist")
    _write_env_launcher(
        launcher_paths["start_proxy"],
        [
            f"CONFIG_PATH={shlex.quote(str(_runtime_proxy_allowlist_path(resolved)))}",
            'exec python3 -m skills.local_forward_proxy --config "$CONFIG_PATH" "$@"',
        ],
    )
    _write_env_launcher(
        launcher_paths["gateway"],
        [
            f"MANIFEST_PATH={shlex.quote(str(_runtime_gateway_manifest_path(resolved)))}",
            f"GATEWAY_URL={shlex.quote(resolved.hermes_gateway_url)}",
            'if [ -n "${HERMES_GATEWAY_CMD:-}" ]; then',
            '  exec sh -lc "$HERMES_GATEWAY_CMD"',
            "fi",
            'printf "gateway_url=%s\\nmanifest=%s\\n" "$GATEWAY_URL" "$MANIFEST_PATH"',
        ],
    )
    _write_env_launcher(
        launcher_paths["workspace"],
        [
            f"MANIFEST_PATH={shlex.quote(str(_runtime_workspace_manifest_path(resolved)))}",
            f"WORKSPACE_URL={shlex.quote(resolved.hermes_workspace_url)}",
            'if [ -n "${HERMES_WORKSPACE_CMD:-}" ]; then',
            '  exec sh -lc "$HERMES_WORKSPACE_CMD"',
            "fi",
            'printf "workspace_url=%s\\nmanifest=%s\\n" "$WORKSPACE_URL" "$MANIFEST_PATH"',
        ],
    )

    return RuntimeProfileInstallResult(
        config=resolved,
        repo_root=str(root),
        profile_dir=str(profile_dir),
        profile_config_path=str(_runtime_profile_config_path(resolved)),
        spec_profile_path=str(_runtime_spec_profile_path(resolved)),
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
        "logs_dir": _runtime_logs_dir(resolved).is_dir(),
        "profile_manifest": _runtime_profile_manifest_path(resolved).is_file(),
        "profile_dir": _runtime_profile_dir(resolved).is_dir(),
        "profile_config": _runtime_profile_config_path(resolved).is_file(),
        "spec_profile": _runtime_spec_profile_path(resolved).is_file(),
        "network_controls": _runtime_network_controls_path(resolved).is_file(),
        "proxy_allowlist": _runtime_proxy_allowlist_path(resolved).is_file(),
        "gateway_manifest": _runtime_gateway_manifest_path(resolved).is_file(),
        "workspace_manifest": _runtime_workspace_manifest_path(resolved).is_file(),
        "operator_validation_checklist": _runtime_operator_validation_checklist_path(resolved).is_file(),
        "evidence_factory_manifest": _runtime_evidence_factory_manifest_path(resolved).is_file(),
        "replay_readiness_report": _runtime_replay_readiness_report_path(resolved).is_file(),
        "mac_studio_day_one_handoff": _runtime_mac_studio_day_one_handoff_path(resolved).is_file(),
        "bootstrap_launcher": launcher_paths["bootstrap"].is_file(),
        "bootstrap_stack_launcher": launcher_paths["bootstrap_stack"].is_file(),
        "doctor_launcher": launcher_paths["doctor"].is_file(),
        "readiness_launcher": launcher_paths["readiness"].is_file(),
        "start_proxy_launcher": launcher_paths["start_proxy"].is_file(),
        "proxy_self_test_launcher": launcher_paths["proxy_self_test"].is_file(),
        "operator_workflow_launcher": launcher_paths["operator_workflow"].is_file(),
        "contract_harness_launcher": launcher_paths["contract_harness"].is_file(),
        "task_loop_proof_launcher": launcher_paths["task_loop_proof"].is_file(),
        "research_cron_proof_launcher": launcher_paths["research_cron_proof"].is_file(),
        "evidence_factory_launcher": launcher_paths["evidence_factory"].is_file(),
        "replay_readiness_report_launcher": launcher_paths["replay_readiness_report"].is_file(),
        "mac_studio_day_one_launcher": launcher_paths["mac_studio_day_one"].is_file(),
        "gateway_launcher": launcher_paths["gateway"].is_file(),
        "workspace_launcher": launcher_paths["workspace"].is_file(),
        "operator_checklist_launcher": launcher_paths["operator_checklist"].is_file(),
        "milestone_status_launcher": launcher_paths["milestone_status"].is_file(),
        "workspace_overview_launcher": launcher_paths["workspace_overview"].is_file(),
    }
    profile_validation = _validate_profile_artifacts(
        resolved,
        _runtime_profile_validation_repo_root(resolved),
    )

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
    missing_items.extend(f"profile:{issue}" for issue in profile_validation.issues)
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
        profile_validation=profile_validation,
    )


def exercise_hermes_contract(
    *,
    config: IntegrationConfig | None = None,
    repo_root: str | None = None,
    tool_registry: HermesToolRegistry | None = None,
) -> HermesContractHarnessResult:
    """Exercise the repo-local Hermes gate/dispatch/halt/restart contract end to end."""
    resolved = _normalize_runtime_layout(config or IntegrationConfig()).resolve_paths()
    root = Path(repo_root).expanduser().resolve() if repo_root else _repo_root()
    registry = tool_registry or MockHermesRuntime(data_dir=resolved.data_dir)
    issues: list[str] = []
    route_decision: dict[str, Any] | None = None
    approval_request: dict[str, Any] | None = None
    approval_review: dict[str, Any] | None = None
    dispatch_result: dict[str, Any] | None = None
    judge_deadlock_event: dict[str, Any] | None = None
    runtime_halt: dict[str, Any] | None = None
    restart_result: dict[str, Any] | None = None
    final_runtime_status: dict[str, Any] | None = None
    trace_id: str | None = None
    blocked_dispatch_pre_side_effect = False
    blocked_dispatch_reason: str | None = None
    trace_steps: list[ExecutionTraceStep] = []

    def append_trace_step(step_name: str, payload: Any, *, model_used: str = "repo-contract") -> None:
        serialized = payload if isinstance(payload, str) else json.dumps(payload, sort_keys=True, default=str)
        trace_steps.append(
            ExecutionTraceStep(
                step_index=len(trace_steps) + 1,
                tool_call=step_name,
                tool_result=serialized[:4096],
                tool_result_file=None,
                tokens_in=0,
                tokens_out=0,
                latency_ms=0,
                model_used=model_used,
            )
        )

    env_overrides = {
        "IMMUNE_JUDGE_DEADLOCK_WINDOW_SECONDS": "2",
        "IMMUNE_JUDGE_DEADLOCK_FALLBACK_MINUTES": "1",
        "IMMUNE_JUDGE_DEADLOCK_GUARD_HOURS": "1",
    }
    with _temporary_env(env_overrides):
        install_runtime_profile(resolved, repo_root=str(root))
        bootstrap = bootstrap_runtime(
            registry,
            config=resolved,
            model_name="contract-harness",
            jwt_claims={"max_api_spend_usd": 5.0, "current_session_spend_usd": 0.0},
        )
        doctor = doctor_runtime(registry, config=resolved, bootstrap_if_needed=False)
        contract_checks = _validate_profile_artifacts(resolved, root).checks
        if not bootstrap.ok:
            issues.append("bootstrap failed")
        if not doctor.ok:
            issues.append("doctor failed")
        failed_contract_checks = [name for name, ok in contract_checks.items() if not ok]
        if failed_contract_checks:
            issues.append(f"profile contract failed: {', '.join(failed_contract_checks)}")

        session_id = bootstrap.session_context.session_id
        correlation_id = f"contract-route-{generate_uuid_v7()}"
        route = registry.invoke_tool(
            "financial_router",
            {
                "action": "route",
                "task": TaskMetadata(
                    task_id=f"task-{generate_uuid_v7()}",
                    task_type="hermes_contract_harness",
                    required_capability="shell_command",
                    quality_threshold=0.9,
                    estimated_task_value_usd=500.0,
                    project_id="contract-project",
                    idempotency_key=correlation_id,
                    is_operating_phase=True,
                ),
                "models": [ModelInfo("gpt-paid-strong", "paid", True, 0.99, 0.25)],
                "budget": BudgetState(system_phase=SystemPhase.OPERATING),
                "jwt": JWTClaims(session_id=session_id, max_api_spend_usd=5.0, current_session_spend_usd=0.0),
            },
        )
        if not route.success:
            issues.append(f"route failed: {route.error}")
        else:
            route_output = route.output
            route_decision = {
                "tier": route_output.tier.value,
                "model_id": route_output.model_id,
                "g3_path": route_output.g3_path.value,
                "estimated_cost_usd": route_output.estimated_cost_usd,
                "quality_warning": route_output.quality_warning,
                "justification": route_output.justification,
                "requires_operator_approval": route_output.requires_operator_approval,
                "compute_starved": route_output.compute_starved,
            }
            append_trace_step("financial_router.route", route_decision)
            if route_output.tier.value != "paid_cloud" or not route_output.requires_operator_approval:
                issues.append("contract harness did not produce a Path B paid approval request")

        pending_requests = registry.invoke_tool(
            "operator_interface",
            {"action": "list_g3_approval_requests", "limit": 5, "status": "PENDING"},
        )
        if not pending_requests.success:
            issues.append(f"list_g3_approval_requests failed: {pending_requests.error}")
        else:
            approval_request = next(
                (row for row in pending_requests.output if row["correlation_id"] == correlation_id),
                None,
            )
            append_trace_step(
                "operator_interface.list_g3_approval_requests",
                {"pending_count": len(pending_requests.output), "matched": approval_request is not None},
            )
            if approval_request is None:
                issues.append("approval request not found after paid route selection")

        if approval_request is not None:
            approval = registry.invoke_tool(
                "operator_interface",
                {
                    "action": "review_g3_approval_request",
                    "request_id": approval_request["request_id"],
                    "decision": "APPROVE",
                    "operator_notes": "contract harness approval",
                },
            )
            if not approval.success:
                issues.append(f"approval review failed: {approval.error}")
            else:
                approval_review = approval.output
                append_trace_step("operator_interface.review_g3_approval_request", approval_review)
                if approval_review["status"] != "APPROVED":
                    issues.append("approval request did not move to APPROVED")

        if approval_request is not None:
            dispatch = registry.invoke_tool(
                "operator_interface",
                {
                    "action": "dispatch_approved_paid_route",
                    "correlation_id": correlation_id,
                    "jwt_claims": {
                        "session_id": session_id,
                        "max_api_spend_usd": 5.0,
                        "current_session_spend_usd": 0.0,
                    },
                },
            )
            if not dispatch.success:
                issues.append(f"dispatch_approved_paid_route failed: {dispatch.error}")
            else:
                dispatch_result = dispatch.output
                append_trace_step("operator_interface.dispatch_approved_paid_route", dispatch_result)
                if dispatch_result["dispatch_status"] != "DISPATCHED":
                    issues.append("paid route did not enter DISPATCHED state")

        immune_db_path = Path(resolved.data_dir) / "immune_system.db"
        judge_lifecycle = JudgeLifecycleManager(str(immune_db_path), load_config())
        base_time = _next_contract_harness_base_time(
            immune_db_path,
            judge_lifecycle._config.judge_deadlock_guard_hours,
        )
        deadlock_task_types = ["analysis", "routing", "operator_interface", "analysis"]
        active_event_id: str | None = None
        with sqlite3.connect(immune_db_path) as conn:
            for offset, task_type in enumerate(deadlock_task_types):
                ts = (base_time + datetime.timedelta(seconds=offset)).replace(microsecond=0).isoformat()
                verdict = ImmuneVerdict(
                    verdict_id=generate_uuid_v7(),
                    check_type=CheckType.JUDGE,
                    tier=Tier.FAST_PATH,
                    skill_name="contract_harness",
                    session_id=session_id,
                    outcome=Outcome.BLOCK,
                    block_reason=BlockReason.INTERNAL_ERROR,
                    block_detail="contract deadlock sample",
                    latency_ms=0.0,
                    alert_severity=AlertSeverity.SECURITY_ALERT,
                    judge_mode=JudgeMode.NORMAL,
                    task_type=task_type,
                )
                conn.execute(
                    """
                    INSERT INTO immune_verdicts (
                        verdict_id, verdict_type, scan_tier, session_id, skill_name,
                        task_type, result, match_pattern, latency_ms, judge_mode, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        verdict.verdict_id,
                        "judge_output",
                        verdict.tier.value,
                        verdict.session_id,
                        verdict.skill_name,
                        verdict.task_type,
                        verdict.outcome.value,
                        verdict.block_reason.value,
                        int(verdict.latency_ms),
                        verdict.judge_mode.value,
                        ts,
                    ),
                )
                conn.commit()
                event = judge_lifecycle.record_verdict(
                    JudgePayload(
                        session_id=session_id,
                        skill_name="contract_harness",
                        tool_name="shell_command",
                        output={"ok": False},
                        task_type=task_type,
                    ),
                    verdict,
                    reference_time=ts,
                )
                if event is not None and event["status"] == "ACTIVE":
                    active_event_id = event["event_id"]

        if active_event_id is None:
            issues.append("judge deadlock fallback did not activate")
        else:
            judge_deadlock_event = judge_lifecycle.status(
                reference_time=(base_time + datetime.timedelta(seconds=3)).replace(microsecond=0).isoformat()
            )["active_event"]
            append_trace_step("judge_deadlock.activate", {"event_id": active_event_id, "status": "ACTIVE"})
            runtime_control = RuntimeControlManager(str(Path(resolved.data_dir) / "operator_digest.db"))
            runtime_halt = runtime_control.activate_halt(
                source="JUDGE_DEADLOCK",
                trigger_event_id=active_event_id,
                halt_reason="contract_deadlock_halt",
                halt_scope="FULL_SYSTEM_HALT",
                requires_human=True,
                reference_time=(base_time + datetime.timedelta(seconds=3)).replace(microsecond=0).isoformat(),
            )
            append_trace_step("runtime_control.activate_halt", runtime_halt)

        runtime_status = registry.invoke_tool("operator_interface", {"action": "runtime_status"})
        if not runtime_status.success:
            issues.append(f"runtime_status failed: {runtime_status.error}")
        else:
            runtime_halt = runtime_status.output["active_halt"] or runtime_halt
            append_trace_step("operator_interface.runtime_status", runtime_status.output)
            if runtime_status.output["lifecycle_state"] != "HALTED":
                issues.append("runtime did not enter HALTED state after judge deadlock halt")

        dispatch_called = {"count": 0}

        def _fake_dispatch(*args: Any, **kwargs: Any) -> dict[str, Any]:
            dispatch_called["count"] += 1
            return {"ok": True, "claimed_trust_tier": 4}

        def _pass_verdict(*_args: Any, **_kwargs: Any) -> ImmuneVerdict:
            return ImmuneVerdict(
                verdict_id=generate_uuid_v7(),
                check_type=CheckType.SHERIFF,
                tier=Tier.FAST_PATH,
                skill_name="contract_harness",
                session_id=session_id,
                outcome=Outcome.PASS,
                latency_ms=0.0,
                task_type="hermes_contract_harness",
            )

        previous_modules = _install_fake_dispatch_module(_fake_dispatch)
        try:
            logger = VerdictLogger(str(immune_db_path), load_config())
            if not apply_immune_patch(_pass_verdict, _pass_verdict, load_config(), logger):
                issues.append("immune bootstrap patch could not be applied for contract harness")
            else:
                try:
                    sys.modules["hermes.tools.base"].execute_tool(
                        tool_name="safe_tool",
                        arguments={},
                        skill_name="contract_harness",
                        session_id=session_id,
                        execution_stack="immune_system",
                        task_type="hermes_contract_harness",
                    )
                    issues.append("halted dispatch unexpectedly executed underlying tool")
                except ImmuneBlockError as exc:
                    blocked_dispatch_pre_side_effect = dispatch_called["count"] == 0
                    blocked_dispatch_reason = str(exc)
                    append_trace_step(
                        "immune.bootstrap_patch.blocked_dispatch",
                        {
                            "blocked": True,
                            "dispatch_called": dispatch_called["count"],
                            "reason": blocked_dispatch_reason,
                        },
                    )
                    if not blocked_dispatch_pre_side_effect:
                        issues.append("halted dispatch called underlying tool before blocking")
        finally:
            _restore_fake_dispatch_module(previous_modules)

        restart = registry.invoke_tool(
            "operator_interface",
            {
                "action": "restart_runtime_after_halt",
                "judge_event_id": active_event_id,
                "restart_reason": "contract_harness_clear",
                "notes": "Clear deadlock after contract harness proof",
                "reference_time": (base_time + datetime.timedelta(seconds=8)).replace(microsecond=0).isoformat(),
            },
        )
        if not restart.success:
            issues.append(f"restart_runtime_after_halt failed: {restart.error}")
        else:
            restart_result = restart.output
            append_trace_step("operator_interface.restart_runtime_after_halt", restart_result)
            if restart_result["status"] != "COMPLETED":
                issues.append("runtime restart did not complete after deadlock window cleared")

        runtime_status_after = registry.invoke_tool("operator_interface", {"action": "runtime_status"})
        if runtime_status_after.success:
            final_runtime_status = runtime_status_after.output
            append_trace_step("operator_interface.runtime_status.final", final_runtime_status)
            if final_runtime_status["lifecycle_state"] != "ACTIVE":
                issues.append("runtime did not return to ACTIVE after restart")
        else:
            issues.append(f"final runtime_status failed: {runtime_status_after.error}")

        ok = not issues
        trace_id = generate_uuid_v7()
        _log_execution_trace(
            resolved,
            ExecutionTrace(
                trace_id=trace_id,
                task_id=f"contract-harness-{trace_id}",
                role="runtime_contract",
                skill_name="runtime",
                harness_version="hermes_contract_harness_v1",
                intent_goal="Validate repo-local Hermes gate, dispatch, halt, blocked execution, and restart contract.",
                steps=trace_steps,
                prompt_template="repo-local Hermes contract harness",
                context_assembled="runtime profile + operator interface + judge deadlock + bootstrap patch",
                retrieval_queries=[],
                judge_verdict="PASS" if ok else "FAIL",
                judge_reasoning="Contract harness completed end to end." if ok else "Contract harness exposed blocking issues.",
                outcome_score=1.0 if ok else 0.0,
                cost_usd=0.0,
                duration_ms=0,
                training_eligible=ok,
                retention_class="STANDARD" if ok else "FAILURE_AUDIT",
                source_chain_id=correlation_id,
                source_session_id=session_id,
                source_trace_id=None,
                created_at=_utc_now(),
            ),
        )

        return HermesContractHarnessResult(
            ok=ok,
            config=resolved,
            bootstrap=bootstrap,
            doctor=doctor,
            contract_checks=contract_checks,
            route_decision=route_decision,
            approval_request=approval_request,
            approval_review=approval_review,
            dispatch_result=dispatch_result,
            judge_deadlock_event=judge_deadlock_event,
            runtime_halt=runtime_halt,
            blocked_dispatch_pre_side_effect=blocked_dispatch_pre_side_effect,
            blocked_dispatch_reason=blocked_dispatch_reason,
            restart_result=restart_result,
            final_runtime_status=final_runtime_status,
            trace_id=trace_id,
            issues=issues,
        )


def run_task_loop_proof(
    *,
    config: IntegrationConfig | None = None,
    repo_root: str | None = None,
    tool_registry: HermesToolRegistry | None = None,
) -> TaskLoopProofResult:
    resolved = _normalize_runtime_layout(config or IntegrationConfig()).resolve_paths()
    root = Path(repo_root).expanduser().resolve() if repo_root else _repo_root()
    registry = tool_registry or MockHermesRuntime(data_dir=resolved.data_dir)
    install_runtime_profile(resolved, repo_root=str(root))
    bootstrap = bootstrap_runtime(registry, config=resolved, model_name="task-loop-proof")
    _ensure_task_loop_chain_definition(resolved)
    chain_id = f"task-loop-{generate_uuid_v7()}"
    issues: list[str] = []
    task_id: str | None = None
    brief_id: str | None = None
    route_summary: dict[str, Any] | None = None
    trace_steps: list[ExecutionTraceStep] = []

    def append_trace_step(step_name: str, skill_name: str, result: Any) -> None:
        payload = {
            "success": bool(getattr(result, "success", False)),
            "error": getattr(result, "error", None),
            "output": getattr(result, "output", None),
        }
        trace_steps.append(
            ExecutionTraceStep(
                step_index=len(trace_steps) + 1,
                tool_call=f"{skill_name}.{step_name}",
                tool_result=json.dumps(payload, sort_keys=True, default=str)[:4096],
                tool_result_file=None,
                tokens_in=0,
                tokens_out=0,
                latency_ms=int(getattr(result, "duration_ms", 0) or 0),
                model_used="task-loop-proof",
            )
        )

    create_task = registry.invoke_tool(
        "research_domain_2",
        {
            "action": "create_task",
            "title": "Task loop proof",
            "brief": "Validate the deterministic research task loop and downstream routing surfaces.",
            "priority": "P1_HIGH",
            "tags": ["runtime", "task-loop", "proof"],
        },
    )
    _record_tool_step(resolved, chain_id=chain_id, step_type="create_task", skill="research_domain_2", result=create_task)
    append_trace_step("create_task", "research_domain_2", create_task)
    if not create_task.success:
        issues.append(create_task.error or "create_task failed")
    else:
        task_id = create_task.output

    if task_id is not None:
        start_task = registry.invoke_tool("research_domain_2", {"action": "start_task", "task_id": task_id})
        _record_tool_step(resolved, chain_id=chain_id, step_type="start_task", skill="research_domain_2", result=start_task)
        append_trace_step("start_task", "research_domain_2", start_task)
        if not start_task.success:
            issues.append(start_task.error or "start_task failed")

        write_brief = registry.invoke_tool(
            "strategic_memory",
            {
                "action": "write_brief",
                "task_id": task_id,
                "title": "Task loop proof brief",
                "summary": "The deterministic research task loop completed and produced a routable brief.",
                "confidence": 0.79,
                "actionability": "ACTION_RECOMMENDED",
                "action_type": "opportunity_feed",
                "depth_tier": "FULL",
                "source_urls": ["https://example.com/task-loop", "https://api.example.com/task-loop"],
                "source_assessments": [
                    {"url": "https://example.com/task-loop", "relevance": 0.84, "freshness": "2026-04-22", "source_type": "tier2_web"},
                    {"url": "https://api.example.com/task-loop", "relevance": 0.9, "freshness": "2026-04-22", "source_type": "tier1_api"},
                ],
                "uncertainty_statement": "The proof still runs against mock/runtime-local surfaces rather than a live Hermes install.",
                "counter_thesis": "The remaining risk is that the real operator environment introduces timing or config drift.",
                "tags": ["runtime", "task-loop", "proof"],
            },
        )
        _record_tool_step(resolved, chain_id=chain_id, step_type="write_brief", skill="strategic_memory", result=write_brief)
        append_trace_step("write_brief", "strategic_memory", write_brief)
        if not write_brief.success:
            issues.append(write_brief.error or "write_brief failed")
        else:
            brief_id = write_brief.output

    if task_id is not None:
        complete_task = registry.invoke_tool(
            "research_domain_2",
            {
                "action": "complete_task",
                "task_id": task_id,
                "output_brief_id": brief_id,
                "actual_spend_usd": 0.0,
            },
        )
        _record_tool_step(resolved, chain_id=chain_id, step_type="complete_task", skill="research_domain_2", result=complete_task)
        append_trace_step("complete_task", "research_domain_2", complete_task)
        if not complete_task.success:
            issues.append(complete_task.error or "complete_task failed")

    if task_id is not None:
        route_task_output = registry.invoke_tool(
            "research_domain_2",
            {
                "action": "route_task_output",
                "task_id": task_id,
                "target_interface": "Hermes Workspace",
            },
        )
        _record_tool_step(resolved, chain_id=chain_id, step_type="route_task_output", skill="research_domain_2", result=route_task_output)
        append_trace_step("route_task_output", "research_domain_2", route_task_output)
        if not route_task_output.success:
            issues.append(route_task_output.error or "route_task_output failed")
        else:
            route_summary = {
                "action_count": len(route_task_output.output["actions"]),
                "action_types": [action["type"] for action in route_task_output.output["actions"]],
            }

    judge = registry.invoke_tool(
        "immune_system",
        {
            "action": "judge",
            "payload": JudgePayload(
                session_id=bootstrap.session_context.session_id,
                skill_name="task_loop_proof",
                tool_name="research_domain_2",
                output={"task_id": task_id, "brief_id": brief_id, "route_summary": route_summary},
                task_type="task_loop_proof",
            ),
        },
    )
    _record_tool_step(resolved, chain_id=chain_id, step_type="judge", skill="immune_system", result=judge)
    append_trace_step("judge", "immune_system", judge)
    if not judge.success:
        issues.append(judge.error or "judge failed")
    else:
        _persist_immune_verdict(resolved, judge.output, judge.duration_ms)

    doctor = doctor_runtime(registry, config=resolved, bootstrap_if_needed=False)
    trace_id = generate_uuid_v7()
    _log_execution_trace(
        resolved,
        ExecutionTrace(
            trace_id=trace_id,
            task_id=task_id or chain_id,
            role="task_loop_proof",
            skill_name="runtime",
            harness_version="task_loop_proof_v1",
            intent_goal="Validate deterministic task-loop execution and routing.",
            steps=trace_steps,
            prompt_template="task loop proof",
            context_assembled="research task lifecycle + brief completion + downstream routing",
            retrieval_queries=["runtime", "task-loop", "proof"],
            judge_verdict="PASS" if not issues and doctor.ok else "FAIL",
            judge_reasoning="Task loop proof completed." if not issues and doctor.ok else "Task loop proof exposed issues.",
            outcome_score=1.0 if not issues and doctor.ok else 0.0,
            cost_usd=0.0,
            duration_ms=sum(step.latency_ms for step in trace_steps),
            training_eligible=not issues and doctor.ok,
            retention_class="STANDARD" if not issues and doctor.ok else "FAILURE_AUDIT",
            source_chain_id=chain_id,
            source_session_id=bootstrap.session_context.session_id,
            source_trace_id=None,
            created_at=_utc_now(),
        ),
    )
    return TaskLoopProofResult(
        ok=not issues and doctor.ok,
        config=resolved,
        bootstrap=bootstrap,
        doctor=doctor,
        task_id=task_id,
        brief_id=brief_id,
        route_summary=route_summary,
        trace_id=trace_id,
        issues=issues,
    )


def run_research_cron_proof(
    *,
    config: IntegrationConfig | None = None,
    repo_root: str | None = None,
    tool_registry: HermesToolRegistry | None = None,
) -> ResearchCronProofResult:
    from skills.research_domain.skill import ResearchDomainSkill

    resolved = _normalize_runtime_layout(config or IntegrationConfig()).resolve_paths()
    root = Path(repo_root).expanduser().resolve() if repo_root else _repo_root()
    registry = tool_registry or MockHermesRuntime(data_dir=resolved.data_dir)
    install_runtime_profile(resolved, repo_root=str(root))
    bootstrap = bootstrap_runtime(registry, config=resolved, model_name="research-cron-proof")
    _ensure_research_cron_chain_definition(resolved)
    db = DatabaseManager(resolved.data_dir)
    research = ResearchDomainSkill(db)
    chain_id = f"research-cron-{generate_uuid_v7()}"
    issues: list[str] = []
    trace_steps: list[ExecutionTraceStep] = []

    def append_step(step_name: str, payload: Any) -> None:
        trace_steps.append(
            ExecutionTraceStep(
                step_index=len(trace_steps) + 1,
                tool_call=f"research_domain_2.{step_name}",
                tool_result=json.dumps(payload, sort_keys=True, default=str)[:4096],
                tool_result_file=None,
                tokens_in=0,
                tokens_out=0,
                latency_ms=0,
                model_used="research-cron-proof",
            )
        )

    standing = research.create_standing_brief(
        "Standing brief proof",
        "Scan for meaningful changes and route the resulting brief into the operator surface.",
        "0 9 * * 1",
        target_interface="Hermes Workspace",
        include_council_review=True,
        tags=["standing_brief", "proof"],
    )
    append_step("create_standing_brief", standing)
    _persist_step_outcome(
        resolved,
        chain_id=chain_id,
        step_type="create_standing_brief",
        skill="research_domain_2",
        outcome="PASS",
        latency_ms=1,
    )
    scheduled = research.schedule_standing_brief(
        standing["standing_brief_id"],
        registry,
        model="local-default",
        reference_time="2026-04-22T09:00:00+00:00",
    )
    append_step("schedule_standing_brief", scheduled)
    _persist_step_outcome(
        resolved,
        chain_id=chain_id,
        step_type="schedule_standing_brief",
        skill="research_domain_2",
        outcome="PASS",
        latency_ms=1,
    )
    queued = research.queue_standing_brief_run(
        standing["standing_brief_id"],
        reference_time="2026-04-22T09:01:00+00:00",
    )
    append_step("queue_standing_brief_run", queued)
    _persist_step_outcome(
        resolved,
        chain_id=chain_id,
        step_type="queue_standing_brief_run",
        skill="research_domain_2",
        outcome="PASS",
        latency_ms=1,
    )
    if scheduled["job_id"] not in getattr(registry, "scheduled_jobs", {}):
        issues.append("scheduled job was not retained by runtime")

    doctor = doctor_runtime(registry, config=resolved, bootstrap_if_needed=False)
    trace_id = generate_uuid_v7()
    _log_execution_trace(
        resolved,
        ExecutionTrace(
            trace_id=trace_id,
            task_id=queued["task"]["task_id"],
            role="research_cron_proof",
            skill_name="runtime",
            harness_version="research_cron_proof_v1",
            intent_goal="Validate standing-brief cron scaffolding and queued research runs.",
            steps=trace_steps,
            prompt_template="research cron proof",
            context_assembled="standing brief creation + cron scheduling + queued research task",
            retrieval_queries=["standing_brief", "cron", "proof"],
            judge_verdict="PASS" if not issues and doctor.ok else "FAIL",
            judge_reasoning="Research cron proof completed." if not issues and doctor.ok else "Research cron proof exposed issues.",
            outcome_score=1.0 if not issues and doctor.ok else 0.0,
            cost_usd=0.0,
            duration_ms=0,
            training_eligible=not issues and doctor.ok,
            retention_class="STANDARD" if not issues and doctor.ok else "FAILURE_AUDIT",
            source_chain_id=chain_id,
            source_session_id=bootstrap.session_context.session_id,
            source_trace_id=None,
            created_at=_utc_now(),
        ),
    )
    db.close_all()
    return ResearchCronProofResult(
        ok=not issues and doctor.ok,
        config=resolved,
        bootstrap=bootstrap,
        doctor=doctor,
        standing_brief_id=standing["standing_brief_id"],
        scheduled_job_id=scheduled["job_id"],
        queued_task_id=queued["task"]["task_id"],
        trace_id=trace_id,
        issues=issues,
    )


class _ProxyProbeServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class _ProxyProbeHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        payload = json.dumps({"ok": True, "path": self.path}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:
        _ = (format, args)
        return None


@contextlib.contextmanager
def _serve_proxy_probe_server() -> Any:
    server = _ProxyProbeServer(("127.0.0.1", 0), _ProxyProbeHandler)
    thread = threading.Thread(target=server.serve_forever, name="proxy-probe-upstream", daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def run_proxy_self_test(config: IntegrationConfig | None = None) -> ProxySelfTestResult:
    resolved = _normalize_runtime_layout(config or IntegrationConfig()).resolve_paths()
    install_runtime_profile(resolved)
    prepare_runtime_directories(resolved)
    migrate_runtime_databases(resolved)
    audit_log_path = _runtime_proxy_audit_log_path(resolved)
    if audit_log_path.exists():
        audit_log_path.unlink()

    issues: list[str] = []
    allowed_success = 0
    blocked_reject = 0
    proxy_url: str | None = None
    start_time = time.time()

    with _serve_proxy_probe_server() as allowed_server:
        allowed_port = int(allowed_server.server_address[1])
        blocked_port = allowed_port + 1
        proxy_config = ProxyServerConfig.from_payload(
            _proxy_config_payload(
                resolved,
                bind_port=0,
                audit_log_path=str(audit_log_path),
                allowed_domains=("localhost", "127.0.0.1"),
                allowed_ports=(allowed_port,),
            )
        )
        with start_proxy_server(proxy_config) as running_proxy:
            proxy_url = running_proxy.proxy_url
            opener = urllib_request.build_opener(
                urllib_request.ProxyHandler(
                    {
                        "http": proxy_url,
                    }
                )
            )
            for index in range(5):
                request = urllib_request.Request(f"http://127.0.0.1:{allowed_port}/allowed?probe={index + 1}")
                with opener.open(request, timeout=5) as response:
                    if response.status == 200:
                        allowed_success += 1
                    else:
                        issues.append(f"allowed request {index + 1} returned {response.status}")
            for index in range(5):
                request = urllib_request.Request(f"http://127.0.0.1:{blocked_port}/blocked?probe={index + 1}")
                try:
                    opener.open(request, timeout=5)
                    issues.append(f"blocked request {index + 1} unexpectedly succeeded")
                except urllib_error.HTTPError as exc:
                    if exc.code == 403:
                        blocked_reject += 1
                    else:
                        issues.append(f"blocked request {index + 1} returned {exc.code}")

    audit_entries: list[dict[str, Any]] = []
    if audit_log_path.is_file():
        for line in audit_log_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                audit_entries.append(json.loads(line))
    else:
        issues.append("proxy audit log was not created")

    allow_events = sum(1 for item in audit_entries if item.get("decision") == "ALLOW")
    deny_events = sum(1 for item in audit_entries if item.get("decision") == "DENY")
    if allow_events != allowed_success:
        issues.append(f"expected {allowed_success} allow audit events, found {allow_events}")
    if deny_events != blocked_reject:
        issues.append(f"expected {blocked_reject} deny audit events, found {deny_events}")

    ok = allowed_success == 5 and blocked_reject == 5 and not issues
    trace_id = generate_uuid_v7()
    _log_execution_trace(
        resolved,
        ExecutionTrace(
            trace_id=trace_id,
            task_id="proxy-self-test",
            role="proxy_self_test",
            skill_name="runtime",
            harness_version="proxy_self_test_v1",
            intent_goal="Validate local forward proxy allow/deny enforcement before live Hermes launch.",
            steps=[
                _scenario_step(
                    1,
                    "start_proxy_server",
                    {"proxy_url": proxy_url, "audit_log_path": str(audit_log_path)},
                ),
                _scenario_step(
                    2,
                    "proxy_http_validation",
                    {
                        "allowed_success": allowed_success,
                        "blocked_reject": blocked_reject,
                        "audit_entries": len(audit_entries),
                    },
                ),
            ],
            prompt_template="proxy self test",
            context_assembled=json.dumps(
                {
                    "proxy_url": proxy_url,
                    "audit_log_path": str(audit_log_path),
                },
                sort_keys=True,
            )[:2000],
            retrieval_queries=["local_forward_proxy", "audit_log", "m2_proxy_validation"],
            judge_verdict="PASS" if ok else "FAIL",
            judge_reasoning=(
                "Proxy self-test completed cleanly."
                if ok
                else "Proxy self-test exposed allow/deny or audit drift."
            ),
            outcome_score=1.0 if ok else 0.0,
            cost_usd=0.0,
            duration_ms=int((time.time() - start_time) * 1000),
            training_eligible=False,
            retention_class="STANDARD" if ok else "FAILURE_AUDIT",
            source_chain_id=None,
            source_session_id=None,
            source_trace_id=None,
            created_at=_utc_now(),
        ),
    )
    return ProxySelfTestResult(
        ok=ok,
        config=resolved,
        proxy_url=proxy_url,
        allowed_request_count=allowed_success,
        blocked_request_count=blocked_reject,
        audit_log_path=str(audit_log_path),
        trace_id=trace_id,
        issues=issues,
    )


def workspace_overview(config: IntegrationConfig | None = None) -> dict[str, Any]:
    from skills.observability.skill import ObservabilitySkill
    from skills.operator_interface.skill import OperatorInterfaceSkill

    resolved = _normalize_runtime_layout(config or IntegrationConfig()).resolve_paths()
    prepare_runtime_directories(resolved)
    migrate_runtime_databases(resolved)
    install_runtime_profile(resolved)
    db = DatabaseManager(resolved.data_dir)
    operator = OperatorInterfaceSkill(db)
    observability = ObservabilitySkill(db, telemetry_buffer=None, immune_buffer=None)
    health = observability.system_health()
    overview = {
        "runtime_status": operator.runtime_status(),
        "pending_g3_requests": operator.list_g3_approval_requests(limit=5, status="PENDING"),
        "pending_quarantines": operator.list_quarantined_responses(limit=5, pending_review_only=True),
        "execution_traces": operator.list_execution_traces(limit=5),
        "harness_frontier": operator.harness_frontier(limit=5),
        "replay_readiness": health["harness_variants"]["execution_traces"]["replay_readiness"],
        "replay_readiness_report_path": str(_runtime_replay_readiness_report_path(resolved)),
        "proxy_allowlist_path": str(_runtime_proxy_allowlist_path(resolved)),
        "proxy_audit_log_path": str(_runtime_proxy_audit_log_path(resolved)),
        "milestone_health": evaluate_milestone_status(resolved, db_manager=db),
        "workspace_manifest_path": str(_runtime_workspace_manifest_path(resolved)),
        "gateway_manifest_path": str(_runtime_gateway_manifest_path(resolved)),
    }
    db.close_all()
    return overview


def bootstrap_stack(
    *,
    config: IntegrationConfig | None = None,
    repo_root: str | None = None,
    tool_registry: HermesToolRegistry | None = None,
) -> BootstrapStackResult:
    resolved = _normalize_runtime_layout(config or IntegrationConfig()).resolve_paths()
    registry = tool_registry or MockHermesRuntime(data_dir=resolved.data_dir)
    install = install_runtime_profile(resolved, repo_root=repo_root)
    bootstrap_runtime(registry, config=resolved)
    doctor = doctor_runtime(registry, config=resolved, bootstrap_if_needed=False)
    operator_workflow = run_operator_workflow(registry, config=resolved, model_name="bootstrap-stack")
    contract_harness = exercise_hermes_contract(config=resolved, repo_root=repo_root, tool_registry=registry)
    task_loop = run_task_loop_proof(config=resolved, repo_root=repo_root, tool_registry=registry)
    research_cron = run_research_cron_proof(config=resolved, repo_root=repo_root, tool_registry=registry)
    proxy_self_test = run_proxy_self_test(config=resolved)
    milestone_status = evaluate_milestone_status(resolved)
    return BootstrapStackResult(
        ok=doctor.ok and operator_workflow.ok and contract_harness.ok and task_loop.ok and research_cron.ok and proxy_self_test.ok,
        install=install,
        doctor=doctor,
        operator_workflow=operator_workflow,
        contract_harness=contract_harness,
        task_loop_proof=task_loop,
        research_cron_proof=research_cron,
        proxy_self_test=proxy_self_test,
        milestone_status=milestone_status,
    )


def _scenario_step(step_index: int, tool_call: str, payload: Any, *, model_used: str = "evidence-factory") -> ExecutionTraceStep:
    return ExecutionTraceStep(
        step_index=step_index,
        tool_call=tool_call,
        tool_result=json.dumps(payload, sort_keys=True, default=str)[:4096],
        tool_result_file=None,
        tokens_in=0,
        tokens_out=0,
        latency_ms=0,
        model_used=model_used,
    )


def _log_evidence_scenario_trace(
    config: IntegrationConfig,
    *,
    scenario_id: str,
    cycle_index: int,
    classification: str,
    scenario_ok: bool,
    steps: list[ExecutionTraceStep],
    details: dict[str, Any],
    issues: list[str],
) -> str:
    trace_id = generate_uuid_v7()
    is_known_bad = classification == "known_bad"
    judge_verdict = "FAIL" if is_known_bad or not scenario_ok else "PASS"
    judge_reasoning = (
        "Known-bad scenario failed safely and produced audit-grade evidence."
        if is_known_bad and scenario_ok
        else ("Evidence scenario completed." if scenario_ok else "Evidence scenario exposed blocking issues.")
    )
    _log_execution_trace(
        config,
        ExecutionTrace(
            trace_id=trace_id,
            task_id=f"evidence-{scenario_id}-{cycle_index}",
            role=f"evidence_{scenario_id}",
            skill_name="runtime",
            harness_version="evidence_factory_v1",
            intent_goal=f"Evidence factory scenario: {scenario_id}",
            steps=steps,
            prompt_template=scenario_id,
            context_assembled=json.dumps(
                {
                    "classification": classification,
                    "details": details,
                    "issues": issues,
                },
                sort_keys=True,
                default=str,
            )[:2048],
            retrieval_queries=["evidence_factory", scenario_id, classification],
            judge_verdict=judge_verdict,
            judge_reasoning=judge_reasoning,
            outcome_score=1.0 if scenario_ok and not is_known_bad else 0.0,
            cost_usd=0.0,
            duration_ms=sum(step.latency_ms for step in steps),
            training_eligible=scenario_ok and not is_known_bad,
            retention_class="STANDARD" if scenario_ok and not is_known_bad else "FAILURE_AUDIT",
            source_chain_id=f"evidence-cycle-{cycle_index}",
            source_session_id=None,
            source_trace_id=None,
            created_at=_utc_now(),
        ),
    )
    return trace_id


def _run_evidence_research_to_opportunity_flow(
    config: IntegrationConfig,
    runtime: HermesToolRegistry,
    *,
    cycle_index: int,
) -> EvidenceScenarioResult:
    from skills.council.skill import configure_skill as configure_council_skill
    from skills.db_manager import DatabaseManager
    from skills.research_domain.skill import ResearchDomainSkill
    from skills.strategic_memory.skill import StrategicMemorySkill

    db = DatabaseManager(config.data_dir)
    steps: list[ExecutionTraceStep] = []
    issues: list[str] = []
    try:
        _seed_mock_council_roles(runtime)
        configure_council_skill(runtime, db)
        _seed_mock_council_synthesis(
            runtime,
            decision_type="opportunity_screen",
            recommendation="PURSUE",
            confidence=0.74,
            reasoning_summary="The brief is sufficiently corroborated to advance into validation.",
            dissenting_views="The opportunity looks real, but it still needs broader execution evidence.",
            risk_watch=["brief quality drift", "validation throughput"],
        )
        operator = db.get_connection("operator_digest")
        operator.execute(
            "INSERT INTO operator_heartbeat VALUES (?, ?, ?, ?)",
            (
                f"evidence-heartbeat-{generate_uuid_v7()}",
                "command",
                "CLI",
                _utc_now(),
            ),
        )
        operator.commit()

        research = ResearchDomainSkill(db)
        memory = StrategicMemorySkill(db)
        task_id = research.create_task(
            f"Evidence opportunity scan {cycle_index}",
            "Collect a routable brief with enough sourcing to trigger the opportunity pipeline.",
            priority="P1_HIGH",
            tags=["evidence", "opportunity", f"cycle-{cycle_index}"],
        )
        steps.append(_scenario_step(1, "research_domain.create_task", {"task_id": task_id}))
        brief_id = memory.write_brief(
            task_id,
            f"Evidence opportunity brief {cycle_index}",
            "A reusable opportunity looks viable and is sufficiently grounded to enter council review.",
            confidence=0.79,
            actionability="ACTION_RECOMMENDED",
            action_type="opportunity_feed",
            depth_tier="FULL",
            source_urls=[
                f"https://example.com/evidence/{cycle_index}",
                f"https://api.example.com/evidence/{cycle_index}",
            ],
            source_assessments=[
                {
                    "url": f"https://example.com/evidence/{cycle_index}",
                    "relevance": 0.82,
                    "freshness": "2026-04-23",
                    "source_type": "tier2_web",
                },
                {
                    "url": f"https://api.example.com/evidence/{cycle_index}",
                    "relevance": 0.91,
                    "freshness": "2026-04-23",
                    "source_type": "tier1_api",
                },
            ],
            uncertainty_statement="The market need looks real, but the exact acquisition channel mix still needs validation over a broader window.",
            counter_thesis="The opportunity could still be weaker than it appears if the visible demand is concentrated in a narrow early-adopter segment.",
            tags=["evidence", "opportunity", f"cycle-{cycle_index}"],
        )
        steps.append(_scenario_step(2, "strategic_memory.write_brief", {"brief_id": brief_id}))
        route = research.route_task_output(task_id, include_council_review=True)
        steps.append(_scenario_step(3, "research_domain.route_task_output", route))
        action_types = [action["type"] for action in route["actions"]]
        if "opportunity_created" not in action_types:
            issues.append("route_task_output did not create an opportunity")
        if "council_review_created" not in action_types:
            issues.append("route_task_output did not trigger council review")
        details = {
            "task_id": task_id,
            "brief_id": brief_id,
            "action_types": action_types,
        }
        trace_id = _log_evidence_scenario_trace(
            config,
            scenario_id="research_to_opportunity_flow",
            cycle_index=cycle_index,
            classification="activation_positive",
            scenario_ok=not issues,
            steps=steps,
            details=details,
            issues=issues,
        )
        return EvidenceScenarioResult(
            scenario_id="research_to_opportunity_flow",
            cycle_index=cycle_index,
            classification="activation_positive",
            ok=not issues,
            trace_id=trace_id,
            produced_skill_families=["research_domain", "strategic_memory", "council"],
            issues=issues,
            details=details,
        )
    finally:
        db.close_all()


def _run_evidence_opportunity_project_flow(
    config: IntegrationConfig,
    *,
    cycle_index: int,
) -> EvidenceScenarioResult:
    from skills.db_manager import DatabaseManager
    from skills.opportunity_pipeline.skill import OpportunityPipelineSkill

    db = DatabaseManager(config.data_dir)
    steps: list[ExecutionTraceStep] = []
    issues: list[str] = []
    try:
        pipeline = OpportunityPipelineSkill(db)
        opportunity_id = pipeline.create_opportunity(
            f"Evidence project opportunity {cycle_index}",
            "Convert a validated opportunity into a tracked project and preserve the learning loop.",
            income_mechanism="software_product",
            detected_by="research_prompted",
            cashflow_estimate={"low": 250, "mid": 900, "high": 1500, "currency": "USD", "period": "month"},
            provenance_links=[f"evidence-brief-{cycle_index}"],
        )
        steps.append(_scenario_step(1, "opportunity_pipeline.create_opportunity", {"opportunity_id": opportunity_id}))
        for step_index, status in enumerate(("SCREENED", "QUALIFIED", "IN_VALIDATION", "GO_NO_GO"), start=2):
            transitioned = pipeline.transition_opportunity(
                opportunity_id,
                status,
                validation_spend=0.0 if status == "IN_VALIDATION" else None,
                validation_report="Evidence batch transition." if status in {"IN_VALIDATION", "GO_NO_GO"} else None,
            )
            steps.append(
                _scenario_step(
                    step_index,
                    "opportunity_pipeline.transition_opportunity",
                    {"status": transitioned["status"], "updated_at": transitioned["updated_at"]},
                )
            )
        handoff = pipeline.handoff_to_project(opportunity_id, project_name=f"Evidence Project {cycle_index}")
        steps.append(_scenario_step(6, "opportunity_pipeline.handoff_to_project", handoff))
        closed = pipeline.close_from_project(
            handoff["project_id"],
            project_status="COMPLETE",
            learning_record={"result": "positive", "note": "Evidence batch project loop completed cleanly."},
        )
        steps.append(_scenario_step(7, "opportunity_pipeline.close_from_project", closed))
        if handoff["opportunity"]["status"] != "ACTIVE":
            issues.append("project handoff did not activate the opportunity")
        if closed["status"] != "CLOSED":
            issues.append("project closure did not backpropagate the learning record")
        details = {
            "opportunity_id": opportunity_id,
            "project_id": handoff["project_id"],
            "status": closed["status"],
        }
        trace_id = _log_evidence_scenario_trace(
            config,
            scenario_id="opportunity_project_flow",
            cycle_index=cycle_index,
            classification="activation_positive",
            scenario_ok=not issues,
            steps=steps,
            details=details,
            issues=issues,
        )
        return EvidenceScenarioResult(
            scenario_id="opportunity_project_flow",
            cycle_index=cycle_index,
            classification="activation_positive",
            ok=not issues,
            trace_id=trace_id,
            produced_skill_families=["opportunity_pipeline"],
            issues=issues,
            details=details,
        )
    finally:
        db.close_all()


def _run_evidence_invalid_brief_completion(
    config: IntegrationConfig,
    *,
    cycle_index: int,
) -> EvidenceScenarioResult:
    from skills.db_manager import DatabaseManager
    from skills.research_domain.skill import ResearchDomainSkill
    from skills.strategic_memory.skill import StrategicMemorySkill

    db = DatabaseManager(config.data_dir)
    steps: list[ExecutionTraceStep] = []
    issues: list[str] = []
    blocked = False
    error_message = ""
    try:
        research = ResearchDomainSkill(db)
        memory = StrategicMemorySkill(db)
        task_id = research.create_task(f"Evidence invalid completion {cycle_index}", "Primary task")
        other_task_id = research.create_task(f"Evidence mismatched brief {cycle_index}", "Secondary task")
        other_brief_id = memory.write_brief(other_task_id, "Mismatched brief", "This brief belongs elsewhere.")
        research.start_task(task_id)
        steps.extend(
            [
                _scenario_step(1, "research_domain.create_task", {"task_id": task_id}),
                _scenario_step(2, "research_domain.create_task", {"task_id": other_task_id}),
                _scenario_step(3, "strategic_memory.write_brief", {"brief_id": other_brief_id}),
                _scenario_step(4, "research_domain.start_task", {"task_id": task_id}),
            ]
        )
        try:
            research.complete_task(task_id, output_brief_id=other_brief_id)
            issues.append("mismatched brief completion unexpectedly succeeded")
        except ValueError as exc:
            blocked = True
            error_message = str(exc)
            steps.append(_scenario_step(5, "research_domain.complete_task", {"blocked": True, "error": error_message}))
            if "does not belong" not in error_message:
                issues.append("mismatched brief failure reason drifted")
        details = {
            "task_id": task_id,
            "other_task_id": other_task_id,
            "blocked": blocked,
            "error": error_message,
        }
        scenario_ok = blocked and not issues
        trace_id = _log_evidence_scenario_trace(
            config,
            scenario_id="invalid_brief_completion",
            cycle_index=cycle_index,
            classification="known_bad",
            scenario_ok=scenario_ok,
            steps=steps,
            details=details,
            issues=issues,
        )
        return EvidenceScenarioResult(
            scenario_id="invalid_brief_completion",
            cycle_index=cycle_index,
            classification="known_bad",
            ok=scenario_ok,
            trace_id=trace_id,
            produced_skill_families=["research_domain", "strategic_memory"],
            issues=issues,
            details=details,
        )
    finally:
        db.close_all()


def _run_evidence_archived_standing_brief_queue(
    config: IntegrationConfig,
    runtime: HermesToolRegistry,
    *,
    cycle_index: int,
) -> EvidenceScenarioResult:
    from skills.db_manager import DatabaseManager
    from skills.research_domain.skill import ResearchDomainSkill

    db = DatabaseManager(config.data_dir)
    steps: list[ExecutionTraceStep] = []
    issues: list[str] = []
    blocked = False
    error_message = ""
    try:
        research = ResearchDomainSkill(db)
        standing = research.create_standing_brief(
            f"Archived standing brief {cycle_index}",
            "This brief should never queue once archived.",
            "0 9 * * 1",
            target_interface="Hermes Workspace",
            include_council_review=False,
            tags=["evidence", "known-bad", f"cycle-{cycle_index}"],
        )
        research.schedule_standing_brief(standing["standing_brief_id"], runtime, model="local-default")
        archived = research.update_standing_brief_status(standing["standing_brief_id"], "ARCHIVED")
        steps.extend(
            [
                _scenario_step(1, "research_domain.create_standing_brief", {"standing_brief_id": standing["standing_brief_id"]}),
                _scenario_step(2, "research_domain.schedule_standing_brief", {"standing_brief_id": standing["standing_brief_id"]}),
                _scenario_step(3, "research_domain.update_standing_brief_status", {"status": archived["status"]}),
            ]
        )
        try:
            research.queue_standing_brief_run(standing["standing_brief_id"])
            issues.append("archived standing brief unexpectedly queued a run")
        except ValueError as exc:
            blocked = True
            error_message = str(exc)
            steps.append(_scenario_step(4, "research_domain.queue_standing_brief_run", {"blocked": True, "error": error_message}))
            if "not active" not in error_message:
                issues.append("archived standing brief failure reason drifted")
        details = {
            "standing_brief_id": standing["standing_brief_id"],
            "blocked": blocked,
            "error": error_message,
        }
        scenario_ok = blocked and not issues
        trace_id = _log_evidence_scenario_trace(
            config,
            scenario_id="archived_standing_brief_queue",
            cycle_index=cycle_index,
            classification="known_bad",
            scenario_ok=scenario_ok,
            steps=steps,
            details=details,
            issues=issues,
        )
        return EvidenceScenarioResult(
            scenario_id="archived_standing_brief_queue",
            cycle_index=cycle_index,
            classification="known_bad",
            ok=scenario_ok,
            trace_id=trace_id,
            produced_skill_families=["research_domain"],
            issues=issues,
            details=details,
        )
    finally:
        db.close_all()


def _run_evidence_invalid_opportunity_transition(
    config: IntegrationConfig,
    *,
    cycle_index: int,
) -> EvidenceScenarioResult:
    from skills.db_manager import DatabaseManager
    from skills.opportunity_pipeline.skill import OpportunityPipelineSkill

    db = DatabaseManager(config.data_dir)
    steps: list[ExecutionTraceStep] = []
    issues: list[str] = []
    blocked = False
    error_message = ""
    try:
        pipeline = OpportunityPipelineSkill(db)
        opportunity_id = pipeline.create_opportunity(
            f"Invalid transition opportunity {cycle_index}",
            "Force the state machine to reject an invalid jump.",
            income_mechanism="client_work",
            detected_by="research_prompted",
        )
        steps.append(_scenario_step(1, "opportunity_pipeline.create_opportunity", {"opportunity_id": opportunity_id}))
        try:
            pipeline.transition_opportunity(opportunity_id, "GO_NO_GO")
            issues.append("invalid opportunity transition unexpectedly succeeded")
        except ValueError as exc:
            blocked = True
            error_message = str(exc)
            steps.append(_scenario_step(2, "opportunity_pipeline.transition_opportunity", {"blocked": True, "error": error_message}))
            if "invalid transition" not in error_message:
                issues.append("invalid opportunity transition failure reason drifted")
        details = {
            "opportunity_id": opportunity_id,
            "blocked": blocked,
            "error": error_message,
        }
        scenario_ok = blocked and not issues
        trace_id = _log_evidence_scenario_trace(
            config,
            scenario_id="invalid_opportunity_transition",
            cycle_index=cycle_index,
            classification="known_bad",
            scenario_ok=scenario_ok,
            steps=steps,
            details=details,
            issues=issues,
        )
        return EvidenceScenarioResult(
            scenario_id="invalid_opportunity_transition",
            cycle_index=cycle_index,
            classification="known_bad",
            ok=scenario_ok,
            trace_id=trace_id,
            produced_skill_families=["opportunity_pipeline"],
            issues=issues,
            details=details,
        )
    finally:
        db.close_all()


def replay_readiness_report(
    config: IntegrationConfig | None = None,
    *,
    repo_root: str | None = None,
    limit: int = DEFAULT_REPLAY_REPORT_LIMIT,
) -> dict[str, Any]:
    resolved = _normalize_runtime_layout(config or IntegrationConfig()).resolve_paths()
    root = Path(repo_root).expanduser().resolve() if repo_root else _repo_root()
    prepare_runtime_directories(resolved)
    migrate_runtime_databases(resolved)
    install_runtime_profile(resolved, repo_root=str(root))
    manager = HarnessVariantManager(str(Path(resolved.data_dir) / "telemetry.db"))
    report = manager.replay_readiness_report(limit=limit)
    payload = {
        **report,
        "artifact_path": str(_runtime_replay_readiness_report_path(resolved)),
        "profile_manifest_path": str(_runtime_profile_manifest_path(resolved)),
        "workspace_manifest_path": str(_runtime_workspace_manifest_path(resolved)),
        "evidence_factory_manifest_path": str(_runtime_evidence_factory_manifest_path(resolved)),
    }
    _write_replay_readiness_report_artifact(resolved, payload)
    return payload


def _evidence_progress_projection(
    before_report: dict[str, Any],
    after_report: dict[str, Any],
    *,
    executed_cycles: int,
) -> dict[str, Any]:
    metrics = (
        ("eligible_source_traces", "minimum_eligible_traces"),
        ("known_bad_source_traces", "minimum_known_bad_traces"),
        ("distinct_skill_count", "minimum_distinct_skills"),
    )
    projections: list[dict[str, Any]] = []
    estimate_candidates: list[int] = []
    blocked_metrics: list[str] = []
    safe_cycles = max(executed_cycles, 1)
    for current_key, minimum_key in metrics:
        before_value = int(before_report.get(current_key, 0) or 0)
        after_value = int(after_report.get(current_key, 0) or 0)
        minimum = int(after_report.get(minimum_key, 0) or 0)
        delta = after_value - before_value
        remaining = max(0, minimum - after_value)
        per_cycle_growth = delta / safe_cycles
        if remaining == 0:
            estimate = 0
        elif delta <= 0:
            estimate = None
            blocked_metrics.append(current_key)
        else:
            estimate = int(math.ceil(remaining / per_cycle_growth))
            estimate_candidates.append(estimate)
        projections.append(
            {
                "metric": current_key,
                "before": before_value,
                "after": after_value,
                "minimum": minimum,
                "delta": delta,
                "per_cycle_growth": round(per_cycle_growth, 4),
                "remaining": remaining,
                "estimated_cycles_remaining": estimate,
            }
        )
    ready = after_report.get("status") == "READY_FOR_BROADER_REPLAY"
    return {
        "ready_for_broader_replay": ready,
        "executed_cycles": executed_cycles,
        "estimated_cycles_to_threshold": 0 if ready else (max(estimate_candidates) if not blocked_metrics and projections else None),
        "blocked_metrics_without_growth": blocked_metrics,
        "metrics": projections,
    }


def run_evidence_factory(
    *,
    config: IntegrationConfig | None = None,
    repo_root: str | None = None,
    tool_registry: HermesToolRegistry | None = None,
    cycles: int = DEFAULT_EVIDENCE_CYCLES,
    report_limit: int = DEFAULT_REPLAY_REPORT_LIMIT,
    until_replay_ready: bool = False,
) -> EvidenceBatchResult:
    if cycles <= 0:
        raise ValueError("cycles must be positive")
    resolved = _normalize_runtime_layout(config or IntegrationConfig()).resolve_paths()
    root = Path(repo_root).expanduser().resolve() if repo_root else _repo_root()
    registry = tool_registry or MockHermesRuntime(data_dir=resolved.data_dir)
    install_runtime_profile(resolved, repo_root=str(root))
    bootstrap = bootstrap_runtime(registry, config=resolved, model_name="evidence-factory")
    doctor = doctor_runtime(registry, config=resolved, bootstrap_if_needed=False)
    _seed_mock_council_roles(registry)

    manager = HarnessVariantManager(str(Path(resolved.data_dir) / "telemetry.db"))
    before_summary = manager.execution_trace_summary()
    before_report = manager.replay_readiness_report(limit=report_limit)

    scenario_results: list[EvidenceScenarioResult] = []
    executed_cycles = 0
    stopped_reason = "completed_requested_cycles"
    if until_replay_ready and before_report.get("status") == "READY_FOR_BROADER_REPLAY":
        stopped_reason = "already_ready"

    for cycle_index in range(1, cycles + 1):
        if until_replay_ready and before_report.get("status") == "READY_FOR_BROADER_REPLAY":
            break
        executed_cycles += 1
        operator = run_operator_workflow(
            registry,
            config=resolved,
            model_name="evidence-factory",
            task_id=f"evidence-operator-workflow-{cycle_index}",
            title=f"Evidence operator workflow {cycle_index}",
            summary="Drive the repo-local operator workflow to generate activation-relevant multi-skill evidence.",
        )
        scenario_results.append(
            EvidenceScenarioResult(
                scenario_id="operator_workflow",
                cycle_index=cycle_index,
                classification="activation_positive",
                ok=operator.ok,
                trace_id=operator.trace_id,
                produced_skill_families=["runtime", "financial_router", "immune_system", "strategic_memory", "council", "operator_interface"],
                issues=[] if operator.ok else [operator.error or "operator workflow failed"],
                details={
                    "routing_tier": operator.routing_tier,
                    "phase_gate_verdict": operator.phase_gate_verdict,
                    "digest_id": operator.digest_id,
                },
            )
        )

        task_loop = run_task_loop_proof(config=resolved, repo_root=str(root), tool_registry=registry)
        scenario_results.append(
            EvidenceScenarioResult(
                scenario_id="task_loop_proof",
                cycle_index=cycle_index,
                classification="activation_positive",
                ok=task_loop.ok,
                trace_id=task_loop.trace_id,
                produced_skill_families=["runtime", "research_domain", "strategic_memory", "immune_system"],
                issues=list(task_loop.issues),
                details={
                    "task_id": task_loop.task_id,
                    "brief_id": task_loop.brief_id,
                    "route_summary": task_loop.route_summary,
                },
            )
        )

        research_cron = run_research_cron_proof(config=resolved, repo_root=str(root), tool_registry=registry)
        scenario_results.append(
            EvidenceScenarioResult(
                scenario_id="research_cron_proof",
                cycle_index=cycle_index,
                classification="activation_positive",
                ok=research_cron.ok,
                trace_id=research_cron.trace_id,
                produced_skill_families=["runtime", "research_domain"],
                issues=list(research_cron.issues),
                details={
                    "standing_brief_id": research_cron.standing_brief_id,
                    "queued_task_id": research_cron.queued_task_id,
                },
            )
        )

        scenario_results.append(_run_evidence_research_to_opportunity_flow(resolved, registry, cycle_index=cycle_index))
        scenario_results.append(_run_evidence_opportunity_project_flow(resolved, cycle_index=cycle_index))
        scenario_results.append(_run_evidence_invalid_brief_completion(resolved, cycle_index=cycle_index))
        scenario_results.append(_run_evidence_archived_standing_brief_queue(resolved, registry, cycle_index=cycle_index))
        scenario_results.append(_run_evidence_invalid_opportunity_transition(resolved, cycle_index=cycle_index))
        if until_replay_ready:
            current_report = manager.replay_readiness_report(limit=report_limit)
            if current_report.get("status") == "READY_FOR_BROADER_REPLAY":
                stopped_reason = "replay_ready_reached"
                break

    if until_replay_ready and stopped_reason not in {"already_ready", "replay_ready_reached"}:
        stopped_reason = "max_cycles_reached"

    after_summary = manager.execution_trace_summary()
    after_report = replay_readiness_report(config=resolved, repo_root=str(root), limit=report_limit)
    progress_projection = _evidence_progress_projection(before_report, after_report, executed_cycles=executed_cycles)
    generated_trace_count = int(after_summary["total_count"]) - int(before_summary["total_count"])
    generated_source_trace_count = int(after_summary["source_trace_count"]) - int(before_summary["source_trace_count"])
    generated_activation_trace_count = int(after_report["activation_source_trace_count"]) - int(before_report["activation_source_trace_count"])
    generated_known_bad_trace_count = int(after_report["known_bad_source_traces"]) - int(before_report["known_bad_source_traces"])
    result = EvidenceBatchResult(
        ok=bootstrap.ok and doctor.ok and all(item.ok for item in scenario_results),
        config=resolved,
        bootstrap=bootstrap,
        doctor=doctor,
        requested_cycles=cycles,
        cycles=executed_cycles,
        until_replay_ready=until_replay_ready,
        stopped_reason=stopped_reason,
        scenario_results=scenario_results,
        generated_trace_count=generated_trace_count,
        generated_source_trace_count=generated_source_trace_count,
        generated_activation_trace_count=generated_activation_trace_count,
        generated_known_bad_trace_count=generated_known_bad_trace_count,
        before_replay_report=before_report,
        replay_report=after_report,
        progress_projection=progress_projection,
        report_path=str(_runtime_replay_readiness_report_path(resolved)),
    )

    evidence_manifest = _read_json_yaml(_runtime_evidence_factory_manifest_path(resolved)) or {}
    evidence_manifest["last_run"] = {
        "generated_at": _utc_now(),
        "requested_cycles": cycles,
        "executed_cycles": executed_cycles,
        "until_replay_ready": until_replay_ready,
        "stopped_reason": stopped_reason,
        "scenario_count": len(scenario_results),
        "scenario_passed": sum(1 for item in scenario_results if item.ok),
        "generated_trace_count": generated_trace_count,
        "generated_activation_trace_count": generated_activation_trace_count,
        "generated_known_bad_trace_count": generated_known_bad_trace_count,
        "progress_projection": progress_projection,
        "report_path": result.report_path,
    }
    _write_json_yaml(_runtime_evidence_factory_manifest_path(resolved), evidence_manifest)
    return result


def build_mac_studio_day_one_handoff(
    *,
    config: IntegrationConfig | None = None,
    repo_root: str | None = None,
    tool_registry: HermesToolRegistry | None = None,
    cycles: int = DEFAULT_EVIDENCE_CYCLES,
    report_limit: int = DEFAULT_REPLAY_REPORT_LIMIT,
) -> MacStudioDayOneResult:
    resolved = _normalize_runtime_layout(config or IntegrationConfig()).resolve_paths()
    root = Path(repo_root).expanduser().resolve() if repo_root else _repo_root()
    registry = tool_registry or MockHermesRuntime(data_dir=resolved.data_dir)
    install = install_runtime_profile(resolved, repo_root=str(root))
    stack = bootstrap_stack(config=resolved, repo_root=str(root), tool_registry=registry)
    doctor = stack.doctor
    evidence = run_evidence_factory(
        config=resolved,
        repo_root=str(root),
        tool_registry=registry,
        cycles=cycles,
        report_limit=report_limit,
    )
    report = replay_readiness_report(config=resolved, repo_root=str(root), limit=report_limit)
    issues: list[str] = []
    if not doctor.ok:
        issues.append("doctor failed")
    if not stack.ok:
        issues.append("bootstrap stack failed")
    if not evidence.ok:
        issues.append("evidence batch failed")
    _write_mac_studio_day_one_handoff(
        resolved,
        root,
        install=install,
        evidence_batch=evidence,
        bootstrap_stack_result=stack,
        replay_report=report,
    )
    return MacStudioDayOneResult(
        ok=not issues,
        install=install,
        doctor=doctor,
        bootstrap_stack=stack,
        evidence_batch=evidence,
        replay_report=report,
        handoff_path=str(_runtime_mac_studio_day_one_handoff_path(resolved)),
        issues=issues,
    )


def assess_hermes_readiness(
    *,
    config: IntegrationConfig | None = None,
    repo_root: str | None = None,
    hermes_binary: str = "hermes",
    run_cli_smoke: bool = True,
    smoke_query: str | None = None,
    command_runner: Callable[[Sequence[str]], ExternalCommandResult] | None = None,
    tool_registry: HermesToolRegistry | None = None,
) -> HermesReadinessResult:
    """Prepare repo-managed runtime artifacts and verify live Hermes readiness."""
    resolved = _normalize_runtime_layout(config or IntegrationConfig()).resolve_paths()
    install = install_runtime_profile(resolved, repo_root=repo_root)
    repo_root_path = Path(install.repo_root)
    database_status = migrate_runtime_databases(resolved)
    registry = tool_registry or MockHermesRuntime(data_dir=resolved.data_dir)
    contract = HermesProfileContract(config=resolved, repo_root=str(repo_root_path))
    contract_harness = exercise_hermes_contract(
        config=resolved,
        repo_root=str(repo_root_path),
        tool_registry=registry,
    )
    doctor = doctor_runtime(registry, config=resolved, bootstrap_if_needed=False)
    checkpoint_backup_path = _snapshot_runtime_data(resolved)
    profile_validation = _validate_profile_artifacts(resolved, repo_root_path)

    profile_dir = _runtime_profile_dir(resolved)
    profile_config = _runtime_profile_config_path(resolved)
    spec_profile = _runtime_spec_profile_path(resolved)
    logs_dir = _runtime_logs_dir(resolved)
    path_status = {
        "data_dir": Path(resolved.data_dir).is_dir(),
        "skills_dir": Path(resolved.skills_dir).is_dir(),
        "checkpoints_dir": Path(resolved.checkpoints_dir).is_dir(),
        "alerts_dir": Path(resolved.alerts_dir).is_dir(),
        "runtime_bundle": _runtime_bundle_dir(resolved).is_dir(),
        "profile_manifest": _runtime_profile_manifest_path(resolved).is_file(),
        "profile_dir": profile_dir.is_dir(),
        "profile_config": profile_config.is_file(),
        "spec_profile": spec_profile.is_file(),
        "logs_dir": logs_dir.is_dir(),
    }
    legacy_database_files = sorted(
        path.name
        for path in Path(resolved.data_dir).glob("*.db")
        if path.name in LEGACY_SPLIT_DATABASES
    )

    blocking_items: list[str] = []
    drift_items = [VERSION_DRIFT_NOTE, PROFILE_DRIFT_NOTE, CONFIG_SURFACE_UNCERTAINTY_NOTE]
    missing_paths = [name for name, ok in path_status.items() if not ok]
    if missing_paths:
        blocking_items.append(f"missing expected Hermes paths: {', '.join(missing_paths)}")
    if not profile_validation.ok:
        blocking_items.append(
            "generated Hermes profile validation failed: "
            f"{', '.join(profile_validation.issues)}"
        )
    failed_databases = [name for name, ok in database_status.items() if not ok]
    if failed_databases:
        blocking_items.append(f"schema deployment/verification failed for: {', '.join(failed_databases)}")
    if legacy_database_files:
        blocking_items.append(f"legacy split databases present: {', '.join(legacy_database_files)}")
    if not doctor.ok:
        blocking_items.append(
            "install-profile/doctor compatibility failed: "
            f"{', '.join(doctor.missing_items) if doctor.missing_items else 'unknown doctor error'}"
        )
    if not contract_harness.ok:
        blocking_items.append(
            "repo-local Hermes contract harness failed: "
            f"{', '.join(contract_harness.issues)}"
        )

    runner = command_runner or _run_external_command
    hermes_installed = shutil.which(hermes_binary) is not None
    hermes_version: str | None = None
    hermes_version_ok = False
    profile_listed = False
    live_tools: list[str] = []
    seed_tool_status = {tool_name: False for tool_name in EXPECTED_SEED_TOOLS}
    config_status = dict(contract_harness.contract_checks)
    cli_smoke_attempted = False
    cli_smoke_ok = False
    cli_smoke_marker: str | None = None
    cli_smoke_step_outcomes_delta = 0
    cli_smoke_log_trace = False
    cli_smoke_output: str | None = None

    if not hermes_installed:
        blocking_items.append(
            f"Hermes CLI '{hermes_binary}' not found in PATH; install Hermes Agent before live readiness can pass."
        )
    else:
        version_result = runner((hermes_binary, "--version"))
        if not version_result.ok:
            blocking_items.append(f"`{hermes_binary} --version` failed: {_format_probe_failure(version_result)}")
        else:
            parsed_version = _parse_semver(version_result.stdout or version_result.stderr)
            if parsed_version is None:
                blocking_items.append(
                    f"Could not parse Hermes version from `{hermes_binary} --version`: "
                    f"{version_result.stdout or version_result.stderr or 'empty output'}"
                )
            else:
                hermes_version = ".".join(str(part) for part in parsed_version)
                hermes_version_ok = parsed_version >= MANIFEST_HERMES_VERSION_FLOOR
                if parsed_version < MANIFEST_HERMES_VERSION_FLOOR:
                    blocking_items.append(
                        f"Hermes {hermes_version} is below the manifest floor "
                        f"{'.'.join(str(part) for part in MANIFEST_HERMES_VERSION_FLOOR)}; "
                        "older v0.8/v0.9 assumptions are now stale."
                    )
                elif parsed_version < CHECKLIST_HERMES_VERSION_FLOOR:
                    blocking_items.append(
                        f"Hermes {hermes_version} is below the checklist floor "
                        f"{'.'.join(str(part) for part in CHECKLIST_HERMES_VERSION_FLOOR)}."
                    )

        profiles_result = runner((hermes_binary, "profile", "list"))
        if not profiles_result.ok:
            blocking_items.append(
                f"`{hermes_binary} profile list` failed: {_format_probe_failure(profiles_result)}"
            )
        else:
            profile_listed = resolved.profile_name in profiles_result.stdout
            if not profile_listed:
                blocking_items.append(
                    f"Hermes profile `{resolved.profile_name}` is not listed by `{hermes_binary} profile list`."
                )

        tools_result = runner((hermes_binary, "tools", "list"))
        if not tools_result.ok:
            blocking_items.append(f"`{hermes_binary} tools list` failed: {_format_probe_failure(tools_result)}")
        else:
            live_tools = _extract_named_entries(tools_result.stdout)
            for tool_name in EXPECTED_SEED_TOOLS:
                seed_tool_status[tool_name] = tool_name in live_tools
            missing_seed_tools = [name for name, ok in seed_tool_status.items() if not ok]
            if missing_seed_tools:
                blocking_items.append(
                    f"missing Hermes seed tools: {', '.join(missing_seed_tools)}"
                )

        config_result = _run_command_candidates(
            runner,
            [
                (hermes_binary, "--profile", resolved.profile_name, "config", "show"),
                (hermes_binary, "-p", resolved.profile_name, "config", "show"),
                (hermes_binary, "--profile", resolved.profile_name, "config"),
                (hermes_binary, "-p", resolved.profile_name, "config"),
                (hermes_binary, "config", "show"),
                (hermes_binary, "config"),
            ],
        )
        if not config_result.ok:
            blocking_items.append(f"Could not probe Hermes config surface: {_format_probe_failure(config_result)}")
        elif not (config_result.stdout or config_result.stderr):
            blocking_items.append("Hermes config probe returned empty output.")
        else:
            parsed_config = None
            try:
                parsed_config = json.loads(config_result.stdout or config_result.stderr)
            except json.JSONDecodeError:
                blocking_items.append("Hermes config probe output was not parseable as structured JSON/YAML.")
            else:
                if not isinstance(parsed_config, dict):
                    blocking_items.append("Hermes config probe returned a non-object payload.")
                    parsed_config = None
            if parsed_config is not None:
                config_status = contract.live_config_checks(parsed_config)
        failed_config_checks = [name for name, ok in config_status.items() if not ok]
        if failed_config_checks:
            blocking_items.append(
                "profile/config contract assertions failed: "
                f"{', '.join(failed_config_checks)}"
            )

        if run_cli_smoke:
            cli_smoke_attempted = True
            cli_smoke_marker = f"hermes-readiness-{generate_uuid_v7()}"
            query = smoke_query or _default_cli_smoke_query(cli_smoke_marker)
            step_count_before = _step_outcome_count(resolved)
            log_state_before = _capture_log_state(logs_dir)
            smoke_result = _run_command_candidates(
                runner,
                [
                    (hermes_binary, "--profile", resolved.profile_name, "chat", "-q", query),
                    (hermes_binary, "-p", resolved.profile_name, "chat", "-q", query),
                    (hermes_binary, "chat", "-q", query),
                ],
            )
            cli_smoke_output = smoke_result.stdout or smoke_result.stderr or smoke_result.error
            if not smoke_result.ok:
                blocking_items.append(
                    f"CLI smoke command failed: {_format_probe_failure(smoke_result)}"
                )
            else:
                time.sleep(0.05)
                step_count_after = _step_outcome_count(resolved)
                cli_smoke_step_outcomes_delta = max(0, step_count_after - step_count_before)
                cli_smoke_log_trace = _log_trace_present(logs_dir, log_state_before, cli_smoke_marker)
                missing_cli_evidence: list[str] = []
                if cli_smoke_step_outcomes_delta < 1:
                    missing_cli_evidence.append("STEP_OUTCOME evidence")
                if not cli_smoke_log_trace:
                    missing_cli_evidence.append("log trace evidence")
                cli_smoke_ok = not missing_cli_evidence
                if missing_cli_evidence:
                    blocking_items.append(
                        "CLI smoke did not produce "
                        f"{' and '.join(missing_cli_evidence)}."
                    )

    return HermesReadinessResult(
        ok=not blocking_items,
        config=resolved,
        hermes_installed=hermes_installed,
        hermes_version=hermes_version,
        hermes_version_ok=hermes_version_ok,
        profile_listed=profile_listed,
        live_tools=live_tools,
        seed_tool_status=seed_tool_status,
        config_status=config_status,
        profile_validation=profile_validation,
        path_status=path_status,
        database_status=database_status,
        legacy_database_files=legacy_database_files,
        cli_smoke_attempted=cli_smoke_attempted,
        cli_smoke_ok=cli_smoke_ok,
        cli_smoke_marker=cli_smoke_marker,
        cli_smoke_step_outcomes_delta=cli_smoke_step_outcomes_delta,
        cli_smoke_log_trace=cli_smoke_log_trace,
        cli_smoke_output=cli_smoke_output,
        checkpoint_backup_path=checkpoint_backup_path,
        blocking_items=blocking_items,
        drift_items=drift_items,
        install=install,
        doctor=doctor,
        contract_harness=contract_harness,
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
    trace_steps: list[ExecutionTraceStep] = []
    logged_trace_id: str | None = None

    def append_trace_step(step_name: str, skill_name: str, result: Any) -> None:
        payload = {
            "success": bool(getattr(result, "success", False)),
            "error": getattr(result, "error", None),
            "output": getattr(result, "output", None),
        }
        trace_steps.append(
            ExecutionTraceStep(
                step_index=len(trace_steps) + 1,
                tool_call=f"{skill_name}.{step_name}",
                tool_result=json.dumps(payload, sort_keys=True, default=str)[:4096],
                tool_result_file=None,
                tokens_in=0,
                tokens_out=0,
                latency_ms=int(getattr(result, "duration_ms", 0) or 0),
                model_used=model_name,
            )
        )

    def persist_trace(*, ok: bool, error: str | None) -> str:
        nonlocal logged_trace_id
        if logged_trace_id is not None:
            return logged_trace_id
        logged_trace_id = generate_uuid_v7()
        _log_execution_trace(
            resolved,
            ExecutionTrace(
                trace_id=logged_trace_id,
                task_id=task_id,
                role="operator_workflow",
                skill_name="runtime",
                harness_version="operator_workflow_v1",
                intent_goal=title,
                steps=list(trace_steps),
                prompt_template="deterministic operator workflow smoke test",
                context_assembled=summary,
                retrieval_queries=[],
                judge_verdict="PASS" if ok else "FAIL",
                judge_reasoning="Operator workflow completed." if ok else (error or "Operator workflow failed."),
                outcome_score=1.0 if ok else 0.0,
                cost_usd=0.0,
                duration_ms=sum(step.latency_ms for step in trace_steps),
                training_eligible=ok,
                retention_class="STANDARD" if ok else "FAILURE_AUDIT",
                source_chain_id=chain_id,
                source_session_id=session_id,
                source_trace_id=None,
                created_at=_utc_now(),
            ),
        )
        return logged_trace_id

    def _fail(error: str | None, *, observability: WorkflowObservabilitySnapshot | None = None) -> OperatorWorkflowResult:
        trace_id = persist_trace(ok=False, error=error)
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
            trace_id=trace_id,
            error=error,
        )

    runtime_control = RuntimeControlManager(str(Path(resolved.data_dir) / "operator_digest.db"))
    runtime_status = runtime_control.status()
    if runtime_status["lifecycle_state"] == "HALTED":
        active_halt = runtime_status["active_halt"]
        return _fail(
            "runtime halted before workflow execution"
            if active_halt is None
            else (
                "runtime halted before workflow execution: "
                f"{active_halt['source']} ({active_halt['halt_reason']})"
            )
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
    append_trace_step("record_heartbeat", "operator_interface", heartbeat)
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
    append_trace_step("sheriff", "immune_system", sheriff)
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
    append_trace_step("route", "financial_router", route)
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
    append_trace_step("create_task", "research_domain_2", opportunity_task)
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
    append_trace_step("write_brief", "strategic_memory", write_result)
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
    append_trace_step("read_brief", "strategic_memory", read_result)
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
    append_trace_step("complete_task", "research_domain_2", complete_opportunity_task)
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
    append_trace_step("route_task_output", "research_domain_2", route_opportunity_brief)
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
    append_trace_step("create_task", "research_domain_2", harvest_task)
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
    append_trace_step("write_brief", "strategic_memory", harvest_brief)
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
    append_trace_step("complete_task", "research_domain_2", complete_harvest_task)
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
    append_trace_step("route_task_output", "research_domain_2", route_harvest_brief)
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
    append_trace_step("transition_opportunity", "opportunity_pipeline", transition_validation)
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
    append_trace_step("transition_opportunity", "opportunity_pipeline", transition_go_no_go)
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
    append_trace_step("handoff_to_project", "opportunity_pipeline", project_handoff)
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
    append_trace_step("trigger_phase_gate", "opportunity_pipeline", phase_gate_trigger)
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
    append_trace_step("deliberate", "council", phase_gate_council)
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
    append_trace_step("apply_phase_gate_verdict", "opportunity_pipeline", apply_phase_gate)
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
    append_trace_step("judge", "immune_system", judge)
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
    append_trace_step("alert", "operator_interface", alert_result)
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
    append_trace_step("generate_digest", "operator_interface", digest_result)
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
    for step_name, result in (
        ("query_alert_history", observability_alerts),
        ("query_council_verdicts", observability_council),
        ("recent_digests", observability_digests),
        ("query_immune_verdicts", observability_immune),
        ("query_telemetry", observability_telemetry),
        ("reliability_dashboard", observability_reliability),
        ("system_health", observability_health),
    ):
        append_trace_step(step_name, "observability", result)
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
    trace_id = persist_trace(ok=doctor.ok, error=None if doctor.ok else "doctor reported missing runtime components")
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
        trace_id=trace_id,
        error=None if doctor.ok else "doctor reported missing runtime components",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and smoke-test the Hermes integration bootstrap")
    parser.add_argument("--bootstrap-live", action="store_true", help="Bootstrap the runtime against the selected registry")
    parser.add_argument("--bootstrap-stack", action="store_true", help="Install the runtime bundle, run proofs, and print milestone status")
    parser.add_argument("--install-profile", action="store_true", help="Install a local Hermes runtime profile bundle")
    parser.add_argument("--doctor", action="store_true", help="Verify runtime layout, databases, and skill registration")
    parser.add_argument("--readiness", action="store_true", help="Run the real-Hermes readiness checklist against the selected runtime paths")
    parser.add_argument("--proxy-self-test", action="store_true", help="Run the standalone local forward proxy allow/deny self-test")
    parser.add_argument("--operator-workflow", action="store_true", help="Run the operator workflow plus council-backed project smoke test")
    parser.add_argument("--contract-harness", action="store_true", help="Run the repo-local Hermes contract harness without requiring live Hermes")
    parser.add_argument("--task-loop-proof", action="store_true", help="Run the deterministic research task-loop proof")
    parser.add_argument("--research-cron-proof", action="store_true", help="Run the standing-brief cron proof")
    parser.add_argument("--evidence-factory", action="store_true", help="Run the production evidence batch across positive and known-bad scenarios")
    parser.add_argument("--replay-readiness-report", action="store_true", help="Print the detailed replay-readiness coverage report")
    parser.add_argument("--mac-studio-day-one", action="store_true", help="Generate the one-command Mac Studio rehearsal and handoff package")
    parser.add_argument("--milestone-status", action="store_true", help="Print machine-readable milestone build/proof status")
    parser.add_argument("--workspace-overview", action="store_true", help="Print a Hermes Workspace-oriented operator snapshot")
    parser.add_argument("--operator-checklist", action="store_true", help="Print the operator validation checklist path")
    parser.add_argument("--data-dir", default="~/.hermes/data/")
    parser.add_argument("--skills-dir", default="~/.hermes/skills/hybrid-autonomous-ai/")
    parser.add_argument("--checkpoints-dir", default="~/.hermes/skills/hybrid-autonomous-ai/checkpoints/")
    parser.add_argument("--alerts-dir", default="~/.hermes/alerts/")
    parser.add_argument("--profile-name", default="hybrid-autonomous-ai")
    parser.add_argument("--hermes-bin", default="hermes", help="Override the Hermes CLI binary used for readiness checks")
    parser.add_argument("--skip-cli-smoke", action="store_true", help="Skip the live Hermes chat smoke test inside --readiness")
    parser.add_argument("--smoke-query", default=None, help="Override the readiness chat prompt used by --readiness")
    parser.add_argument("--model-name", default="local-default")
    parser.add_argument("--repo-root", default=None, help="Override the repository root used for profile installation")
    parser.add_argument("--task-id", default="stage0-operator-workflow")
    parser.add_argument("--title", default="Operator workflow smoke test")
    parser.add_argument("--evidence-cycles", type=int, default=DEFAULT_EVIDENCE_CYCLES, help="How many times to run the evidence scenario suite")
    parser.add_argument("--until-replay-ready", action="store_true", help="For --evidence-factory, stop early if broader replay readiness is reached")
    parser.add_argument("--report-limit", type=int, default=DEFAULT_REPLAY_REPORT_LIMIT, help="How many coverage rows to include in replay-readiness reports")
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
    try:
        return _main_impl(args, parser, config, runtime)
    except RuntimeError as exc:
        print(f"runtime setup failed: {exc}")
        return 1


def _main_impl(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
    config: IntegrationConfig,
    runtime: MockHermesRuntime,
) -> int:

    if args.install_profile:
        result = install_runtime_profile(config, repo_root=args.repo_root)
        print(f"profile manifest={result.profile_manifest_path}")
        print(f"profile_dir={result.profile_dir}")
        print(f"profile_config={result.profile_config_path}")
        print(f"spec_profile={result.spec_profile_path}")
        print(f"launchers={','.join(sorted(result.launcher_paths.values()))}")
        print(f"linked_skills={len(result.linked_skill_paths)}")
        return 0

    if args.operator_checklist:
        result = install_runtime_profile(config, repo_root=args.repo_root)
        print(f"operator_validation_checklist={_runtime_operator_validation_checklist_path(result.config)}")
        return 0

    if args.doctor:
        result = doctor_runtime(runtime, config=config)
        print("doctor ok" if result.ok else "doctor failed")
        print(f"missing={','.join(result.missing_items) if result.missing_items else 'none'}")
        print(f"tools={','.join(result.registered_tools)}")
        return 0 if result.ok else 1

    if args.readiness:
        result = assess_hermes_readiness(
            config=config,
            repo_root=args.repo_root,
            hermes_binary=args.hermes_bin,
            run_cli_smoke=not args.skip_cli_smoke,
            smoke_query=args.smoke_query,
        )
        missing_paths = [name for name, ok in result.path_status.items() if not ok]
        missing_seed_tools = [name for name, ok in result.seed_tool_status.items() if not ok]
        failed_config_checks = [name for name, ok in result.config_status.items() if not ok]
        print("readiness ok" if result.ok else "readiness failed")
        print(f"hermes_installed={'yes' if result.hermes_installed else 'no'}")
        print(f"hermes_version={result.hermes_version or 'missing'}")
        print(f"profile_listed={'yes' if result.profile_listed else 'no'}")
        print(f"path_missing={','.join(missing_paths) if missing_paths else 'none'}")
        print(f"seed_tools_missing={','.join(missing_seed_tools) if missing_seed_tools else 'none'}")
        print(f"config_failed={','.join(failed_config_checks) if failed_config_checks else 'none'}")
        print(f"cli_smoke={'ok' if result.cli_smoke_ok else ('skipped' if not result.cli_smoke_attempted else 'failed')}")
        print(f"cli_step_outcomes_delta={result.cli_smoke_step_outcomes_delta}")
        print(f"cli_log_trace={'yes' if result.cli_smoke_log_trace else 'no'}")
        print(f"checkpoint_backup={result.checkpoint_backup_path or 'none'}")
        print(f"legacy_db_files={','.join(result.legacy_database_files) if result.legacy_database_files else 'none'}")
        print(f"doctor_missing={','.join(result.doctor.missing_items) if result.doctor.missing_items else 'none'}")
        print(f"contract_harness={'ok' if result.contract_harness.ok else 'failed'}")
        print(f"contract_trace_id={result.contract_harness.trace_id or 'none'}")
        print(f"blocking={'; '.join(result.blocking_items) if result.blocking_items else 'none'}")
        print(f"drift={'; '.join(result.drift_items) if result.drift_items else 'none'}")
        print(f"tools={','.join(result.live_tools) if result.live_tools else 'none'}")
        return 0 if result.ok else 1

    if args.proxy_self_test:
        result = run_proxy_self_test(config=config)
        print("proxy self-test ok" if result.ok else "proxy self-test failed")
        print(f"proxy_url={result.proxy_url or 'none'}")
        print(f"allowed={result.allowed_request_count}")
        print(f"blocked={result.blocked_request_count}")
        print(f"audit_log={result.audit_log_path}")
        print(f"trace_id={result.trace_id or 'none'}")
        print(f"issues={'; '.join(result.issues) if result.issues else 'none'}")
        return 0 if result.ok else 1

    if args.contract_harness:
        result = exercise_hermes_contract(
            config=config,
            repo_root=args.repo_root,
            tool_registry=runtime,
        )
        print("contract harness ok" if result.ok else "contract harness failed")
        print(f"trace_id={result.trace_id or 'none'}")
        print(f"blocked_pre_dispatch={'yes' if result.blocked_dispatch_pre_side_effect else 'no'}")
        print(f"issues={'; '.join(result.issues) if result.issues else 'none'}")
        return 0 if result.ok else 1

    if args.task_loop_proof:
        result = run_task_loop_proof(
            config=config,
            repo_root=args.repo_root,
            tool_registry=runtime,
        )
        print("task loop proof ok" if result.ok else "task loop proof failed")
        print(f"task_id={result.task_id or 'none'}")
        print(f"brief_id={result.brief_id or 'none'}")
        print(f"trace_id={result.trace_id or 'none'}")
        print(f"issues={'; '.join(result.issues) if result.issues else 'none'}")
        return 0 if result.ok else 1

    if args.research_cron_proof:
        result = run_research_cron_proof(
            config=config,
            repo_root=args.repo_root,
            tool_registry=runtime,
        )
        print("research cron proof ok" if result.ok else "research cron proof failed")
        print(f"standing_brief_id={result.standing_brief_id or 'none'}")
        print(f"scheduled_job_id={result.scheduled_job_id or 'none'}")
        print(f"queued_task_id={result.queued_task_id or 'none'}")
        print(f"trace_id={result.trace_id or 'none'}")
        print(f"issues={'; '.join(result.issues) if result.issues else 'none'}")
        return 0 if result.ok else 1

    if args.evidence_factory:
        result = run_evidence_factory(
            config=config,
            repo_root=args.repo_root,
            tool_registry=runtime,
            cycles=args.evidence_cycles,
            report_limit=args.report_limit,
            until_replay_ready=args.until_replay_ready,
        )
        passed = sum(1 for item in result.scenario_results if item.ok)
        print("evidence factory ok" if result.ok else "evidence factory failed")
        print(f"requested_cycles={result.requested_cycles}")
        print(f"executed_cycles={result.cycles}")
        print(f"until_replay_ready={'yes' if result.until_replay_ready else 'no'}")
        print(f"stop_reason={result.stopped_reason}")
        print(f"scenarios={passed}/{len(result.scenario_results)}")
        print(f"generated_traces={result.generated_trace_count}")
        print(f"generated_source_traces={result.generated_source_trace_count}")
        print(f"generated_activation_traces={result.generated_activation_trace_count}")
        print(f"generated_known_bad_traces={result.generated_known_bad_trace_count}")
        print(
            "replay="
            f"{result.replay_report['eligible_source_traces']}/"
            f"{result.replay_report['minimum_eligible_traces']} eligible,"
            f"{result.replay_report['known_bad_source_traces']}/"
            f"{result.replay_report['minimum_known_bad_traces']} known_bad,"
            f"{result.replay_report['distinct_skill_count']}/"
            f"{result.replay_report['minimum_distinct_skills']} skills"
        )
        projection = result.progress_projection
        print(
            "estimated_cycles_to_threshold="
            + (
                str(projection["estimated_cycles_to_threshold"])
                if projection["estimated_cycles_to_threshold"] is not None
                else "unknown"
            )
        )
        print(f"report_path={result.report_path}")
        return 0 if result.ok else 1

    if args.replay_readiness_report:
        print(json.dumps(replay_readiness_report(config, repo_root=args.repo_root, limit=args.report_limit), indent=2, sort_keys=True))
        return 0

    if args.mac_studio_day_one:
        result = build_mac_studio_day_one_handoff(
            config=config,
            repo_root=args.repo_root,
            tool_registry=runtime,
            cycles=args.evidence_cycles,
            report_limit=args.report_limit,
        )
        print("mac studio day one ok" if result.ok else "mac studio day one failed")
        print(f"doctor={'ok' if result.doctor.ok else 'failed'}")
        print(f"bootstrap_stack={'ok' if result.bootstrap_stack.ok else 'failed'}")
        print(f"evidence_factory={'ok' if result.evidence_batch.ok else 'failed'}")
        print(
            "replay="
            f"{result.replay_report['eligible_source_traces']}/"
            f"{result.replay_report['minimum_eligible_traces']} eligible,"
            f"{result.replay_report['known_bad_source_traces']}/"
            f"{result.replay_report['minimum_known_bad_traces']} known_bad,"
            f"{result.replay_report['distinct_skill_count']}/"
            f"{result.replay_report['minimum_distinct_skills']} skills"
        )
        print(f"handoff_path={result.handoff_path}")
        print(f"issues={'; '.join(result.issues) if result.issues else 'none'}")
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

    if args.milestone_status:
        print(json.dumps(evaluate_milestone_status(config), indent=2, sort_keys=True))
        return 0

    if args.workspace_overview:
        print(json.dumps(workspace_overview(config), indent=2, sort_keys=True, default=str))
        return 0

    if args.bootstrap_stack:
        result = bootstrap_stack(
            config=config,
            repo_root=args.repo_root,
            tool_registry=runtime,
        )
        print("bootstrap stack ok" if result.ok else "bootstrap stack failed")
        print(f"proxy_self_test={'ok' if result.proxy_self_test.ok else 'failed'}")
        print(f"contract_harness={'ok' if result.contract_harness.ok else 'failed'}")
        print(f"task_loop_proof={'ok' if result.task_loop_proof.ok else 'failed'}")
        print(f"research_cron_proof={'ok' if result.research_cron_proof.ok else 'failed'}")
        print(json.dumps(result.milestone_status, indent=2, sort_keys=True))
        return 0 if result.ok else 1

    if args.bootstrap_live:
        result = bootstrap_runtime(runtime, config=config, model_name=args.model_name)
        print("bootstrap ok" if result.ok else "bootstrap failed")
        print(f"session_id={result.session_context.session_id}")
        print(f"tools={','.join(result.registered_tools)}")
        return 0 if result.ok else 1

    result = bootstrap_runtime(runtime, config=config, model_name=args.model_name)
    print("bootstrap ok" if result.ok else "bootstrap failed")
    print(f"session_id={result.session_context.session_id}")
    print(f"tools={','.join(result.registered_tools)}")
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
