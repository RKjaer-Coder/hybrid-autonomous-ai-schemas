from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from harness_variants import HarnessVariantManager
from skills.config import IntegrationConfig
from skills.db_manager import CANONICAL_DATABASES, DatabaseManager


SUPPORT_ARTIFACT_FILENAMES = {
    "network_controls": "network_controls.json",
    "proxy_allowlist": "proxy_allowlist.json",
    "gateway_manifest": "gateway_manifest.json",
    "workspace_manifest": "workspace_manifest.json",
    "operator_validation_checklist": "operator_validation_checklist.md",
    "evidence_factory_manifest": "evidence_factory_manifest.json",
    "replay_readiness_report": "replay_readiness_report.json",
    "mac_studio_day_one_handoff": "mac_studio_day_one_handoff.md",
}


def _runtime_bundle_dir(config: IntegrationConfig) -> Path:
    return Path(config.skills_dir) / "runtime"


def runtime_support_artifact_paths(config: IntegrationConfig) -> dict[str, Path]:
    bundle = _runtime_bundle_dir(config)
    return {
        name: bundle / filename
        for name, filename in SUPPORT_ARTIFACT_FILENAMES.items()
    }


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


def _trace_role_counts(config: IntegrationConfig) -> dict[str, int]:
    telemetry_db = Path(config.data_dir) / "telemetry.db"
    if not telemetry_db.is_file():
        return {}
    manager = HarnessVariantManager(str(telemetry_db))
    if not manager.available:
        return {}
    traces = manager.list_execution_traces(limit=500)
    counts: dict[str, int] = {}
    for row in traces:
        counts[row["role"]] = counts.get(row["role"], 0) + 1
    return counts


def _standing_brief_summary(config: IntegrationConfig) -> dict[str, int]:
    db_path = Path(config.data_dir) / "strategic_memory.db"
    if not db_path.is_file():
        return {"available": 0, "active": 0, "paused": 0, "archived": 0}
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'standing_briefs'"
        ).fetchone()
        if table is None:
            return {"available": 0, "active": 0, "paused": 0, "archived": 0}
        counts = conn.execute(
            """
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_count,
                SUM(CASE WHEN status = 'PAUSED' THEN 1 ELSE 0 END) AS paused_count,
                SUM(CASE WHEN status = 'ARCHIVED' THEN 1 ELSE 0 END) AS archived_count
            FROM standing_briefs
            """
        ).fetchone()
    return {
        "available": 1,
        "active": int(counts["active_count"] or 0),
        "paused": int(counts["paused_count"] or 0),
        "archived": int(counts["archived_count"] or 0),
    }


def _milestone_record(
    name: str,
    *,
    implemented: bool,
    proof_status: str,
    blockers: list[str],
    evidence: dict[str, Any],
) -> dict[str, Any]:
    if blockers:
        state = "BLOCKED"
    elif proof_status == "PASS":
        state = "PROVEN_REPO"
    elif implemented:
        state = "IMPLEMENTED_UNPROVEN"
    else:
        state = "PARTIAL"
    return {
        "milestone": name,
        "state": state,
        "implemented": implemented,
        "proof_status": proof_status,
        "blockers": blockers,
        "evidence": evidence,
    }


def evaluate_milestone_status(
    config: IntegrationConfig | None = None,
    *,
    db_manager: DatabaseManager | None = None,
) -> dict[str, Any]:
    base = (config or IntegrationConfig()).resolve_paths()
    defaults = IntegrationConfig().resolve_paths()
    if base.data_dir != defaults.data_dir:
        root_dir = Path(base.data_dir).expanduser().resolve().parent
        if base.skills_dir == defaults.skills_dir:
            base = IntegrationConfig(
                data_dir=base.data_dir,
                skills_dir=str(root_dir / "skills" / "hybrid-autonomous-ai"),
                checkpoints_dir=str(root_dir / "skills" / "hybrid-autonomous-ai" / "checkpoints"),
                alerts_dir=str(root_dir / "alerts"),
                max_api_spend_usd=base.max_api_spend_usd,
                construction_phase=base.construction_phase,
                profile_name=base.profile_name,
                proxy_bind_url=base.proxy_bind_url,
                outbound_allowlist_domains=tuple(base.outbound_allowlist_domains),
                outbound_allowlist_ports=tuple(base.outbound_allowlist_ports),
                hermes_gateway_url=base.hermes_gateway_url,
                hermes_workspace_url=base.hermes_workspace_url,
            ).resolve_paths()
    resolved = base
    launchers = _runtime_launcher_paths(resolved)
    artifacts = runtime_support_artifact_paths(resolved)
    trace_roles = _trace_role_counts(resolved)
    standing_briefs = _standing_brief_summary(resolved)

    if db_manager is None:
        db_manager = DatabaseManager(resolved.data_dir)
    db_status = db_manager.verify_all_databases()

    common_paths = {
        "runtime_bundle": _runtime_bundle_dir(resolved).is_dir(),
        "bootstrap_launcher": launchers["bootstrap"].is_file(),
        "doctor_launcher": launchers["doctor"].is_file(),
        "readiness_launcher": launchers["readiness"].is_file(),
    }

    m1_blockers = [name for name, ok in db_status.items() if not ok]
    m1 = _milestone_record(
        "M1",
        implemented=all(db_status.values()),
        proof_status="PASS" if trace_roles.get("runtime_contract", 0) > 0 else "NOT_RUN",
        blockers=m1_blockers,
        evidence={
            "db_status": db_status,
            "runtime_contract_traces": trace_roles.get("runtime_contract", 0),
            "contract_harness_launcher": launchers["contract_harness"].is_file(),
            "common_paths": common_paths,
        },
    )

    m2_blockers = [name for name, ok in common_paths.items() if not ok]
    for name in ("network_controls", "proxy_allowlist", "gateway_manifest"):
        if not artifacts[name].is_file():
            m2_blockers.append(name)
    if not launchers["start_proxy"].is_file():
        m2_blockers.append("start_proxy_launcher")
    if not launchers["proxy_self_test"].is_file():
        m2_blockers.append("proxy_self_test_launcher")
    m2 = _milestone_record(
        "M2",
        implemented=not m2_blockers,
        proof_status="PASS" if trace_roles.get("proxy_self_test", 0) > 0 else "NOT_RUN",
        blockers=m2_blockers,
        evidence={
            "proxy_bind_url": resolved.proxy_bind_url,
            "allowlist_domains": list(resolved.outbound_allowlist_domains),
            "allowlist_ports": list(resolved.outbound_allowlist_ports),
            "proxy_self_test_traces": trace_roles.get("proxy_self_test", 0),
            "artifact_paths": {name: str(path) for name, path in artifacts.items()},
        },
    )

    m3_blockers: list[str] = []
    if not launchers["operator_workflow"].is_file():
        m3_blockers.append("operator_workflow_launcher")
    if not launchers["task_loop_proof"].is_file():
        m3_blockers.append("task_loop_proof_launcher")
    m3 = _milestone_record(
        "M3",
        implemented=not m3_blockers,
        proof_status=(
            "PASS"
            if trace_roles.get("operator_workflow", 0) > 0 and trace_roles.get("task_loop_proof", 0) > 0
            else "NOT_RUN"
        ),
        blockers=m3_blockers,
        evidence={
            "operator_workflow_traces": trace_roles.get("operator_workflow", 0),
            "task_loop_proof_traces": trace_roles.get("task_loop_proof", 0),
        },
    )

    m4_blockers: list[str] = []
    if not launchers["readiness"].is_file():
        m4_blockers.append("readiness_launcher")
    if not artifacts["gateway_manifest"].is_file():
        m4_blockers.append("gateway_manifest")
    m4 = _milestone_record(
        "M4",
        implemented=not m4_blockers,
        proof_status="PASS" if trace_roles.get("runtime_contract", 0) > 0 else "NOT_RUN",
        blockers=m4_blockers,
        evidence={
            "runtime_contract_traces": trace_roles.get("runtime_contract", 0),
            "gateway_url": resolved.hermes_gateway_url,
        },
    )

    m5_blockers: list[str] = []
    if not launchers["research_cron_proof"].is_file():
        m5_blockers.append("research_cron_proof_launcher")
    if not artifacts["workspace_manifest"].is_file():
        m5_blockers.append("workspace_manifest")
    if standing_briefs["available"] == 0:
        m5_blockers.append("standing_briefs_table")
    m5 = _milestone_record(
        "M5",
        implemented=not m5_blockers,
        proof_status=(
            "PASS"
            if trace_roles.get("research_cron_proof", 0) > 0 and trace_roles.get("standing_brief_run", 0) > 0
            else "NOT_RUN"
        ),
        blockers=m5_blockers,
        evidence={
            "research_cron_proof_traces": trace_roles.get("research_cron_proof", 0),
            "standing_brief_run_traces": trace_roles.get("standing_brief_run", 0),
            "standing_briefs": standing_briefs,
            "workspace_url": resolved.hermes_workspace_url,
        },
    )

    milestones = {item["milestone"]: item for item in (m1, m2, m3, m4, m5)}
    proven = sum(1 for item in milestones.values() if item["state"] == "PROVEN_REPO")
    blocked = sum(1 for item in milestones.values() if item["state"] == "BLOCKED")
    return {
        "milestones": milestones,
        "summary": {
            "proven_repo": proven,
            "blocked": blocked,
            "implemented_or_better": sum(1 for item in milestones.values() if item["implemented"]),
            "runtime_bundle_dir": str(_runtime_bundle_dir(resolved)),
        },
    }
