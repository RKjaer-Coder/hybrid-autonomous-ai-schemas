from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from kernel.runtime import runtime_logs_dir as _kernel_runtime_logs_dir
from skills.config import IntegrationConfig
from skills.milestone_status import runtime_support_artifact_paths


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
        "flywheel_drill": bin_dir / "flywheel_drill.sh",
        "evidence_factory": bin_dir / "evidence_factory.sh",
        "replay_readiness_report": bin_dir / "replay_readiness_report.sh",
        "export_replay_corpus": bin_dir / "export_replay_corpus.sh",
        "optimizer_snapshot": bin_dir / "optimizer_snapshot.sh",
        "analyze_harness_candidates": bin_dir / "analyze_harness_candidates.sh",
        "propose_best_harness_candidate": bin_dir / "propose_best_harness_candidate.sh",
        "known_bad_hardening_operator_review": bin_dir / "known_bad_hardening_operator_review.sh",
        "known_bad_hardening_follow_on_review": bin_dir / "known_bad_hardening_follow_on_review.sh",
        "mac_studio_day_one": bin_dir / "mac_studio_day_one.sh",
        "recovery_readiness": bin_dir / "recovery_readiness.sh",
        "hermes_adapter_readiness": bin_dir / "hermes_adapter_readiness.sh",
        "migration_readiness": bin_dir / "migration_readiness.sh",
        "pre_hermes_readiness": bin_dir / "pre_hermes_readiness.sh",
        "self_improvement_evidence_pipeline": bin_dir / "self_improvement_evidence_pipeline.sh",
        "self_improvement_snapshot": bin_dir / "self_improvement_snapshot.sh",
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
    return _kernel_runtime_logs_dir(config)


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


def _runtime_local_provider_doctor_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["local_provider_doctor"]


def _runtime_curator_readiness_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["curator_readiness"]


def _runtime_evidence_factory_manifest_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["evidence_factory_manifest"]


def _runtime_flywheel_drill_report_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["flywheel_drill_report"]


def _runtime_replay_readiness_report_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["replay_readiness_report"]


def _runtime_replay_corpus_export_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["replay_corpus_export"]


def _runtime_optimizer_snapshot_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["optimizer_snapshot"]


def _runtime_harness_candidate_report_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["harness_candidate_report"]


def _runtime_known_bad_hardening_follow_on_review_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["known_bad_hardening_follow_on_review"]


def _runtime_mac_studio_day_one_handoff_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["mac_studio_day_one_handoff"]


def _runtime_recovery_readiness_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["recovery_readiness"]


def _runtime_hermes_adapter_readiness_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["hermes_adapter_readiness"]


def _runtime_migration_readiness_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["migration_readiness"]


def _runtime_pre_hermes_readiness_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["pre_hermes_readiness"]


def _runtime_self_improvement_snapshot_path(config: IntegrationConfig) -> Path:
    return runtime_support_artifact_paths(config)["self_improvement_snapshot"]


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



def _write_json_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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
