from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from skills.config import IntegrationConfig
from skills.milestone_status import runtime_support_artifact_paths


@dataclass(frozen=True)
class RuntimeArtifactDefaults:
    generated_at: bool = False
    live_controls_enabled: bool | None = None


RUNTIME_ARTIFACT_DEFAULTS: dict[str, RuntimeArtifactDefaults] = {
    "hermes_adapter_readiness": RuntimeArtifactDefaults(live_controls_enabled=False),
    "recovery_readiness": RuntimeArtifactDefaults(live_controls_enabled=False),
    "migration_readiness": RuntimeArtifactDefaults(live_controls_enabled=False),
    "pre_hermes_readiness": RuntimeArtifactDefaults(live_controls_enabled=False),
    "pre_live_mission_control": RuntimeArtifactDefaults(live_controls_enabled=False),
    "hermes_adapter_gauntlet": RuntimeArtifactDefaults(live_controls_enabled=False),
    "first_live_project_packet": RuntimeArtifactDefaults(live_controls_enabled=False),
    "model_shadow_ops": RuntimeArtifactDefaults(live_controls_enabled=False),
    "target_machine_validation_run_packet": RuntimeArtifactDefaults(live_controls_enabled=False),
    "pre_live_bundle_verification": RuntimeArtifactDefaults(live_controls_enabled=False),
    "target_machine_evidence_check": RuntimeArtifactDefaults(live_controls_enabled=False),
    "first_live_project_acceptance_check": RuntimeArtifactDefaults(live_controls_enabled=False),
    "model_efficiency_service_packet": RuntimeArtifactDefaults(live_controls_enabled=False),
    "pre_live_completion_bundle": RuntimeArtifactDefaults(live_controls_enabled=False),
    "pre_live_evidence_crosswalk": RuntimeArtifactDefaults(live_controls_enabled=False),
    "replay_readiness_report": RuntimeArtifactDefaults(generated_at=True),
    "replay_corpus_export": RuntimeArtifactDefaults(generated_at=True),
    "optimizer_snapshot": RuntimeArtifactDefaults(generated_at=True),
    "harness_candidate_report": RuntimeArtifactDefaults(generated_at=True),
}


PRE_LIVE_CLOSED_CONTROL_CONTRACT: dict[str, bool] = {
    "live_controls_enabled": False,
    "dashboard_writes_enabled": False,
    "paid_provider_calls_enabled": False,
    "customer_visible_commitments_enabled": False,
    "model_route_promotion_enabled": False,
    "autonomous_patch_application_enabled": False,
    "side_effect_replay_enabled": False,
}

PRE_LIVE_FAIL_CLOSED_CONTROLS: tuple[str, ...] = (
    "live_hermes_attachment",
    "dashboard_write_controls",
    "paid_provider_calls",
    "customer_visible_commitments",
    "model_route_promotion",
    "autonomous_patch_application",
    "side_effect_replay",
)


def runtime_artifact_path(config: IntegrationConfig, artifact_name: str) -> Path:
    paths = runtime_support_artifact_paths(config)
    try:
        return paths[artifact_name]
    except KeyError as exc:
        available = ", ".join(sorted(paths))
        raise KeyError(f"unknown runtime artifact {artifact_name!r}; available: {available}") from exc


def write_runtime_artifact(
    config: IntegrationConfig,
    artifact_name: str,
    payload: dict[str, Any],
    *,
    default_fields: dict[str, Any] | None = None,
    generated_at: bool | None = None,
    live_controls_enabled: bool | None = None,
) -> Path:
    path = runtime_artifact_path(config, artifact_name)
    defaults = RUNTIME_ARTIFACT_DEFAULTS.get(artifact_name, RuntimeArtifactDefaults())
    artifact = dict(payload)
    should_add_generated_at = generated_at if generated_at is not None else defaults.generated_at
    if should_add_generated_at:
        artifact.setdefault("generated_at", _utc_now())
    artifact.setdefault("artifact_path", str(path))
    for key, value in (default_fields or {}).items():
        artifact.setdefault(key, value)
    live_control_default = (
        live_controls_enabled
        if live_controls_enabled is not None
        else defaults.live_controls_enabled
    )
    if live_control_default is not None:
        artifact.setdefault("live_controls_enabled", live_control_default)
    _write_json(path, artifact)
    return path


def stable_json_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def write_hashed_runtime_artifact(
    config: IntegrationConfig,
    artifact_name: str,
    payload: dict[str, Any],
    *,
    hash_key: str = "packet_hash",
) -> dict[str, Any]:
    payload[hash_key] = stable_json_hash({key: value for key, value in payload.items() if key != hash_key})
    write_runtime_artifact(config, artifact_name, payload)
    return payload


def pre_live_closed_control_contract() -> dict[str, bool]:
    return dict(PRE_LIVE_CLOSED_CONTROL_CONTRACT)


def pre_live_fail_closed_controls() -> list[str]:
    return list(PRE_LIVE_FAIL_CLOSED_CONTROLS)


def pre_live_controls_are_closed(contract: dict[str, Any] | None) -> bool:
    if not isinstance(contract, dict) or not contract:
        return False
    return all(contract.get(key) is False for key in PRE_LIVE_CLOSED_CONTROL_CONTRACT)


def pre_live_artifact_controls_disabled(payload: dict[str, Any]) -> bool:
    if payload.get("live_controls_enabled") is not False:
        return False
    contract = payload.get("closed_control_contract")
    if contract is None:
        return True
    return pre_live_controls_are_closed(contract)


def runtime_evidence_manifest_item(
    name: str,
    path: Path,
    *,
    packet_hash: str | None = None,
    required_before_live_authority: bool = True,
) -> dict[str, Any]:
    return {
        "name": name,
        "path": str(path),
        "exists": path.is_file(),
        "sha256": file_sha256(path),
        "packet_hash": packet_hash,
        "required_before_live_authority": required_before_live_authority,
    }


def target_machine_evidence_check_packet(
    *,
    bundle: Path,
    packet_path: Path,
    sha_path: Path,
    packet: dict[str, Any],
    sha_entries: dict[str, str],
    evidence_records: dict[str, dict[str, Any]],
    generated_at: str,
    artifact_path: Path,
) -> dict[str, Any]:
    required_evidence = sorted(
        {
            str(item)
            for step in packet.get("run_steps", [])
            if isinstance(step, dict)
            for item in step.get("required_evidence", [])
        }
    )
    missing_required_evidence = [
        item
        for item in required_evidence
        if item not in evidence_records and not (bundle / "evidence" / f"{item}.json").is_file()
    ]
    ambiguous_required_evidence = [
        item
        for item, record in evidence_records.items()
        if item in required_evidence
        and (record.get("status") in {"ambiguous", "stale"} or record.get("ambiguous") is True)
    ]
    artifact_results = []
    for item in packet.get("evidence_manifest", []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or Path(str(item.get("path", ""))).stem)
        basename = Path(str(item.get("path") or f"{name}.json")).name
        bundle_path = bundle / basename
        actual_hash = file_sha256(bundle_path)
        manifest_hash = sha_entries.get(basename)
        expected_hash = item.get("sha256")
        artifact_results.append(
            {
                "name": name,
                "filename": basename,
                "exists": bundle_path.is_file(),
                "sha256": actual_hash,
                "sha256sum_entry": manifest_hash,
                "run_packet_sha256": expected_hash,
                "matches_sha256sums": bool(actual_hash and manifest_hash and actual_hash == manifest_hash),
                "matches_run_packet_manifest": bool(actual_hash and expected_hash and actual_hash == expected_hash),
                "required_before_live_authority": bool(item.get("required_before_live_authority")),
            }
        )
    blockers = []
    if not bundle.is_dir():
        blockers.append("target_machine_artifact_bundle_missing")
    if not packet:
        blockers.append("target_machine_run_packet_missing_or_invalid")
    if not sha_entries:
        blockers.append("sha256sums_missing_or_empty")
    if missing_required_evidence:
        blockers.append("required_evidence_missing")
    if ambiguous_required_evidence:
        blockers.append("required_evidence_stale_or_ambiguous")
    if any(not item["exists"] for item in artifact_results):
        blockers.append("manifest_artifact_missing")
    if any(not item["matches_sha256sums"] for item in artifact_results):
        blockers.append("sha256sum_mismatch")
    if any(not item["matches_run_packet_manifest"] for item in artifact_results):
        blockers.append("run_packet_manifest_hash_mismatch")
    if packet.get("live_controls_enabled") is not False:
        blockers.append("run_packet_live_controls_not_disabled")
    closed_control_contract = packet.get("closed_control_contract", {})
    closed_control_contract_ok = pre_live_controls_are_closed(closed_control_contract)
    if packet and not closed_control_contract_ok:
        blockers.append("closed_control_contract_opened_live_control")
    required_artifact_names = sorted(
        str(item.get("name") or Path(str(item.get("path", ""))).stem)
        for item in packet.get("evidence_manifest", [])
        if isinstance(item, dict)
    )
    replay_projection_contract = {
        "packet_declares_events_before_projection": all(
            bool(step.get("fail_closed_on_missing_evidence")) for step in packet.get("run_steps", []) if isinstance(step, dict)
        ),
        "external_side_effect_replay_disabled": (
            isinstance(closed_control_contract, dict)
            and closed_control_contract.get("side_effect_replay_enabled") is False
        ),
        "artifacts_hash_checked_before_live_authority": bool(artifact_results)
        and all(
            item["matches_sha256sums"]
            and item["matches_run_packet_manifest"]
            and item["required_before_live_authority"]
            for item in artifact_results
        ),
        "required_evidence_non_ambiguous": not missing_required_evidence and not ambiguous_required_evidence,
    }
    if packet and not all(replay_projection_contract.values()):
        blockers.append("replay_projection_contract_not_proven")
    status = "validated_preserved_target_machine_bundle" if not blockers else "blocked"
    return {
        "available": True,
        "generated_at": generated_at,
        "packet_name": "target_machine_evidence_check",
        "status": status,
        "bundle_dir": str(bundle),
        "run_packet_path": str(packet_path),
        "sha256sums_path": str(sha_path),
        "required_evidence": required_evidence,
        "required_artifacts": required_artifact_names,
        "missing_required_evidence": missing_required_evidence,
        "ambiguous_required_evidence": ambiguous_required_evidence,
        "artifact_results": artifact_results,
        "closed_control_contract_ok": closed_control_contract_ok,
        "replay_projection_contract": replay_projection_contract,
        "blockers": blockers,
        "live_controls_enabled": False,
        "activation_effect": "none",
        "artifact_path": str(artifact_path),
    }


def first_live_project_acceptance_check_packet(
    *,
    packet: dict[str, Any],
    generated_at: str,
    artifact_path: Path,
) -> dict[str, Any]:
    workflow = packet.get("workflow", [])
    dry_run = packet.get("dry_run", {})
    close_path = dry_run.get("close_path", {}) if isinstance(dry_run, dict) else {}
    checks = {
        "local_only_artifact_output": bool(packet.get("summary", {}).get("local_artifact_only"))
        and packet.get("artifact_contract", {}).get("external_delivery")
        == "prepared_intent_only_until_operator_gate",
        "operator_gate_presence": any(item.get("operator_gate_required") for item in workflow if isinstance(item, dict)),
        "feedback_ingestion": bool(close_path.get("feedback_ingested"))
        and bool(close_path.get("close_or_continue_requires_operator_gate")),
        "no_external_side_effect_execution": all(
            item.get("external_side_effects_executed") is False for item in workflow if isinstance(item, dict)
        ),
        "live_controls_disabled": packet.get("live_controls_enabled") is False,
        "external_commitments_disabled": packet.get("summary", {}).get("external_commitments_allowed") is False,
    }
    blockers = [name for name, ok in checks.items() if not ok]
    return {
        "available": True,
        "generated_at": generated_at,
        "packet_name": "first_live_project_acceptance_check",
        "status": "accepted_pre_live_local_only" if not blockers else "blocked",
        "fixture_id": packet.get("fixture_id"),
        "checks": checks,
        "blockers": blockers,
        "live_controls_enabled": False,
        "activation_effect": "none",
        "artifact_path": str(artifact_path),
    }


def pre_live_evidence_crosswalk_row(
    *,
    checklist_id: str,
    requirement: str,
    step_names: list[str],
    artifact_names: list[str],
    closed_control_keys: list[str],
    blocker_conditions: list[str],
    steps_by_name: dict[str, dict[str, Any]],
    artifacts_by_name: dict[str, dict[str, Any]],
    closed_controls: dict[str, Any],
) -> dict[str, Any]:
    mapped_steps = [steps_by_name[name] for name in step_names if name in steps_by_name]
    mapped_artifacts = [artifacts_by_name[name] for name in artifact_names if name in artifacts_by_name]
    missing_steps = [name for name in step_names if name not in steps_by_name]
    missing_artifacts = [name for name in artifact_names if name not in artifacts_by_name]
    artifact_checks = [
        {
            "name": item["name"],
            "path": item.get("path"),
            "exists": bool(item.get("exists")),
            "sha256": item.get("sha256"),
            "required_before_live_authority": bool(item.get("required_before_live_authority")),
        }
        for item in mapped_artifacts
    ]
    failing_artifacts = [
        item["name"]
        for item in artifact_checks
        if not item["exists"] or not item["sha256"] or not item["required_before_live_authority"]
    ]
    opened_controls = [key for key in closed_control_keys if closed_controls.get(key) is not False]
    required_evidence = sorted(
        {
            str(evidence_id)
            for step in mapped_steps
            for evidence_id in step.get("required_evidence", [])
        }
    )
    ready = not missing_steps and not missing_artifacts and not failing_artifacts and not opened_controls
    return {
        "checklist_id": checklist_id,
        "requirement": requirement,
        "step_names": step_names,
        "artifact_names": artifact_names,
        "mapped_step_count": len(mapped_steps),
        "mapped_artifact_count": len(mapped_artifacts),
        "required_evidence": required_evidence,
        "artifact_checks": artifact_checks,
        "closed_control_keys": closed_control_keys,
        "missing_steps": missing_steps,
        "missing_artifacts": missing_artifacts,
        "failing_artifacts": failing_artifacts,
        "opened_controls": opened_controls,
        "blocker_conditions": blocker_conditions,
        "ready": ready,
    }


def file_sha256(path: Path) -> str | None:
    try:
        with path.open("rb") as handle:
            digest = hashlib.sha256()
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return None
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{json.dumps(payload, indent=2, sort_keys=True)}\n", encoding="utf-8")
