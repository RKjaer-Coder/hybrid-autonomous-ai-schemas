from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kernel.runtime_catalog import runtime_support_artifact_paths
from skills.config import IntegrationConfig


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
    "model_efficiency_customer_validation_brief": RuntimeArtifactDefaults(live_controls_enabled=False),
    "pre_live_completion_bundle": RuntimeArtifactDefaults(live_controls_enabled=False),
    "pre_live_evidence_crosswalk": RuntimeArtifactDefaults(live_controls_enabled=False),
    "replay_readiness_report": RuntimeArtifactDefaults(generated_at=True),
    "replay_corpus_export": RuntimeArtifactDefaults(generated_at=True),
    "optimizer_snapshot": RuntimeArtifactDefaults(generated_at=True),
    "harness_candidate_report": RuntimeArtifactDefaults(generated_at=True),
}

PLACEHOLDER_RUNTIME_ARTIFACT_STATUSES: frozenset[str] = frozenset(
    {"NOT_RUN", "UNAVAILABLE", "PLACEHOLDER", "EMPTY"}
)


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

TARGET_MACHINE_REPLAY_PROJECTION_PROOF_KEYS: tuple[str, ...] = (
    "first_live_project_events_before_projection",
    "readiness_requires_projection_checks",
    "resume_replay_reconstructs_intents_only",
    "external_side_effect_replay_disabled",
    "manifest_artifacts_hash_bound_before_live_authority",
)

TARGET_MACHINE_REPLAY_PROJECTION_EVIDENCE: tuple[str, ...] = (
    "projection_checks_verified",
    "first_live_project_events_before_projection_verified",
    "resume_replay_intents_reconstructed_only",
    "external_side_effect_replay_disabled_verified",
    "manifest_artifacts_hash_bound_before_live_authority",
)

TARGET_MACHINE_REPLAY_PROJECTION_EVIDENCE_PROOF_KEYS: dict[str, str] = {
    "projection_checks_verified": "readiness_requires_projection_checks",
    "first_live_project_events_before_projection_verified": "first_live_project_events_before_projection",
    "resume_replay_intents_reconstructed_only": "resume_replay_reconstructs_intents_only",
    "external_side_effect_replay_disabled_verified": "external_side_effect_replay_disabled",
    "manifest_artifacts_hash_bound_before_live_authority": "manifest_artifacts_hash_bound_before_live_authority",
}


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


def write_placeholder_runtime_artifact(
    config: IntegrationConfig,
    artifact_name: str,
    payload: dict[str, Any],
) -> Path:
    """Write install-time placeholders without replacing generated runtime packets."""
    path = runtime_artifact_path(config, artifact_name)
    existing = _read_json(path)
    if (
        isinstance(existing, dict)
        and existing.get("available") is True
        and existing.get("status") not in PLACEHOLDER_RUNTIME_ARTIFACT_STATUSES
    ):
        return path
    write_hashed_runtime_artifact(config, artifact_name, payload)
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


def target_machine_replay_projection_proof_contract(
    *,
    project_packet: dict[str, Any],
    adapter_packet: dict[str, Any],
    closed_control_contract: dict[str, Any],
    evidence_manifest: list[dict[str, Any]],
    run_steps: list[dict[str, Any]],
) -> dict[str, bool]:
    return {
        "first_live_project_events_before_projection": all(
            step.get("event_before_projection") is True
            for step in project_packet.get("workflow", [])
        ),
        "readiness_requires_projection_checks": any(
            step.get("name") == "recovery_and_migration_readiness"
            and "projection_checks_verified" in step.get("required_evidence", [])
            for step in run_steps
            if isinstance(step, dict)
        ),
        "resume_replay_reconstructs_intents_only": adapter_packet.get("resume_replay_summary", {}).get(
            "replay_intents_reconstructed_only"
        )
        is True,
        "external_side_effect_replay_disabled": closed_control_contract.get("side_effect_replay_enabled") is False,
        "manifest_artifacts_hash_bound_before_live_authority": bool(evidence_manifest)
        and all(
            item.get("exists") and item.get("sha256") and item.get("required_before_live_authority")
            for item in evidence_manifest
        ),
    }


def target_machine_replay_projection_proof_records() -> list[dict[str, Any]]:
    return [
        {
            "proof_contract_key": "first_live_project_events_before_projection",
            "evidence_id": "first_live_project_events_before_projection_verified",
            "reconstructed_intent": "first_live_project workflow phase intents are reconstructed from kernel task events before projection",
            "projected_state": "first_live_project_packet.workflow event_before_projection flags remain true for every phase",
            "forbidden_side_effect_reexecution": {
                "allowed": False,
                "control": "workflow.external_side_effects_executed_false",
            },
        },
        {
            "proof_contract_key": "readiness_requires_projection_checks",
            "evidence_id": "projection_checks_verified",
            "reconstructed_intent": "readiness validation reconstructs recovery and migration intent before adapter use",
            "projected_state": "readiness_suite projection checks must pass before live authority",
            "forbidden_side_effect_reexecution": {
                "allowed": False,
                "control": "readiness_projection_checks_inspect_only",
            },
        },
        {
            "proof_contract_key": "resume_replay_reconstructs_intents_only",
            "evidence_id": "resume_replay_intents_reconstructed_only",
            "reconstructed_intent": "Hermes resume replay reconstructs side-effect intents and receipts only",
            "projected_state": "adapter resume replay summary reports replay_intents_reconstructed_only",
            "forbidden_side_effect_reexecution": {
                "allowed": False,
                "control": "adapter_resume_external_side_effects_reexecuted_false",
            },
        },
        {
            "proof_contract_key": "external_side_effect_replay_disabled",
            "evidence_id": "external_side_effect_replay_disabled_verified",
            "reconstructed_intent": "external side-effect intents remain inspect-or-compensate replay records",
            "projected_state": "closed_control_contract.side_effect_replay_enabled remains false",
            "forbidden_side_effect_reexecution": {
                "allowed": False,
                "control": "closed_control_contract.side_effect_replay_enabled_false",
            },
        },
        {
            "proof_contract_key": "manifest_artifacts_hash_bound_before_live_authority",
            "evidence_id": "manifest_artifacts_hash_bound_before_live_authority",
            "reconstructed_intent": "artifact manifest preserves generated packet identity before live authority",
            "projected_state": "every manifest artifact has an existing SHA-256 and required_before_live_authority=true",
            "forbidden_side_effect_reexecution": {
                "allowed": False,
                "control": "manifest_artifact_verification_read_only",
            },
        },
    ]


def target_machine_replay_projection_proof_records_contract(records: list[dict[str, Any]]) -> bool:
    return _replay_projection_proof_records_distinguish_effects(records)


def pre_live_completion_bundle_packet(
    *,
    target_run: dict[str, Any],
    mission: dict[str, Any],
    efficiency: dict[str, Any],
    source_files: dict[str, Path],
    component_paths: dict[str, Path],
    generated_at: str,
    repo_root: Path,
    artifact_path: Path,
) -> dict[str, Any]:
    """Build the all-ten pre-live completion proof packet without activating live authority."""
    first_project = mission["components"]["first_live_project"]
    adapter = mission["components"]["hermes_adapter_gauntlet"]
    shadow = mission["components"]["model_shadow_ops"]
    readiness = mission["components"]["readiness_suite"]

    source_status = {name: path.is_file() for name, path in source_files.items()}
    component_artifacts = {
        name: {"path": str(path), "exists": path.is_file(), "sha256": file_sha256(path)}
        for name, path in component_paths.items()
    }

    workflow = first_project["workflow"]
    workflow_has_events_and_grants = all(
        step.get("event_before_projection") and step.get("capability_grants_required")
        for step in workflow
    )
    mission_ready = mission.get("go_no_go") == "ready_for_target_machine_validation"
    target_ready = target_run.get("status") == "ready_for_target_machine_execution"
    closed_controls = target_run.get("closed_control_contract", {})
    closed_control_ok = pre_live_controls_are_closed(closed_controls)

    goals = [
        _pre_live_goal(
            "operator_project_loop",
            "End-to-end operator validate/build/ship/feedback loop",
            source_status["kernel_commercial"] and source_status["research_tests"],
            first_project["summary"]["ready_for_target_machine_fixture"]
            and first_project["summary"]["phase_count"] == 4
            and first_project["summary"]["local_artifact_only"]
            and not first_project["summary"]["external_commitments_allowed"],
            ["kernel/commercial.py", "tests/test_kernel_research.py", "first_live_project_packet.json"],
        ),
        _pre_live_goal(
            "model_efficiency_service",
            "Governed model-efficiency service packet and shadow evidence",
            source_status["kernel_model_intelligence"] and source_status["runtime_tests"],
            efficiency["summary"]["seed_task_class_count"] == 3
            and efficiency["summary"]["operator_gate_required_for_customer_delivery"]
            and not efficiency["summary"]["route_mutation_enabled"]
            and not efficiency["summary"]["external_side_effects_allowed"],
            ["kernel/model_intelligence.py", "model_efficiency_service_packet.json"],
        ),
        _pre_live_goal(
            "seed_model_intelligence",
            "Seed Model Intelligence registry, eval, route, and promotion gates",
            source_status["kernel_model_intelligence"] and source_status["model_tests"],
            shadow["summary"]["seed_task_class_count"] == 3
            and shadow["summary"]["shadow_mode_only"]
            and shadow["summary"]["operator_gate_required_for_promotion"]
            and not shadow["summary"]["live_route_mutation_enabled"],
            ["kernel/model_intelligence.py", "tests/test_kernel_model_intelligence.py", "model_shadow_ops.json"],
        ),
        _pre_live_goal(
            "research_retrieval",
            "Research Engine retrieval planning, grants, acquisition, and evidence bundles",
            source_status["kernel_research"] and source_status["research_tests"],
            workflow_has_events_and_grants
            and "research_findings" in first_project["artifact_contract"]["sections"],
            ["kernel/research.py", "tests/test_kernel_research.py", "first_live_project_packet.json"],
        ),
        _pre_live_goal(
            "council_execution",
            "Council/scarce-deliberation records for high-uncertainty decisions",
            source_status["kernel_commercial"] and source_status["research_tests"],
            adapter["summary"]["authority_boundary_case_count"] >= 8
            and "customer_visible_commitments" in target_run["fail_closed_controls"],
            ["kernel/commercial.py", "tests/test_kernel_research.py", "hermes_adapter_gauntlet.json"],
        ),
        _pre_live_goal(
            "hermes_adapter_proxy",
            "Hermes adapter, migration, and proxy enforcement readiness",
            source_status["kernel_runtime"] and source_status["kernel_runtime_compat"],
            adapter["summary"]["all_surfaces_covered"]
            and adapter["summary"]["surface_count"] >= 10
            and readiness.get("ok")
            and target_run["execution_order_contract"]["recovery_and_migration_before_adapter"],
            ["kernel/runtime.py", "kernel/runtime_compat.py", "hermes_adapter_gauntlet.json"],
        ),
        _pre_live_goal(
            "side_effect_delivery",
            "Side-effect and customer-visible delivery governance",
            source_status["kernel_commercial"] and source_status["foundation_tests"],
            first_project["artifact_contract"]["external_delivery"] == "prepared_intent_only_until_operator_gate"
            and "side_effect_replay" in target_run["fail_closed_controls"]
            and not closed_controls.get("side_effect_replay_enabled", True),
            ["kernel/commercial.py", "tests/test_kernel_foundation.py", "first_live_project_packet.json"],
        ),
        _pre_live_goal(
            "operator_gate_surface",
            "Local operator command and gate surface",
            source_status["kernel_runtime_compat"] and source_status["runtime_tests"],
            mission_ready
            and closed_control_ok
            and "operator_gate_before_external_delivery" in target_run["run_steps"][6]["required_evidence"],
            ["kernel/runtime_compat.py", "tests/test_skills/test_runtime.py", "pre_live_mission_control.json"],
        ),
        _pre_live_goal(
            "data_governance",
            "Artifact, redaction, encrypted storage, backup, and recovery proof",
            source_status["kernel_store_artifacts"] and source_status["kernel_store_recovery"],
            readiness.get("ok")
            and target_run["execution_order_contract"]["recovery_and_migration_before_adapter"]
            and "artifact_descriptor_verified" in target_run["run_steps"][2]["required_evidence"],
            ["kernel/store_artifacts.py", "kernel/store_recovery.py", "target_machine_validation_run_packet.json"],
        ),
        _pre_live_goal(
            "evidence_packaging",
            "Replay/projection comparisons and pre-live evidence packaging",
            source_status["kernel_pre_live"] and source_status["pre_live_tests"],
            target_ready
            and all(item["exists"] and item["sha256"] for item in component_artifacts.values())
            and "target_machine_artifact_bundle" in target_run["run_steps"][-1]["required_evidence"],
            ["kernel/pre_live.py", "tests/test_kernel_pre_live.py", "target_machine_validation_run_packet.json"],
        ),
    ]
    goal_blockers = [
        f"{goal['goal_id']}:{blocker}"
        for goal in goals
        for blocker in goal["blockers"]
    ]
    completion_contract = {
        "all_ten_goals_complete": not goal_blockers and len(goals) == 10,
        "closed_control_contract_ok": closed_control_ok,
        "component_artifacts_hash_bound": bool(component_artifacts)
        and all(item["exists"] and item["sha256"] for item in component_artifacts.values()),
        "no_live_authority_activation": (
            target_run.get("live_controls_enabled") is False
            and efficiency.get("live_controls_enabled") is False
            and mission.get("live_controls_enabled") is False
            and target_run.get("status") == "ready_for_target_machine_execution"
        ),
    }
    contract_blockers = [
        f"completion_contract:{key}"
        for key, ok in completion_contract.items()
        if not ok
    ]
    blockers = goal_blockers + contract_blockers
    payload = {
        "available": True,
        "generated_at": generated_at,
        "packet_name": "pre_live_completion_bundle",
        "status": "complete_pre_live_coding" if not blockers else "blocked",
        "repo_root": str(repo_root),
        "summary": {
            "completed_goals": sum(1 for goal in goals if goal["complete"]),
            "total_goals": len(goals),
            "all_ten_complete": not blockers,
            "mission_go_no_go": mission.get("go_no_go"),
            "target_machine_run_status": target_run.get("status"),
            "live_controls_enabled": False,
        },
        "goals": goals,
        "source_status": source_status,
        "component_artifacts": component_artifacts,
        "completion_contract": completion_contract,
        "closed_control_contract": closed_controls,
        "blockers": blockers,
        "activation_effect": "none_until_target_machine_evidence_and_operator_gates_pass",
        "live_controls_enabled": False,
        "artifact_path": str(artifact_path),
    }
    return payload


def _pre_live_goal(
    goal_id: str,
    title: str,
    implementation_present: bool,
    proof_present: bool,
    evidence_refs: list[str],
) -> dict[str, Any]:
    blockers = []
    if not implementation_present:
        blockers.append("implementation_surface_missing")
    if not proof_present:
        blockers.append("proof_surface_missing_or_open_control")
    return {
        "goal_id": goal_id,
        "title": title,
        "complete": not blockers,
        "blockers": blockers,
        "evidence_refs": evidence_refs,
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
    evidence_step_by_id = {
        str(item): {
            "step": step.get("step"),
            "name": step.get("name"),
        }
        for step in packet.get("run_steps", [])
        if isinstance(step, dict)
        for item in step.get("required_evidence", [])
    }
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
    required_replay_projection_evidence = [
        item for item in TARGET_MACHINE_REPLAY_PROJECTION_EVIDENCE if item in required_evidence
    ]
    missing_replay_projection_evidence = [
        item for item in TARGET_MACHINE_REPLAY_PROJECTION_EVIDENCE if item not in required_evidence
    ]
    ambiguous_replay_projection_evidence = [
        item
        for item in required_replay_projection_evidence
        if item in ambiguous_required_evidence
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
    proof_contract = packet.get("replay_projection_proof_contract", {})
    proof_records = packet.get("replay_projection_proof_records", [])
    required_artifact_names = sorted(
        str(item.get("name") or Path(str(item.get("path", ""))).stem)
        for item in packet.get("evidence_manifest", [])
        if isinstance(item, dict)
    )
    evidence_record_binding_failures = _target_machine_evidence_record_binding_failures(
        evidence_records=evidence_records,
        evidence_step_by_id=evidence_step_by_id,
        required_artifact_names=required_artifact_names,
    )
    replay_projection_contract = {
        "run_packet_proof_contract_declared": isinstance(proof_contract, dict)
        and all(proof_contract.get(key) is True for key in TARGET_MACHINE_REPLAY_PROJECTION_PROOF_KEYS),
        "run_packet_proof_records_distinguish_replay_projection_effects": (
            _replay_projection_proof_records_distinguish_effects(proof_records)
        ),
        "required_replay_projection_evidence_declared": not missing_replay_projection_evidence,
        "required_replay_projection_evidence_non_ambiguous": (
            bool(required_replay_projection_evidence)
            and not ambiguous_replay_projection_evidence
            and all(item not in missing_required_evidence for item in required_replay_projection_evidence)
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
        "preserved_evidence_records_bound_to_contract": not evidence_record_binding_failures,
    }
    if evidence_record_binding_failures:
        blockers.append("preserved_evidence_record_binding_missing")
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
        "required_replay_projection_evidence": required_replay_projection_evidence,
        "missing_replay_projection_evidence": missing_replay_projection_evidence,
        "ambiguous_replay_projection_evidence": ambiguous_replay_projection_evidence,
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


def _replay_projection_proof_records_distinguish_effects(records: Any) -> bool:
    if not isinstance(records, list) or not records:
        return False
    required_proof_keys = set(TARGET_MACHINE_REPLAY_PROJECTION_PROOF_KEYS)
    seen_proof_keys: set[str] = set()
    for record in records:
        if not isinstance(record, dict):
            return False
        proof_key = record.get("proof_contract_key")
        if proof_key not in required_proof_keys:
            return False
        seen_proof_keys.add(str(proof_key))
        if not record.get("evidence_id"):
            return False
        if not record.get("reconstructed_intent"):
            return False
        if not record.get("projected_state"):
            return False
        forbidden = record.get("forbidden_side_effect_reexecution")
        if not isinstance(forbidden, dict) or forbidden.get("allowed") is not False:
            return False
        if not forbidden.get("control"):
            return False
    return required_proof_keys.issubset(seen_proof_keys)


def _target_machine_evidence_record_binding_failures(
    *,
    evidence_records: dict[str, dict[str, Any]],
    evidence_step_by_id: dict[str, dict[str, Any]],
    required_artifact_names: list[str],
) -> list[str]:
    failures: list[str] = []
    required_artifacts = set(required_artifact_names)
    for evidence_id, record in sorted(evidence_records.items()):
        step = evidence_step_by_id.get(evidence_id)
        if not step:
            failures.append(f"{evidence_id}:run_step")
            continue

        bound_step = record.get("run_step_name") or record.get("run_step")
        bound_step_number = record.get("run_step_number")
        step_name = step.get("name")
        step_number = step.get("step")
        if bound_step != step_name and bound_step_number != step_number:
            failures.append(f"{evidence_id}:run_step")

        bound_artifacts = record.get("artifact_names", record.get("artifacts", record.get("artifact_name")))
        if isinstance(bound_artifacts, str):
            bound_artifact_names = {bound_artifacts}
        elif isinstance(bound_artifacts, list):
            bound_artifact_names = {str(item) for item in bound_artifacts if item}
        else:
            bound_artifact_names = set()
        if not bound_artifact_names or not bound_artifact_names.issubset(required_artifacts):
            failures.append(f"{evidence_id}:artifact")

        proof_key = TARGET_MACHINE_REPLAY_PROJECTION_EVIDENCE_PROOF_KEYS.get(evidence_id)
        if proof_key and record.get("proof_contract_key") != proof_key:
            failures.append(f"{evidence_id}:proof_contract_key")
    return failures


def first_live_project_acceptance_check_packet(
    *,
    packet: dict[str, Any],
    generated_at: str,
    artifact_path: Path,
) -> dict[str, Any]:
    workflow = packet.get("workflow", [])
    dry_run = packet.get("dry_run", {})
    close_path = dry_run.get("close_path", {}) if isinstance(dry_run, dict) else {}
    operator_packet = packet.get("operator_acceptance_packet", {})
    acceptance_contract = _first_live_project_acceptance_contract(
        packet=packet,
        workflow=workflow,
        close_path=close_path,
        operator_packet=operator_packet,
    )
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
    blockers.extend(
        f"operator_acceptance_contract_{name}"
        for name, ok in acceptance_contract.items()
        if not ok
    )
    return {
        "available": True,
        "generated_at": generated_at,
        "packet_name": "first_live_project_acceptance_check",
        "status": "accepted_pre_live_local_only" if not blockers else "blocked",
        "fixture_id": packet.get("fixture_id"),
        "checks": checks,
        "acceptance_contract": acceptance_contract,
        "blockers": blockers,
        "live_controls_enabled": False,
        "activation_effect": "none",
        "artifact_path": str(artifact_path),
    }


def _first_live_project_acceptance_contract(
    *,
    packet: dict[str, Any],
    workflow: Any,
    close_path: dict[str, Any],
    operator_packet: Any,
) -> dict[str, bool]:
    if not isinstance(operator_packet, dict):
        operator_packet = {}
    delivery = operator_packet.get("customer_visible_delivery", {})
    feedback = operator_packet.get("feedback_ingestion", {})
    commitments = operator_packet.get("external_commitments", {})
    signoff = operator_packet.get("operator_signoff", {})
    signoffs_required = packet.get("operator_signoffs_required", [])
    workflow_items = [item for item in workflow if isinstance(item, dict)] if isinstance(workflow, list) else []
    return {
        "customer_visible_delivery_bound": (
            isinstance(delivery, dict)
            and delivery.get("artifact_contract_key") == "external_delivery"
            and delivery.get("expected_value") == "prepared_intent_only_until_operator_gate"
            and delivery.get("local_artifact_only") is True
            and delivery.get("operator_gate_required") is True
            and packet.get("artifact_contract", {}).get("external_delivery") == delivery.get("expected_value")
        ),
        "feedback_ingestion_bound": (
            isinstance(feedback, dict)
            and feedback.get("close_path_key") == "feedback_ingested"
            and feedback.get("feedback_ingested") is True
            and feedback.get("close_or_continue_requires_operator_gate") is True
            and close_path.get("feedback_ingested") is True
            and close_path.get("close_or_continue_requires_operator_gate") is True
        ),
        "external_commitments_bound": (
            isinstance(commitments, dict)
            and commitments.get("project_key") == "external_commitments_allowed"
            and commitments.get("allowed") is False
            and commitments.get("workflow_external_side_effects_executed") is False
            and packet.get("summary", {}).get("external_commitments_allowed") is False
            and all(item.get("external_side_effects_executed") is False for item in workflow_items)
        ),
        "operator_signoff_bound": (
            isinstance(signoff, dict)
            and signoff.get("required_authority") == "operator_gate"
            and signoff.get("default_on_timeout") == "keep_local_only"
            and isinstance(signoff.get("required_signoffs"), list)
            and bool(signoff.get("required_signoffs"))
            and set(signoff.get("required_signoffs", [])).issubset(set(signoffs_required))
        ),
        "fail_closed": operator_packet.get("fail_closed_unless_all_bindings_present") is True,
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


def pre_live_evidence_crosswalk_contract(
    rows: list[dict[str, Any]],
    *,
    run_packet_status: str,
    closed_control_ok: bool,
) -> dict[str, Any]:
    all_rows_ready = bool(rows) and all(row.get("ready") is True for row in rows)
    all_rows_have_steps = bool(rows) and all(row.get("mapped_step_count", 0) > 0 for row in rows)
    all_rows_have_artifacts = bool(rows) and all(row.get("mapped_artifact_count", 0) > 0 for row in rows)
    all_rows_have_required_evidence = bool(rows) and all(bool(row.get("required_evidence")) for row in rows)
    all_rows_have_closed_control_keys = bool(rows) and all(bool(row.get("closed_control_keys")) for row in rows)
    all_rows_have_blocker_conditions = bool(rows) and all(bool(row.get("blocker_conditions")) for row in rows)
    no_missing_mappings = bool(rows) and all(
        not row.get("missing_steps") and not row.get("missing_artifacts") for row in rows
    )
    no_opened_controls = bool(rows) and all(not row.get("opened_controls") for row in rows)
    all_artifacts_hash_bound = bool(rows) and all(
        artifact.get("exists") is True
        and bool(artifact.get("sha256"))
        and artifact.get("required_before_live_authority") is True
        for row in rows
        for artifact in row.get("artifact_checks", [])
    )
    all_rows_have_artifact_checks = bool(rows) and all(bool(row.get("artifact_checks")) for row in rows)
    contract = {
        "run_packet_ready": run_packet_status == "ready_for_target_machine_execution",
        "closed_control_contract_ok": closed_control_ok,
        "all_rows_ready": all_rows_ready,
        "all_rows_have_steps": all_rows_have_steps,
        "all_rows_have_artifacts": all_rows_have_artifacts,
        "all_rows_have_required_evidence": all_rows_have_required_evidence,
        "all_rows_have_closed_control_keys": all_rows_have_closed_control_keys,
        "all_rows_have_blocker_conditions": all_rows_have_blocker_conditions,
        "no_missing_mappings": no_missing_mappings,
        "no_opened_controls": no_opened_controls,
        "all_rows_have_artifact_checks": all_rows_have_artifact_checks,
        "all_artifacts_hash_bound_before_live_authority": (
            all_rows_have_artifact_checks and all_artifacts_hash_bound
        ),
    }
    blocker_names = {
        "run_packet_ready": "pre_live_crosswalk_run_packet_not_ready",
        "closed_control_contract_ok": "pre_live_crosswalk_closed_control_contract_open",
        "all_rows_ready": "pre_live_crosswalk_rows_not_ready",
        "all_rows_have_steps": "pre_live_crosswalk_rows_missing_steps",
        "all_rows_have_artifacts": "pre_live_crosswalk_rows_missing_artifacts",
        "all_rows_have_required_evidence": "pre_live_crosswalk_rows_missing_required_evidence",
        "all_rows_have_closed_control_keys": "pre_live_crosswalk_rows_missing_closed_control_keys",
        "all_rows_have_blocker_conditions": "pre_live_crosswalk_rows_missing_blocker_conditions",
        "no_missing_mappings": "pre_live_crosswalk_rows_have_missing_mappings",
        "no_opened_controls": "pre_live_crosswalk_rows_have_opened_controls",
        "all_rows_have_artifact_checks": "pre_live_crosswalk_rows_missing_artifact_checks",
        "all_artifacts_hash_bound_before_live_authority": "pre_live_crosswalk_artifacts_not_hash_bound",
    }
    return {
        "contract": contract,
        "blockers": sorted({blocker_names[key] for key, ok in contract.items() if not ok}),
    }


def pre_live_bundle_verification_packet(
    *,
    bundle: Path,
    generated_at: str,
    artifact_path: Path,
) -> dict[str, Any]:
    """Build the pre-live bundle verification packet without runtime side effects."""
    sha_entries = parse_sha256sums(bundle / "SHA256SUMS")
    required_json_files = [
        "pre_live_mission_control.json",
        "hermes_adapter_gauntlet.json",
        "first_live_project_packet.json",
        "model_shadow_ops.json",
        "target_machine_validation_run_packet.json",
    ]
    optional_json_files = ["model_efficiency_service_packet.json"]
    expected_json_files = {*required_json_files, *optional_json_files}
    inert_declarations = _bundle_inert_artifact_declarations(bundle)
    extra_artifact_checks = _pre_live_bundle_extra_artifact_checks(
        bundle=bundle,
        expected_filenames=expected_json_files,
        inert_declarations=inert_declarations,
    )
    file_checks = []
    for filename in [*required_json_files, *optional_json_files]:
        path = bundle / filename
        parsed = _bundle_json(path)
        actual_hash = file_sha256(path)
        manifest_hash = sha_entries.get(filename)
        file_checks.append(
            {
                "filename": filename,
                "required": filename in required_json_files,
                "exists": path.is_file(),
                "json_non_empty": bool(parsed),
                "live_controls_disabled": pre_live_artifact_controls_disabled(parsed) if parsed else False,
                "sha256": actual_hash,
                "matches_sha256sums": bool(actual_hash and manifest_hash and actual_hash == manifest_hash),
                "status": parsed.get("status") or parsed.get("go_no_go") or parsed.get("packet_name"),
            }
        )
    run_packet = _bundle_json(bundle / "target_machine_validation_run_packet.json")
    mission = _bundle_json(bundle / "pre_live_mission_control.json")
    closed_control_contract = run_packet.get("closed_control_contract", {})
    execution_order_contract = run_packet.get("execution_order_contract", {})
    blockers = []
    if not bundle.is_dir():
        blockers.append("pre_live_bundle_missing")
    if not sha_entries:
        blockers.append("sha256sums_missing_or_empty")
    if any(item["required"] and not item["exists"] for item in file_checks):
        blockers.append("required_pre_live_artifact_missing")
    if any(item["required"] and not item["json_non_empty"] for item in file_checks):
        blockers.append("required_pre_live_artifact_empty_or_invalid")
    if any(item["required"] and not item["matches_sha256sums"] for item in file_checks):
        blockers.append("required_pre_live_artifact_checksum_mismatch")
    if any(item["required"] and not item["live_controls_disabled"] for item in file_checks):
        blockers.append("required_pre_live_artifact_live_controls_not_disabled")
    if run_packet.get("status") not in {"ready_for_target_machine_execution", "blocked"}:
        blockers.append("target_machine_run_packet_status_missing")
    if (
        run_packet.get("status") == "ready_for_target_machine_execution"
        and mission.get("go_no_go") != "ready_for_target_machine_validation"
    ):
        blockers.append("mission_control_not_ready_for_target_machine_validation")
    if execution_order_contract and not all(bool(value) for value in execution_order_contract.values()):
        blockers.append("target_machine_execution_order_invalid")
    if closed_control_contract and not pre_live_controls_are_closed(closed_control_contract):
        blockers.append("closed_control_contract_opened_live_control")
    if any(item["authority_bearing"] and not item["declared_inert_read_only"] for item in extra_artifact_checks):
        blockers.append("unexpected_authority_bearing_artifact")
    return {
        "available": True,
        "generated_at": generated_at,
        "packet_name": "pre_live_bundle_verification",
        "status": "verified_pre_live_bundle" if not blockers else "blocked",
        "bundle_dir": str(bundle),
        "sha256sums_path": str(bundle / "SHA256SUMS"),
        "file_checks": file_checks,
        "extra_artifact_checks": extra_artifact_checks,
        "summary": {
            "required_file_count": len(required_json_files),
            "required_files_present": sum(1 for item in file_checks if item["required"] and item["exists"]),
            "required_json_non_empty": all(item["json_non_empty"] for item in file_checks if item["required"]),
            "required_checksums_match": all(item["matches_sha256sums"] for item in file_checks if item["required"]),
            "live_controls_disabled": all(item["live_controls_disabled"] for item in file_checks if item["required"]),
            "target_machine_status": run_packet.get("status"),
            "extra_authority_bearing_artifacts": sum(
                1 for item in extra_artifact_checks if item["authority_bearing"]
            ),
        },
        "blockers": blockers,
        "live_controls_enabled": False,
        "activation_effect": "none",
        "artifact_path": str(artifact_path),
    }


def parse_sha256sums(path: Path) -> dict[str, str]:
    entries: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return entries
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        parts = stripped.split(maxsplit=1)
        if len(parts) != 2 or not re.fullmatch(r"[0-9a-fA-F]{64}", parts[0]):
            continue
        entries[Path(parts[1].lstrip("*")).name] = parts[0].lower()
    return entries


def _bundle_json(path: Path) -> dict[str, Any]:
    parsed = _read_json(path)
    return parsed if parsed is not None else {}


def _bundle_inert_artifact_declarations(bundle_dir: Path) -> dict[str, dict[str, Any]]:
    parsed = _bundle_json(bundle_dir / "evidence_records.json")
    declarations = parsed.get("inert_artifacts", [])
    if not isinstance(declarations, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for declaration in declarations:
        if not isinstance(declaration, dict):
            continue
        filename = declaration.get("filename") or declaration.get("artifact")
        if filename:
            result[Path(str(filename)).name] = declaration
    return result


def _pre_live_bundle_extra_artifact_checks(
    *,
    bundle: Path,
    expected_filenames: set[str],
    inert_declarations: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for path in sorted(bundle.glob("*.json")):
        if path.name in expected_filenames or path.name == "evidence_records.json":
            continue
        parsed = _bundle_json(path)
        authority_bearing = _pre_live_extra_artifact_is_authority_bearing(parsed)
        declaration = inert_declarations.get(path.name, {})
        declared_inert_read_only = (
            isinstance(declaration, dict)
            and declaration.get("mode") == "read_only"
            and declaration.get("authority_effect") == "inert_evidence"
            and declaration.get("may_grant_authority") is False
        )
        checks.append(
            {
                "filename": path.name,
                "json_non_empty": bool(parsed),
                "authority_bearing": authority_bearing,
                "declared_inert_read_only": declared_inert_read_only,
                "status": "allowed_inert_evidence"
                if authority_bearing and declared_inert_read_only
                else ("unexpected_authority_bearing_artifact" if authority_bearing else "ignored_extra_artifact"),
            }
        )
    return checks


def _pre_live_extra_artifact_is_authority_bearing(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict) or not payload:
        return False
    authority_keys = {
        "closed_control_contract",
        "operator_decision_packet",
        "operator_acceptance_packet",
        "required_authority",
        "capability_grants",
        "side_effect_intents",
        "customer_visible_delivery",
        "external_commitments_allowed",
    }
    if any(key in payload for key in authority_keys):
        return True
    if payload.get("live_controls_enabled") is not None:
        return True
    return any(
        isinstance(value, dict) and _pre_live_extra_artifact_is_authority_bearing(value)
        for value in payload.values()
    )


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


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None
