from __future__ import annotations

from pathlib import Path

from skills.config import IntegrationConfig


SUPPORT_ARTIFACT_FILENAMES = {
    "network_controls": "network_controls.json",
    "proxy_allowlist": "proxy_allowlist.json",
    "gateway_manifest": "gateway_manifest.json",
    "workspace_manifest": "workspace_manifest.json",
    "local_provider_doctor": "local_provider_doctor.json",
    "curator_readiness": "curator_readiness.json",
    "operator_validation_checklist": "operator_validation_checklist.md",
    "flywheel_drill_report": "flywheel_drill_report.json",
    "evidence_factory_manifest": "evidence_factory_manifest.json",
    "replay_readiness_report": "replay_readiness_report.json",
    "replay_corpus_export": "replay_corpus_export.json",
    "optimizer_snapshot": "optimizer_snapshot.json",
    "harness_candidate_report": "harness_candidate_report.json",
    "known_bad_hardening_operator_review_summary": "known_bad_hardening_operator_review_summary.json",
    "known_bad_hardening_follow_on_review": "known_bad_hardening_follow_on_review.json",
    "known_bad_hardening_operator_patch_gate": "known_bad_hardening_operator_patch_gate.json",
    "mac_studio_day_one_handoff": "mac_studio_day_one_handoff.md",
    "recovery_readiness": "recovery_readiness.json",
    "hermes_adapter_readiness": "hermes_adapter_readiness.json",
    "migration_readiness": "migration_readiness.json",
    "pre_hermes_readiness": "pre_hermes_readiness.json",
    "pre_live_mission_control": "pre_live_mission_control.json",
    "hermes_adapter_gauntlet": "hermes_adapter_gauntlet.json",
    "first_live_project_packet": "first_live_project_packet.json",
    "model_shadow_ops": "model_shadow_ops.json",
    "target_machine_validation_run_packet": "target_machine_validation_run_packet.json",
    "pre_live_bundle_verification": "pre_live_bundle_verification.json",
    "target_machine_evidence_check": "target_machine_evidence_check.json",
    "first_live_project_acceptance_check": "first_live_project_acceptance_check.json",
    "model_efficiency_service_packet": "model_efficiency_service_packet.json",
    "model_efficiency_customer_validation_brief": "model_efficiency_customer_validation_brief.json",
    "pre_live_completion_bundle": "pre_live_completion_bundle.json",
    "pre_live_evidence_crosswalk": "pre_live_evidence_crosswalk.json",
    "self_improvement_snapshot": "self_improvement_snapshot.json",
}


def runtime_bundle_dir(config: IntegrationConfig) -> Path:
    return Path(config.skills_dir) / "runtime"


def runtime_support_artifact_paths(config: IntegrationConfig) -> dict[str, Path]:
    bundle = runtime_bundle_dir(config)
    return {name: bundle / filename for name, filename in SUPPORT_ARTIFACT_FILENAMES.items()}


def runtime_launcher_paths(config: IntegrationConfig) -> dict[str, Path]:
    bin_dir = runtime_bundle_dir(config) / "bin"
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
        "known_bad_hardening_operator_review_summary": bin_dir / "known_bad_hardening_operator_review_summary.sh",
        "known_bad_hardening_follow_on_review": bin_dir / "known_bad_hardening_follow_on_review.sh",
        "known_bad_hardening_operator_patch_gate": bin_dir / "known_bad_hardening_operator_patch_gate.sh",
        "mac_studio_day_one": bin_dir / "mac_studio_day_one.sh",
        "recovery_readiness": bin_dir / "recovery_readiness.sh",
        "hermes_adapter_readiness": bin_dir / "hermes_adapter_readiness.sh",
        "migration_readiness": bin_dir / "migration_readiness.sh",
        "pre_hermes_readiness": bin_dir / "pre_hermes_readiness.sh",
        "pre_live_mission_control": bin_dir / "pre_live_mission_control.sh",
        "hermes_adapter_gauntlet": bin_dir / "hermes_adapter_gauntlet.sh",
        "first_live_project_packet": bin_dir / "first_live_project_packet.sh",
        "model_shadow_ops": bin_dir / "model_shadow_ops.sh",
        "target_machine_validation_run_packet": bin_dir / "target_machine_validation_run_packet.sh",
        "pre_live_bundle_verification": bin_dir / "pre_live_bundle_verification.sh",
        "target_machine_evidence_check": bin_dir / "target_machine_evidence_check.sh",
        "first_live_project_acceptance_check": bin_dir / "first_live_project_acceptance_check.sh",
        "model_efficiency_service_packet": bin_dir / "model_efficiency_service_packet.sh",
        "model_efficiency_customer_validation_brief": bin_dir / "model_efficiency_customer_validation_brief.sh",
        "pre_live_completion_bundle": bin_dir / "pre_live_completion_bundle.sh",
        "pre_live_evidence_crosswalk": bin_dir / "pre_live_evidence_crosswalk.sh",
        "self_improvement_evidence_pipeline": bin_dir / "self_improvement_evidence_pipeline.sh",
        "self_improvement_snapshot": bin_dir / "self_improvement_snapshot.sh",
        "gateway": bin_dir / "start_gateway.sh",
        "workspace": bin_dir / "start_workspace.sh",
        "operator_checklist": bin_dir / "operator_validation_checklist.sh",
        "milestone_status": bin_dir / "milestone_status.sh",
        "workspace_overview": bin_dir / "workspace_overview.sh",
    }
