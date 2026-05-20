from __future__ import annotations

import json
import hashlib
import sqlite3
import sys
from pathlib import Path

import pytest

from kernel import runtime_compat
from harness_variants import HarnessVariantManager
from kernel import KernelStore
from runtime_control import RuntimeControlManager
from skills.config import IntegrationConfig
from skills.hermes_interfaces import HermesSessionContext, MockHermesRuntime
from skills.runtime import (
    VERSION_DRIFT_NOTE,
    ExternalCommandResult,
    _symlink_skill_directory,
    analyze_harness_candidates,
    assess_hermes_readiness,
    build_mac_studio_day_one_handoff,
    bootstrap_stack,
    bootstrap_runtime,
    doctor_runtime,
    export_replay_corpus,
    exercise_hermes_contract,
    hermes_adapter_readiness,
    install_runtime_profile,
    known_bad_hardening_follow_on_review_packet,
    known_bad_hardening_operator_patch_gate,
    known_bad_hardening_operator_review_bundle,
    known_bad_hardening_operator_review_summary,
    known_bad_hardening_shadow_report,
    make_session_context,
    main as runtime_main,
    migration_readiness,
    first_live_project_packet,
    first_live_project_acceptance_check,
    hermes_adapter_gauntlet,
    model_efficiency_service_packet,
    model_shadow_ops,
    migrate_runtime_databases,
    optimizer_snapshot,
    pre_live_evidence_crosswalk,
    pre_live_mission_control,
    pre_live_completion_bundle,
    pre_hermes_readiness,
    prepare_runtime_directories,
    readiness_suite,
    recovery_readiness,
    replay_readiness_report,
    require_runtime_databases,
    self_improvement_evidence_pipeline,
    self_improvement_snapshot,
    target_machine_validation_run_packet,
    pre_live_bundle_verification,
    target_machine_evidence_check,
    run_flywheel_drill,
    run_research_cron_proof,
    run_evidence_factory,
    run_proxy_self_test,
    run_task_loop_proof,
    run_operator_workflow,
    workspace_overview,
)


def test_prepare_runtime_directories_creates_layout(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    resolved = prepare_runtime_directories(cfg)
    assert Path(resolved.data_dir).is_dir()
    assert Path(resolved.skills_dir).is_dir()
    assert Path(resolved.checkpoints_dir).is_dir()
    assert Path(resolved.alerts_dir).is_dir()
    assert (tmp_path / "logs").is_dir()


def test_migrate_runtime_databases_builds_all_sqlite_files(tmp_path):
    cfg = IntegrationConfig(data_dir=str(tmp_path / "data"))
    status = migrate_runtime_databases(cfg)
    assert all(status.values())
    assert (tmp_path / "data" / "strategic_memory.db").exists()
    assert (tmp_path / "data" / "immune_system.db").exists()
    assert (tmp_path / "data" / "telemetry.db").exists()
    assert (tmp_path / "data" / "financial_ledger.db").exists()
    assert (tmp_path / "data" / "operator_digest.db").exists()


def test_require_runtime_databases_fails_closed_on_schema_drift(tmp_path):
    cfg = IntegrationConfig(data_dir=str(tmp_path / "data"))
    migrate_runtime_databases(cfg)
    operator_db = tmp_path / "data" / "operator_digest.db"
    with sqlite3.connect(operator_db) as conn:
        conn.execute("ALTER TABLE digest_history RENAME TO digest_history__new")
        conn.execute(
            """
            CREATE TABLE digest_history (
              digest_id TEXT PRIMARY KEY,
              digest_type TEXT NOT NULL CHECK (digest_type IN ('daily', 'catch_up')),
              content TEXT NOT NULL,
              sections_included TEXT NOT NULL CHECK (json_valid(sections_included)),
              word_count INTEGER NOT NULL,
              operator_state TEXT NOT NULL CHECK (operator_state IN ('ACTIVE', 'CONSERVATIVE', 'ABSENT')),
              delivered_at TEXT,
              acknowledged_at TEXT,
              created_at TEXT NOT NULL
            ) STRICT
            """
        )
        conn.execute("DROP TABLE digest_history__new")
        conn.commit()

    try:
        require_runtime_databases(cfg)
    except RuntimeError as exc:
        assert "operator_digest" in str(exc)
    else:
        raise AssertionError("expected schema drift to fail closed")


def test_runtime_control_manager_does_not_create_telemetry_db_when_missing(tmp_path):
    operator_db = tmp_path / "operator_digest.db"
    with sqlite3.connect(operator_db) as conn:
        conn.executescript(Path("schemas/operator_digest.sql").read_text(encoding="utf-8"))
        conn.commit()

    telemetry_db = tmp_path / "telemetry.db"
    assert not telemetry_db.exists()

    manager = RuntimeControlManager(str(operator_db))

    assert manager.available is True
    assert not telemetry_db.exists()


def test_runtime_control_reuses_active_halt_without_logging_extra_trace(tmp_path):
    cfg = IntegrationConfig(data_dir=str(tmp_path / "data"))
    migrate_runtime_databases(cfg)

    manager = RuntimeControlManager(str(tmp_path / "data" / "operator_digest.db"))
    traces = HarnessVariantManager(str(tmp_path / "data" / "telemetry.db"))

    first = manager.activate_halt(
        source="MANUAL_TEST",
        halt_reason="runtime_halt_contract_test",
        reference_time="2026-04-23T10:00:00+00:00",
    )
    second = manager.activate_halt(
        source="MANUAL_TEST",
        halt_reason="runtime_halt_contract_test",
        reference_time="2026-04-23T10:01:00+00:00",
    )

    runtime_traces = traces.list_execution_traces(limit=10, skill_name="runtime")

    assert second["halt_id"] == first["halt_id"]
    assert [row["role"] for row in runtime_traces] == ["runtime_halt_activation"]


def test_make_session_context_uses_resolved_profile_and_data_dir(tmp_path):
    cfg = IntegrationConfig(data_dir=str(tmp_path / "data"), profile_name="hybrid-test")
    ctx = make_session_context(cfg, model_name="gpt-local")
    assert ctx.profile_name == "hybrid-test"
    assert ctx.model_name == "gpt-local"
    assert ctx.data_dir == str(tmp_path / "data")
    assert ctx.session_id


def test_bootstrap_runtime_migrates_and_registers_skills(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    rt = MockHermesRuntime(data_dir=str(tmp_path / "data"))
    result = bootstrap_runtime(rt, config=cfg, model_name="gpt-local")
    assert result.ok is True
    assert set(result.database_status) == {
        "strategic_memory",
        "telemetry",
        "immune_system",
        "financial_ledger",
        "operator_digest",
    }
    assert "immune_system" in result.registered_tools
    assert "strategic_memory" in result.registered_tools


def test_bootstrap_runtime_uses_supplied_session_context(tmp_path):
    cfg = IntegrationConfig(data_dir=str(tmp_path / "data"))
    ctx = HermesSessionContext("session-fixed", "profile-fixed", "model-fixed", {}, str(tmp_path / "data"))
    rt = MockHermesRuntime(data_dir=str(tmp_path / "data"))
    result = bootstrap_runtime(rt, config=cfg, session_context=ctx)
    assert result.ok is True
    assert result.session_context is ctx


def test_install_runtime_profile_writes_manifest_and_launchers(tmp_path):
    repo_root = tmp_path / "repo"
    (repo_root / "skills" / "immune_system").mkdir(parents=True)
    (repo_root / "skills" / "strategic_memory").mkdir(parents=True)
    (repo_root / "skills" / "immune_system" / "manifest.yaml").write_text("name: immune_system\n", encoding="utf-8")
    (repo_root / "skills" / "strategic_memory" / "manifest.yaml").write_text("name: strategic_memory\n", encoding="utf-8")
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "bundle"),
        checkpoints_dir=str(tmp_path / "bundle" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
        profile_name="hybrid-test",
    )

    result = install_runtime_profile(cfg, repo_root=str(repo_root))

    manifest_path = Path(result.profile_manifest_path)
    profile_config_path = Path(result.profile_config_path)
    spec_profile_path = Path(result.spec_profile_path)
    assert manifest_path.is_file()
    assert profile_config_path.is_file()
    assert spec_profile_path.is_file()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    profile_config = json.loads(profile_config_path.read_text(encoding="utf-8"))
    spec_profile = json.loads(spec_profile_path.read_text(encoding="utf-8"))
    workspace_manifest = json.loads(Path(manifest["workspace_manifest_path"]).read_text(encoding="utf-8"))
    assert manifest["profile_name"] == "hybrid-test"
    assert manifest["repo_root"] == str(repo_root.resolve())
    assert manifest["profile_config_path"] == str(profile_config_path)
    assert manifest["spec_profile_path"] == str(spec_profile_path)
    assert Path(manifest["network_controls_path"]).is_file()
    assert Path(manifest["proxy_allowlist_path"]).is_file()
    assert Path(manifest["gateway_manifest_path"]).is_file()
    assert Path(manifest["workspace_manifest_path"]).is_file()
    assert Path(manifest["local_provider_doctor_path"]).is_file()
    assert Path(manifest["curator_readiness_path"]).is_file()
    assert Path(manifest["operator_validation_checklist_path"]).is_file()
    assert Path(manifest["flywheel_drill_report_path"]).is_file()
    assert Path(manifest["evidence_factory_manifest_path"]).is_file()
    assert Path(manifest["replay_readiness_report_path"]).is_file()
    assert Path(manifest["replay_corpus_export_path"]).is_file()
    assert Path(manifest["optimizer_snapshot_path"]).is_file()
    assert Path(manifest["harness_candidate_report_path"]).is_file()
    assert Path(manifest["mac_studio_day_one_handoff_path"]).is_file()
    assert Path(manifest["recovery_readiness_path"]).is_file()
    assert Path(manifest["hermes_adapter_readiness_path"]).is_file()
    assert Path(manifest["migration_readiness_path"]).is_file()
    assert Path(manifest["pre_hermes_readiness_path"]).is_file()
    assert Path(manifest["pre_live_mission_control_path"]).is_file()
    assert Path(manifest["hermes_adapter_gauntlet_path"]).is_file()
    assert Path(manifest["first_live_project_packet_path"]).is_file()
    assert Path(manifest["model_shadow_ops_path"]).is_file()
    assert Path(manifest["model_efficiency_service_packet_path"]).is_file()
    assert Path(manifest["pre_live_completion_bundle_path"]).is_file()
    assert Path(manifest["target_machine_validation_run_packet_path"]).is_file()
    assert Path(manifest["pre_live_bundle_verification_path"]).is_file()
    assert Path(manifest["target_machine_evidence_check_path"]).is_file()
    assert Path(manifest["first_live_project_acceptance_check_path"]).is_file()
    assert Path(manifest["self_improvement_snapshot_path"]).is_file()
    assert "dashboard_plugins" not in manifest
    assert manifest["dashboard"]["mode"] == "hermes_native"
    assert manifest["dashboard"]["custom_plugin"] is False
    assert manifest["dashboard"]["live_controls_enabled"] is False
    assert manifest["dashboard"]["surfaces"] == [
        "Models",
        "Chat",
        "Plugins",
        "Kanban",
        "Agent Profiles",
        "Analytics",
    ]
    assert "--readiness-suite" in workspace_manifest["readiness_suite_command"]
    assert "--pre-live-mission-control" in workspace_manifest["pre_live_mission_control_command"]
    assert "--hermes-adapter-gauntlet" in workspace_manifest["hermes_adapter_gauntlet_command"]
    assert "--first-live-project-packet" in workspace_manifest["first_live_project_packet_command"]
    assert "--model-shadow-ops" in workspace_manifest["model_shadow_ops_command"]
    assert "--model-efficiency-service-packet" in workspace_manifest["model_efficiency_service_packet_command"]
    assert "--pre-live-completion-bundle" in workspace_manifest["pre_live_completion_bundle_command"]
    assert "--target-machine-validation-run-packet" in workspace_manifest["target_machine_validation_run_packet_command"]
    assert "--pre-live-bundle-verification" in workspace_manifest["pre_live_bundle_verification_command"]
    assert "--target-machine-evidence-check" in workspace_manifest["target_machine_evidence_check_command"]
    assert "--first-live-project-acceptance-check" in workspace_manifest["first_live_project_acceptance_check_command"]
    assert "--self-improvement-evidence-pipeline" in workspace_manifest["self_improvement_evidence_pipeline_command"]
    assert "--self-improvement-snapshot" in workspace_manifest["self_improvement_snapshot_command"]
    assert "readiness_suite" in workspace_manifest["read_only_readiness_surfaces"]
    assert "pre_live_mission_control" in workspace_manifest["read_only_readiness_surfaces"]
    assert "hermes_adapter_gauntlet" in workspace_manifest["read_only_readiness_surfaces"]
    assert "first_live_project_packet" in workspace_manifest["read_only_readiness_surfaces"]
    assert "model_shadow_ops" in workspace_manifest["read_only_readiness_surfaces"]
    assert "model_efficiency_service_packet" in workspace_manifest["read_only_readiness_surfaces"]
    assert "pre_live_completion_bundle" in workspace_manifest["read_only_readiness_surfaces"]
    assert "target_machine_validation_run_packet" in workspace_manifest["read_only_readiness_surfaces"]
    assert "pre_live_bundle_verification" in workspace_manifest["read_only_readiness_surfaces"]
    assert "target_machine_evidence_check" in workspace_manifest["read_only_readiness_surfaces"]
    assert "first_live_project_acceptance_check" in workspace_manifest["read_only_readiness_surfaces"]
    assert "self_improvement_evidence_pipeline" in workspace_manifest["read_only_readiness_surfaces"]
    assert "self_improvement_snapshot" in workspace_manifest["read_only_readiness_surfaces"]
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["profile_name"] == "hybrid-test"
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["repo_contract_version"] == 1
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["routing"]["max_api_spend_usd"] == 0.0
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["network_controls"]["proxy_bind_url"] == "http://127.0.0.1:18080"
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["workspace"]["enabled"] is True
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["gateway"]["expected_tools"] == [
        "web_search",
        "web_fetch",
        "image_generation",
        "browser_automation",
        "tts",
    ]
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["workspace"]["preferred_surfaces"] == [
        "models",
        "chat",
        "plugins",
        "gates",
        "execution_traces",
        "quarantines",
        "replay_readiness",
        "recovery_readiness",
        "runtime_halt_state",
        "milestone_health",
    ]
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["local_provider"]["provider"] == "lm_studio"
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["curator"]["mode"] == "report_first"
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["curator"]["pinned_skills_mutable"] is False
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["plugin_hooks"]["required_hooks"] == [
        "pre_tool_call",
        "pre_approval_request",
        "post_approval_response",
        "api_run_approval_event",
    ]
    assert profile_config["approvals"]["mode"] == "manual"
    assert spec_profile["profile"] == "hybrid-test"
    assert spec_profile["limits"]["max_api_spend_usd"] == 0.0
    assert spec_profile["network_controls"]["proxy_bind_url"] == "http://127.0.0.1:18080"
    assert "readiness" in manifest["commands"]
    assert manifest["commands"]["start_proxy"] == result.launcher_paths["start_proxy"]
    assert "start_proxy" in manifest["commands"]
    assert "proxy_self_test" in manifest["commands"]
    assert "contract_harness" in manifest["commands"]
    assert "bootstrap_stack" in manifest["commands"]
    assert "task_loop_proof" in manifest["commands"]
    assert "research_cron_proof" in manifest["commands"]
    assert "flywheel_drill" in manifest["commands"]
    assert "evidence_factory" in manifest["commands"]
    assert "replay_readiness_report" in manifest["commands"]
    assert "export_replay_corpus" in manifest["commands"]
    assert "optimizer_snapshot" in manifest["commands"]
    assert "analyze_harness_candidates" in manifest["commands"]
    assert "propose_best_harness_candidate" in manifest["commands"]
    assert "mac_studio_day_one" in manifest["commands"]
    assert "recovery_readiness" in manifest["commands"]
    assert "hermes_adapter_readiness" in manifest["commands"]
    assert "self_improvement_evidence_pipeline" in manifest["commands"]
    assert "milestone_status" in manifest["commands"]
    assert "mission_control" not in manifest["commands"]
    assert sorted(Path(path).name for path in result.linked_skill_paths) == ["immune_system", "strategic_memory"]
    for launcher_path in result.launcher_paths.values():
        launcher = Path(launcher_path)
        assert launcher.is_file()
        assert launcher.stat().st_mode & 0o111


def test_doctor_runtime_reports_ready_runtime(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    install_runtime_profile(cfg)
    migrate_runtime_databases(cfg)

    rt = MockHermesRuntime(data_dir=str(tmp_path / "data"))
    result = doctor_runtime(rt, config=cfg)

    assert result.ok is True
    assert not result.missing_items
    assert "immune_system" in result.registered_tools
    assert all(result.database_status.values())
    assert result.profile_validation.ok is True
    assert result.path_status["readiness_launcher"] is True
    assert result.path_status["start_proxy_launcher"] is True
    assert result.path_status["proxy_self_test_launcher"] is True
    assert result.path_status["contract_harness_launcher"] is True
    assert result.path_status["local_provider_doctor"] is True
    assert result.path_status["curator_readiness"] is True
    assert result.path_status["flywheel_drill_launcher"] is True
    assert result.path_status["evidence_factory_launcher"] is True
    assert result.path_status["replay_readiness_report_launcher"] is True
    assert result.path_status["mac_studio_day_one_launcher"] is True
    assert result.path_status["recovery_readiness"] is True
    assert result.path_status["recovery_readiness_launcher"] is True
    assert result.path_status["hermes_adapter_readiness"] is True
    assert result.path_status["hermes_adapter_readiness_launcher"] is True


def test_run_proxy_self_test_exercises_real_allow_and_deny_paths(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    result = run_proxy_self_test(cfg)

    assert result.ok is True
    assert result.proxy_url is not None
    assert result.allowed_request_count == 5
    assert result.blocked_request_count == 5
    assert Path(result.audit_log_path).is_file()
    assert result.trace_id is not None

    with sqlite3.connect(tmp_path / "data" / "telemetry.db") as conn:
        trace_row = conn.execute(
            "SELECT role, judge_verdict FROM execution_traces WHERE trace_id = ?",
            (result.trace_id,),
        ).fetchone()
    assert trace_row == ("proxy_self_test", "PASS")


def test_exercise_hermes_contract_runs_full_lifecycle_and_logs_trace(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
        profile_name="hybrid-test",
    )

    result = exercise_hermes_contract(config=cfg, repo_root=str(tmp_path))

    assert result.ok is True
    assert result.bootstrap.ok is True
    assert result.doctor.ok is True
    assert all(result.contract_checks.values())
    assert result.route_decision is not None
    assert result.route_decision["tier"] == "paid_cloud"
    assert result.approval_request is not None
    assert result.approval_review is not None
    assert result.approval_review["status"] == "APPROVED"
    assert result.dispatch_result is not None
    assert result.dispatch_result["dispatch_status"] == "DISPATCHED"
    assert all(result.hermes_contract_checks.values())
    assert result.runtime_halt is not None
    assert result.runtime_halt["source"] == "JUDGE_DEADLOCK"
    assert result.blocked_dispatch_pre_side_effect is True
    assert result.restart_result is not None
    assert result.restart_result["status"] == "COMPLETED"
    assert result.final_runtime_status is not None
    assert result.final_runtime_status["lifecycle_state"] == "ACTIVE"
    assert result.trace_id is not None

    with sqlite3.connect(Path(cfg.data_dir) / "telemetry.db") as conn:
        row = conn.execute(
            "SELECT judge_verdict, retention_class FROM execution_traces WHERE trace_id = ?",
            (result.trace_id,),
        ).fetchone()
    assert row == ("PASS", "STANDARD")


def test_doctor_runtime_detects_corrupted_profile_artifacts(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
        profile_name="hybrid-test",
    )
    install = install_runtime_profile(cfg)
    migrate_runtime_databases(cfg)
    Path(install.profile_config_path).write_text("not-json-yaml\n", encoding="utf-8")

    result = doctor_runtime(config=cfg)

    assert result.ok is False
    assert result.profile_validation.ok is False
    assert "profile:config_yaml_shape" in result.missing_items


def test_assess_hermes_readiness_fails_clearly_without_hermes(tmp_path, monkeypatch):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
        profile_name="hybrid-test",
    )
    monkeypatch.setattr("skills.runtime.shutil.which", lambda _name: None)

    result = assess_hermes_readiness(config=cfg, repo_root=str(tmp_path))

    assert result.ok is False
    assert result.hermes_installed is False
    assert result.doctor.ok is True
    assert result.doctor.profile_validation.ok is True
    assert result.profile_validation.ok is True
    assert all(result.config_status.values())
    assert all(result.database_status.values())
    assert Path(result.checkpoint_backup_path).is_file()
    assert Path(result.install.profile_config_path).is_file()
    assert Path(result.install.spec_profile_path).is_file()
    assert any("not found in PATH" in item for item in result.blocking_items)
    assert "lm_studio_local_provider_doctor" in result.deferred_items
    assert "hermes_z_one_shot_smoke" in result.deferred_items
    assert result.replay_report["growth_plan"]["next_actions"]
    assert result.recommended_actions
    assert VERSION_DRIFT_NOTE in result.drift_items


def test_assess_hermes_readiness_passes_with_live_hermes_signals(tmp_path, monkeypatch):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
        profile_name="hybrid-test",
    )
    (tmp_path / "logs").mkdir()

    monkeypatch.setattr("skills.runtime.shutil.which", lambda _name: "/usr/bin/hermes")

    def runner(argv):
        key = tuple(argv)
        if key == ("hermes", "--version"):
            return ExternalCommandResult(True, key, 0, "Hermes Agent 0.14.0", "")
        if key == ("hermes", "profile", "list"):
            return ExternalCommandResult(True, key, 0, "default\nhybrid-test\n", "")
        if key == ("hermes", "tools", "list"):
            return ExternalCommandResult(
                True,
                key,
                0,
                "\n".join(
                    [
                        "code_execution",
                        "file_operations",
                        "web_search",
                        "web_fetch",
                        "shell_command",
                    ]
                ),
                "",
            )
        if key == ("hermes", "--profile", "hybrid-test", "config", "show"):
            return ExternalCommandResult(
                True,
                key,
                0,
                (tmp_path / "profiles" / "hybrid-test" / "config.yaml").read_text(encoding="utf-8"),
                "",
            )
        if key == ("hermes", "doctor", "providers", "--provider", "lm_studio"):
            return ExternalCommandResult(True, key, 0, "lm_studio ok", "")
        raise AssertionError(f"unexpected command: {key}")

    result = assess_hermes_readiness(
        config=cfg,
        repo_root=str(tmp_path),
        run_cli_smoke=False,
        command_runner=runner,
    )

    assert result.ok is True
    assert result.hermes_installed is True
    assert result.hermes_version == "0.14.0"
    assert result.profile_listed is True
    assert all(result.seed_tool_status.values())
    assert all(result.config_status.values())
    assert result.doctor.profile_validation.ok is True
    assert result.profile_validation.ok is True
    assert Path(result.checkpoint_backup_path).is_file()
    assert result.doctor.ok is True
    assert result.contract_harness.ok is True
    assert result.council_isolation_canary["ok"] is True
    assert result.replay_report["growth_plan"]["next_actions"]
    assert result.recommended_actions
    assert not result.blocking_items


def test_assess_hermes_readiness_rejects_0_8_x_even_if_checklist_allows_it(tmp_path, monkeypatch):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
        profile_name="hybrid-test",
    )
    (tmp_path / "logs").mkdir()

    monkeypatch.setattr("skills.runtime.shutil.which", lambda _name: "/usr/bin/hermes")

    def runner(argv):
        key = tuple(argv)
        if key == ("hermes", "--version"):
            return ExternalCommandResult(True, key, 0, "Hermes Agent 0.8.9", "")
        if key == ("hermes", "profile", "list"):
            return ExternalCommandResult(True, key, 0, "hybrid-test\n", "")
        if key == ("hermes", "tools", "list"):
            return ExternalCommandResult(
                True,
                key,
                0,
                "\n".join(
                    [
                        "code_execution",
                        "file_operations",
                        "web_search",
                        "web_fetch",
                        "shell_command",
                    ]
                ),
                "",
            )
        if key == ("hermes", "--profile", "hybrid-test", "config", "show"):
            return ExternalCommandResult(
                True,
                key,
                0,
                (tmp_path / "profiles" / "hybrid-test" / "config.yaml").read_text(encoding="utf-8"),
                "",
            )
        if key == ("hermes", "doctor", "providers", "--provider", "lm_studio"):
            return ExternalCommandResult(True, key, 0, "lm_studio ok", "")
        raise AssertionError(f"unexpected command: {key}")

    result = assess_hermes_readiness(
        config=cfg,
        repo_root=str(tmp_path),
        run_cli_smoke=False,
        command_runner=runner,
    )

    assert result.ok is False
    assert result.hermes_version == "0.8.9"
    assert result.doctor.profile_validation.ok is True
    assert result.profile_validation.ok is True
    assert any("below the manifest floor" in item for item in result.blocking_items)
    assert VERSION_DRIFT_NOTE in result.drift_items


def test_assess_hermes_readiness_fails_when_live_config_contract_drifts(tmp_path, monkeypatch):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
        profile_name="hybrid-test",
    )
    (tmp_path / "logs").mkdir()

    monkeypatch.setattr("skills.runtime.shutil.which", lambda _name: "/usr/bin/hermes")

    def runner(argv):
        key = tuple(argv)
        if key == ("hermes", "--version"):
            return ExternalCommandResult(True, key, 0, "Hermes Agent 0.14.0", "")
        if key == ("hermes", "profile", "list"):
            return ExternalCommandResult(True, key, 0, "hybrid-test\n", "")
        if key == ("hermes", "tools", "list"):
            return ExternalCommandResult(True, key, 0, "code_execution\nfile_operations\nweb_search\nweb_fetch\nshell_command\n", "")
        if key == ("hermes", "--profile", "hybrid-test", "config", "show"):
            return ExternalCommandResult(
                True,
                key,
                0,
                json.dumps(
                    {
                        "approvals": {"mode": "auto"},
                        "model": {"provider": "custom", "default": "hybrid-autonomous-ai-local", "base_url": "http://127.0.0.1:8080/v1"},
                        "fallback_model": {"provider": "main", "model": "hybrid-autonomous-ai-strong"},
                        "skills": {"config": {"hybrid_autonomous_ai": {"profile_name": "hybrid-test", "routing": {"max_api_spend_usd": 0.0}, "runtime": {"data_dir": str(tmp_path / 'data')}}}},
                    }
                ),
                "",
            )
        if key == ("hermes", "doctor", "providers", "--provider", "lm_studio"):
            return ExternalCommandResult(True, key, 0, "lm_studio ok", "")
        raise AssertionError(f"unexpected command: {key}")

    result = assess_hermes_readiness(
        config=cfg,
        repo_root=str(tmp_path),
        run_cli_smoke=False,
        command_runner=runner,
    )

    assert result.ok is False
    assert result.contract_harness.ok is True
    assert result.config_status["approvals_manual"] is False
    assert result.config_status["dangerous_commands"] is False
    assert result.config_status["repo_contract_version"] is False
    assert result.config_status["gateway_expected_tools"] is False
    assert result.config_status["workspace_preferred_surfaces"] is False
    assert any("profile/config contract assertions failed" in item for item in result.blocking_items)


def test_assess_hermes_readiness_cli_smoke_checks_step_outcome_and_logs(tmp_path, monkeypatch):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
        profile_name="hybrid-test",
    )
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()

    monkeypatch.setattr("skills.runtime.shutil.which", lambda _name: "/usr/bin/hermes")

    def runner(argv):
        key = tuple(argv)
        if key == ("hermes", "--version"):
            return ExternalCommandResult(True, key, 0, "Hermes Agent 0.14.0", "")
        if key == ("hermes", "profile", "list"):
            return ExternalCommandResult(True, key, 0, "hybrid-test\n", "")
        if key == ("hermes", "tools", "list"):
            return ExternalCommandResult(True, key, 0, "code_execution\nfile_operations\nweb_search\nweb_fetch\nshell_command\n", "")
        if key == ("hermes", "--profile", "hybrid-test", "config", "show"):
            return ExternalCommandResult(
                True,
                key,
                0,
                (tmp_path / "profiles" / "hybrid-test" / "config.yaml").read_text(encoding="utf-8"),
                "",
            )
        if key == ("hermes", "doctor", "providers", "--provider", "lm_studio"):
            return ExternalCommandResult(True, key, 0, "lm_studio ok", "")
        if key[:4] == ("hermes", "--profile", "hybrid-test", "-z"):
            query = key[-1]
            marker = query.split("`echo ", 1)[1].split("`", 1)[0]
            db_path = Path(cfg.data_dir) / "telemetry.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO step_outcomes (
                        event_id, step_type, skill, chain_id, outcome, latency_ms,
                        quality_warning, recovery_tier, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("evt-readiness", "shell_command", "shell_command", "readiness-cli", "PASS", 12, 0, None, "2026-04-15T00:00:00+00:00"),
                )
                conn.commit()
            (logs_dir / "errors.log").write_text(f"shell_command marker={marker}\n", encoding="utf-8")
            return ExternalCommandResult(True, key, 0, marker, "")
        raise AssertionError(f"unexpected command: {key}")

    result = assess_hermes_readiness(
        config=cfg,
        repo_root=str(tmp_path),
        command_runner=runner,
    )

    assert result.ok is True
    assert result.cli_smoke_attempted is True
    assert result.cli_smoke_ok is True
    assert result.one_shot_smoke_attempted is True
    assert result.one_shot_smoke_ok is True
    assert result.doctor.profile_validation.ok is True
    assert result.cli_smoke_step_outcomes_delta == 1
    assert result.cli_smoke_log_trace is True
    assert result.cli_smoke_output == result.cli_smoke_marker
    assert Path(result.checkpoint_backup_path).is_file()


def test_task_loop_and_research_cron_proofs_emit_runtime_evidence(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
        profile_name="hybrid-test",
    )
    rt = MockHermesRuntime(data_dir=str(tmp_path / "data"))

    task_loop = run_task_loop_proof(config=cfg, repo_root=str(tmp_path), tool_registry=rt)
    research_cron = run_research_cron_proof(config=cfg, repo_root=str(tmp_path), tool_registry=rt)

    assert task_loop.ok is True
    assert task_loop.task_id is not None
    assert task_loop.brief_id is not None
    assert "opportunity_created" in task_loop.route_summary["action_types"]
    assert research_cron.ok is True
    assert research_cron.standing_brief_id is not None
    assert research_cron.scheduled_job_id is not None
    assert research_cron.queued_task_id is not None


def test_bootstrap_stack_returns_machine_readable_milestone_status(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
        profile_name="hybrid-test",
    )
    rt = MockHermesRuntime(data_dir=str(tmp_path / "data"))

    result = bootstrap_stack(config=cfg, repo_root=str(tmp_path), tool_registry=rt)

    assert result.ok is True
    assert result.proxy_self_test.ok is True
    assert result.milestone_status["milestones"]["M2"]["implemented"] is True
    assert result.milestone_status["milestones"]["M2"]["proof_status"] == "PASS"
    assert result.milestone_status["milestones"]["M3"]["proof_status"] == "PASS"
    assert result.milestone_status["milestones"]["M5"]["proof_status"] == "PASS"


def test_run_operator_workflow_installs_profile_before_final_doctor(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
        profile_name="hybrid-test",
    )
    rt = MockHermesRuntime(data_dir=str(tmp_path / "data"))

    result = run_operator_workflow(rt, config=cfg)

    assert result.ok is True
    assert result.doctor.ok is True
    assert Path(result.doctor.profile_manifest_path).is_file()
    assert result.digest_id
    assert result.digest is not None
    assert result.opportunity_id
    assert result.harvest_id
    assert result.project_id
    assert result.phase_gate_id
    assert result.phase_gate_verdict == "CONTINUE"
    assert len(result.council_verdict_ids) == 2
    assert "PENDING DECISIONS:" in result.digest["content"]
    assert result.observability is not None
    assert result.observability.alert_history
    assert result.observability.council_verdicts
    assert result.observability.digest_history
    assert result.observability.immune_verdicts
    assert result.observability.telemetry_events
    assert any(item["step_type"] == "phase_gate_apply" for item in result.observability.telemetry_events)
    assert result.observability.system_health["heartbeat_state"] == "ACTIVE"
    assert result.observability.system_health["pending_harvests"] == 1
    assert result.trace_id is not None
    with sqlite3.connect(tmp_path / "data" / "telemetry.db") as conn:
        trace_row = conn.execute(
            "SELECT role, judge_verdict, retention_class FROM execution_traces WHERE trace_id = ?",
            (result.trace_id,),
        ).fetchone()
    assert trace_row == ("operator_workflow", "PASS", "STANDARD")


def test_run_operator_workflow_fails_closed_when_runtime_is_halted(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    migrate_runtime_databases(cfg)
    RuntimeControlManager(str(tmp_path / "data" / "operator_digest.db")).activate_halt(
        source="MANUAL_TEST",
        halt_reason="runtime_halt_contract_test",
    )

    rt = MockHermesRuntime(data_dir=str(tmp_path / "data"))
    result = run_operator_workflow(rt, config=cfg)

    assert result.ok is False
    assert result.error is not None
    assert "runtime halted before workflow execution" in result.error
    assert result.trace_id is not None
    with sqlite3.connect(tmp_path / "data" / "telemetry.db") as conn:
        trace_row = conn.execute(
            "SELECT role, judge_verdict, retention_class FROM execution_traces WHERE trace_id = ?",
            (result.trace_id,),
        ).fetchone()
    assert trace_row == ("operator_workflow", "FAIL", "FAILURE_AUDIT")


def test_run_flywheel_drill_generates_phase_gate_and_replay_artifact(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    runtime = MockHermesRuntime(data_dir=str(tmp_path / "data"))

    result = run_flywheel_drill(
        config=cfg,
        repo_root=str(tmp_path),
        tool_registry=runtime,
        report_limit=5,
    )

    assert result.ok is True
    assert result.workflow.opportunity_id
    assert result.workflow.project_id
    assert result.workflow.phase_gate_id
    assert result.workflow.phase_gate_verdict == "CONTINUE"
    assert len(result.workflow.council_verdict_ids) == 2
    assert result.trace_id == result.workflow.trace_id
    assert result.generated_trace_count > 0
    assert result.generated_activation_trace_count > 0
    assert result.generated_known_bad_trace_count == 0
    assert Path(result.artifact_path).is_file()
    artifact = json.loads(Path(result.artifact_path).read_text(encoding="utf-8"))
    assert artifact["status"] == "PASS"
    assert artifact["dashboard_dependency"] is False
    assert artifact["goal"] == "Research -> Opportunity -> Council -> Project phase gate -> replay trace"
    assert artifact["trace_id"] == result.trace_id
    assert artifact["phase_gate_verdict"] == "CONTINUE"
    assert artifact["generated_activation_trace_count"] == result.generated_activation_trace_count
    assert result.replay_report["eligible_source_traces"] > result.before_replay_report["eligible_source_traces"]

    with sqlite3.connect(tmp_path / "data" / "telemetry.db") as conn:
        trace_row = conn.execute(
            "SELECT role, judge_verdict, training_eligible FROM execution_traces WHERE trace_id = ?",
            (result.trace_id,),
        ).fetchone()
    assert trace_row == ("operator_workflow", "PASS", 1)


def test_replay_readiness_report_writes_runtime_artifact(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    report = replay_readiness_report(cfg, repo_root=str(tmp_path))

    assert report["artifact_path"].endswith("replay_readiness_report.json")
    assert Path(report["artifact_path"]).is_file()
    assert report["minimum_eligible_traces"] == 500
    assert report["minimum_known_bad_traces"] == 25
    assert report["minimum_distinct_skills"] == 3
    assert report["growth_plan"]["commands"]["until_replay_ready"].endswith("--evidence-cycles 5")
    assert report["growth_plan"]["next_actions"]


def test_recovery_readiness_creates_packet_artifact_and_workspace_surface(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = recovery_readiness(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:00:00+00:00",
    )

    assert payload["available"] is True
    assert payload["packet"]["scope"] == "kernel.db"
    assert payload["packet"]["readiness_status"] == "action_required"
    assert payload["packet"]["live_controls_enabled"] is False
    assert payload["comparison"]["matches"] is True
    assert payload["required_authority"] == "operator_gate"
    assert payload["live_controls_enabled"] is False
    assert "live_hermes_attachment" in payload["disabled_live_controls"]
    assert Path(payload["artifact_path"]).is_file()

    overview = workspace_overview(cfg)
    recovery = overview["recovery_readiness"]
    assert recovery["available"] is True
    assert recovery["packet"]["packet_id"] == payload["packet"]["packet_id"]
    assert recovery["comparison"]["matches"] is True
    assert recovery["comparison"]["packet_id"] == payload["packet"]["packet_id"]
    assert recovery["comparison"]["replay_packet"]["packet_id"] == payload["packet"]["packet_id"]
    assert recovery["comparison"]["projection_packet"]["packet_id"] == payload["packet"]["packet_id"]
    assert recovery["comparison"]["mismatches"] == []
    assert recovery["live_controls_enabled"] is False
    assert overview["recovery_readiness_path"] == payload["artifact_path"]


def test_hermes_adapter_readiness_creates_kernel_packet_and_workspace_surface(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = hermes_adapter_readiness(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:00:00+00:00",
    )

    assert payload["available"] is True
    assert payload["packet"]["adapter_name"] == "hermes-v0.14"
    assert payload["packet"]["hermes_version"] == "0.14.0"
    assert payload["packet"]["readiness_status"] == "action_required"
    assert payload["packet"]["recovery_readiness_packet_id"] == payload["recovery_readiness"]["packet_id"]
    assert payload["packet"]["live_controls_enabled"] is False
    assert payload["live_controls_enabled"] is False
    assert "provider_calls" in payload["disabled_live_controls"]
    assert payload["comparison"]["matches"] is True
    assert Path(payload["artifact_path"]).is_file()
    assert {check["surface"] for check in payload["packet"]["surface_checks"]} >= {
        "kanban_worker_lifecycle",
        "dashboard_profile_provider_controls",
        "provider_plugin_calls",
        "gateway_goal_checkpoint_resume",
    }
    assert {check["check"] for check in payload["packet"]["reconciliation_checks"]} >= {
        "kernel_task_status",
        "grant_status_scope_expiry_use_count",
        "side_effect_intent_idempotency_receipt",
    }

    overview = workspace_overview(cfg)
    adapter = overview["hermes_adapter_readiness"]
    assert adapter["available"] is True
    assert adapter["packet"]["packet_id"] == payload["packet"]["packet_id"]
    assert adapter["comparison"]["matches"] is True
    assert adapter["comparison"]["packet_id"] == payload["packet"]["packet_id"]
    assert adapter["comparison"]["replay_packet"]["packet_id"] == payload["packet"]["packet_id"]
    assert adapter["comparison"]["projection_packet"]["packet_id"] == payload["packet"]["packet_id"]
    assert adapter["comparison"]["mismatches"] == []
    assert adapter["recovery_readiness"]["packet_id"] == payload["recovery_readiness"]["packet_id"]
    assert adapter["live_controls_enabled"] is False
    assert overview["hermes_adapter_readiness_path"] == payload["artifact_path"]
    assert overview["recovery_readiness"]["available"] is True
    assert overview["recovery_readiness"]["packet"]["packet_id"] == payload["recovery_readiness"]["packet_id"]
    assert overview["recovery_readiness"]["live_controls_enabled"] is False


def test_hermes_adapter_readiness_refreshes_recovery_packet_before_adapter_packet(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    ).resolve_paths()
    Path(cfg.data_dir).mkdir(parents=True)
    store = KernelStore(Path(cfg.data_dir) / "kernel.db")
    with store.connect() as conn:
        conn.execute(
            """
            INSERT INTO recovery_readiness_packets (
              packet_id, scope, as_of, backup_cadence_summary_json,
              restore_drill_summary_json, encrypted_payload_descriptor_summary_json,
              payload_access_failure_summary_json, fail_closed_state_json,
              next_operator_actions_json, readiness_status, evidence_refs_json,
              live_controls_enabled, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "recovery-ready-runtime",
                "kernel.db",
                "2026-05-12T00:00:00+00:00",
                "{}",
                "{}",
                "{}",
                "{}",
                "{}",
                "[]",
                "ready",
                "[]",
                0,
                "2026-05-12T00:00:00+00:00",
            ),
        )

    payload = hermes_adapter_readiness(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:01:00+00:00",
    )

    assert payload["packet"]["recovery_readiness_packet_id"] != "recovery-ready-runtime"
    assert payload["packet"]["recovery_readiness_packet_id"] == payload["recovery_readiness"]["packet_id"]
    assert payload["packet"]["readiness_status"] == "action_required"
    assert payload["recovery_readiness"]["readiness_status"] == "action_required"
    assert payload["packet"]["live_controls_enabled"] is False

    overview = workspace_overview(cfg)
    assert overview["recovery_readiness"]["packet"]["packet_id"] == payload["recovery_readiness"]["packet_id"]
    assert overview["recovery_readiness"]["live_controls_enabled"] is False
    adapter = overview["hermes_adapter_readiness"]
    assert adapter["packet"]["recovery_readiness_packet_id"] == payload["recovery_readiness"]["packet_id"]
    assert adapter["recovery_readiness"]["packet_id"] == payload["recovery_readiness"]["packet_id"]
    assert adapter["live_controls_enabled"] is False


def test_migration_readiness_creates_kernel_map_and_workspace_surface(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = migration_readiness(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:00:00+00:00",
    )

    assert payload["available"] is True
    assert payload["live_controls_enabled"] is False
    assert "provider_calls" in payload["disabled_live_controls"]
    assert payload["comparison"]["matches"] is True
    assert payload["operator_projection"]["applied"] == payload["summary"]["total_records"]
    assert payload["operator_projection"]["live_controls_enabled"] is False
    assert payload["operator_projection_comparison"]["matches"] is True
    assert payload["summary"]["ownership_action_counts"]["adopt"] >= 1
    assert payload["summary"]["ownership_action_counts"]["convert-to-projection"] >= 5
    assert Path(payload["artifact_path"]).is_file()
    by_surface = {record["surface_ref"]: record for record in payload["records"]}
    assert by_surface["kernel.db"]["ownership_action"] == "adopt"
    assert by_surface["strategic_memory.db"]["ownership_action"] == "convert-to-projection"
    assert not any("mission_control" in surface_ref for surface_ref in by_surface)
    assert by_surface["hermes_adapter_readiness.json"]["readiness_status"] == "action_required"
    assert by_surface["strategic_memory.db"]["next_operator_actions"]

    overview = workspace_overview(cfg)
    migration = overview["migration_readiness"]
    assert migration["available"] is True
    assert migration["summary"]["total_records"] == payload["summary"]["total_records"]
    assert migration["operator_projection_comparison"]["matches"] is True
    assert migration["live_controls_enabled"] is False
    assert overview["migration_readiness_path"] == payload["artifact_path"]
    with sqlite3.connect(Path(cfg.data_dir) / "operator_digest.db") as conn:
        conn.row_factory = sqlite3.Row
        projected = conn.execute(
            """
            SELECT *
            FROM kernel_migration_readiness_projection
            WHERE surface_ref='operator_digest.db'
            """
        ).fetchone()
    assert projected is not None
    assert projected["authoritative_source"] == "kernel.events"
    assert projected["live_controls_enabled"] == 0


def test_migration_readiness_repeated_run_recovers_idempotent_kernel_payloads(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    as_of = "2026-05-12T00:00:00+00:00"

    first = migration_readiness(
        cfg,
        repo_root=str(Path.cwd()),
        as_of=as_of,
    )
    repeated = migration_readiness(
        cfg,
        repo_root=str(Path.cwd()),
        as_of=as_of,
    )

    assert repeated["available"] is True
    assert repeated["as_of"] == first["as_of"]
    assert repeated["summary"] == first["summary"]
    assert repeated["record_ids"] == first["record_ids"]
    assert repeated["comparison"]["matches"] is True
    assert repeated["comparison"]["scope"] == "legacy_repo"
    assert repeated["comparison"]["replay_records"] == repeated["comparison"]["projection_records"]
    assert "idempotent" not in repeated["comparison"]
    assert repeated["operator_projection_comparison"]["matches"] is True
    assert repeated["live_controls_enabled"] is False
    assert json.loads(Path(repeated["artifact_path"]).read_text(encoding="utf-8")) == repeated


def test_pre_hermes_readiness_summarizes_blocked_substrate_without_live_controls(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = pre_hermes_readiness(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:02:00+00:00",
    )

    assert payload["available"] is True
    assert payload["live_controls_enabled"] is False
    assert "provider_calls" in payload["disabled_live_controls"]
    assert payload["summary"]["status"] == "action_required"
    assert payload["summary"]["component_status"]["migration_readiness"] == "action_required"
    assert payload["summary"]["component_status"]["hermes_adapter_readiness"] == "action_required"
    assert payload["summary"]["component_status"]["recovery_readiness"] == "action_required"
    assert payload["summary"]["component_status"]["replay_readiness"] == "action_required"
    assert payload["components"]["migration_readiness"]["operator_projection_comparison"]["matches"] is True
    assert payload["components"]["recovery_readiness"]["packet"]["live_controls_enabled"] is False
    assert payload["components"]["hermes_adapter_readiness"]["packet"]["live_controls_enabled"] is False
    assert Path(payload["artifact_path"]).is_file()

    overview = workspace_overview(cfg)
    pre_hermes = overview["pre_hermes_readiness"]
    assert pre_hermes["available"] is True
    assert pre_hermes["summary"]["status"] == "action_required"
    assert pre_hermes["live_controls_enabled"] is False
    assert overview["pre_hermes_readiness_path"] == payload["artifact_path"]


def test_pre_hermes_readiness_uses_single_timestamp_for_refreshed_packets(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    as_of = "2026-05-12T00:02:00+00:00"

    payload = pre_hermes_readiness(
        cfg,
        repo_root=str(Path.cwd()),
        as_of=as_of,
    )

    assert payload["as_of"] == as_of
    assert payload["components"]["recovery_readiness"]["packet"]["as_of"] == as_of
    assert payload["components"]["hermes_adapter_readiness"]["packet"]["as_of"] == as_of
    assert payload["components"]["migration_readiness"]["as_of"] == as_of


def test_readiness_suite_runs_read_only_invariant_checks(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = readiness_suite(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:03:00+00:00",
    )

    assert payload["available"] is True
    assert payload["ok"] is True
    assert payload["status"] == "passed_read_only_invariants"
    assert payload["live_controls_enabled"] is False
    assert payload["summary"]["pre_hermes_status"] == "action_required"
    assert payload["summary"]["failed_components"] == []
    by_component = {item["component"]: item for item in payload["component_checks"]}
    assert set(by_component) == {
        "pre_hermes_readiness",
        "replay_readiness",
        "recovery_readiness",
        "hermes_adapter_readiness",
        "migration_readiness",
    }
    assert all(item["ok"] for item in by_component.values())
    assert by_component["migration_readiness"]["checks"]["comparison_matches"] is True
    assert by_component["hermes_adapter_readiness"]["checks"]["live_controls_disabled"] is True
    assert Path(payload["artifact_path"]).is_file()


def test_hermes_adapter_gauntlet_covers_v014_surfaces_without_authority(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = hermes_adapter_gauntlet(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:04:00+00:00",
    )

    assert payload["available"] is True
    assert payload["summary"]["surface_count"] == 20
    assert payload["summary"]["authority_boundary_case_count"] == 13
    assert payload["summary"]["all_surfaces_covered"] is True
    assert payload["live_controls_enabled"] is False
    assert payload["activation_effect"] == "none"
    assert all(item["live_controls_enabled"] is False for item in payload["surface_matrix"])
    proof_results = payload["proof_results"]
    assert proof_results["summary"]["surface_result_count"] == 20
    assert proof_results["summary"]["authority_boundary_result_count"] == 13
    assert proof_results["summary"]["fail_closed_result_count"] == 33
    assert proof_results["summary"]["missing_stale_or_ambiguous_evidence_blocks_live_authority"] is True
    assert all(item["status"] == "fail_closed_missing_evidence" for item in proof_results["surface_results"])
    assert all(item["fail_closed"] is True for item in proof_results["authority_boundary_results"])
    assert payload["resume_replay_summary"] == {
        "case_count": 8,
        "all_resume_paths_revalidate_kernel_authority": True,
        "external_side_effects_reexecuted": False,
        "replay_intents_reconstructed_only": True,
        "live_controls_enabled": False,
    }
    assert all(
        item["stale_or_failed_check_result"] == "blocked_before_worker_continuation"
        for item in payload["resume_side_effect_replay_cases"]
    )
    assert {item["surface"] for item in payload["surface_matrix"]} >= {
        "kanban_worker_lifecycle",
        "goal_checkpoint_gateway_resume",
        "provider_plugins_and_model_profiles",
        "break_glass_halt",
    }
    assert Path(payload["artifact_path"]).is_file()


def test_model_efficiency_service_packet_is_operator_gated_local_only_offer(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = model_efficiency_service_packet(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:06:30+00:00",
    )

    assert payload["available"] is True
    assert payload["summary"]["buyer_profile_count"] == 3
    assert payload["summary"]["seed_task_class_count"] == 3
    assert payload["summary"]["route_mutation_enabled"] is False
    assert payload["summary"]["operator_gate_required_for_customer_delivery"] is True
    assert payload["summary"]["external_side_effects_allowed"] is False
    assert payload["summary"]["operator_decision_options"] == 2
    assert payload["status"] == "operator_decision_packet_ready"
    assert payload["blockers"] == []
    assert all(value for key, value in payload["decision_packet_contract"].items() if key != "option_bindings")
    assert payload["operator_decision_packet"]["required_authority"] == "operator_gate"
    assert payload["operator_decision_packet"]["default_on_timeout"] == "pause"
    assert "model_route_promotion" in payload["operator_decision_packet"]["forbidden_without_operator_gate"]
    for option in payload["operator_decision_packet"]["options"]:
        assert option["fail_closed_unless_all_bindings_present"] is True
        assert option["recommendation"] in {"pursue", "pause"}
        assert option["required_evidence"]
        assert option["bound_kill_criteria"] == payload["kill_criteria"]
        assert option["forbidden_autonomous_actions"] == payload["blocked_autonomous_actions"]
        assert option["closed_live_control_contract"] == payload["operator_decision_packet"]["closed_live_control_contract"]
        assert option["operator_signoff_requirements"] == payload["operator_decision_packet"]["operator_signoff_requirements"]
    assert payload["kernel_boundaries"]["model_intelligence_supplies_evidence_only"] is True
    assert "customer_visible_delivery" in payload["blocked_autonomous_actions"]
    assert payload["live_controls_enabled"] is False
    assert Path(payload["artifact_path"]).is_file()


def test_model_efficiency_operator_packet_fails_closed_when_pursue_is_unbound(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    payload = model_efficiency_service_packet(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:06:30+00:00",
    )
    packet = json.loads(json.dumps(payload["operator_decision_packet"]))
    pursue = next(option for option in packet["options"] if option["recommendation"] == "pursue")
    pursue["required_evidence"] = []
    pursue["bound_kill_criteria"] = []
    pursue["forbidden_autonomous_actions"] = []
    pursue["closed_live_control_contract"] = {"live_controls_enabled": True}
    pursue["operator_signoff_requirements"] = []
    pursue["fail_closed_unless_all_bindings_present"] = False

    result = runtime_compat._model_efficiency_operator_decision_contract(packet, payload)

    assert "model_efficiency_pursue_evidence_bound" in result["blockers"]
    assert "model_efficiency_pursue_kill_criteria_bound" in result["blockers"]
    assert "model_efficiency_pursue_forbidden_actions_bound" in result["blockers"]
    assert "model_efficiency_pursue_live_control_contract_bound" in result["blockers"]
    assert "model_efficiency_pursue_operator_signoff_bound" in result["blockers"]
    assert "model_efficiency_pursue_fail_closed" in result["blockers"]
    assert result["contract"]["recommendations_bound_to_explicit_evidence"] is False
    assert result["contract"]["recommendations_bound_to_closed_live_controls"] is False
    assert result["contract"]["options_fail_closed"] is False


def test_pre_live_completion_bundle_proves_all_ten_goals_and_stays_closed(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = pre_live_completion_bundle(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-18T00:00:00+00:00",
    )

    assert payload["status"] == "complete_pre_live_coding"
    assert payload["summary"]["all_ten_complete"] is True
    assert payload["summary"]["completed_goals"] == 10
    assert payload["summary"]["total_goals"] == 10
    assert payload["blockers"] == []
    assert {goal["goal_id"] for goal in payload["goals"]} == {
        "operator_project_loop",
        "model_efficiency_service",
        "seed_model_intelligence",
        "research_retrieval",
        "council_execution",
        "hermes_adapter_proxy",
        "side_effect_delivery",
        "operator_gate_surface",
        "data_governance",
        "evidence_packaging",
    }
    assert all(goal["complete"] for goal in payload["goals"])
    assert payload["closed_control_contract"]["live_controls_enabled"] is False
    assert payload["closed_control_contract"]["paid_provider_calls_enabled"] is False
    assert payload["closed_control_contract"]["customer_visible_commitments_enabled"] is False
    assert payload["live_controls_enabled"] is False
    assert Path(payload["artifact_path"]).is_file()


def test_first_live_project_packet_is_productized_local_only_loop(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = first_live_project_packet(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:05:00+00:00",
    )

    assert payload["available"] is True
    assert payload["summary"]["ready_for_target_machine_fixture"] is True
    assert payload["summary"]["phase_count"] == 4
    assert payload["summary"]["cloud_spend_cap_usd"] == 0.0
    assert payload["summary"]["external_commitments_allowed"] is False
    assert payload["artifact_contract"]["artifact_name"] == "operator_digest_readiness_handoff_pack"
    assert payload["artifact_contract"]["external_delivery"] == "prepared_intent_only_until_operator_gate"
    assert [item["phase"] for item in payload["workflow"]] == ["validate", "build", "ship", "operate"]
    assert all(item["external_side_effects_executed"] is False for item in payload["workflow"])
    assert payload["live_controls_enabled"] is False
    assert Path(payload["artifact_path"]).is_file()


def test_first_live_project_acceptance_check_keeps_first_run_local_only(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = first_live_project_acceptance_check(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:05:30+00:00",
    )

    assert payload["status"] == "accepted_pre_live_local_only"
    assert payload["checks"] == {
        "local_only_artifact_output": True,
        "operator_gate_presence": True,
        "feedback_ingestion": True,
        "no_external_side_effect_execution": True,
        "live_controls_disabled": True,
        "external_commitments_disabled": True,
    }
    assert payload["blockers"] == []
    assert payload["live_controls_enabled"] is False
    assert Path(payload["artifact_path"]).is_file()


def test_model_shadow_ops_packet_preserves_shadow_only_authority(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = model_shadow_ops(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:06:00+00:00",
    )

    assert payload["available"] is True
    assert payload["summary"]["seed_task_class_count"] == 3
    assert payload["summary"]["shadow_mode_only"] is True
    assert payload["summary"]["live_route_mutation_enabled"] is False
    assert payload["summary"]["operator_gate_required_for_promotion"] is True
    assert "model_route_promotion" in payload["blocked_autonomous_actions"]
    assert payload["kernel_counts"]["local_route_decisions"] == 0
    assert payload["live_controls_enabled"] is False
    assert Path(payload["artifact_path"]).is_file()


def test_pre_live_mission_control_composes_high_value_packets(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = pre_live_mission_control(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:07:00+00:00",
        candidate_limit=2,
    )

    assert payload["available"] is True
    assert payload["go_no_go"] == "ready_for_target_machine_validation"
    assert payload["summary"]["readiness_suite_ok"] is True
    assert payload["summary"]["all_adapter_surfaces_covered"] is True
    assert payload["summary"]["first_live_project_ready"] is True
    assert payload["summary"]["model_shadow_live_route_mutation"] is False
    assert set(payload["components"]) == {
        "readiness_suite",
        "self_improvement",
        "first_live_project",
        "hermes_adapter_gauntlet",
        "model_shadow_ops",
        "known_bad_manual_patch_gate",
    }
    assert payload["live_controls_enabled"] is False
    assert "autonomous_patch_application" in payload["disabled_live_controls"]
    assert Path(payload["artifact_path"]).is_file()


def test_target_machine_validation_run_packet_preserves_evidence_manifest(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = target_machine_validation_run_packet(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:08:00+00:00",
        candidate_limit=2,
    )

    assert payload["available"] is True
    assert payload["status"] == "ready_for_target_machine_execution"
    assert [step["step"] for step in payload["run_steps"]] == list(range(1, 11))
    assert payload["component_summary"]["pre_live_mission_control"] == "ready_for_target_machine_validation"
    assert payload["component_summary"]["adapter_surface_count"] == 20
    assert payload["component_summary"]["adapter_authority_boundary_case_count"] == 13
    assert payload["component_summary"]["first_live_project_phase_count"] == 4
    assert payload["component_summary"]["model_shadow_seed_task_class_count"] == 3
    assert payload["component_summary"]["model_efficiency_seed_task_class_count"] == 3
    assert all(payload["execution_order_contract"].values())
    assert all(payload["replay_projection_proof_contract"].values())
    assert payload["closed_control_contract"] == {
        "live_controls_enabled": False,
        "dashboard_writes_enabled": False,
        "paid_provider_calls_enabled": False,
        "customer_visible_commitments_enabled": False,
        "model_route_promotion_enabled": False,
        "autonomous_patch_application_enabled": False,
        "side_effect_replay_enabled": False,
    }
    assert all(item["exists"] for item in payload["evidence_manifest"])
    assert all(item["sha256"] for item in payload["evidence_manifest"])
    assert all(item["required_before_live_authority"] for item in payload["evidence_manifest"])
    assert "target_machine_artifact_bundle" in payload["run_steps"][-1]["required_evidence"]
    assert "paid_provider_calls" in payload["fail_closed_controls"]
    assert payload["live_controls_enabled"] is False
    assert Path(payload["artifact_path"]).is_file()


def test_pre_live_runtime_artifact_packets_preserve_hashes_and_contract_shapes(tmp_path):
    def cfg_for(name: str) -> IntegrationConfig:
        root = tmp_path / name
        return IntegrationConfig(
            data_dir=str(root / "data"),
            skills_dir=str(root / "skills"),
            checkpoints_dir=str(root / "skills" / "checkpoints"),
            alerts_dir=str(root / "alerts"),
        )

    cfg = cfg_for("run-packet")
    timestamp = "2026-05-12T00:08:00+00:00"

    run_packet = target_machine_validation_run_packet(
        cfg,
        repo_root=str(Path.cwd()),
        as_of=timestamp,
        candidate_limit=2,
    )
    expected_run_keys = {
        "available",
        "generated_at",
        "packet_name",
        "status",
        "repo_root",
        "run_steps",
        "evidence_manifest",
        "component_summary",
        "execution_order_contract",
        "replay_projection_proof_contract",
        "closed_control_contract",
        "blockers",
        "fail_closed_controls",
        "operator_signoffs_required",
        "artifact_path",
        "live_controls_enabled",
        "activation_effect",
        "packet_hash",
    }
    assert set(run_packet) == expected_run_keys
    assert run_packet["packet_name"] == "target_machine_validation_run_packet"
    assert run_packet["status"] == "ready_for_target_machine_execution"
    assert run_packet["closed_control_contract"] == {
        "live_controls_enabled": False,
        "dashboard_writes_enabled": False,
        "paid_provider_calls_enabled": False,
        "customer_visible_commitments_enabled": False,
        "model_route_promotion_enabled": False,
        "autonomous_patch_application_enabled": False,
        "side_effect_replay_enabled": False,
    }
    assert [step["step"] for step in run_packet["run_steps"]] == list(range(1, 11))
    assert run_packet["replay_projection_proof_contract"] == {
        "first_live_project_events_before_projection": True,
        "readiness_requires_projection_checks": True,
        "resume_replay_reconstructs_intents_only": True,
        "external_side_effect_replay_disabled": True,
        "manifest_artifacts_hash_bound_before_live_authority": True,
    }
    assert {item["name"] for item in run_packet["evidence_manifest"]} == {
        "pre_live_mission_control",
        "hermes_adapter_gauntlet",
        "first_live_project_packet",
        "model_shadow_ops",
        "model_efficiency_service_packet",
    }

    for item in run_packet["evidence_manifest"]:
        artifact = json.loads(Path(item["path"]).read_text(encoding="utf-8"))
        assert item["sha256"] == hashlib.sha256(Path(item["path"]).read_bytes()).hexdigest()
        assert item["packet_hash"] == artifact["packet_hash"]
        assert item["required_before_live_authority"] is True

    crosswalk = pre_live_evidence_crosswalk(
        cfg_for("crosswalk"),
        repo_root=str(Path.cwd()),
        as_of=timestamp,
        candidate_limit=2,
    )
    expected_crosswalk_keys = {
        "available",
        "generated_at",
        "packet_name",
        "status",
        "repo_root",
        "source_spec",
        "run_packet_path",
        "run_packet_status",
        "checklist_rows",
        "crosswalk_contract",
        "summary",
        "blockers",
        "live_controls_enabled",
        "activation_effect",
        "artifact_path",
        "packet_hash",
    }
    assert set(crosswalk) == expected_crosswalk_keys
    assert crosswalk["packet_name"] == "pre_live_evidence_crosswalk"
    assert crosswalk["status"] == "mapped_pre_live_handoff_evidence"
    assert crosswalk["source_spec"] == "spec/s10_pre_live_handoff.md"
    assert all(crosswalk["crosswalk_contract"].values())
    assert crosswalk["summary"] == {
        "checklist_item_count": 11,
        "ready_item_count": 11,
        "all_items_ready": True,
        "closed_control_contract_ok": True,
        "artifact_count": 6,
    }
    assert all(set(row) == {
        "checklist_id",
        "requirement",
        "step_names",
        "artifact_names",
        "mapped_step_count",
        "mapped_artifact_count",
        "required_evidence",
        "artifact_checks",
        "closed_control_keys",
        "missing_steps",
        "missing_artifacts",
        "failing_artifacts",
        "opened_controls",
        "blocker_conditions",
        "ready",
    } for row in crosswalk["checklist_rows"])

    bundle = tmp_path / "target-machine-bundle"
    bundle.mkdir()
    copied = []
    for item in run_packet["evidence_manifest"]:
        source = Path(item["path"])
        target = bundle / source.name
        target.write_bytes(source.read_bytes())
        copied.append(target)
    run_packet_path = Path(run_packet["artifact_path"])
    target_run_packet = bundle / run_packet_path.name
    target_run_packet.write_bytes(run_packet_path.read_bytes())
    copied.append(target_run_packet)
    evidence_ids = {
        evidence_id
        for step in run_packet["run_steps"]
        for evidence_id in step["required_evidence"]
    }
    evidence_records = {"evidence": {evidence_id: {"status": "present"} for evidence_id in sorted(evidence_ids)}}
    evidence_records_path = bundle / "evidence_records.json"
    evidence_records_path.write_text(json.dumps(evidence_records, sort_keys=True), encoding="utf-8")
    copied.append(evidence_records_path)
    (bundle / "SHA256SUMS").write_text(
        "".join(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in sorted(copied)),
        encoding="utf-8",
    )

    evidence_check = target_machine_evidence_check(
        cfg,
        bundle_dir=str(bundle),
        as_of="2026-05-12T00:09:00+00:00",
    )
    expected_evidence_check_keys = {
        "available",
        "generated_at",
        "packet_name",
        "status",
        "bundle_dir",
        "run_packet_path",
        "sha256sums_path",
        "required_evidence",
        "required_replay_projection_evidence",
        "missing_replay_projection_evidence",
        "ambiguous_replay_projection_evidence",
        "required_artifacts",
        "missing_required_evidence",
        "ambiguous_required_evidence",
        "artifact_results",
        "closed_control_contract_ok",
        "replay_projection_contract",
        "blockers",
        "live_controls_enabled",
        "activation_effect",
        "artifact_path",
        "packet_hash",
    }
    assert set(evidence_check) == expected_evidence_check_keys
    assert evidence_check["packet_name"] == "target_machine_evidence_check"
    assert evidence_check["status"] == "validated_preserved_target_machine_bundle"
    assert evidence_check["blockers"] == []
    assert evidence_check["closed_control_contract_ok"] is True
    assert all(evidence_check["replay_projection_contract"].values())
    assert evidence_check["required_replay_projection_evidence"] == [
        "projection_checks_verified",
        "first_live_project_events_before_projection_verified",
        "resume_replay_intents_reconstructed_only",
        "external_side_effect_replay_disabled_verified",
        "manifest_artifacts_hash_bound_before_live_authority",
    ]
    assert evidence_check["missing_replay_projection_evidence"] == []
    assert evidence_check["ambiguous_replay_projection_evidence"] == []
    assert all(set(item) == {
        "name",
        "filename",
        "exists",
        "sha256",
        "sha256sum_entry",
        "run_packet_sha256",
        "matches_sha256sums",
        "matches_run_packet_manifest",
        "required_before_live_authority",
    } for item in evidence_check["artifact_results"])
    assert all(item["matches_sha256sums"] and item["matches_run_packet_manifest"] for item in evidence_check["artifact_results"])

    acceptance = first_live_project_acceptance_check(
        cfg_for("acceptance"),
        repo_root=str(Path.cwd()),
        as_of=timestamp,
    )
    expected_acceptance_keys = {
        "available",
        "generated_at",
        "packet_name",
        "status",
        "fixture_id",
        "checks",
        "blockers",
        "live_controls_enabled",
        "activation_effect",
        "artifact_path",
        "packet_hash",
    }
    assert set(acceptance) == expected_acceptance_keys
    assert acceptance["packet_name"] == "first_live_project_acceptance_check"
    assert acceptance["status"] == "accepted_pre_live_local_only"
    assert acceptance["checks"] == {
        "local_only_artifact_output": True,
        "operator_gate_presence": True,
        "feedback_ingestion": True,
        "no_external_side_effect_execution": True,
        "live_controls_disabled": True,
        "external_commitments_disabled": True,
    }

    for packet in [run_packet, crosswalk, evidence_check, acceptance]:
        emitted = json.loads(Path(packet["artifact_path"]).read_text(encoding="utf-8"))
        assert emitted == packet
        assert packet["packet_hash"] == runtime_compat._stable_json_hash(
            {key: value for key, value in packet.items() if key != "packet_hash"}
        )
        assert packet["live_controls_enabled"] is False
        assert packet["activation_effect"] in {
            "none",
            "none_until_target_machine_evidence_and_operator_gates_pass",
        }
        assert "events" not in packet
        assert "event_id" not in packet
        assert "command_id" not in packet


def test_pre_live_crosswalk_can_reuse_warmed_target_packet_runtime_layout(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    as_of = "2026-05-12T00:08:00+00:00"

    run_packet = target_machine_validation_run_packet(
        cfg,
        repo_root=str(Path.cwd()),
        as_of=as_of,
        candidate_limit=2,
    )
    crosswalk = pre_live_evidence_crosswalk(
        cfg,
        repo_root=str(Path.cwd()),
        as_of=as_of,
        candidate_limit=2,
    )

    assert run_packet["status"] == "ready_for_target_machine_execution"
    assert crosswalk["status"] == "mapped_pre_live_handoff_evidence"
    assert crosswalk["summary"]["ready_item_count"] == 11
    assert crosswalk["blockers"] == []
    assert crosswalk["live_controls_enabled"] is False
    assert json.loads(Path(crosswalk["artifact_path"]).read_text(encoding="utf-8")) == crosswalk


def test_pre_live_evidence_crosswalk_maps_s10_requirements_to_repo_proofs(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = pre_live_evidence_crosswalk(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:08:00+00:00",
        candidate_limit=2,
    )

    assert payload["status"] == "mapped_pre_live_handoff_evidence"
    assert payload["source_spec"] == "spec/s10_pre_live_handoff.md"
    assert payload["summary"]["checklist_item_count"] == 11
    assert payload["summary"]["ready_item_count"] == 11
    assert payload["summary"]["all_items_ready"] is True
    assert payload["summary"]["closed_control_contract_ok"] is True
    assert payload["blockers"] == []
    assert payload["crosswalk_contract"] == {
        "run_packet_ready": True,
        "closed_control_contract_ok": True,
        "all_rows_ready": True,
        "all_rows_have_steps": True,
        "all_rows_have_artifacts": True,
        "all_rows_have_required_evidence": True,
        "all_rows_have_closed_control_keys": True,
        "all_rows_have_blocker_conditions": True,
        "no_missing_mappings": True,
        "no_opened_controls": True,
        "all_rows_have_artifact_checks": True,
        "all_artifacts_hash_bound_before_live_authority": True,
    }
    assert all(row["ready"] for row in payload["checklist_rows"])
    assert all(row["required_evidence"] for row in payload["checklist_rows"])
    assert all(not row["opened_controls"] for row in payload["checklist_rows"])
    assert all(
        artifact["exists"] and artifact["sha256"] and artifact["required_before_live_authority"]
        for row in payload["checklist_rows"]
        for artifact in row["artifact_checks"]
    )
    assert payload["live_controls_enabled"] is False
    assert Path(payload["artifact_path"]).is_file()


def test_pre_live_evidence_crosswalk_contract_fails_closed_on_weak_row_binding():
    rows = [
        {
            "ready": True,
            "mapped_step_count": 1,
            "mapped_artifact_count": 1,
            "required_evidence": ["projection_checks_verified"],
            "artifact_checks": [
                {
                    "name": "target_machine_validation_run_packet",
                    "exists": True,
                    "sha256": "a" * 64,
                    "required_before_live_authority": True,
                }
            ],
            "closed_control_keys": ["live_controls_enabled"],
            "missing_steps": [],
            "missing_artifacts": [],
            "opened_controls": [],
            "blocker_conditions": [],
        }
    ]

    result = runtime_compat._pre_live_evidence_crosswalk_contract(
        rows,
        run_packet_status="ready_for_target_machine_execution",
        closed_control_ok=True,
    )

    assert result["contract"]["all_rows_have_blocker_conditions"] is False
    assert "pre_live_crosswalk_rows_missing_blocker_conditions" in result["blockers"]


def test_pre_live_evidence_crosswalk_fails_closed_on_open_control_contract(tmp_path, monkeypatch):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    artifact_names = [
        "hermes_adapter_gauntlet",
        "pre_live_mission_control",
        "model_efficiency_service_packet",
        "model_shadow_ops",
    ]
    evidence_manifest = []
    for name in artifact_names:
        path = tmp_path / f"{name}.json"
        path.write_text(json.dumps({"packet_name": name}, sort_keys=True), encoding="utf-8")
        evidence_manifest.append(
            {
                "name": name,
                "path": str(path),
                "exists": True,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "required_before_live_authority": True,
            }
        )
    run_packet_path = tmp_path / "target_machine_validation_run_packet.json"
    run_packet_path.write_text("{}", encoding="utf-8")
    closed_contract = {
        "live_controls_enabled": False,
        "dashboard_writes_enabled": False,
        "paid_provider_calls_enabled": True,
        "customer_visible_commitments_enabled": False,
        "model_route_promotion_enabled": False,
        "autonomous_patch_application_enabled": False,
        "side_effect_replay_enabled": False,
    }
    step_names = [
        "repo_metadata_snapshot",
        "handoff_checksum_verification",
        "recovery_and_migration_readiness",
        "hermes_adapter_gauntlet",
        "pre_live_mission_control",
        "model_shadow_ops",
        "first_live_project_packet",
        "known_bad_manual_patch_gate_review",
        "model_efficiency_service_packet",
        "preserve_target_machine_outputs",
    ]

    def opened_run_packet(*args, **kwargs):
        return {
            "status": "ready_for_target_machine_execution",
            "artifact_path": str(run_packet_path),
            "run_steps": [
                {"name": name, "required_evidence": [f"{name}_evidence"]}
                for name in step_names
            ],
            "evidence_manifest": evidence_manifest,
            "closed_control_contract": closed_contract,
        }

    monkeypatch.setattr(runtime_compat, "target_machine_validation_run_packet", opened_run_packet)

    payload = pre_live_evidence_crosswalk(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:08:00+00:00",
    )

    assert payload["status"] == "blocked"
    assert "closed_control_contract_opened_live_control" in payload["blockers"]
    assert "pre_live_crosswalk_closed_control_contract_open" in payload["blockers"]
    assert payload["crosswalk_contract"]["closed_control_contract_ok"] is False
    assert payload["crosswalk_contract"]["no_opened_controls"] is False
    assert payload["summary"]["closed_control_contract_ok"] is False
    assert any("paid_provider_calls_enabled" in row["opened_controls"] for row in payload["checklist_rows"])
    assert payload["live_controls_enabled"] is False


def test_pre_live_bundle_verification_checks_required_packets_and_checksums(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    run_packet = target_machine_validation_run_packet(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:08:00+00:00",
        candidate_limit=2,
    )
    bundle = tmp_path / "pre-live-bundle"
    bundle.mkdir()
    copied = []
    for item in run_packet["evidence_manifest"]:
        source = Path(item["path"])
        target = bundle / source.name
        target.write_bytes(source.read_bytes())
        copied.append(target)
    run_packet_path = Path(run_packet["artifact_path"])
    target = bundle / run_packet_path.name
    target.write_bytes(run_packet_path.read_bytes())
    copied.append(target)
    (bundle / "SHA256SUMS").write_text(
        "".join(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in sorted(copied)),
        encoding="utf-8",
    )

    payload = pre_live_bundle_verification(
        cfg,
        bundle_dir=str(bundle),
        as_of="2026-05-12T00:08:30+00:00",
    )

    assert payload["status"] == "verified_pre_live_bundle"
    assert payload["blockers"] == []
    assert payload["summary"]["required_files_present"] == payload["summary"]["required_file_count"]
    assert payload["summary"]["required_json_non_empty"] is True
    assert payload["summary"]["required_checksums_match"] is True
    assert payload["summary"]["live_controls_disabled"] is True
    assert payload["summary"]["target_machine_status"] == "ready_for_target_machine_execution"
    assert Path(payload["artifact_path"]).is_file()


def test_pre_live_bundle_verification_fails_closed_on_open_control_contract(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    run_packet = target_machine_validation_run_packet(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:08:00+00:00",
        candidate_limit=2,
    )
    bundle = tmp_path / "pre-live-bundle"
    bundle.mkdir()
    copied = []
    for item in run_packet["evidence_manifest"]:
        source = Path(item["path"])
        target = bundle / source.name
        target.write_bytes(source.read_bytes())
        copied.append(target)
    opened_packet = dict(run_packet)
    opened_packet["closed_control_contract"] = dict(run_packet["closed_control_contract"])
    opened_packet["closed_control_contract"]["paid_provider_calls_enabled"] = True
    target = bundle / Path(run_packet["artifact_path"]).name
    target.write_text(json.dumps(opened_packet, sort_keys=True), encoding="utf-8")
    copied.append(target)
    (bundle / "SHA256SUMS").write_text(
        "".join(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in sorted(copied)),
        encoding="utf-8",
    )

    payload = pre_live_bundle_verification(
        cfg,
        bundle_dir=str(bundle),
        as_of="2026-05-12T00:08:30+00:00",
    )

    assert payload["status"] == "blocked"
    assert "closed_control_contract_opened_live_control" in payload["blockers"]
    assert payload["summary"]["required_checksums_match"] is True
    assert payload["live_controls_enabled"] is False


def test_target_machine_evidence_check_validates_preserved_bundle(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    run_packet = target_machine_validation_run_packet(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:08:00+00:00",
        candidate_limit=2,
    )
    bundle = tmp_path / "target-machine-bundle"
    bundle.mkdir()
    for item in run_packet["evidence_manifest"]:
        source = Path(item["path"])
        (bundle / source.name).write_bytes(source.read_bytes())
    run_packet_path = Path(run_packet["artifact_path"])
    (bundle / run_packet_path.name).write_bytes(run_packet_path.read_bytes())
    evidence_ids = {
        evidence_id
        for step in run_packet["run_steps"]
        for evidence_id in step["required_evidence"]
    }
    evidence_records = {"evidence": {evidence_id: {"status": "present"} for evidence_id in sorted(evidence_ids)}}
    (bundle / "evidence_records.json").write_text(json.dumps(evidence_records, sort_keys=True), encoding="utf-8")
    sha_lines = []
    for path in sorted(bundle.iterdir()):
        if path.name == "SHA256SUMS":
            continue
        sha_lines.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}")
    (bundle / "SHA256SUMS").write_text("\n".join(sha_lines) + "\n", encoding="utf-8")

    payload = target_machine_evidence_check(
        cfg,
        bundle_dir=str(bundle),
        as_of="2026-05-12T00:09:00+00:00",
    )

    assert payload["status"] == "validated_preserved_target_machine_bundle"
    assert payload["blockers"] == []
    assert payload["missing_required_evidence"] == []
    assert payload["closed_control_contract_ok"] is True
    assert payload["missing_replay_projection_evidence"] == []
    assert payload["ambiguous_replay_projection_evidence"] == []
    assert payload["required_artifacts"] == [
        "first_live_project_packet",
        "hermes_adapter_gauntlet",
        "model_efficiency_service_packet",
        "model_shadow_ops",
        "pre_live_mission_control",
    ]
    assert all(payload["replay_projection_contract"].values())
    assert all(item["matches_sha256sums"] for item in payload["artifact_results"])
    assert all(item["matches_run_packet_manifest"] for item in payload["artifact_results"])
    assert payload["live_controls_enabled"] is False
    assert Path(payload["artifact_path"]).is_file()


def test_target_machine_evidence_check_fails_closed_when_run_packet_proof_contract_drifts(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    run_packet = target_machine_validation_run_packet(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:08:00+00:00",
        candidate_limit=2,
    )
    bundle = tmp_path / "target-machine-bundle"
    bundle.mkdir()
    for item in run_packet["evidence_manifest"]:
        source = Path(item["path"])
        (bundle / source.name).write_bytes(source.read_bytes())
    drifted_packet = dict(run_packet)
    drifted_packet["replay_projection_proof_contract"] = dict(run_packet["replay_projection_proof_contract"])
    drifted_packet["replay_projection_proof_contract"]["resume_replay_reconstructs_intents_only"] = False
    run_packet_path = bundle / Path(run_packet["artifact_path"]).name
    run_packet_path.write_text(json.dumps(drifted_packet, sort_keys=True), encoding="utf-8")
    evidence_ids = {
        evidence_id
        for step in run_packet["run_steps"]
        for evidence_id in step["required_evidence"]
    }
    evidence_records = {"evidence": {evidence_id: {"status": "present"} for evidence_id in sorted(evidence_ids)}}
    (bundle / "evidence_records.json").write_text(json.dumps(evidence_records, sort_keys=True), encoding="utf-8")
    sha_lines = []
    for path in sorted(bundle.iterdir()):
        if path.name == "SHA256SUMS":
            continue
        sha_lines.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}")
    (bundle / "SHA256SUMS").write_text("\n".join(sha_lines) + "\n", encoding="utf-8")

    payload = target_machine_evidence_check(
        cfg,
        bundle_dir=str(bundle),
        as_of="2026-05-12T00:09:00+00:00",
    )

    assert payload["status"] == "blocked"
    assert "replay_projection_contract_not_proven" in payload["blockers"]
    assert payload["replay_projection_contract"]["run_packet_proof_contract_declared"] is False
    assert payload["replay_projection_contract"]["required_replay_projection_evidence_non_ambiguous"] is True
    assert payload["live_controls_enabled"] is False


def test_target_machine_evidence_check_fails_closed_on_missing_required_evidence(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    run_packet = target_machine_validation_run_packet(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-12T00:08:00+00:00",
        candidate_limit=2,
    )
    bundle = tmp_path / "target-machine-bundle"
    bundle.mkdir()
    run_packet_path = Path(run_packet["artifact_path"])
    (bundle / run_packet_path.name).write_bytes(run_packet_path.read_bytes())
    (bundle / "SHA256SUMS").write_text(
        f"{hashlib.sha256(run_packet_path.read_bytes()).hexdigest()}  {run_packet_path.name}\n",
        encoding="utf-8",
    )

    payload = target_machine_evidence_check(
        cfg,
        bundle_dir=str(bundle),
        as_of="2026-05-12T00:09:00+00:00",
    )

    assert payload["status"] == "blocked"
    assert "required_evidence_missing" in payload["blockers"]
    assert "manifest_artifact_missing" in payload["blockers"]
    assert payload["live_controls_enabled"] is False


def test_self_improvement_snapshot_is_read_only_and_surfaces_kernel_counts(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = self_improvement_snapshot(cfg)

    assert payload["available"] is True
    assert payload["live_controls_enabled"] is False
    assert payload["summary"]["proposal_count"] == 0
    assert payload["summary"]["eval_record_count"] == 0
    assert payload["summary"]["patch_review_packet_count"] == 0
    assert payload["patch_review_packets"] == []
    assert "autonomous_promotion" in payload["disabled_live_controls"]
    assert Path(payload["artifact_path"]).is_file()


def test_self_improvement_snapshot_reads_legacy_bundles_without_patch_review_table(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    prepare_runtime_directories(cfg)
    require_runtime_databases(cfg)

    payload = self_improvement_snapshot(cfg)

    assert payload["available"] is True
    assert payload["live_controls_enabled"] is False
    assert payload["summary"]["patch_review_packet_count"] == 0
    assert payload["summary"]["missing_optional_tables"] == ["self_improvement_patch_review_packets"]
    assert payload["patch_review_packets"] == []


def test_known_bad_hardening_shadow_report_runtime_surface_is_operator_gated(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    prepare_runtime_directories(cfg)
    require_runtime_databases(cfg)
    traces = HarnessVariantManager(str(tmp_path / "data" / "telemetry.db"))
    traces.log_skill_action_trace(
        task_id="council-pass",
        role="council_contract",
        skill_name="council",
        action_name="deliberate",
        intent_goal="safe council deliberation",
        action_payload={"ok": True},
        context_assembled="council policy context",
        retrieval_queries=["council policy"],
        judge_verdict="PASS",
        outcome_score=0.9,
        created_at="2026-05-16T10:00:00+00:00",
    )
    traces.log_skill_action_trace(
        task_id="council-bad",
        role="council_contract",
        skill_name="council",
        action_name="deliberate",
        intent_goal="invalid council decision type",
        action_payload={"ok": False},
        context_assembled="council policy context",
        judge_verdict="FAIL",
        outcome_score=0.0,
        training_eligible=False,
        retention_class="FAILURE_AUDIT",
        created_at="2026-05-16T10:01:00+00:00",
    )

    payload = known_bad_hardening_shadow_report(
        cfg,
        repo_root=str(Path.cwd()),
        skill_name="council",
        sample_size=10,
        as_of="2026-05-16T10:03:00+00:00",
    )

    assert payload["available"] is True
    assert payload["live_controls_enabled"] is False
    assert payload["authority_effect"] == "evidence_only"
    assert payload["promotion_requires_operator_approval"] is True
    assert payload["selected_candidate"]["candidate_id"] == "council:known_bad_hardening"
    assert payload["evaluation"]["status"] == "SHADOW_EVAL"
    assert payload["evaluation"]["promoted_at"] is None
    assert payload["evaluation"]["eval_result"]["side_effect_safety"] == {
        "external_intents_reconstructed_only": True,
        "reexecuted_side_effects": False,
    }
    assert payload["active_frontier_promotion"] is False
    assert payload["replay_evidence_checks"]["source_trace_lineage_preserved"] is True
    assert payload["replay_evidence_checks"]["external_side_effects_reexecuted"] is False
    assert payload["portfolio_summary"][0]["required_operator_action"] == "review_shadow_evidence_before_promotion"
    snapshot = self_improvement_snapshot(cfg)
    assert snapshot["shadow_known_bad_hardening"]["items"][0]["skill_name"] == "council"
    pipeline = self_improvement_evidence_pipeline(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-16T10:04:00+00:00",
        candidate_limit=3,
    )
    assert pipeline["source_counts"]["known_bad_hardening_shadow"] == 1
    shadow_items = [
        item
        for item in pipeline["portfolio"]
        if item["source"] == "known_bad_hardening_shadow"
    ]
    assert len(shadow_items) == 1
    assert shadow_items[0]["target_id"] == "council.known_bad_hardening"
    assert shadow_items[0]["eval_status"] == "passed"
    assert shadow_items[0]["recommendation"] == "approve"
    assert shadow_items[0]["required_authority"] == "operator_gate"
    assert pipeline["shadow_known_bad_hardening"][0]["active_frontier_promotion"] is False


def test_known_bad_hardening_operator_review_bundle_joins_shadow_snapshot_and_pipeline_packets(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    prepare_runtime_directories(cfg)
    require_runtime_databases(cfg)
    traces = HarnessVariantManager(str(tmp_path / "data" / "telemetry.db"))
    traces.log_skill_action_trace(
        task_id="runtime-pass",
        role="runtime_contract",
        skill_name="runtime",
        action_name="prepare",
        intent_goal="safe runtime request",
        action_payload={"ok": True},
        context_assembled="runtime policy context",
        retrieval_queries=["runtime policy"],
        judge_verdict="PASS",
        outcome_score=0.92,
        created_at="2026-05-16T11:00:00+00:00",
    )
    traces.log_skill_action_trace(
        task_id="runtime-bad",
        role="runtime_contract",
        skill_name="runtime",
        action_name="prepare",
        intent_goal="known-bad runtime request",
        action_payload={"ok": False},
        context_assembled="runtime policy context",
        judge_verdict="FAIL",
        outcome_score=0.0,
        training_eligible=False,
        retention_class="FAILURE_AUDIT",
        created_at="2026-05-16T11:01:00+00:00",
    )
    known_bad_hardening_shadow_report(
        cfg,
        repo_root=str(Path.cwd()),
        skill_name="runtime",
        sample_size=10,
        as_of="2026-05-16T11:02:00+00:00",
    )
    self_improvement_evidence_pipeline(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-16T11:03:00+00:00",
        candidate_limit=3,
    )

    kernel_db = tmp_path / "data" / "kernel.db"
    with sqlite3.connect(kernel_db) as conn:
        before_counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "self_improvement_proposals",
                "self_improvement_eval_records",
                "self_improvement_promotion_packets",
                "self_improvement_evidence_pipeline_runs",
            )
        }
    before_frontier = traces.frontier(skill_name="runtime")

    payload = known_bad_hardening_operator_review_bundle(
        cfg,
        skill_name="runtime",
        limit=5,
    )

    with sqlite3.connect(kernel_db) as conn:
        after_counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in before_counts
        }

    assert payload["read_only"] is True
    assert payload["operator_gated"] is True
    assert payload["live_controls_enabled"] is False
    assert payload["active_frontier_promotion"] is False
    candidate = payload["candidates"][0]
    assert payload["recommended_next_candidate"] == {
        "candidate_id": "runtime:known_bad_hardening",
        "skill": "runtime",
        "operator_packet_order": candidate["operator_packet_order"],
        "candidate_rank": candidate["candidate_rank"],
        "required_operator_action": "review_shadow_evidence_before_promotion",
    }
    decision_packet = payload["operator_decision_packet"]
    assert decision_packet["decision_type"] == "known_bad_hardening_shadow_review"
    assert decision_packet["candidate_id"] == "runtime:known_bad_hardening"
    assert decision_packet["required_authority"] == "operator_gate"
    assert decision_packet["default_on_timeout"] == "keep_current_behavior"
    assert decision_packet["promotion_effect"] == "none_until_separate_operator_gate"
    assert decision_packet["allowed_operator_resolutions"] == [
        "approve_for_manual_promotion_review",
        "defer_pending_more_shadow_evidence",
        "reject_shadow_candidate",
    ]
    assert decision_packet["blocked_autonomous_actions"] == [
        "active_behavior_mutation",
        "autonomous_harness_promotion",
        "frontier_route_update",
        "external_side_effect_reexecution",
    ]

    with sqlite3.connect(tmp_path / "data" / "telemetry.db") as conn:
        conn.execute("DELETE FROM harness_variants")
        conn.commit()

    fallback_payload = known_bad_hardening_operator_review_bundle(
        cfg,
        skill_name="runtime",
        limit=5,
    )

    assert fallback_payload["recommended_next_candidate"]["candidate_id"] == "runtime:known_bad_hardening"
    assert fallback_payload["operator_decision_packet"]["required_authority"] == "operator_gate"
    assert fallback_payload["operator_decision_packet"]["promotion_effect"] == "none_until_separate_operator_gate"
    assert decision_packet["evidence_summary"]["known_bad_block_rate"] == 1.0
    assert decision_packet["evidence_summary"]["regression_rate"] == 0.0
    assert decision_packet["evidence_summary"]["active_frontier_promotion"] is False
    assert before_counts == after_counts
    assert before_frontier == []
    assert traces.frontier(skill_name="runtime") == []
    assert candidate["skill"] == "runtime"
    assert candidate["candidate_id"] == "runtime:known_bad_hardening"
    assert candidate["status"] == "SHADOW_EVAL"
    assert candidate["known_bad_block_rate"] == 1.0
    assert candidate["regression_rate"] == 0.0
    assert candidate["side_effect_safety"] == {
        "external_intents_reconstructed_only": True,
        "reexecuted_side_effects": False,
    }
    assert candidate["replay_lineage_status"] == "preserved"
    assert candidate["active_frontier_promotion"] is False
    assert candidate["required_authority"] == "operator_gate"
    assert candidate["default_on_timeout"] == "keep_current_behavior"
    assert candidate["required_operator_action"] == "review_shadow_evidence_before_promotion"
    packet_evidence = candidate["pipeline_packet_evidence"]
    assert packet_evidence["source"] == "known_bad_hardening_shadow"
    assert packet_evidence["packet_id"]
    assert packet_evidence["latest_pipeline_run_id"] == payload["latest_pipeline_run_id"]
    assert packet_evidence["pipeline_portfolio_order"] == candidate["operator_packet_order"]
    latest_shadow_item = next(
        item
        for item in self_improvement_snapshot(cfg)["portfolio"]
        if item["source"] == "known_bad_hardening_shadow"
        and item["target_id"] == "runtime.known_bad_hardening"
    )
    latest_generic_item = next(
        item
        for item in self_improvement_snapshot(cfg)["portfolio"]
        if item["source"] == "harness_candidate"
        and item["target_id"] == "runtime.known_bad_hardening"
    )
    assert packet_evidence["packet_id"] == latest_shadow_item["packet_id"]
    assert packet_evidence["packet_id"] != latest_generic_item["packet_id"]
    assert packet_evidence["promotion_packet"]["required_authority"] == "operator_gate"
    assert packet_evidence["promotion_packet"]["default_on_timeout"] == "keep_current_behavior"
    assert packet_evidence["eval_record"]["authority_effect"] == "evidence_only"
    assert candidate["runtime_shadow_report_evidence"]["portfolio_summary_present"] is True
    assert candidate["runtime_shadow_report_evidence"]["portfolio_summary"]["candidate_id"] == "runtime:known_bad_hardening"


def test_known_bad_hardening_follow_on_review_packet_is_durable_and_inert(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    prepare_runtime_directories(cfg)
    require_runtime_databases(cfg)
    traces = HarnessVariantManager(str(tmp_path / "data" / "telemetry.db"))
    traces.log_skill_action_trace(
        task_id="council-pass",
        role="council_contract",
        skill_name="council",
        action_name="deliberate",
        intent_goal="safe council deliberation",
        action_payload={"ok": True},
        context_assembled="council policy context",
        judge_verdict="PASS",
        outcome_score=0.94,
        created_at="2026-05-17T10:00:00+00:00",
    )
    traces.log_skill_action_trace(
        task_id="council-bad",
        role="council_contract",
        skill_name="council",
        action_name="deliberate",
        intent_goal="known-bad council deliberation",
        action_payload={"ok": False},
        context_assembled="council policy context",
        judge_verdict="FAIL",
        outcome_score=0.0,
        training_eligible=False,
        retention_class="FAILURE_AUDIT",
        created_at="2026-05-17T10:01:00+00:00",
    )
    known_bad_hardening_shadow_report(
        cfg,
        repo_root=str(Path.cwd()),
        skill_name="council",
        sample_size=10,
        as_of="2026-05-17T10:02:00+00:00",
    )
    self_improvement_evidence_pipeline(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-17T10:03:00+00:00",
        candidate_limit=3,
    )

    before_frontier = traces.frontier(skill_name="council")
    with sqlite3.connect(tmp_path / "data" / "kernel.db") as conn:
        conn.execute("DROP TABLE self_improvement_patch_review_packets")
        conn.commit()
    payload = known_bad_hardening_follow_on_review_packet(
        cfg,
        candidate_id="council:known_bad_hardening",
        operator_resolution="approve_for_manual_promotion_review",
        skill_name="council",
        limit=5,
    )
    repeated = known_bad_hardening_follow_on_review_packet(
        cfg,
        candidate_id="council:known_bad_hardening",
        operator_resolution="approve_for_manual_promotion_review",
        skill_name="council",
        limit=5,
    )

    assert payload["durable"] is True
    assert payload["operator_gated"] is True
    assert payload["live_controls_enabled"] is False
    assert payload["active_frontier_promotion"] is False
    assert payload["patch_packet"]["activation_effect"] == "none_until_separate_operator_gate"
    assert payload["patch_review_packet"]["required_authority"] == "operator_gate"
    assert payload["patch_review_packet"]["authority_effect"] == "review_only"
    assert payload["patch_review_packet"]["status"] == "prepared"
    assert "autonomous_patch_application" in payload["patch_review_packet"]["blocked_autonomous_actions"]
    assert "frontier_route_update" in payload["patch_review_packet"]["blocked_autonomous_actions"]
    assert Path(payload["artifact_path"]).is_file()
    assert repeated["patch_review_packet"]["patch_packet_id"] == payload["patch_review_packet"]["patch_packet_id"]
    assert traces.frontier(skill_name="council") == before_frontier == []
    snapshot = self_improvement_snapshot(cfg)
    assert snapshot["summary"]["patch_review_packet_count"] == 1


def test_known_bad_hardening_operator_summary_and_patch_gate_are_manual_only(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    prepare_runtime_directories(cfg)
    require_runtime_databases(cfg)
    traces = HarnessVariantManager(str(tmp_path / "data" / "telemetry.db"))
    traces.log_skill_action_trace(
        task_id="council-pass",
        role="council_contract",
        skill_name="council",
        action_name="deliberate",
        intent_goal="safe council request",
        action_payload={"ok": True},
        context_assembled="council policy context",
        judge_verdict="PASS",
        outcome_score=0.9,
        created_at="2026-05-17T13:00:00+00:00",
    )
    traces.log_skill_action_trace(
        task_id="council-bad",
        role="council_contract",
        skill_name="council",
        action_name="deliberate",
        intent_goal="unsafe council request",
        action_payload={"ok": False},
        context_assembled="council policy context",
        judge_verdict="FAIL",
        outcome_score=0.0,
        training_eligible=False,
        retention_class="FAILURE_AUDIT",
        created_at="2026-05-17T13:01:00+00:00",
    )
    known_bad_hardening_shadow_report(
        cfg,
        repo_root=str(Path.cwd()),
        skill_name="council",
        sample_size=10,
        as_of="2026-05-17T13:02:00+00:00",
    )
    self_improvement_evidence_pipeline(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-17T13:03:00+00:00",
        candidate_limit=3,
    )
    follow_on = known_bad_hardening_follow_on_review_packet(
        cfg,
        candidate_id="council:known_bad_hardening",
        operator_resolution="approve_for_manual_promotion_review",
        skill_name="council",
        limit=5,
    )

    summary = known_bad_hardening_operator_review_summary(
        cfg,
        candidate_id="council:known_bad_hardening",
        skill_name="council",
        limit=5,
    )
    gate = known_bad_hardening_operator_patch_gate(
        cfg,
        patch_packet_id=follow_on["patch_review_packet"]["patch_packet_id"],
        operator_patch_resolution="approve_manual_patch_gate",
        candidate_id="council:known_bad_hardening",
        skill_name="council",
        limit=5,
    )

    assert summary["review_surface"] == "known_bad_hardening_operator_review_summary"
    assert summary["patch_review"]["prepared"] is True
    assert summary["patch_review"]["patch_packet_id"] == follow_on["patch_review_packet"]["patch_packet_id"]
    assert summary["next_required_gate"] == "known_bad_hardening_operator_patch_gate"
    assert summary["active_frontier_promotion"] is False
    assert summary["autonomous_patch_application_enabled"] is False
    assert Path(summary["artifact_path"]).is_file()

    assert gate["review_surface"] == "known_bad_hardening_operator_patch_gate"
    assert gate["source_patch_packet_id"] == follow_on["patch_review_packet"]["patch_packet_id"]
    assert gate["manual_patch_gate"]["manual_application_only"] is True
    assert gate["manual_patch_gate"]["autonomous_application_enabled"] is False
    assert gate["manual_patch_gate"]["activation_effect"] == "manual_patch_review_only_no_runtime_activation"
    assert gate["active_frontier_promotion"] is False
    assert gate["route_updates_enabled"] is False
    assert gate["side_effect_replay_enabled"] is False
    assert "full_pytest_suite" in gate["required_verification_before_manual_application"]
    assert Path(gate["artifact_path"]).is_file()
    assert traces.frontier(skill_name="council") == []


def test_known_bad_hardening_follow_on_review_fails_closed_for_unapproved_resolution(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    with pytest.raises(ValueError, match="approve_for_manual_promotion_review"):
        known_bad_hardening_follow_on_review_packet(
            cfg,
            candidate_id="council:known_bad_hardening",
            operator_resolution="defer_pending_more_shadow_evidence",
            skill_name="council",
        )


def test_known_bad_hardening_follow_on_review_fails_closed_for_active_frontier(tmp_path, monkeypatch):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    def active_bundle(*_args, **_kwargs):
        return {
            "active_frontier_promotion": True,
            "operator_decision_packet": {},
            "candidates": [
                {
                    "candidate_id": "council:known_bad_hardening",
                    "active_frontier_promotion": True,
                    "pipeline_packet_evidence": {
                        "proposal_id": "proposal",
                        "packet_id": "packet",
                    },
                }
            ],
        }

    monkeypatch.setattr(
        "kernel.runtime_compat.known_bad_hardening_operator_review_bundle",
        active_bundle,
    )

    with pytest.raises(PermissionError, match="inactive frontier"):
        known_bad_hardening_follow_on_review_packet(
            cfg,
            candidate_id="council:known_bad_hardening",
            operator_resolution="approve_for_manual_promotion_review",
            skill_name="council",
        )


def test_known_bad_hardening_follow_on_review_fails_closed_without_durable_packet_evidence(tmp_path, monkeypatch):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    def missing_evidence_bundle(*_args, **_kwargs):
        return {
            "active_frontier_promotion": False,
            "operator_decision_packet": {},
            "candidates": [
                {
                    "candidate_id": "council:known_bad_hardening",
                    "active_frontier_promotion": False,
                    "pipeline_packet_evidence": {},
                }
            ],
        }

    monkeypatch.setattr(
        "kernel.runtime_compat.known_bad_hardening_operator_review_bundle",
        missing_evidence_bundle,
    )

    with pytest.raises(ValueError, match="durable proposal and promotion packet evidence"):
        known_bad_hardening_follow_on_review_packet(
            cfg,
            candidate_id="council:known_bad_hardening",
            operator_resolution="approve_for_manual_promotion_review",
            skill_name="council",
        )


def test_known_bad_hardening_operator_patch_gate_fails_closed_for_bad_resolution(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    with pytest.raises(ValueError, match="approve_manual_patch_gate"):
        known_bad_hardening_operator_patch_gate(
            cfg,
            patch_packet_id="known-bad-follow-on-test",
            operator_patch_resolution="defer_manual_patch_gate",
            candidate_id="council:known_bad_hardening",
            skill_name="council",
        )


def test_self_improvement_evidence_pipeline_surfaces_operator_ready_portfolio(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )

    payload = self_improvement_evidence_pipeline(
        cfg,
        repo_root=str(Path(__file__).resolve().parents[2]),
        as_of="2026-05-16T00:00:00+00:00",
        candidate_limit=2,
    )

    assert payload["available"] is True
    assert payload["live_controls_enabled"] is False
    assert payload["signal_count"] >= 1
    assert payload["run"]["status"] == "recorded"
    assert payload["run"]["proposal_ids"]
    assert payload["run"]["eval_record_ids"]
    assert payload["run"]["promotion_packet_ids"]
    assert payload["portfolio"]
    assert "active_behavior_mutation" in payload["disabled_live_controls"]
    snapshot = self_improvement_snapshot(cfg)
    assert snapshot["summary"]["pipeline_run_count"] == 1
    assert snapshot["portfolio"]
    assert Path(snapshot["artifact_path"]).is_file()


def test_run_evidence_factory_generates_cross_skill_evidence(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    runtime = MockHermesRuntime(data_dir=str(tmp_path / "data"))

    result = run_evidence_factory(
        config=cfg,
        repo_root=str(tmp_path),
        tool_registry=runtime,
        cycles=1,
        report_limit=8,
    )

    assert result.ok is True
    assert result.requested_cycles == 1
    assert result.cycles == 1
    assert result.until_replay_ready is False
    assert result.stopped_reason == "completed_requested_cycles"
    assert result.generated_trace_count > 0
    assert result.generated_activation_trace_count > 0
    assert result.generated_known_bad_trace_count > 0
    assert Path(result.report_path).is_file()
    assert any(item.scenario_id == "research_to_opportunity_flow" and item.ok for item in result.scenario_results)
    assert any(item.scenario_id == "invalid_brief_completion" and item.ok for item in result.scenario_results)
    assert any(item.scenario_id == "missing_brief_route" and item.ok for item in result.scenario_results)
    assert any(item.scenario_id == "council_invalid_decision_type" and item.ok for item in result.scenario_results)
    assert any(item.scenario_id == "financial_g3_denial" and item.ok for item in result.scenario_results)
    assert result.before_replay_report["eligible_source_traces"] == 0
    readiness = result.replay_report
    assert readiness["eligible_source_traces"] > 0
    assert readiness["known_bad_source_traces"] > 0
    assert readiness["distinct_skill_count"] >= 3
    assert {"strategic_memory", "council", "financial_router"} <= {
        row["skill_name"] for row in readiness["known_bad_by_skill"]
    }
    assert readiness["growth_plan"]["recommended_scenarios"]
    projection = result.progress_projection
    assert projection["ready_for_broader_replay"] is False
    assert projection["executed_cycles"] == 1
    assert len(projection["metrics"]) == 3

    with sqlite3.connect(tmp_path / "data" / "telemetry.db") as conn:
        role_rows = conn.execute(
            """
            SELECT role, judge_verdict
            FROM execution_traces
            WHERE role IN ('evidence_research_to_opportunity_flow', 'evidence_invalid_brief_completion')
            ORDER BY created_at ASC
            """
        ).fetchall()
    assert ("evidence_research_to_opportunity_flow", "PASS") in role_rows
    assert ("evidence_invalid_brief_completion", "FAIL") in role_rows


def test_run_evidence_factory_until_replay_ready_uses_cycle_cap_when_threshold_not_met(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    runtime = MockHermesRuntime(data_dir=str(tmp_path / "data"))

    result = run_evidence_factory(
        config=cfg,
        repo_root=str(tmp_path),
        tool_registry=runtime,
        cycles=2,
        report_limit=8,
        until_replay_ready=True,
    )

    assert result.ok is True
    assert result.requested_cycles == 2
    assert result.cycles == 2
    assert result.until_replay_ready is True
    assert result.stopped_reason == "max_cycles_reached"
    assert result.progress_projection["executed_cycles"] == 2


def test_optimizer_prep_artifacts_are_generated_from_runtime_surface(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    runtime = MockHermesRuntime(data_dir=str(tmp_path / "data"))

    evidence = run_evidence_factory(
        config=cfg,
        repo_root=str(tmp_path),
        tool_registry=runtime,
        cycles=1,
        report_limit=8,
    )
    assert evidence.ok is True

    corpus = export_replay_corpus(cfg, repo_root=str(tmp_path), limit=25)
    assert corpus["trace_count"] > 0
    assert corpus["eligible_trace_count"] > 0
    assert corpus["known_bad_trace_count"] > 0
    assert Path(corpus["artifact_path"]).is_file()

    snapshot = optimizer_snapshot(cfg, repo_root=str(tmp_path), corpus_limit=25, candidate_limit=5)
    assert snapshot["snapshot_status"] in {"READY", "DOCTOR_WARNINGS"}
    assert snapshot["telemetry"]["corpus_export_summary"]["trace_count"] > 0
    assert Path(snapshot["artifacts"]["optimizer_snapshot_path"]).is_file()

    candidates = analyze_harness_candidates(cfg, repo_root=str(tmp_path), limit=5, propose_best=True)
    assert candidates["candidate_count"] > 0
    assert Path(candidates["artifact_path"]).is_file()
    proposal = candidates["proposal"]
    assert proposal["proposed_variant"] is not None
    assert proposal["proposed_variant"]["touches_infrastructure"] is False


def test_build_mac_studio_day_one_handoff_writes_handoff_bundle(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    runtime = MockHermesRuntime(data_dir=str(tmp_path / "data"))

    result = build_mac_studio_day_one_handoff(
        config=cfg,
        repo_root=str(tmp_path),
        tool_registry=runtime,
        cycles=1,
        report_limit=8,
    )

    assert result.ok is True
    assert result.bootstrap_stack.ok is True
    assert result.evidence_batch.ok is True
    assert Path(result.handoff_path).is_file()
    handoff_text = Path(result.handoff_path).read_text(encoding="utf-8")
    assert "Mac Studio Day-One Handoff" in handoff_text
    assert "start_local_forward_proxy.sh" in handoff_text
    assert "--until-replay-ready" in handoff_text
    assert "--bootstrap-stack" in handoff_text
    assert "--evidence-factory" in handoff_text
    assert "--export-replay-corpus" in handoff_text
    assert "--optimizer-snapshot" in handoff_text
    assert "--analyze-harness-candidates" in handoff_text
    assert "Growth Focus" in handoff_text
    assert "Live Hermes Validation" in handoff_text
    assert "Priority Skills" in handoff_text


def test_contract_harness_can_repeat_on_warmed_runtime(tmp_path):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    runtime = MockHermesRuntime(data_dir=str(tmp_path / "data"))

    first = exercise_hermes_contract(config=cfg, tool_registry=runtime)
    second = exercise_hermes_contract(config=cfg, tool_registry=runtime)

    assert first.ok is True
    assert second.ok is True
    assert not second.issues


def test_runtime_main_bootstrap_live_flag_executes_bootstrap(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "skills.runtime",
            "--bootstrap-live",
            "--data-dir",
            str(tmp_path / "data"),
            "--skills-dir",
            str(tmp_path / "skills"),
            "--checkpoints-dir",
            str(tmp_path / "skills" / "checkpoints"),
            "--alerts-dir",
            str(tmp_path / "alerts"),
            "--profile-name",
            "hybrid-test",
        ],
    )

    exit_code = runtime_main()
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "bootstrap ok" in output
    assert "session_id=" in output


def test_runtime_main_hermes_adapter_readiness_prints_json_without_live_controls(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "skills.runtime",
            "--hermes-adapter-readiness",
            "--data-dir",
            str(tmp_path / "data"),
            "--skills-dir",
            str(tmp_path / "skills"),
            "--checkpoints-dir",
            str(tmp_path / "skills" / "checkpoints"),
            "--alerts-dir",
            str(tmp_path / "alerts"),
            "--repo-root",
            str(Path.cwd()),
        ],
    )

    exit_code = runtime_main()
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["packet"]["adapter_name"] == "hermes-v0.14"
    assert output["packet"]["live_controls_enabled"] is False
    assert output["live_controls_enabled"] is False
    assert output["comparison"]["matches"] is True


def test_runtime_main_recovery_readiness_prints_json_without_live_controls(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "skills.runtime",
            "--recovery-readiness",
            "--data-dir",
            str(tmp_path / "data"),
            "--skills-dir",
            str(tmp_path / "skills"),
            "--checkpoints-dir",
            str(tmp_path / "skills" / "checkpoints"),
            "--alerts-dir",
            str(tmp_path / "alerts"),
            "--repo-root",
            str(Path.cwd()),
        ],
    )

    exit_code = runtime_main()
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["packet"]["scope"] == "kernel.db"
    assert output["packet"]["live_controls_enabled"] is False
    assert output["live_controls_enabled"] is False
    assert output["required_authority"] == "operator_gate"
    assert output["comparison"]["matches"] is True


def test_runtime_main_readiness_suite_prints_json_invariant_status(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "skills.runtime",
            "--readiness-suite",
            "--data-dir",
            str(tmp_path / "data"),
            "--skills-dir",
            str(tmp_path / "skills"),
            "--checkpoints-dir",
            str(tmp_path / "skills" / "checkpoints"),
            "--alerts-dir",
            str(tmp_path / "alerts"),
            "--repo-root",
            str(Path.cwd()),
        ],
    )

    exit_code = runtime_main()
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["ok"] is True
    assert output["status"] == "passed_read_only_invariants"
    assert output["live_controls_enabled"] is False
    assert output["summary"]["failed_components"] == []


def test_runtime_main_self_improvement_snapshot_prints_read_only_json(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "skills.runtime",
            "--self-improvement-snapshot",
            "--data-dir",
            str(tmp_path / "data"),
            "--skills-dir",
            str(tmp_path / "skills"),
            "--checkpoints-dir",
            str(tmp_path / "skills" / "checkpoints"),
            "--alerts-dir",
            str(tmp_path / "alerts"),
        ],
    )

    exit_code = runtime_main()
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["available"] is True
    assert output["live_controls_enabled"] is False
    assert output["summary"]["proposal_count"] == 0


def test_runtime_main_known_bad_hardening_shadow_report_prints_read_only_json(tmp_path, monkeypatch, capsys):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    prepare_runtime_directories(cfg)
    require_runtime_databases(cfg)
    traces = HarnessVariantManager(str(tmp_path / "data" / "telemetry.db"))
    traces.log_skill_action_trace(
        task_id="runtime-pass",
        role="runtime_contract",
        skill_name="runtime",
        action_name="prepare",
        intent_goal="safe runtime request",
        action_payload={"ok": True},
        context_assembled="runtime policy context",
        retrieval_queries=["runtime policy"],
        judge_verdict="PASS",
        outcome_score=0.9,
        created_at="2026-05-16T10:00:00+00:00",
    )
    traces.log_skill_action_trace(
        task_id="runtime-bad",
        role="runtime_contract",
        skill_name="runtime",
        action_name="prepare",
        intent_goal="unsafe runtime request",
        action_payload={"ok": False},
        context_assembled="runtime policy context",
        judge_verdict="FAIL",
        outcome_score=0.0,
        training_eligible=False,
        retention_class="FAILURE_AUDIT",
        created_at="2026-05-16T10:01:00+00:00",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "skills.runtime",
            "--known-bad-hardening-shadow-report",
            "--skill-name",
            "runtime",
            "--shadow-sample-size",
            "10",
            "--data-dir",
            str(tmp_path / "data"),
            "--skills-dir",
            str(tmp_path / "skills"),
            "--checkpoints-dir",
            str(tmp_path / "skills" / "checkpoints"),
            "--alerts-dir",
            str(tmp_path / "alerts"),
            "--repo-root",
            str(Path.cwd()),
        ],
    )

    exit_code = runtime_main()
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["live_controls_enabled"] is False
    assert output["authority_effect"] == "evidence_only"
    assert output["promotion_requires_operator_approval"] is True
    assert output["evaluation"]["status"] == "SHADOW_EVAL"
    assert output["active_frontier_promotion"] is False


def test_runtime_main_known_bad_hardening_operator_review_prints_json_only(tmp_path, monkeypatch, capsys):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    prepare_runtime_directories(cfg)
    require_runtime_databases(cfg)
    traces = HarnessVariantManager(str(tmp_path / "data" / "telemetry.db"))
    traces.log_skill_action_trace(
        task_id="runtime-pass",
        role="runtime_contract",
        skill_name="runtime",
        action_name="prepare",
        intent_goal="safe runtime request",
        action_payload={"ok": True},
        context_assembled="runtime policy context",
        judge_verdict="PASS",
        outcome_score=0.9,
        created_at="2026-05-16T12:00:00+00:00",
    )
    traces.log_skill_action_trace(
        task_id="runtime-bad",
        role="runtime_contract",
        skill_name="runtime",
        action_name="prepare",
        intent_goal="unsafe runtime request",
        action_payload={"ok": False},
        context_assembled="runtime policy context",
        judge_verdict="FAIL",
        outcome_score=0.0,
        training_eligible=False,
        retention_class="FAILURE_AUDIT",
        created_at="2026-05-16T12:01:00+00:00",
    )
    known_bad_hardening_shadow_report(
        cfg,
        repo_root=str(Path.cwd()),
        skill_name="runtime",
        sample_size=10,
        as_of="2026-05-16T12:02:00+00:00",
    )
    self_improvement_evidence_pipeline(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-16T12:03:00+00:00",
        candidate_limit=3,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "skills.runtime",
            "--known-bad-hardening-operator-review",
            "--skill-name",
            "runtime",
            "--data-dir",
            str(tmp_path / "data"),
            "--skills-dir",
            str(tmp_path / "skills"),
            "--checkpoints-dir",
            str(tmp_path / "skills" / "checkpoints"),
            "--alerts-dir",
            str(tmp_path / "alerts"),
        ],
    )

    exit_code = runtime_main()
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["review_surface"] == "known_bad_hardening_shadow_operator_review"
    assert output["operator_packet_ordering"] == "latest_self_improvement_pipeline_portfolio_order"
    assert output["read_only"] is True
    assert output["operator_gated"] is True
    assert output["candidates"][0]["required_authority"] == "operator_gate"
    assert output["candidates"][0]["operator_packet_order"] is not None
    assert output["candidates"][0]["pipeline_packet_evidence"]["source"] == "known_bad_hardening_shadow"
    assert output["recommended_next_candidate"]["candidate_id"] == "runtime:known_bad_hardening"
    assert output["operator_decision_packet"]["required_authority"] == "operator_gate"
    assert output["operator_decision_packet"]["promotion_effect"] == "none_until_separate_operator_gate"
    assert output["active_frontier_promotion"] is False


def test_runtime_main_known_bad_hardening_follow_on_review_prints_json_only(tmp_path, monkeypatch, capsys):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    )
    prepare_runtime_directories(cfg)
    require_runtime_databases(cfg)
    traces = HarnessVariantManager(str(tmp_path / "data" / "telemetry.db"))
    traces.log_skill_action_trace(
        task_id="council-pass",
        role="council_contract",
        skill_name="council",
        action_name="deliberate",
        intent_goal="safe council request",
        action_payload={"ok": True},
        context_assembled="council policy context",
        judge_verdict="PASS",
        outcome_score=0.9,
        created_at="2026-05-17T12:00:00+00:00",
    )
    traces.log_skill_action_trace(
        task_id="council-bad",
        role="council_contract",
        skill_name="council",
        action_name="deliberate",
        intent_goal="unsafe council request",
        action_payload={"ok": False},
        context_assembled="council policy context",
        judge_verdict="FAIL",
        outcome_score=0.0,
        training_eligible=False,
        retention_class="FAILURE_AUDIT",
        created_at="2026-05-17T12:01:00+00:00",
    )
    known_bad_hardening_shadow_report(
        cfg,
        repo_root=str(Path.cwd()),
        skill_name="council",
        sample_size=10,
        as_of="2026-05-17T12:02:00+00:00",
    )
    self_improvement_evidence_pipeline(
        cfg,
        repo_root=str(Path.cwd()),
        as_of="2026-05-17T12:03:00+00:00",
        candidate_limit=3,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "skills.runtime",
            "--known-bad-hardening-follow-on-review",
            "--candidate-id",
            "council:known_bad_hardening",
            "--skill-name",
            "council",
            "--operator-resolution",
            "approve_for_manual_promotion_review",
            "--data-dir",
            str(tmp_path / "data"),
            "--skills-dir",
            str(tmp_path / "skills"),
            "--checkpoints-dir",
            str(tmp_path / "skills" / "checkpoints"),
            "--alerts-dir",
            str(tmp_path / "alerts"),
        ],
    )

    exit_code = runtime_main()
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["review_surface"] == "known_bad_hardening_manual_promotion_follow_on_review"
    assert output["candidate_id"] == "council:known_bad_hardening"
    assert output["operator_resolution"] == "approve_for_manual_promotion_review"
    assert output["durable"] is True
    assert output["patch_review_packet"]["required_authority"] == "operator_gate"
    assert output["patch_review_packet"]["authority_effect"] == "review_only"
    assert output["active_frontier_promotion"] is False


def test_runtime_main_self_improvement_evidence_pipeline_prints_read_only_json(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "skills.runtime",
            "--self-improvement-evidence-pipeline",
            "--data-dir",
            str(tmp_path / "data"),
            "--skills-dir",
            str(tmp_path / "skills"),
            "--checkpoints-dir",
            str(tmp_path / "skills" / "checkpoints"),
            "--alerts-dir",
            str(tmp_path / "alerts"),
        ],
    )

    exit_code = runtime_main()
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["available"] is True
    assert output["live_controls_enabled"] is False
    assert output["run"]["promotion_packet_ids"]


def test_runtime_main_reports_runtime_setup_failure_cleanly(tmp_path, monkeypatch, capsys):
    def _boom(*args, **kwargs):
        raise RuntimeError(
            f"cannot create runtime directory '{tmp_path / 'blocked'}' (Operation not permitted); "
            "choose a writable path with --data-dir"
        )

    monkeypatch.setattr("skills.runtime.install_runtime_profile", _boom)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "skills.runtime",
            "--evidence-factory",
            "--data-dir",
            str(tmp_path / "data"),
            "--skills-dir",
            str(tmp_path / "skills"),
            "--checkpoints-dir",
            str(tmp_path / "skills" / "checkpoints"),
            "--alerts-dir",
            str(tmp_path / "alerts"),
        ],
    )

    exit_code = runtime_main()
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "runtime setup failed:" in output
    assert "choose a writable path with --data-dir" in output


def test_symlink_skill_directory_tolerates_racy_existing_link(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    dest = tmp_path / "dest"
    original = Path.symlink_to

    def _racy_symlink(self, target, target_is_directory=False):
        original(self, target, target_is_directory=target_is_directory)
        raise FileExistsError("simulated race")

    monkeypatch.setattr(Path, "symlink_to", _racy_symlink)

    _symlink_skill_directory(source, dest)

    assert dest.is_symlink()
    assert dest.resolve() == source.resolve()
