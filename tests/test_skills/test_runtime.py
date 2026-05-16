from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

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
    make_session_context,
    main as runtime_main,
    migration_readiness,
    migrate_runtime_databases,
    optimizer_snapshot,
    pre_hermes_readiness,
    prepare_runtime_directories,
    readiness_suite,
    recovery_readiness,
    replay_readiness_report,
    require_runtime_databases,
    self_improvement_evidence_pipeline,
    self_improvement_snapshot,
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
    assert "--self-improvement-evidence-pipeline" in workspace_manifest["self_improvement_evidence_pipeline_command"]
    assert "--self-improvement-snapshot" in workspace_manifest["self_improvement_snapshot_command"]
    assert "readiness_suite" in workspace_manifest["read_only_readiness_surfaces"]
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
    assert all(result.v012_contract_checks.values())
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
            return ExternalCommandResult(True, key, 0, "Hermes Agent 0.12.0", "")
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
    assert result.hermes_version == "0.12.0"
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
            return ExternalCommandResult(True, key, 0, "Hermes Agent 0.12.0", "")
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
            return ExternalCommandResult(True, key, 0, "Hermes Agent 0.12.0", "")
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
    assert payload["packet"]["adapter_name"] == "hermes-v0.13"
    assert payload["packet"]["hermes_version"] == "0.13.0"
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
    assert by_surface["custom_mission_control_dashboard"]["readiness_status"] == "retired"
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
    assert "autonomous_promotion" in payload["disabled_live_controls"]
    assert Path(payload["artifact_path"]).is_file()


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
    assert output["packet"]["adapter_name"] == "hermes-v0.13"
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
