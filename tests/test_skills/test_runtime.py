from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

from harness_variants import HarnessVariantManager
from runtime_control import RuntimeControlManager
from skills.config import IntegrationConfig
from skills.hermes_interfaces import HermesSessionContext, MockHermesRuntime
from skills.runtime import (
    VERSION_DRIFT_NOTE,
    ExternalCommandResult,
    assess_hermes_readiness,
    bootstrap_stack,
    bootstrap_runtime,
    doctor_runtime,
    exercise_hermes_contract,
    install_runtime_profile,
    make_session_context,
    main as runtime_main,
    migrate_runtime_databases,
    prepare_runtime_directories,
    run_research_cron_proof,
    run_task_loop_proof,
    run_operator_workflow,
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
    assert manifest["profile_name"] == "hybrid-test"
    assert manifest["repo_root"] == str(repo_root.resolve())
    assert manifest["profile_config_path"] == str(profile_config_path)
    assert manifest["spec_profile_path"] == str(spec_profile_path)
    assert Path(manifest["network_controls_path"]).is_file()
    assert Path(manifest["gateway_manifest_path"]).is_file()
    assert Path(manifest["workspace_manifest_path"]).is_file()
    assert Path(manifest["operator_validation_checklist_path"]).is_file()
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["profile_name"] == "hybrid-test"
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["repo_contract_version"] == 1
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["routing"]["max_api_spend_usd"] == 0.0
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["network_controls"]["proxy_bind_url"] == "http://127.0.0.1:8877"
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["workspace"]["enabled"] is True
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["gateway"]["expected_tools"] == [
        "web_search",
        "web_fetch",
        "image_generation",
        "browser_automation",
        "tts",
    ]
    assert profile_config["skills"]["config"]["hybrid_autonomous_ai"]["workspace"]["preferred_surfaces"] == [
        "gates",
        "execution_traces",
        "quarantines",
        "replay_readiness",
        "runtime_halt_state",
        "milestone_health",
    ]
    assert profile_config["approvals"]["mode"] == "manual"
    assert spec_profile["profile"] == "hybrid-test"
    assert spec_profile["limits"]["max_api_spend_usd"] == 0.0
    assert spec_profile["network_controls"]["proxy_bind_url"] == "http://127.0.0.1:8877"
    assert "readiness" in manifest["commands"]
    assert "contract_harness" in manifest["commands"]
    assert "bootstrap_stack" in manifest["commands"]
    assert "task_loop_proof" in manifest["commands"]
    assert "research_cron_proof" in manifest["commands"]
    assert "milestone_status" in manifest["commands"]
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
    assert result.path_status["contract_harness_launcher"] is True


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
            return ExternalCommandResult(True, key, 0, "Hermes Agent 0.10.0", "")
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
        raise AssertionError(f"unexpected command: {key}")

    result = assess_hermes_readiness(
        config=cfg,
        repo_root=str(tmp_path),
        run_cli_smoke=False,
        command_runner=runner,
    )

    assert result.ok is True
    assert result.hermes_installed is True
    assert result.hermes_version == "0.10.0"
    assert result.profile_listed is True
    assert all(result.seed_tool_status.values())
    assert all(result.config_status.values())
    assert result.doctor.profile_validation.ok is True
    assert result.profile_validation.ok is True
    assert Path(result.checkpoint_backup_path).is_file()
    assert result.doctor.ok is True
    assert result.contract_harness.ok is True
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
            return ExternalCommandResult(True, key, 0, "Hermes Agent 0.10.0", "")
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
            return ExternalCommandResult(True, key, 0, "Hermes Agent 0.10.0", "")
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
        if key[:4] == ("hermes", "--profile", "hybrid-test", "chat"):
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
    assert result.milestone_status["milestones"]["M2"]["implemented"] is True
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
