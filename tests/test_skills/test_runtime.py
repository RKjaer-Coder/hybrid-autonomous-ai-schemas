from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from skills.config import IntegrationConfig
from skills.hermes_interfaces import HermesSessionContext, MockHermesRuntime
from skills.runtime import (
    VERSION_DRIFT_NOTE,
    ExternalCommandResult,
    assess_hermes_readiness,
    bootstrap_runtime,
    doctor_runtime,
    install_runtime_profile,
    make_session_context,
    migrate_runtime_databases,
    prepare_runtime_directories,
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


def test_migrate_runtime_databases_builds_all_sqlite_files(tmp_path):
    cfg = IntegrationConfig(data_dir=str(tmp_path / "data"))
    status = migrate_runtime_databases(cfg)
    assert all(status.values())
    assert (tmp_path / "data" / "strategic_memory.db").exists()
    assert (tmp_path / "data" / "immune_system.db").exists()
    assert (tmp_path / "data" / "telemetry.db").exists()
    assert (tmp_path / "data" / "financial_ledger.db").exists()
    assert (tmp_path / "data" / "operator_digest.db").exists()


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
    assert manifest_path.is_file()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["profile_name"] == "hybrid-test"
    assert manifest["repo_root"] == str(repo_root.resolve())
    assert "readiness" in manifest["commands"]
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
    assert result.path_status["readiness_launcher"] is True


def test_assess_hermes_readiness_fails_clearly_without_hermes(tmp_path, monkeypatch):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
        profile_name="hybrid-test",
    )
    profile_dir = tmp_path / "profiles" / "hybrid-test"
    profile_dir.mkdir(parents=True)
    (profile_dir / "profile.yaml").write_text("profile: hybrid-test\n", encoding="utf-8")
    monkeypatch.setattr("skills.runtime.shutil.which", lambda _name: None)

    result = assess_hermes_readiness(config=cfg, repo_root=str(tmp_path))

    assert result.ok is False
    assert result.hermes_installed is False
    assert result.doctor.ok is True
    assert all(result.database_status.values())
    assert Path(result.checkpoint_backup_path).is_file()
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
    profile_dir = tmp_path / "profiles" / "hybrid-test"
    profile_dir.mkdir(parents=True)
    (profile_dir / "profile.yaml").write_text(
        "\n".join(
            [
                "profile: hybrid-test",
                "routing:",
                "  strong_model: anthropic/claude-opus-4",
                "limits:",
                "  max_api_spend_usd: 0.0",
                "endpoint: http://localhost:11434",
                "approvals:",
                "  mode: manual",
                "dangerous_commands:",
                "  - rm -rf",
                "  - chmod 777",
                "  - sudo",
                "  - mkfs",
                "  - iptables",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "logs").mkdir()

    monkeypatch.setattr("skills.runtime.shutil.which", lambda _name: "/usr/bin/hermes")

    responses = {
        ("hermes", "--version"): ExternalCommandResult(
            ok=True,
            command=("hermes", "--version"),
            returncode=0,
            stdout="Hermes Agent 0.9.1",
            stderr="",
        ),
        ("hermes", "profile", "list"): ExternalCommandResult(
            ok=True,
            command=("hermes", "profile", "list"),
            returncode=0,
            stdout="default\nhybrid-test\n",
            stderr="",
        ),
        ("hermes", "tools", "list"): ExternalCommandResult(
            ok=True,
            command=("hermes", "tools", "list"),
            returncode=0,
            stdout="\n".join(
                [
                    "code_execution",
                    "file_operations",
                    "web_search",
                    "web_fetch",
                    "shell_command",
                ]
            ),
            stderr="",
        ),
        ("hermes", "--profile", "hybrid-test", "config"): ExternalCommandResult(
            ok=True,
            command=("hermes", "--profile", "hybrid-test", "config"),
            returncode=0,
            stdout=(tmp_path / "profiles" / "hybrid-test" / "profile.yaml").read_text(encoding="utf-8"),
            stderr="",
        ),
    }

    result = assess_hermes_readiness(
        config=cfg,
        repo_root=str(tmp_path),
        run_cli_smoke=False,
        command_runner=lambda argv: responses[tuple(argv)],
    )

    assert result.ok is True
    assert result.hermes_installed is True
    assert result.hermes_version == "0.9.1"
    assert result.profile_listed is True
    assert all(result.seed_tool_status.values())
    assert all(result.config_status.values())
    assert Path(result.checkpoint_backup_path).is_file()
    assert result.doctor.ok is True
    assert not result.blocking_items


def test_assess_hermes_readiness_rejects_0_8_x_even_if_checklist_allows_it(tmp_path, monkeypatch):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
        profile_name="hybrid-test",
    )
    profile_dir = tmp_path / "profiles" / "hybrid-test"
    profile_dir.mkdir(parents=True)
    (profile_dir / "profile.yaml").write_text(
        "profile: hybrid-test\nendpoint: http://localhost:11434\nstrong_model: local\nmax_api_spend_usd: 0.0\napprovals:\n  mode: manual\ndangerous_commands:\n  - rm -rf\n  - chmod 777\n  - sudo\n  - mkfs\n  - iptables\n",
        encoding="utf-8",
    )
    (tmp_path / "logs").mkdir()

    monkeypatch.setattr("skills.runtime.shutil.which", lambda _name: "/usr/bin/hermes")

    responses = {
        ("hermes", "--version"): ExternalCommandResult(
            ok=True,
            command=("hermes", "--version"),
            returncode=0,
            stdout="Hermes Agent 0.8.9",
            stderr="",
        ),
        ("hermes", "profile", "list"): ExternalCommandResult(
            ok=True,
            command=("hermes", "profile", "list"),
            returncode=0,
            stdout="hybrid-test\n",
            stderr="",
        ),
        ("hermes", "tools", "list"): ExternalCommandResult(
            ok=True,
            command=("hermes", "tools", "list"),
            returncode=0,
            stdout="\n".join(
                [
                    "code_execution",
                    "file_operations",
                    "web_search",
                    "web_fetch",
                    "shell_command",
                ]
            ),
            stderr="",
        ),
        ("hermes", "--profile", "hybrid-test", "config"): ExternalCommandResult(
            ok=True,
            command=("hermes", "--profile", "hybrid-test", "config"),
            returncode=0,
            stdout=(tmp_path / "profiles" / "hybrid-test" / "profile.yaml").read_text(encoding="utf-8"),
            stderr="",
        ),
    }

    result = assess_hermes_readiness(
        config=cfg,
        repo_root=str(tmp_path),
        run_cli_smoke=False,
        command_runner=lambda argv: responses[tuple(argv)],
    )

    assert result.ok is False
    assert result.hermes_version == "0.8.9"
    assert any("below the manifest floor" in item for item in result.blocking_items)
    assert VERSION_DRIFT_NOTE in result.drift_items


def test_assess_hermes_readiness_cli_smoke_checks_step_outcome_and_logs(tmp_path, monkeypatch):
    cfg = IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
        profile_name="hybrid-test",
    )
    profile_dir = tmp_path / "profiles" / "hybrid-test"
    profile_dir.mkdir(parents=True)
    profile_text = (
        "profile: hybrid-test\n"
        "endpoint: http://localhost:11434\n"
        "strong_model: anthropic/claude-opus-4\n"
        "max_api_spend_usd: 0.0\n"
        "approvals:\n"
        "  mode: manual\n"
        "dangerous_commands:\n"
        "  - rm -rf\n"
        "  - chmod 777\n"
        "  - sudo\n"
        "  - mkfs\n"
        "  - iptables\n"
    )
    (profile_dir / "profile.yaml").write_text(profile_text, encoding="utf-8")
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()

    monkeypatch.setattr("skills.runtime.shutil.which", lambda _name: "/usr/bin/hermes")

    def runner(argv):
        key = tuple(argv)
        if key == ("hermes", "--version"):
            return ExternalCommandResult(True, key, 0, "Hermes Agent 0.9.1", "")
        if key == ("hermes", "profile", "list"):
            return ExternalCommandResult(True, key, 0, "hybrid-test\n", "")
        if key == ("hermes", "tools", "list"):
            return ExternalCommandResult(True, key, 0, "code_execution\nfile_operations\nweb_search\nweb_fetch\nshell_command\n", "")
        if key == ("hermes", "--profile", "hybrid-test", "config"):
            return ExternalCommandResult(True, key, 0, profile_text, "")
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
    assert result.cli_smoke_step_outcomes_delta == 1
    assert result.cli_smoke_log_trace is True
    assert result.cli_smoke_output == result.cli_smoke_marker
    assert Path(result.checkpoint_backup_path).is_file()


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
