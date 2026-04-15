from __future__ import annotations

import json
from pathlib import Path

from skills.config import IntegrationConfig
from skills.hermes_interfaces import HermesSessionContext, MockHermesRuntime
from skills.runtime import (
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
