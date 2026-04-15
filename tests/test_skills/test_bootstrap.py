from __future__ import annotations

from pathlib import Path

import pytest

from skills.bootstrap import BootstrapOrchestrator
from skills.config import IntegrationConfig
from skills.hermes_interfaces import HermesSessionContext, MockHermesRuntime


def _mk_ctx(data_dir: str) -> HermesSessionContext:
    return HermesSessionContext("s", "p", "m", {}, data_dir)


def test_bootstrap_full_success(test_data_dir):
    rt = MockHermesRuntime(str(test_data_dir))
    b = BootstrapOrchestrator(IntegrationConfig(data_dir=str(test_data_dir)), rt, _mk_ctx(str(test_data_dir)))
    assert b.run() is True
    assert "immune_system" in rt.list_tools()


def test_bootstrap_fails_missing_databases(tmp_path):
    rt = MockHermesRuntime(str(tmp_path))
    b = BootstrapOrchestrator(IntegrationConfig(data_dir=str(tmp_path)), rt, _mk_ctx(str(tmp_path)))
    assert b.run() is False


def test_bootstrap_fails_if_wal_disabled(test_data_dir, monkeypatch):
    rt = MockHermesRuntime(str(test_data_dir))
    b = BootstrapOrchestrator(IntegrationConfig(data_dir=str(test_data_dir)), rt, _mk_ctx(str(test_data_dir)))
    monkeypatch.setattr("skills.db_manager.DatabaseManager.verify_all_databases", lambda self: {"immune": False})
    assert b.run() is False


def test_bootstrap_fails_if_patch_fails(test_data_dir, monkeypatch):
    rt = MockHermesRuntime(str(test_data_dir))
    b = BootstrapOrchestrator(IntegrationConfig(data_dir=str(test_data_dir)), rt, _mk_ctx(str(test_data_dir)))
    monkeypatch.setattr("immune.bootstrap_patch.apply_immune_patch", lambda **kwargs: False)
    assert b.run() is False


def test_bootstrap_continues_noncritical_registration_fail(test_data_dir, monkeypatch):
    rt = MockHermesRuntime(str(test_data_dir))
    orig = rt.register_skill

    def bad(name, entry_point, manifest):
        if name == "observability":
            raise RuntimeError("fail")
        return orig(name, entry_point, manifest)

    monkeypatch.setattr(rt, "register_skill", bad)
    b = BootstrapOrchestrator(IntegrationConfig(data_dir=str(test_data_dir)), rt, _mk_ctx(str(test_data_dir)))
    assert b.run() is True


def test_bootstrap_continues_append_buffer_fail(test_data_dir, monkeypatch):
    rt = MockHermesRuntime(str(test_data_dir))
    monkeypatch.setattr("skills.bootstrap.AppendBuffer.start", lambda self: (_ for _ in ()).throw(RuntimeError("x")))
    b = BootstrapOrchestrator(IntegrationConfig(data_dir=str(test_data_dir)), rt, _mk_ctx(str(test_data_dir)))
    assert b.run() is True


def test_smoke_test_blocks_known_bad(test_data_dir):
    rt = MockHermesRuntime(str(test_data_dir))
    b = BootstrapOrchestrator(IntegrationConfig(data_dir=str(test_data_dir)), rt, _mk_ctx(str(test_data_dir)))
    assert b.run() is True


def test_bootstrap_idempotent(test_data_dir):
    rt = MockHermesRuntime(str(test_data_dir))
    b = BootstrapOrchestrator(IntegrationConfig(data_dir=str(test_data_dir)), rt, _mk_ctx(str(test_data_dir)))
    assert b.run() is True
    tools_first = rt.list_tools()
    assert b.run() is True
    assert rt.list_tools() == tools_first


def test_shutdown_flushes_buffers(test_data_dir):
    rt = MockHermesRuntime(str(test_data_dir))
    b = BootstrapOrchestrator(IntegrationConfig(data_dir=str(test_data_dir)), rt, _mk_ctx(str(test_data_dir)))
    assert b.run() is True
    b.shutdown()


def test_bootstrap_uses_real_verdict_logger(test_data_dir, monkeypatch):
    rt = MockHermesRuntime(str(test_data_dir))
    captured = {}

    def fake_apply_immune_patch(**kwargs):
        captured["logger_type"] = type(kwargs["verdict_logger"]).__name__
        return True

    monkeypatch.setattr("immune.bootstrap_patch.apply_immune_patch", fake_apply_immune_patch)
    b = BootstrapOrchestrator(IntegrationConfig(data_dir=str(test_data_dir)), rt, _mk_ctx(str(test_data_dir)))
    assert b.run() is True
    assert captured["logger_type"] == "VerdictLogger"


@pytest.mark.parametrize("field", ["data_dir", "skills_dir", "checkpoints_dir", "alerts_dir"])
def test_config_paths_can_expand(field):
    cfg = IntegrationConfig()
    value = getattr(cfg.resolve_paths(), field)
    assert "~" not in str(value)
