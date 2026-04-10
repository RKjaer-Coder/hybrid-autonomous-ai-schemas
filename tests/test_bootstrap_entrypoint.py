from __future__ import annotations

import sys
import types

import bootstrap_patch


def test_bootstrap_entrypoint_applies_patch(monkeypatch, default_config):
    hermes = types.ModuleType("hermes")
    tools = types.ModuleType("hermes.tools")
    base = types.ModuleType("hermes.tools.base")
    base.execute_tool = lambda **_: {"ok": True}
    sys.modules["hermes"] = hermes
    sys.modules["hermes.tools"] = tools
    sys.modules["hermes.tools.base"] = base

    class DummyLogger:
        def __init__(self, *_):
            pass

        def log_verdict(self, *_):
            return None

        def log_bypass(self, *_):
            return None

    monkeypatch.setattr(bootstrap_patch, "load_config", lambda: default_config)
    monkeypatch.setattr(bootstrap_patch, "VerdictLogger", DummyLogger)

    assert bootstrap_patch.bootstrap_immune_patch("/tmp/immune_test.db") is True
