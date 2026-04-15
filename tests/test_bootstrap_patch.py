from __future__ import annotations

import sys
import time
import types

from immune.bootstrap_patch import _stack_has_immune_wrapper, apply_immune_patch
from immune.types import BlockReason, ImmuneBlockError, Outcome

from immune.types import CheckType, ImmuneVerdict, Tier, generate_uuid_v7


def _pass_verdict():
    return ImmuneVerdict(
        verdict_id=generate_uuid_v7(),
        check_type=CheckType.SHERIFF,
        tier=Tier.FAST_PATH,
        skill_name="immune_system",
        session_id=generate_uuid_v7(),
        outcome=Outcome.PASS,
        latency_ms=0.1,
    )


def _block_verdict():
    return ImmuneVerdict(
        verdict_id=generate_uuid_v7(),
        check_type=CheckType.SHERIFF,
        tier=Tier.FAST_PATH,
        skill_name="immune_system",
        session_id=generate_uuid_v7(),
        outcome=Outcome.BLOCK,
        block_reason=BlockReason.INTERNAL_ERROR,
        block_detail="x",
        latency_ms=0.1,
    )
from immune.verdict_logger import VerdictLogger


def _install_fake(dispatch):
    hermes = types.ModuleType("hermes")
    tools = types.ModuleType("hermes.tools")
    base = types.ModuleType("hermes.tools.base")
    base.execute_tool = dispatch
    sys.modules["hermes"] = hermes
    sys.modules["hermes.tools"] = tools
    sys.modules["hermes.tools.base"] = base


def test_patch_returns_true(default_config, test_db):
    _install_fake(lambda **_: {"ok": True})
    logger = VerdictLogger(test_db, default_config)
    assert apply_immune_patch(lambda p, c: _pass_verdict(), lambda p, c: _pass_verdict(), default_config, logger)


def test_patch_returns_false_when_missing(default_config, test_db):
    for key in ["hermes", "hermes.tools", "hermes.tools.base", "hermes.agent.executor", "hermes.core.tool_registry"]:
        sys.modules.pop(key, None)
    logger = VerdictLogger(test_db, default_config)
    assert apply_immune_patch(lambda p, c: _pass_verdict(), lambda p, c: _pass_verdict(), default_config, logger) is False


def test_pre_and_post_hooks(clean_sheriff_payload, default_config, test_db):
    called = {"s": 0, "j": 0, "dispatch": 0}

    def dispatch(*args, **kwargs):
        called["dispatch"] += 1
        return {"ok": True}

    _install_fake(dispatch)
    logger = VerdictLogger(test_db, default_config)

    def s_fn(*_):
        called["s"] += 1
        return _pass_verdict()

    def j_fn(*_):
        called["j"] += 1
        return _pass_verdict()

    assert apply_immune_patch(s_fn, j_fn, default_config, logger)
    out = sys.modules["hermes.tools.base"].execute_tool(tool_name="safe_tool", arguments={}, skill_name="immune_system", session_id=clean_sheriff_payload.session_id, execution_stack="immune_system")
    assert out["ok"] is True
    assert called == {"s": 1, "j": 1, "dispatch": 1}


def test_sheriff_block_raises(clean_sheriff_payload, default_config, test_db):
    _install_fake(lambda **_: {"ok": True})
    logger = VerdictLogger(test_db, default_config)
    assert apply_immune_patch(
        lambda *_: _block_verdict(),
        lambda *_: _pass_verdict(),
        default_config,
        logger,
    )
    try:
        sys.modules["hermes.tools.base"].execute_tool(tool_name="safe_tool", arguments={}, skill_name="immune_system", session_id=clean_sheriff_payload.session_id)
    except ImmuneBlockError:
        assert True
    else:
        assert False


def test_judge_block_raises(clean_sheriff_payload, default_config, test_db):
    _install_fake(lambda **_: {"ok": True})
    logger = VerdictLogger(test_db, default_config)
    assert apply_immune_patch(
        lambda *_: _pass_verdict(),
        lambda *_: _block_verdict(),
        default_config,
        logger,
    )
    try:
        sys.modules["hermes.tools.base"].execute_tool(tool_name="safe_tool", arguments={}, skill_name="immune_system", session_id=clean_sheriff_payload.session_id)
    except ImmuneBlockError:
        assert True
    else:
        assert False


def test_patch_overhead(clean_sheriff_payload, default_config, test_db):
    _install_fake(lambda **_: {"ok": True})
    logger = VerdictLogger(test_db, default_config)
    assert apply_immune_patch(lambda *_: _pass_verdict(), lambda *_: _pass_verdict(), default_config, logger)
    fn = sys.modules["hermes.tools.base"].execute_tool
    start = time.monotonic_ns()
    for _ in range(100):
        fn(tool_name="safe_tool", arguments={}, skill_name="immune_system", session_id=clean_sheriff_payload.session_id, execution_stack="immune_system")
    avg_ms = ((time.monotonic_ns() - start) / 1_000_000) / 100
    assert avg_ms < 5


def test_patch_disabled(default_config, test_db):
    cfg = default_config.__class__(**{**default_config.__dict__, "bootstrap_patch_enabled": False})
    logger = VerdictLogger(test_db, cfg)
    assert apply_immune_patch(lambda *_: None, lambda *_: None, cfg, logger)


def test_stack_parser_handles_structured_stacks():
    assert _stack_has_immune_wrapper(("planner", "immune_system", "shell_command"))
    assert _stack_has_immune_wrapper("planner > immune_system > shell_command")
    assert not _stack_has_immune_wrapper({"stack": ("planner", "shell_command")})
