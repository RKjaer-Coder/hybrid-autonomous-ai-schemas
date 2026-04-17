from __future__ import annotations

import asyncio
import functools
import importlib
import re
import threading
from typing import Any, Callable

from immune.config import load_config
from immune.judge import judge_check
from immune.judge_lifecycle import JudgeLifecycleManager
from immune.sheriff import sheriff_check, trigger_deep_scan
from immune.types import ImmuneBlockError, ImmuneConfig, JudgePayload, SheriffPayload
from immune.verdict_logger import VerdictLogger

CANDIDATES = [
    ("hermes.tools.base", "execute_tool"),
    ("hermes.agent.executor", "dispatch_tool"),
    ("hermes.core.tool_registry", "invoke"),
]
_STACK_TOKEN_SPLIT_RE = re.compile(r"[\s>/,:]+")


def _stack_has_immune_wrapper(execution_stack: Any) -> bool:
    """Detect the immune wrapper in structured or string execution stacks."""
    if execution_stack is None:
        return False
    if isinstance(execution_stack, str):
        tokens = [token for token in _STACK_TOKEN_SPLIT_RE.split(execution_stack) if token]
        return "immune_system" in tokens
    if isinstance(execution_stack, dict):
        return any(_stack_has_immune_wrapper(value) for value in execution_stack.values())
    if isinstance(execution_stack, (list, tuple, set, frozenset)):
        return any(_stack_has_immune_wrapper(value) for value in execution_stack)
    return False


def _locate_dispatch() -> tuple[Any, str, Callable[..., Any]] | None:
    for mod_name, attr in CANDIDATES:
        try:
            mod = importlib.import_module(mod_name)
            fn = getattr(mod, attr)
            if callable(fn):
                return mod, attr, fn
        except Exception:
            continue
    return None


def apply_immune_patch(
    sheriff_fn: Callable[..., Any] | None = None,
    judge_fn: Callable[..., Any] | None = None,
    config: ImmuneConfig | None = None,
    verdict_logger: VerdictLogger | None = None,
) -> bool:
    """Apply monkey-patch wrapper around Hermes dispatch.

    Returns False when dispatch cannot be located. Callers must treat False as a
    fatal startup condition because immune enforcement is not active.
    """
    config = config or load_config()
    sheriff_fn = sheriff_fn or sheriff_check
    judge_fn = judge_fn or judge_check
    if verdict_logger is None:
        raise ValueError("verdict_logger is required")
    if not config.bootstrap_patch_enabled:
        return True
    judge_lifecycle = None
    db_path = getattr(verdict_logger, "db_path", None)
    if isinstance(db_path, str):
        judge_lifecycle = JudgeLifecycleManager(db_path, config)
    located = _locate_dispatch()
    if located is None:
        return False
    module, attr, original_fn = located

    @functools.wraps(original_fn)
    def wrapped_dispatch(*args: Any, **kwargs: Any) -> Any:
        tool_name = kwargs.get("tool_name") or (args[0] if args else "unknown")
        tool_args = kwargs.get("arguments") or kwargs.get("args") or {}
        skill_name = kwargs.get("skill_name", "unknown_skill")
        session_id = kwargs.get("session_id", "unknown_session")
        payload = SheriffPayload(
            session_id=session_id,
            skill_name=skill_name,
            tool_name=tool_name,
            arguments=tool_args if isinstance(tool_args, dict) else {"value": tool_args},
            raw_prompt=kwargs.get("raw_prompt"),
            source_trust_tier=int(kwargs.get("source_trust_tier", 4)),
            jwt_claims=kwargs.get("jwt_claims"),
        )
        verdict = sheriff_fn(payload, config)
        verdict_logger.log_verdict(verdict)
        if verdict.outcome.value == "BLOCK":
            raise ImmuneBlockError(verdict)
        if config.deep_scan_enabled:
            model = kwargs.get("deep_scan_model")
            if model is not None:
                try:
                    asyncio.get_running_loop().create_task(trigger_deep_scan(payload, config, model))
                except RuntimeError:
                    threading.Thread(
                        target=lambda: asyncio.run(trigger_deep_scan(payload, config, model)),
                        daemon=True,
                    ).start()

        if not _stack_has_immune_wrapper(kwargs.get("execution_stack")):
            verdict_logger.log_bypass(skill_name, session_id, "direct_dispatch", "Missing immune wrapper")

        output = original_fn(*args, **kwargs)
        judge_payload = JudgePayload(
            session_id=session_id,
            skill_name=skill_name,
            tool_name=tool_name,
            output=output if isinstance(output, dict) else {"result": output},
            task_type=kwargs.get("task_type"),
            expected_schema=kwargs.get("expected_schema"),
            max_trust_tier=int(kwargs.get("max_trust_tier", 4)),
            memory_write_target=kwargs.get("memory_write_target"),
            allow_structural_fallback=bool(
                kwargs.get("allow_judge_structural_fallback", config.judge_structural_fallback_enabled)
            ),
            force_structural_fallback=bool(
                kwargs.get("force_judge_structural_fallback")
                or str(kwargs.get("judge_mode", "")).upper() == "FALLBACK"
                or kwargs.get("judge_degraded", False)
            ),
            fallback_reason=kwargs.get("judge_fallback_reason"),
        )
        prepared_payload = judge_payload
        lifecycle_event = None
        if judge_lifecycle is not None:
            prepared_payload, lifecycle_event = judge_lifecycle.prepare_payload(judge_payload)
        if judge_lifecycle is not None and lifecycle_event is not None and lifecycle_event["status"] == "HALTED":
            jverdict = judge_lifecycle.halted_verdict(prepared_payload)
        else:
            jverdict = judge_fn(prepared_payload, config)
        verdict_logger.log_verdict(jverdict)
        if judge_lifecycle is not None:
            judge_lifecycle.record_verdict(prepared_payload, jverdict)
        if jverdict.outcome.value == "BLOCK":
            raise ImmuneBlockError(jverdict)
        return output

    setattr(module, attr, wrapped_dispatch)
    return True


if __name__ == "__main__":
    print("ok")
