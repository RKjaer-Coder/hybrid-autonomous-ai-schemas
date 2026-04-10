from __future__ import annotations

import asyncio
import functools
import importlib
from typing import Any, Callable

from immune.judge import judge_check
from immune.sheriff import sheriff_check, trigger_deep_scan
from immune.types import ImmuneBlockError, ImmuneConfig, JudgePayload, SheriffPayload
from immune.verdict_logger import VerdictLogger

CANDIDATES = [
    ("hermes.tools.base", "execute_tool"),
    ("hermes.agent.executor", "dispatch_tool"),
    ("hermes.core.tool_registry", "invoke"),
]


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
    sheriff_fn: Callable[..., Any],
    judge_fn: Callable[..., Any],
    config: ImmuneConfig,
    verdict_logger: VerdictLogger,
) -> bool:
    """Apply monkey-patch wrapper around Hermes dispatch."""
    if not config.bootstrap_patch_enabled:
        return True
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
                asyncio.get_event_loop().create_task(trigger_deep_scan(payload, config, model))

        if "immune_system" not in str(kwargs.get("execution_stack", "")):
            verdict_logger.log_bypass(skill_name, session_id, "direct_dispatch", "Missing immune wrapper")

        output = original_fn(*args, **kwargs)
        judge_payload = JudgePayload(
            session_id=session_id,
            skill_name=skill_name,
            tool_name=tool_name,
            output=output if isinstance(output, dict) else {"result": output},
            expected_schema=kwargs.get("expected_schema"),
            max_trust_tier=int(kwargs.get("max_trust_tier", 4)),
            memory_write_target=kwargs.get("memory_write_target"),
        )
        jverdict = judge_fn(judge_payload, config)
        verdict_logger.log_verdict(jverdict)
        if jverdict.outcome.value == "BLOCK":
            raise ImmuneBlockError(jverdict)
        return output

    setattr(module, attr, wrapped_dispatch)
    return True


if __name__ == "__main__":
    print("ok")
