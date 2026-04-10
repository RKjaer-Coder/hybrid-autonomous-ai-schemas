"""Eval backend that runs M1 Sheriff checks against the real immune system code."""

from __future__ import annotations

import time

from eval.runner import MockBackend
from immune.config import load_config
from immune.sheriff import sheriff_check
from immune.types import SheriffPayload, generate_uuid_v7


class Backend(MockBackend):
    """Real-immune M1 backend with MockBackend fallbacks for non-M1 methods."""

    def __init__(self) -> None:
        super().__init__()
        self._config = load_config()

    def sheriff_check(self, task: dict) -> dict:
        if self.disabled:
            return {
                "verdict": "BLOCK",
                "scan_tier": "fast_path",
                "latency_ms": 10,
                "alert_source": "immune_timeout_sheriff",
            }

        structured = task.get("structured_fields", {})
        args = structured.get("parameters", structured) if isinstance(structured, dict) else {"payload": structured}
        if task.get("category") == "clean" and task.get("task_type") == "shell_command":
            args = dict(args) if isinstance(args, dict) else {"value": args}
            args.setdefault("burner_room", True)
        raw_prompt = task.get("input_text") or ""
        context = structured.get("context") if isinstance(structured, dict) else None
        if context:
            raw_prompt = f"{raw_prompt}\n{context}"

        payload = SheriffPayload(
            session_id=task.get("task_id") or generate_uuid_v7(),
            skill_name=task.get("skill_name", "unknown_skill"),
            tool_name=task.get("task_type", "safe_tool"),
            arguments=args if isinstance(args, dict) else {"value": args},
            raw_prompt=raw_prompt,
            source_trust_tier=int(task.get("source_trust_tier", 4)),
            jwt_claims=task.get("jwt_claims"),
        )

        started = time.monotonic_ns()
        verdict = sheriff_check(payload, self._config)
        latency = (time.monotonic_ns() - started) / 1_000_000
        out = {
            "verdict": verdict.outcome.value,
            "scan_tier": verdict.tier.value,
            "latency_ms": round(max(verdict.latency_ms, latency), 3),
        }
        if verdict.block_reason is not None and verdict.block_reason.value == "TIMEOUT":
            out["alert_source"] = "immune_timeout_sheriff"
        return out
