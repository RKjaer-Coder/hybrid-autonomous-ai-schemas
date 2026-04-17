from __future__ import annotations

import datetime
import time
from typing import Optional

from immune.config import load_config
from immune.judge import judge_check
from immune.judge_lifecycle import JudgeLifecycleManager
from immune.sheriff import sheriff_check
from immune.types import ImmuneVerdict, JudgePayload, SheriffPayload
from skills.append_buffer import AppendBuffer


class ImmuneSystemSkill:
    def __init__(self, verdict_buffer: Optional[AppendBuffer] = None, immune_db_path: str | None = None):
        self._config = load_config()
        self._buffer = verdict_buffer
        self._judge_lifecycle = None if immune_db_path is None else JudgeLifecycleManager(immune_db_path, self._config)

    def check_sheriff(self, payload: SheriffPayload) -> ImmuneVerdict:
        start = time.monotonic_ns()
        verdict = sheriff_check(payload, self._config)
        latency_ms = (time.monotonic_ns() - start) / 1_000_000
        if self._buffer:
            self._buffer.append(self._verdict_to_row(verdict, latency_ms))
        return verdict

    def check_judge(self, payload: JudgePayload) -> ImmuneVerdict:
        start = time.monotonic_ns()
        active_event = None
        prepared = payload
        if self._judge_lifecycle is not None:
            prepared, active_event = self._judge_lifecycle.prepare_payload(payload)
        if active_event is not None and active_event["status"] == "HALTED":
            verdict = self._judge_lifecycle.halted_verdict(prepared)
        else:
            verdict = judge_check(prepared, self._config)
        latency_ms = (time.monotonic_ns() - start) / 1_000_000
        if self._buffer:
            self._buffer.append(self._verdict_to_row(verdict, latency_ms))
        if self._judge_lifecycle is not None:
            self._judge_lifecycle.record_verdict(prepared, verdict)
        return verdict

    def _verdict_to_row(self, verdict: ImmuneVerdict, latency_ms: float) -> tuple:
        return (
            verdict.verdict_id,
            "sheriff_input" if verdict.check_type.value == "sheriff" else "judge_output",
            verdict.tier.value,
            verdict.session_id,
            verdict.skill_name,
            verdict.outcome.value,
            verdict.block_reason.value if verdict.block_reason else verdict.block_detail,
            int(latency_ms),
            verdict.judge_mode.value,
            datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
        )


_SKILL: Optional[ImmuneSystemSkill] = None


def configure_skill(verdict_buffer: Optional[AppendBuffer] = None, immune_db_path: str | None = None):
    global _SKILL
    _SKILL = ImmuneSystemSkill(verdict_buffer=verdict_buffer, immune_db_path=immune_db_path)


def immune_system_entry(action: str, **kwargs):
    if _SKILL is None:
        configure_skill()
    assert _SKILL is not None
    if action == "sheriff":
        return _SKILL.check_sheriff(kwargs["payload"])
    if action == "judge":
        return _SKILL.check_judge(kwargs["payload"])
    raise ValueError(f"Unknown action: {action}")
