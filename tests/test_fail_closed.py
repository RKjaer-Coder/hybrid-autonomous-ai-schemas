from __future__ import annotations

import types

from immune import judge, sheriff
from immune.types import BlockReason, JudgePayload, Outcome, SheriffPayload, generate_uuid_v7


def test_sheriff_none_payload(default_config):
    v = sheriff.sheriff_check(None, default_config)
    assert v.outcome == Outcome.BLOCK and v.block_reason == BlockReason.INTERNAL_ERROR


def test_sheriff_wrong_session_type(default_config):
    p = SheriffPayload(session_id=123, skill_name="x", tool_name="safe_tool", arguments={})
    assert sheriff.sheriff_check(p, default_config).outcome == Outcome.BLOCK


def test_sheriff_arguments_access_error(default_config):
    class Bad(dict):
        def items(self):
            raise RuntimeError("boom")

    p = SheriffPayload(session_id=generate_uuid_v7(), skill_name="x", tool_name="safe_tool", arguments=Bad())
    assert sheriff.sheriff_check(p, default_config).outcome == Outcome.BLOCK


def test_sheriff_structural_raises(monkeypatch, clean_sheriff_payload, default_config):
    monkeypatch.setattr(sheriff, "validate_structure", lambda *_: (_ for _ in ()).throw(RuntimeError("x")))
    assert sheriff.sheriff_check(clean_sheriff_payload, default_config).outcome == Outcome.BLOCK


def test_sheriff_ipi_raises(monkeypatch, clean_sheriff_payload, default_config):
    monkeypatch.setattr(sheriff, "validate_structure", lambda *_: None)
    monkeypatch.setattr(sheriff, "classify_ipi", lambda *_: (_ for _ in ()).throw(RuntimeError("x")))
    assert sheriff.sheriff_check(clean_sheriff_payload, default_config).outcome == Outcome.BLOCK


def test_sheriff_policy_raises(monkeypatch, clean_sheriff_payload, default_config):
    monkeypatch.setattr(sheriff, "validate_structure", lambda *_: None)
    monkeypatch.setattr(sheriff, "classify_ipi", lambda *_: None)
    monkeypatch.setattr(sheriff, "check_policy", lambda *_: (_ for _ in ()).throw(RuntimeError("x")))
    assert sheriff.sheriff_check(clean_sheriff_payload, default_config).outcome == Outcome.BLOCK


def test_sheriff_time_raises(monkeypatch, clean_sheriff_payload, default_config):
    monkeypatch.setattr(sheriff.time, "monotonic_ns", lambda: (_ for _ in ()).throw(RuntimeError("x")))
    assert sheriff.sheriff_check(clean_sheriff_payload, default_config).outcome == Outcome.BLOCK


def test_sheriff_uuid_generation_raises(monkeypatch, clean_sheriff_payload, default_config):
    monkeypatch.setattr(sheriff, "generate_uuid_v7", lambda: (_ for _ in ()).throw(RuntimeError("x")))
    v = sheriff.sheriff_check(clean_sheriff_payload, default_config)
    assert v.outcome == Outcome.BLOCK


def test_sheriff_timeout(monkeypatch, clean_sheriff_payload, default_config):
    seq = iter([1, 100_000_000])
    monkeypatch.setattr(sheriff.time, "monotonic_ns", lambda: next(seq))
    assert sheriff.sheriff_check(clean_sheriff_payload, default_config).outcome == Outcome.BLOCK


def test_judge_none_payload(default_config):
    assert judge.judge_check(None, default_config).outcome == Outcome.BLOCK


def test_judge_schema_raises(monkeypatch, clean_judge_payload, default_config):
    monkeypatch.setattr(judge, "_validate_schema", lambda *_: (_ for _ in ()).throw(RuntimeError("x")))
    assert judge.judge_check(clean_judge_payload, default_config).outcome == Outcome.BLOCK


def test_judge_json_dumps_raises(monkeypatch, clean_judge_payload, default_config):
    monkeypatch.setattr(judge.json, "dumps", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("x")))
    p = JudgePayload(**{**clean_judge_payload.__dict__, "expected_schema": None})
    assert judge.judge_check(p, default_config).outcome == Outcome.BLOCK


def test_judge_large_output(clean_judge_payload, default_config):
    p = JudgePayload(**{**clean_judge_payload.__dict__, "expected_schema": None, "output": {"x": "a" * (1_048_577)}})
    assert judge.judge_check(p, default_config).outcome == Outcome.BLOCK


def test_judge_non_serializable(clean_judge_payload, default_config):
    p = JudgePayload(**{**clean_judge_payload.__dict__, "expected_schema": None, "output": {"x": types.SimpleNamespace(a=1)}})
    assert judge.judge_check(p, default_config).outcome == Outcome.BLOCK


def test_judge_timeout(monkeypatch, clean_judge_payload, default_config):
    seq = iter([1, 100_000_000])
    monkeypatch.setattr(judge.time, "monotonic_ns", lambda: next(seq))
    p = JudgePayload(**{**clean_judge_payload.__dict__, "expected_schema": None})
    assert judge.judge_check(p, default_config).outcome == Outcome.BLOCK
