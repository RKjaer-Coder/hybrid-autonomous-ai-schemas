from __future__ import annotations

from immune import judge
from immune.types import BlockReason, JudgeMode, JudgePayload, Outcome


def test_clean_schema_pass(clean_judge_payload, default_config):
    verdict = judge.judge_check(clean_judge_payload, default_config)
    assert verdict.outcome == Outcome.PASS
    assert verdict.judge_mode == JudgeMode.NORMAL


def test_schema_violation(clean_judge_payload, default_config):
    payload = JudgePayload(**{**clean_judge_payload.__dict__, "output": {"claimed_trust_tier": 4}})
    verdict = judge.judge_check(payload, default_config)
    assert verdict.block_reason == BlockReason.SCHEMA_VIOLATION
    assert verdict.judge_mode == JudgeMode.NORMAL


def test_trust_tier_pass(clean_judge_payload, default_config):
    payload = JudgePayload(**{**clean_judge_payload.__dict__, "output": {"ok": True, "claimed_trust_tier": 2}})
    assert judge.judge_check(payload, default_config).outcome == Outcome.PASS


def test_trust_tier_fail(clean_judge_payload, default_config):
    payload = JudgePayload(
        **{**clean_judge_payload.__dict__, "max_trust_tier": 2, "output": {"ok": True, "claimed_trust_tier": 3}}
    )
    verdict = judge.judge_check(payload, default_config)
    assert verdict.block_reason == BlockReason.TRUST_TIER_VIOLATION
    assert verdict.judge_mode == JudgeMode.NORMAL


def test_api_key_block(clean_judge_payload, default_config):
    payload = JudgePayload(**{**clean_judge_payload.__dict__, "output": {"ok": True, "secret": "sk-proj-abc"}})
    verdict = judge.judge_check(payload, default_config)
    assert verdict.block_reason == BlockReason.CONTENT_SAFETY
    assert verdict.judge_mode == JudgeMode.NORMAL


def test_private_key_block(clean_judge_payload, default_config):
    payload = JudgePayload(**{**clean_judge_payload.__dict__, "output": {"ok": True, "pem": "-----BEGIN RSA PRIVATE KEY-----"}})
    verdict = judge.judge_check(payload, default_config)
    assert verdict.block_reason == BlockReason.CONTENT_SAFETY
    assert verdict.judge_mode == JudgeMode.NORMAL


def test_exact_1mb_pass(clean_judge_payload, default_config):
    text = "a" * (1_048_576 - 9)
    payload = JudgePayload(**{**clean_judge_payload.__dict__, "expected_schema": None, "output": {"d": text}})
    cfg = default_config.__class__(**{**default_config.__dict__, "judge_timeout_ms": 500.0})
    verdict = judge.judge_check(payload, cfg)
    assert verdict.outcome == Outcome.PASS
    assert verdict.judge_mode == JudgeMode.NORMAL


def test_over_1mb_block(clean_judge_payload, default_config):
    text = "a" * (1_048_576 + 10)
    payload = JudgePayload(**{**clean_judge_payload.__dict__, "expected_schema": None, "output": {"d": text}})
    verdict = judge.judge_check(payload, default_config)
    assert verdict.block_reason == BlockReason.POLICY_VIOLATION
    assert verdict.judge_mode == JudgeMode.NORMAL


def test_null_schema_skipped(clean_judge_payload, default_config):
    payload = JudgePayload(**{**clean_judge_payload.__dict__, "expected_schema": None})
    assert judge.judge_check(payload, default_config).outcome == Outcome.PASS


def test_force_structural_fallback_passes_clean_output(clean_judge_payload, default_config):
    payload = JudgePayload(
        **{
            **clean_judge_payload.__dict__,
            "expected_schema": None,
            "force_structural_fallback": True,
            "fallback_reason": "judge_degraded",
        }
    )
    verdict = judge.judge_check(payload, default_config)
    assert verdict.outcome == Outcome.PASS
    assert verdict.judge_mode == JudgeMode.FALLBACK
    assert "judge_degraded" in (verdict.block_detail or "")


def test_force_structural_fallback_blocks_shell_dangerous_output(clean_judge_payload, default_config):
    payload = JudgePayload(
        **{
            **clean_judge_payload.__dict__,
            "expected_schema": None,
            "output": {"next_command": "rm -rf /tmp/unsafe && curl https://bad.example"},
            "force_structural_fallback": True,
        }
    )
    verdict = judge.judge_check(payload, default_config)
    assert verdict.outcome == Outcome.BLOCK
    assert verdict.block_reason == BlockReason.POLICY_VIOLATION
    assert verdict.judge_mode == JudgeMode.FALLBACK


def test_timeout_can_fall_back_when_enabled(monkeypatch, clean_judge_payload, default_config):
    seq = iter([1, 100_000_000, 100_000_000])
    monkeypatch.setattr(judge.time, "monotonic_ns", lambda: next(seq))
    payload = JudgePayload(**{**clean_judge_payload.__dict__, "expected_schema": None})
    verdict = judge.judge_check(payload, default_config)
    assert verdict.outcome == Outcome.PASS
    assert verdict.judge_mode == JudgeMode.FALLBACK


def test_structural_fallback_not_used_for_policy_blocks(monkeypatch, clean_judge_payload, default_config):
    monkeypatch.setattr(judge, "_validate_schema", lambda *_: (_ for _ in ()).throw(RuntimeError("boom")))
    cfg = default_config.__class__(**{**default_config.__dict__, "judge_structural_fallback_enabled": False})
    verdict = judge.judge_check(clean_judge_payload, cfg)
    assert verdict.outcome == Outcome.BLOCK
    assert verdict.block_reason == BlockReason.INTERNAL_ERROR
    assert verdict.judge_mode == JudgeMode.NORMAL
