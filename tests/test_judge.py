from __future__ import annotations

from immune.judge import judge_check
from immune.types import BlockReason, JudgePayload, Outcome


def test_clean_schema_pass(clean_judge_payload, default_config):
    assert judge_check(clean_judge_payload, default_config).outcome == Outcome.PASS


def test_schema_violation(clean_judge_payload, default_config):
    payload = JudgePayload(**{**clean_judge_payload.__dict__, "output": {"claimed_trust_tier": 4}})
    assert judge_check(payload, default_config).block_reason == BlockReason.SCHEMA_VIOLATION


def test_trust_tier_pass(clean_judge_payload, default_config):
    payload = JudgePayload(**{**clean_judge_payload.__dict__, "output": {"ok": True, "claimed_trust_tier": 2}})
    assert judge_check(payload, default_config).outcome == Outcome.PASS


def test_trust_tier_fail(clean_judge_payload, default_config):
    payload = JudgePayload(
        **{**clean_judge_payload.__dict__, "max_trust_tier": 2, "output": {"ok": True, "claimed_trust_tier": 3}}
    )
    assert judge_check(payload, default_config).block_reason == BlockReason.TRUST_TIER_VIOLATION


def test_api_key_block(clean_judge_payload, default_config):
    payload = JudgePayload(**{**clean_judge_payload.__dict__, "output": {"ok": True, "secret": "sk-proj-abc"}})
    assert judge_check(payload, default_config).block_reason == BlockReason.CONTENT_SAFETY


def test_private_key_block(clean_judge_payload, default_config):
    payload = JudgePayload(**{**clean_judge_payload.__dict__, "output": {"ok": True, "pem": "-----BEGIN RSA PRIVATE KEY-----"}})
    assert judge_check(payload, default_config).block_reason == BlockReason.CONTENT_SAFETY


def test_exact_1mb_pass(clean_judge_payload, default_config):
    text = "a" * (1_048_576 - 9)
    payload = JudgePayload(**{**clean_judge_payload.__dict__, "expected_schema": None, "output": {"d": text}})
    cfg = default_config.__class__(**{**default_config.__dict__, "judge_timeout_ms": 500.0})
    assert judge_check(payload, cfg).outcome == Outcome.PASS


def test_over_1mb_block(clean_judge_payload, default_config):
    text = "a" * (1_048_576 + 10)
    payload = JudgePayload(**{**clean_judge_payload.__dict__, "expected_schema": None, "output": {"d": text}})
    assert judge_check(payload, default_config).block_reason == BlockReason.POLICY_VIOLATION


def test_null_schema_skipped(clean_judge_payload, default_config):
    payload = JudgePayload(**{**clean_judge_payload.__dict__, "expected_schema": None})
    assert judge_check(payload, default_config).outcome == Outcome.PASS
