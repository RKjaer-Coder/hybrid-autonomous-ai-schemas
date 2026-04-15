from __future__ import annotations

import re

from immune.context_params import ContextParams, KNOWN_BAD_TRACES
from immune import sheriff
from immune.types import BlockReason, Outcome

UUID7_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7")


def test_clean_payload_pass(clean_sheriff_payload, default_config):
    verdict = sheriff.sheriff_check(clean_sheriff_payload, default_config)
    assert verdict.outcome == Outcome.PASS


def test_structural_short_circuit(monkeypatch, clean_sheriff_payload, default_config):
    monkeypatch.setattr(sheriff, "validate_structure", lambda *_: (BlockReason.STRUCTURAL_MALFORMATION, "x"))
    called = {"ipi": False}
    monkeypatch.setattr(sheriff, "classify_ipi", lambda *_: called.__setitem__("ipi", True))
    verdict = sheriff.sheriff_check(clean_sheriff_payload, default_config)
    assert verdict.outcome == Outcome.BLOCK
    assert not called["ipi"]


def test_ipi_short_circuit(monkeypatch, clean_sheriff_payload, default_config):
    monkeypatch.setattr(sheriff, "validate_structure", lambda *_: None)
    monkeypatch.setattr(sheriff, "classify_ipi", lambda *_: (BlockReason.IPI_DETECTED, "x"))
    called = {"policy": False}
    monkeypatch.setattr(sheriff, "check_policy", lambda *_: called.__setitem__("policy", True))
    verdict = sheriff.sheriff_check(clean_sheriff_payload, default_config)
    assert verdict.outcome == Outcome.BLOCK
    assert not called["policy"]


def test_timeout_block(monkeypatch, clean_sheriff_payload, default_config):
    seq = iter([1, 100_000_000])
    monkeypatch.setattr(sheriff.time, "monotonic_ns", lambda: next(seq))
    verdict = sheriff.sheriff_check(clean_sheriff_payload, default_config)
    assert verdict.block_reason == BlockReason.TIMEOUT


def test_latency_and_uuid(clean_sheriff_payload, default_config):
    verdict = sheriff.sheriff_check(clean_sheriff_payload, default_config)
    assert verdict.latency_ms >= 0
    assert UUID7_RE.search(verdict.verdict_id)


def test_explicit_context_params_used(clean_sheriff_payload, default_config):
    cfg = default_config.__class__(**{**default_config.__dict__, "context_params_enabled": True})
    verdict = sheriff.sheriff_check(
        clean_sheriff_payload,
        cfg,
        context_params=ContextParams(tool_window=("memory_read", "config_read", "web_fetch")),
    )
    assert verdict.outcome == Outcome.BLOCK
    assert "EXFIL_SEQUENCE" in (verdict.block_detail or "")


def test_trace_anomaly_flags_without_fast_path_block(clean_sheriff_payload, default_config):
    cfg = default_config.__class__(**{**default_config.__dict__, "context_params_enabled": True})
    KNOWN_BAD_TRACES.add("bad-trace")
    try:
        verdict = sheriff.sheriff_check(
            clean_sheriff_payload,
            cfg,
            context_params=ContextParams(execution_trace_hash="bad-trace"),
        )
    finally:
        KNOWN_BAD_TRACES.discard("bad-trace")
    assert verdict.outcome == Outcome.PASS
