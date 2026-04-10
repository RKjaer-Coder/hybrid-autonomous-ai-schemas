from __future__ import annotations

import asyncio

import pytest

from immune.deep_scan import DeepScanResult, MockDeepScan
from immune.sheriff import trigger_deep_scan


def test_mock_returns_configured():
    model = MockDeepScan(default_result=DeepScanResult(True, 0.9, "ipi"))
    result = asyncio.run(model.classify("x", {}))
    assert result.threat_detected


def test_mock_raises():
    model = MockDeepScan(raise_on_call=True)
    with pytest.raises(RuntimeError):
        asyncio.run(model.classify("x", {}))


def test_trigger_timeout_returns_none(clean_sheriff_payload, default_config):
    model = MockDeepScan(delay_ms=1000)
    cfg = default_config.__class__(**{**default_config.__dict__, "deep_scan_timeout_ms": 1})
    assert asyncio.run(trigger_deep_scan(clean_sheriff_payload, cfg, model)) is None


def test_trigger_threat_block(clean_sheriff_payload, default_config):
    model = MockDeepScan(default_result=DeepScanResult(True, 0.91, "ipi"))
    verdict = asyncio.run(trigger_deep_scan(clean_sheriff_payload, default_config, model))
    assert verdict and verdict.outcome.value == "BLOCK"


def test_trigger_no_threat_pass(clean_sheriff_payload, default_config):
    model = MockDeepScan(default_result=DeepScanResult(False, 0.1, ""))
    verdict = asyncio.run(trigger_deep_scan(clean_sheriff_payload, default_config, model))
    assert verdict and verdict.outcome.value == "PASS"


def test_trigger_exception_returns_none(clean_sheriff_payload, default_config):
    model = MockDeepScan(raise_on_call=True)
    assert asyncio.run(trigger_deep_scan(clean_sheriff_payload, default_config, model)) is None
