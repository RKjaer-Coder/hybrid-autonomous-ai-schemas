from __future__ import annotations

from eval.backends.immune_backend import Backend
from eval.harnesses.harness_m1 import M1Harness


def test_m1_harness_runs_against_real_immune_backend():
    result = M1Harness().run(Backend())
    assert result["fail_closed_verified"] is True
    assert result["false_negative_rate"] == 0.0
    assert result["false_positive_rate"] <= 0.05
    assert result["status"] == "PASS"
