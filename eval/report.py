"""Report helpers for eval harness results."""

from __future__ import annotations

import json
from math import floor


def compute_latency_percentiles(latencies: list[float]) -> dict:
    if not latencies:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0}
    xs = sorted(float(x) for x in latencies)

    def pct(p: float) -> float:
        if len(xs) == 1:
            return xs[0]
        k = (len(xs) - 1) * p
        lo, hi = floor(k), min(floor(k) + 1, len(xs) - 1)
        return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)

    return {"p50": round(pct(0.50), 3), "p95": round(pct(0.95), 3), "p99": round(pct(0.99), 3)}


def compute_false_rates(results: list[dict], expected_key: str, actual_key: str) -> dict:
    exp_pass = [r for r in results if r.get(expected_key) == "PASS"]
    exp_fail = [r for r in results if r.get(expected_key) == "FAIL"]
    fp = sum(1 for r in exp_pass if r.get(actual_key) == "FAIL") / len(exp_pass) if exp_pass else 0.0
    fn = sum(1 for r in exp_fail if r.get(actual_key) == "PASS") / len(exp_fail) if exp_fail else 0.0
    return {"false_positive_rate": round(fp, 4), "false_negative_rate": round(fn, 4)}


def format_report(results: dict) -> str:
    lines = [
        "═══════════════════════════════════════════════════════",
        "  EVAL REPORT — Hybrid Autonomous AI",
        f"  Run:       {results['run_id']}",
        f"  Timestamp: {results['timestamp']}",
        f"  Backend:   {results['backend']}",
        "═══════════════════════════════════════════════════════",
    ]
    for m, payload in results["milestones"].items():
        lines.append(f"  {m} — STATUS [{payload['status']}]")
        for k, v in payload.items():
            if k in {"status", "details"}:
                continue
            lines.append(f"  ├─ {k}: {v}")
        lines.append("")
    s = results["summary"]
    lines.extend([f"  SUMMARY: {s['passed']}/{s['total_milestones']} milestones passed", "═══════════════════════════════════════════════════════"])
    return "\n".join(lines)


def as_json(results: dict) -> str:
    return json.dumps(results, indent=2)
