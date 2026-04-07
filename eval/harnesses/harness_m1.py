"""M1 Immune System harness."""

from __future__ import annotations

from eval.fixtures.m1_immune_system import generate_m1_test_set
from eval.report import compute_latency_percentiles


class M1Harness:
    def run(self, backend) -> dict:
        data = generate_m1_test_set()
        clean = self.evaluate_false_positive_rate(backend, data["clean_tasks"])
        bad = self.evaluate_false_negative_rate(backend, data["known_bad_inputs"])
        latency = self.evaluate_latency(backend, data["clean_tasks"] + data["known_bad_inputs"])
        fail_closed = self.evaluate_fail_closed(backend, data["unavailability_test"])
        c = data["eval_criteria"]
        status = (
            clean <= c["max_false_positive_rate"]
            and bad <= c["max_false_negative_rate"]
            and latency["p95"] <= c["latency_p95_ms"]
            and fail_closed
        )
        return {
            "status": "PASS" if status else "FAIL",
            "false_positive_rate": clean,
            "false_negative_rate": bad,
            "latency_p50_ms": latency["p50"],
            "latency_p95_ms": latency["p95"],
            "latency_p99_ms": latency["p99"],
            "fail_closed_verified": fail_closed,
            "details": [],
        }

    def evaluate_false_positive_rate(self, backend, clean_tasks) -> float:
        blocked = sum(1 for t in clean_tasks if backend.sheriff_check(t)["verdict"] == "BLOCK")
        return round(blocked / len(clean_tasks), 4)

    def evaluate_false_negative_rate(self, backend, bad_inputs) -> float:
        passed = sum(1 for t in bad_inputs if backend.sheriff_check(t)["verdict"] == "PASS")
        return round(passed / len(bad_inputs), 4)

    def evaluate_latency(self, backend, all_tasks) -> dict:
        lats = [backend.sheriff_check(t)["latency_ms"] for t in all_tasks]
        return compute_latency_percentiles(lats)

    def evaluate_fail_closed(self, backend, test_scenario) -> bool:
        backend.sheriff_disable()
        verdict = backend.sheriff_check(test_scenario["input_task"])
        backend.sheriff_enable()
        return verdict["verdict"] == "BLOCK" and verdict.get("alert_source") == "immune_timeout_sheriff"
