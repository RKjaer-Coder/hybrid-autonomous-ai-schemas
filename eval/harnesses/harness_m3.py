"""M3 Task Execution harness."""

from __future__ import annotations

from eval.fixtures.m3_task_execution import generate_m3_test_set
from eval.report import compute_latency_percentiles


class M3Harness:
    def run(self, backend) -> dict:
        data = generate_m3_test_set()
        e2e = self.evaluate_e2e_completions(backend, data["e2e_scenarios"])
        rec = self.evaluate_recovery(backend, data["failure_scenarios"])
        val = self.evaluate_validation_rates(backend, data["validation_outputs"])
        tel = self.evaluate_telemetry_integrity(backend, e2e["completed"])
        lat = self.evaluate_latency(backend, e2e["latencies"])
        c = data["eval_criteria"]
        status = (
            e2e["completions"] >= c["min_completions"]
            and e2e["task_types_covered"] >= c["min_task_types"]
            and rec["recovery_successes"] >= c["min_recovery_successes"]
            and val["fp_rate"] <= c["max_task_validation_fp_rate"]
            and val["fn_rate"] <= c["max_task_validation_fn_rate"]
            and lat["p95"] <= c["latency_p95_s"]
            and tel["coverage"] >= c["telemetry_coverage"]
            and tel["integrity"]
        )
        return {
            "status": "PASS" if status else "FAIL",
            "completions": e2e["completions"],
            "task_types_covered": e2e["task_types_covered"],
            "recovery_successes": rec["recovery_successes"],
            "validation_fp_rate": val["fp_rate"],
            "validation_fn_rate": val["fn_rate"],
            "latency_p50_s": lat["p50"],
            "latency_p95_s": lat["p95"],
            "telemetry_coverage": tel["coverage"],
            "telemetry_integrity": tel["integrity"],
            "details": [],
        }

    def evaluate_e2e_completions(self, backend, scenarios) -> dict:
        completed, latencies, types = [], [], set()
        for s in scenarios:
            r = backend.execute_task(s)
            if r.get("output"):
                completed.append({"scenario": s, "chain_id": r.get("chain_id", s["scenario_id"])})
            latencies.append(r["latency_ms"] / 1000.0)
            types.add(s["task_type"])
        return {"completions": len(completed), "task_types_covered": len(types), "completed": completed, "latencies": latencies}

    def evaluate_recovery(self, backend, failure_scenarios) -> dict:
        successes = 0
        for s in failure_scenarios:
            backend.inject_failure(s)
            r = backend.execute_task(s)
            successes += 1 if r.get("recovered") else 0
        return {"recovery_successes": successes}

    def evaluate_validation_rates(self, backend, outputs) -> dict:
        exp_pass = exp_fail = fp = fn = 0
        for o in outputs:
            v = backend.validate_output(o)
            if o["expected_verdict"] == "PASS":
                exp_pass += 1
                if v["verdict"] == "FAIL":
                    fp += 1
            else:
                exp_fail += 1
                if v["verdict"] == "PASS":
                    fn += 1
        return {"fp_rate": round(fp / exp_pass, 4), "fn_rate": round(fn / exp_fail, 4)}

    def evaluate_telemetry_integrity(self, backend, completed_tasks) -> dict:
        expected = logged = 0
        for c in completed_tasks:
            exp = c["scenario"]["expected_chain_length"]
            events = backend.get_step_outcomes(c["chain_id"])
            expected += exp
            logged += len(events)
        coverage = (logged / expected) if expected else 0.0
        return {"coverage": round(coverage, 4), "integrity": expected == logged}

    def evaluate_latency(self, backend, completed_tasks) -> dict:
        _ = backend
        return compute_latency_percentiles(completed_tasks)
