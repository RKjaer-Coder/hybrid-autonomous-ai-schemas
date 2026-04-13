"""Kill recommender calibration harness."""

from __future__ import annotations

from eval.fixtures.kill_recommender import generate_calibration_set


class KillHarness:
    def run(self, backend) -> dict:
        projects = generate_calibration_set()
        s = self.evaluate_signal_accuracy(backend, projects)
        t = self.evaluate_timing(backend, projects)
        f = self.evaluate_false_rates(backend, projects)
        status = s["signal_accuracy"] >= 0.8 and f["kill_fp_rate"] <= 0.2 and f["kill_fn_rate"] <= 0.2
        return {"status": "PASS" if status else "FAIL", **s, **t, **f, "details": []}

    def evaluate_signal_accuracy(self, backend, projects) -> dict:
        ok = 0
        for p in projects:
            calc = backend.compute_kill_score(p)
            ok += 1 if abs(calc["kill_score"] - p["kill_score"]) < 1e-6 else 0
        return {"signal_accuracy": round(ok / len(projects), 4)}

    def evaluate_timing(self, backend, projects) -> dict:
        correct = 0
        for p in projects:
            _ = backend
            optimal_week = p["ground_truth"]["optimal_kill_week"]
            recommended_week = p.get("recommended_kill_week")
            tolerance = p["ground_truth"].get("timing_tolerance_weeks", 2)
            if optimal_week is None:
                correct += 1
            elif recommended_week is not None and abs(recommended_week - optimal_week) <= tolerance:
                correct += 1
        return {"timing_accuracy": round(correct / len(projects), 4)}

    def evaluate_false_rates(self, backend, projects) -> dict:
        fp = fn = pos = neg = 0
        for p in projects:
            r = backend.recommend_kill(p)["recommendation"]
            should = p["ground_truth"]["should_have_killed"]
            if should:
                pos += 1
                fn += 1 if r != "KILL" else 0
            else:
                neg += 1
                fp += 1 if r == "KILL" else 0
        fp_rate = round(fp / neg, 4) if neg else 0.0
        fn_rate = round(fn / pos, 4) if pos else 0.0
        return {"kill_fp_rate": fp_rate, "kill_fn_rate": fn_rate}
