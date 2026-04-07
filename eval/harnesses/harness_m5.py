"""M5 Council + Research harness."""

from __future__ import annotations

from eval.fixtures.m5_council_calibration import generate_m5_test_set
from eval.report import compute_latency_percentiles


class M5Harness:
    def run(self, backend) -> dict:
        data = generate_m5_test_set()
        c = self.evaluate_council_verdicts(backend, data["test_opportunities"])
        r = self.evaluate_research_loops(backend, data["research_scenarios"])
        b = self.evaluate_brief_quality(backend, r["briefs"])
        t = data["eval_criteria"]
        status = (
            c["fp_rate"] <= t["max_opportunity_fp_rate"]
            and c["fn_rate"] <= t["max_opportunity_fn_rate"]
            and c["da_quality_mean"] >= t["min_da_quality_score"]
            and r["complete"] >= t["min_research_loops_complete"]
            and r["p95_min"] <= t["research_latency_p95_min"]
            and b["sufficient"] >= t["min_brief_quality_sufficient"]
        )
        return {
            "status": "PASS" if status else "FAIL",
            "opportunity_fp_rate": c["fp_rate"],
            "opportunity_fn_rate": c["fn_rate"],
            "da_quality_mean": c["da_quality_mean"],
            "research_loops_complete": r["complete"],
            "research_latency_p95_min": r["p95_min"],
            "brief_quality_sufficient_count": b["sufficient"],
            "details": [],
        }

    def evaluate_council_verdicts(self, backend, opportunities) -> dict:
        fp = fn = bad = good = 0
        da_scores = []
        for o in opportunities:
            v = backend.council_deliberate(o)
            label = o["ground_truth"]["label"]
            if label == "BAD":
                bad += 1
                if v["recommendation"] == "PURSUE":
                    fp += 1
            if label == "GOOD":
                good += 1
                if v["recommendation"] == "REJECT":
                    fn += 1
            da_scores.append(v.get("da_quality_score", 0.0))
        return {"fp_rate": round(fp / bad, 4), "fn_rate": round(fn / good, 4), "da_quality_mean": round(sum(da_scores) / len(da_scores), 4)}

    def evaluate_research_loops(self, backend, scenarios) -> dict:
        lat, briefs = [], []
        for s in scenarios:
            b = backend.research_loop(s["domain"], s["seed_query"])
            briefs.append(b)
            lat.append(b.get("latency_minutes", 0.0))
        pct = compute_latency_percentiles(lat)
        return {"complete": len(briefs), "p95_min": pct["p95"], "briefs": briefs}

    def evaluate_brief_quality(self, backend, briefs) -> dict:
        sufficient = sum(1 for b in briefs if backend.rate_brief_quality(b)["rating"] == "sufficient")
        return {"sufficient": sufficient}
