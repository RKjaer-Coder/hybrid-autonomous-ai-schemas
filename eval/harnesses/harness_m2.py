"""M2 Memory Integrity harness."""

from __future__ import annotations

from eval.fixtures.m2_memory_integrity import generate_m2_test_set
from eval.report import compute_latency_percentiles


class M2Harness:
    def run(self, backend) -> dict:
        data = generate_m2_test_set()
        rt = self.evaluate_roundtrips(backend, data["memory_roundtrips"])
        rel = self.evaluate_relevance(backend, data["relevance_queries"])
        wal = self.evaluate_wal_recovery(backend, data["wal_recovery_nodes"])
        proxy = self.evaluate_proxy_validation(backend, data["proxy_requests"])
        latency = self.evaluate_latency(backend, rt["latencies"] + rel["latencies"])
        c = data["eval_criteria"]
        status = (
            rt["provenance_deviations"] <= c["max_provenance_deviations"]
            and wal["wal_recovery_success"]
            and rel["fp_rate"] <= c["max_relevance_false_positive_rate"]
            and rel["fn_rate"] <= c["max_relevance_false_negative_rate"]
            and latency["p95"] <= c["latency_p95_ms"]
            and proxy["allowed_success_count"] >= c["required_allowed_proxy_requests"]
            and proxy["blocked_reject_count"] >= c["required_blocked_proxy_requests"]
        )
        return {
            "status": "PASS" if status else "FAIL",
            "provenance_deviations": rt["provenance_deviations"],
            "wal_recovery_success": wal["wal_recovery_success"],
            "relevance_fp_rate": rel["fp_rate"],
            "relevance_fn_rate": rel["fn_rate"],
            "proxy_allowed_success_count": proxy["allowed_success_count"],
            "proxy_blocked_reject_count": proxy["blocked_reject_count"],
            "latency_p50_ms": latency["p50"],
            "latency_p95_ms": latency["p95"],
            "latency_p99_ms": latency["p99"],
            "details": [],
        }

    def evaluate_roundtrips(self, backend, roundtrip_fixtures) -> dict:
        deviations, lats = 0, []
        for r in roundtrip_fixtures:
            lats.append(backend.memory_write(r["write_payload"])["latency_ms"])
            resp = backend.memory_read({"query": r["read_query"]})
            lats.append(resp["latency_ms"])
            if not self._has_expected_roundtrip(resp["results"], r):
                deviations += 1
        return {"provenance_deviations": deviations, "latencies": lats}

    def _has_expected_roundtrip(self, results, fixture) -> bool:
        expected = fixture["write_payload"]
        expected_id = expected.get("node_id") or fixture.get("roundtrip_id")
        expected_provenance = expected.get("provenance_links")
        expected_trust_tier = expected.get("trust_tier")

        if not results:
            return False
        for candidate in results:
            candidate_id = candidate.get("node_id") or candidate.get("roundtrip_id") or candidate.get("id")
            if candidate_id != expected_id:
                continue

            if expected_provenance is not None and candidate.get("provenance_links") != expected_provenance:
                continue
            if expected_trust_tier is not None and candidate.get("trust_tier") != expected_trust_tier:
                continue
            return True
        return False

    def evaluate_relevance(self, backend, queries) -> dict:
        fp = fn = 0
        lat = []
        total_pos = total_neg = 0
        for q in queries:
            resp = backend.memory_read(q)
            lat.append(resp["latency_ms"])
            got = {x.get("roundtrip_id") for x in resp["results"]}
            exp = set(q["expected_match_ids"])
            non = set(q["expected_non_match_ids"])
            total_pos += len(exp)
            total_neg += len(non)
            fn += len(exp - got)
            fp += len(got & non)
        return {"fp_rate": round(fp / total_neg, 4) if total_neg else 0.0, "fn_rate": round(fn / total_pos, 4) if total_pos else 0.0, "latencies": lat}

    def evaluate_wal_recovery(self, backend, wal_nodes) -> dict:
        for n in wal_nodes:
            backend.memory_write(n["write_payload"])
        backend.memory_force_kill()
        backend.memory_reopen()
        ok = all(backend.memory_read({"query": n["node_id"]})["results"] for n in wal_nodes)
        return {"wal_recovery_success": ok}

    def evaluate_proxy_validation(self, backend, proxy_requests) -> dict:
        allowed_success = 0
        blocked_reject = 0
        for request in proxy_requests["allowed"]:
            response = backend.proxy_request(request)
            if response["status_code"] == request["expected_status"]:
                allowed_success += 1
        for request in proxy_requests["blocked"]:
            response = backend.proxy_request(request)
            if response["status_code"] == request["expected_status"]:
                blocked_reject += 1
        return {
            "allowed_success_count": allowed_success,
            "blocked_reject_count": blocked_reject,
        }

    def evaluate_latency(self, backend, all_operations) -> dict:
        _ = backend
        return compute_latency_percentiles(all_operations)
