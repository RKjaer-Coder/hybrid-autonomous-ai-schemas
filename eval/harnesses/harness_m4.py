"""M4 Financial routing and compliance harness."""

from __future__ import annotations

from eval.fixtures.m4_financial_routing import generate_m4_test_set


class M4Harness:
    milestone = "M4"

    def run(self, backend) -> dict:
        data = generate_m4_test_set()
        evaluation = self.evaluate_scenarios(backend, data["scenarios"])
        criteria = data["evaluation_criteria"]
        status = (
            evaluation["routing_match_rate"] >= criteria["min_routing_match_rate"]
            and evaluation["ledger_persistence_rate"] >= criteria["min_ledger_persistence_rate"]
            and evaluation["g3_enforcement_rate"] >= criteria["min_g3_enforcement_rate"]
            and evaluation["routing_path_coverage"] >= criteria["min_routing_path_coverage"]
            and evaluation["false_autonomous_spend"] <= criteria["max_false_autonomous_spend"]
        )
        return {
            "status": "PASS" if status else "FAIL",
            "routing_match_rate": evaluation["routing_match_rate"],
            "ledger_persistence_rate": evaluation["ledger_persistence_rate"],
            "g3_enforcement_rate": evaluation["g3_enforcement_rate"],
            "routing_path_coverage": evaluation["routing_path_coverage"],
            "false_autonomous_spend": evaluation["false_autonomous_spend"],
            "details": evaluation["details"],
        }

    def evaluate_scenarios(self, backend, scenarios: list[dict]) -> dict:
        results = []
        for scenario in scenarios:
            actual = backend.route_financial_task(scenario)
            decision = actual["decision"]
            ledger_row = actual["ledger_row"]
            expected = scenario["expected"]
            actual_path = self._path_label(decision, ledger_row)
            decision_match = (
                decision["tier"] == expected["tier"]
                and decision["model_id"] == expected["model_id"]
                and decision["g3_path"] == expected["g3_path"]
                and decision["requires_operator_approval"] == expected["requires_operator_approval"]
                and decision["quality_warning"] == expected["quality_warning"]
                and decision["compute_starved"] == expected["compute_starved"]
                and bool(decision["reservation_id"]) == expected["reservation_required"]
                and actual_path == expected["path_label"]
            )
            ledger_match = self._ledger_matches(ledger_row, expected)
            g3_enforced = self._g3_enforced(scenario, decision, ledger_row)
            false_autonomous = bool(
                scenario.get("autonomous_paid_forbidden")
                and actual_path == "paid_cloud_within_budget"
            )
            results.append(
                {
                    "scenario_id": scenario["scenario_id"],
                    "decision_match": decision_match,
                    "ledger_match": ledger_match,
                    "g3_enforced": g3_enforced,
                    "false_autonomous_spend": false_autonomous,
                    "path_label": actual_path,
                }
            )

        total = len(results)
        ledger_ok = sum(1 for r in results if r["ledger_match"])
        routing_ok = sum(1 for r in results if r["decision_match"])
        g3_sensitive = [r for r, s in zip(results, scenarios) if s.get("g3_enforcement")]
        g3_ok = sum(1 for r in g3_sensitive if r["g3_enforced"])
        details = [
            r["scenario_id"]
            for r in results
            if not r["decision_match"] or not r["ledger_match"] or not r["g3_enforced"] or r["false_autonomous_spend"]
        ]
        return {
            "routing_match_rate": round(routing_ok / total, 4) if total else 0.0,
            "ledger_persistence_rate": round(ledger_ok / total, 4) if total else 0.0,
            "g3_enforcement_rate": round(g3_ok / len(g3_sensitive), 4) if g3_sensitive else 1.0,
            "routing_path_coverage": len({r["path_label"] for r in results}),
            "false_autonomous_spend": sum(1 for r in results if r["false_autonomous_spend"]),
            "details": details,
        }

    def _ledger_matches(self, row: dict | None, expected: dict) -> bool:
        if row is None:
            return False
        ledger = expected["ledger"]
        return (
            row["route_selected"] == ledger["route_selected"]
            and row["model_used"] == expected["model_id"]
            and int(row["g3_required"]) == ledger["g3_required"]
            and row["g3_status"] == ledger["g3_status"]
            and int(row["quality_warning"]) == ledger["quality_warning"]
            and bool(row["reservation_id"]) == expected["reservation_required"]
        )

    def _g3_enforced(self, scenario: dict, decision: dict, row: dict | None) -> bool:
        mode = scenario.get("g3_enforcement")
        if mode is None:
            return True
        if mode == "pending_approval":
            return bool(
                row is not None
                and decision["tier"] == "paid_cloud"
                and decision["requires_operator_approval"]
                and row["g3_required"] == 1
                and row["g3_status"] == "PENDING"
            )
        if mode == "non_paid_block":
            return decision["tier"] != "paid_cloud"
        raise ValueError(f"Unknown g3 enforcement mode: {mode}")

    def _path_label(self, decision: dict, ledger_row: dict | None) -> str:
        if decision["tier"] != "paid_cloud":
            return decision["tier"]
        pending = decision["requires_operator_approval"]
        if ledger_row is not None:
            pending = pending or bool(ledger_row["g3_required"])
        return "paid_cloud_pending_g3" if pending else "paid_cloud_within_budget"
