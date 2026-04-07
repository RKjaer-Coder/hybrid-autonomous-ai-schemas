"""Unified eval runner for Hybrid Autonomous AI milestones."""

from __future__ import annotations

import argparse
import importlib
import json
import signal
import sys
import time
from abc import ABC, abstractmethod

import pathlib

if __package__ in {None, ""}:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))


from eval.fixtures.common import DeterministicFactory, weighted_sum
from eval.harnesses.harness_kill import KillHarness
from eval.harnesses.harness_m1 import M1Harness
from eval.harnesses.harness_m2 import M2Harness
from eval.harnesses.harness_m3 import M3Harness
from eval.harnesses.harness_m5 import M5Harness
from eval.report import format_report


class EvalBackend(ABC):
    @abstractmethod
    def sheriff_check(self, task: dict) -> dict: ...
    @abstractmethod
    def sheriff_disable(self) -> None: ...
    @abstractmethod
    def sheriff_enable(self) -> None: ...
    @abstractmethod
    def memory_write(self, node: dict) -> dict: ...
    @abstractmethod
    def memory_read(self, query: dict) -> dict: ...
    @abstractmethod
    def memory_force_kill(self) -> None: ...
    @abstractmethod
    def memory_reopen(self) -> None: ...
    @abstractmethod
    def execute_task(self, scenario: dict) -> dict: ...
    @abstractmethod
    def inject_failure(self, scenario: dict) -> None: ...
    @abstractmethod
    def validate_output(self, output: dict) -> dict: ...
    @abstractmethod
    def get_step_outcomes(self, chain_id: str) -> list: ...
    @abstractmethod
    def council_deliberate(self, opportunity: dict) -> dict: ...
    @abstractmethod
    def research_loop(self, domain: int, query: str) -> dict: ...
    @abstractmethod
    def rate_brief_quality(self, brief: dict) -> dict: ...
    @abstractmethod
    def compute_kill_score(self, project: dict) -> dict: ...
    @abstractmethod
    def recommend_kill(self, project: dict) -> dict: ...


class MockBackend(EvalBackend):
    """Deterministic backend that mirrors fixture expected outcomes."""

    def __init__(self) -> None:
        self.disabled = False
        self.memory: dict[str, dict] = {}
        self.step_outcomes: dict[str, list] = {}
        self.failure_cfg: dict | None = None

    def sheriff_check(self, task: dict) -> dict:
        if self.disabled:
            return {"verdict": "BLOCK", "scan_tier": "fast_path", "latency_ms": 10, "alert_source": "immune_timeout_sheriff"}
        return {"verdict": task.get("expected_verdict", "PASS"), "scan_tier": task.get("expected_scan_tier", "fast_path"), "latency_ms": 40}

    def sheriff_disable(self) -> None:
        self.disabled = True

    def sheriff_enable(self) -> None:
        self.disabled = False

    def memory_write(self, node: dict) -> dict:
        key = node.get("node_id") or node.get("brief_id") or node.get("id") or str(len(self.memory))
        self.memory[key] = node
        return {"success": True, "latency_ms": 80}

    def memory_read(self, query: dict) -> dict:
        if "expected_match_ids" in query:
            results = [{"roundtrip_id": i} for i in query["expected_match_ids"]]
            return {"results": results, "latency_ms": 120}
        q = query.get("query", "")
        results = [v for k, v in self.memory.items() if q in {k, v.get("node_id"), v.get("brief_id")} or q in str(v)]
        return {"results": results[:1] if results else [{"roundtrip_id": q}] if q else [], "latency_ms": 90}

    def memory_force_kill(self) -> None:
        return None

    def memory_reopen(self) -> None:
        return None

    def execute_task(self, scenario: dict) -> dict:
        chain_id = scenario["scenario_id"]
        steps = [{"outcome": "PASS"} for _ in range(scenario["expected_chain_length"])]
        recovered = True
        if self.failure_cfg and self.failure_cfg["scenario_id"] == scenario["scenario_id"]:
            fails = self.failure_cfg["force_failure"]["failure_count"]
            steps.extend({"outcome": "FAIL"} for _ in range(fails))
        self.step_outcomes[chain_id] = steps
        return {"steps": steps, "output": {"ok": True}, "latency_ms": 30_000, "telemetry": steps, "chain_id": chain_id, "recovered": recovered}

    def inject_failure(self, scenario: dict) -> None:
        self.failure_cfg = scenario

    def validate_output(self, output: dict) -> dict:
        return {"verdict": output["expected_verdict"], "reason": output.get("failure_reason")}

    def get_step_outcomes(self, chain_id: str) -> list:
        return self.step_outcomes.get(chain_id, [])

    def council_deliberate(self, opportunity: dict) -> dict:
        gt = opportunity["ground_truth"]
        rec = gt["expected_recommendation"] if gt["expected_recommendation"] != "any" else "PAUSE"
        return {"recommendation": rec, "confidence": gt["expected_min_confidence"], "da_quality_score": 0.5, "dissenting_views": ["risk note"], "da_assessment": {"quality": "ok"}}

    def research_loop(self, domain: int, query: str) -> dict:
        _ = query
        return {"domain": domain, "summary": "brief", "source_urls": ["https://example.com"], "confidence": 0.7, "actionability": "WATCH", "latency_minutes": 12}

    def rate_brief_quality(self, brief: dict) -> dict:
        _ = brief
        return {"rating": "sufficient"}

    def compute_kill_score(self, project: dict) -> dict:
        return {"kill_score": weighted_sum(project["kill_signals"]), "signals": project["kill_signals"]}

    def recommend_kill(self, project: dict) -> dict:
        return {"recommendation": "KILL" if project["ground_truth"]["should_have_killed"] else "CONTINUE", "confidence": 0.8}


def _load_backend(path: str) -> EvalBackend:
    if path == "mock":
        return MockBackend()
    module = importlib.import_module(path)
    backend_cls = getattr(module, "Backend")
    return backend_cls()


def run_all(backend: EvalBackend, milestones: list[str], on_milestone_complete=None) -> dict:
    rf = DeterministicFactory(99)
    harnesses = {"M1": M1Harness(), "M2": M2Harness(), "M3": M3Harness(), "M5": M5Harness(), "KILL": KillHarness()}
    results = {}
    for m in milestones:
        results[m] = harnesses[m].run(backend)
        if on_milestone_complete:
            on_milestone_complete(m, results[m])
    passed = sum(1 for v in results.values() if v["status"] == "PASS")
    return {
        "run_id": rf.uuid_v7(),
        "timestamp": rf.now(),
        "backend": backend.__class__.__name__,
        "milestones": results,
        "summary": {
            "total_milestones": len(milestones),
            "passed": passed,
            "failed": len(milestones) - passed,
            "overall_status": "PASS" if passed == len(milestones) else "FAIL",
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", default="mock")
    parser.add_argument("--milestone", default="ALL", choices=["M1", "M2", "M3", "M5", "KILL", "ALL"])
    parser.add_argument("--output")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--timeout", type=int, default=600)
    args = parser.parse_args(argv)

    milestones = ["M1", "M2", "M3", "M5", "KILL"] if args.milestone == "ALL" else [args.milestone]
    backend = _load_backend(args.backend)
    partial: dict = {"milestones": {}, "summary": {"total_milestones": len(milestones), "passed": 0, "failed": 0}}
    run_started = time.monotonic()

    def handler(sig, frame):
        _ = (sig, frame)
        print("Interrupted. Partial results:")
        print(json.dumps(partial, indent=2))
        raise SystemExit(130)

    def timeout_handler(sig, frame):
        _ = (sig, frame)
        elapsed_s = round(time.monotonic() - run_started, 2)
        partial["timeout_seconds"] = args.timeout
        partial["elapsed_seconds"] = elapsed_s
        partial["status"] = "TIMEOUT"
        print(f"Timed out after {args.timeout}s. Partial results:")
        print(json.dumps(partial, indent=2))
        raise SystemExit(124)

    def record_partial(milestone: str, result: dict) -> None:
        partial["milestones"][milestone] = result
        passed = sum(1 for m in partial["milestones"].values() if m["status"] == "PASS")
        partial["summary"]["passed"] = passed
        partial["summary"]["failed"] = len(partial["milestones"]) - passed

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(args.timeout)
    report = run_all(backend, milestones, on_milestone_complete=record_partial)
    signal.alarm(0)
    partial.update(report)
    output = json.dumps(report, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(output)
    else:
        print(output)
    if args.verbose:
        print(format_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
