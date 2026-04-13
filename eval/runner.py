"""Unified eval runner for Hybrid Autonomous AI milestones."""

from __future__ import annotations

import argparse
import datetime
import importlib
import json
import logging
import multiprocessing as mp
import queue
import signal
import sys
import time
import uuid
from abc import ABC, abstractmethod

import pathlib

if __package__ in {None, ""}:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))


from eval.fixtures.common import weighted_sum
from eval.harnesses.harness_kill import KillHarness
from eval.harnesses.harness_m1 import M1Harness
from eval.harnesses.harness_m2 import M2Harness
from eval.harnesses.harness_m3 import M3Harness
from eval.harnesses.harness_m4 import M4Harness
from eval.harnesses.harness_m5 import M5Harness
from eval.report import format_report

ALLOWED_MILESTONES = ("M1", "M2", "M3", "M4", "M5", "KILL")
LOGGER = logging.getLogger(__name__)


class MetricsSink:
    """Minimal metrics hook interface for operability integrations."""

    def incr(self, name: str, value: int = 1, tags: dict | None = None) -> None:  # noqa: ARG002
        return None

    def observe(self, name: str, value: float, tags: dict | None = None) -> None:  # noqa: ARG002
        return None


METRICS = MetricsSink()


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
        if not q:
            return {"results": [], "latency_ms": 90}

        # Prefer exact identity lookups first for deterministic roundtrip tests.
        direct = self.memory.get(q)
        if direct is not None:
            return {"results": [direct], "latency_ms": 90}

        # Support semantic-style roundtrip query strings used in fixtures.
        normalized = q.lower()
        title_hint = q.split("about ", 1)[1].strip() if "about " in normalized else ""

        matches = []
        for key, value in self.memory.items():
            node_id = value.get("node_id")
            brief_id = value.get("brief_id")
            title = str(value.get("title", ""))
            if q in {key, node_id, brief_id}:
                matches.append(value)
                continue
            if title_hint and title_hint == title:
                matches.append(value)
                continue
        return {"results": matches[:1], "latency_ms": 90}

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
            # Key fix: recovery should reflect scenario contract, not a hardcoded True.
            if self.failure_cfg and "force_failure" in self.failure_cfg:
                recovered = bool(self.failure_cfg["force_failure"].get("expected_recovery_success", False))
            else:
                recovered = True
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
    backend = backend_cls()
    if not isinstance(backend, EvalBackend):
        required = [
            "sheriff_check", "sheriff_disable", "sheriff_enable", "memory_write", "memory_read",
            "memory_force_kill", "memory_reopen", "execute_task", "inject_failure", "validate_output",
            "get_step_outcomes", "council_deliberate", "research_loop", "rate_brief_quality",
            "compute_kill_score", "recommend_kill",
        ]
        if not all(callable(getattr(backend, name, None)) for name in required):
            raise TypeError(f"{path}.Backend must implement EvalBackend")
    return backend


def _normalize_milestones(milestones: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for m in milestones:
        if m not in ALLOWED_MILESTONES:
            raise ValueError(f"Unknown milestone: {m}")
        if m not in seen:
            normalized.append(m)
            seen.add(m)
    return normalized


def _run_with_timeout(fn, timeout_s: float | None):
    """Execute fn without timeout (kept for non-isolated fallback path)."""
    if not timeout_s:
        return fn()
    # Portable note: true preemptive timeout is provided via subprocess isolation path.
    return fn()


def _run_milestone_worker(backend_path: str, milestone: str, output_queue: mp.Queue) -> None:
    try:
        backend = _load_backend(backend_path)
        harnesses = {"M1": M1Harness(), "M2": M2Harness(), "M3": M3Harness(), "M4": M4Harness(), "M5": M5Harness(), "KILL": KillHarness()}
        result = harnesses[milestone].run(backend)
        output_queue.put(("ok", result))
    except Exception as exc:  # noqa: BLE001
        output_queue.put(("error", {"status": "FAIL", "error_type": exc.__class__.__name__, "error_message": str(exc), "details": []}))


def run_all(
    backend: EvalBackend,
    milestones: list[str],
    on_milestone_complete=None,
    per_milestone_timeout_s: float | None = None,
    backend_path: str | None = None,
    global_timeout_s: float | None = None,
    allow_backend_reinit_under_timeout: bool = False,
) -> dict:
    if (per_milestone_timeout_s is not None or global_timeout_s is not None) and not backend_path:
        raise ValueError("timeout enforcement requires backend_path for isolated execution")
    milestones = _normalize_milestones(milestones)
    timeout_mode = per_milestone_timeout_s is not None or global_timeout_s is not None
    if timeout_mode and len(milestones) > 1 and not allow_backend_reinit_under_timeout and backend_path != "mock":
        raise ValueError(
            "timeout-isolated execution re-initializes backend per milestone; "
            "set allow_backend_reinit_under_timeout=True only for stateless/safe backends"
        )
    harnesses = {"M1": M1Harness(), "M2": M2Harness(), "M3": M3Harness(), "M4": M4Harness(), "M5": M5Harness(), "KILL": KillHarness()}
    results = {}
    run_started = time.monotonic()
    for m in milestones:
        started = time.monotonic()
        remaining_global = None
        if global_timeout_s is not None:
            remaining_global = max(0.0, global_timeout_s - (started - run_started))
            if remaining_global <= 0:
                result = {
                    "status": "FAIL",
                    "error_type": "RunTimeout",
                    "error_message": f"operation exceeded {global_timeout_s:.2f}s",
                    "details": [],
                }
                results[m] = result
                if on_milestone_complete:
                    on_milestone_complete(m, results[m])
                continue
        effective_timeout_s = per_milestone_timeout_s
        if remaining_global is not None:
            effective_timeout_s = (
                remaining_global
                if effective_timeout_s is None
                else min(effective_timeout_s, remaining_global)
            )
        try:
            if effective_timeout_s and backend_path:
                # Portable preemptive timeout via process isolation (works across OS/thread models).
                q: mp.Queue = mp.Queue()
                proc = mp.Process(target=_run_milestone_worker, args=(backend_path, m, q), daemon=True)
                proc.start()
                proc.join(timeout=effective_timeout_s)
                if proc.is_alive():
                    proc.terminate()
                    proc.join(1.0)
                    raise TimeoutError(f"operation exceeded {effective_timeout_s:.2f}s")
                try:
                    status, payload = q.get(timeout=1.0)
                except queue.Empty as exc:
                    raise RuntimeError("milestone process exited without result") from exc
                result = payload if status == "ok" else payload
            else:
                result = _run_with_timeout(lambda: harnesses[m].run(backend), effective_timeout_s)
        except TimeoutError as exc:
            elapsed = time.monotonic() - started
            LOGGER.error("milestone_timeout", extra={"milestone": m, "elapsed_seconds": round(elapsed, 3)})
            METRICS.incr("milestone_timeout_total", tags={"milestone": m})
            result = {
                "status": "FAIL",
                "error_type": "MilestoneTimeout",
                "error_message": str(exc),
                "elapsed_seconds": round(elapsed, 3),
                "details": [],
            }
        except Exception as exc:  # noqa: BLE001
            # Key fix: emit typed failure metadata instead of crashing caller pipelines.
            LOGGER.exception("milestone_failure", extra={"milestone": m})
            METRICS.incr("milestone_failure_total", tags={"milestone": m, "error_type": exc.__class__.__name__})
            result = {"status": "FAIL", "error_type": exc.__class__.__name__, "error_message": str(exc), "details": []}
        METRICS.observe("milestone_latency_seconds", time.monotonic() - started, tags={"milestone": m, "status": result.get("status", "UNKNOWN")})
        results[m] = result
        if on_milestone_complete:
            on_milestone_complete(m, results[m])
    passed = sum(1 for v in results.values() if v["status"] == "PASS")
    return {
        "run_id": str(uuid.uuid4()),
        "timestamp": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
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
    parser.add_argument("--milestone", default="ALL", choices=["M1", "M2", "M3", "M4", "M5", "KILL", "ALL"])
    parser.add_argument("--output")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--per-milestone-timeout", type=int, default=0, help="Optional timeout per milestone in seconds")
    parser.add_argument(
        "--allow-backend-reinit-under-timeout",
        action="store_true",
        help="Allow isolated timeout mode to recreate backend per milestone (safe only for stateless backends).",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)

    if args.timeout <= 0:
        raise SystemExit("--timeout must be > 0")
    if args.per_milestone_timeout < 0:
        raise SystemExit("--per-milestone-timeout must be >= 0")

    milestones = list(ALLOWED_MILESTONES) if args.milestone == "ALL" else [args.milestone]
    backend = _load_backend(args.backend)
    partial: dict = {"milestones": {}, "summary": {"total_milestones": len(milestones), "passed": 0, "failed": 0}}

    def handler(sig, frame):
        _ = (sig, frame)
        print("Interrupted. Partial results:")
        print(json.dumps(partial, indent=2))
        raise SystemExit(130)

    def record_partial(milestone: str, result: dict) -> None:
        partial["milestones"][milestone] = result
        passed = sum(1 for m in partial["milestones"].values() if m["status"] == "PASS")
        partial["summary"]["passed"] = passed
        partial["summary"]["failed"] = len(partial["milestones"]) - passed

    signal.signal(signal.SIGINT, handler)
    report = run_all(
        backend,
        milestones,
        on_milestone_complete=record_partial,
        per_milestone_timeout_s=args.per_milestone_timeout or None,
        backend_path=args.backend,
        global_timeout_s=float(args.timeout),
        allow_backend_reinit_under_timeout=args.allow_backend_reinit_under_timeout,
    )
    partial.update(report)
    output = json.dumps(report, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(output)
    else:
        print(output)
    if args.verbose:
        print(format_report(report))
    return 0 if report["summary"]["overall_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
