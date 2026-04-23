# Hybrid Autonomous AI

Implementation baseline for a hybrid, local-first autonomous intelligence
system built on Hermes Agent.

The repo is pre-live: the codebase is real, tested, and already carries the
core runtime, governance, routing, council, research, operator, and replay
substrates, but live Hermes attachment on the target machine is still pending.
The launch posture is prebuilt-first: Hermes on the Mac Studio should attach
to an already-built system, validate it, and operate it rather than build the
application logic from scratch.

## Current State

- Five-database SQLite baseline with migration and verification tooling
- Financial router with typed routing, Path B / per-call G3 lifecycle, and
  disputed-cost handling
- Immune subsystem with Sheriff, Judge, bootstrap patching, audited fallback,
  and deadlock-driven runtime halt/restart control
- Council Tier 1 and Tier 2 orchestration, including multi-model deliberation,
  G3-gated paid Tier 2 flow, degraded fallback, and operator/observability
  surfacing
- Research, strategic-memory, opportunity, operator, and observability skills
- Repo-local Hermes contract harness, deterministic operator workflow proof,
  and readiness/doctor flows
- Standalone local forward proxy with generated allowlist artifact, audit log,
  launcher, and repo-local `5 allow / 5 deny` self-test
- Replayable `execution_traces` across runtime, council, research,
  opportunity, and strategic-memory routing/writeback paths
- Production evidence factory for repeated multi-skill corpus growth plus
  detailed replay-readiness reporting
- Mac Studio day-one handoff bundle that composes bootstrap proofs, evidence
  growth, and cutover commands into one generated package
- §8.3b harness-variant substrate with replay-readiness guardrails and
  explicit operator acknowledgement for below-threshold shadow replay
- CI on `main`, `codex/**`, and PRs to `main`

## What This Repo Is Not Yet

- A verified live Hermes deployment
- Proof of end-to-end autonomy on the real Mac Studio environment
- The full target-state architecture running live without operator gating

## Repository Layout

```text
schemas/             SQLite contracts for the five-database baseline
migrate.py           Applies schemas and verifies structural drift
financial_router/    Routing policy, spend controls, approval logic
immune/              Fail-closed security and validation layer
council/             Deliberation types, prompts, validators, orchestration
skills/              Hermes-facing skills and runtime integration
eval/                Milestone harnesses, fixtures, reporting
tests/               Unit and integration coverage
.github/workflows/   CI
```

## The Five Databases

- `strategic_memory.db`: briefs, opportunities, research tasks, council outputs
- `telemetry.db`: step outcomes, execution traces, harness variants, replay data
- `immune_system.db`: immune verdicts, alerts, breakers, quarantine/fallback audit
- `financial_ledger.db`: routing decisions, costs, revenue, projects, phase state
- `operator_digest.db`: digests, alerts, gates, harvest requests, runtime control

## Quick Start

Install dev dependencies:

```bash
python3 -m pip install -r requirements-dev.txt
```

Verify the schema baseline:

```bash
python3 migrate.py --db-dir ./data --verify
```

Run the full test suite:

```bash
python3 -m pytest -q
```

Run the deterministic runtime proof:

```bash
python3 -m skills.runtime --operator-workflow
```

Run the standalone proxy validation:

```bash
python3 -m skills.runtime --proxy-self-test
```

Grow the replay corpus with production scenarios:

```bash
python3 -m skills.runtime --evidence-factory
```

Run a bounded replay-growth pass that stops early if the activation threshold
is reached:

```bash
python3 -m skills.runtime --evidence-factory --until-replay-ready --evidence-cycles 5
```

Print the detailed replay-readiness coverage report:

```bash
python3 -m skills.runtime --replay-readiness-report
```

Run the repo-local Hermes contract harness:

```bash
python3 -m skills.runtime --contract-harness
```

Check live-Hermes readiness on a machine with Hermes installed:

```bash
python3 -m skills.runtime --readiness
```

## Useful Commands

```bash
python3 -m eval.runner --milestone M1
python3 -m eval.runner --milestone M4
python3 -m eval.runner --milestone M5
python3 -m skills.runtime --doctor
python3 -m skills.runtime --proxy-self-test
python3 -m skills.runtime --evidence-factory --until-replay-ready --evidence-cycles 5
python3 -m skills.runtime --bootstrap-live
python3 -m skills.runtime --mac-studio-day-one
```

## Working Description

The best short description of this repository today is:

> A strong pre-live implementation substrate with deterministic runtime proofs,
> audited governance, and a production-quality repo baseline for Hermes
> attachment.
