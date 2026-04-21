# Hybrid Autonomous AI

This repository is the implementation baseline for Hybrid Autonomous AI: a
hybrid, local-first autonomous intelligence system designed to run primarily on
operator-owned infrastructure and use cloud models selectively where they add
clear leverage or safety value.

This is a real, test-backed codebase, not just an idea dump. It already
contains the persistence layer, routing contracts, immune guardrails, Hermes
integration scaffolding, milestone evals, and a deterministic runtime proof.
It is still pre-live from a real Hermes deployment perspective.

## Latest Progress

As of April 21, 2026, GitHub `main` still includes the latest validated Hermes
runtime attachment work:

- repo-owned Hermes profile generation under `~/.hermes/profiles/<profile>/`
- a docs-aligned `config.yaml` plus a spec-compat `profile.yaml` projection
- structural profile validation in both `--doctor` and `--readiness`
- an explicit `--bootstrap-live` CLI path that matches the generated launchers
- deterministic runtime proof coverage for install, bootstrap, workflow, and
  post-workflow doctor checks

On top of that baseline, the current task branch now adds compound-breaker
audit persistence and operator-facing observability:

- `immune_system.db` now has a durable `compound_breaker_events` table
- breaker precedence is resolved with the spec's S/H/D/R severity ordering
- observability can now query raw breaker trips plus unresolved compound events
- operator digests now surface unresolved compound-breaker events even in
  `critical_only` mode

This branch now also lands the next planned §6.3a governance/runtime gap:

- `immune_system.db` now persists durable `quarantined_responses` records with
  correlation IDs and operator-review fields
- the financial router skill now has an explicit
  `quarantine_inflight_paid_response` path that marks approved interrupted
  paid calls `cost_status=DISPUTED`
- disputed paid calls now persist into `cost_records` so they still count
  against project P&L under conservative accounting
- observability and operator digests now surface pending quarantine review and
  disputed spend instead of leaving those incident states implicit

This branch now also lands the next planned Judge fallback governance gap:

- `immune_system.db` audit rows now carry explicit `judge_mode` state:
  `NOT_APPLICABLE`, `NORMAL`, or `FALLBACK`
- Judge now has a minimum structural fallback path for degraded/unavailable
  validation branches; fallback stays deterministic and tags audit rows
  `judge_mode=FALLBACK` instead of silently behaving like a normal pass
- observability and operator system-health surfaces now expose recent fallback
  Judge incidents, blocked fallback counts, and digest-level incident summaries

This branch now also lands the explicit fallback/restart lifecycle around that
audited Judge fallback mode:

- `immune_system.db` now persists explicit `judge_fallback_events` and
  `judge_fallback_review_queue` state instead of treating fallback as an
  implicit runtime condition
- normal Judge audit rows now also persist explicit `task_type` metadata, so
  deadlock diversity keys off the spec's task-type contract with legacy
  fallback for older rows
- automatic `JUDGE_DEADLOCK` triggering now promotes sustained normal-Judge
  block spikes into a 30-minute audited fallback window
- the once-per-24h guard now halts into a fail-closed `FULL_SYSTEM_HALT`
  posture on retrigger instead of oscillating between bypass and deadlock
- `operator_digest.db` now persists a runtime halt/restart control plane with
  active halt state, halt history, and blocked/completed restart attempts
- deadlock-triggered `FULL_SYSTEM_HALT` now escalates into that runtime halt
  contract instead of living only inside the Judge lifecycle
- the Hermes dispatch patch now checks runtime halt state before executing the
  underlying tool, so halted runtimes fail closed before tool side effects
  happen instead of blocking only after output validation
- operator and observability surfaces now expose active deadlock state,
  fallback expiry, restart-required halt state, runtime halt/restart status,
  and retroactive review queue counts
- fallback-passed outputs are now queued for retroactive full-Judge review
  when the deadlock is cleared, so fallback passes do not disappear from later
  audit

That still does not amount to the full target-state restart story from the
spec:

- the repo now has an audited runtime halt/restart contract, but it is still
  not a directly verified Hermes-wide process stop and restart workflow on a
  live install
- none of this changes the repo's pre-live boundary because Hermes is still
  not installed and live autonomy has not been re-verified on real hardware

This branch now also lands the remaining pre-Hermes Path B / per-call G3
governance lifecycle for paid routes outside project budget:

- `financial_ledger.db` now persists durable `g3_approval_requests` records
  keyed by correlation id, with project/session/task context, requested model,
  estimated cost, justification, expiry, and explicit request status
- paid route selection is now separated from paid dispatch:
  Path B requests persist as pending approval instead of being treated as
  already spend-authorized, and even Path A paid routes remain
  `APPROVED_PENDING_DISPATCH` until the call is actually dispatched
- operator-facing actions now explicitly approve, deny, or expire those
  requests and keep `gate_log`, routing, and request state aligned
- paid dispatch now creates the conservative estimated cost record only when
  the call is actually dispatched, with idempotent reservation protection and
  explicit final-cost reconciliation into `FINAL` or `DISPUTED` states
- operator digests and observability now surface pending, approved, denied,
  and expired G3 request state instead of leaving Path B lifecycle state
  implicit in a single routing row

The highest-priority remaining step is still hardware-gated: run the readiness
flow against a real Hermes installation on the Mac Studio and confirm the live
CLI/profile surface end to end.

The highest-priority remaining non-hardware work is now to widen this replay
substrate across the remaining high-value repo surfaces, especially
strategic-memory routing and other non-runtime flows that still do not emit
first-class `execution_traces`, so the §8.3b frontier keeps moving away from
runtime-heavy evidence.

## What Is In This Repo

Today, this repository includes:

- a five-database SQLite baseline with migrations and drift verification
- a typed financial router with spend controls and hard approval boundaries
- an immune subsystem with Sheriff, Judge, bootstrap patching, verdict logs,
  compound-breaker audit persistence, explicit audited Judge fallback mode,
  and a runtime halt/restart control plane for deadlock-driven full halts
- council contracts and orchestration support for structured deliberation
- research, strategic-memory, opportunity, operator, and observability skills
- milestone eval harnesses and deterministic fixtures
- a Hermes runtime integration layer that can prepare a local profile bundle,
  generate repo-owned Hermes profile artifacts, migrate the databases,
  bootstrap a runtime, run a doctor check that validates the runtime layout
  and generated profile files, run a repo-local Hermes contract harness that
  exercises approval, dispatch, deadlock halt, blocked execution, and audited
  restart against the mock/runtime substrate, run a real-Hermes readiness
  check against the operator bootstrap checklist, and prove a deterministic
  operator workflow plus a council-backed opportunity and phase-gate path
  against a mock runtime
- a working §8.3b replay substrate with persisted `execution_traces`,
  `harness_variants`, replay-derived `VariantEvalResult`, promoted frontier
  views, replay artifact traces, and operator/observability lifecycle surfaces
- runtime workflow trace logging beyond the contract harness, so both the
  deterministic operator workflow and replay engine write durable telemetry
  evidence into `telemetry.db`
- first-class non-runtime trace capture in the research-task completion/routing,
  opportunity-transition/phase-gate, and council-deliberation paths
- explicit replay-readiness guardrails in system-health/operator surfaces, so
  the repo can distinguish "replay implemented" from "replay sufficiently
  broad to trust at activation scale"
- GitHub Actions CI that runs the full test suite on `main`, `codex/**`, and
  pull requests to `main`

## What This Repo Is Not Yet

This repository should not be described as:

- a confirmed live Hermes installation
- a production-attached runtime on a real operator machine
- the full target-state autonomous system described in the broader architecture
- proof that the richer long-horizon loops are already operating live

The current state is best described as a strong pre-live implementation
substrate with deterministic runtime proofs.

## Repo Map

```text
.
├── schemas/             # SQLite contracts for the five-database baseline
├── migrate.py           # Applies schemas and verifies structural drift
├── financial_router/    # Routing policy, cost logic, approval gates
├── immune/              # Fail-closed security and policy validation
├── council/             # Deliberation types, prompts, validators, orchestration
├── skills/              # Hermes-facing integration and runtime scaffolding
├── eval/                # Milestone runners, mock backends, fixtures, reporting
├── tests/               # Unit and integration coverage
└── .github/workflows/   # CI automation
```

## The Five Databases

Runtime state is intentionally split across five SQLite databases:

- `strategic_memory.db`: briefs, research tasks, opportunities, council outputs
- `telemetry.db`: chain definitions, step outcomes, execution traces,
  harness variants, frontier view, and reliability views
- `immune_system.db`: verdicts, alerts, revocations, security audit trails
- `financial_ledger.db`: routing decisions, costs, revenue, projects, kill data
- `operator_digest.db`: digests, alerts, gates, harvest requests, operator load

This separation keeps safety, finance, operator UX, and strategic state easier
to reason about and audit.

## Key Entry Points

- `migrate.py`
  Bootstraps and verifies the full SQLite baseline.
- `financial_router/`
  Enforces routing, quality, and spend policy.
- `immune/`
  Implements the fail-closed validation layer.
- `skills/runtime.py`
  The main Hermes-facing runtime bootstrap and workflow proof entry point.
- `eval/runner.py`
  Runs milestone-oriented evals against deterministic harnesses.

## Quick Verification

Install development dependencies:

```bash
python3 -m pip install -r requirements-dev.txt
```

Run the full test suite:

```bash
python3 -m pytest -q
```

Run the skill-focused suite:

```bash
python3 -m pytest -q tests/test_skills
```

Run the M2 eval:

```bash
python3 -m eval.runner --milestone M2
```

Run the deterministic runtime proof:

```bash
python3 -m skills.runtime --operator-workflow
```

Run the repo-local Hermes contract harness:

```bash
python3 -m skills.runtime --contract-harness
```

Run the explicit runtime bootstrap path:

```bash
python3 -m skills.runtime --bootstrap-live
```

Run the runtime doctor check:

```bash
python3 -m skills.runtime --doctor
```

Run the real-Hermes readiness command:

```bash
python3 -m skills.runtime --readiness
```

That command prepares the repo-managed runtime bundle and canonical databases
under the selected Hermes paths, generates a repo-owned Hermes profile under
`~/.hermes/profiles/<profile>/` (`config.yaml` plus a spec-compat
`profile.yaml` projection), validates that generated profile/config shape
structurally, checks the live Hermes CLI/version/profile/seed-tool surface,
runs a live Hermes CLI smoke test when Hermes is available, verifies
`STEP_OUTCOME`/log evidence, creates a deterministic data snapshot under the
runtime checkpoints directory, and exits clearly when Hermes is not installed.

It also reports two current drifts explicitly instead of hiding them:

- `spec/00_manifest.md` declares Hermes `v0.9.0+`, while `spec/s07_hermes_config.md`
  §7.5c still says `v0.8.0+`
- `spec/s07_hermes_config.md` §7.5c D1-2 still names
  `~/.hermes/profiles/<profile>/profile.yaml`, while current Hermes docs/profile
  commands center `config.yaml` inside the profile directory, so the repo now
  emits both artifacts from one owned profile spec instead of pretending the
  surfaces already match

One remaining uncertainty is still called out by the command itself: current
Hermes public docs clearly describe `config.yaml` and `approvals.mode`, but
they do not clearly document a first-class `dangerous_commands` config schema,
so the repo continues to project and validate the §7.5c dangerous-command set
as a repo-owned contract until live Hermes proves the exact upstream key
shape.

The readiness command now also runs the repo-local Hermes contract harness
first, so readiness failures distinguish between:

- repo-local contract breaks in the generated profile/gate/halt/restart path
- live Hermes CLI/profile drift on top of an otherwise healthy local contract

On machines without Hermes installed yet, you can still verify the offline
attachment path without attempting the live chat smoke test:

```bash
python3 -m skills.runtime --readiness --skip-cli-smoke
```

That runtime proof now exercises this chain from a clean layout:

`heartbeat -> immune check -> route -> research brief -> opportunity routing ->
harvest request -> council review -> project handoff -> phase gate ->
operator alert -> digest -> observability readback`

If you only want to bootstrap and verify the databases directly:

```bash
python3 migrate.py --db-dir ./data --verify
```

## How To Read This Codebase

If you are new here, start in this order:

1. `README.md`
2. `migrate.py`
3. `schemas/*.sql`
4. the module you want to change
5. the nearest test file under `tests/`

Treat the code and tests as the source of truth for what is implemented today.
If you also have access to the broader architecture workspace, use that for
target-state intent and this repository for implemented behavior.

## Working Style

- contracts first
- local first
- fail closed
- auditability over hidden magic
- deterministic tests over vague claims
- small, verified increments over broad rewrites

## Short Version

This repository already contains a substantial, validated implementation
substrate for Hybrid Autonomous AI: databases, routing, immune guardrails,
council scaffolding, research and operator skills, evals, CI, and a
deterministic Hermes-facing runtime proof. It is not yet the fully live system,
but it is well past a skeleton.
