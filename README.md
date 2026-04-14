# Hybrid Autonomous AI

This repository is the implementation substrate for `Hybrid Autonomous AI`: a
hybrid, local-first autonomous intelligence system built on Hermes Agent and
designed to run primarily on operator-owned infrastructure while selectively
using cloud frontier models for high-value or safety-critical work.

It is not the whole live system yet. This repo is the contract-first baseline
that makes the architecture concrete: persistence schemas, routing policy,
immune guardrails, Hermes-facing integration scaffolding, and milestone evals.
If the surrounding architecture workspace is available, treat the sibling
`spec/` directory as architecture truth and this repo as implemented truth.

## What This Repo Is For

Use this repo to answer:

- what has actually been implemented today
- how core persistence contracts are shaped
- how model routing and spend controls work
- how the immune subsystem fails closed
- how Hermes integration is being scaffolded and tested
- how to verify changes with deterministic tests and evals

The repo is designed to be useful for both humans and agents: low-ambiguity
interfaces, inspectable contracts, and fast verification loops.

## Current Reality

This codebase is in the transition from validated reference implementation to
real Hermes runtime integration.

Implemented here today:

- five SQLite databases with migration and drift verification tooling
- financial router logic with typed contracts and gated spend behavior
- immune subsystem core: Sheriff, Judge, config, deep-scan adapter, bootstrap
  patching, and verdict logging
- council and skill-layer scaffolding for bootstrap, memory, routing, operator,
  observability, and research flows
- eval harnesses and deterministic fixtures for milestone verification
- a Hermes runtime integration layer in `skills/runtime.py` that can prepare
  runtime directories, install a local profile bundle, migrate the databases,
  create a Hermes session context, run a doctor check, and prove a narrow
  operator workflow against a mock runtime

Not yet proven live in this repo:

- confirmed wiring into a real Hermes startup/profile path
- full production workflows for research, strategic memory, operator, and
  observability layers
- the richer target-state autonomy described in the broader architecture spec

## Repository Layout

```text
.
├── schemas/             # SQLite contracts for the five-database baseline
├── migrate.py           # Applies schemas and verifies structural drift
├── financial_router/    # Typed routing policy, cost logic, approval gates
├── immune/              # Fail-closed guardrail subsystem
├── council/             # Deliberation-related contracts and helpers
├── skills/              # Hermes-facing integration and bootstrap scaffolding
├── eval/                # Milestone runners, backends, fixtures, reporting
├── tests/               # Unit and integration tests
└── README.md            # Human + agent orientation
```

## The Five Databases

The baseline runtime state is split across five SQLite databases with explicit
responsibility boundaries:

- `strategic_memory.db`: opportunity records, council outputs, research briefs,
  and other long-horizon memory artifacts
- `telemetry.db`: step outcomes, chain definitions, and reliability views
- `immune_system.db`: security verdicts, alerts, revocations, and guardrail logs
- `financial_ledger.db`: routing decisions, revenue, costs, projects, and
  kill-governance inputs
- `operator_digest.db`: digest history, alerts, gate state, and operator load

This split is intentional. It keeps audit domains clearer and reduces hidden
coupling between strategic state, safety controls, finances, and operator UX.

## Key Modules

### `migrate.py`

Creates and verifies the full SQLite baseline. This is the authoritative entry
point for bootstrapping repo state and checking schema drift.

### `financial_router/`

Implements typed routing decisions across local, free-cloud, subscription, and
paid paths under budget, approval, and quality constraints. G3 spend approval
is treated as a hard boundary, not a soft preference.

### `immune/`

Implements the fail-closed validation layer. This is where Sheriff, Judge,
context-parameterized checks, verdict logging, and deep-scan model integration
live.

### `skills/`

Contains Hermes-facing integration scaffolding. In particular,
`skills/runtime.py` is the current bootstrap path for preparing a runtime
layout, migrating all databases, constructing a Hermes session context, and
running bootstrap against a tool registry or mock runtime.

### `eval/`

Provides milestone-oriented eval entry points and deterministic fixtures so key
architecture claims can be exercised before full live deployment.

## Fast Start

Install dev dependencies:

```bash
python3 -m pip install -r requirements-dev.txt
```

Run the full test suite:

```bash
python3 -m pytest -q
```

Run a representative milestone eval:

```bash
python3 -m eval.runner --milestone M1
```

Smoke-test the Hermes runtime scaffold locally:

```bash
python3 -m skills.runtime --data-dir /tmp/hybrid-autonomous-ai-data
```

Install a local runtime profile bundle:

```bash
python3 -m skills.runtime --install-profile
```

Run the runtime doctor:

```bash
python3 -m skills.runtime --doctor
```

Prove the Stage 0/1 operator workflow:

```bash
python3 -m skills.runtime --operator-workflow
```

This workflow now installs the local profile bundle automatically before its
final doctor check, so it succeeds from a clean runtime layout.

Create and verify the five-database baseline directly:

```bash
python3 migrate.py --db-dir ./data --verify
```

## How To Work In This Repo

For humans:

1. Read this README for current intent and boundaries.
2. Treat code and tests as the source of truth for what exists now.
3. Start with `migrate.py`, `schemas/*.sql`, and the module under change.
4. Verify changes with targeted tests before broad refactors.

For agents:

1. Assume this repo is the implemented truth, not the aspirational architecture.
2. Prefer small reads: the touched module, its types, and its tests.
3. Preserve contract boundaries unless the task explicitly changes them.
4. Run the smallest relevant tests and evals after edits.
5. Surface spec/repo drift explicitly rather than smoothing it over.

## Working Principles

- Contracts first: schemas and typed interfaces should be explicit.
- Local first: prefer local execution paths unless cloud use is intentionally
  justified by quality, leverage, or safety.
- Fail closed: unsafe or ambiguous behavior should stop or degrade safely.
- Auditability over magic: state transitions and decisions should be visible.
- Small verified increments: prefer test-backed progress over broad rewrites.

## If You Are New Here

The shortest accurate summary is:

This repo is the implementation backbone for Hybrid Autonomous AI. It already
contains a strong, test-backed substrate for persistence, routing, immune
guardrails, evals, and Hermes bootstrap scaffolding, but it should still be
described as pre-live integration rather than a fully deployed autonomous
system.
