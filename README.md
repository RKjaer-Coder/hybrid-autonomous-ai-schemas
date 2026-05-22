# Hybrid Autonomous AI

Deterministic control-kernel substrate for a local-first Hermes Agent
workspace.

This repo is not trying to make agents authoritative. It builds the boring
center of the system: state, policy, gates, spend, audit, replay, recovery,
artifact governance, side-effect records, seed model intelligence, and bounded
commercial/project loops. Agents and Hermes workers are replaceable execution
surfaces around that kernel.

For workspace-level orientation, read `../WORKSPACE.md`, then
`../CURRENT_STATE.md`, then `../spec/00_manifest.md`.

## Current Status

- Pre-live and prebuilt-first.
- Hermes Agent v0.14.0 is the target execution substrate, but live Hermes,
  dashboard authority, provider plugins, local-provider routes, and Mac Studio
  validation remain gated.
- `main` is the stable integration branch. New implementation work should use a
  short-lived `codex/<task>` branch.
- `kernel/` and `schemas/kernel.sql` are the v3.1 authority baseline.
- Legacy modules and databases remain adapters, projections, wrappers, or
  compatibility surfaces unless a kernel slice explicitly promotes them.

## What The Repo Contains

```text
kernel/              Authoritative v3.1 records, services, replay, runtime glue
schemas/             SQLite contracts, including kernel authority and projections
skills/              Hermes-facing skills and compatibility entrypoints
immune/              Safety validation and broker-bypass detection helpers
financial_router/    Budget/routing helper being adapted under kernel authority
council/             Deliberation protocol; recommends, never owns gates
eval/                Harnesses, fixtures, and reporting
tests/               Unit and integration coverage
.github/workflows/   CI
```

## Authority Model

The control kernel owns:

- commands, events, workflow transitions, and replay/projection comparison
- capability grants, budget reservations, and spend records
- operator gates, halt/recovery state, and audit evidence
- artifact descriptors, retention/deletion policy, and side-effect receipts
- research evidence, decision packets, project/commercial records, and seed
  model-routing state

Hermes skills, profiles, dashboards, legacy databases, and generated runtime
packets are compatibility or projection surfaces unless kernel policy says
otherwise.

## Quick Start

Install development dependencies:

```bash
python3 -m pip install -r requirements-dev.txt
```

Run the full local verification suite:

```bash
python3 -m pytest -q
python3 migrate.py --db-dir ./data --verify
```

Run common runtime proof packets:

```bash
python3 -m skills.runtime --readiness-suite
python3 -m skills.runtime --operator-workflow
python3 -m skills.runtime --proxy-self-test
```

## Useful Runtime Commands

```bash
python3 -m skills.runtime --recovery-readiness
python3 -m skills.runtime --pre-hermes-readiness
python3 -m skills.runtime --hermes-adapter-readiness
python3 -m skills.runtime --target-machine-validation-run-packet
python3 -m skills.runtime --pre-live-completion-bundle
python3 -m skills.runtime --model-efficiency-service-packet
python3 -m skills.runtime --model-efficiency-customer-validation-brief
python3 -m skills.runtime --self-improvement-snapshot
python3 -m skills.runtime --export-replay-corpus
python3 -m skills.runtime --analyze-harness-candidates
```

Live attachment commands such as profile installation, Hermes dashboard checks,
local-provider doctor checks, and Mac Studio day-one validation are future-gated
until the target machine is available.

## Development Rules

- Keep authoritative behavior in `kernel/`.
- Keep `skills/runtime.py` as a thin compatibility entrypoint over
  `kernel.runtime_compat`.
- Prefer service helpers in `kernel/services/` for repeated runtime-artifact
  packet/checker construction.
- Preserve fail-closed behavior for gates, budgets, provider calls, route
  mutation, side effects, and customer-visible delivery.
- Do not add a repo-maintained dashboard product; use Hermes native dashboard
  surfaces only after auth, audit, timeout, and replay semantics are proven.
- Do not store secrets, raw customer data, or large transcripts in events; use
  governed artifact references.

## GitHub Flow

1. Start from `main`.
2. Create a short-lived branch named `codex/<task>`.
3. Keep each PR focused on one behavior or cleanup slice.
4. Run focused tests first, then `python3 -m pytest -q` for shared runtime or
   kernel changes.
5. Update `../CURRENT_STATE.md` only when live status or next priority changes.
6. Merge when CI is green, then delete the task branch.

## Working Description

> A deterministic v3.1 control-kernel substrate for a local-first Hermes Agent
> workspace, with legacy modules kept only as adapters, projections, or
> compatibility surfaces until promoted into kernel authority.
