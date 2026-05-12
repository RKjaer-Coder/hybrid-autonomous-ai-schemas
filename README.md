# Hybrid Autonomous AI

Implementation repo for a hybrid, local-first autonomous intelligence system
built on Hermes Agent and governed by the v3.1 architecture spec.

For stable workspace rules, read `../WORKSPACE.md` first. For live status,
current blockers, and latest verification, use `../CURRENT_STATE.md`.

## Orientation

- Use branch `main` as the stable integration baseline.
- Ignore old feature branches unless you are doing git history work.
- Treat this README as a repo runbook, not a rolling project-status document.
- Treat `../spec/*.md` as the architecture source of truth and this repo as
  implemented behavior.
- The current code is a deterministic pre-Hermes substrate with kernel-owned
  authority for state, policy, evidence, replay, recovery, artifacts, side
  effects, model intelligence seeds, and the first bounded commercial loops.
  Do not treat legacy databases, skills, or dashboards as authority unless a
  kernel module explicitly owns that behavior.

## Current Posture

The repo is built to harden the control kernel before live Hermes attachment.
Hermes Agent remains the target execution and operator environment, but live
Hermes, LM Studio, Mac Studio validation, provider plugins, and dashboard
authority are still gated until the target machine can prove them.

The custom Mission Control server/dashboard plugin has been retired. Operator
visual workflows should use Hermes Agent's native dashboard surfaces: Kanban,
agent profiles, analytics, chat, models, and plugin controls. The repo keeps
the install/profile/runtime contracts, while gates, spend, side effects,
artifact governance, recovery, and replay remain kernel-owned.

## v3.1 Kernel Posture

The first deterministic foundation-kernel slice lives in `kernel/` and
`schemas/kernel.sql`. The kernel now owns the audit-critical records for
commands, events, grants, budgets, research evidence, decisions, project loops,
artifact governance, side-effect receipts, recovery readiness, encrypted
storage descriptors, backup/restore verification, model routing seeds, and
replay/projection comparison. The existing modules are classified as:

- `adopt`: keep as v3.1 authoritative behavior with minimal changes
- `adapt`: preserve and change to satisfy v3.1 contracts
- `wrap`: preserve behind a kernel adapter, broker, or compatibility boundary
- `convert-to-projection`: keep as derived state fed by kernel events
- `retire`: remove only after replacement or compatibility evidence

The highest-level posture is:

- extend kernel authority in `kernel/` and `schemas/kernel.sql`
- keep `skills/runtime.py` as a thin CLI/import compatibility entrypoint for
  `kernel.runtime_compat`
- adapt `immune/`, `financial_router/`, `skills/local_forward_proxy.py`,
  `council/`, `eval/`, `harness_variants.py`, `migrate.py`, and
  `runtime_control.py`
- wrap Hermes dispatch/adapters, generated profile artifacts, and
  provider/session integration
- convert strategic memory, research domain, opportunity pipeline, operator
  interface, observability, and current `schemas/*.sql` into
  projections
- retire ignored local artifacts, superseded shims, and repo-maintained UI
  surfaces after tests prove they are unused; the root `bootstrap_patch.py`
  shim and custom Mission Control dashboard have been retired

## Common Workflows

1. Run `python3 -m pytest -q`
2. Run `python3 migrate.py --db-dir ./data --verify`
3. Run `python3 -m skills.runtime --proxy-self-test`
4. Run `python3 -m skills.runtime --operator-workflow`
5. For replay/eval work, run
   `python3 -m skills.runtime --evidence-factory --until-replay-ready --evidence-cycles 5`
6. For Hermes attachment work, run `python3 -m skills.runtime --install-profile`,
   then `hermes dashboard --no-open`

## Repository Layout

```text
schemas/             Legacy SQLite contracts; v3.1 projection references
migrate.py           Applies schemas and verifies structural drift
financial_router/    Budget/routing helper to adapt under kernel authority
immune/              Safety validation and broker-bypass detection helper
council/             Deliberation protocol; recommends, never gate-authoritative
skills/              Hermes-facing skills, projections, and runtime integration
eval/                Harnesses, fixtures, reporting, future holdout governance
tests/               Unit and integration coverage
.github/workflows/   CI
```

## Runtime State

- `kernel.db`: v3.1 command/event, capability grant, budget, artifact,
  side-effect, research evidence, commercial project loop, phase/status rollup,
  close-decision, customer commitment receipt, model intelligence,
  backup/restore, encrypted payload, recovery-readiness, and replay/projection
  comparison authority
- `strategic_memory.db`: briefs, opportunities, research tasks, council outputs
- `telemetry.db`: step outcomes, execution traces, harness variants, replay data
- `immune_system.db`: immune verdicts, alerts, breakers, quarantine/fallback audit
- `financial_ledger.db`: routing decisions, costs, revenue, projects, phase state
- `operator_digest.db`: digests, alerts, gates, harvest requests, runtime control

`kernel.db` is the new v3.1 authority baseline. The other databases are verified
legacy implementation surfaces and should be fed as projections where they
remain useful.

## Quick Start

Install dev dependencies:

```bash
python3 -m pip install -r requirements-dev.txt
```

Verify the schema baseline:

```bash
python3 migrate.py --db-dir ./data --verify
```

Runtime launch paths fail closed on schema drift. `migrate.py --verify` checks
table SQL semantics, including CHECK constraints and STRICT tables, and the
runtime refuses to boot if any deployed database does not match the current
schema contract.

The commercial loop, artifact lifecycle, backup/restore path, recovery
readiness packets, encrypted payload descriptors, and model-routing seed records
are kernel-owned deterministic state. The legacy module/database migration map
is also kernel-owned and replay-checked, with a deterministic outbox-fed
operator digest projection for read-only inspection. The pre-Hermes readiness
summary composes replay, recovery, adapter, and migration evidence into one
read-only operator packet. Live workers, real customer integrations, revenue
webhooks, and Hermes-native dashboard authority remain future-gated.

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

Create or refresh the read-only recovery-readiness packet before Hermes
adapter validation:

```bash
python3 -m skills.runtime --recovery-readiness
```

Create or surface the read-only Hermes v0.13 adapter-readiness packet from
repo-local proof inputs:

```bash
python3 -m skills.runtime --hermes-adapter-readiness
```

Create or surface the read-only legacy module/database migration-readiness map:

```bash
python3 -m skills.runtime --migration-readiness
```

Create or surface the read-only pre-Hermes readiness summary:

```bash
python3 -m skills.runtime --pre-hermes-readiness
```

Export activation-relevant replay traces for offline harness work:

```bash
python3 -m skills.runtime --export-replay-corpus
```

Capture a runtime/bootstrap snapshot plus replay summary:

```bash
python3 -m skills.runtime --optimizer-snapshot
```

Install the repo-owned Hermes profile and use Hermes Agent's native dashboard
for Kanban, agent profiles, analytics, chat, models, and plugin controls:

```bash
python3 -m skills.runtime --install-profile
hermes dashboard --no-open
```

The repo no longer ships a custom Mission Control server or dashboard plugin.
Gate and side-effect authority remains in the kernel/runtime contracts; the
dashboard is a native Hermes operator surface.

Rank constrained harness candidates from real replay evidence:

```bash
python3 -m skills.runtime --analyze-harness-candidates
```

Create one constrained proposal from the top replay candidate:

```bash
python3 -m skills.runtime --propose-best-harness-candidate
```

Run the repo-local Hermes contract harness:

```bash
python3 -m skills.runtime --contract-harness
```

Check live-Hermes readiness on a machine with Hermes installed:

```bash
python3 -m skills.runtime --readiness
```

Readiness also requires the Council delegate isolation canary, the repo-local
v0.12 approval-hook adapter contract, the read-only Hermes v0.13
adapter-readiness packet linked to current recovery-readiness evidence, LM
Studio/local-provider doctor readiness, and
`hermes -z` one-shot smoke evidence before Council Tier 1 or paid-capable
operation is treated as launch-safe. The adapter-readiness path never enables
dashboard write controls, customer commitments, or provider/plugin calls.

## Useful Commands

```bash
python3 -m eval.runner --milestone M1
python3 -m eval.runner --milestone M4
python3 -m eval.runner --milestone M5
python3 -m skills.runtime --doctor
python3 -m skills.runtime --proxy-self-test
python3 -m skills.runtime --evidence-factory --until-replay-ready --evidence-cycles 5
python3 -m skills.runtime --export-replay-corpus
python3 -m skills.runtime --optimizer-snapshot
python3 -m skills.runtime --analyze-harness-candidates
python3 -m skills.runtime --recovery-readiness
python3 -m skills.runtime --pre-hermes-readiness
python3 -m skills.runtime --bootstrap-live
python3 -m skills.runtime --mac-studio-day-one
```

## Working Description

The best short description of this repository is:

> A deterministic v3.1 control-kernel substrate for a local-first Hermes Agent
> workspace, with legacy modules kept only as adapters, projections, or
> compatibility surfaces until promoted into kernel authority.
