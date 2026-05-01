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
- Read `docs/V31_MODULE_MIGRATION_MAP.md` before extending foundation-kernel
  implementation.
- The current code is a useful legacy substrate plus the first v3.1 kernel
  slice. Do not assume the legacy five-database schemas or broad runtime harness
  are v3.1 authority.

## v3.1 Kernel Posture

The first deterministic foundation-kernel slice lives in `kernel/` and
`schemas/kernel.sql`. The existing modules are classified as:

- `adopt`: keep as v3.1 authoritative behavior with minimal changes
- `adapt`: preserve and change to satisfy v3.1 contracts
- `wrap`: preserve behind a kernel adapter, broker, or compatibility boundary
- `convert-to-projection`: keep as derived state fed by kernel events
- `retire`: remove only after replacement or compatibility evidence

The canonical repo-side map is
`docs/V31_MODULE_MIGRATION_MAP.md`.

The highest-level posture is:

- extend kernel authority in `kernel/` and `schemas/kernel.sql` rather than
  mutating legacy domain schemas first
- adapt `immune/`, `financial_router/`, `skills/local_forward_proxy.py`,
  `council/`, `eval/`, `harness_variants.py`, `migrate.py`, and
  `runtime_control.py`
- wrap `skills/runtime.py`, Hermes dispatch/adapters, generated profile
  artifacts, and provider/session integration
- convert strategic memory, research domain, opportunity pipeline, operator
  interface, observability, Mission Control, and current `schemas/*.sql` into
  projections
- retire ignored local artifacts and superseded shims after tests prove they
  are unused; the root `bootstrap_patch.py` shim has been retired

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
docs/                v3.1 migration map and repo planning notes
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

## Current Databases

- `kernel.db`: v3.1 command/event, capability grant, budget, artifact, and
  side-effect authority
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

Export activation-relevant replay traces for offline harness work:

```bash
python3 -m skills.runtime --export-replay-corpus
```

Capture a runtime/bootstrap snapshot plus replay summary:

```bash
python3 -m skills.runtime --optimizer-snapshot
```

Start the lean local operator UI prototype:

```bash
python3 -m skills.runtime --mission-control
```

Install the Hermes-native Mission Control dashboard plugin:

```bash
python3 -m skills.runtime --install-profile
hermes dashboard --no-open
```

The standalone Mission Control server is a prototype and API-contract harness.
The Hermes dashboard plugin remains read-only until auth, audit, timeout, and
replay semantics are proven equivalent to the local gate path.

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
v0.12 approval-hook adapter contract, LM Studio/local-provider doctor readiness,
and `hermes -z` one-shot smoke evidence before Council Tier 1 or paid-capable
operation is treated as launch-safe.

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
python3 -m skills.runtime --bootstrap-live
python3 -m skills.runtime --mac-studio-day-one
```

## Working Description

The best short description of this repository is:

> The tested legacy substrate plus the first v3.1 deterministic control-kernel
> authority slice.
