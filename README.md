# Hybrid Autonomous AI

Implementation repo for a hybrid, local-first autonomous intelligence system
built on Hermes Agent.

For stable workspace rules, read `../WORKSPACE.md` first. For live status,
current blockers, and latest verification, use `../CURRENT_STATE.md`.

## Orientation

- Use branch `main` as the stable integration baseline.
- Ignore old feature branches unless you are doing git history work.
- Treat this README as a repo runbook, not a rolling project-status document.

## Common Workflows

1. Run `python3 -m pytest -q`
2. Run `python3 -m skills.runtime --evidence-factory --until-replay-ready --evidence-cycles 5`
3. Run `python3 -m skills.runtime --replay-readiness-report`
4. Run `python3 -m skills.runtime --export-replay-corpus`
5. Run `python3 -m skills.runtime --optimizer-snapshot`
6. Run `python3 -m skills.runtime --analyze-harness-candidates`
7. Run `python3 -m skills.runtime --install-profile`, then `hermes dashboard --no-open`

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

Start the lean local operator UI:

```bash
python3 -m skills.runtime --mission-control
```

Install the Hermes-native Mission Control dashboard plugin:

```bash
python3 -m skills.runtime --install-profile
hermes dashboard --no-open
```

The standalone Mission Control server is now a prototype and API-contract
harness. The deployable operator UI target is the `hybrid-mission-control`
Hermes dashboard plugin installed under `~/.hermes/plugins/`.

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

> The implementation surface for runtime, governance, routing, evaluation, and
> Hermes integration work.
