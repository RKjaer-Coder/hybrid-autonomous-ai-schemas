# Hybrid Autonomous AI Schemas

This repository contains the **data contracts, migration tooling, routing logic, and evaluation harnesses** for a multi-database autonomous AI system design.

While it started as a SQLite schema bundle, it now serves as a compact reference implementation for:
- database-first system modeling,
- deterministic migration + verification,
- policy-aware financial model routing, and
- milestone-based eval harnesses with reproducible fixtures.

---

## Repository Intent

The core intent of this repo is to make critical autonomous-system contracts **explicit, testable, and inspectable**:

- **Schemas** define what must be persisted and how components interoperate.
- **Migration tooling** ensures databases are created consistently and validated for drift.
- **Router logic** encodes model-selection policy under budget, quality, and approval constraints.
- **Eval harnesses** provide milestone-oriented checks to validate behavior with stable fixtures.

This structure is designed to be useful for both:
- **Human readers** (architecture understanding, audits, onboarding), and
- **AI/code agents** (clear interfaces, deterministic checks, low ambiguity).

---

## Repository Layout

```text
.
├── schemas/                 # SQLite schema definitions (source of truth)
├── migrate.py               # Applies schemas + verifies required objects and drift
├── financial_router/        # Financial routing policy + typed contracts
├── eval/                    # Milestone harnesses, fixtures, and report formatting
├── tests/                   # Unit/integration tests across schemas, router, and harnesses
└── FINAL_STRICT_REAUDIT.md  # Audit-oriented notes
```

---

## Data Layer: SQLite Schema Suite

The system is modeled as **five SQLite databases** with focused responsibilities.

### 1) `strategic_memory.db`
Institutional memory and research/council artifacts.

Representative tables:
- `opportunity_records`
- `council_verdicts`
- `intelligence_briefs`
- `research_tasks`
- `dedup_records`
- `model_scout_reports`
- `model_assess_reports`
- `shadow_trial_reports`

### 2) `telemetry.db`
Reliability telemetry for step/chain-level outcomes.

Representative tables/views:
- `step_outcomes`
- `chain_definitions`
- `reliability_by_step` (view)
- `chain_reliability` (view)

### 3) `immune_system.db`
Security/guardrail verdicting and alert/circuit tracking.

Representative tables:
- `immune_verdicts`
- `security_alerts`
- `circuit_breaker_log`
- `jwt_revocation_log`
- `skill_improvement_log`

### 4) `financial_ledger.db`
Project finance, routing economics, and kill-governance inputs.

Representative tables/views:
- `projects`
- `kill_signals`
- `kill_recommendations`
- `revenue_records`
- `cost_records`
- `routing_decisions`
- `project_pnl` (view)

### 5) `operator_digest.db`
Operator-facing status, gates, and workload tracking.

Representative tables:
- `digest_history`
- `alert_log`
- `harvest_requests`
- `gate_log`
- `operator_heartbeat`
- `operator_load_tracking`

---

## Migration + Verification Workflow

Use the migration runner to create/apply all databases and optionally verify correctness.

```bash
python migrate.py --db-dir ./data --verify
```

What `migrate.py` does:
- creates the DB directory if needed,
- enables `WAL` + foreign keys,
- applies each schema script,
- tracks schema hash in `_schema_meta`,
- verifies expected tables/indexes exist,
- performs semantic drift checks on table/index signatures.

This makes the repo suitable for CI and for deterministic environment bootstrapping.

---

## Financial Router Module

`financial_router/` contains a typed routing policy that selects model tiers based on quality, cost, approvals, and session/project budget context.

Key concepts:
- Routing tiers (`local`, `free_cloud`, `subscription`, `paid_cloud`, fallback modes),
- typed contracts via dataclasses/enums in `types.py`,
- G3 approval paths + timeout behavior,
- spend reservation support with a SQLite-backed reservation registry for idempotency and concurrency safety.

This module can be imported independently from the schema/migration flow when you only need routing logic.

---

## Eval Harnesses and Fixtures

`eval/` provides milestone-driven evaluation entry points with deterministic fixtures.

Included milestones:
- `M1`, `M2`, `M3`, `M5`, and `KILL`

Runner capabilities include:
- selecting milestones to run,
- backend abstraction via `EvalBackend`,
- a deterministic `MockBackend` for repeatable checks,
- optional per-milestone timeout isolation,
- report formatting utilities.

This allows architectural behaviors to be exercised before wiring into a full production runtime.

---


## Immune System Integration Path

To run the immune subsystem end-to-end with current wiring:

1. **M1 validation (eval runner):** run `python -m eval.runner --backend eval.backends.immune_backend --milestone M1`.
   This uses the same fail-closed criteria as `tests/test_fail_closed.py`, but through the eval harness path.
2. **Hermes integration:** call `bootstrap_immune_patch()` from `bootstrap_patch.py` before opening any Hermes v0.8.0 agent session.
   The bootstrap helper wires `apply_immune_patch(...)` into Hermes tool dispatch candidates.
3. **Deep-scan model swap:** set `IMMUNE_DEEP_SCAN_MODEL_PATH` to a local Hugging Face model (for example a quantized DeBERTa-classifier)
   and use `build_deep_scan_model()` for a zero-contract swap with Sheriff/Judge.
4. **Canary audit boundary:** `canary_audits` is intentionally external to immune write-paths. The immune code enforces allowlists;
   the `immune_canary_audit` cron skill should execute independent canary probes and write those rows.

## Testing

Run the test suite:

```bash
python -m unittest discover -s tests -v
```

Tests cover (at a high level):
- schema constraints and index presence,
- migration verification behavior,
- WAL mode and persistence assumptions,
- financial router decision logic,
- eval fixture/harness behavior.

---

## Typical Usage Paths

### For humans (architecture review / onboarding)
1. Read this README for high-level intent.
2. Inspect `schemas/*.sql` for contracts.
3. Run migrations and tests locally.
4. Review router policy + eval harness behavior.

### For AI agents (implementation / checks)
1. Use `migrate.py --verify` to establish/validate DB baseline.
2. Treat schema files + `financial_router/types.py` as interface contracts.
3. Use eval fixtures/harnesses for deterministic behavioral checks.
4. Extend tests when changing schema, routing, or eval semantics.

---

## Design Principles

- **Contracts first**: persistence and typed interfaces are explicit.
- **Determinism**: reproducible fixtures, hash/signature checks, strict validations.
- **Auditability**: clear boundaries between memory, telemetry, security, finance, and operator layers.
- **Incremental extensibility**: you can add tables, router rules, or harnesses without rewriting the entire stack.

If you are extending this repository, prefer small, test-backed changes that preserve these principles.
