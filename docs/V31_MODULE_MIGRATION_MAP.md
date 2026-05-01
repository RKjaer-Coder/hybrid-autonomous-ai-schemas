# v3.1 Module Migration Map

Date: 2026-05-01
Status: kernel-readiness baseline; first foundation-kernel slice implemented

This map classifies the current legacy repo modules for the v3.1 migration. It
is intentionally conservative: preserve verified behavior where it helps v3.1,
wrap unsafe authority paths, convert domain state to projections, and retire
only after replacement or compatibility evidence.

## Categories

- `adopt`: keep as v3.1 authoritative behavior with minimal changes
- `adapt`: change the module to satisfy v3.1 contracts directly
- `wrap`: keep behind a kernel adapter, broker, or compatibility boundary
- `convert-to-projection`: keep data/UI as derived state fed by kernel events
- `retire`: remove only after replacement, replay, or compatibility evidence

## Executive Map

### Adopt

`hermes_profile_contract.py`
: Keep as the repo-owned profile/config contract validator. It is deterministic
  and should change only when the Hermes contract changes.

`.github/workflows/ci.yml`
: Keep as the baseline verification gate. It already runs the test suite on
  `main` and `codex/**`; add kernel-focused checks later.

### Adapt

`immune/`
: Use as safety validation and broker-bypass detection support. It has strong
  fail-closed and pattern coverage, but needs a kernel-facing validation API
  over existing sheriff/judge/checker paths.

`skills/local_forward_proxy.py`
: Use as the enforced local network/provider proxy. Keep the implementation and
  add future grant, side-effect, data-class, and budget hooks.

`financial_router/`
: Use as a budget, quota, routing, and spend-reservation helper. Preserve tests,
  but make it subordinate to kernel budget grants and provider-call receipts.

`council/`
: Use as the deliberation protocol and calibration support. Council recommends;
  kernel policy assigns authority and gates.

`eval/` and `harness_variants.py`
: Use as replay, eval, known-bad, and holdout-governance substrate. Preserve
  fixtures and add v3.1 eval governance before using outputs for promotion.

`migrate.py`
: Use for schema fidelity and migration execution. Keep current database checks
  and extend it after the foundation schema is designed.

`runtime_control.py`
: Use as local halt/restart and break-glass support. Preserve as a helper and
  integrate with evented commands and side-effect halts later.

### Wrap

`skills/runtime.py`
: Keep as construction/runtime harness and installer. It is too broad to become
  kernel authority; future kernel commands must be separate.

`skills/hermes_v011_adapter.py`
: Keep as Hermes v0.12 approval/pre-tool adapter compatibility despite name
  drift. It should sit behind kernel broker semantics.

`skills/hermes_dispatcher.py` and `skills/hermes_interfaces.py`
: Keep as worker execution adapter interfaces. They must not own state, policy,
  or grants.

`profiles/`, `plugins/`, generated `skills/hybrid-autonomous-ai/`
: Treat as generated deployment artifacts. Regenerate as needed; do not treat
  them as source of truth.

### Convert To Projection

`schemas/*.sql`
: Treat as legacy domain projection and compatibility schemas. Do not build new
  authority into these schemas.

`skills/strategic_memory/`
: Keep as research/decision memory projection until v3.1 EvidenceBundle and
  Decision authority replaces direct writes.

`skills/research_domain/`
: Keep as ResearchRequest/EvidenceBundle projection and legacy helper. Map its
  records to v3.1 ResearchRequest before expanding behavior.

`skills/opportunity_pipeline/`
: Keep as commercial opportunity/project projection. Project authority moves to
  kernel Project, Decision, and Gate events.

`skills/operator_interface/`
: Keep as digest, alert, and local operator projection. Gate writes must route
  through future kernel commands.

`skills/observability/`
: Keep as telemetry and health projection. Feed from kernel events later; do not
  make it authoritative.

`skills/mission_control.py`
: Keep as prototype/API-contract harness only. It stays read-only and parked.

`hermes_plugins/hybrid-mission-control/`
: Keep as Hermes read-only dashboard projection. Do not add mutating controls
  until auth, audit, timeout, and replay semantics are proven.

### Retire

`bootstrap_patch.py`
: Retired. The root-level Hermes compatibility shim was removed after confirming
  runtime/bootstrap code and remaining tests use `immune.bootstrap_patch`
  directly.

`.DS_Store`, `.pytest_cache/`, `data/`, `alerts/`, `logs/`
: Local ignored runtime artifacts. Never commit them.

## v3.1 Primitive Ownership

`Command`
: New kernel module. Must be implemented before new authority paths.

`Event`
: New kernel module. Current telemetry events are not authoritative event-log
  records.

`CapabilityGrant`
: New kernel module plus proxy/router adapters. Existing proxy/router checks
  become enforcement helpers.

`Budget` and spend reservation
: `financial_router/` plus new kernel ledger. Kernel owns budget state; router
  helps evaluate routes.

`SideEffectIntent`
: New kernel module. No current module is sufficient; it must precede external
  autonomy.

`ArtifactRef`
: New kernel/artifact module. Current source/provenance fields are partial
  projections only.

`ResearchRequest`
: `skills/research_domain/` is a reference projection. Use existing records to
  design compatibility, not authority.

`EvidenceBundle`
: `skills/strategic_memory/` and research outputs are reference projections.
  Claims/sources need v3.1 data-class and retention policy.

`Decision`
: `council/` plus gate logs are references. Kernel assigns authority; Council
  recommends.

`Project`
: `skills/opportunity_pipeline/` and `financial_ledger` are references. Project
  phase state becomes kernel event authority.

`ModelCandidate`
: `financial_router/` model metadata and eval fixtures are references. The v3.1
  registry needs supply-chain fields and holdout governance.

## Execution Order

1. Done: create the foundation kernel schema and transaction boundary in a new
   module, without modifying legacy domain schemas.
2. Done: implement command, event, capability, budget, artifact, and
   side-effect records as the first authoritative contracts.
3. Next: wrap `financial_router/`, `skills/local_forward_proxy.py`, and `immune/` as
   enforcement helpers called by kernel-owned command processing.
4. Convert research, memory, opportunity, operator, observability, and Mission
   Control surfaces into projections fed by kernel events.
5. Adapt Council and eval code to v3.1 Decision, CouncilVerdict, ModelCandidate,
   and holdout-governance contracts.
6. Retire superseded shims and legacy writes only after compatibility tests and
   replay/projection checks prove replacement behavior.

## Non-Goals For This Map

- It does not implement the foundation kernel.
- It does not rewrite current schemas into v3.1 authority.
- It does not promote Mission Control to an active control surface.
- It does not activate autonomous discovery, external commitments, broad model
  promotion, or self-improvement proposers.
