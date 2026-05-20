# Hybrid Autonomous AI Review Rules

This repository implements a deterministic control kernel first and agent
behavior second. Code review should optimize for preserved authority,
auditability, replay safety, and reduced context load.

## Kernel Authority

- Critical state changes must flow through commands, events, and the
  kernel-owned transaction boundary.
- Security, spend, gates, side effects, and model promotion must fail closed.
- Derived projections may be rebuilt or compared, but they must not become the
  source of truth.

## Service Layer Shape

- Service modules are allowed when they simplify repeated orchestration, packet
  assembly, runtime glue, or context loading.
- Services must remain thin. They should call the authoritative store/domain
  APIs rather than reimplement policy or persistence rules.
- Avoid broad manager classes and duplicate helper logic.

## Runtime And Hermes

- Runtime/Hermes/dashboard/provider surfaces are adapters until live proof
  passes.
- Resume, handoff, checkpoints, gateway, and dashboard paths must revalidate
  kernel task state, grants, budgets, policy version, halt/quarantine state, and
  side-effect idempotency before continuing work.

## Self-Improvement And Review Loops

- Review-code loops may fix concrete findings and rerun verification.
- Ambiguous policy, workflow, customer-impacting, or authority changes require
  operator review or a patch-review packet.
- Self-improvement evals are evidence only. Promotion is operator-gated.

## Data Hygiene

- Do not store raw secrets, credentials, client files, or large raw transcripts
  in event payloads.
- Use artifact refs, hashes, retention metadata, and secret references.
