# Mission Control Dashboard Design

Mission Control is a Hermes dashboard tab, not a standalone product surface.
The native Hermes sidebar, routing, themes, sessions, logs, analytics, config,
skills, gateway, and update controls remain the shell. This plugin contributes
only the project-specific operator layer.

## Final Dashboard Map

1. Overview: system logic map, compact status strip with green/yellow/red
   operating state, per-area status cards, and active alerts. Yellow means the
   operator is needed for a decision that is hampering continued work.
2. Projects: kanban lanes mapped to real project/phase states with project
   priority controls.
3. Tasks: operator-created tasks plus system research and harvest queues,
   segmented into workflow-specific kanban boards with priority/status controls
   where safe.
4. Council: bounded strategic verdict history, Tier 2 pressure, degradation,
   confidence, and deliberation-quality signals.
5. Research: separate workflow lanes for model/tooling radar, system
   architecture, business/opportunity work, security/compliance,
   operator-prompted research, standing briefs, and harvest follow-ups. Show
   the conversion path from research task to intelligence brief to action
   signal to opportunity candidate to council confirmation.
6. Finance: project P&L, route mix, usage, resource pressure, token accounting,
   G3 pressure, and explicit $0 autonomous paid spend posture.
7. Self-Improve: Hermes harness activation threshold progress, reliability
   watch rows, traces, and frontier summaries for prompt/skill improvement.
8. Decisions: read-only G1-G4/G3/quarantine/runtime-halt queues until Hermes
   dashboard auth, audit, timeout, and replay semantics pass gate validation.

## Design Rules

- Reflect real workflow objects; do not abstract Council, Research, Gates, or
  Finance into generic productivity concepts.
- Keep the plugin bundle tiny: plain JavaScript IIFE, Hermes SDK components,
  dashboard theme variables, and no bundled React.
- Use `MissionControlService.snapshot()` as the backend contract so standalone
  and Hermes-native views cannot drift.
- Keep runtime cost negligible: no Node bridge, no Vite dev server, no WebSocket
  fanout, no client-side polling faster than 15 seconds, and no unbounded
  database scans.
- Prefer read-only visibility first for high-consequence surfaces. Promote
  write actions only after CLI-equivalent tests exist.
- Keep visual density calm: the operator should see pressure, flow, and next
  action without reading implementation internals.
- Treat priority as an operating signal, not a label: project cards should
  preserve a short focus note alongside P0-P3 priority so the operator can see
  why a project is being pulled forward.
- Make research purpose visible before task detail. The Research Agent works
  for multiple reasons; model scouting, architectural improvement, market
  discovery, compliance/security, and one-off operator asks should never appear
  as one undifferentiated queue.
- Make actionability visible early. A research finding can become an
  OpportunityRecord when it creates cause for action; important or risky
  findings should visibly pass through council confirmation rather than
  disappearing into a generic brief list.
- Match Hermes visually. Keep the plugin in a restrained dark dashboard style
  using Hermes theme variables, compact controls, and plain operational labels
  instead of a separate product aesthetic.

## External Reference Boundary

Kori's MIT Hermes dashboard is a useful reference for live-session, tool-feed,
subagent, and auto-wiki information architecture. Mission Control deliberately
does not import its heavier React/Vite/Node/WebSocket architecture; those ideas
belong here only when they can be expressed through Hermes dashboard plugin SDK
components and existing repo snapshots.

## Promotion Path

1. Keep the plugin source Hermes-shaped even while Hermes itself is unavailable.
2. Confirm `/api/plugins/hybrid-mission-control/health` and `/snapshot` once a
   Hermes dashboard exists.
3. Verify project priority, task priority, manual task creation, and alert
   acknowledgement write heartbeat rows with channel `hermes_dashboard`.
4. Only after gate-validation tests pass, add writable gate/quarantine review
   controls.
