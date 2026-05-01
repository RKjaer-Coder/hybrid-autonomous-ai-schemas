# Mission Control Dashboard Design

Mission Control is a Hermes dashboard tab, not a standalone product surface.
The native Hermes sidebar, routing, themes, sessions, logs, analytics, config,
skills, gateway, and update controls remain the shell. This plugin contributes
only the project-specific operator layer.

## Final Dashboard Map

1. Overview: a single system flow from research task to finding to opportunity,
   with branches for council, backlog, and further research. The view shows
   status, pressure, and model selection for each step.
2. Work: active project lanes and workflow-specific task boards in one place,
   with only the priority/status controls that are safe before live Hermes
   write validation.
3. Council: bounded strategic verdict history, current deliberation
   architecture and model lineup, Tier 2 pressure, decision queues, finished
   verdicts awaiting operator action, degradation, confidence, and
   deliberation-quality signals.
4. Research: separate workflow lanes for model/tooling radar, system
   architecture, business/opportunity work, security/compliance,
   operator-prompted research, standing briefs, and harvest follow-ups. Show
   the conversion path from research task to intelligence brief to action
   signal to opportunity candidate to council confirmation.
5. Finance: project P&L, route mix, usage, resource pressure, token accounting,
   G3 pressure, and explicit $0 autonomous paid spend posture.
6. Improve: Hermes harness activation threshold progress, reliability watch
   rows, traces, and frontier summaries for prompt/skill improvement.
Decision queues live under Council and remain read-only until Hermes dashboard
auth, audit, timeout, and replay semantics pass gate validation.

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
