# Hybrid Mission Control Hermes Plugin

This plugin is the Hermes-native Mission Control surface for Hybrid Autonomous
AI. It renders the operator cockpit inside Hermes as a dashboard tab instead of
growing a separate frontend stack.

The bundle is intentionally small:

- plain JavaScript IIFE
- Hermes-provided React and UI components
- Hermes theme CSS variables
- bounded snapshot APIs
- no Node bridge, Vite server, WebSocket fanout, or bundled React

The dashboard covers Overview, Work, Council, Research, Finance, and Improve
from the single
`MissionControlService.snapshot()` contract.

Install it through the runtime profile installer:

```bash
python3 -m skills.runtime --install-profile
```

The installer copies this directory to `~/.hermes/plugins/hybrid-mission-control`
and writes `runtime_config.json` with the repo root and data directory. Then run:

```bash
hermes dashboard --no-open
```

Gate and quarantine review actions are intentionally read-only. They should
become writable only after Hermes dashboard auth, audit logging, timeout
handling, and replay semantics pass the same checks as the CLI gate path.
