from __future__ import annotations

import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from skills.db_manager import DatabaseManager
from skills.mission_control import MissionControlService, seed_demo_state
from migrate import SCHEMAS, apply_schema


PLUGIN_ROOT = Path(__file__).resolve().parent
DIST_ROOT = PLUGIN_ROOT / "dashboard" / "dist"
API_BASE = "/api/plugins/hybrid-mission-control"


PREVIEW_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Mission Control Preview</title>
  <link rel="stylesheet" href="/plugin/style.css">
  <style>
    :root {
      --color-background: #090d14;
      --color-foreground: #e5e7eb;
      --color-card: #111827;
      --color-border: #253044;
      --color-muted: #1f2937;
      --color-muted-foreground: #98a2b3;
      --color-primary: #7dd3fc;
      --color-primary-foreground: #ffffff;
      --color-destructive: #fb7185;
      --color-warning: #fbbf24;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--color-background);
      color: var(--color-foreground);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    .preview-shell {
      min-height: 100vh;
      display: grid;
      grid-template-columns: 236px minmax(0, 1fr);
    }
    .preview-sidebar {
      border-right: 1px solid var(--color-border);
      background: #070b12;
      color: #98a2b3;
      padding: 1rem;
    }
    .preview-brand {
      color: #fff;
      font-weight: 800;
      margin-bottom: 1.2rem;
    }
    .preview-nav {
      display: grid;
      gap: 0.25rem;
    }
    .preview-nav span {
      border-radius: 0.4rem;
      padding: 0.55rem 0.65rem;
      color: #9ca3af;
      font-size: 0.9rem;
    }
    .preview-nav .active {
      background: #172033;
      color: #fff;
    }
    .preview-main {
      min-width: 0;
      padding: 1.1rem;
    }
    .preview-topbar {
      display: flex;
      justify-content: space-between;
      gap: 1rem;
      margin-bottom: 1rem;
      color: var(--color-muted-foreground);
      font-size: 0.9rem;
    }
    .hermes-card {
      border: 1px solid var(--color-border);
      border-radius: 0.5rem;
      background: var(--color-card);
      box-shadow: 0 1px 2px rgba(0, 0, 0, 0.22);
    }
    .hermes-card-header {
      padding: 0.9rem 1rem 0;
    }
    .hermes-card-title {
      margin: 0;
      font-size: 1rem;
    }
    .hermes-card-content {
      padding: 1rem;
    }
    .hermes-badge {
      border: 1px solid var(--color-border);
      border-radius: 999px;
      padding: 0.18rem 0.45rem;
      background: var(--color-muted);
      color: var(--color-muted-foreground);
      font-size: 0.72rem;
      font-weight: 700;
    }
    button {
      border: 1px solid var(--color-border);
      border-radius: 0.45rem;
      background: var(--color-card);
      color: var(--color-foreground);
      cursor: pointer;
      padding: 0.45rem 0.7rem;
      font: inherit;
      font-size: 0.88rem;
    }
    button:hover { border-color: var(--color-primary); }
    input, textarea, select { font: inherit; }
    @media (max-width: 900px) {
      .preview-shell { grid-template-columns: 1fr; }
      .preview-sidebar { display: none; }
    }
  </style>
</head>
<body>
  <div class="preview-shell">
    <aside class="preview-sidebar">
      <div class="preview-brand">Hermes</div>
      <nav class="preview-nav">
        <span>Sessions</span>
        <span>Skills</span>
        <span class="active">Mission Control</span>
        <span>Logs</span>
        <span>Config</span>
      </nav>
    </aside>
    <main class="preview-main">
      <div class="preview-topbar">
        <span>Hermes dashboard plugin preview</span>
        <span>Demo data · local API</span>
      </div>
      <div id="root"></div>
    </main>
  </div>
  <script src="https://unpkg.com/react@18/umd/react.development.js"></script>
  <script src="https://unpkg.com/react-dom@18/umd/react-dom.development.js"></script>
  <script>
    window.__HERMES_PLUGINS__ = {
      register: function (_name, Component) {
        ReactDOM.createRoot(document.getElementById("root")).render(React.createElement(Component));
      }
    };
    window.__HERMES_PLUGIN_SDK__ = {
      React: React,
      hooks: {
        useState: React.useState,
        useEffect: React.useEffect
      },
      components: {
        Card: function (props) {
          return React.createElement("section", Object.assign({}, props, {className: "hermes-card " + (props.className || "")}), props.children);
        },
        CardHeader: function (props) {
          return React.createElement("div", Object.assign({}, props, {className: "hermes-card-header " + (props.className || "")}), props.children);
        },
        CardTitle: function (props) {
          return React.createElement("h2", Object.assign({}, props, {className: "hermes-card-title " + (props.className || "")}), props.children);
        },
        CardContent: function (props) {
          return React.createElement("div", Object.assign({}, props, {className: "hermes-card-content " + (props.className || "")}), props.children);
        },
        Badge: function (props) {
          return React.createElement("span", Object.assign({}, props, {className: "hermes-badge " + (props.className || "")}), props.children);
        },
        Button: function (props) {
          return React.createElement("button", props, props.children);
        },
        Input: function (props) {
          return React.createElement("input", props);
        },
        Separator: function (props) {
          return React.createElement("hr", props);
        }
      },
      fetchJSON: function (url, options) {
        return fetch(url, options).then(function (response) {
          if (!response.ok) return response.text().then(function (text) { throw new Error(text || response.statusText); });
          return response.json();
        });
      },
      utils: {
        isoTimeAgo: function (iso) {
          var delta = Math.max(0, Date.now() - new Date(iso).getTime());
          var minutes = Math.floor(delta / 60000);
          if (minutes < 1) return "just now";
          if (minutes < 60) return minutes + "m ago";
          return Math.floor(minutes / 60) + "h ago";
        }
      }
    };
  </script>
  <script src="/plugin/index.js"></script>
</body>
</html>
"""


class PreviewHandler(BaseHTTPRequestHandler):
    service: MissionControlService

    def _json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _text(self, body: str, content_type: str = "text/html; charset=utf-8") -> None:
        encoded = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._text(PREVIEW_HTML)
            return
        if parsed.path == "/plugin/index.js":
            self._text((DIST_ROOT / "index.js").read_text(encoding="utf-8"), "application/javascript; charset=utf-8")
            return
        if parsed.path == "/plugin/style.css":
            self._text((DIST_ROOT / "style.css").read_text(encoding="utf-8"), "text/css; charset=utf-8")
            return
        if parsed.path == f"{API_BASE}/snapshot":
            self._json(self.service.snapshot())
            return
        if parsed.path == f"{API_BASE}/health":
            self._json({"ok": True, "plugin": "hybrid-mission-control-preview"})
            return
        self.send_error(404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        body = self._read_body()
        try:
            if parsed.path.startswith(f"{API_BASE}/projects/") and parsed.path.endswith("/priority"):
                project_id = parsed.path.removeprefix(f"{API_BASE}/projects/").removesuffix("/priority")
                self._json(
                    self.service.set_project_priority(
                        project_id,
                        str(body.get("priority", "")),
                        focus_note=str(body.get("focus_note", "")),
                    )
                )
                return
            if parsed.path == f"{API_BASE}/manual-tasks":
                self._json(
                    self.service.create_manual_task(
                        title=str(body.get("title", "")),
                        details=str(body.get("details", "")),
                        priority=str(body.get("priority", "P2_NORMAL")),
                    )
                )
                return
            if parsed.path == f"{API_BASE}/research-tasks":
                self._json(
                    self.service.create_research_task(
                        title=str(body.get("title", "")),
                        brief=str(body.get("brief", "")),
                        workflow_id=str(body.get("workflow_id", "operator_prompts")),
                        domain=int(body["domain"]) if body.get("domain") not in (None, "") else None,
                        priority=str(body.get("priority", "P2_NORMAL")),
                        source=str(body.get("source", "operator")),
                        depth=str(body.get("depth", "QUICK")),
                        stale_after=body.get("stale_after"),
                    )
                )
                return
            if parsed.path.startswith(f"{API_BASE}/manual-tasks/"):
                task_id = parsed.path.removeprefix(f"{API_BASE}/manual-tasks/")
                self._json(
                    self.service.update_manual_task(
                        task_id,
                        status=body.get("status"),
                        priority=body.get("priority"),
                    )
                )
                return
            if parsed.path == f"{API_BASE}/tasks/priority":
                self._json(
                    self.service.update_system_task_priority(
                        str(body.get("kind", "")),
                        str(body.get("id", "")),
                        str(body.get("priority", "")),
                    )
                )
                return
        except Exception as exc:
            self._json({"error": str(exc)}, status=400)
            return
        self.send_error(404)

    def log_message(self, format: str, *args: object) -> None:
        return


def run(host: str = "127.0.0.1", port: int = 8770) -> None:
    data_dir = REPO_ROOT.parent / "tmp" / "mission-control-preview-v5"
    data_dir.mkdir(parents=True, exist_ok=True)
    for db_name, schema_rel in SCHEMAS.items():
        apply_schema(data_dir / f"{db_name}.db", REPO_ROOT / schema_rel)
    seed_demo_state(str(data_dir))
    db = DatabaseManager(str(data_dir))
    server = ThreadingHTTPServer((host, port), PreviewHandler)
    PreviewHandler.service = MissionControlService(db)
    print(f"mission_control_preview=http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    finally:
        db.close_all()
        server.server_close()


if __name__ == "__main__":
    run()
