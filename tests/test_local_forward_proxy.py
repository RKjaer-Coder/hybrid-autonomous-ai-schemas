from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib import error as urllib_error
from urllib import request as urllib_request

from skills.local_forward_proxy import ProxyServerConfig, start_proxy_server


class _UpstreamServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class _UpstreamHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        payload = json.dumps({"ok": True, "path": self.path}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args) -> None:
        _ = (format, args)
        return None


def test_local_forward_proxy_forwards_allowed_and_denies_blocked_requests(tmp_path):
    audit_log = tmp_path / "proxy-audit.jsonl"
    upstream = _UpstreamServer(("127.0.0.1", 0), _UpstreamHandler)
    upstream_thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    upstream_thread.start()

    try:
        allowed_port = int(upstream.server_address[1])
        blocked_port = allowed_port + 1
        proxy_config = ProxyServerConfig.from_payload(
            {
                "bind_host": "127.0.0.1",
                "bind_port": 0,
                "audit_log_path": str(audit_log),
                "outbound_allowlist": {
                    "domains": ["127.0.0.1", "localhost"],
                    "ports": [allowed_port],
                    "schemes": ["http"],
                },
            }
        )

        with start_proxy_server(proxy_config) as running:
            opener = urllib_request.build_opener(
                urllib_request.ProxyHandler(
                    {
                        "http": running.proxy_url,
                    }
                )
            )
            with opener.open(f"http://127.0.0.1:{allowed_port}/allowed", timeout=5) as response:
                assert response.status == 200
                payload = json.loads(response.read().decode("utf-8"))
            assert payload["ok"] is True

            try:
                opener.open(f"http://127.0.0.1:{blocked_port}/blocked", timeout=5)
                raise AssertionError("blocked request unexpectedly succeeded")
            except urllib_error.HTTPError as exc:
                assert exc.code == 403
    finally:
        upstream.shutdown()
        upstream.server_close()
        upstream_thread.join(timeout=5)

    entries = [
        json.loads(line)
        for line in Path(audit_log).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [entry["decision"] for entry in entries] == ["ALLOW", "DENY"]
