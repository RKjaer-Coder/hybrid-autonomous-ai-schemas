from __future__ import annotations

import argparse
import contextlib
import http.client
import json
import select
import socket
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import TracebackType
from typing import Any
from urllib.parse import SplitResult, urlsplit


HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


class ProxyConfigError(RuntimeError):
    """Raised when the local forward proxy configuration is invalid."""


@dataclass(frozen=True)
class ProxyServerConfig:
    bind_host: str
    bind_port: int
    allowed_domains: tuple[str, ...]
    allowed_ports: tuple[int, ...]
    allowed_schemes: tuple[str, ...]
    audit_log_path: str
    connect_timeout_s: float = 5.0
    read_timeout_s: float = 15.0

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "ProxyServerConfig":
        allowlist = payload.get("outbound_allowlist") or {}
        bind_host = payload.get("bind_host") or "127.0.0.1"
        raw_bind_port = payload.get("bind_port")
        bind_port = int(18080 if raw_bind_port is None else raw_bind_port)
        audit_log_path = payload.get("audit_log_path")
        if not audit_log_path:
            raise ProxyConfigError("proxy config requires audit_log_path")
        domains = tuple(str(item).lower() for item in allowlist.get("domains", ()))
        ports = tuple(int(item) for item in allowlist.get("ports", ()))
        schemes = tuple(str(item).lower() for item in allowlist.get("schemes", ("http", "https")))
        if not domains:
            raise ProxyConfigError("proxy config requires at least one allowed domain")
        if not ports:
            raise ProxyConfigError("proxy config requires at least one allowed port")
        if not schemes:
            raise ProxyConfigError("proxy config requires at least one allowed scheme")
        return cls(
            bind_host=bind_host,
            bind_port=bind_port,
            allowed_domains=domains,
            allowed_ports=ports,
            allowed_schemes=schemes,
            audit_log_path=str(Path(audit_log_path).expanduser()),
            connect_timeout_s=float(payload.get("connect_timeout_s", 5.0)),
            read_timeout_s=float(payload.get("read_timeout_s", 15.0)),
        )

    @classmethod
    def from_file(cls, path: str | Path) -> "ProxyServerConfig":
        config_path = Path(path).expanduser()
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
        except OSError as exc:
            raise ProxyConfigError(f"cannot read proxy config: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise ProxyConfigError(f"invalid proxy config JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise ProxyConfigError("proxy config must be a JSON object")
        return cls.from_payload(payload)

    def to_payload(self) -> dict[str, Any]:
        return {
            "bind_host": self.bind_host,
            "bind_port": self.bind_port,
            "audit_log_path": self.audit_log_path,
            "connect_timeout_s": self.connect_timeout_s,
            "read_timeout_s": self.read_timeout_s,
            "outbound_allowlist": {
                "domains": list(self.allowed_domains),
                "ports": list(self.allowed_ports),
                "schemes": list(self.allowed_schemes),
            },
        }


@dataclass(frozen=True)
class ProxyDecision:
    allowed: bool
    reason: str
    scheme: str
    host: str
    port: int
    url: str


@dataclass(frozen=True)
class RunningProxyServer:
    server: "LocalForwardProxyServer"
    thread: threading.Thread

    @property
    def bind_host(self) -> str:
        return str(self.server.server_address[0])

    @property
    def bind_port(self) -> int:
        return int(self.server.server_address[1])

    @property
    def proxy_url(self) -> str:
        return f"http://{self.bind_host}:{self.bind_port}"

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def __enter__(self) -> "RunningProxyServer":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.stop()


class LocalForwardProxyServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], config: ProxyServerConfig) -> None:
        super().__init__(server_address, LocalForwardProxyHandler)
        self.proxy_config = config
        self.audit_lock = threading.Lock()
        audit_path = Path(config.audit_log_path).expanduser()
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        self.audit_log_path = audit_path

    def decide(self, *, scheme: str, host: str, port: int, url: str) -> ProxyDecision:
        normalized_scheme = scheme.lower()
        normalized_host = host.lower()
        if normalized_scheme not in self.proxy_config.allowed_schemes:
            return ProxyDecision(False, f"scheme_not_allowed:{normalized_scheme}", normalized_scheme, normalized_host, port, url)
        if port not in self.proxy_config.allowed_ports:
            return ProxyDecision(False, f"port_not_allowed:{port}", normalized_scheme, normalized_host, port, url)
        if not _host_allowed(normalized_host, self.proxy_config.allowed_domains):
            return ProxyDecision(False, f"domain_not_allowed:{normalized_host}", normalized_scheme, normalized_host, port, url)
        return ProxyDecision(True, "allowed", normalized_scheme, normalized_host, port, url)

    def write_audit_event(self, payload: dict[str, Any]) -> None:
        line = json.dumps(payload, sort_keys=True)
        with self.audit_lock:
            with self.audit_log_path.open("a", encoding="utf-8") as handle:
                handle.write(f"{line}\n")


class LocalForwardProxyHandler(BaseHTTPRequestHandler):
    server: LocalForwardProxyServer
    protocol_version = "HTTP/1.1"

    def do_CONNECT(self) -> None:
        host, port = _parse_connect_target(self.path)
        decision = self.server.decide(
            scheme="https",
            host=host,
            port=port,
            url=f"https://{host}:{port}",
        )
        if not decision.allowed:
            self._deny_request(decision, method="CONNECT")
            return
        try:
            upstream = socket.create_connection(
                (decision.host, decision.port),
                timeout=self.server.proxy_config.connect_timeout_s,
            )
        except OSError as exc:
            self._emit_audit(
                method="CONNECT",
                decision=decision,
                status_code=502,
                error=str(exc),
            )
            self.send_error(502, f"upstream connect failed: {exc}")
            return

        self.send_response(200, "Connection Established")
        self.end_headers()
        self.connection.setblocking(False)
        upstream.setblocking(False)
        try:
            _pipe_bidirectional(self.connection, upstream)
            self._emit_audit(method="CONNECT", decision=decision, status_code=200, error=None)
        finally:
            with contextlib.suppress(OSError):
                upstream.close()

    def do_GET(self) -> None:
        self._forward_http()

    def do_POST(self) -> None:
        self._forward_http()

    def do_PUT(self) -> None:
        self._forward_http()

    def do_PATCH(self) -> None:
        self._forward_http()

    def do_DELETE(self) -> None:
        self._forward_http()

    def do_HEAD(self) -> None:
        self._forward_http()

    def do_OPTIONS(self) -> None:
        self._forward_http()

    def log_message(self, format: str, *args: Any) -> None:
        _ = (format, args)
        return None

    def _forward_http(self) -> None:
        request_url = _resolve_request_url(self.path, self.headers)
        split = urlsplit(request_url)
        port = split.port or _default_port(split.scheme)
        decision = self.server.decide(
            scheme=split.scheme or "http",
            host=split.hostname or "",
            port=port,
            url=request_url,
        )
        if not decision.allowed:
            self._deny_request(decision, method=self.command)
            return

        body = self._read_body()
        upstream_cls = http.client.HTTPSConnection if decision.scheme == "https" else http.client.HTTPConnection
        upstream = upstream_cls(
            decision.host,
            decision.port,
            timeout=self.server.proxy_config.read_timeout_s,
        )
        try:
            upstream.request(
                self.command,
                _upstream_path(split),
                body=body,
                headers=_forward_headers(self.headers, decision.host, decision.port),
            )
            response = upstream.getresponse()
            payload = response.read()
        except OSError as exc:
            self._emit_audit(
                method=self.command,
                decision=decision,
                status_code=502,
                error=str(exc),
            )
            self.send_error(502, f"upstream request failed: {exc}")
            return
        finally:
            with contextlib.suppress(OSError):
                upstream.close()

        self.send_response(response.status, response.reason)
        for key, value in response.getheaders():
            if key.lower() in HOP_BY_HOP_HEADERS:
                continue
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)
        self._emit_audit(
            method=self.command,
            decision=decision,
            status_code=response.status,
            error=None,
        )

    def _deny_request(self, decision: ProxyDecision, *, method: str) -> None:
        payload = {
            "status": "DENIED",
            "reason": decision.reason,
            "url": decision.url,
            "host": decision.host,
            "port": decision.port,
        }
        encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(403, "Forbidden")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)
        self._emit_audit(method=method, decision=decision, status_code=403, error=None)

    def _emit_audit(
        self,
        *,
        method: str,
        decision: ProxyDecision,
        status_code: int,
        error: str | None,
    ) -> None:
        self.server.write_audit_event(
            {
                "decision": "ALLOW" if decision.allowed else "DENY",
                "reason": decision.reason,
                "method": method,
                "scheme": decision.scheme,
                "host": decision.host,
                "port": decision.port,
                "url": decision.url,
                "status_code": status_code,
                "client_ip": self.client_address[0] if self.client_address else None,
                "source_pid": None,
                "error": error,
            }
        )

    def _read_body(self) -> bytes | None:
        content_length = self.headers.get("Content-Length")
        if not content_length:
            return None
        return self.rfile.read(int(content_length))


def _host_allowed(host: str, allowed_domains: tuple[str, ...]) -> bool:
    normalized_host = host.lower().strip(".")
    for allowed in allowed_domains:
        normalized_allowed = allowed.lower().strip(".")
        if normalized_host == normalized_allowed:
            return True
        if normalized_allowed and normalized_host.endswith(f".{normalized_allowed}"):
            return True
    return False


def _parse_connect_target(target: str) -> tuple[str, int]:
    host, separator, raw_port = target.partition(":")
    if not separator:
        return host, 443
    return host, int(raw_port)


def _default_port(scheme: str) -> int:
    return 443 if scheme.lower() == "https" else 80


def _resolve_request_url(path: str, headers: Any) -> str:
    split = urlsplit(path)
    if split.scheme and split.hostname:
        return path
    host = headers.get("Host")
    if not host:
        raise ProxyConfigError("proxy request missing Host header")
    return f"http://{host}{path}"


def _upstream_path(split: SplitResult) -> str:
    path = split.path or "/"
    if split.query:
        return f"{path}?{split.query}"
    return path


def _forward_headers(headers: Any, host: str, port: int) -> dict[str, str]:
    forwarded: dict[str, str] = {}
    for key, value in headers.items():
        lower = key.lower()
        if lower in HOP_BY_HOP_HEADERS or lower == "host":
            continue
        forwarded[key] = value
    default_port = _default_port("https" if port == 443 else "http")
    forwarded["Host"] = host if port == default_port else f"{host}:{port}"
    return forwarded


def _pipe_bidirectional(client: socket.socket, upstream: socket.socket) -> None:
    sockets = [client, upstream]
    while True:
        readable, _, exceptional = select.select(sockets, [], sockets, 0.5)
        if exceptional:
            break
        if not readable:
            continue
        for sock in readable:
            try:
                chunk = sock.recv(65536)
            except OSError:
                return
            if not chunk:
                return
            other = upstream if sock is client else client
            try:
                other.sendall(chunk)
            except OSError:
                return


def start_proxy_server(config: ProxyServerConfig) -> RunningProxyServer:
    server = LocalForwardProxyServer((config.bind_host, config.bind_port), config)
    thread = threading.Thread(target=server.serve_forever, name="local-forward-proxy", daemon=True)
    thread.start()
    return RunningProxyServer(server=server, thread=thread)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Hybrid Autonomous AI local forward proxy")
    parser.add_argument("--config", required=True, help="Path to the proxy JSON config")
    parser.add_argument("--print-config", action="store_true", help="Print the normalized proxy config and exit")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    config = ProxyServerConfig.from_file(args.config)
    if args.print_config:
        print(json.dumps(config.to_payload(), indent=2, sort_keys=True))
        return 0
    with start_proxy_server(config) as running:
        print(f"proxy_url={running.proxy_url}")
        try:
            running.thread.join()
        except KeyboardInterrupt:
            return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
