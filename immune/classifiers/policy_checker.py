from __future__ import annotations

import re

from immune.types import BlockReason, ImmuneConfig, SheriffPayload

NETWORK_TOOLS = {"web_fetch", "web_search", "shell_command", "http_request", "api_call"}
PRIVILEGED_TOOLS = {"system_config", "key_management", "agent_spawn", "memory_write_trusted"}
URL_RE = re.compile(r"https?://([^\s/]+)", re.IGNORECASE)
DANGEROUS_SHELL = ["rm -rf /", "rm -rf ~", "mkfs", "dd if=", ":(){:|:&};:"]


def _extract_hostname(args: dict, tool_name: str) -> str | None:
    for key in ("url", "endpoint", "target", "href"):
        value = args.get(key)
        if isinstance(value, str) and "://" in value:
            host_match = re.search(r"https?://([^/:?#]+)", value, re.IGNORECASE)
            return (host_match.group(1).lower() if host_match else None)
    if tool_name == "shell_command":
        cmd = args.get("command")
        if isinstance(cmd, str):
            m = URL_RE.search(cmd)
            if m:
                return m.group(1).lower()
    return None


def check_policy(
    payload: SheriffPayload,
    config: ImmuneConfig,
) -> tuple[BlockReason, str] | None:
    """Return policy violation tuple or None."""
    args = payload.arguments
    if payload.tool_name in NETWORK_TOOLS:
        host = _extract_hostname(args, payload.tool_name)
        if host and host not in config.permitted_endpoints:
            return (BlockReason.POLICY_VIOLATION, f"Endpoint not in allowlist: {host}")

    claims = payload.jwt_claims or {}
    limits = [
        ("max_tool_calls", "current_tool_calls"),
        ("max_memory_writes", "current_memory_writes"),
        ("max_api_spend_usd", "current_spend_usd"),
    ]
    for limit, current in limits:
        if limit in claims and current in claims and float(claims[current]) > float(claims[limit]):
            return (BlockReason.POLICY_VIOLATION, f"Resource limit exceeded: {limit}")

    if payload.source_trust_tier == 4:
        if payload.tool_name == "memory_write" and int(args.get("target_tier", 4)) < 4:
            return (
                BlockReason.TRUST_TIER_VIOLATION,
                f"Untrusted source cannot invoke: {payload.tool_name}",
            )
        if payload.tool_name == "shell_command" and not bool(args.get("burner_room")):
            return (
                BlockReason.TRUST_TIER_VIOLATION,
                f"Untrusted source cannot invoke: {payload.tool_name}",
            )
        if payload.tool_name in PRIVILEGED_TOOLS:
            return (
                BlockReason.TRUST_TIER_VIOLATION,
                f"Untrusted source cannot invoke: {payload.tool_name}",
            )

    if payload.tool_name == "shell_command":
        cmd = str(args.get("command", ""))
        for pattern in DANGEROUS_SHELL:
            if pattern in cmd:
                return (BlockReason.POLICY_VIOLATION, f"Dangerous tool pattern: {pattern}")
    if payload.tool_name == "file_write":
        path = str(args.get("path", ""))
        if path.startswith(("/etc/", "/usr/", "/var/", "/System/")):
            return (BlockReason.POLICY_VIOLATION, "Dangerous tool pattern: protected_path")
    if payload.tool_name == "memory_write":
        claimed = int(args.get("claimed_tier", payload.source_trust_tier))
        if claimed > payload.source_trust_tier:
            return (BlockReason.POLICY_VIOLATION, "Dangerous tool pattern: privilege_escalation")

    return None


if __name__ == "__main__":
    print("ok")
