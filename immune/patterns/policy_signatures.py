from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple
import re
from urllib.parse import urlparse


@dataclass(frozen=True)
class EndpointAllowlist:
    """Immutable endpoint allowlist. Loaded at boot, never modified at runtime."""

    permitted_domains: frozenset[str]
    permitted_ports: frozenset[int]
    permitted_schemes: frozenset[str]

    def is_permitted(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme.lower() not in self.permitted_schemes:
            return False
        host = (parsed.hostname or "").lower()
        if host not in self.permitted_domains:
            return False
        port = parsed.port
        if port is None:
            port = 443 if parsed.scheme.lower() == "https" else 80
        return port in self.permitted_ports


CONSTRUCTION_ALLOWLIST = EndpointAllowlist(
    permitted_domains=frozenset({"localhost", "127.0.0.1", "::1"}),
    permitted_ports=frozenset({11434, 8080, 8443}),
    permitted_schemes=frozenset({"http", "https"}),
)

NETWORK_TOOLS: frozenset[str] = frozenset(
    {
        "web_fetch",
        "web_search",
        "shell_command",
        "http_request",
    }
)


@dataclass(frozen=True)
class ResourceLimits:
    """JWT-derived resource limits for the current session."""

    max_tool_calls: int = 500
    max_memory_writes: int = 100
    max_api_spend_usd: float = 0.00
    max_file_size_bytes: int = 10_485_760


@dataclass(frozen=True)
class ResourceUsage:
    """Current usage counters."""

    tool_calls: int = 0
    memory_writes: int = 0
    api_spend_usd: float = 0.0


def check_resource_limits(limits: ResourceLimits, usage: ResourceUsage) -> Optional[str]:
    if usage.tool_calls > limits.max_tool_calls:
        return f"Resource limit exceeded: max_tool_calls ({usage.tool_calls}>{limits.max_tool_calls})"
    if usage.memory_writes > limits.max_memory_writes:
        return f"Resource limit exceeded: max_memory_writes ({usage.memory_writes}>{limits.max_memory_writes})"
    if usage.api_spend_usd > limits.max_api_spend_usd:
        return f"Resource limit exceeded: max_api_spend_usd ({usage.api_spend_usd}>{limits.max_api_spend_usd})"
    return None


PRIVILEGED_TOOLS: frozenset[str] = frozenset(
    {
        "memory_write",
        "shell_command",
        "file_write",
        "system_config",
        "skill_install",
        "cron_schedule",
    }
)

TRUST_TIER_MINIMUM: dict[str, int] = {
    "memory_write": 2,
    "shell_command": 1,
    "file_write": 2,
    "system_config": 1,
    "skill_install": 1,
    "cron_schedule": 1,
}


def check_trust_tier(tool_name: str, source_trust_tier: int) -> Optional[str]:
    # In this repo, lower number means more trusted. Tier 4 is least trusted.
    required_max_tier = TRUST_TIER_MINIMUM.get(tool_name)
    if required_max_tier is None:
        return None
    if source_trust_tier > required_max_tier:
        return (
            f"Trust tier violation: {tool_name} requires tier <= {required_max_tier}, "
            f"received tier {source_trust_tier}"
        )
    return None


@dataclass(frozen=True)
class DenyRule:
    tool_name: str
    pattern: re.Pattern[str]
    description: str


DENY_RULES: List[DenyRule] = [
    DenyRule(
        "shell_command",
        re.compile(r"rm\s+(-[rRf]+\s+)*[/~](?:\s|$)", re.IGNORECASE),
        "Recursive delete from root or home",
    ),
    DenyRule(
        "shell_command",
        re.compile(r"chmod\s+[0-7]*777", re.IGNORECASE),
        "World-writable permissions",
    ),
    DenyRule(
        "shell_command",
        re.compile(r"(sudo|su\s+root|doas)\s", re.IGNORECASE),
        "Privilege escalation attempt",
    ),
    DenyRule(
        "file_write",
        re.compile(r"^/(etc|usr|var|sys|proc|boot)/", re.IGNORECASE),
        "Write to system directory",
    ),
    DenyRule(
        "file_write",
        re.compile(r"\.(ssh|gnupg|aws|kube)/", re.IGNORECASE),
        "Write to credential directory",
    ),
    DenyRule(
        "memory_write",
        re.compile(r"source_trust_tier.*[>].*claimed_tier", re.IGNORECASE),
        "Trust tier privilege escalation",
    ),
    DenyRule(
        "shell_command",
        re.compile(r"(mkfs|fdisk|dd\s+if=.*of=/dev)", re.IGNORECASE),
        "Disk destructive operation",
    ),
    DenyRule(
        "shell_command",
        re.compile(r"iptables|ufw|firewall-cmd", re.IGNORECASE),
        "Firewall modification",
    ),
]


def check_deny_rules(tool_name: str, arguments: str) -> Optional[Tuple[str, str]]:
    for rule in DENY_RULES:
        if rule.tool_name != tool_name:
            continue
        match = rule.pattern.search(arguments)
        if match:
            return (rule.description, match.group(0))
    return None
