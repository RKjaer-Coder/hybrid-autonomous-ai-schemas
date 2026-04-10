from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ContextParams:
    execution_trace_hash: str | None = None
    tool_window: tuple[str, ...] = ()
    session_age_seconds: float = 0.0


DETECTION_RULES = {
    "EXFIL_SEQUENCE": {
        "trigger": "tool_window matches [*_read, *_read, web_fetch|shell_command(curl|wget)]",
        "action": "IMMUNE_BLOCK_FAST + SECURITY_ALERT + JWT revoke",
    },
    "BURST_INVOCATION": {
        "trigger": "session_age_seconds < 10 AND len(tool_window) == 3",
        "action": "IMMUNE_BLOCK_FAST + throttle to 1 call/5s",
    },
    "TRACE_ANOMALY": {
        "trigger": "execution_trace_hash in known_bad_traces",
        "action": "Flag for deep-scan (async)",
    },
}


def check_context_params(context: ContextParams) -> None:
    """Stage-2 stub checker that currently always passes."""
    del context
    return None


if __name__ == "__main__":
    print(check_context_params(ContextParams()))
