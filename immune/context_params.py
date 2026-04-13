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


def check_context_params(context: ContextParams) -> tuple[str, str] | None:
    """Check context parameters for suspicious patterns."""
    tool_window = context.tool_window
    if len(tool_window) >= 3:
        reads = all("_read" in name for name in tool_window[-3:-1])
        exfil = any(token in tool_window[-1] for token in ("web_fetch", "shell_command", "curl", "wget"))
        if reads and exfil:
            return ("EXFIL_SEQUENCE", DETECTION_RULES["EXFIL_SEQUENCE"]["action"])

    if context.session_age_seconds < 10.0 and len(tool_window) >= 3:
        return ("BURST_INVOCATION", DETECTION_RULES["BURST_INVOCATION"]["action"])

    return None


if __name__ == "__main__":
    print(check_context_params(ContextParams()))
