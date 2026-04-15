from immune.context_params import KNOWN_BAD_TRACES, ContextParams, check_context_params


def test_exfil_sequence_detected():
    out = check_context_params(ContextParams(tool_window=("memory_read", "config_read", "web_fetch")))
    assert out is not None
    assert out[0] == "EXFIL_SEQUENCE"


def test_burst_invocation_detected():
    out = check_context_params(ContextParams(tool_window=("a", "b", "c"), session_age_seconds=5.0))
    assert out is not None
    assert out[0] == "BURST_INVOCATION"


def test_normal_window_passes():
    assert check_context_params(ContextParams(tool_window=("council_call", "memory_write"), session_age_seconds=60.0)) is None


def test_trace_anomaly_detected():
    KNOWN_BAD_TRACES.add("bad-trace")
    try:
        out = check_context_params(ContextParams(execution_trace_hash="bad-trace"))
    finally:
        KNOWN_BAD_TRACES.discard("bad-trace")
    assert out is not None
    assert out[0] == "TRACE_ANOMALY"
