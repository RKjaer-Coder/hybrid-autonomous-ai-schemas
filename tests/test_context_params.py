from immune.context_params import ContextParams, check_context_params


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
