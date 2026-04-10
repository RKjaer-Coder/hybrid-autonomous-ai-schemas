from __future__ import annotations

import re

from immune.types import BlockReason, ImmuneConfig, SheriffPayload

ENCODING_EXEMPT_FIELDS = {"image_data", "file_content", "binary_payload", "encoded_body", "attachment"}

BASE64_RE = re.compile(r"[A-Za-z0-9+/]{20,}={0,2}")
URL_ENC_RE = re.compile(r"%[0-9a-fA-F]{2}")
HEX_ENC_RE = re.compile(r"\\x[0-9a-fA-F]{2}")

INSTRUCTION_OVERRIDE_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"ignore\W*previous\W*instructions",
        r"i\s*g\s*n\s*o\s*r\s*e\W+previous",
        r"new\W+instructions\s*:",
        r"override\W+mode",
        r"forget\W+everything",
        r"disregard\W+all\W+prior",
        r"you\W+are\W+now",
        r"switch\W+to\W+mode|enter\W+maintenance\W+mode",
    ]
]
ROLE_IMPERSONATION_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"^\s*system\s*:",
        r"admin\W+message\s*:",
        r"speaking\W+as\W+the\W+operator",
        r"developer\W+override\s*:",
        r"system\W+note\s*:",
        r"from\W+the\W+administrator\s*:",
        r"authorized\W+by\W+anthropic",
    ]
]
EXFIL_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"send\W+this\W+data\W+to",
        r"curl\s+https?://",
        r"wget\s+https?://",
        r"post\W+to\W+endpoint",
        r"upload\W+to",
        r"base64\W+encode\W+and\W+send",
        r"exfiltrate|pipe\W+output\W+to",
    ]
]
HIDDEN_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"<!--\s*ignore.*?-->",
        r"<script[^>]*>",
        r"javascript:\s*",
        r"\x00",
        r"data:text/html",
        r"[\u200b\u200c\u200d]",
    ]
]
EXTRACT_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"show\W+your\W+prompt",
        r"what\W+are\W+your\W+instructions",
        r"repeat\W+your\W+rules",
        r"print\W+your\W+system\W+message",
        r"reveal\W+your\W+configuration",
        r"output\W+your\W+directives",
    ]
]




def _percent_decode(value: str) -> str:
    chars: list[str] = []
    i = 0
    while i < len(value):
        if i + 2 < len(value) and value[i] == "%":
            h = value[i + 1:i + 3]
            if re.fullmatch(r"[0-9a-fA-F]{2}", h):
                chars.append(chr(int(h, 16)))
                i += 3
                continue
        chars.append(value[i])
        i += 1
    return "".join(chars)
def _detect_patterns(text: str) -> str | None:
    groups = [
        ("instruction_override", INSTRUCTION_OVERRIDE_PATTERNS),
        ("role_impersonation", ROLE_IMPERSONATION_PATTERNS),
        ("exfiltration", EXFIL_PATTERNS),
        ("hidden", HIDDEN_PATTERNS),
        ("extraction", EXTRACT_PATTERNS),
    ]
    for name, patterns in groups:
        for pat in patterns:
            if pat.search(text):
                return name
    return None


def _iter_strings(data: object, prefix: str = "arguments") -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    stack: list[tuple[str, object]] = [(prefix, data)]
    while stack:
        path, value = stack.pop()
        if isinstance(value, str):
            result.append((path, value))
        elif isinstance(value, dict):
            for k, v in value.items():
                stack.append((f"{path}.{k}", v))
        elif isinstance(value, (list, tuple)):
            for i, v in enumerate(value):
                stack.append((f"{path}[{i}]", v))
    return result


def classify_ipi(
    payload: SheriffPayload,
    config: ImmuneConfig,
) -> tuple[BlockReason, str] | None:
    """Return IPI detection tuple or None. Pure and deterministic."""
    del config
    fields = _iter_strings(payload.arguments)
    if payload.raw_prompt:
        fields.append(("raw_prompt", payload.raw_prompt))

    for path, text in fields:
        category = _detect_patterns(text)
        if category:
            return (BlockReason.IPI_DETECTED, f"IPI detected ({category}) in {path}")

        if path.split(".")[-1] not in ENCODING_EXEMPT_FIELDS:
            enc_hits = len(URL_ENC_RE.findall(text)) * 3 + len(HEX_ENC_RE.findall(text)) * 4
            density = (enc_hits / len(text)) if text else 0.0
            if density > 0.3:
                return (BlockReason.IPI_DETECTED, f"Suspicious encoding density in {path}")

        for match in BASE64_RE.findall(text):
            try:
                decoded = __import__("base64").b64decode(match + "===", validate=False).decode(
                    "utf-8", errors="ignore"
                )
            except Exception:
                continue
            category = _detect_patterns(decoded)
            if category:
                return (BlockReason.IPI_DETECTED, f"IPI detected in base64 payload at {path}")
            if URL_ENC_RE.search(decoded):
                decoded2 = _percent_decode(decoded)
                category = _detect_patterns(decoded2)
                if category:
                    return (BlockReason.IPI_DETECTED, f"IPI detected in nested encoding at {path}")

        if URL_ENC_RE.search(text):
            decoded = _percent_decode(text)
            category = _detect_patterns(decoded)
            if category:
                return (BlockReason.IPI_DETECTED, f"IPI detected in url-encoded payload at {path}")

        if HEX_ENC_RE.search(text):
            decoded = HEX_ENC_RE.sub(lambda m: chr(int(m.group(0)[2:], 16)), text)
            category = _detect_patterns(decoded)
            if category:
                return (BlockReason.IPI_DETECTED, f"IPI detected in hex-encoded payload at {path}")

    return None


if __name__ == "__main__":
    print("ok")
