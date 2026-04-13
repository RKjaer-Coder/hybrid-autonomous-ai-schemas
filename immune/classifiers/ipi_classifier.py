from __future__ import annotations

from immune.patterns.encoding_detector import decode_and_recheck, detect_encodings
from immune.patterns.false_positive_allowlist import is_allowlisted
from immune.patterns.ipi_patterns import check_ipi
from immune.types import BlockReason, ImmuneConfig, SheriffPayload

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
        if len(text) > 20_000 and not any(token in text.lower() for token in ("ignore", "system", "prompt", "%", "\\x", "<script", "curl", "wget")):
            continue
        raw_matches = check_ipi(text)
        if raw_matches:
            field_name = path.split(".", 1)[0]
            if is_allowlisted(text, field_name=field_name):
                continue
            return (BlockReason.IPI_DETECTED, f"IPI detected ({raw_matches[0][0].value}) in {path}")

        field_name = path.rsplit(".", 1)[-1]
        detections = detect_encodings(text, field_name=field_name)
        if detections:
            decoded_matches = decode_and_recheck(detections)
            if decoded_matches:
                category, _, detection = decoded_matches[0]
                return (
                    BlockReason.IPI_DETECTED,
                    f"IPI detected in {detection.encoding_type} payload ({category.value}) at {path}",
                )
            suspicious_density = next(
                (d for d in detections if d.encoding_type == "url_encoding" and d.confidence >= 0.9),
                None,
            )
            if suspicious_density is not None:
                return (
                    BlockReason.IPI_DETECTED,
                    f"Suspicious high-density URL encoding detected at {path}",
                )

    return None


if __name__ == "__main__":
    print("ok")
