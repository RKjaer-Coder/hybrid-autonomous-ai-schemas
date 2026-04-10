from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
import re
from typing import List, Tuple
from urllib.parse import unquote

from immune.patterns.ipi_patterns import IPICategory, check_ipi


@dataclass(frozen=True)
class EncodingDetection:
    encoding_type: str
    original_segment: str
    decoded_content: str
    field_name: str
    confidence: float


ENCODING_SAFE_FIELDS: frozenset[str] = frozenset(
    {
        "avatar",
        "thumbnail",
        "image",
        "icon",
        "photo",
        "binary",
        "file_content",
        "attachment",
        "data",
        "payload_binary",
    }
)

BASE64_RE = re.compile(r"(?<![A-Za-z0-9+/=])([A-Za-z0-9+/]{20,}={0,2})(?![A-Za-z0-9+/=])")
HEX_ESCAPED_RE = re.compile(r"(?:\\x[0-9a-fA-F]{2}){4,}")
HEX_RAW_RE = re.compile(r"\b[0-9a-fA-F]{16,}\b")
URL_BYTE_RE = re.compile(r"%[0-9a-fA-F]{2}")
ZERO_WIDTH_RE = re.compile(r"[\u200b\u200c\u200d\ufeff]")
RTL_RE = re.compile(r"[\u202e\u202d\u2066\u2067\u2068\u2069]")
CYR_GREEK_RE = re.compile(r"[\u0370-\u03FF\u0400-\u04FF]")
LATIN_RE = re.compile(r"[A-Za-z]")


def _decode_base64(candidate: str) -> str:
    padding = "=" * ((4 - len(candidate) % 4) % 4)
    decoded = base64.b64decode(candidate + padding, validate=False)
    return decoded.decode("utf-8", errors="ignore")


def detect_encodings(text: str, field_name: str = "") -> List[EncodingDetection]:
    if field_name in ENCODING_SAFE_FIELDS:
        return []

    detections: list[EncodingDetection] = []

    for match in BASE64_RE.finditer(text):
        segment = match.group(1)
        try:
            decoded = _decode_base64(segment)
        except (ValueError, binascii.Error):
            continue
        if len(decoded) >= 8:
            entropy_boost = min(0.4, len(segment) / 200)
            detections.append(EncodingDetection("base64", segment, decoded, field_name, 0.55 + entropy_boost))

    for match in HEX_ESCAPED_RE.finditer(text):
        segment = match.group(0)
        try:
            decoded = bytes.fromhex(segment.replace("\\x", "")).decode("utf-8", errors="ignore")
        except ValueError:
            continue
        detections.append(EncodingDetection("hex", segment, decoded, field_name, 0.82))

    for match in HEX_RAW_RE.finditer(text):
        segment = match.group(0)
        if len(segment) % 2 != 0:
            continue
        try:
            decoded = bytes.fromhex(segment).decode("utf-8", errors="ignore")
        except ValueError:
            continue
        if decoded.strip():
            detections.append(EncodingDetection("hex", segment, decoded, field_name, 0.70))

    url_hits = URL_BYTE_RE.findall(text)
    if text and (len(url_hits) * 3 / len(text)) > 0.30:
        decoded = unquote(text)
        detections.append(EncodingDetection("url_encoding", text, decoded, field_name, 0.88))

    zw_count = len(ZERO_WIDTH_RE.findall(text))
    rtl_count = len(RTL_RE.findall(text))
    cyr_greek = len(CYR_GREEK_RE.findall(text))
    latin = len(LATIN_RE.findall(text))
    homoglyph_density = (cyr_greek / max(1, latin + cyr_greek))
    if zw_count > 0 or rtl_count > 0 or (latin > 6 and homoglyph_density > 0.12):
        confidence = min(1.0, 0.45 + (zw_count * 0.08) + (rtl_count * 0.2) + homoglyph_density)
        detections.append(EncodingDetection("unicode_tricks", text, text, field_name, confidence))

    return detections


def decode_and_recheck(
    detections: List[EncodingDetection],
) -> List[Tuple[IPICategory, str, EncodingDetection]]:
    matches: list[Tuple[IPICategory, str, EncodingDetection]] = []
    for detection in detections:
        for category, description in check_ipi(detection.decoded_content):
            matches.append((category, description, detection))
    return matches
