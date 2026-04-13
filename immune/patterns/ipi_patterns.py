from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, unique
import re
from typing import List, Tuple


@unique
class IPICategory(Enum):
    INSTRUCTION_OVERRIDE = "instruction_override"
    ROLE_IMPERSONATION = "role_impersonation"
    EXFILTRATION_ATTEMPT = "exfiltration_attempt"
    HIDDEN_INSTRUCTIONS = "hidden_instructions"
    PROMPT_EXTRACTION = "prompt_extraction"


@dataclass(frozen=True)
class IPIPattern:
    category: IPICategory
    pattern: re.Pattern[str]
    description: str
    severity: str  # "high" or "critical"


ZERO_WIDTH = "[\u200b\u200c\u200d\ufeff]*"
DELIMS = r"[.\-_\|\s]*"


def _spaced_word(word: str) -> str:
    return r"\s*".join(re.escape(ch) for ch in word)


def _delim_word(word: str) -> str:
    return DELIMS.join(re.escape(ch) for ch in word)


def _homoglyph_word(word: str) -> str:
    mapping = {
        "a": "[aаａ]",
        "e": "[eеｅ]",
        "o": "[oоｏ]",
        "i": "[iіｉ]",
        "s": "[sѕｓ]",
        "r": "[rｒ]",
        "p": "[pрｐ]",
        "v": "[vｖ]",
        "u": "[uｕ]",
    }
    parts: list[str] = []
    for ch in word.lower():
        token = mapping.get(ch, re.escape(ch))
        parts.append(token)
    return ZERO_WIDTH.join(parts)


def _compile_all() -> List[IPIPattern]:
    patterns: list[IPIPattern] = []

    # Instruction override
    override_literals = [
        r"\b(ignore\s+previous|new\s+instructions\s*:|override\s+mode|forget\s+everything|disregard\s+above|cancel\s+prior|reset\s+instructions)\b",
        r"\bignore\s+(all\s+)?previous\s+instructions\b",
        r"\b(disregard|cancel|replace)\s+(all\s+)?prior\s+(rules|instructions)\b",
        r"\bfrom\s+now\s+on\s+you\s+must\b",
        r"\bf\s*o\s*r\s*g\s*e\s*t\s+e\s*v\s*e\s*r\s*y\s*t\s*h\s*i\s*n\s*g\b",
    ]
    for idx, expr in enumerate(override_literals, 1):
        patterns.append(IPIPattern(IPICategory.INSTRUCTION_OVERRIDE, re.compile(expr, re.IGNORECASE), f"Instruction override literal #{idx}", "critical"))

    spaced_phrases = ["ignore previous", "new instructions", "forget everything", "reset instructions"]
    for phrase in spaced_phrases:
        words = phrase.split()
        expr = r"\b" + r"\s+".join(_spaced_word(w) for w in words) + r"\b"
        patterns.append(IPIPattern(IPICategory.INSTRUCTION_OVERRIDE, re.compile(expr, re.IGNORECASE), f"Instruction override whitespace variant: {phrase}", "critical"))

    homoglyph_phrases = ["ignore previous", "override mode", "new instructions", "disregard above"]
    for phrase in homoglyph_phrases:
        words = phrase.split()
        expr = r"\b" + r"\s+".join(_homoglyph_word(w) for w in words) + r"\b"
        patterns.append(IPIPattern(IPICategory.INSTRUCTION_OVERRIDE, re.compile(expr, re.IGNORECASE), f"Instruction override homoglyph variant: {phrase}", "critical"))

    patterns.append(IPIPattern(IPICategory.INSTRUCTION_OVERRIDE, re.compile(r"ign[oо]re\s+previ[oо]us", re.IGNORECASE), "Instruction override explicit homoglyph o", "critical"))

    delim_phrases = ["ignore previous", "new instructions", "override mode", "reset instructions"]
    for phrase in delim_phrases:
        expr = DELIMS.join(_delim_word(word) for word in phrase.split())
        patterns.append(IPIPattern(IPICategory.INSTRUCTION_OVERRIDE, re.compile(expr, re.IGNORECASE), f"Instruction override delimiter variant: {phrase}", "critical"))

    # Role impersonation
    role_variants = [
        r"(?m)^\s*(system|admin|developer|operator)\s*:\s*(do|execute|run|ignore|override|change|set|enable|disable)\b",
        r"(?m)^\s*admin\s+message\s*:\s*(do|execute|run|ignore|override|change|set|enable|disable)\b",
        r"(?m)^\s*developer\s+override\s*:\s*(do|execute|run|ignore|override|change|set|enable|disable)\b",
        r"(?i)\bfrom\s+the\s+administrator\s*:\s*(do|execute|run|ignore|override|change|set|enable|disable)\b",
        r"(?m)^\s*\[(SYSTEM|ADMIN|DEVELOPER|OPERATOR)\]\s*(do|execute|run|ignore|override|change|set|enable|disable)\b",
        r"(?m)^\s*<<\s*SYS\s*>>\s*(do|execute|run|ignore|override|change|set|enable|disable)\b",
        r"(?i)\b(speaking\s+as\s+the\s+operator|this\s+is\s+a\s+system\s+message|admin\s+override\s+activated|developer\s+mode\s+enabled|i\s+am\s+the\s+system\s+administrator)\b",
        r"(?is)(\*\*\s*SYSTEM\s+MESSAGE\s*:?\*\*|#\s*admin\s+override|<div[^>]*class=[\"']system[\"'][^>]*>)",
    ]
    for idx, expr in enumerate(role_variants, 1):
        patterns.append(IPIPattern(IPICategory.ROLE_IMPERSONATION, re.compile(expr, re.IGNORECASE), f"Role impersonation variant #{idx}", "critical"))

    # Exfiltration
    exfil_variants = [
        r"\b(send\s+this\s+to|post\s+data\s+to|upload\s+to|forward\s+to|exfiltrate|transmit\s+to)\b.{0,120}(https?://|ftp://|[\w.+-]+@[\w.-]+\.[a-z]{2,})",
        r"\b(send\s+this\s+data\s+to|send\s+data\s+to|exfiltrate\s+to)\b.{0,40}\b[a-z0-9.-]+\b",
        r"\b(curl|wget|nc|netcat|scp|rsync)\b.{0,120}(https?://|ftp://|\b\d{1,3}(?:\.\d{1,3}){3}\b|[a-z0-9.-]+\.[a-z]{2,})",
        r"\bpipe\s+output\s+to\b.{0,80}(https?://|ftp://|[a-z0-9.-]+\.[a-z]{2,})",
        r"(\|\s*curl\b|\|\s*nc\b)",
        r"\b(base64\s+encode|btoa\(|hex\s+encode)\b.{0,120}\b(password|secret|key|token|credential|config)\b",
        r"\b(nslookup|dig|host)\b\s+[A-Za-z0-9+/=_-]{8,}\.[a-z0-9.-]+",
        r"\?[a-z0-9_]*(data|payload|secret|token|key)=[A-Za-z0-9%+/=_-]{8,}",
    ]
    for idx, expr in enumerate(exfil_variants, 1):
        patterns.append(IPIPattern(IPICategory.EXFILTRATION_ATTEMPT, re.compile(expr, re.IGNORECASE), f"Exfiltration variant #{idx}", "high"))

    # Hidden instructions
    hidden_variants = [
        r"<!--.*?-->",
        r"<script\b[^>]*>.*?</script>",
        r"\bon(?:load|error|click)\s*=",
        r"javascript:\s*",
        r"data:text/html",
        r"(\\x00|%00|\x00)",
        r"[\u200b\u200c\u200d\ufeff\u202e]",
        r"(?:[A-Za-z0-9+/]{20,}={0,2}|(?:\\x[0-9a-fA-F]{2}){4,}|(?:%[0-9a-fA-F]{2}){6,}).{0,120}\b(ignore|override|instruction|system\s+prompt)\b",
    ]
    for idx, expr in enumerate(hidden_variants, 1):
        patterns.append(IPIPattern(IPICategory.HIDDEN_INSTRUCTIONS, re.compile(expr, re.IGNORECASE | re.DOTALL), f"Hidden instruction variant #{idx}", "high"))

    # Prompt extraction
    extract_variants = [
        r"\b(show(\s+me)?\s+your\s+(complete\s+)?(system\s+)?prompt|what\s+are\s+your\s+instructions|repeat\s+your\s+rules|display\s+your\s+system\s+message|print\s+your\s+configuration|output\s+your\s+(prompt|directives))\b",
        r"\b(start\s+your\s+response\s+by\s+repeating\s+your\s+(initial\s+)?instructions|start\s+your\s+response\s+with\s+your\s+system\s+prompt|translate\s+your\s+(system\s+message|instructions)\s+into?\s+\w+|write\s+a\s+poem\s+using\s+your\s+rules|summarize\s+your\s+directives)\b",
        r"\b(pretend\s+you\s+have\s+no\s+rules|act\s+as\s+if\s+you\s+had\s+no\s+system\s+prompt|imagine\s+your\s+instructions\s+say|in\s+a\s+world\s+where\s+your\s+prompt\s+is)\b",
        r"(echo\s+\$SYSTEM_PROMPT|cat\s+/system/prompt|process\.env\.INSTRUCTIONS|getattr\(self\s*,\s*['\"]system_prompt['\"]\))",
    ]
    for idx, expr in enumerate(extract_variants, 1):
        patterns.append(IPIPattern(IPICategory.PROMPT_EXTRACTION, re.compile(expr, re.IGNORECASE), f"Prompt extraction variant #{idx}", "high"))

    return patterns


ALL_PATTERNS: List[IPIPattern] = _compile_all()


def check_ipi(text: str) -> List[Tuple[IPICategory, str]]:
    """Check text against all IPI patterns. Returns list of (category, description) matches."""
    return [(p.category, p.description) for p in ALL_PATTERNS if p.pattern.search(text)]
