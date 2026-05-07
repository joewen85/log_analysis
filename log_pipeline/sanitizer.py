import re
from typing import Iterable, Tuple


DEFAULT_SENSITIVE_PATTERNS = [
    (r"\b\d{1,3}(?:\.\d{1,3}){3}\b", "[IP]"),
    (r"password[\s:=]+[^\s,;]+", "password=[MASKED]"),
    (r"token[\s:=]+[^\s,;]+", "token=[MASKED]"),
    (r"key[\s:=]+[^\s,;]+", "key=[MASKED]"),
]


class Sanitizer:
    def __init__(self, patterns: Iterable[Tuple[str, str]] = DEFAULT_SENSITIVE_PATTERNS):
        self.patterns = list(patterns)

    def sanitize(self, text: str) -> str:
        sanitized = text
        for pattern, replacement in self.patterns:
            sanitized = re.sub(pattern, replacement, sanitized, flags=re.IGNORECASE)
        return sanitized
