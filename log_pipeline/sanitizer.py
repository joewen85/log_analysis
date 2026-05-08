import re
from typing import Iterable, List, Tuple


DEFAULT_SENSITIVE_PATTERNS = [
    (r"\b\d{1,3}(?:\.\d{1,3}){3}\b", "[IP]"),
    (r"password[\s:=]+[^\s,;]+", "password=[MASKED]"),
    (r"token[\s:=]+[^\s,;]+", "token=[MASKED]"),
    (r"key[\s:=]+[^\s,;]+", "key=[MASKED]"),
]


class Sanitizer:
    def __init__(
        self,
        patterns: Iterable[Tuple[str, str]] = DEFAULT_SENSITIVE_PATTERNS,
        enabled: bool = True,
        mask_ip: bool = True,
        mask_credentials: bool = True,
        extra_rules: str = "",
    ):
        self.enabled = enabled
        self.patterns = self._build_patterns(list(patterns), mask_ip, mask_credentials, extra_rules)

    @staticmethod
    def _build_patterns(
        base_patterns: List[Tuple[str, str]],
        mask_ip: bool,
        mask_credentials: bool,
        extra_rules: str,
    ) -> List[Tuple[str, str]]:
        selected: List[Tuple[str, str]] = []
        for pattern, replacement in base_patterns:
            if pattern == r"\b\d{1,3}(?:\.\d{1,3}){3}\b" and not mask_ip:
                continue
            if (
                pattern in (r"password[\s:=]+[^\s,;]+", r"token[\s:=]+[^\s,;]+", r"key[\s:=]+[^\s,;]+")
                and not mask_credentials
            ):
                continue
            selected.append((pattern, replacement))

        for raw_rule in (extra_rules or "").split("||"):
            rule = raw_rule.strip()
            if not rule:
                continue
            if "=>" not in rule:
                continue
            pattern, replacement = rule.split("=>", 1)
            pattern = pattern.strip()
            replacement = replacement.strip()
            if not pattern:
                continue
            selected.append((pattern, replacement))
        return selected

    def sanitize(self, text: str) -> str:
        if not self.enabled:
            return text
        sanitized = text
        for pattern, replacement in self.patterns:
            sanitized = re.sub(pattern, replacement, sanitized, flags=re.IGNORECASE)
        return sanitized
