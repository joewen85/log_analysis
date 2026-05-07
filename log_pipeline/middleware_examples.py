import logging
import re
from typing import Any, Dict, Optional, Set

from .middleware import WorkerMiddleware


class HostAllowlistAuditMiddleware(WorkerMiddleware):
    def __init__(self, allowed_hosts: Optional[Set[str]], logger: logging.Logger, audit_enabled: bool = True):
        self.allowed_hosts = allowed_hosts or set()
        self.logger = logger
        self.audit_enabled = audit_enabled
        self.total_seen = 0
        self.total_dropped = 0
        self.total_processed = 0

    def before_decode(self, raw_message: bytes) -> bytes:
        self.total_seen += 1
        return raw_message

    def after_decode(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        host = str(payload.get("host", "unknown"))
        if self.allowed_hosts and host not in self.allowed_hosts:
            self.total_dropped += 1
            if self.audit_enabled:
                self.logger.info(f"🧹 中间件过滤日志 host={host}")
            return None
        return payload

    def on_process_success(self, payload: Dict[str, Any]):
        self.total_processed += 1
        if self.audit_enabled and self.total_processed % 100 == 0:
            self.logger.info(
                "📌 中间件审计统计 "
                f"seen={self.total_seen} processed={self.total_processed} dropped={self.total_dropped}"
            )

    def on_process_error(self, raw_message: bytes, error: Exception):
        if self.audit_enabled:
            preview = raw_message[:200].decode("utf-8", errors="ignore")
            self.logger.warning(f"⚠️ 中间件捕获处理异常: {error}; raw_preview={preview}")


class RegexDenylistAuditMiddleware(WorkerMiddleware):
    def __init__(self, deny_patterns: Set[str], logger: logging.Logger, audit_enabled: bool = True):
        self.logger = logger
        self.audit_enabled = audit_enabled
        self.total_seen = 0
        self.total_dropped = 0
        self.total_processed = 0
        self.compiled_patterns = []
        for pattern in deny_patterns:
            try:
                self.compiled_patterns.append(re.compile(pattern, flags=re.IGNORECASE))
            except re.error as error:
                self.logger.warning(f"⚠️ 无效 deny regex 已忽略: {pattern}; error={error}")

    def before_decode(self, raw_message: bytes) -> bytes:
        self.total_seen += 1
        return raw_message

    def after_decode(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.compiled_patterns:
            return payload
        message = str(payload.get("message", ""))
        for pattern in self.compiled_patterns:
            if pattern.search(message):
                self.total_dropped += 1
                if self.audit_enabled:
                    self.logger.info(f"🧹 正则黑名单过滤日志 pattern={pattern.pattern}")
                return None
        return payload

    def on_process_success(self, payload: Dict[str, Any]):
        self.total_processed += 1
        if self.audit_enabled and self.total_processed % 100 == 0:
            self.logger.info(
                "📌 黑名单中间件统计 "
                f"seen={self.total_seen} processed={self.total_processed} dropped={self.total_dropped}"
            )

    def on_process_error(self, raw_message: bytes, error: Exception):
        if self.audit_enabled:
            preview = raw_message[:200].decode("utf-8", errors="ignore")
            self.logger.warning(f"⚠️ 黑名单中间件捕获处理异常: {error}; raw_preview={preview}")
