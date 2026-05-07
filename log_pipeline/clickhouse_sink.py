import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import clickhouse_connect

from .config import AppConfig

CONVERGED_COLUMNS = ["window", "host", "event_pattern", "level", "count", "first_seen", "last_seen", "samples", "ai_analyzed", "ai_result"]
ALERT_COLUMNS = ["host", "pattern", "summary", "webhook_status"]
CREATE_CONVERGED_SQL = """
CREATE TABLE IF NOT EXISTS converged_logs (
    window DateTime,
    host LowCardinality(String),
    event_pattern LowCardinality(String),
    level LowCardinality(String),
    count UInt32,
    first_seen DateTime,
    last_seen DateTime,
    samples String,
    ai_analyzed UInt8,
    ai_result String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (window, host, event_pattern)
"""
CREATE_ALERT_SQL = """
CREATE TABLE IF NOT EXISTS alert_history (
    alert_time DateTime DEFAULT now(),
    host LowCardinality(String),
    pattern LowCardinality(String),
    summary String,
    webhook_status UInt8
) ENGINE = MergeTree() ORDER BY alert_time
"""


def parse_ch_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc)
        return value.replace(tzinfo=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return datetime.utcnow().replace(tzinfo=timezone.utc)
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc)
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.utcnow().replace(tzinfo=timezone.utc)


class ClickHouseSink:
    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.client = None
        self.buffer = deque()
        self.lock = threading.Lock()
        self.schema_ready = False
        self._init_client()

    def _init_client(self):
        try:
            self.client = clickhouse_connect.get_client(
                host=self.config.clickhouse_host,
                port=self.config.clickhouse_port,
                database=self.config.clickhouse_db,
                username=self.config.clickhouse_user,
                password=self.config.clickhouse_password,
            )
            self._ensure_schema()
            self._start_flusher()
            self.logger.info("✅ ClickHouse 写入通道初始化成功")
        except Exception as error:
            self.logger.error(f"❌ ClickHouse 初始化失败: {error}")
            self.client = None

    @property
    def available(self) -> bool:
        return self.client is not None

    def enqueue_converged(self, records: List[Dict[str, Any]]):
        if not self.available or not records:
            return
        with self.lock:
            self.buffer.extend(records)

    def insert_alert(self, host: str, pattern: str, summary: str, status: int):
        if not self.available:
            return
        try:
            with self.lock:
                self.client.insert(
                    "alert_history",
                    [[host, pattern, summary, status]],
                    column_names=ALERT_COLUMNS,
                )
        except Exception as error:
            if "UNKNOWN_TABLE" in str(error):
                self._ensure_schema(force=True)
                try:
                    with self.lock:
                        self.client.insert(
                            "alert_history",
                            [[host, pattern, summary, status]],
                            column_names=ALERT_COLUMNS,
                        )
                    return
                except Exception as retry_error:
                    self.logger.error(f"❌ CH 告警重试写入失败: {retry_error}")
            self.logger.error(f"❌ CH 告警写入失败: {error}")

    def _ensure_schema(self, force: bool = False):
        if not self.available:
            return
        if self.schema_ready and not force:
            return
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.config.clickhouse_db}")
        self.client.command(CREATE_CONVERGED_SQL)
        self.client.command(CREATE_ALERT_SQL)
        self.schema_ready = True
        self.logger.info("✅ ClickHouse 表结构检查完成")

    def _start_flusher(self):
        def loop():
            while True:
                time.sleep(3)
                if not self.buffer:
                    continue
                with self.lock:
                    batch = list(self.buffer)
                    self.buffer.clear()
                try:
                    rows = [
                        [
                            parse_ch_datetime(row.get("window")),
                            row.get("host", "unknown"),
                            row.get("event_pattern", ""),
                            row.get("level", "INFO"),
                            int(row.get("count", 0)),
                            parse_ch_datetime(row.get("first_seen")),
                            parse_ch_datetime(row.get("last_seen")),
                            row.get("samples", "[]"),
                            int(row.get("ai_analyzed", 0)),
                            row.get("ai_result", "{}"),
                        ]
                        for row in batch
                    ]
                    self.client.insert("converged_logs", rows, column_names=CONVERGED_COLUMNS)
                    self.logger.info(f"📊 写入 ClickHouse {len(batch)} 条")
                except Exception as error:
                    if "UNKNOWN_TABLE" in str(error):
                        try:
                            self._ensure_schema(force=True)
                            self.client.insert("converged_logs", rows, column_names=CONVERGED_COLUMNS)
                            self.logger.info(f"📊 写入 ClickHouse {len(batch)} 条 (schema recovered)")
                            continue
                        except Exception as retry_error:
                            self.logger.error(f"❌ CH 重试写入失败: {retry_error}")
                    self.logger.error(f"❌ CH 写入失败: {error}")

        threading.Thread(target=loop, daemon=True).start()
