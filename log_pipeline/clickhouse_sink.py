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
BEHAVIOR_COLUMNS = [
    "window",
    "host",
    "client_ip",
    "request_count",
    "unique_path_count",
    "top_paths",
    "method_counts",
    "status_2xx",
    "status_3xx",
    "status_4xx",
    "status_5xx",
    "status_other",
    "total_bytes",
    "avg_bytes",
    "first_seen",
    "last_seen",
    "ai_analyzed",
    "ai_result",
]
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
CREATE_BEHAVIOR_SQL = """
CREATE TABLE IF NOT EXISTS user_behavior_windows (
    window DateTime,
    host LowCardinality(String),
    client_ip String,
    request_count UInt32,
    unique_path_count UInt32,
    top_paths String,
    method_counts String,
    status_2xx UInt32,
    status_3xx UInt32,
    status_4xx UInt32,
    status_5xx UInt32,
    status_other UInt32,
    total_bytes UInt64,
    avg_bytes Float64,
    first_seen DateTime,
    last_seen DateTime,
    ai_analyzed UInt8,
    ai_result String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (window, host, client_ip)
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
        self.behavior_buffer = deque()
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

    def enqueue_behavior(self, records: List[Dict[str, Any]]):
        if not self.available or not records:
            return
        with self.lock:
            self.behavior_buffer.extend(records)

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
        self.client.command(CREATE_BEHAVIOR_SQL)
        self.schema_ready = True
        self.logger.info("✅ ClickHouse 表结构检查完成")

    def _start_flusher(self):
        def loop():
            while True:
                time.sleep(3)
                if not self.buffer and not self.behavior_buffer:
                    continue
                with self.lock:
                    batch = list(self.buffer)
                    self.buffer.clear()
                    behavior_batch = list(self.behavior_buffer)
                    self.behavior_buffer.clear()
                try:
                    if batch:
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
                        self.logger.info(f"📊 写入 ClickHouse converged_logs {len(batch)} 条")

                    if behavior_batch:
                        behavior_rows = [
                            [
                                parse_ch_datetime(row.get("window")),
                                row.get("host", "unknown"),
                                row.get("client_ip", ""),
                                int(row.get("request_count", 0)),
                                int(row.get("unique_path_count", 0)),
                                row.get("top_paths", "[]"),
                                row.get("method_counts", "{}"),
                                int(row.get("status_2xx", 0)),
                                int(row.get("status_3xx", 0)),
                                int(row.get("status_4xx", 0)),
                                int(row.get("status_5xx", 0)),
                                int(row.get("status_other", 0)),
                                int(row.get("total_bytes", 0)),
                                float(row.get("avg_bytes", 0.0)),
                                parse_ch_datetime(row.get("first_seen")),
                                parse_ch_datetime(row.get("last_seen")),
                                int(row.get("ai_analyzed", 0)),
                                row.get("ai_result", "{}"),
                            ]
                            for row in behavior_batch
                        ]
                        self.client.insert("user_behavior_windows", behavior_rows, column_names=BEHAVIOR_COLUMNS)
                        self.logger.info(f"📊 写入 ClickHouse user_behavior_windows {len(behavior_batch)} 条")
                except Exception as error:
                    if "UNKNOWN_TABLE" in str(error):
                        try:
                            self._ensure_schema(force=True)
                            if batch:
                                self.client.insert("converged_logs", rows, column_names=CONVERGED_COLUMNS)
                            if behavior_batch:
                                self.client.insert("user_behavior_windows", behavior_rows, column_names=BEHAVIOR_COLUMNS)
                            self.logger.info("📊 写入 ClickHouse完成 (schema recovered)")
                            continue
                        except Exception as retry_error:
                            self.logger.error(f"❌ CH 重试写入失败: {retry_error}")
                    self.logger.error(f"❌ CH 写入失败: {error}")

        threading.Thread(target=loop, daemon=True).start()
