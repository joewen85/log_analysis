import json
import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Producer

from .ai_analyzer import AIAnalyzer
from .clickhouse_sink import ClickHouseSink
from .config import AppConfig
from .interfaces import AIAnalyzerProtocol, NotifierProtocol, StorageSinkProtocol, TemplateExtractorProtocol
from .notifier import WebhookNotifier
from .sanitizer import Sanitizer
from .template_extractor import TemplateExtractor


class LogConverger:
    @staticmethod
    def _new_buffer_entry() -> Dict[str, Any]:
        return {"count": 0, "first_ts": None, "last_ts": None, "samples": []}

    def __init__(
        self,
        config: AppConfig,
        logger: logging.Logger,
        sanitizer: Optional[Sanitizer] = None,
        template_extractor: Optional[TemplateExtractorProtocol] = None,
        ai_analyzer: Optional[AIAnalyzerProtocol] = None,
        storage_sink: Optional[StorageSinkProtocol] = None,
        notifier: Optional[NotifierProtocol] = None,
        kafka_producer: Optional[Producer] = None,
    ):
        self.config = config
        self.logger = logger
        self.sanitizer = sanitizer or Sanitizer()
        self.template_extractor = template_extractor or TemplateExtractor(config, logger)
        self.ai_analyzer = ai_analyzer
        if self.ai_analyzer is None and self.config.ai_analysis_enabled:
            self.ai_analyzer = AIAnalyzer(config, logger)
        if not self.config.ai_analysis_enabled:
            self.logger.info("AI analysis is disabled by AI_ANALYSIS_ENABLED=false")
        self.storage_sink = storage_sink or ClickHouseSink(config, logger)
        self.notifier = notifier or WebhookNotifier(config.webhook_url, logger)
        self.kafka_prod = kafka_producer or Producer(
            {"bootstrap.servers": config.kafka_brokers, "enable.idempotence": True}
        )
        self.buffer = defaultdict(self._new_buffer_entry)
        self.prev_window_counts: Dict[Tuple[str, str, str], int] = {}
        self.window_start = self._align_window(datetime.utcnow())
        self.executor = ThreadPoolExecutor(max_workers=3)

    def _align_window(self, dt: datetime) -> str:
        offset = dt.minute % self.config.window_minutes
        aligned = dt.replace(minute=dt.minute - offset, second=0, microsecond=0)
        return aligned.strftime("%Y-%m-%dT%H:%M:%SZ")

    def process_message(self, msg: Dict[str, Any]):
        try:
            ts_str = msg.get("timestamp", "")
            host = msg.get("host", "unknown")
            raw = msg.get("message", "")
            level = msg.get("level", "INFO")
            sanitized = self.sanitizer.sanitize(raw)
            pattern = self.template_extractor.extract(sanitized)
            window = self._align_window(datetime.utcnow())
            key = (window, host, pattern, level)
            entry = self.buffer[key]
            entry["count"] += 1
            entry["last_ts"] = ts_str
            if entry["first_ts"] is None:
                entry["first_ts"] = ts_str
            if len(entry["samples"]) < self.config.max_sample_size:
                entry["samples"].append(sanitized[:300])
        except Exception as error:
            self.logger.error(f"❌ 处理失败: {error}", exc_info=True)

    def flush_window(self):
        now_str = self._align_window(datetime.utcnow())
        if now_str <= self.window_start:
            return

        self.logger.info(f"🔄 窗口收敛: {self.window_start}")
        results = []
        next_prev_counts: Dict[Tuple[str, str, str], int] = {}

        for key, data in self.buffer.items():
            window, host, pattern, level = key
            if window != self.window_start:
                continue

            count = data["count"]
            trend = "stable"
            prev_count = self.prev_window_counts.get((host, pattern, level), 0)
            if prev_count > 0:
                ratio = count / prev_count
                trend = f"{int((ratio - 1) * 100)}%" if ratio != 1 else "stable"

            if count < self.config.min_count_threshold:
                results.append(self._build_converged(key, data, ai_analyzed=False))
                next_prev_counts[(host, pattern, level)] = count
                continue

            if not self.config.ai_analysis_enabled or self.ai_analyzer is None:
                results.append(self._build_converged(key, data, ai_analyzed=False))
                next_prev_counts[(host, pattern, level)] = count
                continue

            ai_result = self.ai_analyzer.analyze(host, pattern, level, count, data["samples"], trend)
            converged_record = self._build_converged(key, data, ai_result, ai_analyzed=True)
            results.append(converged_record)
            next_prev_counts[(host, pattern, level)] = count

            if (
                self.notifier.enabled
                and ai_result.get("is_anomaly")
                and ai_result.get("confidence", 0) >= self.config.alert_confidence_threshold
            ):
                self.executor.submit(self._handle_alert, host, pattern, ai_result)

        if results:
            self._push_to_kafka(results)
            if self.storage_sink.available:
                self.storage_sink.enqueue_converged(results)

        retained_buffer = defaultdict(self._new_buffer_entry)
        for key, data in self.buffer.items():
            if key[0] != self.window_start:
                retained_buffer[key] = data
        self.buffer = retained_buffer
        self.prev_window_counts = next_prev_counts
        self.window_start = now_str

    def _build_converged(self, key: Tuple[str, str, str, str], data: Dict[str, Any], ai_result: Dict[str, Any] = None, ai_analyzed: bool = False) -> Dict[str, Any]:
        window, host, pattern, level = key
        first_seen = data["first_ts"] or window
        last_seen = data["last_ts"] or window
        return {
            "window": window,
            "host": host,
            "event_pattern": pattern,
            "level": level,
            "count": data["count"],
            "first_seen": first_seen,
            "last_seen": last_seen,
            "samples": json.dumps(data["samples"][: self.config.max_sample_size], ensure_ascii=False),
            "ai_analyzed": 1 if ai_analyzed else 0,
            "ai_result": json.dumps(ai_result or {}, ensure_ascii=False),
        }

    def _push_to_kafka(self, results: List[Dict[str, Any]]):
        if not results:
            return
        for record in results:
            self.kafka_prod.poll(0)
            payload = json.dumps(record, ensure_ascii=False)
            try:
                self.kafka_prod.produce(self.config.converged_topic, value=payload)
            except BufferError:
                self.kafka_prod.poll(0.5)
                self.kafka_prod.produce(self.config.converged_topic, value=payload)
        self.kafka_prod.flush(timeout=10)

    def _handle_alert(self, host: str, pattern: str, ai_result: Dict[str, Any]):
        status = self.notifier.send(host, pattern, ai_result)
        if self.storage_sink.available:
            self.storage_sink.insert_alert(host, pattern, ai_result.get("root_cause", ""), status)
