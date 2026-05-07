import json
import logging
from typing import List, Optional

from confluent_kafka import Consumer, KafkaError

from .commit_manager import CommitManager
from .converger import LogConverger
from .middleware import NoopWorkerMiddleware, WorkerMiddleware


class ConsumerWorker:
    def __init__(
        self,
        consumer: Consumer,
        converger: LogConverger,
        commit_manager: CommitManager,
        logger: logging.Logger,
        middlewares: Optional[List[WorkerMiddleware]] = None,
    ):
        self.consumer = consumer
        self.converger = converger
        self.commit_manager = commit_manager
        self.logger = logger
        self.middlewares = middlewares or [NoopWorkerMiddleware()]

    def run_forever(self):
        self.logger.info("🚀 日志收敛+AI分析服务已启动")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                self.converger.flush_window()

                if msg is None:
                    self.commit_manager.on_idle(self.consumer)
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise Exception(msg.error())

                self._process_message(msg.value())
                self.commit_manager.mark_processed(self.consumer)
        except KeyboardInterrupt:
            self.logger.info("✅ 服务已安全退出")
        finally:
            self.commit_manager.flush_on_shutdown(self.consumer)
            self.consumer.close()

    def _process_message(self, message_bytes: bytes) -> None:
        raw = message_bytes
        try:
            for middleware in self.middlewares:
                raw = middleware.before_decode(raw)

            payload = json.loads(raw.decode("utf-8"))

            for middleware in self.middlewares:
                payload = middleware.after_decode(payload)
                if payload is None:
                    return

            self.converger.process_message(payload)
            for middleware in self.middlewares:
                middleware.on_process_success(payload)
        except Exception as error:
            self.logger.error(f"❌ JSON解析失败: {error}")
            for middleware in self.middlewares:
                middleware.on_process_error(raw, error)
