import logging
import time

from confluent_kafka import Consumer


class CommitManager:
    def __init__(self, commit_batch: int, commit_interval_sec: int, logger: logging.Logger):
        self.commit_batch = max(1, commit_batch)
        self.commit_interval_sec = max(1, commit_interval_sec)
        self.logger = logger
        self.pending_commit = 0
        self.last_commit_at = time.monotonic()

    def on_idle(self, consumer: Consumer):
        now = time.monotonic()
        if self.pending_commit > 0 and (now - self.last_commit_at) >= self.commit_interval_sec:
            self._commit_async(consumer, now)

    def mark_processed(self, consumer: Consumer):
        self.pending_commit += 1
        now = time.monotonic()
        if self.pending_commit >= self.commit_batch or (now - self.last_commit_at) >= self.commit_interval_sec:
            self._commit_async(consumer, now)

    def flush_on_shutdown(self, consumer: Consumer):
        if self.pending_commit <= 0:
            return
        try:
            consumer.commit()
        except Exception as error:
            self.logger.warning(f"⚠️ 关闭前提交 offset 失败: {error}")

    def _commit_async(self, consumer: Consumer, now: float):
        consumer.commit(asynchronous=True)
        self.pending_commit = 0
        self.last_commit_at = now
