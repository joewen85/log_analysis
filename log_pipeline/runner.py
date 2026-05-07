import logging

from confluent_kafka import Consumer
from typing import List, Optional

from .commit_manager import CommitManager
from .config import AppConfig
from .converger import LogConverger
from .consumer_worker import ConsumerWorker
from .logging_utils import setup_logging
from .middleware import WorkerMiddleware
from .middleware_examples import HostAllowlistAuditMiddleware, RegexDenylistAuditMiddleware


def build_consumer(config: AppConfig) -> Consumer:
    consumer_conf = {
        "bootstrap.servers": config.kafka_brokers,
        "group.id": config.kafka_group_id,
        "auto.offset.reset": config.kafka_auto_offset_reset,
        "enable.auto.commit": False,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([config.raw_topic])
    return consumer


def build_middlewares(config: AppConfig, logger: logging.Logger) -> Optional[List[WorkerMiddleware]]:
    middlewares: List[WorkerMiddleware] = []

    allowlist = {
        host.strip()
        for host in config.middleware_host_allowlist.split(",")
        if host.strip()
    }

    if allowlist or config.middleware_audit_enabled:
        middlewares.append(
            HostAllowlistAuditMiddleware(
                allowed_hosts=allowlist,
                logger=logger,
                audit_enabled=config.middleware_audit_enabled,
            )
        )
        logger.info(
            "🧩 已启用示例中间件 HostAllowlistAuditMiddleware "
            f"allowlist={sorted(list(allowlist)) if allowlist else 'ALL'} "
            f"audit={config.middleware_audit_enabled}"
        )

    deny_patterns = {
        pattern.strip()
        for pattern in config.middleware_message_deny_regex.split("||")
        if pattern.strip()
    }
    if deny_patterns:
        middlewares.append(
            RegexDenylistAuditMiddleware(
                deny_patterns=deny_patterns,
                logger=logger,
                audit_enabled=config.middleware_audit_enabled,
            )
        )
        logger.info(
            "🧩 已启用示例中间件 RegexDenylistAuditMiddleware "
            f"deny_patterns={sorted(list(deny_patterns))} "
            f"audit={config.middleware_audit_enabled}"
        )

    return middlewares or None


def run_service(config: AppConfig, middlewares: Optional[List[WorkerMiddleware]] = None):
    logger = setup_logging()
    converger = LogConverger(config=config, logger=logger)
    consumer = build_consumer(config)
    configured_middlewares = middlewares if middlewares is not None else build_middlewares(config, logger)
    commit_manager = CommitManager(
        commit_batch=config.kafka_commit_batch,
        commit_interval_sec=config.kafka_commit_interval_sec,
        logger=logger,
    )
    worker = ConsumerWorker(
        consumer=consumer,
        converger=converger,
        commit_manager=commit_manager,
        logger=logger,
        middlewares=configured_middlewares,
    )

    worker.run_forever()


def main():
    config = AppConfig.from_env()
    run_service(config)
