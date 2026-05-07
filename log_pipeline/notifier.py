import logging
from typing import Dict

import requests


class WebhookNotifier:
    def __init__(self, webhook_url: str, logger: logging.Logger):
        self.webhook_url = webhook_url
        self.logger = logger

    @property
    def enabled(self) -> bool:
        return bool(self.webhook_url)

    def send(self, host: str, pattern: str, ai_result: Dict) -> int:
        payload = {
            "msgtype": "text",
            "text": {
                "content": (
                    "🚨 异常日志告警\n"
                    f"主机: {host}\n"
                    f"模式: {pattern}\n"
                    f"根因: {ai_result.get('root_cause', '')}\n"
                    f"建议: {', '.join(ai_result.get('actions', []))}\n"
                    f"置信度: {ai_result.get('confidence', 0)}"
                )
            },
        }
        try:
            response = requests.post(self.webhook_url, json=payload, timeout=5)
            self.logger.info(f"📤 告警推送: {response.status_code}")
            return 1 if response.status_code == 200 else 0
        except Exception as error:
            self.logger.error(f"❌ 告警推送失败: {error}")
            return 0
