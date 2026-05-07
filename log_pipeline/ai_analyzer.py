import json
import logging
import time
from collections import OrderedDict
from typing import Any, Dict, List, Tuple

from openai import OpenAI

from .config import AppConfig


class AIAnalyzer:
    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        client_kwargs: Dict[str, Any] = {
            "api_key": config.ai_api_key or "EMPTY_API_KEY",
        }
        if config.ai_base_url:
            client_kwargs["base_url"] = config.ai_base_url
        if config.ai_organization:
            client_kwargs["organization"] = config.ai_organization
        if config.ai_project:
            client_kwargs["project"] = config.ai_project
        self.client = OpenAI(**client_kwargs)
        self.logger.info(
            "AI client ready: provider=%s, base_url=%s, model=%s",
            config.ai_provider,
            config.ai_base_url or "(default)",
            config.ai_model,
        )
        if not config.ai_api_key:
            self.logger.warning("AI_API_KEY 未配置，若目标模型要求鉴权会触发降级兜底。")
        self.cache: OrderedDict[str, Tuple[float, Dict[str, Any]]] = OrderedDict()

    def _get_cached(self, key: str):
        cached = self.cache.get(key)
        if not cached:
            return None
        cached_at, data = cached
        if (time.time() - cached_at) > self.config.ai_cache_ttl_sec:
            self.cache.pop(key, None)
            return None
        self.cache.move_to_end(key)
        return data

    def _set_cached(self, key: str, value: Dict[str, Any]):
        self.cache[key] = (time.time(), value)
        self.cache.move_to_end(key)
        while len(self.cache) > self.config.ai_cache_max_size:
            self.cache.popitem(last=False)

    def analyze(self, host: str, pattern: str, level: str, count: int, samples: List[str], trend: str) -> Dict[str, Any]:
        cache_key = f"{host}|{pattern}|{level}|{trend}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        samples_str = "\n".join([f"  - {sample}" for sample in samples])
        prompt = f"""你是一个资深Linux运维专家。请分析以下收敛日志，输出严格JSON格式。
数据:
  主机: {host}
  事件模板: {pattern}
  级别: {level}
  频次: {count}
  趋势: {trend}
  样例:
{samples_str}
要求:
1. is_anomaly: boolean
2. root_cause: string
3. actions: list
4. confidence: float (0~1)
仅返回JSON。"""

        for retry in range(self.config.ai_retry_times):
            try:
                response = self.client.chat.completions.create(
                    model=self.config.ai_model,
                    messages=[
                        {"role": "system", "content": "Output valid JSON only."},
                        {"role": "user", "content": prompt},
                    ],
                    response_format={"type": "json_object"},
                    timeout=self.config.ai_timeout_sec,
                )
                result = json.loads(response.choices[0].message.content)
                self._set_cached(cache_key, result)
                return result
            except Exception as error:
                self.logger.warning(f"⚠️ AI 调用失败 (重试 {retry + 1}): {error}")
                time.sleep(2)

        self.logger.info(f"🛡️ AI 降级兜底: {cache_key}")
        return {
            "is_anomaly": level in ["ERROR", "CRIT", "ALERT", "EMERG"],
            "root_cause": f"高频{level}: {pattern[:50]}",
            "actions": ["检查负载", "核对变更"],
            "confidence": 0.7,
        }
