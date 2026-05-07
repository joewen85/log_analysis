import logging
import re
from typing import Optional

from .config import AppConfig

try:
    from drain3 import TemplateMiner
    from drain3.template_miner_config import TemplateMinerConfig
except Exception:
    TemplateMiner = None
    TemplateMinerConfig = None


class TemplateExtractor:
    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.template_miner: Optional[TemplateMiner] = None
        self._init_template_miner()

    def _init_template_miner(self):
        if TemplateMiner is None or TemplateMinerConfig is None:
            self.logger.warning("⚠️ Drain3 不可用，降级至正则模式")
            return
        try:
            drain3_config = TemplateMinerConfig()
            drain3_config.profiling_enabled = False
            drain3_config.drain_depth = self.config.drain3_depth
            drain3_config.drain_max_children = self.config.drain3_stub_count
            self.template_miner = TemplateMiner(config=drain3_config)
            self.logger.info("✅ Drain3 模板提取器初始化成功")
        except Exception as error:
            self.logger.warning(f"⚠️ Drain3 初始化失败({error})，降级至正则模式")
            self.template_miner = None

    def extract(self, log_text: str) -> str:
        if self.template_miner:
            try:
                result = self.template_miner.add_log_message(log_text)
                template = (result or {}).get("template_mined")
                if template:
                    return template.strip()[:200]
            except Exception:
                pass
        template = re.sub(r"\b\d+\.?\d*", "<NUM>", log_text)
        template = re.sub(r"/\S+/\S+", "<PATH>", template)
        return template[:200].strip()
