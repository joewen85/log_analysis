from typing import Any, Dict, List, Protocol


class TemplateExtractorProtocol(Protocol):
    def extract(self, log_text: str) -> str:
        ...


class AIAnalyzerProtocol(Protocol):
    def analyze(self, host: str, pattern: str, level: str, count: int, samples: List[str], trend: str) -> Dict[str, Any]:
        ...


class StorageSinkProtocol(Protocol):
    @property
    def available(self) -> bool:
        ...

    def enqueue_converged(self, records: List[Dict[str, Any]]):
        ...

    def insert_alert(self, host: str, pattern: str, summary: str, status: int):
        ...


class NotifierProtocol(Protocol):
    @property
    def enabled(self) -> bool:
        ...

    def send(self, host: str, pattern: str, ai_result: Dict[str, Any]) -> int:
        ...
