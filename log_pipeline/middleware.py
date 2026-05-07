from typing import Any, Dict, Optional, Protocol


class WorkerMiddleware(Protocol):
    def before_decode(self, raw_message: bytes) -> bytes:
        ...

    def after_decode(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ...

    def on_process_success(self, payload: Dict[str, Any]):
        ...

    def on_process_error(self, raw_message: bytes, error: Exception):
        ...


class NoopWorkerMiddleware:
    def before_decode(self, raw_message: bytes) -> bytes:
        return raw_message

    def after_decode(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return payload

    def on_process_success(self, payload: Dict[str, Any]):
        return None

    def on_process_error(self, raw_message: bytes, error: Exception):
        return None
