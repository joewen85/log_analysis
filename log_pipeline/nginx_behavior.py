import json
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit


def _parse_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(str(value).strip()))
    except Exception:
        return default


def _extract_path(msg: Dict[str, Any]) -> str:
    direct = msg.get("path") or msg.get("request_path") or msg.get("uri")
    if direct:
        return str(direct).strip()[:256]

    request_line = str(msg.get("request", "")).strip()
    if not request_line:
        return "/"
    parts = request_line.split()
    if len(parts) < 2:
        return "/"
    raw_path = parts[1]
    if raw_path.startswith("http://") or raw_path.startswith("https://"):
        return urlsplit(raw_path).path[:256] or "/"
    return raw_path.split("?", 1)[0][:256] or "/"


def _extract_method(msg: Dict[str, Any]) -> str:
    method = msg.get("method") or msg.get("request_method")
    if method:
        return str(method).upper()[:16]
    request_line = str(msg.get("request", "")).strip()
    if request_line:
        return request_line.split()[0].upper()[:16]
    return "UNKNOWN"


def _extract_client_ip(msg: Dict[str, Any]) -> str:
    for key in ("remote_addr", "client_ip", "real_ip", "x_forwarded_for", "ip"):
        value = msg.get(key)
        if not value:
            continue
        text = str(value).strip()
        if not text:
            continue
        if "," in text:
            text = text.split(",", 1)[0].strip()
        return text[:64]
    return ""


class NginxBehaviorAggregator:
    @staticmethod
    def _new_entry() -> Dict[str, Any]:
        return {
            "request_count": 0,
            "first_seen": None,
            "last_seen": None,
            "total_bytes": 0,
            "status_2xx": 0,
            "status_3xx": 0,
            "status_4xx": 0,
            "status_5xx": 0,
            "status_other": 0,
            "paths": Counter(),
            "methods": Counter(),
        }

    def __init__(self, top_paths: int = 5):
        self.top_paths = max(1, top_paths)
        self.buffer = defaultdict(self._new_entry)

    def process_message(self, msg: Dict[str, Any], window: str) -> None:
        if str(msg.get("source_type", "")).strip() != "nginx_access":
            return

        client_ip = _extract_client_ip(msg)
        if not client_ip:
            return
        host = str(msg.get("host", "unknown")).strip() or "unknown"
        status = _parse_int(msg.get("status"))
        bytes_sent = _parse_int(msg.get("body_bytes_sent") or msg.get("bytes_sent") or msg.get("response_size"))
        path = _extract_path(msg)
        method = _extract_method(msg)
        timestamp = str(msg.get("timestamp", "")).strip() or window

        key = (window, host, client_ip)
        entry = self.buffer[key]
        entry["request_count"] += 1
        entry["last_seen"] = timestamp
        if entry["first_seen"] is None:
            entry["first_seen"] = timestamp
        entry["total_bytes"] += max(0, bytes_sent)
        entry["paths"][path] += 1
        entry["methods"][method] += 1

        if 200 <= status < 300:
            entry["status_2xx"] += 1
        elif 300 <= status < 400:
            entry["status_3xx"] += 1
        elif 400 <= status < 500:
            entry["status_4xx"] += 1
        elif 500 <= status < 600:
            entry["status_5xx"] += 1
        else:
            entry["status_other"] += 1

    def flush_window(self, window: str) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        retained = defaultdict(self._new_entry)
        for key, entry in self.buffer.items():
            key_window, host, client_ip = key
            if key_window != window:
                retained[key] = entry
                continue

            request_count = int(entry["request_count"])
            avg_bytes = float(entry["total_bytes"]) / request_count if request_count > 0 else 0.0
            top_paths = [{"path": path, "count": count} for path, count in entry["paths"].most_common(self.top_paths)]
            method_counts = {method: count for method, count in entry["methods"].items()}

            results.append(
                {
                    "window": window,
                    "host": host,
                    "client_ip": client_ip,
                    "request_count": request_count,
                    "unique_path_count": len(entry["paths"]),
                    "top_paths": json.dumps(top_paths, ensure_ascii=False),
                    "method_counts": json.dumps(method_counts, ensure_ascii=False),
                    "status_2xx": int(entry["status_2xx"]),
                    "status_3xx": int(entry["status_3xx"]),
                    "status_4xx": int(entry["status_4xx"]),
                    "status_5xx": int(entry["status_5xx"]),
                    "status_other": int(entry["status_other"]),
                    "total_bytes": int(entry["total_bytes"]),
                    "avg_bytes": avg_bytes,
                    "first_seen": entry["first_seen"] or window,
                    "last_seen": entry["last_seen"] or window,
                }
            )
        self.buffer = retained
        return results
