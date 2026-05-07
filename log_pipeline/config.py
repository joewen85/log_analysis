import os
from dataclasses import dataclass
from pathlib import Path


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, value)


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _strip_quotes(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        return text[1:-1]
    return text


def _env_first(name: str, aliases: tuple[str, ...], default: str = "") -> str:
    names = (name, *aliases)
    for key in names:
        value = os.getenv(key)
        if value is not None and value != "":
            return value
    return default


def load_dotenv_file(env_file: str = ".env") -> None:
    env_path = Path(env_file)
    if not env_path.is_absolute():
        env_path = Path.cwd() / env_path
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = _strip_quotes(value)


@dataclass(frozen=True)
class AppConfig:
    kafka_brokers: str
    kafka_group_id: str
    kafka_auto_offset_reset: str
    raw_topic: str
    converged_topic: str
    ai_model: str
    ai_provider: str
    ai_base_url: str
    ai_api_key: str
    ai_organization: str
    ai_project: str
    window_minutes: int
    min_count_threshold: int
    max_sample_size: int
    ai_retry_times: int
    ai_timeout_sec: int
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_db: str
    clickhouse_user: str
    clickhouse_password: str
    webhook_url: str
    alert_confidence_threshold: float
    drain3_depth: int
    drain3_stub_count: int
    ai_cache_max_size: int
    ai_cache_ttl_sec: int
    kafka_commit_batch: int
    kafka_commit_interval_sec: int
    middleware_host_allowlist: str
    middleware_message_deny_regex: str
    middleware_audit_enabled: bool

    @classmethod
    def from_env(cls, env_file: str = ".env") -> "AppConfig":
        load_dotenv_file(env_file)
        ai_provider = os.getenv("AI_PROVIDER", "openai_compatible")
        ai_base_url = _env_first("AI_BASE_URL", ("OPENAI_BASE_URL",), "")
        ai_api_key = _env_first("AI_API_KEY", ("OPENAI_API_KEY",), "")
        ai_organization = _env_first("AI_ORGANIZATION", ("OPENAI_ORG_ID",), "")
        ai_project = _env_first("AI_PROJECT", ("OPENAI_PROJECT_ID",), "")
        if ai_provider == "ollama":
            if not ai_base_url:
                ai_base_url = "http://localhost:11434/v1"
            if not ai_api_key:
                ai_api_key = "ollama"

        return cls(
            kafka_brokers=os.getenv("KAFKA_BROKERS", "localhost:9092"),
            kafka_group_id=os.getenv("KAFKA_GROUP_ID", "ai-convergence-pipeline"),
            kafka_auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest"),
            raw_topic=os.getenv("RAW_TOPIC", "linux_raw_logs"),
            converged_topic=os.getenv("CONVERGED_TOPIC", "linux_converged_logs"),
            ai_model=os.getenv("AI_MODEL", "llama3.1:8b"),
            ai_provider=ai_provider,
            ai_base_url=ai_base_url,
            ai_api_key=ai_api_key,
            ai_organization=ai_organization,
            ai_project=ai_project,
            window_minutes=_env_int("WINDOW_MINUTES", 5),
            min_count_threshold=_env_int("MIN_COUNT_THRESHOLD", 5),
            max_sample_size=_env_int("MAX_SAMPLE_SIZE", 3),
            ai_retry_times=_env_int("AI_RETRY_TIMES", 2),
            ai_timeout_sec=_env_int("AI_TIMEOUT_SEC", 15),
            clickhouse_host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            clickhouse_port=_env_int("CLICKHOUSE_PORT", 8123),
            clickhouse_db=os.getenv("CLICKHOUSE_DB", "log_ai"),
            clickhouse_user=os.getenv("CLICKHOUSE_USER", "logai"),
            clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", "logai123456"),
            webhook_url=os.getenv("ALERT_WEBHOOK_URL", ""),
            alert_confidence_threshold=float(os.getenv("ALERT_CONFIDENCE", "0.8")),
            drain3_depth=_env_int("DRAIN3_DEPTH", 3),
            drain3_stub_count=_env_int("DRAIN3_STUB_COUNT", 20),
            ai_cache_max_size=_env_int("AI_CACHE_MAX_SIZE", 1000),
            ai_cache_ttl_sec=_env_int("AI_CACHE_TTL_SEC", 600),
            kafka_commit_batch=_env_int("KAFKA_COMMIT_BATCH", 100),
            kafka_commit_interval_sec=_env_int("KAFKA_COMMIT_INTERVAL_SEC", 5),
            middleware_host_allowlist=os.getenv("MIDDLEWARE_HOST_ALLOWLIST", ""),
            middleware_message_deny_regex=os.getenv("MIDDLEWARE_MESSAGE_DENY_REGEX", ""),
            middleware_audit_enabled=_env_bool("MIDDLEWARE_AUDIT_ENABLED", False),
        )
