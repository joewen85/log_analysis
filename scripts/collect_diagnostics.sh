#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

LABEL="manual"
SERVICE_LOG=""
OUTPUT_DIR="${ROOT_DIR}/diagnostics"
TEST_HOST=""
RETENTION_DAYS="${RETENTION_DAYS:-7}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-logai}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-logai123456}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --label)
      LABEL="${2:-manual}"
      shift 2
      ;;
    --service-log)
      SERVICE_LOG="${2:-}"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="${2:-${ROOT_DIR}/diagnostics}"
      shift 2
      ;;
    --test-host)
      TEST_HOST="${2:-}"
      shift 2
      ;;
    --retention-days)
      RETENTION_DAYS="${2:-7}"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

if ! [[ "${RETENTION_DAYS}" =~ ^[0-9]+$ ]]; then
  echo "RETENTION_DAYS 必须是非负整数，当前: ${RETENTION_DAYS}"
  exit 1
fi

mkdir -p "${OUTPUT_DIR}"
removed_count=0
while IFS= read -r old_path; do
  rm -rf "${old_path}"
  removed_count=$((removed_count + 1))
done < <(find "${OUTPUT_DIR}" -mindepth 1 -maxdepth 1 \( -type d -o -type f -name '*.tar.gz' \) -mtime +"${RETENTION_DAYS}" -print 2>/dev/null)

timestamp="$(date +%Y%m%d_%H%M%S)"
safe_label="$(echo "${LABEL}" | tr -cs 'a-zA-Z0-9._-' '_')"
bundle_dir="${OUTPUT_DIR}/${safe_label}_${timestamp}"
mkdir -p "${bundle_dir}"

run_capture() {
  local file="$1"
  shift
  (
    "$@"
  ) >"${bundle_dir}/${file}" 2>&1 || true
}

{
  echo "timestamp=$(date -Iseconds)"
  echo "label=${LABEL}"
  echo "cwd=${ROOT_DIR}"
  echo "test_host=${TEST_HOST}"
  echo "service_log=${SERVICE_LOG}"
  echo "user=$(id -un 2>/dev/null || true)"
  echo "hostname=$(hostname 2>/dev/null || true)"
} >"${bundle_dir}/meta.txt"

run_capture "uname.txt" uname -a
run_capture "python_version.txt" python3 --version
run_capture "docker_version.txt" docker version
run_capture "docker_info.txt" docker info
run_capture "compose_ps.txt" docker compose ps -a
run_capture "compose_config.txt" docker compose config
run_capture "compose_logs_kafka.txt" docker compose logs --no-color kafka
run_capture "compose_logs_clickhouse.txt" docker compose logs --no-color clickhouse
run_capture "compose_logs_vector.txt" docker compose logs --no-color vector
run_capture "compose_logs_ollama.txt" docker compose logs --no-color ollama

kafka_cid="$(docker compose ps -q kafka 2>/dev/null || true)"
if [[ -n "${kafka_cid}" ]]; then
  run_capture "kafka_inspect.txt" docker inspect "${kafka_cid}"
  run_capture "kafka_top.txt" docker top "${kafka_cid}"
fi

clickhouse_cid="$(docker compose ps -q clickhouse 2>/dev/null || true)"
if [[ -n "${clickhouse_cid}" ]]; then
  run_capture "clickhouse_inspect.txt" docker inspect "${clickhouse_cid}"
  if [[ -n "${CLICKHOUSE_PASSWORD}" ]]; then
    run_capture "clickhouse_ping.txt" curl -fsS --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" "http://localhost:8123/ping"
    run_capture "clickhouse_tables.txt" docker exec "${clickhouse_cid}" clickhouse-client --user "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" --query "SHOW TABLES FROM log_ai"
    run_capture "clickhouse_recent_converged.txt" docker exec "${clickhouse_cid}" clickhouse-client --user "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" --query "SELECT window, host, event_pattern, level, count, created_at FROM log_ai.converged_logs ORDER BY created_at DESC LIMIT 50 FORMAT Pretty"
    run_capture "clickhouse_recent_behavior.txt" docker exec "${clickhouse_cid}" clickhouse-client --user "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" --query "SELECT window, host, client_ip, request_count, unique_path_count, status_4xx, status_5xx, ai_analyzed, created_at FROM log_ai.user_behavior_windows ORDER BY created_at DESC LIMIT 50 FORMAT Pretty"
    run_capture "clickhouse_recent_alerts.txt" docker exec "${clickhouse_cid}" clickhouse-client --user "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" --query "SELECT alert_time, host, pattern, summary, webhook_status FROM log_ai.alert_history ORDER BY alert_time DESC LIMIT 50 FORMAT Pretty"
  else
    run_capture "clickhouse_ping.txt" curl -fsS "http://localhost:8123/ping"
    run_capture "clickhouse_tables.txt" docker exec "${clickhouse_cid}" clickhouse-client --user "${CLICKHOUSE_USER}" --query "SHOW TABLES FROM log_ai"
    run_capture "clickhouse_recent_converged.txt" docker exec "${clickhouse_cid}" clickhouse-client --user "${CLICKHOUSE_USER}" --query "SELECT window, host, event_pattern, level, count, created_at FROM log_ai.converged_logs ORDER BY created_at DESC LIMIT 50 FORMAT Pretty"
    run_capture "clickhouse_recent_behavior.txt" docker exec "${clickhouse_cid}" clickhouse-client --user "${CLICKHOUSE_USER}" --query "SELECT window, host, client_ip, request_count, unique_path_count, status_4xx, status_5xx, ai_analyzed, created_at FROM log_ai.user_behavior_windows ORDER BY created_at DESC LIMIT 50 FORMAT Pretty"
    run_capture "clickhouse_recent_alerts.txt" docker exec "${clickhouse_cid}" clickhouse-client --user "${CLICKHOUSE_USER}" --query "SELECT alert_time, host, pattern, summary, webhook_status FROM log_ai.alert_history ORDER BY alert_time DESC LIMIT 50 FORMAT Pretty"
  fi
fi

if [[ -n "${SERVICE_LOG}" && -f "${SERVICE_LOG}" ]]; then
  cp "${SERVICE_LOG}" "${bundle_dir}/service.log" || true
  tail -n 200 "${SERVICE_LOG}" >"${bundle_dir}/service_tail.log" 2>/dev/null || true
fi

bundle_path="${OUTPUT_DIR}/${safe_label}_${timestamp}.tar.gz"
tar -czf "${bundle_path}" -C "${OUTPUT_DIR}" "$(basename "${bundle_dir}")"
echo "清理历史诊断: ${removed_count} 项 (保留 ${RETENTION_DAYS} 天)"
echo "诊断目录: ${bundle_dir}"
echo "诊断包: ${bundle_path}"
