#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

load_env_defaults() {
  local env_file="${ROOT_DIR}/.env"
  [[ -f "${env_file}" ]] || return 0
  while IFS= read -r raw_line || [[ -n "${raw_line}" ]]; do
    local line="${raw_line#"${raw_line%%[![:space:]]*}"}"
    [[ -z "${line}" || "${line}" == \#* ]] && continue
    [[ "${line}" == export\ * ]] && line="${line#export }"
    [[ "${line}" == *"="* ]] || continue
    local key="${line%%=*}"
    local value="${line#*=}"
    key="${key%"${key##*[![:space:]]}"}"
    key="${key#"${key%%[![:space:]]*}"}"
    [[ -n "${key}" ]] || continue
    if [[ "${value}" == \"*\" && "${value}" == *\" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "${value}" == \'*\' && "${value}" == *\' ]]; then
      value="${value:1:${#value}-2}"
    fi
    if [[ -z "${!key+x}" ]]; then
      export "${key}=${value}"
    fi
  done < "${env_file}"
}

load_env_defaults

WAIT_SECONDS="${WAIT_SECONDS:-75}"
KAFKA_WAIT_SECONDS="${KAFKA_WAIT_SECONDS:-300}"
CH_WAIT_SECONDS="${CH_WAIT_SECONDS:-120}"
TEST_HOST="${TEST_HOST:-smoke-host-$(date +%s)}"
WINDOW_MINUTES="${WINDOW_MINUTES:-1}"
MIN_COUNT_THRESHOLD="${MIN_COUNT_THRESHOLD:-10}"
KAFKA_GROUP_ID="${KAFKA_GROUP_ID:-ai-convergence-smoke-$(date +%s)}"
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
KAFKA_TEST_TOPIC="${KAFKA_TEST_TOPIC:-linux_raw_logs}"
SERVICE_LOG="${SERVICE_LOG:-/tmp/log_ai_service_smoke.log}"
DIAG_ON_FAIL="${DIAG_ON_FAIL:-1}"
DIAG_OUTPUT_DIR="${DIAG_OUTPUT_DIR:-${ROOT_DIR}/diagnostics}"
DIAG_RETENTION_DAYS="${DIAG_RETENTION_DAYS:-7}"
FAIL_STAGE="startup"
diag_collected=0

collect_diag() {
  if [[ "${DIAG_ON_FAIL}" != "1" || "${diag_collected}" -eq 1 ]]; then
    return
  fi
  echo "==> 自动抓取诊断包(stage=${FAIL_STAGE})"
  bash scripts/collect_diagnostics.sh \
    --label "smoke_${FAIL_STAGE}" \
    --service-log "${SERVICE_LOG}" \
    --test-host "${TEST_HOST}" \
    --retention-days "${DIAG_RETENTION_DAYS}" \
    --output-dir "${DIAG_OUTPUT_DIR}" || true
  diag_collected=1
}

on_err() {
  local code=$?
  if [[ "${code}" -ne 0 ]]; then
    collect_diag
  fi
}

trap on_err ERR

echo "==> 启动 Kafka + ClickHouse"
docker compose up -d kafka clickhouse

echo "==> 等待 Kafka healthy"
kafka_cid="$(docker compose ps -q kafka)"
for _ in $(seq 1 "${KAFKA_WAIT_SECONDS}"); do
  status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${kafka_cid}" 2>/dev/null || true)"
  if [[ "${status}" == "healthy" ]]; then
    break
  fi
  sleep 1
done
if [[ "$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${kafka_cid}")" != "healthy" ]]; then
  echo "Kafka 未就绪"
  docker compose logs --no-color kafka | tail -n 120 || true
  FAIL_STAGE="kafka_not_ready"
  collect_diag
  exit 1
fi

echo "==> 等待 ClickHouse ready"
for _ in $(seq 1 "${CH_WAIT_SECONDS}"); do
  if curl -fsS "http://localhost:8123/ping" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! curl -fsS "http://localhost:8123/ping" >/dev/null 2>&1; then
  echo "ClickHouse 未就绪"
  FAIL_STAGE="clickhouse_not_ready"
  collect_diag
  exit 1
fi

echo "==> 初始化表结构"
FAIL_STAGE="setup_schema"
CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=8123 bash setup_ch.sh

echo "==> 启动收敛服务"
FAIL_STAGE="start_service"
KAFKA_BROKERS="${KAFKA_BROKERS}" \
KAFKA_GROUP_ID="${KAFKA_GROUP_ID}" \
KAFKA_AUTO_OFFSET_RESET=latest \
CLICKHOUSE_HOST=localhost \
CLICKHOUSE_PORT=8123 \
WINDOW_MINUTES="${WINDOW_MINUTES}" \
MIN_COUNT_THRESHOLD="${MIN_COUNT_THRESHOLD}" \
python3 ai_convergence_service.py >"${SERVICE_LOG}" 2>&1 &
svc_pid=$!

cleanup() {
  if kill -0 "${svc_pid}" >/dev/null 2>&1; then
    kill "${svc_pid}" >/dev/null 2>&1 || true
    wait "${svc_pid}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

sleep 5

echo "==> 注入测试日志到 Kafka"
FAIL_STAGE="produce_test_logs"
python3 - <<PY
import json
import time
from confluent_kafka import Producer

host = "${TEST_HOST}"
topic = "${KAFKA_TEST_TOPIC}"
producer = Producer({"bootstrap.servers": "${KAFKA_BROKERS}"})
for i in range(6):
    payload = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "host": host,
        "message": f"smoke event {i}",
        "level": "INFO",
    }
    producer.produce(topic, value=json.dumps(payload).encode("utf-8"))
producer.flush()
print("produced=6")
PY

echo "==> 等待窗口刷盘 ${WAIT_SECONDS}s"
FAIL_STAGE="wait_flush"
sleep "${WAIT_SECONDS}"

echo "==> 查询 ClickHouse"
FAIL_STAGE="verify_clickhouse"
count_result="$(
  docker exec "$(docker compose ps -q clickhouse)" clickhouse-client \
    --send_timeout=10 \
    --receive_timeout=10 \
    --query "SELECT count() FROM log_ai.converged_logs WHERE host='${TEST_HOST}' AND created_at >= now() - INTERVAL 15 MINUTE"
)"

echo "rows=${count_result}"
if [[ "${count_result}" -ge 1 ]]; then
  echo "✅ E2E smoke test 通过"
else
  echo "❌ 未查询到测试数据，服务日志尾部："
  tail -n 120 "${SERVICE_LOG}" || true
  FAIL_STAGE="no_rows"
  collect_diag
  exit 1
fi
