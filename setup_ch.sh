#!/usr/bin/env bash
set -euo pipefail
CH_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CH_PORT="${CLICKHOUSE_PORT:-8123}"
CH_DB="${CLICKHOUSE_DB:-log_ai}"
CH_USER="${CLICKHOUSE_USER:-logai}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-logai123456}"

curl_cmd=(curl -fsS "http://${CH_HOST}:${CH_PORT}/")
if [[ -n "${CH_USER}" ]]; then
  curl_cmd+=(--user "${CH_USER}:${CH_PASSWORD}")
fi

"${curl_cmd[@]}" --data-binary "CREATE DATABASE IF NOT EXISTS ${CH_DB}"
"${curl_cmd[@]}" --data-binary "
CREATE TABLE IF NOT EXISTS ${CH_DB}.converged_logs (
    window DateTime,
    host LowCardinality(String),
    event_pattern LowCardinality(String),
    level LowCardinality(String),
    count UInt32,
    first_seen DateTime,
    last_seen DateTime,
    samples String,
    ai_analyzed UInt8,
    ai_result String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (window, host, event_pattern)
"
"${curl_cmd[@]}" --data-binary "
CREATE TABLE IF NOT EXISTS ${CH_DB}.alert_history (
    alert_time DateTime DEFAULT now(),
    host LowCardinality(String),
    pattern LowCardinality(String),
    summary String,
    webhook_status UInt8
) ENGINE = MergeTree() ORDER BY alert_time
"
"${curl_cmd[@]}" --data-binary "
CREATE TABLE IF NOT EXISTS ${CH_DB}.user_behavior_windows (
    window DateTime,
    host LowCardinality(String),
    client_ip String,
    request_count UInt32,
    unique_path_count UInt32,
    top_paths String,
    method_counts String,
    status_2xx UInt32,
    status_3xx UInt32,
    status_4xx UInt32,
    status_5xx UInt32,
    status_other UInt32,
    total_bytes UInt64,
    avg_bytes Float64,
    first_seen DateTime,
    last_seen DateTime,
    ai_analyzed UInt8,
    ai_result String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (window, host, client_ip)
"
echo "✅ ClickHouse 表结构初始化完成"
