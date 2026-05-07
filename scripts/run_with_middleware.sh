#!/usr/bin/env bash
set -euo pipefail

# 示例：按 host 白名单 + message 正则黑名单过滤，并开启中间件审计日志
# 用法：
#   MIDDLEWARE_HOST_ALLOWLIST="host-a,host-b" \
#   MIDDLEWARE_MESSAGE_DENY_REGEX="healthcheck||heartbeat||debug noise" \
#   MIDDLEWARE_AUDIT_ENABLED=1 \
#   bash scripts/run_with_middleware.sh

python3 ai_convergence_service.py
