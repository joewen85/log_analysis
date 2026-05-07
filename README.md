# Linux 日志收敛 + AI 分析平台 (MVP)
## 架构
Linux Host → Vector → Kafka → Python(收敛+Drain3+AI) → Kafka → ClickHouse / 告警
## Vector 采集配置（当前）
- 输入源：
  - Docker 容器日志（`docker_logs`）
  - Linux 系统日志文件（`/var/log/messages`、`/var/log/syslog`、`/var/log/auth.log`、`/var/log/kern.log`）
  - Nginx Access/Error 日志文件（`/var/log/nginx/*.log`）
- 输出：
  - Kafka（Vector 容器内地址 `kafka:29092`，topic `linux_raw_logs`）
- 说明：
  - 已移除 `syslog` 网络输入，避免额外端口依赖
  - `journald` 在部分容器环境不可用，当前采用文件日志 + docker logs 方案
## 代码结构（已模块化）
- `ai_convergence_service.py`：兼容入口（保持原启动方式）
- `log_pipeline/config.py`：集中配置加载
- `log_pipeline/converger.py`：收敛主流程编排
- `log_pipeline/ai_analyzer.py`：AI 分析（缓存/重试/降级）
- `log_pipeline/template_extractor.py`：模板提取（Drain3/正则回退）
- `log_pipeline/clickhouse_sink.py`：ClickHouse 缓冲与批量写入
- `log_pipeline/notifier.py`：Webhook 告警发送
- `log_pipeline/commit_manager.py`：offset 批量/定时提交策略
- `log_pipeline/consumer_worker.py`：消费循环与消息处理工作器
- `log_pipeline/middleware.py`：消费中间件钩子（过滤/审计/采样扩展）
- `log_pipeline/middleware_examples.py`：示例中间件（host 白名单、正则黑名单、审计）
- `log_pipeline/runner.py`：组件装配入口（装配 worker + commit manager）
## 文档索引
- `docs/workflow.md`：流程图 + 时序图 + 文字总览
- `docs/workflow_flowchart.mmd`：流程图源码
- `docs/workflow_sequence.mmd`：时序图源码
- `docs/refactored_execution_flow.md`：模块化拆分后的执行流程（入口、循环、扩展点）
## 启动
0. 初始化配置（推荐）
```bash
cp .env.template .env
```
0.1 安装依赖（推荐使用 uv）
```bash
uv sync --no-dev
```
兼容 pip：
```bash
python3 -m pip install -r requirements.txt
```
1. docker compose up -d kafka clickhouse vector
2. （可选，本地 Ollama 模式）docker compose up -d ollama && docker exec -it $(docker compose ps -q ollama) ollama pull llama3.1:8b
3. bash setup_ch.sh
4. python ai_convergence_service.py
> Python 服务默认本机直连：Kafka `localhost:9092`、ClickHouse `localhost:8123`
> AI 分析支持远程 OpenAI 兼容接口；仅在 `AI_PROVIDER=ollama` 且未显式设置 `AI_BASE_URL` 时才默认本地 `http://localhost:11434/v1`
> Vector 容器内默认连接 Kafka：`kafka:29092`
## 可选环境变量
> 现在默认会自动加载项目根目录 `.env`，系统环境变量优先于 `.env`。
- WINDOW_MINUTES（默认5）：收敛窗口分钟数（建议压测/联调用1）
- MIN_COUNT_THRESHOLD（默认5）：触发 AI 分析阈值
- MAX_SAMPLE_SIZE（默认3）：样本日志保留条数
- KAFKA_GROUP_ID（默认ai-convergence-pipeline）：消费组ID
- KAFKA_AUTO_OFFSET_RESET（默认latest）：`latest` 或 `earliest`
- AI_PROVIDER（默认openai_compatible）：`openai_compatible` 或 `ollama`
- AI_MODEL：模型名称（由目标模型服务决定）
- AI_BASE_URL：模型服务地址（如 `https://your-llm-gateway/v1`，官方 OpenAI 可留空）
- AI_API_KEY：模型服务访问密钥
- AI_ORGANIZATION / AI_PROJECT（默认空）：可选组织/项目标识
- AI_RETRY_TIMES（默认2）/ AI_TIMEOUT_SEC（默认15）：AI 调用重试与超时
- KAFKA_COMMIT_BATCH（默认100）：批量提交 offset 条数
- KAFKA_COMMIT_INTERVAL_SEC（默认5）：最迟提交间隔秒数
- KAFKA_TEST_TOPIC（默认linux_raw_logs）：联调脚本注入测试日志 topic
- AI_CACHE_MAX_SIZE（默认1000）/ AI_CACHE_TTL_SEC（默认600）：AI 结果缓存容量与TTL
- MIDDLEWARE_HOST_ALLOWLIST（默认空）：按 host 白名单过滤（逗号分隔）
- MIDDLEWARE_MESSAGE_DENY_REGEX（默认空）：按 message 正则黑名单过滤（`||` 分隔多个表达式）
- MIDDLEWARE_AUDIT_ENABLED（默认false）：开启示例中间件审计日志
## 扩展点（中间件）
- 在 `ConsumerWorker` 中支持中间件钩子：
  - `before_decode(raw_message)`
  - `after_decode(payload)`（返回 `None` 可跳过处理）
  - `on_process_success(payload)`
  - `on_process_error(raw_message, error)`
示例启动（白名单 + 审计）：
```bash
MIDDLEWARE_HOST_ALLOWLIST="host-a,host-b" MIDDLEWARE_AUDIT_ENABLED=1 bash scripts/run_with_middleware.sh
```
示例启动（白名单 + 正则黑名单 + 审计）：
```bash
MIDDLEWARE_HOST_ALLOWLIST="host-a,host-b" MIDDLEWARE_MESSAGE_DENY_REGEX="healthcheck||heartbeat||debug noise" MIDDLEWARE_AUDIT_ENABLED=1 bash scripts/run_with_middleware.sh
```
## 一键联调（推荐）
```bash
uv sync --no-dev
bash scripts/e2e_smoke_test.sh
```
可选参数示例：
```bash
WAIT_SECONDS=90 WINDOW_MINUTES=1 MIN_COUNT_THRESHOLD=10 TEST_HOST=smoke-host bash scripts/e2e_smoke_test.sh
```
联调脚本关键参数：
- `KAFKA_BROKERS`（默认 `localhost:9092`）
- `KAFKA_TEST_TOPIC`（默认 `linux_raw_logs`）
- `KAFKA_GROUP_ID`（默认自动生成）
- `WINDOW_MINUTES` / `MIN_COUNT_THRESHOLD`
- `WAIT_SECONDS` / `KAFKA_WAIT_SECONDS` / `CH_WAIT_SECONDS`
说明：`scripts/e2e_smoke_test.sh` 会读取 `.env` 作为默认值来源（仅填补未设置变量）。
远程模型示例：
```bash
AI_PROVIDER=openai_compatible \
AI_BASE_URL="https://your-llm-gateway/v1" \
AI_API_KEY="your_api_key" \
AI_MODEL="your_model_name" \
python ai_convergence_service.py
```
本地 Ollama 示例：
```bash
AI_PROVIDER=ollama AI_MODEL=llama3.1:8b python ai_convergence_service.py
```
失败自动诊断包（默认开启）：
- `DIAG_ON_FAIL=1`（默认）失败时自动抓取
- `DIAG_OUTPUT_DIR=./diagnostics` 诊断包输出目录
- `DIAG_RETENTION_DAYS=7` 自动清理超过N天的诊断目录和压缩包
单独手动抓取：
```bash
bash scripts/collect_diagnostics.sh --label manual --service-log /tmp/log_ai_service_smoke.log --test-host smoke-host --retention-days 7
```
## 测试
```bash
for i in {1..50}; do echo "$(date -Iseconds) sshd[1234]: Failed password for invalid user admin from 10.0.0.5 port 22" | logger -t sshd; sleep 0.2; done
```
## 查询
```bash
docker exec -it $(docker compose ps -q clickhouse) clickhouse-client --query "SELECT host, event_pattern, count, ai_result FROM log_ai.converged_logs ORDER BY count DESC LIMIT 5 FORMAT Pretty"
```
