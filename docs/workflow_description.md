# 日志处理工作流说明

## 一、整体目标
系统将 Linux 主机日志进行采集、标准化、聚合、AI 分析与告警，并把结果持久化到 Kafka 与 ClickHouse，形成可查询、可告警、可诊断的日志收敛链路。

## 二、组件职责
- Linux Host：产生日志（systemd journal）。
- Vector：采集日志并做字段标准化，发送到 Kafka 原始主题。
- Kafka（`linux_raw_logs`）：承接原始日志流，解耦采集与分析。
- `ai_convergence_service.py`：核心处理服务，负责脱敏、模板提取、窗口聚合、AI 分析、告警与落库。
- Kafka（`linux_converged_logs`）：承接收敛后的结果消息。
- ClickHouse：存储收敛结果与告警历史。
- Webhook：接收异常告警通知。

## 三、详细处理流程（逐步）

### 1) 日志采集与标准化
- Vector 通过 `journal_logs` 读取 Linux journal。
- 在 `remap` 阶段把日志转换为统一字段：
  - `message`：日志正文
  - `level`：日志级别（来自 PRIORITY）
  - `host`：主机名
  - `timestamp`：采集时间
- 转换后的 JSON 发往 Kafka 主题 `linux_raw_logs`。

### 2) 核心服务初始化
- 启动 Kafka Consumer（消费 `linux_raw_logs`）与 Producer（写 `linux_converged_logs`）。
- 初始化 Drain3 模板挖掘器；若不可用则自动降级到正则模板提取。
- 初始化 ClickHouse 客户端与后台刷盘线程。
- 初始化内存结构：
  - 聚合缓冲 `buffer`
  - 上一窗口计数 `prev_window_counts`
  - AI 缓存（TTL + LRU）

### 3) 消费与单条处理
- 服务循环 `poll` Kafka 消息。
- 每条消息执行：
  1. 读取 `timestamp/host/message/level`
  2. `sanitize` 脱敏（IP、password、token、key）
  3. 模板提取（Drain3 或正则）
  4. 计算当前时间所属窗口（如 1 分钟或 5 分钟）
  5. 以 `(window, host, pattern, level)` 作为 key 聚合：
     - `count` 增加
     - 记录 `first_ts`/`last_ts`
     - 保留有限样本 `samples`

### 4) 窗口收敛（flush）
- 每轮消费都会检查是否跨过窗口边界。
- 当窗口结束时，遍历当前窗口的聚合项并计算趋势：
  - 与上一窗口同 key 计数对比，得到 `stable` 或百分比变化。
- 分流逻辑：
  - 若 `count < min_count_threshold`：直接生成结果，`ai_analyzed=0`
  - 若 `count >= min_count_threshold`：进入 AI 分析

### 5) AI 分析与降级
- 构造提示词（主机、模板、级别、频次、趋势、样本）请求 AI 返回 JSON。
- 结果先查缓存，未命中才调用模型。
- 调用失败会重试；仍失败则使用降级规则结果（不中断主流程）。

### 6) 告警处理
- 当 AI 判定 `is_anomaly=true` 且 `confidence` 超阈值时：
  - 异步调用 webhook 发送告警文本
  - 记录告警结果到 `alert_history`

### 7) 结果输出与落库
- 所有窗口结果先发送到 Kafka `linux_converged_logs`。
- 同时加入 ClickHouse 内存缓冲队列。
- 后台线程定期批量写入 ClickHouse `converged_logs`。

### 8) 位点提交与可靠性策略
- Kafka offset 按“批量条数 + 时间间隔”提交，减少逐条提交开销。
- 退出前会做一次兜底提交。
- 若某些下游失败：
  - AI 失败：走降级结果继续处理
  - Webhook 失败：仅影响通知，不影响收敛主链路
  - ClickHouse 写入失败：记录错误日志（主链路继续）

## 四、联调与诊断流程

### 1) 一键联调（Smoke）
- `scripts/e2e_smoke_test.sh` 自动执行：
  1. 拉起 Kafka + ClickHouse
  2. 等待服务就绪
  3. 初始化表结构
  4. 启动核心服务
  5. 注入测试日志
  6. 等待窗口刷新
  7. 查询 ClickHouse 验证结果

### 2) 失败自动诊断包
- Smoke 失败时会自动调用 `scripts/collect_diagnostics.sh`：
  - 采集容器状态、compose 配置、容器日志、ClickHouse近况、服务日志等
  - 打包到 `diagnostics/*.tar.gz`
  - 支持按天清理旧诊断包（保留天数可配置）

## 五、关键配置点
- 窗口与阈值：`WINDOW_MINUTES`、`MIN_COUNT_THRESHOLD`
- AI 行为：`AI_MODEL`、`AI_RETRY_TIMES`、`AI_TIMEOUT_SEC`
- 缓存控制：`AI_CACHE_MAX_SIZE`、`AI_CACHE_TTL_SEC`
- Kafka 提交：`KAFKA_COMMIT_BATCH`、`KAFKA_COMMIT_INTERVAL_SEC`
- 诊断保留：`DIAG_RETENTION_DAYS`

## 六、模块化执行流程（拆分后）
- 入口保持兼容：`ai_convergence_service.py` 仅负责调用 `log_pipeline.runner.main()`。
- 运行装配在 `log_pipeline/runner.py`：负责创建 `LogConverger`、`Consumer`、`CommitManager`、`ConsumerWorker`。
- 消费主循环在 `log_pipeline/consumer_worker.py`：`poll -> flush_window -> process_message -> commit`。
- 提交策略在 `log_pipeline/commit_manager.py`：批量/定时异步提交，关闭前兜底提交。
- 中间件扩展在 `log_pipeline/middleware.py` 与 `log_pipeline/middleware_examples.py`。
- 详细模块执行链路见：`docs/refactored_execution_flow.md`。
