/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 任务执行日志记录器
 *
 * <p>提供统一的任务生命周期日志追踪能力，支持：
 *
 * <ul>
 *   <li>任务接收确认与详情记录
 *   <li>任务执行过程追踪
 *   <li>任务完成结果记录
 *   <li>性能指标统计
 * </ul>
 *
 * <p>日志格式规范：
 *
 * <pre>
 * {timestamp} {level} [{prefix}] taskId={taskId}, {key1}={value1}, {key2}={value2}, ...
 * </pre>
 *
 * <p>日志级别使用规范：
 *
 * <ul>
 *   <li>SEVERE: 任务执行失败、不可恢复错误
 *   <li>WARNING: 任务超时、可恢复警告、重试
 *   <li>INFO: 关键生命周期事件（接收、开始、完成）
 *   <li>FINE: 详细调试信息（参数、进度、结果详情）
 *   <li>FINER: 更详细的追踪信息（每条命令输入输出）
 * </ul>
 */
public final class TaskExecutionLogger {

  private static final Logger logger = Logger.getLogger(TaskExecutionLogger.class.getName());

  // ==================== 日志前缀常量 ====================
  private static final String LOG_PREFIX_RECEIVED = "[TASK-RECEIVED]";
  private static final String LOG_PREFIX_STARTED = "[TASK-STARTED]";
  private static final String LOG_PREFIX_PROGRESS = "[TASK-PROGRESS]";
  private static final String LOG_PREFIX_COMPLETED = "[TASK-COMPLETED]";
  private static final String LOG_PREFIX_FAILED = "[TASK-FAILED]";
  private static final String LOG_PREFIX_TIMEOUT = "[TASK-TIMEOUT]";
  private static final String LOG_PREFIX_CANCELLED = "[TASK-CANCELLED]";

  // ==================== 任务类型常量 ====================
  /** Arthas 命令任务 */
  public static final String TASK_TYPE_ARTHAS_COMMAND = "ARTHAS_COMMAND";

  /** Arthas 会话任务 */
  public static final String TASK_TYPE_ARTHAS_SESSION = "ARTHAS_SESSION";

  /** Arthas 启动任务 */
  public static final String TASK_TYPE_ARTHAS_START = "ARTHAS_START";

  /** Arthas 停止任务 */
  public static final String TASK_TYPE_ARTHAS_STOP = "ARTHAS_STOP";

  /** 配置轮询任务 */
  public static final String TASK_TYPE_CONFIG_POLL = "CONFIG_POLL";

  /** 任务轮询任务 */
  public static final String TASK_TYPE_TASK_POLL = "TASK_POLL";

  /** 状态上报任务 */
  public static final String TASK_TYPE_STATUS_REPORT = "STATUS_REPORT";

  /** 清理任务 */
  public static final String TASK_TYPE_CLEANUP = "CLEANUP";

  // ==================== 任务来源常量 ====================
  /** Tunnel 服务器 */
  public static final String SOURCE_TUNNEL_SERVER = "tunnel_server";

  /** 终端输入 */
  public static final String SOURCE_TERMINAL_INPUT = "terminal_input";

  /** 调度器 */
  public static final String SOURCE_SCHEDULER = "scheduler";

  /** API 请求 */
  public static final String SOURCE_API = "api";

  // ==================== 错误码常量 ====================
  /** 内部错误 */
  public static final String ERROR_INTERNAL = "INTERNAL_ERROR";

  /** 超时 */
  public static final String ERROR_TIMEOUT = "TIMEOUT";

  /** 会话不存在 */
  public static final String ERROR_SESSION_NOT_FOUND = "SESSION_NOT_FOUND";

  /** 达到最大会话数 */
  public static final String ERROR_MAX_SESSIONS = "MAX_SESSIONS_REACHED";

  /** Arthas 未启动 */
  public static final String ERROR_ARTHAS_NOT_STARTED = "ARTHAS_NOT_STARTED";

  /** 绑定失败 */
  public static final String ERROR_BIND_FAILED = "BIND_FAILED";

  /** 网络错误 */
  public static final String ERROR_NETWORK = "NETWORK_ERROR";

  // ==================== 实例字段 ====================
  /** 活跃任务上下文缓存 */
  private final Map<String, TaskContext> activeContexts = new ConcurrentHashMap<>();

  /** 日志配置 */
  private volatile boolean enabled = true;

  /** 命令详情是否记录（可能包含敏感信息） */
  private volatile boolean logCommandDetails = true;

  /** 结果日志最大长度 */
  private volatile int maxResultLogLength = 500;

  /** 单例实例 */
  private static final TaskExecutionLogger INSTANCE = new TaskExecutionLogger();

  private TaskExecutionLogger() {}

  /**
   * 获取单例实例
   *
   * @return TaskExecutionLogger 实例
   */
  public static TaskExecutionLogger getInstance() {
    return INSTANCE;
  }

  // ==================== 配置方法 ====================

  /**
   * 设置是否启用日志
   *
   * @param enabled 是否启用
   */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * 设置是否记录命令详情
   *
   * @param logCommandDetails 是否记录
   */
  public void setLogCommandDetails(boolean logCommandDetails) {
    this.logCommandDetails = logCommandDetails;
  }

  /**
   * 设置结果日志最大长度
   *
   * @param maxLength 最大长度
   */
  public void setMaxResultLogLength(int maxLength) {
    this.maxResultLogLength = maxLength;
  }

  // ==================== 任务接收阶段 ====================

  /**
   * 记录任务接收
   *
   * @param taskId 任务ID
   * @param taskType 任务类型（使用 TASK_TYPE_* 常量）
   * @param source 任务来源（使用 SOURCE_* 常量）
   * @return TaskContext 用于后续追踪
   */
  public TaskContext logTaskReceived(String taskId, String taskType, String source) {
    return logTaskReceived(taskId, taskType, source, null);
  }

  /**
   * 记录任务接收（带详情）
   *
   * @param taskId 任务ID
   * @param taskType 任务类型（使用 TASK_TYPE_* 常量）
   * @param source 任务来源（使用 SOURCE_* 常量）
   * @param details 任务详情
   * @return TaskContext 用于后续追踪
   */
  public TaskContext logTaskReceived(
      String taskId, String taskType, String source, @Nullable Map<String, Object> details) {

    TaskContext context = new TaskContext(taskId, taskType, source);
    activeContexts.put(taskId, context);

    if (!enabled) {
      return context;
    }

    // INFO 级别：记录任务接收的关键信息
    logger.log(
        Level.INFO,
        "{0} taskId={1}, type={2}, source={3}, receivedAt={4}",
        new Object[] {LOG_PREFIX_RECEIVED, taskId, taskType, source, context.getReceivedAt()});

    // FINE 级别：记录任务详细内容
    if (details != null && !details.isEmpty() && logger.isLoggable(Level.FINE)) {
      logger.log(
          Level.FINE,
          "{0} taskId={1}, details={2}",
          new Object[] {LOG_PREFIX_RECEIVED, taskId, formatDetails(details)});
    }

    return context;
  }

  /**
   * 记录任务参数配置
   *
   * @param taskId 任务ID
   * @param priority 优先级（可选）
   * @param timeout 超时时间（可选）
   */
  public void logTaskConfig(
      String taskId, @Nullable String priority, @Nullable Duration timeout) {
    logTaskConfig(taskId, priority, timeout, null);
  }

  /**
   * 记录任务参数配置（带额外参数）
   *
   * @param taskId 任务ID
   * @param priority 优先级（可选）
   * @param timeout 超时时间（可选）
   * @param additionalParams 额外参数（可选）
   */
  public void logTaskConfig(
      String taskId,
      @Nullable String priority,
      @Nullable Duration timeout,
      @Nullable Map<String, Object> additionalParams) {

    TaskContext context = activeContexts.get(taskId);
    if (context != null) {
      context.setPriority(priority);
      context.setTimeout(timeout);
    }

    if (!enabled || !logger.isLoggable(Level.FINE)) {
      return;
    }

    logger.log(
        Level.FINE,
        "{0} taskId={1}, priority={2}, timeout={3}, params={4}",
        new Object[] {
          LOG_PREFIX_RECEIVED,
          taskId,
          priority != null ? priority : "default",
          timeout != null ? timeout.toMillis() + "ms" : "none",
          additionalParams != null ? formatDetails(additionalParams) : "none"
        });
  }

  // ==================== 任务执行阶段 ====================

  /**
   * 记录任务开始执行
   *
   * @param taskId 任务ID
   */
  public void logTaskStarted(String taskId) {
    logTaskStarted(taskId, null);
  }

  /**
   * 记录任务开始执行（带执行器信息）
   *
   * @param taskId 任务ID
   * @param executorInfo 执行器信息（如线程名、会话ID等）
   */
  public void logTaskStarted(String taskId, @Nullable String executorInfo) {
    TaskContext context = activeContexts.get(taskId);
    if (context != null) {
      context.markStarted();
    }

    if (!enabled) {
      return;
    }

    long queueTimeMs = context != null ? context.getQueueTimeMs() : 0;

    logger.log(
        Level.INFO,
        "{0} taskId={1}, executor={2}, queueTimeMs={3}",
        new Object[] {
          LOG_PREFIX_STARTED, taskId, executorInfo != null ? executorInfo : "default", queueTimeMs
        });
  }

  /**
   * 记录任务执行进度/中间状态
   *
   * @param taskId 任务ID
   * @param step 当前步骤
   * @param message 进度消息
   */
  public void logTaskProgress(String taskId, String step, String message) {
    if (!enabled || !logger.isLoggable(Level.FINE)) {
      return;
    }

    TaskContext context = activeContexts.get(taskId);
    long elapsedMs = context != null ? context.getElapsedMs() : 0;

    logger.log(
        Level.FINE,
        "{0} taskId={1}, step={2}, elapsedMs={3}, message={4}",
        new Object[] {LOG_PREFIX_PROGRESS, taskId, step, elapsedMs, message});
  }

  /**
   * 记录命令执行详情（FINER 级别）
   *
   * @param taskId 任务ID
   * @param command 命令内容
   * @param sessionId 会话ID
   */
  public void logCommandExecution(String taskId, String command, String sessionId) {
    if (!enabled || !logCommandDetails || !logger.isLoggable(Level.FINER)) {
      return;
    }

    logger.log(
        Level.FINER,
        "{0} taskId={1}, sessionId={2}, command={3}",
        new Object[] {LOG_PREFIX_PROGRESS, taskId, sessionId, truncate(command, 200)});
  }

  /**
   * 记录任务执行中的警告
   *
   * @param taskId 任务ID
   * @param warningCode 警告代码
   * @param message 警告信息
   */
  public void logTaskWarning(String taskId, String warningCode, String message) {
    if (!enabled) {
      return;
    }

    logger.log(
        Level.WARNING,
        "{0} taskId={1}, warningCode={2}, message={3}",
        new Object[] {LOG_PREFIX_PROGRESS, taskId, warningCode, message});
  }

  /**
   * 记录任务执行中的异常（但任务继续执行）
   *
   * @param taskId 任务ID
   * @param exception 异常信息
   * @param recoverable 是否可恢复
   */
  public void logTaskException(String taskId, Throwable exception, boolean recoverable) {
    if (!enabled) {
      return;
    }

    Level level = recoverable ? Level.WARNING : Level.SEVERE;
    logger.log(
        level,
        "{0} taskId={1}, recoverable={2}, error={3}",
        new Object[] {LOG_PREFIX_PROGRESS, taskId, recoverable, exception.getMessage()});

    // FINE 级别记录完整堆栈
    if (logger.isLoggable(Level.FINE)) {
      logger.log(Level.FINE, "Task exception stacktrace for " + taskId, exception);
    }
  }

  // ==================== 任务完成阶段 ====================

  /**
   * 记录任务成功完成
   *
   * @param taskId 任务ID
   */
  public void logTaskCompleted(String taskId) {
    logTaskCompleted(taskId, null, 0);
  }

  /**
   * 记录任务成功完成（带结果摘要）
   *
   * @param taskId 任务ID
   * @param result 执行结果摘要
   */
  public void logTaskCompleted(String taskId, @Nullable String result) {
    logTaskCompleted(taskId, result, 0);
  }

  /**
   * 记录任务成功完成（带详细信息）
   *
   * @param taskId 任务ID
   * @param result 执行结果摘要
   * @param outputSize 输出大小（字节）
   */
  public void logTaskCompleted(String taskId, @Nullable String result, long outputSize) {
    TaskContext context = activeContexts.remove(taskId);
    long executionTimeMs = context != null ? context.markCompleted() : 0;
    long totalTimeMs = context != null ? context.getTotalTimeMs() : 0;

    if (!enabled) {
      return;
    }

    logger.log(
        Level.INFO,
        "{0} taskId={1}, status=SUCCESS, executionTimeMs={2}, totalTimeMs={3}, outputBytes={4}",
        new Object[] {LOG_PREFIX_COMPLETED, taskId, executionTimeMs, totalTimeMs, outputSize});

    // FINE 级别：结果摘要
    if (result != null && logger.isLoggable(Level.FINE)) {
      logger.log(
          Level.FINE,
          "{0} taskId={1}, resultSummary={2}",
          new Object[] {LOG_PREFIX_COMPLETED, taskId, truncate(result, maxResultLogLength)});
    }
  }

  /**
   * 记录任务失败
   *
   * @param taskId 任务ID
   * @param errorCode 错误代码（使用 ERROR_* 常量）
   * @param errorMessage 错误信息（可为 null）
   */
  public void logTaskFailed(String taskId, String errorCode, @Nullable String errorMessage) {
    logTaskFailed(taskId, errorCode, errorMessage, null);
  }

  /**
   * 记录任务失败（带异常）
   *
   * @param taskId 任务ID
   * @param errorCode 错误代码（使用 ERROR_* 常量）
   * @param errorMessage 错误信息（可为 null）
   * @param exception 异常（可选）
   */
  public void logTaskFailed(
      String taskId, String errorCode, @Nullable String errorMessage, @Nullable Throwable exception) {

    TaskContext context = activeContexts.remove(taskId);
    long executionTimeMs = context != null ? context.markCompleted() : 0;
    long totalTimeMs = context != null ? context.getTotalTimeMs() : 0;

    if (!enabled) {
      return;
    }

    // 处理 null 错误消息
    String safeErrorMessage = errorMessage != null ? errorMessage : "Unknown error";

    logger.log(
        Level.SEVERE,
        "{0} taskId={1}, status=FAILED, errorCode={2}, errorMessage={3}, executionTimeMs={4}, totalTimeMs={5}",
        new Object[] {
          LOG_PREFIX_FAILED, taskId, errorCode, safeErrorMessage, executionTimeMs, totalTimeMs
        });

    if (exception != null && logger.isLoggable(Level.FINE)) {
      logger.log(Level.FINE, "Task failure stacktrace for " + taskId, exception);
    }
  }

  /**
   * 记录任务超时
   *
   * @param taskId 任务ID
   * @param configuredTimeout 配置的超时时间
   */
  public void logTaskTimeout(String taskId, Duration configuredTimeout) {
    TaskContext context = activeContexts.remove(taskId);
    long actualDurationMs = context != null ? context.getElapsedMs() : 0;

    if (!enabled) {
      return;
    }

    logger.log(
        Level.WARNING,
        "{0} taskId={1}, status=TIMEOUT, configuredTimeoutMs={2}, actualDurationMs={3}",
        new Object[] {LOG_PREFIX_TIMEOUT, taskId, configuredTimeout.toMillis(), actualDurationMs});
  }

  /**
   * 记录任务取消
   *
   * @param taskId 任务ID
   * @param reason 取消原因
   */
  public void logTaskCancelled(String taskId, String reason) {
    TaskContext context = activeContexts.remove(taskId);
    long elapsedMs = context != null ? context.getElapsedMs() : 0;

    if (!enabled) {
      return;
    }

    logger.log(
        Level.INFO,
        "{0} taskId={1}, status=CANCELLED, reason={2}, elapsedMs={3}",
        new Object[] {LOG_PREFIX_CANCELLED, taskId, reason, elapsedMs});
  }

  // ==================== 便捷方法 ====================

  /**
   * 创建任务详情 Map 的 Builder
   *
   * @return DetailsBuilder 实例
   */
  public static DetailsBuilder details() {
    return new DetailsBuilder();
  }

  /**
   * 获取任务上下文
   *
   * @param taskId 任务ID
   * @return TaskContext 或 null
   */
  @Nullable
  public TaskContext getContext(String taskId) {
    return activeContexts.get(taskId);
  }

  /**
   * 获取活跃任务数量
   *
   * @return 活跃任务数量
   */
  public int getActiveTaskCount() {
    return activeContexts.size();
  }

  /**
   * 清理指定任务的上下文（用于异常情况）
   *
   * @param taskId 任务ID
   */
  public void clearContext(String taskId) {
    activeContexts.remove(taskId);
  }

  // ==================== 辅助方法 ====================

  private static String formatDetails(Map<String, Object> details) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<String, Object> entry : details.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(entry.getKey()).append("=");
      Object value = entry.getValue();
      if (value instanceof String) {
        sb.append("\"").append(truncate((String) value, 100)).append("\"");
      } else {
        sb.append(value);
      }
      first = false;
    }
    sb.append("}");
    return sb.toString();
  }

  private static String truncate(String str, int maxLength) {
    if (str == null) {
      return "null";
    }
    if (str.length() <= maxLength) {
      return str;
    }
    return str.substring(0, maxLength) + "...[truncated]";
  }

  // ==================== 内部类 ====================

  /**
   * 任务执行上下文，用于追踪任务的整个生命周期
   *
   * <p>记录以下时间指标：
   *
   * <ul>
   *   <li>receivedAt: 任务接收时间
   *   <li>startedAt: 任务开始执行时间
   *   <li>completedAt: 任务完成时间
   *   <li>queueTime: 排队时间（receivedAt -> startedAt）
   *   <li>executionTime: 执行时间（startedAt -> completedAt）
   *   <li>totalTime: 总时间（receivedAt -> completedAt）
   * </ul>
   */
  public static final class TaskContext {

    private final String taskId;
    private final String taskType;
    private final String source;
    private final Instant receivedAt;

    @Nullable private volatile Instant startedAt;
    @Nullable private volatile Instant completedAt;
    @Nullable private volatile String priority;
    @Nullable private volatile Duration timeout;

    TaskContext(String taskId, String taskType, String source) {
      this.taskId = taskId;
      this.taskType = taskType;
      this.source = source;
      this.receivedAt = Instant.now();
    }

    /**
     * 获取任务ID
     *
     * @return 任务ID
     */
    public String getTaskId() {
      return taskId;
    }

    /**
     * 获取任务类型
     *
     * @return 任务类型
     */
    public String getTaskType() {
      return taskType;
    }

    /**
     * 获取任务来源
     *
     * @return 任务来源
     */
    public String getSource() {
      return source;
    }

    /**
     * 获取任务接收时间
     *
     * @return 接收时间
     */
    public Instant getReceivedAt() {
      return receivedAt;
    }

    /**
     * 获取任务开始时间
     *
     * @return 开始时间，未开始时返回 null
     */
    @Nullable
    public Instant getStartedAt() {
      return startedAt;
    }

    /**
     * 获取优先级
     *
     * @return 优先级
     */
    @Nullable
    public String getPriority() {
      return priority;
    }

    /**
     * 获取超时设置
     *
     * @return 超时时间
     */
    @Nullable
    public Duration getTimeout() {
      return timeout;
    }

    void setPriority(@Nullable String priority) {
      this.priority = priority;
    }

    void setTimeout(@Nullable Duration timeout) {
      this.timeout = timeout;
    }

    void markStarted() {
      this.startedAt = Instant.now();
    }

    long markCompleted() {
      this.completedAt = Instant.now();
      return getExecutionTimeMs();
    }

    /**
     * 获取排队时间（接收到开始执行）
     *
     * @return 排队时间（毫秒）
     */
    public long getQueueTimeMs() {
      if (startedAt == null) {
        return 0;
      }
      return Duration.between(receivedAt, startedAt).toMillis();
    }

    /**
     * 获取执行时间（开始到完成）
     *
     * @return 执行时间（毫秒）
     */
    public long getExecutionTimeMs() {
      if (startedAt == null) {
        return 0;
      }
      Instant end = completedAt != null ? completedAt : Instant.now();
      return Duration.between(startedAt, end).toMillis();
    }

    /**
     * 获取已用时间（从接收到现在/完成）
     *
     * @return 已用时间（毫秒）
     */
    public long getElapsedMs() {
      Instant end = completedAt != null ? completedAt : Instant.now();
      return Duration.between(receivedAt, end).toMillis();
    }

    /**
     * 获取总时间（接收到完成）
     *
     * @return 总时间（毫秒）
     */
    public long getTotalTimeMs() {
      if (completedAt == null) {
        return getElapsedMs();
      }
      return Duration.between(receivedAt, completedAt).toMillis();
    }

    /**
     * 检查任务是否已超时
     *
     * @return 是否超时
     */
    public boolean isTimedOut() {
      if (timeout == null) {
        return false;
      }
      return getElapsedMs() > timeout.toMillis();
    }
  }

  /** 任务详情 Map 构建器 */
  public static final class DetailsBuilder {

    private final Map<String, Object> details = new LinkedHashMap<>();

    DetailsBuilder() {}

    /**
     * 添加字符串值
     *
     * @param key 键
     * @param value 值
     * @return this
     */
    public DetailsBuilder put(String key, @Nullable String value) {
      if (value != null) {
        details.put(key, value);
      }
      return this;
    }

    /**
     * 添加数值
     *
     * @param key 键
     * @param value 值
     * @return this
     */
    public DetailsBuilder put(String key, long value) {
      details.put(key, value);
      return this;
    }

    /**
     * 添加数值
     *
     * @param key 键
     * @param value 值
     * @return this
     */
    public DetailsBuilder put(String key, int value) {
      details.put(key, value);
      return this;
    }

    /**
     * 添加布尔值
     *
     * @param key 键
     * @param value 值
     * @return this
     */
    public DetailsBuilder put(String key, boolean value) {
      details.put(key, value);
      return this;
    }

    /**
     * 添加对象值
     *
     * @param key 键
     * @param value 值
     * @return this
     */
    public DetailsBuilder putObject(String key, @Nullable Object value) {
      if (value != null) {
        details.put(key, value);
      }
      return this;
    }

    /**
     * 构建 Map
     *
     * @return 详情 Map
     */
    public Map<String, Object> build() {
      return details;
    }
  }
}
