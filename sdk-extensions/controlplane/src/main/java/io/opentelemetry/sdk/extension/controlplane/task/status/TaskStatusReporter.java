/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.status;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneService;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.CommonProtos.TaskStatus;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.PollProtos.TaskResultRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.PollProtos.TaskResultResponse;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionResult;
import io.opentelemetry.sdk.extension.controlplane.util.JsonUtils;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 任务状态上报器
 *
 * <p>统一负责<b>任务执行状态</b>的上报（RUNNING / SUCCESS / FAILED / TIMEOUT / CANCELLED），
 * 消除 {@code TaskDispatcher} 和 {@code TaskLongPollHandler} 中重复的上报逻辑。
 *
 * <p><b>职责边界</b>：
 * <ul>
 *   <li>仅负责任务执行状态上报
 *   <li>不处理结果文件（由 {@code TaskResultLifecycleService} 处理）
 *   <li>不做重试与持久化（状态上报是轻量级操作，失败仅记录日志）
 * </ul>
 *
 * <p><b>幂等性保证</b>：
 * <ul>
 *   <li>终态（SUCCESS/FAILED/TIMEOUT/CANCELLED）只上报一次
 *   <li>RUNNING 状态可重复上报（由 TaskStatusEventManager 做节流）
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * TaskStatusReporter reporter = new TaskStatusReporter(service, agentId);
 *
 * // 上报任务开始运行
 * reporter.reportRunning(taskId, "Task started");
 *
 * // 上报任务成功
 * reporter.reportSuccess(taskId, resultJson, executionTimeMillis);
 *
 * // 上报任务失败
 * reporter.reportFailed(taskId, "ERROR_CODE", "Error message");
 * }</pre>
 */
public final class TaskStatusReporter {

  private static final Logger logger = Logger.getLogger(TaskStatusReporter.class.getName());

  private final ControlPlaneService service;
  private final String agentId;

  /** 每个任务当前已上报到服务端的终态（用于幂等） */
  private final ConcurrentHashMap<String, AtomicReference<TaskStatus>> reportedTerminalStatus;

  /**
   * 创建任务状态上报器
   *
   * @param service 控制平面服务
   * @param agentId Agent ID
   */
  public TaskStatusReporter(ControlPlaneService service, String agentId) {
    this.service = Objects.requireNonNull(service, "service is required");
    this.agentId = Objects.requireNonNull(agentId, "agentId is required");
    this.reportedTerminalStatus = new ConcurrentHashMap<>();
  }

  /**
   * 上报任务执行状态
   *
   * <p><b>协议约束</b>：
   * <ul>
   *   <li>{@code result_json} 必须是合法的 JSON 字符串，否则服务端解析会失败</li>
   *   <li>RUNNING 状态不应携带 {@code result_json}（进度信息放在日志中）</li>
   *   <li>只有终态（SUCCESS/FAILED/TIMEOUT/CANCELLED）才应携带业务结果</li>
   * </ul>
   *
   * @param report 状态报告
   * @return 上报结果 Future
   */
  public CompletableFuture<TaskStatusReportResponse> report(TaskStatusReport report) {
    Objects.requireNonNull(report, "report is required");

    String taskId = report.getTaskId();
    TaskStatus protoStatus = convertToProtoStatus(report.getStatus());

    // 【协议守门员】对 resultJson 进行校验和修正
    String safeResultJson = sanitizeResultJson(report.getResultJson(), report.getStatus());

    // 终态幂等检查
    if (isTerminalStatus(protoStatus)) {
      AtomicReference<TaskStatus> ref =
          reportedTerminalStatus.computeIfAbsent(taskId, k -> new AtomicReference<>());
      TaskStatus prev = ref.get();
      if (prev != null && isTerminalStatus(prev)) {
        // 已经上报过终态，跳过
        logger.log(
            Level.FINE,
            "[STATUS-REPORTER] Skipping duplicate terminal status report: taskId={0}, status={1}, prev={2}",
            new Object[] {taskId, report.getStatus(), prev});
        return CompletableFuture.completedFuture(
            TaskStatusReportResponse.skipped("Already reported terminal status: " + prev));
      }
      ref.set(protoStatus);
    }

    // 构建 Protobuf 请求（使用校验后的 safeResultJson）
    TaskResultRequest request = TaskResultRequest.newBuilder()
        .setTaskId(taskId)
        .setAgentId(agentId)
        .setStatus(protoStatus)
        .setErrorCode(report.getErrorCode() != null ? report.getErrorCode() : "")
        .setErrorMessage(report.getErrorMessage() != null ? report.getErrorMessage() : "")
        .setResultJson(safeResultJson)
        .setStartedAtMillis(report.getStartedAtMillis())
        .setCompletedAtMillis(report.getCompletedAtMillis())
        .setExecutionTimeMillis(report.getExecutionTimeMillis())
        .build();

    return service.reportTaskResult(request)
        .thenApply(response -> handleResponse(taskId, report.getStatus(), response))
        .exceptionally(error -> handleError(taskId, report.getStatus(), error));
  }

  /**
   * 上报任务执行结果（便捷方法）
   *
   * @param taskId 任务 ID
   * @param result 执行结果
   * @return 上报结果 Future
   */
  public CompletableFuture<TaskStatusReportResponse> report(String taskId, TaskExecutionResult result) {
    TaskStatusReport report = TaskStatusReport.fromExecutionResult(taskId, result);
    return report(report);
  }

  /**
   * 上报运行中状态
   *
   * <p><b>注意</b>：RUNNING 状态不会将 message 放入 {@code result_json}，
   * 因为服务端期望 {@code result_json} 是合法 JSON，而进度消息通常是自由文本。
   * message 仅用于本地日志记录。
   *
   * @param taskId 任务 ID
   * @param message 状态信息（仅用于日志，不会上报到 result_json）
   * @return 上报结果 Future
   */
  public CompletableFuture<TaskStatusReportResponse> reportRunning(String taskId, @Nullable String message) {
    // 记录进度日志（message 不放入 resultJson）
    if (message != null && !message.isEmpty()) {
      logger.log(
          Level.FINE,
          "[STATUS-REPORTER] Task running: taskId={0}, message={1}",
          new Object[] {taskId, message});
    }

    TaskStatusReport report = TaskStatusReport.builder()
        .taskId(taskId)
        .status(TaskExecutionResult.Status.RUNNING)
        // 【重构】RUNNING 状态不再携带 resultJson，避免服务端 JSON 解析失败
        .resultJson(null)
        .startedAtMillis(System.currentTimeMillis())
        .build();
    return report(report);
  }

  /**
   * 上报成功状态
   *
   * @param taskId 任务 ID
   * @param resultJson 结果 JSON
   * @param executionTimeMillis 执行时间
   * @return 上报结果 Future
   */
  public CompletableFuture<TaskStatusReportResponse> reportSuccess(
      String taskId,
      @Nullable String resultJson,
      long executionTimeMillis) {
    long now = System.currentTimeMillis();
    TaskStatusReport report = TaskStatusReport.builder()
        .taskId(taskId)
        .status(TaskExecutionResult.Status.SUCCESS)
        .resultJson(resultJson)
        .completedAtMillis(now)
        .executionTimeMillis(executionTimeMillis)
        .build();
    return report(report);
  }

  /**
   * 上报失败状态
   *
   * @param taskId 任务 ID
   * @param errorCode 错误码
   * @param errorMessage 错误信息
   * @return 上报结果 Future
   */
  public CompletableFuture<TaskStatusReportResponse> reportFailed(
      String taskId,
      String errorCode,
      String errorMessage) {
    long now = System.currentTimeMillis();
    TaskStatusReport report = TaskStatusReport.builder()
        .taskId(taskId)
        .status(TaskExecutionResult.Status.FAILED)
        .errorCode(errorCode)
        .errorMessage(errorMessage)
        .completedAtMillis(now)
        .build();
    return report(report);
  }

  /**
   * 上报超时状态
   *
   * @param taskId 任务 ID
   * @param message 超时信息
   * @param executionTimeMillis 执行时间
   * @return 上报结果 Future
   */
  public CompletableFuture<TaskStatusReportResponse> reportTimeout(
      String taskId,
      String message,
      long executionTimeMillis) {
    long now = System.currentTimeMillis();
    TaskStatusReport report = TaskStatusReport.builder()
        .taskId(taskId)
        .status(TaskExecutionResult.Status.TIMEOUT)
        .errorCode("EXECUTION_TIMEOUT")
        .errorMessage(message)
        .completedAtMillis(now)
        .executionTimeMillis(executionTimeMillis)
        .build();
    return report(report);
  }

  /**
   * 上报取消状态
   *
   * @param taskId 任务 ID
   * @param reason 取消原因
   * @return 上报结果 Future
   */
  public CompletableFuture<TaskStatusReportResponse> reportCancelled(String taskId, String reason) {
    long now = System.currentTimeMillis();
    TaskStatusReport report = TaskStatusReport.builder()
        .taskId(taskId)
        .status(TaskExecutionResult.Status.CANCELLED)
        .errorCode("TASK_CANCELLED")
        .errorMessage(reason)
        .completedAtMillis(now)
        .build();
    return report(report);
  }

  // ===== Fire-and-Forget 便捷方法 =====
  // 以下方法用于不关心上报结果的场景，内部已处理异常并记录日志

  /**
   * 异步上报状态（Fire-and-Forget 模式）
   *
   * <p>适用于不需要等待上报结果的场景，失败时仅记录日志，不阻塞调用方。
   *
   * @param report 状态报告
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public void fire(TaskStatusReport report) {
    report(report);
  }

  /**
   * 异步上报任务执行结果（Fire-and-Forget 模式）
   *
   * @param taskId 任务 ID
   * @param result 执行结果
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public void fire(String taskId, TaskExecutionResult result) {
    report(taskId, result);
  }

  /**
   * 异步上报运行中状态（Fire-and-Forget 模式）
   *
   * <p>RUNNING 状态是非终态，通常不需要关心上报结果。
   *
   * @param taskId 任务 ID
   * @param message 状态信息
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public void fireRunning(String taskId, @Nullable String message) {
    reportRunning(taskId, message);
  }

  /**
   * 异步上报成功状态（Fire-and-Forget 模式）
   *
   * <p>适用于结果已通过其他方式持久化或上传的场景。
   *
   * @param taskId 任务 ID
   * @param resultJson 结果 JSON
   * @param executionTimeMillis 执行时间
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public void fireSuccess(String taskId, @Nullable String resultJson, long executionTimeMillis) {
    reportSuccess(taskId, resultJson, executionTimeMillis);
  }

  /**
   * 异步上报失败状态（Fire-and-Forget 模式）
   *
   * @param taskId 任务 ID
   * @param errorCode 错误码
   * @param errorMessage 错误信息
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public void fireFailed(String taskId, String errorCode, String errorMessage) {
    reportFailed(taskId, errorCode, errorMessage);
  }

  /**
   * 异步上报超时状态（Fire-and-Forget 模式）
   *
   * @param taskId 任务 ID
   * @param message 超时信息
   * @param executionTimeMillis 执行时间
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public void fireTimeout(String taskId, String message, long executionTimeMillis) {
    reportTimeout(taskId, message, executionTimeMillis);
  }

  /**
   * 异步上报取消状态（Fire-and-Forget 模式）
   *
   * @param taskId 任务 ID
   * @param reason 取消原因
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public void fireCancelled(String taskId, String reason) {
    reportCancelled(taskId, reason);
  }

  /**
   * 清理已完成任务的状态记录
   *
   * @param taskId 任务 ID
   */
  public void cleanup(String taskId) {
    reportedTerminalStatus.remove(taskId);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private TaskStatusReportResponse handleResponse(
      String taskId,
      TaskExecutionResult.Status status,
      TaskResultResponse response) {
    if (response.getAcknowledged()) {
      logger.log(
          Level.INFO,
          "[STATUS-REPORTER] Task status reported: taskId={0}, status={1}",
          new Object[] {taskId, status});
      return TaskStatusReportResponse.success();
    } else {
      String message = response.getStatus().getMessage();
      logger.log(
          Level.WARNING,
          "[STATUS-REPORTER] Server rejected status report: taskId={0}, status={1}, error={2}",
          new Object[] {taskId, status, message});
      return TaskStatusReportResponse.rejected(message);
    }
  }

  @SuppressWarnings("MethodCanBeStatic")
  private TaskStatusReportResponse handleError(
      String taskId,
      TaskExecutionResult.Status status,
      Throwable error) {
    String errorMessage = error.getMessage() != null ? error.getMessage() : error.getClass().getName();
    logger.log(
        Level.WARNING,
        "[STATUS-REPORTER] Failed to report task status: taskId={0}, status={1}, error={2}",
        new Object[] {taskId, status, errorMessage});
    return TaskStatusReportResponse.error(errorMessage);
  }

  /**
   * 转换内部状态到 Protobuf 状态
   */
  private static TaskStatus convertToProtoStatus(TaskExecutionResult.Status status) {
    switch (status) {
      case PENDING:
        return TaskStatus.TASK_STATUS_PENDING;
      case RUNNING:
        return TaskStatus.TASK_STATUS_RUNNING;
      case SUCCESS:
        return TaskStatus.TASK_STATUS_SUCCESS;
      case FAILED:
        return TaskStatus.TASK_STATUS_FAILED;
      case TIMEOUT:
        return TaskStatus.TASK_STATUS_TIMEOUT;
      case CANCELLED:
        return TaskStatus.TASK_STATUS_CANCELLED;
    }
    return TaskStatus.TASK_STATUS_UNSPECIFIED;
  }

  /**
   * 判断是否为终态
   */
  private static boolean isTerminalStatus(TaskStatus status) {
    if (status == null) {
      return false;
    }
    return status == TaskStatus.TASK_STATUS_SUCCESS
        || status == TaskStatus.TASK_STATUS_FAILED
        || status == TaskStatus.TASK_STATUS_TIMEOUT
        || status == TaskStatus.TASK_STATUS_CANCELLED;
  }

  // ===== 协议守门员：resultJson 校验与修正 =====

  /**
   * 对 resultJson 进行校验和修正，确保满足协议约束
   *
   * <p><b>规则</b>：
   * <ul>
   *   <li>RUNNING 状态：不携带 resultJson（返回空字符串）</li>
   *   <li>终态：如果 resultJson 是合法 JSON 则原样返回，否则自动封装</li>
   *   <li>null 或空字符串：返回空字符串</li>
   * </ul>
   *
   * @param resultJson 原始 resultJson
   * @param status 任务状态
   * @return 校验/修正后的 resultJson
   */
  private static String sanitizeResultJson(@Nullable String resultJson, TaskExecutionResult.Status status) {
    // RUNNING 状态不携带 resultJson
    if (status == TaskExecutionResult.Status.RUNNING) {
      if (resultJson != null && !resultJson.isEmpty()) {
        logger.log(
            Level.FINE,
            "[STATUS-REPORTER] Dropping resultJson for RUNNING status (protocol constraint): {0}",
            truncateForLog(resultJson));
      }
      return "";
    }

    // null 或空字符串直接返回
    if (resultJson == null || resultJson.isEmpty()) {
      return "";
    }

    // 检查是否是合法 JSON
    if (isValidJson(resultJson)) {
      return resultJson;
    }

    // 非法 JSON：自动封装为合法 JSON
    logger.log(
        Level.WARNING,
        "[STATUS-REPORTER] resultJson is not valid JSON, auto-wrapping: {0}",
        truncateForLog(resultJson));
    return wrapAsJson(resultJson);
  }

  /**
   * 简单校验是否是合法 JSON
   *
   * <p>只做首字符检查，避免引入重量级 JSON 解析库。
   * 合法 JSON 的首字符必须是：<code>{ [ " n t f</code> 或数字。
   *
   * @param str 待检查字符串
   * @return 是否可能是合法 JSON
   */
  private static boolean isValidJson(@Nullable String str) {
    if (str == null || str.isEmpty()) {
      return false;
    }
    char first = str.trim().charAt(0);
    // JSON 合法首字符：{ [ " n(ull) t(rue) f(alse) 或数字
    return first == '{' || first == '[' || first == '"'
        || first == 'n' || first == 't' || first == 'f'
        || first == '-' || Character.isDigit(first);
  }

  /**
   * 将非 JSON 文本封装为合法 JSON
   *
   * <p>【重构】使用 JsonUtils.escapeJson 替代手动转义，避免遗漏特殊字符
   *
   * @param text 原始文本
   * @return 封装后的 JSON
   */
  private static String wrapAsJson(String text) {
    // 使用 JsonUtils.toJsonObject 构建合法 JSON，自动处理转义
    return JsonUtils.toJsonObject("message", text);
  }

  /**
   * 截断日志输出（避免过长）
   */
  private static String truncateForLog(String str) {
    if (str == null) {
      return "null";
    }
    return str.length() > 100 ? str.substring(0, 100) + "..." : str;
  }

  // ===== 内部数据类 =====

  /**
   * 任务状态报告
   */
  public static final class TaskStatusReport {

    private final String taskId;
    private final TaskExecutionResult.Status status;
    @Nullable private final String errorCode;
    @Nullable private final String errorMessage;
    @Nullable private final String resultJson;
    private final long startedAtMillis;
    private final long completedAtMillis;
    private final long executionTimeMillis;

    private TaskStatusReport(Builder builder) {
      this.taskId = Objects.requireNonNull(builder.taskId, "taskId is required");
      this.status = Objects.requireNonNull(builder.status, "status is required");
      this.errorCode = builder.errorCode;
      this.errorMessage = builder.errorMessage;
      this.resultJson = builder.resultJson;
      this.startedAtMillis = builder.startedAtMillis;
      this.completedAtMillis = builder.completedAtMillis;
      this.executionTimeMillis = builder.executionTimeMillis;
    }

    public static TaskStatusReport fromExecutionResult(String taskId, TaskExecutionResult result) {
      return builder()
          .taskId(taskId)
          .status(result.getStatus())
          .errorCode(result.getErrorCode())
          .errorMessage(result.getErrorMessage())
          .resultJson(result.getResultJson())
          .startedAtMillis(result.getStartedAtMillis())
          .completedAtMillis(result.getCompletedAtMillis())
          .executionTimeMillis(result.getExecutionTimeMillis())
          .build();
    }

    public static Builder builder() {
      return new Builder();
    }

    public String getTaskId() {
      return taskId;
    }

    public TaskExecutionResult.Status getStatus() {
      return status;
    }

    @Nullable
    public String getErrorCode() {
      return errorCode;
    }

    @Nullable
    public String getErrorMessage() {
      return errorMessage;
    }

    @Nullable
    public String getResultJson() {
      return resultJson;
    }

    public long getStartedAtMillis() {
      return startedAtMillis;
    }

    public long getCompletedAtMillis() {
      return completedAtMillis;
    }

    public long getExecutionTimeMillis() {
      return executionTimeMillis;
    }

    public static final class Builder {
      @Nullable private String taskId;
      @Nullable private TaskExecutionResult.Status status;
      @Nullable private String errorCode;
      @Nullable private String errorMessage;
      @Nullable private String resultJson;
      private long startedAtMillis;
      private long completedAtMillis;
      private long executionTimeMillis;

      public Builder taskId(String taskId) {
        this.taskId = taskId;
        return this;
      }

      public Builder status(TaskExecutionResult.Status status) {
        this.status = status;
        return this;
      }

      public Builder errorCode(@Nullable String errorCode) {
        this.errorCode = errorCode;
        return this;
      }

      public Builder errorMessage(@Nullable String errorMessage) {
        this.errorMessage = errorMessage;
        return this;
      }

      public Builder resultJson(@Nullable String resultJson) {
        this.resultJson = resultJson;
        return this;
      }

      public Builder startedAtMillis(long startedAtMillis) {
        this.startedAtMillis = startedAtMillis;
        return this;
      }

      public Builder completedAtMillis(long completedAtMillis) {
        this.completedAtMillis = completedAtMillis;
        return this;
      }

      public Builder executionTimeMillis(long executionTimeMillis) {
        this.executionTimeMillis = executionTimeMillis;
        return this;
      }

      public TaskStatusReport build() {
        return new TaskStatusReport(this);
      }
    }
  }

  /**
   * 任务状态上报响应
   */
  public static final class TaskStatusReportResponse {

    public enum Status {
      /** 上报成功 */
      SUCCESS,
      /** 被服务端拒绝 */
      REJECTED,
      /** 上报出错 */
      ERROR,
      /** 跳过（已上报过终态） */
      SKIPPED
    }

    private final Status status;
    @Nullable private final String message;

    private TaskStatusReportResponse(Status status, @Nullable String message) {
      this.status = status;
      this.message = message;
    }

    public static TaskStatusReportResponse success() {
      return new TaskStatusReportResponse(Status.SUCCESS, null);
    }

    public static TaskStatusReportResponse rejected(String message) {
      return new TaskStatusReportResponse(Status.REJECTED, message);
    }

    public static TaskStatusReportResponse error(String message) {
      return new TaskStatusReportResponse(Status.ERROR, message);
    }

    public static TaskStatusReportResponse skipped(String message) {
      return new TaskStatusReportResponse(Status.SKIPPED, message);
    }

    public Status getStatus() {
      return status;
    }

    @Nullable
    public String getMessage() {
      return message;
    }

    public boolean isSuccess() {
      return status == Status.SUCCESS;
    }

    public boolean isSkipped() {
      return status == Status.SKIPPED;
    }
  }
}
