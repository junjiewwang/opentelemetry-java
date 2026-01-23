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
   * @param report 状态报告
   * @return 上报结果 Future
   */
  public CompletableFuture<TaskStatusReportResponse> report(TaskStatusReport report) {
    Objects.requireNonNull(report, "report is required");

    String taskId = report.getTaskId();
    TaskStatus protoStatus = convertToProtoStatus(report.getStatus());

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

    // 构建 Protobuf 请求
    TaskResultRequest request = TaskResultRequest.newBuilder()
        .setTaskId(taskId)
        .setAgentId(agentId)
        .setStatus(protoStatus)
        .setErrorCode(report.getErrorCode() != null ? report.getErrorCode() : "")
        .setErrorMessage(report.getErrorMessage() != null ? report.getErrorMessage() : "")
        .setResultJson(report.getResultJson() != null ? report.getResultJson() : "")
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
   * @param taskId 任务 ID
   * @param message 状态信息
   * @return 上报结果 Future
   */
  public CompletableFuture<TaskStatusReportResponse> reportRunning(String taskId, @Nullable String message) {
    TaskStatusReport report = TaskStatusReport.builder()
        .taskId(taskId)
        .status(TaskExecutionResult.Status.RUNNING)
        .resultJson(message)
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
