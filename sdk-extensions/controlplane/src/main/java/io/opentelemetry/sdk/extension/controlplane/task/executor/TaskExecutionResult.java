/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.executor;

import java.util.Locale;
import javax.annotation.Nullable;

/**
 * 任务执行结果
 *
 * <p>封装任务执行的最终状态，包括：
 * <ul>
 *   <li>状态（SUCCESS/FAILED/TIMEOUT/CANCELLED）
 *   <li>错误码和错误信息（失败时）
 *   <li>结果数据（成功时，JSON 格式）
 *   <li>执行时间统计
 * </ul>
 *
 * <p>使用工厂方法创建实例：
 * <pre>{@code
 * // 成功
 * TaskExecutionResult.success("{\"agentId\":\"xxx\"}");
 *
 * // 失败
 * TaskExecutionResult.failed("ARTHAS_START_FAILED", "Failed to start Arthas: timeout");
 *
 * // 超时
 * TaskExecutionResult.timeout("Execution timeout after 60000ms");
 * }</pre>
 *
 * <p><b>Phase 5 重构</b>：使用内部定义的 {@link Status} 枚举，
 * 消除对旧 {@code ControlPlaneClient.TaskStatus} 的依赖。
 */
public final class TaskExecutionResult {

  /**
   * 任务执行状态（内部定义）
   *
   * <p>与 Protobuf 的 TaskResultStatus 枚举对应，由 TaskDispatcher 负责转换。
   */
  public enum Status {
    /** 任务待执行 */
    PENDING,
    /** 任务执行中 */
    RUNNING,
    /** 任务执行成功 */
    SUCCESS,
    /** 任务执行失败（包括过期、过旧、被拒绝等，具体原因见 error_code） */
    FAILED,
    /** 任务执行超时 */
    TIMEOUT,
    /** 任务被取消 */
    CANCELLED
  }

  private final Status status;
  @Nullable private final String errorCode;
  @Nullable private final String errorMessage;
  @Nullable private final String resultJson;
  private final long executionTimeMillis;
  private final long startedAtMillis;
  private final long completedAtMillis;

  private TaskExecutionResult(Builder builder) {
    this.status = builder.status;
    this.errorCode = builder.errorCode;
    this.errorMessage = builder.errorMessage;
    this.resultJson = builder.resultJson;
    this.executionTimeMillis = builder.executionTimeMillis;
    this.startedAtMillis = builder.startedAtMillis;
    this.completedAtMillis = builder.completedAtMillis;
  }

  // ===== 工厂方法 =====

  /**
   * 创建成功结果
   *
   * @return 成功结果
   */
  public static TaskExecutionResult success() {
    return success(null, 0);
  }

  /**
   * 创建成功结果（带结果数据）
   *
   * @param resultJson 结果数据（JSON 格式）
   * @return 成功结果
   */
  public static TaskExecutionResult success(@Nullable String resultJson) {
    return success(resultJson, 0);
  }

  /**
   * 创建成功结果（带结果数据和执行时间）
   *
   * @param resultJson 结果数据（JSON 格式）
   * @param executionTimeMillis 执行时间
   * @return 成功结果
   */
  public static TaskExecutionResult success(
      @Nullable String resultJson, long executionTimeMillis) {
    return builder()
        .status(Status.SUCCESS)
        .resultJson(resultJson)
        .executionTimeMillis(executionTimeMillis)
        .completedAtMillis(System.currentTimeMillis())
        .build();
  }

  /**
   * 创建运行中结果
   *
   * <p><b>已废弃</b>：TaskExecutionResult 应只用于表达终态（SUCCESS/FAILED/TIMEOUT/CANCELLED）。
   * RUNNING 状态应通过 {@code TaskStatusReporter.reportRunning()} 或 {@code TaskStatusEmitter.running()} 上报，
   * 而不是作为 Executor 的返回值。
   *
   * @param message 状态信息
   * @return 运行中结果
   * @deprecated 请使用 {@code TaskStatusReporter.reportRunning()} 或 {@code TaskStatusEmitter.running()} 上报进度
   */
  @Deprecated
  public static TaskExecutionResult running(@Nullable String message) {
    return builder()
        .status(Status.RUNNING)
        .resultJson(message)
        .startedAtMillis(System.currentTimeMillis())
        .build();
  }

  /**
   * 创建失败结果
   *
   * @param errorCode 错误码
   * @param errorMessage 错误信息
   * @return 失败结果
   */
  public static TaskExecutionResult failed(String errorCode, String errorMessage) {
    return failed(errorCode, errorMessage, 0);
  }

  /**
   * 创建失败结果（带执行时间）
   *
   * @param errorCode 错误码
   * @param errorMessage 错误信息
   * @param executionTimeMillis 执行时间
   * @return 失败结果
   */
  public static TaskExecutionResult failed(
      String errorCode, String errorMessage, long executionTimeMillis) {
    return builder()
        .status(Status.FAILED)
        .errorCode(errorCode)
        .errorMessage(errorMessage)
        .executionTimeMillis(executionTimeMillis)
        .completedAtMillis(System.currentTimeMillis())
        .build();
  }

  /**
   * 从异常创建失败结果
   *
   * @param errorCode 错误码
   * @param throwable 异常
   * @return 失败结果
   */
  public static TaskExecutionResult fromException(String errorCode, Throwable throwable) {
    String message = throwable.getMessage();
    return failed(errorCode, message != null ? message : throwable.getClass().getName());
  }

  /**
   * 创建超时结果
   *
   * @param message 超时信息
   * @return 超时结果
   */
  public static TaskExecutionResult timeout(String message) {
    return timeout(message, 0);
  }

  /**
   * 创建超时结果（带执行时间）
   *
   * @param message 超时信息
   * @param executionTimeMillis 执行时间
   * @return 超时结果
   */
  public static TaskExecutionResult timeout(String message, long executionTimeMillis) {
    return builder()
        .status(Status.TIMEOUT)
        .errorCode("EXECUTION_TIMEOUT")
        .errorMessage(message)
        .executionTimeMillis(executionTimeMillis)
        .completedAtMillis(System.currentTimeMillis())
        .build();
  }

  /**
   * 创建取消结果
   *
   * @param reason 取消原因
   * @return 取消结果
   */
  public static TaskExecutionResult cancelled(String reason) {
    return builder()
        .status(Status.CANCELLED)
        .errorCode("TASK_CANCELLED")
        .errorMessage(reason)
        .completedAtMillis(System.currentTimeMillis())
        .build();
  }

  // ===== Getters =====

  public Status getStatus() {
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

  public long getExecutionTimeMillis() {
    return executionTimeMillis;
  }

  public long getStartedAtMillis() {
    return startedAtMillis;
  }

  public long getCompletedAtMillis() {
    return completedAtMillis;
  }

  /**
   * 是否成功
   *
   * @return 是否成功
   */
  public boolean isSuccess() {
    return status == Status.SUCCESS;
  }

  /**
   * 是否失败
   *
   * @return 是否失败
   */
  public boolean isFailed() {
    return status == Status.FAILED;
  }

  /**
   * 是否超时
   *
   * @return 是否超时
   */
  public boolean isTimeout() {
    return status == Status.TIMEOUT;
  }

  /**
   * 是否取消
   *
   * @return 是否取消
   */
  public boolean isCancelled() {
    return status == Status.CANCELLED;
  }

  /**
   * 是否运行中
   *
   * @return 是否运行中
   */
  public boolean isRunning() {
    return status == Status.RUNNING;
  }

  @Override
  public String toString() {
    if (isSuccess()) {
      return String.format(
          Locale.ROOT,
          "TaskExecutionResult{status=%s, executionTime=%dms}",
          status, executionTimeMillis);
    }
    return String.format(
        Locale.ROOT,
        "TaskExecutionResult{status=%s, errorCode='%s', errorMessage='%s', executionTime=%dms}",
        status, errorCode, errorMessage, executionTimeMillis);
  }

  // ===== Builder =====

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private Status status = Status.PENDING;
    @Nullable private String errorCode;
    @Nullable private String errorMessage;
    @Nullable private String resultJson;
    private long executionTimeMillis = 0;
    private long startedAtMillis = System.currentTimeMillis();
    private long completedAtMillis = 0;

    public Builder status(Status status) {
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

    public Builder executionTimeMillis(long executionTimeMillis) {
      this.executionTimeMillis = executionTimeMillis;
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

    public TaskExecutionResult build() {
      return new TaskExecutionResult(this);
    }
  }
}
