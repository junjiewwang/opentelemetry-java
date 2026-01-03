/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.executor;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskStatus;
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
 */
public final class TaskExecutionResult {

  private final TaskStatus status;
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
        .status(TaskStatus.SUCCESS)
        .resultJson(resultJson)
        .executionTimeMillis(executionTimeMillis)
        .completedAtMillis(System.currentTimeMillis())
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
        .status(TaskStatus.FAILED)
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
        .status(TaskStatus.TIMEOUT)
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
        .status(TaskStatus.CANCELLED)
        .errorCode("TASK_CANCELLED")
        .errorMessage(reason)
        .completedAtMillis(System.currentTimeMillis())
        .build();
  }

  // ===== Getters =====

  public TaskStatus getStatus() {
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
    return status == TaskStatus.SUCCESS;
  }

  /**
   * 是否失败
   *
   * @return 是否失败
   */
  public boolean isFailed() {
    return status == TaskStatus.FAILED;
  }

  /**
   * 是否超时
   *
   * @return 是否超时
   */
  public boolean isTimeout() {
    return status == TaskStatus.TIMEOUT;
  }

  /**
   * 是否取消
   *
   * @return 是否取消
   */
  public boolean isCancelled() {
    return status == TaskStatus.CANCELLED;
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
    private TaskStatus status = TaskStatus.PENDING;
    @Nullable private String errorCode;
    @Nullable private String errorMessage;
    @Nullable private String resultJson;
    private long executionTimeMillis = 0;
    private long startedAtMillis = System.currentTimeMillis();
    private long completedAtMillis = 0;

    public Builder status(TaskStatus status) {
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
