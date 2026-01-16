package io.opentelemetry.sdk.extension.controlplane.task.status;

import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionResult;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * 任务状态事件（事件驱动上报）。
 *
 * <p>用于将任务执行过程中的关键节点（RUNNING / SUCCESS / FAILED 等）抽象为事件，
 * 由统一的事件管理器汇总、去重、节流，并最终转换为 {@link TaskExecutionResult} 上报到服务端。
 *
 * <p><b>Phase 5 重构</b>：使用 {@link TaskExecutionResult.Status} 替代旧的
 * {@code ControlPlaneClient.TaskStatus}。
 */
public final class TaskStatusEvent {

  private final String taskId;
  private final String agentId;
  private final TaskExecutionResult.Status status;
  @Nullable private final String errorCode;
  @Nullable private final String errorMessage;
  @Nullable private final String resultJson;
  private final long timestampMillis;

  private TaskStatusEvent(
      String taskId,
      String agentId,
      TaskExecutionResult.Status status,
      @Nullable String errorCode,
      @Nullable String errorMessage,
      @Nullable String resultJson,
      long timestampMillis) {
    this.taskId = Objects.requireNonNull(taskId, "taskId");
    this.agentId = Objects.requireNonNull(agentId, "agentId");
    this.status = Objects.requireNonNull(status, "status");
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
    this.resultJson = resultJson;
    this.timestampMillis = timestampMillis;
  }

  public static TaskStatusEvent running(String taskId, String agentId, String message) {
    String json =
        "{\"status\":\"running\",\"message\":" + escapeJsonString(message) + "}";
    long now = System.currentTimeMillis();
    return new TaskStatusEvent(taskId, agentId, TaskExecutionResult.Status.RUNNING, null, null, json, now);
  }

  public static TaskStatusEvent success(String taskId, String agentId, @Nullable String resultJson) {
    long now = System.currentTimeMillis();
    return new TaskStatusEvent(taskId, agentId, TaskExecutionResult.Status.SUCCESS, null, null, resultJson, now);
  }

  public static TaskStatusEvent failed(
      String taskId, String agentId, String errorCode, String errorMessage) {
    long now = System.currentTimeMillis();
    return new TaskStatusEvent(
        taskId, agentId, TaskExecutionResult.Status.FAILED, errorCode, errorMessage, null, now);
  }

  public String getTaskId() {
    return taskId;
  }

  public String getAgentId() {
    return agentId;
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

  public long getTimestampMillis() {
    return timestampMillis;
  }

  public TaskExecutionResult toExecutionResult() {
    TaskExecutionResult.Builder b = TaskExecutionResult.builder().status(status);

    if (status == TaskExecutionResult.Status.RUNNING) {
      // RUNNING：没有完成时间，保持 startedAt 为事件时间，completedAt = startedAt
      b.startedAtMillis(timestampMillis).completedAtMillis(timestampMillis).executionTimeMillis(0);
      if (resultJson != null) {
        b.resultJson(resultJson);
      }
      return b.build();
    }

    // SUCCESS/FAILED 等：用事件时间作为 completedAt
    b.startedAtMillis(timestampMillis)
        .completedAtMillis(timestampMillis)
        .executionTimeMillis(0);

    if (status == TaskExecutionResult.Status.SUCCESS) {
      b.resultJson(resultJson);
    } else {
      b.errorCode(errorCode).errorMessage(errorMessage);
    }

    return b.build();
  }

  private static String escapeJsonString(String s) {
    String escaped = s.replace("\\", "\\\\").replace("\"", "\\\"");
    return "\"" + escaped + "\"";
  }
}
