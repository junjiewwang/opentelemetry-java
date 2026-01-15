/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskResultRequest;
import javax.annotation.Nullable;

/**
 * 任务结果上报请求 DTO
 *
 * <p>用于 Jackson 序列化，确保字段名与服务端 Go 结构体完全匹配。
 *
 * <p>对应服务端 Go 结构体：
 * <pre>
 * type TaskResult struct {
 *     TaskID              string          `json:"task_id"`
 *     AgentID             string          `json:"agent_id,omitempty"`
 *     Status              TaskStatus      `json:"status"`
 *     ErrorCode           string          `json:"error_code,omitempty"`
 *     ErrorMessage        string          `json:"error_message,omitempty"`
 *     Result              json.RawMessage `json:"result,omitempty"`
 *     ResultData          []byte          `json:"result_data,omitempty"`
 *     StartedAtMillis     int64           `json:"started_at_millis,omitempty"`
 *     CompletedAtMillis   int64           `json:"completed_at_millis"`
 *     ExecutionTimeMillis int64           `json:"execution_time_millis,omitempty"`
 * }
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class TaskResultRequestDto {

  @JsonProperty("task_id")
  private String taskId = "";

  @JsonProperty("agent_id")
  @Nullable
  private String agentId;

  @JsonProperty("status")
  private String status = "";

  @JsonProperty("error_code")
  @Nullable
  private String errorCode;

  @JsonProperty("error_message")
  @Nullable
  private String errorMessage;

  /**
   * 任务结果（原始 JSON，不做转义）
   *
   * <p>使用 @JsonRawValue 确保 JSON 内容直接嵌入，不做字符串转义
   */
  @JsonProperty("result")
  @JsonRawValue
  @Nullable
  private String result;

  @JsonProperty("started_at_millis")
  private long startedAtMillis;

  @JsonProperty("completed_at_millis")
  private long completedAtMillis;

  @JsonProperty("execution_time_millis")
  private long executionTimeMillis;

  /** Jackson 需要无参构造函数 */
  @SuppressWarnings("unused") // Jackson 需要
  private TaskResultRequestDto() {}

  /**
   * 从接口创建 DTO
   *
   * @param request 任务结果请求接口
   * @return DTO 实例
   */
  public static TaskResultRequestDto from(TaskResultRequest request) {
    TaskResultRequestDto dto = new TaskResultRequestDto();
    dto.setTaskId(request.getTaskId());
    dto.setAgentId(request.getAgentId());
    dto.setStatus(request.getStatus().name());
    dto.setErrorCode(request.getErrorCode());
    dto.setErrorMessage(request.getErrorMessage());
    dto.setResult(request.getResultJson());
    dto.setStartedAtMillis(request.getStartedAtMillis());
    dto.setCompletedAtMillis(request.getCompletedAtMillis());
    dto.setExecutionTimeMillis(request.getExecutionTimeMillis());
    return dto;
  }

  // ===== Getter 和 Setter =====

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  @Nullable
  public String getAgentId() {
    return agentId;
  }

  public void setAgentId(@Nullable String agentId) {
    this.agentId = agentId;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  @Nullable
  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(@Nullable String errorCode) {
    this.errorCode = errorCode;
  }

  @Nullable
  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(@Nullable String errorMessage) {
    this.errorMessage = errorMessage;
  }

  @Nullable
  public String getResult() {
    return result;
  }

  public void setResult(@Nullable String result) {
    this.result = result;
  }

  public long getStartedAtMillis() {
    return startedAtMillis;
  }

  public void setStartedAtMillis(long startedAtMillis) {
    this.startedAtMillis = startedAtMillis;
  }

  public long getCompletedAtMillis() {
    return completedAtMillis;
  }

  public void setCompletedAtMillis(long completedAtMillis) {
    this.completedAtMillis = completedAtMillis;
  }

  public long getExecutionTimeMillis() {
    return executionTimeMillis;
  }

  public void setExecutionTimeMillis(long executionTimeMillis) {
    this.executionTimeMillis = executionTimeMillis;
  }
}
