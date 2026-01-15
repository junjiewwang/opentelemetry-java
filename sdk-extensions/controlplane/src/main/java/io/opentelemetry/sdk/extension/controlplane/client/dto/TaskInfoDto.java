/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import javax.annotation.Nullable;

/**
 * 任务信息 DTO
 *
 * <p>用于 Jackson 反序列化，从控制平面响应中解析任务信息。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class TaskInfoDto {

  @JsonProperty("task_id")
  @Nullable
  private String taskId;

  @JsonProperty("task_type")
  @Nullable
  private String taskType;

  @JsonProperty("parameters")
  @Nullable
  private JsonNode parameters;

  @JsonProperty("priority")
  private int priority;

  @JsonProperty("timeout_millis")
  private long timeoutMillis = 60000;

  @JsonProperty("created_at_millis")
  private long createdAtMillis;

  @JsonProperty("expires_at_millis")
  private long expiresAtMillis;

  @JsonProperty("max_acceptable_delay_millis")
  private long maxAcceptableDelayMillis;

  // Jackson 需要无参构造函数
  public TaskInfoDto() {}

  @Nullable
  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(@Nullable String taskId) {
    this.taskId = taskId;
  }

  @Nullable
  public String getTaskType() {
    return taskType;
  }

  public void setTaskType(@Nullable String taskType) {
    this.taskType = taskType;
  }

  @Nullable
  public JsonNode getParameters() {
    return parameters;
  }

  public void setParameters(@Nullable JsonNode parameters) {
    this.parameters = parameters;
  }

  /**
   * 获取参数的 JSON 字符串表示
   *
   * @return 参数 JSON 字符串，如果为空则返回 "{}"
   */
  public String getParametersJson() {
    return parameters != null ? parameters.toString() : "{}";
  }

  public int getPriority() {
    return priority;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }

  public long getTimeoutMillis() {
    return timeoutMillis;
  }

  public void setTimeoutMillis(long timeoutMillis) {
    this.timeoutMillis = timeoutMillis;
  }

  public long getCreatedAtMillis() {
    return createdAtMillis;
  }

  public void setCreatedAtMillis(long createdAtMillis) {
    this.createdAtMillis = createdAtMillis;
  }

  public long getExpiresAtMillis() {
    return expiresAtMillis;
  }

  public void setExpiresAtMillis(long expiresAtMillis) {
    this.expiresAtMillis = expiresAtMillis;
  }

  public long getMaxAcceptableDelayMillis() {
    return maxAcceptableDelayMillis;
  }

  public void setMaxAcceptableDelayMillis(long maxAcceptableDelayMillis) {
    this.maxAcceptableDelayMillis = maxAcceptableDelayMillis;
  }

  /**
   * 检查任务信息是否有效
   *
   * @return 如果 taskId 不为空则返回 true
   */
  public boolean isValid() {
    return taskId != null && !taskId.isEmpty();
  }
}
