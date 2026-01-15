/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskRequest;

/**
 * 任务轮询请求 DTO
 *
 * <p>用于 Jackson 序列化，确保字段名与服务端 Go 结构体完全匹配。
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class TaskPollRequestDto {

  @JsonProperty("agent_id")
  private String agentId = "";

  @JsonProperty("timeout_millis")
  private long timeoutMillis;

  /** Jackson 需要无参构造函数 */
  @SuppressWarnings("unused") // Jackson 需要
  private TaskPollRequestDto() {}

  /**
   * 从接口创建 DTO
   *
   * @param request 任务请求接口
   * @return DTO 实例
   */
  public static TaskPollRequestDto from(TaskRequest request) {
    TaskPollRequestDto dto = new TaskPollRequestDto();
    dto.setAgentId(request.getAgentId());
    dto.setTimeoutMillis(request.getLongPollTimeoutMillis());
    return dto;
  }

  // ===== Getter 和 Setter =====

  public String getAgentId() {
    return agentId;
  }

  public void setAgentId(String agentId) {
    this.agentId = agentId;
  }

  public long getTimeoutMillis() {
    return timeoutMillis;
  }

  public void setTimeoutMillis(long timeoutMillis) {
    this.timeoutMillis = timeoutMillis;
  }
}
