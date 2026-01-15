/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.UnifiedPollRequest;
import javax.annotation.Nullable;

/**
 * 统一轮询请求 DTO
 *
 * <p>用于 Jackson 序列化，确保字段名与服务端 Go 结构体完全匹配。
 *
 * <p>对应服务端 Go 结构体：
 * <pre>
 * type PollRequest struct {
 *     AgentID              string `json:"agent_id"`
 *     Token                string `json:"token"`
 *     CurrentConfigVersion string `json:"current_config_version,omitempty"`
 *     CurrentConfigEtag    string `json:"current_config_etag,omitempty"`
 *     TimeoutMillis        int64  `json:"timeout_millis,omitempty"`
 * }
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class UnifiedPollRequestDto {

  @JsonProperty("agent_id")
  private String agentId = "";

  @JsonProperty("token")
  @Nullable
  private String token;

  @JsonProperty("current_config_version")
  @Nullable
  private String currentConfigVersion;

  @JsonProperty("current_config_etag")
  @Nullable
  private String currentConfigEtag;

  @JsonProperty("timeout_millis")
  private long timeoutMillis;

  /** Jackson 需要无参构造函数 */
  @SuppressWarnings("unused") // Jackson 需要
  private UnifiedPollRequestDto() {}

  /**
   * 从接口创建 DTO
   *
   * @param request 统一轮询请求接口
   * @return DTO 实例
   */
  public static UnifiedPollRequestDto from(UnifiedPollRequest request) {
    UnifiedPollRequestDto dto = new UnifiedPollRequestDto();
    dto.setAgentId(request.getAgentId());
    dto.setCurrentConfigVersion(request.getCurrentConfigVersion());
    dto.setCurrentConfigEtag(request.getCurrentConfigEtag());
    dto.setTimeoutMillis(request.getTimeoutMillis());
    return dto;
  }

  // ===== Getter 和 Setter =====

  public String getAgentId() {
    return agentId;
  }

  public void setAgentId(String agentId) {
    this.agentId = agentId;
  }

  @Nullable
  public String getToken() {
    return token;
  }

  public void setToken(@Nullable String token) {
    this.token = token;
  }

  @Nullable
  public String getCurrentConfigVersion() {
    return currentConfigVersion;
  }

  public void setCurrentConfigVersion(@Nullable String currentConfigVersion) {
    this.currentConfigVersion = currentConfigVersion;
  }

  @Nullable
  public String getCurrentConfigEtag() {
    return currentConfigEtag;
  }

  public void setCurrentConfigEtag(@Nullable String currentConfigEtag) {
    this.currentConfigEtag = currentConfigEtag;
  }

  public long getTimeoutMillis() {
    return timeoutMillis;
  }

  public void setTimeoutMillis(long timeoutMillis) {
    this.timeoutMillis = timeoutMillis;
  }
}
