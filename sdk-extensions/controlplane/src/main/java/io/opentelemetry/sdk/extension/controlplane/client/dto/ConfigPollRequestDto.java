/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.ConfigRequest;
import javax.annotation.Nullable;

/**
 * 配置轮询请求 DTO
 *
 * <p>用于 Jackson 序列化，确保字段名与服务端 Go 结构体完全匹配。
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class ConfigPollRequestDto {

  @JsonProperty("agent_id")
  private String agentId = "";

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
  private ConfigPollRequestDto() {}

  /**
   * 从接口创建 DTO
   *
   * @param request 配置请求接口
   * @return DTO 实例
   */
  public static ConfigPollRequestDto from(ConfigRequest request) {
    ConfigPollRequestDto dto = new ConfigPollRequestDto();
    dto.setAgentId(request.getAgentId());
    dto.setCurrentConfigVersion(request.getCurrentConfigVersion());
    dto.setCurrentConfigEtag(request.getCurrentEtag());
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
