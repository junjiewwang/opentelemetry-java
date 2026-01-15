/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * 轮询结果 DTO
 *
 * <p>用于 Jackson 反序列化，表示单个类型（CONFIG 或 TASK）的轮询结果。
 *
 * <p>服务端 Go 结构体：
 * <pre>
 * type PollResponse struct {
 *     Type          LongPollType `json:"type"`
 *     HasChanges    bool         `json:"has_changes"`
 *     Config        *AgentConfig `json:"config,omitempty"`
 *     ConfigVersion string       `json:"config_version,omitempty"`
 *     ConfigEtag    string       `json:"config_etag,omitempty"`
 *     Tasks         []*Task      `json:"tasks,omitempty"`
 *     Message       string       `json:"message,omitempty"`
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class PollResultDto {

  /**
   * 结果类型：CONFIG 或 TASK
   */
  @JsonProperty("type")
  @Nullable
  private String type;

  @JsonProperty("has_changes")
  private boolean hasChanges;

  /**
   * 配置数据（服务端返回的是 AgentConfig 对象）
   * 使用 JsonNode 保持原始 JSON 结构，便于后续处理
   */
  @JsonProperty("config")
  @Nullable
  private JsonNode config;

  @JsonProperty("config_version")
  @Nullable
  private String configVersion;

  @JsonProperty("config_etag")
  @Nullable
  private String configEtag;

  /**
   * 向后兼容字段：某些旧版本可能使用 config_data
   */
  @JsonProperty("config_data")
  @Nullable
  private byte[] configData;

  @JsonProperty("tasks")
  @Nullable
  private List<TaskInfoDto> tasks;

  /**
   * 服务端返回的消息（如 "no changes"）
   */
  @JsonProperty("message")
  @Nullable
  private String message;

  // Jackson 需要无参构造函数
  public PollResultDto() {}

  @Nullable
  public String getType() {
    return type;
  }

  public void setType(@Nullable String type) {
    this.type = type;
  }

  public boolean isHasChanges() {
    return hasChanges;
  }

  public void setHasChanges(boolean hasChanges) {
    this.hasChanges = hasChanges;
  }

  /**
   * 获取配置数据
   *
   * <p>优先返回 config 字段（服务端实际返回的格式），如果不存在则尝试返回 configData（向后兼容）
   */
  @Nullable
  public byte[] getConfigData() {
    // 如果有 config 字段（JsonNode），将其序列化为 byte[]
    if (config != null) {
      return config.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
    // 向后兼容
    return configData;
  }

  /**
   * 获取原始 config JsonNode
   */
  @Nullable
  public JsonNode getConfig() {
    return config;
  }

  public void setConfig(@Nullable JsonNode config) {
    this.config = config;
  }

  public void setConfigData(@Nullable byte[] configData) {
    this.configData = configData;
  }

  @Nullable
  public String getConfigVersion() {
    return configVersion;
  }

  public void setConfigVersion(@Nullable String configVersion) {
    this.configVersion = configVersion;
  }

  @Nullable
  public String getConfigEtag() {
    return configEtag;
  }

  public void setConfigEtag(@Nullable String configEtag) {
    this.configEtag = configEtag;
  }

  /**
   * 获取任务列表
   *
   * @return 任务列表，如果为空则返回空列表
   */
  public List<TaskInfoDto> getTasks() {
    return tasks != null ? tasks : Collections.emptyList();
  }

  public void setTasks(@Nullable List<TaskInfoDto> tasks) {
    this.tasks = tasks;
  }

  @Nullable
  public String getMessage() {
    return message;
  }

  public void setMessage(@Nullable String message) {
    this.message = message;
  }
}
