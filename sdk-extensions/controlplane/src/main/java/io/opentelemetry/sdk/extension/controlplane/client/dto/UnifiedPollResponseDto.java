/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * 统一轮询响应 DTO
 *
 * <p>用于 Jackson 反序列化，表示控制平面统一轮询接口的响应。
 *
 * <p>支持两种服务端响应格式：
 * <ul>
 *   <li>嵌套格式（推荐）: {"has_any_changes": true, "results": {"CONFIG": {...}, "TASK": {...}}}
 *   <li>扁平格式（兼容）: {"has_any_changes": true, "CONFIG": {...}, "TASK": {...}}
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class UnifiedPollResponseDto {

  @JsonProperty("has_any_changes")
  private boolean hasAnyChanges;

  /**
   * 嵌套格式的结果映射（服务端实际返回的格式）
   * 
   * <p>服务端 Go 代码返回: Results map[LongPollType]*PollResponse `json:"results,omitempty"`
   */
  @JsonProperty("results")
  @Nullable
  private Map<String, PollResultDto> results;

  /**
   * 扁平格式的 CONFIG 结果（向后兼容）
   */
  @JsonProperty("CONFIG")
  @Nullable
  private PollResultDto configResult;

  /**
   * 扁平格式的 TASK 结果（向后兼容）
   */
  @JsonProperty("TASK")
  @Nullable
  private PollResultDto taskResult;

  @JsonProperty("error_message")
  @Nullable
  private String errorMessage;

  // Jackson 需要无参构造函数
  public UnifiedPollResponseDto() {}

  public boolean isHasAnyChanges() {
    return hasAnyChanges;
  }

  public void setHasAnyChanges(boolean hasAnyChanges) {
    this.hasAnyChanges = hasAnyChanges;
  }

  @Nullable
  public Map<String, PollResultDto> getResults() {
    return results;
  }

  public void setResults(@Nullable Map<String, PollResultDto> results) {
    this.results = results;
  }

  /**
   * 获取 CONFIG 结果
   *
   * <p>优先从 results 嵌套结构中获取，如果不存在则尝试从扁平结构获取（向后兼容）
   */
  @Nullable
  public PollResultDto getConfigResult() {
    // 优先从 results 中获取（服务端实际返回的格式）
    if (results != null && results.containsKey("CONFIG")) {
      return results.get("CONFIG");
    }
    // 兼容扁平格式
    return configResult;
  }

  public void setConfigResult(@Nullable PollResultDto configResult) {
    this.configResult = configResult;
  }

  /**
   * 获取 TASK 结果
   *
   * <p>优先从 results 嵌套结构中获取，如果不存在则尝试从扁平结构获取（向后兼容）
   */
  @Nullable
  public PollResultDto getTaskResult() {
    // 优先从 results 中获取（服务端实际返回的格式）
    if (results != null && results.containsKey("TASK")) {
      return results.get("TASK");
    }
    // 兼容扁平格式
    return taskResult;
  }

  public void setTaskResult(@Nullable PollResultDto taskResult) {
    this.taskResult = taskResult;
  }

  @Nullable
  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(@Nullable String errorMessage) {
    this.errorMessage = errorMessage;
  }

  /**
   * 检查是否有 CONFIG 结果
   */
  public boolean hasConfigResult() {
    return getConfigResult() != null;
  }

  /**
   * 检查是否有 TASK 结果
   */
  public boolean hasTaskResult() {
    return getTaskResult() != null;
  }
}
