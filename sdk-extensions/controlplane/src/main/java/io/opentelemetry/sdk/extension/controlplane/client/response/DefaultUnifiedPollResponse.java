/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.response;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.PollResult;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.UnifiedPollResponse;
import java.util.Collections;
import java.util.Map;

/**
 * 默认统一轮询响应实现
 *
 * <p>供 HTTP 和 gRPC 客户端共享使用
 */
public final class DefaultUnifiedPollResponse implements UnifiedPollResponse {
  private final boolean success;
  private final boolean hasAnyChanges;
  private final Map<String, PollResult> results;
  private final String errorMessage;

  /**
   * 创建统一轮询响应
   *
   * @param success 是否成功
   * @param hasAnyChanges 是否有任何变更
   * @param results 各类型的轮询结果
   * @param errorMessage 错误信息
   */
  public DefaultUnifiedPollResponse(
      boolean success,
      boolean hasAnyChanges,
      Map<String, PollResult> results,
      String errorMessage) {
    this.success = success;
    this.hasAnyChanges = hasAnyChanges;
    this.results = results != null ? results : Collections.emptyMap();
    this.errorMessage = errorMessage != null ? errorMessage : "";
  }

  /**
   * 创建错误响应
   *
   * @param errorMessage 错误信息
   * @return 错误响应实例
   */
  public static UnifiedPollResponse error(String errorMessage) {
    return new DefaultUnifiedPollResponse(
        /* success= */ false, /* hasAnyChanges= */ false, Collections.emptyMap(), errorMessage);
  }

  @Override
  public boolean isSuccess() {
    return success;
  }

  @Override
  public boolean hasAnyChanges() {
    return hasAnyChanges;
  }

  @Override
  public Map<String, PollResult> getResults() {
    return results;
  }

  @Override
  public String getErrorMessage() {
    return errorMessage;
  }
}
