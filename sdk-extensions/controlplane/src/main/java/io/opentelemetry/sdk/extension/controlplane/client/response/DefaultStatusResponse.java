/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.response;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.StatusResponse;
import java.util.Collections;
import java.util.List;

/**
 * 默认状态响应实现
 *
 * <p>供 HTTP 和 gRPC 客户端共享使用
 */
public final class DefaultStatusResponse implements StatusResponse {
  private final boolean success;
  private final List<String> acknowledgedTaskIds;
  private final String errorMessage;
  private final long suggestedReportIntervalMillis;

  /**
   * 创建状态响应
   */
  public DefaultStatusResponse(
      boolean success,
      List<String> acknowledgedTaskIds,
      String errorMessage,
      long suggestedReportIntervalMillis) {
    this.success = success;
    this.acknowledgedTaskIds = acknowledgedTaskIds != null ? acknowledgedTaskIds : Collections.emptyList();
    this.errorMessage = errorMessage != null ? errorMessage : "";
    this.suggestedReportIntervalMillis = suggestedReportIntervalMillis;
  }

  /**
   * 创建错误响应
   *
   * @param errorMessage 错误信息
   * @return 错误响应实例
   */
  public static StatusResponse error(String errorMessage) {
    return new DefaultStatusResponse(
        /* success= */ false, Collections.emptyList(), errorMessage, 60000);
  }

  @Override
  public boolean isSuccess() {
    return success;
  }

  @Override
  public List<String> getAcknowledgedTaskIds() {
    return acknowledgedTaskIds;
  }

  @Override
  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public long getSuggestedReportIntervalMillis() {
    return suggestedReportIntervalMillis;
  }
}
