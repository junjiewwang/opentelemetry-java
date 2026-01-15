/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.response;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskResultResponse;

/**
 * 默认任务结果响应实现
 *
 * <p>供 HTTP 和 gRPC 客户端共享使用
 */
public final class DefaultTaskResultResponse implements TaskResultResponse {
  private final boolean success;
  private final String errorMessage;

  /**
   * 创建任务结果响应
   */
  public DefaultTaskResultResponse(boolean success, String errorMessage) {
    this.success = success;
    this.errorMessage = errorMessage != null ? errorMessage : "";
  }

  /**
   * 创建错误响应
   *
   * @param errorMessage 错误信息
   * @return 错误响应实例
   */
  public static TaskResultResponse error(String errorMessage) {
    return new DefaultTaskResultResponse(/* success= */ false, errorMessage);
  }

  /**
   * 创建成功响应
   *
   * @return 成功响应实例
   */
  public static TaskResultResponse success() {
    return new DefaultTaskResultResponse(/* success= */ true, "");
  }

  @Override
  public boolean isSuccess() {
    return success;
  }

  @Override
  public String getErrorMessage() {
    return errorMessage;
  }
}
