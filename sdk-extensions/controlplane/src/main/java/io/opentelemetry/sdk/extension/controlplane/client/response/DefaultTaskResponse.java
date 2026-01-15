/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.response;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskInfo;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskResponse;
import java.util.Collections;
import java.util.List;

/**
 * 默认任务响应实现
 *
 * <p>供 HTTP 和 gRPC 客户端共享使用
 */
public final class DefaultTaskResponse implements TaskResponse {
  private final boolean success;
  private final List<TaskInfo> tasks;
  private final String errorMessage;
  private final long suggestedPollIntervalMillis;

  /**
   * 创建任务响应
   */
  public DefaultTaskResponse(
      boolean success,
      List<TaskInfo> tasks,
      String errorMessage,
      long suggestedPollIntervalMillis) {
    this.success = success;
    this.tasks = tasks != null ? tasks : Collections.emptyList();
    this.errorMessage = errorMessage != null ? errorMessage : "";
    this.suggestedPollIntervalMillis = suggestedPollIntervalMillis;
  }

  /**
   * 创建错误响应
   *
   * @param errorMessage 错误信息
   * @return 错误响应实例
   */
  public static TaskResponse error(String errorMessage) {
    return new DefaultTaskResponse(
        /* success= */ false, Collections.emptyList(), errorMessage, 10000);
  }

  @Override
  public boolean isSuccess() {
    return success;
  }

  @Override
  public List<TaskInfo> getTasks() {
    return tasks;
  }

  @Override
  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public long getSuggestedPollIntervalMillis() {
    return suggestedPollIntervalMillis;
  }
}
