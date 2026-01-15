/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.response;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskInfo;
import java.util.Locale;
import javax.annotation.Nullable;

/**
 * 默认任务信息实现
 *
 * <p>供 HTTP 和 gRPC 客户端共享使用
 */
public final class DefaultTaskInfo implements TaskInfo {
  private final String taskId;
  private final String taskType;
  private final String parametersJson;
  private final int priority;
  private final long timeoutMillis;
  private final long createdAtMillis;
  private final long expiresAtMillis;
  private final long maxAcceptableDelayMillis;

  /**
   * 创建任务信息
   */
  public DefaultTaskInfo(
      String taskId,
      String taskType,
      @Nullable String parametersJson,
      int priority,
      long timeoutMillis,
      long createdAtMillis,
      long expiresAtMillis,
      long maxAcceptableDelayMillis) {
    this.taskId = taskId != null ? taskId : "";
    this.taskType = taskType != null ? taskType : "UNKNOWN";
    this.parametersJson = parametersJson != null ? parametersJson : "{}";
    this.priority = priority;
    this.timeoutMillis = timeoutMillis;
    this.createdAtMillis = createdAtMillis;
    this.expiresAtMillis = expiresAtMillis;
    this.maxAcceptableDelayMillis = maxAcceptableDelayMillis;
  }

  /**
   * 创建任务信息（使用 Builder 模式）
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String getTaskId() {
    return taskId;
  }

  @Override
  public String getTaskType() {
    return taskType;
  }

  @Override
  public String getParametersJson() {
    return parametersJson;
  }

  @Override
  public int getPriority() {
    return priority;
  }

  @Override
  public long getTimeoutMillis() {
    return timeoutMillis;
  }

  @Override
  public long getCreatedAtMillis() {
    return createdAtMillis;
  }

  @Override
  public long getExpiresAtMillis() {
    return expiresAtMillis;
  }

  @Override
  public long getMaxAcceptableDelayMillis() {
    return maxAcceptableDelayMillis;
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "TaskInfo{id=%s, type=%s, priority=%d, timeout=%dms, maxDelay=%dms}",
        taskId, taskType, priority, timeoutMillis, maxAcceptableDelayMillis);
  }

  /**
   * 任务信息构建器
   */
  public static final class Builder {
    private String taskId = "";
    private String taskType = "UNKNOWN";
    private String parametersJson = "{}";
    private int priority = 0;
    private long timeoutMillis = 60000;
    private long createdAtMillis = 0;
    private long expiresAtMillis = 0;
    private long maxAcceptableDelayMillis = 0;

    private Builder() {}

    public Builder setTaskId(String taskId) {
      this.taskId = taskId;
      return this;
    }

    public Builder setTaskType(String taskType) {
      this.taskType = taskType;
      return this;
    }

    public Builder setParametersJson(String parametersJson) {
      this.parametersJson = parametersJson;
      return this;
    }

    public Builder setPriority(int priority) {
      this.priority = priority;
      return this;
    }

    public Builder setTimeoutMillis(long timeoutMillis) {
      this.timeoutMillis = timeoutMillis;
      return this;
    }

    public Builder setCreatedAtMillis(long createdAtMillis) {
      this.createdAtMillis = createdAtMillis;
      return this;
    }

    public Builder setExpiresAtMillis(long expiresAtMillis) {
      this.expiresAtMillis = expiresAtMillis;
      return this;
    }

    public Builder setMaxAcceptableDelayMillis(long maxAcceptableDelayMillis) {
      this.maxAcceptableDelayMillis = maxAcceptableDelayMillis;
      return this;
    }

    public DefaultTaskInfo build() {
      return new DefaultTaskInfo(
          taskId, taskType, parametersJson, priority, timeoutMillis,
          createdAtMillis, expiresAtMillis, maxAcceptableDelayMillis);
    }
  }
}
