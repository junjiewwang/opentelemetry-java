/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.executor;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;

/**
 * 任务执行上下文
 *
 * <p>封装任务执行所需的所有信息，包括：
 * <ul>
 *   <li>任务元数据（ID、类型、优先级等）
 *   <li>任务参数（JSON 解析后的 Map）
 *   <li>时间约束（超时、创建时间、过期时间）
 *   <li>依赖组件（客户端、调度器等）
 * </ul>
 *
 * <p>使用 Builder 模式创建实例，确保不可变性。
 */
public final class TaskExecutionContext {

  private final String taskId;
  private final String taskType;
  private final int priority;
  private final long timeoutMillis;
  private final long createdAtMillis;
  private final long expiresAtMillis;
  private final String agentId;
  private final Map<String, Object> parameters;
  private final String parametersJson;

  // 依赖组件
  @Nullable private final ControlPlaneClient client;
  @Nullable private final ScheduledExecutorService scheduler;

  // 执行跟踪
  private final long receivedAtMillis;

  private TaskExecutionContext(Builder builder) {
    this.taskId = builder.taskId;
    this.taskType = builder.taskType;
    this.priority = builder.priority;
    this.timeoutMillis = builder.timeoutMillis;
    this.createdAtMillis = builder.createdAtMillis;
    this.expiresAtMillis = builder.expiresAtMillis;
    this.agentId = builder.agentId;
    this.parameters = Collections.unmodifiableMap(new HashMap<>(builder.parameters));
    this.parametersJson = builder.parametersJson;
    this.client = builder.client;
    this.scheduler = builder.scheduler;
    this.receivedAtMillis = builder.receivedAtMillis;
  }

  // ===== Getters =====

  public String getTaskId() {
    return taskId;
  }

  public String getTaskType() {
    return taskType;
  }

  public int getPriority() {
    return priority;
  }

  public long getTimeoutMillis() {
    return timeoutMillis;
  }

  public long getCreatedAtMillis() {
    return createdAtMillis;
  }

  public long getExpiresAtMillis() {
    return expiresAtMillis;
  }

  public String getAgentId() {
    return agentId;
  }

  public Map<String, Object> getParameters() {
    return parameters;
  }

  public String getParametersJson() {
    return parametersJson;
  }

  @Nullable
  public ControlPlaneClient getClient() {
    return client;
  }

  @Nullable
  public ScheduledExecutorService getScheduler() {
    return scheduler;
  }

  public long getReceivedAtMillis() {
    return receivedAtMillis;
  }

  /**
   * 获取任务从创建到接收的延迟时间
   *
   * @return 延迟毫秒数
   */
  public long getDelayMillis() {
    if (createdAtMillis > 0 && receivedAtMillis > 0) {
      return receivedAtMillis - createdAtMillis;
    }
    return 0;
  }

  /**
   * 获取有效的超时时间（考虑任务剩余有效期）
   *
   * @return 有效超时毫秒数
   */
  public long getEffectiveTimeoutMillis() {
    if (expiresAtMillis > 0) {
      long remaining = expiresAtMillis - System.currentTimeMillis();
      if (remaining <= 0) {
        return 0; // 已过期
      }
      return timeoutMillis > 0 ? Math.min(timeoutMillis, remaining) : remaining;
    }
    return timeoutMillis;
  }

  /**
   * 获取字符串类型的参数
   *
   * @param key 参数名
   * @param defaultValue 默认值
   * @return 参数值
   */
  public String getStringParameter(String key, String defaultValue) {
    Object value = parameters.get(key);
    return value != null ? String.valueOf(value) : defaultValue;
  }

  /**
   * 获取整数类型的参数
   *
   * @param key 参数名
   * @param defaultValue 默认值
   * @return 参数值
   */
  public int getIntParameter(String key, int defaultValue) {
    Object value = parameters.get(key);
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    if (value instanceof String) {
      try {
        return Integer.parseInt((String) value);
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  /**
   * 获取长整数类型的参数
   *
   * @param key 参数名
   * @param defaultValue 默认值
   * @return 参数值
   */
  public long getLongParameter(String key, long defaultValue) {
    Object value = parameters.get(key);
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    if (value instanceof String) {
      try {
        return Long.parseLong((String) value);
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  /**
   * 获取布尔类型的参数
   *
   * @param key 参数名
   * @param defaultValue 默认值
   * @return 参数值
   */
  public boolean getBooleanParameter(String key, boolean defaultValue) {
    Object value = parameters.get(key);
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    }
    return defaultValue;
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "TaskExecutionContext{taskId='%s', type='%s', priority=%d, timeout=%dms}",
        taskId, taskType, priority, timeoutMillis);
  }

  // ===== Builder =====

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String taskId = "";
    private String taskType = "";
    private int priority = 0;
    private long timeoutMillis = 60000;
    private long createdAtMillis = 0;
    private long expiresAtMillis = 0;
    private String agentId = "";
    private Map<String, Object> parameters = new HashMap<>();
    private String parametersJson = "{}";
    @Nullable private ControlPlaneClient client;
    @Nullable private ScheduledExecutorService scheduler;
    private long receivedAtMillis = System.currentTimeMillis();

    public Builder taskId(String taskId) {
      this.taskId = taskId;
      return this;
    }

    public Builder taskType(String taskType) {
      this.taskType = taskType;
      return this;
    }

    public Builder priority(int priority) {
      this.priority = priority;
      return this;
    }

    public Builder timeoutMillis(long timeoutMillis) {
      this.timeoutMillis = timeoutMillis;
      return this;
    }

    public Builder createdAtMillis(long createdAtMillis) {
      this.createdAtMillis = createdAtMillis;
      return this;
    }

    public Builder expiresAtMillis(long expiresAtMillis) {
      this.expiresAtMillis = expiresAtMillis;
      return this;
    }

    public Builder agentId(String agentId) {
      this.agentId = agentId;
      return this;
    }

    public Builder parameters(Map<String, Object> parameters) {
      this.parameters = parameters != null ? parameters : new HashMap<>();
      return this;
    }

    public Builder parametersJson(String parametersJson) {
      this.parametersJson = parametersJson != null ? parametersJson : "{}";
      return this;
    }

    public Builder client(@Nullable ControlPlaneClient client) {
      this.client = client;
      return this;
    }

    public Builder scheduler(@Nullable ScheduledExecutorService scheduler) {
      this.scheduler = scheduler;
      return this;
    }

    public Builder receivedAtMillis(long receivedAtMillis) {
      this.receivedAtMillis = receivedAtMillis;
      return this;
    }

    public TaskExecutionContext build() {
      return new TaskExecutionContext(this);
    }
  }
}
