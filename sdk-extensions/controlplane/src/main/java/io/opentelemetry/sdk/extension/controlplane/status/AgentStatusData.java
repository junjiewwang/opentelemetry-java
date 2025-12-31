/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.status;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

/**
 * Agent 状态数据
 *
 * <p>采用 Builder 模式构建，支持动态扩展字段。 核心字段使用强类型，扩展字段使用 Map 存储。
 */
public final class AgentStatusData {

  // ============ 身份信息 ============
  private final String agentId;
  private final String hostname;
  private final String processId;
  private final String serviceName;
  private final String serviceNamespace;
  private final String sdkVersion;

  // ============ 运行状态 ============
  private final long startupTimestamp;
  private final long uptimeMs;
  private final long timestamp;
  private final String runningState;

  // ============ 控制平面状态 ============
  private final String connectionState;
  private final String configVersion;
  private final long lastConfigFetchTime;

  // ============ OTLP 健康状态 ============
  private final String otlpHealthState;
  @Nullable private final ExportStats spanExportStats;
  @Nullable private final ExportStats metricExportStats;
  @Nullable private final ExportStats logExportStats;

  // ============ 系统资源 (可选) ============
  private final long heapMemoryUsed;
  private final long heapMemoryMax;
  private final double cpuUsage;

  // ============ 动态配置状态 ============
  private final String samplerType;
  private final double samplingRate;

  // ============ 扩展属性 ============
  private final Map<String, Object> extensions;

  private AgentStatusData(Builder builder) {
    this.agentId = builder.agentId;
    this.hostname = builder.hostname;
    this.processId = builder.processId;
    this.serviceName = builder.serviceName;
    this.serviceNamespace = builder.serviceNamespace;
    this.sdkVersion = builder.sdkVersion;
    this.startupTimestamp = builder.startupTimestamp;
    this.uptimeMs = builder.uptimeMs;
    this.timestamp = builder.timestamp;
    this.runningState = builder.runningState;
    this.connectionState = builder.connectionState;
    this.configVersion = builder.configVersion;
    this.lastConfigFetchTime = builder.lastConfigFetchTime;
    this.otlpHealthState = builder.otlpHealthState;
    this.spanExportStats = builder.spanExportStats;
    this.metricExportStats = builder.metricExportStats;
    this.logExportStats = builder.logExportStats;
    this.heapMemoryUsed = builder.heapMemoryUsed;
    this.heapMemoryMax = builder.heapMemoryMax;
    this.cpuUsage = builder.cpuUsage;
    this.samplerType = builder.samplerType;
    this.samplingRate = builder.samplingRate;
    this.extensions = Collections.unmodifiableMap(new HashMap<>(builder.extensions));
  }

  public static Builder builder() {
    return new Builder();
  }

  // ============ Getters ============

  public String getAgentId() {
    return agentId;
  }

  public String getHostname() {
    return hostname;
  }

  public String getProcessId() {
    return processId;
  }

  public String getServiceName() {
    return serviceName;
  }

  public String getServiceNamespace() {
    return serviceNamespace;
  }

  public String getSdkVersion() {
    return sdkVersion;
  }

  public long getStartupTimestamp() {
    return startupTimestamp;
  }

  public long getUptimeMs() {
    return uptimeMs;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getRunningState() {
    return runningState;
  }

  public String getConnectionState() {
    return connectionState;
  }

  public String getConfigVersion() {
    return configVersion;
  }

  public long getLastConfigFetchTime() {
    return lastConfigFetchTime;
  }

  public String getOtlpHealthState() {
    return otlpHealthState;
  }

  @Nullable
  public ExportStats getSpanExportStats() {
    return spanExportStats;
  }

  @Nullable
  public ExportStats getMetricExportStats() {
    return metricExportStats;
  }

  @Nullable
  public ExportStats getLogExportStats() {
    return logExportStats;
  }

  public long getHeapMemoryUsed() {
    return heapMemoryUsed;
  }

  public long getHeapMemoryMax() {
    return heapMemoryMax;
  }

  public double getCpuUsage() {
    return cpuUsage;
  }

  public String getSamplerType() {
    return samplerType;
  }

  public double getSamplingRate() {
    return samplingRate;
  }

  public Map<String, Object> getExtensions() {
    return extensions;
  }

  /**
   * 转换为 Map 格式（用于 JSON 序列化）
   *
   * @return Map 表示
   */
  public Map<String, Object> toMap() {
    Map<String, Object> map = new LinkedHashMap<>();

    // 身份信息
    map.put("agentId", agentId);
    map.put("hostname", hostname);
    map.put("processId", processId);
    map.put("serviceName", serviceName);
    if (serviceNamespace != null && !serviceNamespace.isEmpty()) {
      map.put("serviceNamespace", serviceNamespace);
    }
    map.put("sdkVersion", sdkVersion);

    // 运行状态
    map.put("startupTimestamp", startupTimestamp);
    map.put("uptimeMs", uptimeMs);
    map.put("timestamp", timestamp);
    map.put("runningState", runningState);

    // 控制平面状态
    map.put("connectionState", connectionState);
    if (configVersion != null && !configVersion.isEmpty()) {
      map.put("configVersion", configVersion);
    }
    if (lastConfigFetchTime > 0) {
      map.put("lastConfigFetchTime", lastConfigFetchTime);
    }

    // OTLP 健康状态
    map.put("otlpHealthState", otlpHealthState);
    if (spanExportStats != null) {
      map.put("spanExportStats", spanExportStats.toMap());
    }
    if (metricExportStats != null) {
      map.put("metricExportStats", metricExportStats.toMap());
    }
    if (logExportStats != null) {
      map.put("logExportStats", logExportStats.toMap());
    }

    // 系统资源（仅在有值时添加）
    if (heapMemoryUsed > 0) {
      map.put("heapMemoryUsed", heapMemoryUsed);
    }
    if (heapMemoryMax > 0) {
      map.put("heapMemoryMax", heapMemoryMax);
    }
    if (cpuUsage >= 0) {
      map.put("cpuUsage", cpuUsage);
    }

    // 动态配置状态
    if (samplerType != null && !samplerType.isEmpty()) {
      map.put("samplerType", samplerType);
    }
    if (samplingRate >= 0) {
      map.put("samplingRate", samplingRate);
    }

    // 扩展属性
    if (!extensions.isEmpty()) {
      map.putAll(extensions);
    }

    return map;
  }

  /** 导出统计信息 */
  public static final class ExportStats {
    private final long totalExports;
    private final long successCount;
    private final long failureCount;
    private final double successRate;
    private final long lastExportTime;
    @Nullable private final String lastError;

    private ExportStats(Builder builder) {
      this.totalExports = builder.totalExports;
      this.successCount = builder.successCount;
      this.failureCount = builder.failureCount;
      this.successRate = builder.successRate;
      this.lastExportTime = builder.lastExportTime;
      this.lastError = builder.lastError;
    }

    public static Builder builder() {
      return new Builder();
    }

    public long getTotalExports() {
      return totalExports;
    }

    public long getSuccessCount() {
      return successCount;
    }

    public long getFailureCount() {
      return failureCount;
    }

    public double getSuccessRate() {
      return successRate;
    }

    public long getLastExportTime() {
      return lastExportTime;
    }

    @Nullable
    public String getLastError() {
      return lastError;
    }

    public Map<String, Object> toMap() {
      Map<String, Object> map = new LinkedHashMap<>();
      map.put("totalExports", totalExports);
      map.put("successCount", successCount);
      map.put("failureCount", failureCount);
      map.put("successRate", successRate);
      if (lastExportTime > 0) {
        map.put("lastExportTime", lastExportTime);
      }
      if (lastError != null && !lastError.isEmpty()) {
        map.put("lastError", lastError);
      }
      return map;
    }

    /** Builder for ExportStats */
    public static final class Builder {
      private long totalExports;
      private long successCount;
      private long failureCount;
      private double successRate;
      private long lastExportTime;
      @Nullable private String lastError;

      private Builder() {}

      public Builder setTotalExports(long totalExports) {
        this.totalExports = totalExports;
        return this;
      }

      public Builder setSuccessCount(long successCount) {
        this.successCount = successCount;
        return this;
      }

      public Builder setFailureCount(long failureCount) {
        this.failureCount = failureCount;
        return this;
      }

      public Builder setSuccessRate(double successRate) {
        this.successRate = successRate;
        return this;
      }

      public Builder setLastExportTime(long lastExportTime) {
        this.lastExportTime = lastExportTime;
        return this;
      }

      public Builder setLastError(@Nullable String lastError) {
        this.lastError = lastError;
        return this;
      }

      public ExportStats build() {
        return new ExportStats(this);
      }
    }
  }

  /** Builder for AgentStatusData */
  public static final class Builder {
    private String agentId = "";
    private String hostname = "";
    private String processId = "";
    private String serviceName = "";
    private String serviceNamespace = "";
    private String sdkVersion = "";
    private long startupTimestamp;
    private long uptimeMs;
    private long timestamp;
    private String runningState = "RUNNING";
    private String connectionState = "";
    private String configVersion = "";
    private long lastConfigFetchTime;
    private String otlpHealthState = "UNKNOWN";
    @Nullable private ExportStats spanExportStats;
    @Nullable private ExportStats metricExportStats;
    @Nullable private ExportStats logExportStats;
    private long heapMemoryUsed;
    private long heapMemoryMax;
    private double cpuUsage = -1;
    private String samplerType = "";
    private double samplingRate = -1;
    private final Map<String, Object> extensions = new ConcurrentHashMap<>();

    private Builder() {}

    public Builder setAgentId(String agentId) {
      this.agentId = agentId;
      return this;
    }

    public Builder setHostname(String hostname) {
      this.hostname = hostname;
      return this;
    }

    public Builder setProcessId(String processId) {
      this.processId = processId;
      return this;
    }

    public Builder setServiceName(String serviceName) {
      this.serviceName = serviceName;
      return this;
    }

    public Builder setServiceNamespace(String serviceNamespace) {
      this.serviceNamespace = serviceNamespace;
      return this;
    }

    public Builder setSdkVersion(String sdkVersion) {
      this.sdkVersion = sdkVersion;
      return this;
    }

    public Builder setStartupTimestamp(long startupTimestamp) {
      this.startupTimestamp = startupTimestamp;
      return this;
    }

    public Builder setUptimeMs(long uptimeMs) {
      this.uptimeMs = uptimeMs;
      return this;
    }

    public Builder setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder setRunningState(String runningState) {
      this.runningState = runningState;
      return this;
    }

    public Builder setConnectionState(String connectionState) {
      this.connectionState = connectionState;
      return this;
    }

    public Builder setConfigVersion(String configVersion) {
      this.configVersion = configVersion;
      return this;
    }

    public Builder setLastConfigFetchTime(long lastConfigFetchTime) {
      this.lastConfigFetchTime = lastConfigFetchTime;
      return this;
    }

    public Builder setOtlpHealthState(String otlpHealthState) {
      this.otlpHealthState = otlpHealthState;
      return this;
    }

    public Builder setSpanExportStats(ExportStats spanExportStats) {
      this.spanExportStats = spanExportStats;
      return this;
    }

    public Builder setMetricExportStats(ExportStats metricExportStats) {
      this.metricExportStats = metricExportStats;
      return this;
    }

    public Builder setLogExportStats(ExportStats logExportStats) {
      this.logExportStats = logExportStats;
      return this;
    }

    public Builder setHeapMemoryUsed(long heapMemoryUsed) {
      this.heapMemoryUsed = heapMemoryUsed;
      return this;
    }

    public Builder setHeapMemoryMax(long heapMemoryMax) {
      this.heapMemoryMax = heapMemoryMax;
      return this;
    }

    public Builder setCpuUsage(double cpuUsage) {
      this.cpuUsage = cpuUsage;
      return this;
    }

    public Builder setSamplerType(String samplerType) {
      this.samplerType = samplerType;
      return this;
    }

    public Builder setSamplingRate(double samplingRate) {
      this.samplingRate = samplingRate;
      return this;
    }

    public Builder putExtension(String key, Object value) {
      this.extensions.put(key, value);
      return this;
    }

    public Builder putAllExtensions(Map<String, Object> extensions) {
      this.extensions.putAll(extensions);
      return this;
    }

    public AgentStatusData build() {
      return new AgentStatusData(this);
    }
  }
}
