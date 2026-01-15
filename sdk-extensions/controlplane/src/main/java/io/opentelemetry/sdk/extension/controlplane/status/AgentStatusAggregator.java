/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.status;

import io.opentelemetry.sdk.extension.controlplane.util.JsonUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Agent 状态聚合器
 *
 * <p>聚合多个 {@link AgentStatusCollector} 的数据，生成完整的状态上报数据。 采用组合模式，支持动态注册/注销收集器。
 */
public final class AgentStatusAggregator {

  private static final Logger logger = Logger.getLogger(AgentStatusAggregator.class.getName());

  private final List<AgentStatusCollector> collectors;

  public AgentStatusAggregator() {
    this.collectors = new CopyOnWriteArrayList<>();
  }

  /**
   * 注册状态收集器
   *
   * @param collector 收集器实例
   */
  public void registerCollector(AgentStatusCollector collector) {
    collectors.add(collector);
    logger.log(
        Level.FINE,
        "Registered status collector: {0} (priority={1})",
        new Object[] {collector.getName(), collector.getPriority()});
  }

  /**
   * 注销状态收集器
   *
   * @param collector 收集器实例
   */
  public void unregisterCollector(AgentStatusCollector collector) {
    collectors.remove(collector);
    logger.log(Level.FINE, "Unregistered status collector: {0}", collector.getName());
  }

  /**
   * 根据名称注销收集器
   *
   * @param name 收集器名称
   */
  public void unregisterCollector(String name) {
    collectors.removeIf(c -> c.getName().equals(name));
  }

  /**
   * 收集所有状态数据
   *
   * @return 聚合后的状态数据 Map
   */
  public Map<String, Object> collectAll() {
    Map<String, Object> aggregatedData = new LinkedHashMap<>();

    // 按优先级排序收集器
    List<AgentStatusCollector> sortedCollectors = new ArrayList<>(collectors);
    sortedCollectors.sort(Comparator.comparingInt(AgentStatusCollector::getPriority));

    for (AgentStatusCollector collector : sortedCollectors) {
      if (!collector.isEnabled()) {
        continue;
      }

      try {
        Map<String, Object> data = collector.collect();
        if (data != null && !data.isEmpty()) {
          aggregatedData.putAll(data);
        }
      } catch (RuntimeException e) {
        logger.log(
            Level.WARNING,
            "Failed to collect status from {0}: {1}",
            new Object[] {collector.getName(), e.getMessage()});
      }
    }

    return aggregatedData;
  }

  /**
   * 收集所有状态数据并构建 {@link AgentStatusData}
   *
   * @return AgentStatusData 实例
   */
  public AgentStatusData collectStatus() {
    Map<String, Object> data = collectAll();
    return buildStatusData(data);
  }

  /**
   * 收集状态并序列化为 JSON 字节数组
   *
   * <p>使用 {@link JsonUtils#toJsonBytes(java.util.Map)} 进行序列化
   *
   * @return JSON 格式的字节数组
   */
  public byte[] collectAsJsonBytes() {
    Map<String, Object> data = collectAll();
    return JsonUtils.toJsonBytes(data);
  }

  /**
   * 获取已注册的收集器数量
   *
   * @return 收集器数量
   */
  public int getCollectorCount() {
    return collectors.size();
  }

  /**
   * 获取所有已注册收集器的名称
   *
   * @return 收集器名称列表
   */
  public List<String> getCollectorNames() {
    List<String> names = new ArrayList<>();
    for (AgentStatusCollector collector : collectors) {
      names.add(collector.getName());
    }
    return names;
  }

  private static AgentStatusData buildStatusData(Map<String, Object> data) {
    AgentStatusData.Builder builder = AgentStatusData.builder();

    // 设置身份信息
    setIfPresent(data, "agentId", v -> builder.setAgentId((String) v));
    setIfPresent(data, "hostname", v -> builder.setHostname((String) v));
    setIfPresent(data, "processId", v -> builder.setProcessId((String) v));
    setIfPresent(data, "serviceName", v -> builder.setServiceName((String) v));
    setIfPresent(data, "serviceNamespace", v -> builder.setServiceNamespace((String) v));
    setIfPresent(data, "sdkVersion", v -> builder.setSdkVersion((String) v));

    // 设置运行状态
    setIfPresent(data, "startupTimestamp", v -> builder.setStartupTimestamp(toLong(v)));
    setIfPresent(data, "uptimeMs", v -> builder.setUptimeMs(toLong(v)));
    setIfPresent(data, "timestamp", v -> builder.setTimestamp(toLong(v)));
    setIfPresent(data, "runningState", v -> builder.setRunningState((String) v));

    // 设置控制平面状态
    setIfPresent(data, "connectionState", v -> builder.setConnectionState((String) v));
    setIfPresent(data, "configVersion", v -> builder.setConfigVersion((String) v));
    setIfPresent(data, "lastConfigFetchTime", v -> builder.setLastConfigFetchTime(toLong(v)));

    // 设置 OTLP 健康状态
    setIfPresent(data, "otlpHealthState", v -> builder.setOtlpHealthState((String) v));
    setIfPresentMap(data, "spanExportStats", v -> builder.setSpanExportStats(buildExportStats(v)));

    // 设置系统资源
    setIfPresent(data, "heapMemoryUsed", v -> builder.setHeapMemoryUsed(toLong(v)));
    setIfPresent(data, "heapMemoryMax", v -> builder.setHeapMemoryMax(toLong(v)));
    setIfPresent(data, "cpuUsage", v -> builder.setCpuUsage(toDouble(v)));

    // 设置动态配置
    setIfPresent(data, "samplerType", v -> builder.setSamplerType((String) v));
    setIfPresent(data, "samplingRate", v -> builder.setSamplingRate(toDouble(v)));

    return builder.build();
  }

  private static AgentStatusData.ExportStats buildExportStats(Map<String, Object> data) {
    AgentStatusData.ExportStats.Builder builder = AgentStatusData.ExportStats.builder();
    setIfPresent(data, "totalExports", v -> builder.setTotalExports(toLong(v)));
    setIfPresent(data, "successCount", v -> builder.setSuccessCount(toLong(v)));
    setIfPresent(data, "failureCount", v -> builder.setFailureCount(toLong(v)));
    setIfPresent(data, "successRate", v -> builder.setSuccessRate(toDouble(v)));
    setIfPresent(data, "lastExportTime", v -> builder.setLastExportTime(toLong(v)));
    setIfPresent(data, "lastError", v -> builder.setLastError((String) v));
    return builder.build();
  }

  private static void setIfPresent(Map<String, Object> data, String key, ValueSetter setter) {
    Object value = data.get(key);
    if (value != null) {
      try {
        setter.set(value);
      } catch (RuntimeException e) {
        logger.log(Level.FINE, "Failed to set field {0}: {1}", new Object[] {key, e.getMessage()});
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void setIfPresentMap(
      Map<String, Object> data, String key, MapValueSetter setter) {
    Object value = data.get(key);
    if (value instanceof Map) {
      try {
        setter.set((Map<String, Object>) value);
      } catch (RuntimeException e) {
        logger.log(Level.FINE, "Failed to set field {0}: {1}", new Object[] {key, e.getMessage()});
      }
    }
  }

  private static long toLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    return Long.parseLong(value.toString());
  }

  private static double toDouble(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    return Double.parseDouble(value.toString());
  }

  @FunctionalInterface
  private interface ValueSetter {
    void set(Object value);
  }

  @FunctionalInterface
  private interface MapValueSetter {
    void set(Map<String, Object> value);
  }
}
