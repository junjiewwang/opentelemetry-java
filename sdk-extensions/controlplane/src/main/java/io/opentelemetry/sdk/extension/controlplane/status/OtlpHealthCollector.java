/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.status;

import io.opentelemetry.sdk.extension.controlplane.health.CompositeHealthCalculator;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpHealthMonitor;
import io.opentelemetry.sdk.extension.controlplane.health.SignalHealthTracker;
import io.opentelemetry.sdk.extension.controlplane.health.SignalType;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * OTLP 健康状态收集器
 *
 * <p>收集 OTLP 导出的健康状态信息，包括：
 * <ul>
 *   <li>otlpHealthState - 综合健康状态</li>
 *   <li>compositeSuccessRate - 综合成功率</li>
 *   <li>spanExportStats - Span 导出统计</li>
 *   <li>metricExportStats - Metric 导出统计</li>
 *   <li>healthConfig - 健康检测配置</li>
 * </ul>
 */
public final class OtlpHealthCollector implements AgentStatusCollector {

  private static final String NAME = "otlpHealth";

  private final OtlpHealthMonitor healthMonitor;
  @Nullable private volatile String lastSpanError;
  @Nullable private volatile String lastMetricError;

  public OtlpHealthCollector(OtlpHealthMonitor healthMonitor) {
    this.healthMonitor = healthMonitor;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Map<String, Object> collect() {
    Map<String, Object> data = new HashMap<>();

    // 综合健康状态
    data.put("otlpHealthState", healthMonitor.getState().name());
    data.put("compositeSuccessRate", healthMonitor.getSuccessRate());
    data.put("activeSignalCount", healthMonitor.getActiveSignalCount());
    data.put("stateTransitionCount", healthMonitor.getStateTransitionCount());

    // Span 导出统计
    Map<String, Object> spanStats = collectSignalStats(SignalType.SPAN, lastSpanError);
    data.put("spanExportStats", spanStats);

    // Metric 导出统计
    Map<String, Object> metricStats = collectSignalStats(SignalType.METRIC, lastMetricError);
    data.put("metricExportStats", metricStats);

    // 健康配置
    Map<String, Object> healthConfig = new HashMap<>();
    healthConfig.put("windowMillis", healthMonitor.getWindowMillis());
    healthConfig.put("healthyThreshold", healthMonitor.getHealthyThreshold());
    healthConfig.put("unhealthyThreshold", healthMonitor.getUnhealthyThreshold());
    healthConfig.put("cooldownMillis", healthMonitor.getCooldownMillis());
    healthConfig.put("minSamples", healthMonitor.getMinSamples());
    data.put("healthConfig", healthConfig);

    return data;
  }

  /**
   * 收集单个信号类型的统计信息
   */
  private Map<String, Object> collectSignalStats(SignalType signalType, @Nullable String lastError) {
    Map<String, Object> stats = new HashMap<>();
    SignalHealthTracker tracker = healthMonitor.getTracker(signalType);

    if (tracker != null) {
      SignalHealthTracker.SignalHealthSnapshot snapshot = tracker.createSnapshot();

      stats.put("successRate", snapshot.getSuccessRate());
      stats.put("sampleCount", snapshot.getSampleCount());
      stats.put("totalSuccessCount", snapshot.getTotalSuccessCount());
      stats.put("totalFailureCount", snapshot.getTotalFailureCount());
      stats.put("weight", snapshot.getWeight());
      stats.put("hasEnoughSamples", snapshot.hasEnoughSamples());

      // 转换时间戳
      long lastSuccessMillis = snapshot.getLastSuccessTimeMillis();
      if (lastSuccessMillis > 0) {
        stats.put("lastSuccessTime", lastSuccessMillis);
      }

      long lastFailureMillis = snapshot.getLastFailureTimeMillis();
      if (lastFailureMillis > 0) {
        stats.put("lastFailureTime", lastFailureMillis);
      }

      // 获取最后一次错误
      String trackerLastError = snapshot.getLastError();
      if (trackerLastError != null) {
        stats.put("lastError", trackerLastError);
      } else if (lastError != null) {
        stats.put("lastError", lastError);
      }
    } else {
      // 信号不可用
      stats.put("available", false);
    }

    return stats;
  }

  @Override
  public int getPriority() {
    return 20;
  }

  /**
   * 记录最后一次 Span 错误信息
   *
   * @param error 错误信息
   */
  public void recordLastSpanError(String error) {
    this.lastSpanError = error;
  }

  /**
   * 记录最后一次 Metric 错误信息
   *
   * @param error 错误信息
   */
  public void recordLastMetricError(String error) {
    this.lastMetricError = error;
  }

  /**
   * 记录最后一次错误信息（向后兼容，默认记录到 Span）
   *
   * @param error 错误信息
   * @deprecated 使用 {@link #recordLastSpanError(String)} 或 {@link #recordLastMetricError(String)}
   */
  @Deprecated
  public void recordLastError(String error) {
    recordLastSpanError(error);
  }

  /**
   * 获取综合健康快照
   *
   * @return 综合健康快照
   */
  public CompositeHealthCalculator.CompositeHealthSnapshot getHealthSnapshot() {
    return healthMonitor.createSnapshot();
  }
}
