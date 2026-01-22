/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.status;

import io.opentelemetry.sdk.extension.controlplane.health.OtlpExportMetrics;
import io.opentelemetry.sdk.extension.controlplane.health.SignalHealthTracker;
import io.opentelemetry.sdk.extension.controlplane.health.SignalType;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * OTLP 导出指标收集器
 *
 * <p>收集 OTLP 导出的统计信息，用于状态上报，包括：
 * <ul>
 *   <li>compositeSuccessRate - 综合成功率</li>
 *   <li>spanExportStats - Span 导出统计</li>
 *   <li>metricExportStats - Metric 导出统计</li>
 *   <li>exportConfig - 导出配置</li>
 * </ul>
 */
public final class OtlpExportMetricsCollector implements AgentStatusCollector {

  private static final String NAME = "otlpExportMetrics";

  private final OtlpExportMetrics exportMetrics;
  @Nullable private volatile String lastSpanError;
  @Nullable private volatile String lastMetricError;

  public OtlpExportMetricsCollector(OtlpExportMetrics exportMetrics) {
    this.exportMetrics = exportMetrics;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Map<String, Object> collect() {
    Map<String, Object> data = new HashMap<>();

    // 综合指标
    data.put("compositeSuccessRate", exportMetrics.getSuccessRate());
    data.put("activeSignalCount", exportMetrics.getActiveSignalCount());

    // Span 导出统计
    Map<String, Object> spanStats = collectSignalStats(SignalType.SPAN, lastSpanError);
    data.put("spanExportStats", spanStats);

    // Metric 导出统计
    Map<String, Object> metricStats = collectSignalStats(SignalType.METRIC, lastMetricError);
    data.put("metricExportStats", metricStats);

    // 导出配置
    Map<String, Object> exportConfig = new HashMap<>();
    exportConfig.put("windowMillis", exportMetrics.getWindowMillis());
    exportConfig.put("minSamples", exportMetrics.getMinSamples());
    data.put("exportConfig", exportConfig);

    return data;
  }

  /**
   * 收集单个信号类型的统计信息
   */
  private Map<String, Object> collectSignalStats(SignalType signalType, @Nullable String lastError) {
    Map<String, Object> stats = new HashMap<>();
    SignalHealthTracker tracker = exportMetrics.getTracker(signalType);

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
   * 获取导出指标快照
   *
   * @return 导出指标快照
   */
  public OtlpExportMetrics.ExportMetricsSnapshot getExportMetricsSnapshot() {
    return exportMetrics.createSnapshot();
  }
}
