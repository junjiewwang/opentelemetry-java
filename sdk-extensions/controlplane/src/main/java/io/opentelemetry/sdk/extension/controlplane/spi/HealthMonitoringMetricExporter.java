/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.spi;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpHealthMonitor;
import io.opentelemetry.sdk.extension.controlplane.health.SignalType;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 健康监控 MetricExporter 包装器
 *
 * <p>包装原始的 MetricExporter，监控导出结果以更新 OTLP 健康状态。
 * 使用 {@link SignalType#METRIC} 信号类型记录导出结果。
 *
 * <p>Metric 导出相比 Span 导出更稳定和可预测，因此在综合健康计算中权重更高（默认 60%）。
 */
public final class HealthMonitoringMetricExporter implements MetricExporter {

  private static final Logger logger =
      Logger.getLogger(HealthMonitoringMetricExporter.class.getName());

  private final MetricExporter delegate;
  private final OtlpHealthMonitor healthMonitor;

  /**
   * 创建健康监控 MetricExporter
   *
   * @param delegate 原始 MetricExporter
   * @param healthMonitor 健康监控器
   */
  public HealthMonitoringMetricExporter(MetricExporter delegate, OtlpHealthMonitor healthMonitor) {
    this.delegate = delegate;
    this.healthMonitor = healthMonitor;
    logger.log(
        Level.INFO,
        "HealthMonitoringMetricExporter created, wrapping: {0}",
        delegate.getClass().getName());
  }

  @Override
  public CompletableResultCode export(Collection<MetricData> metrics) {
    logger.log(
        Level.FINE, "HealthMonitoringMetricExporter.export() called with {0} metrics", metrics.size());

    CompletableResultCode result = delegate.export(metrics);

    result.whenComplete(
        () -> {
          if (result.isSuccess()) {
            healthMonitor.recordSuccess(SignalType.METRIC);
            logger.log(Level.FINE, "Metric export succeeded, recorded success");
          } else {
            healthMonitor.recordFailure("Metric export failed", SignalType.METRIC);
            logger.log(Level.FINE, "Metric export failed, recorded failure");
          }
        });

    return result;
  }

  @Override
  public CompletableResultCode flush() {
    return delegate.flush();
  }

  @Override
  public CompletableResultCode shutdown() {
    logger.log(Level.INFO, "Shutting down HealthMonitoringMetricExporter");
    return delegate.shutdown();
  }

  @Override
  public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
    return delegate.getAggregationTemporality(instrumentType);
  }

  /**
   * 获取原始 MetricExporter
   *
   * @return 原始 MetricExporter
   */
  public MetricExporter getDelegate() {
    return delegate;
  }

  @Override
  public String toString() {
    return "HealthMonitoringMetricExporter{delegate=" + delegate + "}";
  }
}
