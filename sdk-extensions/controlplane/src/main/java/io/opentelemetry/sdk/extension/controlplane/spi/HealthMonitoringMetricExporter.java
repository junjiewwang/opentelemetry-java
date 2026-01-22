/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.spi;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpExportMetrics;
import io.opentelemetry.sdk.extension.controlplane.health.SignalType;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 导出指标收集 MetricExporter 包装器
 *
 * <p>包装原始的 MetricExporter，收集导出结果指标。
 * 使用 {@link SignalType#METRIC} 信号类型记录导出结果。
 *
 * <p>Metric 导出相比 Span 导出更稳定和可预测，因此在综合指标计算中权重更高（默认 60%）。
 */
public final class HealthMonitoringMetricExporter implements MetricExporter {

  private static final Logger logger =
      Logger.getLogger(HealthMonitoringMetricExporter.class.getName());

  private final MetricExporter delegate;
  private final OtlpExportMetrics exportMetrics;

  /**
   * 创建导出指标收集 MetricExporter
   *
   * @param delegate 原始 MetricExporter
   * @param exportMetrics 导出指标收集器
   */
  public HealthMonitoringMetricExporter(MetricExporter delegate, OtlpExportMetrics exportMetrics) {
    this.delegate = delegate;
    this.exportMetrics = exportMetrics;
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
            exportMetrics.recordSuccess(SignalType.METRIC);
            logger.log(Level.FINE, "Metric export succeeded, recorded success");
          } else {
            exportMetrics.recordFailure("Metric export failed", SignalType.METRIC);
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
