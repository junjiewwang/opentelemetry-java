/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.spi;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpExportMetrics;
import io.opentelemetry.sdk.extension.controlplane.health.SignalType;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 导出指标收集 SpanExporter 包装器
 *
 * <p>包装原始的 SpanExporter，收集导出结果指标。
 * 使用 {@link SignalType#SPAN} 信号类型记录导出结果。
 */
final class HealthMonitoringSpanExporter implements SpanExporter {

  private static final Logger logger =
      Logger.getLogger(HealthMonitoringSpanExporter.class.getName());

  private final SpanExporter delegate;
  private final OtlpExportMetrics exportMetrics;

  HealthMonitoringSpanExporter(SpanExporter delegate, OtlpExportMetrics exportMetrics) {
    this.delegate = delegate;
    this.exportMetrics = exportMetrics;
    logger.log(
        Level.INFO,
        "HealthMonitoringSpanExporter created, wrapping: {0}",
        delegate.getClass().getName());
  }

  @Override
  public CompletableResultCode export(Collection<SpanData> spans) {
    logger.log(Level.FINE, "HealthMonitoringSpanExporter.export() called with {0} spans", spans.size());
    
    CompletableResultCode result = delegate.export(spans);

    result.whenComplete(
        () -> {
          if (result.isSuccess()) {
            // 使用新的多信号源 API
            exportMetrics.recordSuccess(SignalType.SPAN);
            logger.log(Level.FINE, "Span export succeeded, recorded success");
          } else {
            exportMetrics.recordFailure("Span export failed", SignalType.SPAN);
            logger.log(Level.FINE, "Span export failed, recorded failure");
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
    logger.log(Level.INFO, "Shutting down HealthMonitoringSpanExporter");
    return delegate.shutdown();
  }

  /**
   * 获取原始 SpanExporter
   *
   * @return 原始 SpanExporter
   */
  public SpanExporter getDelegate() {
    return delegate;
  }

  @Override
  public String toString() {
    return "HealthMonitoringSpanExporter{delegate=" + delegate + "}";
  }
}
