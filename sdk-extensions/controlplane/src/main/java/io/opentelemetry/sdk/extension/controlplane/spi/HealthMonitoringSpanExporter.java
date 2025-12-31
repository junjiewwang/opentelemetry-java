/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.spi;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpHealthMonitor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 健康监控 SpanExporter 包装器
 *
 * <p>包装原始的 SpanExporter，监控导出结果以更新 OTLP 健康状态。
 */
final class HealthMonitoringSpanExporter implements SpanExporter {

  private static final Logger logger =
      Logger.getLogger(HealthMonitoringSpanExporter.class.getName());

  private final SpanExporter delegate;
  private final OtlpHealthMonitor healthMonitor;

  HealthMonitoringSpanExporter(SpanExporter delegate, OtlpHealthMonitor healthMonitor) {
    this.delegate = delegate;
    this.healthMonitor = healthMonitor;
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
            healthMonitor.recordSuccess();
            logger.log(Level.FINE, "Export succeeded, recorded success");
          } else {
            healthMonitor.recordFailure("Export failed");
            logger.log(Level.FINE, "Export failed, recorded failure");
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

  @Override
  public String toString() {
    return "HealthMonitoringSpanExporter{delegate=" + delegate + "}";
  }
}
