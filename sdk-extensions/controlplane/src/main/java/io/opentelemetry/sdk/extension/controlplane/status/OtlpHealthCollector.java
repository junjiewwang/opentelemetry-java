/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.status;

import io.opentelemetry.sdk.extension.controlplane.health.OtlpHealthMonitor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * OTLP 健康状态收集器
 *
 * <p>收集 OTLP 导出的健康状态信息，包括：
 * <ul>
 *   <li>otlpHealthState - 整体健康状态
 *   <li>spanExportStats - Span 导出统计
 *   <li>metricExportStats - Metric 导出统计（预留）
 *   <li>logExportStats - Log 导出统计（预留）
 * </ul>
 */
public final class OtlpHealthCollector implements AgentStatusCollector {

  private static final String NAME = "otlpHealth";

  private final OtlpHealthMonitor healthMonitor;
  @Nullable private volatile String lastError;

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

    // 整体健康状态
    data.put("otlpHealthState", healthMonitor.getState().name());

    // Span 导出统计
    Map<String, Object> spanStats = new HashMap<>();
    long successCount = healthMonitor.getSuccessCount();
    long failureCount = healthMonitor.getFailureCount();
    spanStats.put("totalExports", successCount + failureCount);
    spanStats.put("successCount", successCount);
    spanStats.put("failureCount", failureCount);
    spanStats.put("successRate", healthMonitor.getSuccessRate());

    // 转换 nanoTime 为 Unix 时间戳
    long lastSuccessNano = healthMonitor.getLastSuccessTimeNano();
    if (lastSuccessNano > 0) {
      spanStats.put("lastExportTime", nanoTimeToUnixMs(lastSuccessNano));
    }
    if (lastError != null) {
      spanStats.put("lastError", lastError);
    }

    data.put("spanExportStats", spanStats);

    // TODO: 后续添加 metricExportStats 和 logExportStats

    return data;
  }

  @Override
  public int getPriority() {
    return 20;
  }

  /**
   * 记录最后一次错误信息
   *
   * @param error 错误信息
   */
  public void recordLastError(String error) {
    this.lastError = error;
  }

  /**
   * 将 System.nanoTime() 转换为 Unix 毫秒时间戳
   *
   * <p>注意：nanoTime 不是绝对时间，这里通过当前时间差来估算
   */
  private static long nanoTimeToUnixMs(long nanoTime) {
    long nowNano = System.nanoTime();
    long nowMs = System.currentTimeMillis();
    long diffNano = nowNano - nanoTime;
    long diffMs = TimeUnit.NANOSECONDS.toMillis(diffNano);
    return nowMs - diffMs;
  }
}
