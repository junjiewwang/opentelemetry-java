/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.health;

/**
 * 遥测信号类型枚举
 *
 * <p>用于区分不同类型的 OTLP 导出信号，支持多信号源综合健康检测。
 */
public enum SignalType {
  /** Span/Trace 信号 */
  SPAN("span", 0.4),

  /** Metric 信号 */
  METRIC("metric", 0.6);

  private final String name;
  private final double defaultWeight;

  SignalType(String name, double defaultWeight) {
    this.name = name;
    this.defaultWeight = defaultWeight;
  }

  /**
   * 获取信号类型名称
   *
   * @return 信号类型名称
   */
  public String getName() {
    return name;
  }

  /**
   * 获取默认权重
   *
   * <p>Metric 权重更高（0.6），因为其导出频率更稳定和可预测。
   * Span 权重较低（0.4），因为其导出频率受业务流量影响较大。
   *
   * @return 默认权重 (0.0 ~ 1.0)
   */
  public double getDefaultWeight() {
    return defaultWeight;
  }
}
