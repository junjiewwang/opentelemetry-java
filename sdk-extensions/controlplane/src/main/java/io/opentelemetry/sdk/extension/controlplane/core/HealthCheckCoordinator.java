/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core;

import io.opentelemetry.sdk.extension.controlplane.health.OtlpHealthMonitor;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 健康检查协调器。
 *
 * <p>负责协调 OTLP 健康状态和控制平面连接状态，包括：
 *
 * <ul>
 *   <li>监听 OTLP 健康状态变化
 *   <li>根据健康状态决定是否允许连接
 *   <li>健康状态与连接状态的联动
 * </ul>
 */
public final class HealthCheckCoordinator implements OtlpHealthMonitor.HealthStateListener {

  private static final Logger logger = Logger.getLogger(HealthCheckCoordinator.class.getName());

  private final OtlpHealthMonitor healthMonitor;
  private final ConnectionStateManager connectionStateManager;

  /**
   * 创建健康检查协调器
   *
   * @param healthMonitor OTLP 健康监控器
   * @param connectionStateManager 连接状态管理器
   */
  public HealthCheckCoordinator(
      OtlpHealthMonitor healthMonitor, ConnectionStateManager connectionStateManager) {
    this.healthMonitor = healthMonitor;
    this.connectionStateManager = connectionStateManager;
  }

  /** 启动协调器，注册监听器 */
  public void start() {
    healthMonitor.addListener(this);
    logger.log(Level.FINE, "Health check coordinator started");
  }

  /** 停止协调器，移除监听器 */
  public void stop() {
    healthMonitor.removeListener(this);
    logger.log(Level.FINE, "Health check coordinator stopped");
  }

  /**
   * 检查是否应该连接控制平面
   *
   * @return 是否应该连接
   */
  public boolean shouldConnect() {
    if (!healthMonitor.isHealthy()) {
      ConnectionStateManager.ConnectionState currentState = connectionStateManager.getState();
      if (currentState != ConnectionStateManager.ConnectionState.WAITING_FOR_OTLP) {
        connectionStateManager.markWaitingForOtlp();
        logger.log(
            Level.INFO,
            "OTLP is not healthy (state: {0}), waiting for recovery before connecting to control plane",
            healthMonitor.getState());
      }
      return false;
    }
    return true;
  }

  /**
   * 检查 OTLP 是否健康
   *
   * @return 是否健康
   */
  public boolean isOtlpHealthy() {
    return healthMonitor.isHealthy();
  }

  /**
   * 获取 OTLP 健康状态
   *
   * @return 健康状态
   */
  public OtlpHealthMonitor.HealthState getOtlpHealthState() {
    return healthMonitor.getState();
  }

  /**
   * 构建详细的 OTLP 健康信息字符串
   *
   * @return 健康信息字符串
   */
  public String buildOtlpHealthInfo() {
    OtlpHealthMonitor.HealthState state = healthMonitor.getState();
    long successCount = healthMonitor.getSuccessCount();
    long failureCount = healthMonitor.getFailureCount();
    double successRate = healthMonitor.getSuccessRate();
    long totalSamples = successCount + failureCount;

    // 当没有采样数据时，显示更友好的提示
    if (totalSamples == 0) {
      return "state=UNKNOWN, no samples yet (waiting for span exports)";
    }

    // 格式: state=HEALTHY, success/fail=95/5, rate=95.0%
    return String.format(
        Locale.ROOT,
        "state=%s, success/fail=%d/%d, rate=%.1f%%",
        state,
        successCount,
        failureCount,
        successRate * 100);
  }

  @Override
  public void onStateChanged(
      OtlpHealthMonitor.HealthState previousState, OtlpHealthMonitor.HealthState newState) {
    logger.log(
        Level.INFO,
        "OTLP health state changed: {0} -> {1}",
        new Object[] {previousState, newState});

    ConnectionStateManager.ConnectionState currentConnectionState = connectionStateManager.getState();

    if (newState == OtlpHealthMonitor.HealthState.HEALTHY
        && currentConnectionState == ConnectionStateManager.ConnectionState.WAITING_FOR_OTLP) {
      // OTLP 恢复健康，尝试重新连接
      connectionStateManager.markConnecting();
      logger.log(Level.INFO, "OTLP recovered, reconnecting to control plane");
    } else if (newState == OtlpHealthMonitor.HealthState.UNHEALTHY) {
      // OTLP 不健康，暂停连接
      connectionStateManager.markWaitingForOtlp();
      logger.log(Level.INFO, "OTLP became unhealthy, pausing control plane connection");
    }
  }
}
