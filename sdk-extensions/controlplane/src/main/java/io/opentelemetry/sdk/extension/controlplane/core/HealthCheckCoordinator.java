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
  private final ConnectionGatePolicy gatePolicy;

  private volatile GateDecision lastGateDecision = GateDecision.allowed("initial");

  /**
   * 创建健康检查协调器
   *
   * @param healthMonitor OTLP 健康监控器
   * @param connectionStateManager 连接状态管理器
   */
  public HealthCheckCoordinator(
      OtlpHealthMonitor healthMonitor, ConnectionStateManager connectionStateManager) {
    this(healthMonitor, connectionStateManager, ConnectionGatePolicies.defaultPolicy());
  }

  /**
   * 创建健康检查协调器（可注入 GatePolicy）
   *
   * @param healthMonitor OTLP 健康监控器
   * @param connectionStateManager 连接状态管理器
   * @param gatePolicy 连接开闸策略
   */
  public HealthCheckCoordinator(
      OtlpHealthMonitor healthMonitor,
      ConnectionStateManager connectionStateManager,
      ConnectionGatePolicy gatePolicy) {
    this.healthMonitor = healthMonitor;
    this.connectionStateManager = connectionStateManager;
    this.gatePolicy = gatePolicy;
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
    GateDecision decision = gatePolicy.decide(healthMonitor);
    lastGateDecision = decision;

    if (!decision.isAllowed()) {
      ConnectionStateManager.ConnectionState currentState = connectionStateManager.getState();
      if (currentState != ConnectionStateManager.ConnectionState.WAITING_FOR_OTLP) {
        connectionStateManager.markWaitingForOtlp();
        logger.log(
            Level.INFO,
            "Control plane connection gated (otlpState={0}, gate={1}), waiting for recovery before connecting to control plane",
            new Object[] {healthMonitor.getState(), decision});
      }
      return false;
    }
    return true;
  }

  /** 获取最近一次 Gate 决策（用于诊断/日志） */
  public GateDecision getLastGateDecision() {
    return lastGateDecision;
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
      return "state=UNKNOWN, no samples yet (waiting for span exports), gate=" + lastGateDecision;
    }

    // 格式: state=HEALTHY, success/fail=95/5, rate=95.0%
    return String.format(
        Locale.ROOT,
        "state=%s, success/fail=%d/%d, rate=%.1f%%, gate=%s",
        state,
        successCount,
        failureCount,
        successRate * 100,
        lastGateDecision);
  }

  @Override
  public void onStateChanged(
      OtlpHealthMonitor.HealthState previousState, OtlpHealthMonitor.HealthState newState) {
    logger.log(
        Level.INFO,
        "OTLP health state changed: {0} -> {1}",
        new Object[] {previousState, newState});

    ConnectionStateManager.ConnectionState currentConnectionState = connectionStateManager.getState();

    // 仅做状态提示，不再直接用 health state 驱动连接开关。
    // 连接开关由 shouldConnect() + gatePolicy 决定。
    if (currentConnectionState == ConnectionStateManager.ConnectionState.WAITING_FOR_OTLP) {
      GateDecision decision = gatePolicy.decide(healthMonitor);
      lastGateDecision = decision;
      if (decision.isAllowed()) {
        connectionStateManager.markConnecting();
        logger.log(Level.INFO, "OTLP gate opened ({0}), reconnecting to control plane", decision);
      }
    }
  }

  /**
   * Gate 决策
   */
  public static final class GateDecision {
    private final boolean allowed;
    private final String reason;

    private GateDecision(boolean allowed, String reason) {
      this.allowed = allowed;
      this.reason = reason;
    }

    public static GateDecision allowed(String reason) {
      return new GateDecision(/* allowed= */ true, reason);
    }

    public static GateDecision blocked(String reason) {
      return new GateDecision(/* allowed= */ false, reason);
    }

    public boolean isAllowed() {
      return allowed;
    }

    public String getReason() {
      return reason;
    }

    @Override
    public String toString() {
      return (allowed ? "OPEN" : "CLOSED") + "(" + reason + ")";
    }
  }

  /**
   * 连接开闸策略。
   *
   * <p>将“健康评估”(monitor) 与 “是否允许连接”(gate) 解耦，避免语义混乱。
   */
  public interface ConnectionGatePolicy {
    GateDecision decide(OtlpHealthMonitor monitor);
  }

  /** 内置策略集合 */
  public static final class ConnectionGatePolicies {

    private ConnectionGatePolicies() {}

    /**
     * 默认策略：
     *
     * <ul>
     *   <li>HEALTHY/UNKNOWN: 允许连接（保持现有语义）
     *   <li>DEGRADED: 若最近有成功(默认10s)则快速放行，否则阻断
     *   <li>UNHEALTHY: 阻断
     * </ul>
     */
    public static ConnectionGatePolicy defaultPolicy() {
      return new RecentSuccessFastOpenPolicy(/* fastOpenWindowMillis= */ 10_000);
    }
  }

  /**
   * 快速恢复策略：只要最近一段时间内有成功样本，就允许连接进入“半开/探测”状态。
   *
   * <p>实现目标：满足“看到 success 就尽快恢复”，同时保留 monitor 的窗口稳定性。
   */
  static final class RecentSuccessFastOpenPolicy implements ConnectionGatePolicy {

    private final long fastOpenWindowMillis;

    RecentSuccessFastOpenPolicy(long fastOpenWindowMillis) {
      this.fastOpenWindowMillis = fastOpenWindowMillis;
    }

    @Override
    public GateDecision decide(OtlpHealthMonitor monitor) {
      OtlpHealthMonitor.HealthState state = monitor.getState();

      if (state == OtlpHealthMonitor.HealthState.HEALTHY
          || state == OtlpHealthMonitor.HealthState.UNKNOWN) {
        return GateDecision.allowed("otlp_state=" + state);
      }

      if (state == OtlpHealthMonitor.HealthState.UNHEALTHY) {
        return GateDecision.blocked("otlp_state=UNHEALTHY");
      }

      // DEGRADED: 允许“快速开闸”以便尽快重连并继续探测
      long lastSuccessNano = monitor.getLastSuccessTimeNano();
      if (lastSuccessNano <= 0) {
        return GateDecision.blocked("otlp_state=DEGRADED,no_recent_success");
      }

      long ageMillis = (System.nanoTime() - lastSuccessNano) / 1_000_000;
      if (ageMillis <= fastOpenWindowMillis) {
        return GateDecision.allowed("otlp_state=DEGRADED,recent_success_age_ms=" + ageMillis);
      }

      return GateDecision.blocked("otlp_state=DEGRADED,stale_success_age_ms=" + ageMillis);
    }
  }
}
