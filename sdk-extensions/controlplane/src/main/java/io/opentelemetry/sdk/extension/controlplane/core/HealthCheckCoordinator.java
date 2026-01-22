/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core;

import io.opentelemetry.sdk.extension.controlplane.status.HeartbeatReporter;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 健康检查协调器。
 *
 * <p>负责协调健康状态和控制平面连接状态，包括：
 *
 * <ul>
 *   <li>根据心跳健康状态决定是否允许连接
 *   <li>健康状态与连接状态的联动
 * </ul>
 *
 * <p>使用心跳作为唯一健康判断依据，避免因服务端下发低采样率或停止采集导致的误判。
 */
public final class HealthCheckCoordinator {

  private static final Logger logger = Logger.getLogger(HealthCheckCoordinator.class.getName());

  private final HeartbeatReporter heartbeatReporter;
  private final ConnectionStateManager connectionStateManager;
  private final ConnectionGatePolicy gatePolicy;

  private volatile GateDecision lastGateDecision = GateDecision.allowed("initial");

  /**
   * 创建健康检查协调器
   *
   * @param heartbeatReporter 心跳上报器（必选）
   * @param connectionStateManager 连接状态管理器
   */
  public HealthCheckCoordinator(
      HeartbeatReporter heartbeatReporter, ConnectionStateManager connectionStateManager) {
    this(heartbeatReporter, connectionStateManager, ConnectionGatePolicies.heartbeatOnlyPolicy());
  }

  /**
   * 创建健康检查协调器（可自定义策略）
   *
   * @param heartbeatReporter 心跳上报器（必选）
   * @param connectionStateManager 连接状态管理器
   * @param gatePolicy 连接开闸策略
   */
  public HealthCheckCoordinator(
      HeartbeatReporter heartbeatReporter,
      ConnectionStateManager connectionStateManager,
      ConnectionGatePolicy gatePolicy) {
    this.heartbeatReporter = heartbeatReporter;
    this.connectionStateManager = connectionStateManager;
    this.gatePolicy = gatePolicy;
  }

  /**
   * 启动协调器
   */
  public void start() {
    logger.log(Level.FINE, "Health check coordinator started");
  }

  /**
   * 停止协调器
   */
  public void stop() {
    logger.log(Level.FINE, "Health check coordinator stopped");
  }

  /**
   * 检查是否应该连接控制平面
   *
   * @return 是否应该连接
   */
  public boolean shouldConnect() {
    GateDecision decision = gatePolicy.decide(this);
    lastGateDecision = decision;

    if (!decision.isAllowed()) {
      ConnectionStateManager.ConnectionState currentState = connectionStateManager.getState();
      if (currentState != ConnectionStateManager.ConnectionState.WAITING_FOR_OTLP) {
        connectionStateManager.markWaitingForOtlp();
        logger.log(
            Level.INFO,
            "Control plane connection gated (gate={0}), waiting for recovery before connecting",
            decision);
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
   * 获取心跳上报器
   *
   * @return 心跳上报器
   */
  public HeartbeatReporter getHeartbeatReporter() {
    return heartbeatReporter;
  }

  /**
   * 检查心跳是否健康
   *
   * @return 是否健康
   */
  public boolean isHeartbeatHealthy() {
    return heartbeatReporter.isHealthy();
  }

  /**
   * 构建详细的健康信息字符串
   *
   * @return 健康信息字符串
   */
  public String buildHealthInfo() {
    double heartbeatRate = heartbeatReporter.getSuccessRate();
    long heartbeatCount = heartbeatReporter.getHeartbeatCount();
    boolean heartbeatHealthy = heartbeatReporter.isHealthy();

    return String.format(Locale.ROOT,
        "heartbeat={healthy=%s, rate=%.1f%%, count=%d}, gate=%s",
        heartbeatHealthy, heartbeatRate * 100, heartbeatCount, lastGateDecision);
  }

  // ==================== Gate 决策 ====================

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

  // ==================== 连接开闸策略 ====================

  /**
   * 连接开闸策略。
   *
   * <p>将"健康评估"与"是否允许连接"解耦。
   */
  public interface ConnectionGatePolicy {
    /**
     * 决定是否允许连接
     *
     * @param coordinator 健康检查协调器
     * @return Gate 决策
     */
    GateDecision decide(HealthCheckCoordinator coordinator);
  }

  /** 内置策略集合 */
  public static final class ConnectionGatePolicies {

    private ConnectionGatePolicies() {}

    /**
     * 仅心跳策略（默认）
     *
     * <p>仅使用心跳作为健康判断依据，解决采样配置导致信号稀疏时的误判问题。
     *
     * <p>决策逻辑：
     * <ol>
     *   <li>如果心跳健康（成功率 &gt;= 80%）：允许连接</li>
     *   <li>如果心跳严重不健康（成功率 &lt; 50%）：阻断连接</li>
     *   <li>中间状态（50%~80%）或无心跳记录：允许连接（乐观策略）</li>
     * </ol>
     *
     * @return 仅心跳策略
     */
    public static ConnectionGatePolicy heartbeatOnlyPolicy() {
      return new HeartbeatOnlyGatePolicy(
          /* heartbeatIntervalMillis= */ 30_000,
          /* heartbeatUnhealthyThreshold= */ 0.5);
    }

    /**
     * 仅心跳策略（可配置心跳间隔）
     *
     * @param heartbeatIntervalMillis 心跳间隔（毫秒），用于判断心跳是否卡住
     * @return 仅心跳策略
     */
    public static ConnectionGatePolicy heartbeatOnlyPolicy(long heartbeatIntervalMillis) {
      return new HeartbeatOnlyGatePolicy(
          heartbeatIntervalMillis,
          /* heartbeatUnhealthyThreshold= */ 0.5);
    }

    /**
     * 仅心跳策略（可配置心跳间隔和不健康阈值）
     *
     * @param heartbeatIntervalMillis 心跳间隔（毫秒），用于判断心跳是否卡住
     * @param heartbeatUnhealthyThreshold 心跳不健康阈值（成功率低于此值则阻断）
     * @return 仅心跳策略
     */
    public static ConnectionGatePolicy heartbeatOnlyPolicy(
        long heartbeatIntervalMillis, double heartbeatUnhealthyThreshold) {
      return new HeartbeatOnlyGatePolicy(heartbeatIntervalMillis, heartbeatUnhealthyThreshold);
    }
  }

  // ==================== 策略实现 ====================

  /**
   * 仅心跳策略
   *
   * <p>仅使用心跳作为健康判断依据，不依赖 OTLP 信号。
   * 中间状态采用乐观策略（允许连接），确保控制平面连接的可用性。
   */
  static final class HeartbeatOnlyGatePolicy implements ConnectionGatePolicy {

    private final long heartbeatIntervalMillis;
    private final double heartbeatUnhealthyThreshold;

    HeartbeatOnlyGatePolicy(long heartbeatIntervalMillis, double heartbeatUnhealthyThreshold) {
      this.heartbeatIntervalMillis = heartbeatIntervalMillis;
      this.heartbeatUnhealthyThreshold = heartbeatUnhealthyThreshold;
    }

    @Override
    public GateDecision decide(HealthCheckCoordinator coordinator) {
      HeartbeatReporter heartbeat = coordinator.getHeartbeatReporter();

      // 1. 心跳新鲜度检查：避免心跳卡住导致误判
      long lastHbTimeMs = heartbeat.getLastHeartbeatTimeMs();
      long now = System.currentTimeMillis();

      if (lastHbTimeMs > 0 && (now - lastHbTimeMs) > 2 * heartbeatIntervalMillis) {
        // 心跳超过2个周期未更新，可能卡住，乐观允许连接
        return GateDecision.allowed("heartbeat_stale,optimistic");
      }

      // 2. 至少有1次心跳记录才开始判断
      if (heartbeat.getHeartbeatCount() < 1) {
        // 无心跳记录，乐观允许连接
        return GateDecision.allowed("no_heartbeat_yet,optimistic");
      }

      // 3. 核心判断：直接复用 HeartbeatReporter.isHealthy()
      if (heartbeat.isHealthy()) {
        return GateDecision.allowed(
            "heartbeat_healthy,rate=" + formatPercent(heartbeat.getSuccessRate()));
      }

      // 4. 心跳严重不健康（<50%）→ 阻断
      double successRate = heartbeat.getSuccessRate();
      if (successRate < heartbeatUnhealthyThreshold) {
        return GateDecision.blocked(
            "heartbeat_unhealthy,rate=" + formatPercent(successRate));
      }

      // 5. 中间状态（50%~80%）→ 乐观允许连接
      return GateDecision.allowed(
          "heartbeat_degraded,rate=" + formatPercent(successRate) + ",optimistic");
    }

    private static String formatPercent(double rate) {
      return String.format(Locale.ROOT, "%.1f%%", rate * 100);
    }
  }
}
