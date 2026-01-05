/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.health;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 综合健康计算器
 *
 * <p>负责根据多个信号源的健康状态计算综合健康状态。
 * 使用加权平均算法，支持动态权重调整和冷却时间机制。
 *
 * <p>设计原则：
 * <ul>
 *   <li>高内聚：仅负责健康状态计算，不涉及数据收集</li>
 *   <li>可扩展：支持自定义阈值和降级策略</li>
 *   <li>健壮性：处理信号不可用、紧急状态等边界情况</li>
 * </ul>
 */
public final class CompositeHealthCalculator {

  private static final Logger logger = Logger.getLogger(CompositeHealthCalculator.class.getName());

  /** 默认健康阈值：成功率 >= 90% 为健康 */
  private static final double DEFAULT_HEALTHY_THRESHOLD = 0.9;

  /** 默认不健康阈值：成功率 <= 50% 为不健康 */
  private static final double DEFAULT_UNHEALTHY_THRESHOLD = 0.5;

  /** 默认冷却时间：5 秒 */
  private static final long DEFAULT_COOLDOWN_MILLIS = 5_000L;

  /** 紧急状态阈值：所有信号成功率为 0 时跳过冷却 */
  private static final double EMERGENCY_THRESHOLD = 0.0;

  private final Map<SignalType, SignalHealthTracker> trackers;
  private final double healthyThreshold;
  private final double unhealthyThreshold;
  private final long cooldownMillis;
  private final AtomicReference<OtlpHealthMonitor.HealthState> currentState;
  private final AtomicLong lastStateChangeTimeMillis;
  private final AtomicLong stateTransitionCount;
  private final AtomicLong cooldownSkipCount;

  /**
   * 创建综合健康计算器（使用默认配置）
   *
   * @param trackers 信号健康跟踪器映射
   */
  public CompositeHealthCalculator(Map<SignalType, SignalHealthTracker> trackers) {
    this(trackers, DEFAULT_HEALTHY_THRESHOLD, DEFAULT_UNHEALTHY_THRESHOLD, DEFAULT_COOLDOWN_MILLIS);
  }

  /**
   * 创建综合健康计算器
   *
   * @param trackers 信号健康跟踪器映射
   * @param healthyThreshold 健康阈值
   * @param unhealthyThreshold 不健康阈值
   * @param cooldownMillis 冷却时间（毫秒）
   */
  public CompositeHealthCalculator(
      Map<SignalType, SignalHealthTracker> trackers,
      double healthyThreshold,
      double unhealthyThreshold,
      long cooldownMillis) {
    if (healthyThreshold <= unhealthyThreshold) {
      throw new IllegalArgumentException(
          "healthyThreshold must be greater than unhealthyThreshold");
    }
    this.trackers = new EnumMap<>(trackers);
    this.healthyThreshold = healthyThreshold;
    this.unhealthyThreshold = unhealthyThreshold;
    this.cooldownMillis = cooldownMillis;
    this.currentState = new AtomicReference<>(OtlpHealthMonitor.HealthState.UNKNOWN);
    this.lastStateChangeTimeMillis = new AtomicLong(0);
    this.stateTransitionCount = new AtomicLong(0);
    this.cooldownSkipCount = new AtomicLong(0);
  }

  /**
   * 计算并更新综合健康状态
   *
   * @return 更新后的健康状态
   */
  public OtlpHealthMonitor.HealthState calculateAndUpdate() {
    // 计算加权成功率
    double compositeSuccessRate = calculateCompositeSuccessRate();
    int activeSampleCount = getActiveSampleCount();

    // 确定目标状态
    OtlpHealthMonitor.HealthState targetState = determineTargetState(compositeSuccessRate, activeSampleCount);

    // 应用冷却时间和紧急状态检测
    OtlpHealthMonitor.HealthState previousState = currentState.get();
    OtlpHealthMonitor.HealthState newState = applyStateTransition(targetState, compositeSuccessRate);

    // 如果状态发生变化，更新统计
    if (previousState != newState) {
      lastStateChangeTimeMillis.set(System.currentTimeMillis());
      stateTransitionCount.incrementAndGet();
      logger.log(
          Level.INFO,
          "OTLP composite health state changed: {0} -> {1} (compositeSuccessRate={2}, activeSamples={3})",
          new Object[] {previousState, newState, compositeSuccessRate, activeSampleCount});
    }

    return newState;
  }

  /**
   * 计算加权综合成功率
   *
   * @return 综合成功率 (0.0 ~ 1.0)
   */
  public double calculateCompositeSuccessRate() {
    double totalWeight = 0.0;
    double weightedSum = 0.0;

    for (SignalHealthTracker tracker : trackers.values()) {
      if (tracker.isActive() && tracker.hasEnoughSamples()) {
        double weight = tracker.getWeight();
        double successRate = tracker.getSuccessRate();
        weightedSum += weight * successRate;
        totalWeight += weight;
      }
    }

    // 如果没有活跃信号，返回 1.0（乐观默认）
    if (totalWeight == 0.0) {
      return 1.0;
    }

    // 归一化：重新分配权重
    return weightedSum / totalWeight;
  }

  /**
   * 获取活跃信号的总样本数
   *
   * @return 总样本数
   */
  public int getActiveSampleCount() {
    int total = 0;
    for (SignalHealthTracker tracker : trackers.values()) {
      if (tracker.isActive()) {
        total += tracker.getSampleCount();
      }
    }
    return total;
  }

  /**
   * 获取活跃信号数量
   *
   * @return 活跃信号数量
   */
  public int getActiveSignalCount() {
    int count = 0;
    for (SignalHealthTracker tracker : trackers.values()) {
      if (tracker.isActive() && tracker.hasEnoughSamples()) {
        count++;
      }
    }
    return count;
  }

  /**
   * 根据成功率确定目标状态
   */
  private OtlpHealthMonitor.HealthState determineTargetState(double successRate, int sampleCount) {
    // 样本数不足时保持 UNKNOWN
    if (sampleCount == 0 || getActiveSignalCount() == 0) {
      return OtlpHealthMonitor.HealthState.UNKNOWN;
    }

    if (successRate >= healthyThreshold) {
      return OtlpHealthMonitor.HealthState.HEALTHY;
    } else if (successRate <= unhealthyThreshold) {
      return OtlpHealthMonitor.HealthState.UNHEALTHY;
    } else {
      return OtlpHealthMonitor.HealthState.DEGRADED;
    }
  }

  /**
   * 应用状态转换（包括冷却时间和紧急状态检测）
   */
  private OtlpHealthMonitor.HealthState applyStateTransition(
      OtlpHealthMonitor.HealthState targetState, double compositeSuccessRate) {
    OtlpHealthMonitor.HealthState currentStateValue = currentState.get();

    // 处理 null 情况（初始状态）
    if (currentStateValue == null) {
      currentState.set(targetState);
      return targetState;
    }

    // 如果目标状态与当前状态相同，直接返回
    if (targetState == currentStateValue) {
      return currentStateValue;
    }

    // 紧急状态检测：所有信号成功率为 0，跳过冷却直接切换到 UNHEALTHY
    if (targetState == OtlpHealthMonitor.HealthState.UNHEALTHY
        && compositeSuccessRate <= EMERGENCY_THRESHOLD) {
      cooldownSkipCount.incrementAndGet();
      logger.log(
          Level.WARNING,
          "Emergency state detected (compositeSuccessRate=0), skipping cooldown");
      currentState.set(targetState);
      return targetState;
    }

    // 从 UNKNOWN 状态切换不需要冷却
    if (currentStateValue == OtlpHealthMonitor.HealthState.UNKNOWN) {
      currentState.set(targetState);
      return targetState;
    }

    // 检查冷却时间
    long now = System.currentTimeMillis();
    long lastChange = lastStateChangeTimeMillis.get();
    if (now - lastChange < cooldownMillis) {
      // 冷却期内，保持当前状态
      logger.log(
          Level.FINE,
          "State transition blocked by cooldown: {0} -> {1} (remainingCooldown={2}ms)",
          new Object[] {currentStateValue, targetState, cooldownMillis - (now - lastChange)});
      return currentStateValue;
    }

    // 冷却期已过，切换状态
    currentState.set(targetState);
    return targetState;
  }

  /**
   * 获取当前健康状态
   *
   * @return 当前健康状态
   */
  public OtlpHealthMonitor.HealthState getCurrentState() {
    OtlpHealthMonitor.HealthState state = currentState.get();
    return state != null ? state : OtlpHealthMonitor.HealthState.UNKNOWN;
  }

  /**
   * 获取健康阈值
   *
   * @return 健康阈值
   */
  public double getHealthyThreshold() {
    return healthyThreshold;
  }

  /**
   * 获取不健康阈值
   *
   * @return 不健康阈值
   */
  public double getUnhealthyThreshold() {
    return unhealthyThreshold;
  }

  /**
   * 获取冷却时间（毫秒）
   *
   * @return 冷却时间
   */
  public long getCooldownMillis() {
    return cooldownMillis;
  }

  /**
   * 获取状态转换次数
   *
   * @return 状态转换次数
   */
  public long getStateTransitionCount() {
    return stateTransitionCount.get();
  }

  /**
   * 获取冷却跳过次数（紧急状态）
   *
   * @return 冷却跳过次数
   */
  public long getCooldownSkipCount() {
    return cooldownSkipCount.get();
  }

  /**
   * 获取最后一次状态变化时间（毫秒）
   *
   * @return 最后状态变化时间
   */
  public long getLastStateChangeTimeMillis() {
    return lastStateChangeTimeMillis.get();
  }

  /**
   * 创建综合健康快照
   *
   * @return 综合健康快照
   */
  public CompositeHealthSnapshot createSnapshot() {
    EnumMap<SignalType, SignalHealthTracker.SignalHealthSnapshot> signalSnapshots =
        new EnumMap<>(SignalType.class);
    for (Map.Entry<SignalType, SignalHealthTracker> entry : trackers.entrySet()) {
      signalSnapshots.put(entry.getKey(), entry.getValue().createSnapshot());
    }

    return new CompositeHealthSnapshot(
        getCurrentState(),
        calculateCompositeSuccessRate(),
        getActiveSignalCount(),
        getActiveSampleCount(),
        signalSnapshots,
        stateTransitionCount.get(),
        cooldownSkipCount.get(),
        lastStateChangeTimeMillis.get());
  }

  @Override
  public String toString() {
    return String.format(
        java.util.Locale.ROOT,
        "CompositeHealthCalculator{state=%s, compositeRate=%.2f%%, activeSignals=%d, transitions=%d}",
        getCurrentState(),
        calculateCompositeSuccessRate() * 100,
        getActiveSignalCount(),
        stateTransitionCount.get());
  }

  /**
   * 综合健康快照
   *
   * <p>用于诊断和状态上报的不可变快照。
   */
  public static final class CompositeHealthSnapshot {
    private final OtlpHealthMonitor.HealthState state;
    private final double compositeSuccessRate;
    private final int activeSignalCount;
    private final int totalSampleCount;
    private final Map<SignalType, SignalHealthTracker.SignalHealthSnapshot> signalSnapshots;
    private final long stateTransitionCount;
    private final long cooldownSkipCount;
    private final long lastStateChangeTimeMillis;

    CompositeHealthSnapshot(
        OtlpHealthMonitor.HealthState state,
        double compositeSuccessRate,
        int activeSignalCount,
        int totalSampleCount,
        Map<SignalType, SignalHealthTracker.SignalHealthSnapshot> signalSnapshots,
        long stateTransitionCount,
        long cooldownSkipCount,
        long lastStateChangeTimeMillis) {
      this.state = state;
      this.compositeSuccessRate = compositeSuccessRate;
      this.activeSignalCount = activeSignalCount;
      this.totalSampleCount = totalSampleCount;
      this.signalSnapshots = new EnumMap<>(signalSnapshots);
      this.stateTransitionCount = stateTransitionCount;
      this.cooldownSkipCount = cooldownSkipCount;
      this.lastStateChangeTimeMillis = lastStateChangeTimeMillis;
    }

    public OtlpHealthMonitor.HealthState getState() {
      return state;
    }

    public double getCompositeSuccessRate() {
      return compositeSuccessRate;
    }

    public int getActiveSignalCount() {
      return activeSignalCount;
    }

    public int getTotalSampleCount() {
      return totalSampleCount;
    }

    public Map<SignalType, SignalHealthTracker.SignalHealthSnapshot> getSignalSnapshots() {
      return signalSnapshots;
    }

    public long getStateTransitionCount() {
      return stateTransitionCount;
    }

    public long getCooldownSkipCount() {
      return cooldownSkipCount;
    }

    public long getLastStateChangeTimeMillis() {
      return lastStateChangeTimeMillis;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("CompositeHealthSnapshot{state=")
          .append(state)
          .append(", compositeRate=")
          .append(String.format(java.util.Locale.ROOT, "%.2f%%", compositeSuccessRate * 100))
          .append(", activeSignals=")
          .append(activeSignalCount)
          .append(", samples=")
          .append(totalSampleCount)
          .append(", transitions=")
          .append(stateTransitionCount)
          .append(", signals=[");

      boolean first = true;
      for (SignalHealthTracker.SignalHealthSnapshot snapshot : signalSnapshots.values()) {
        if (!first) {
          sb.append(", ");
        }
        sb.append(snapshot.getSignalType().getName())
            .append("=")
            .append(String.format(java.util.Locale.ROOT, "%.2f%%", snapshot.getSuccessRate() * 100));
        first = false;
      }
      sb.append("]}");
      return sb.toString();
    }
  }
}
