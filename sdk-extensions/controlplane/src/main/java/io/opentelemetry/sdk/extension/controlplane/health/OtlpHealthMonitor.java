/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.health;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * OTLP 健康状态监控器
 *
 * <p>通过时间窗口和多信号源统计导出成功率，判断 OTLP 连接的健康状态。
 * 控制平面的连接行为将与 OTLP 健康状态联动。
 *
 * <p>支持的信号类型：
 * <ul>
 *   <li>SPAN - Trace/Span 导出（权重 40%）</li>
 *   <li>METRIC - Metric 导出（权重 60%，因为更稳定）</li>
 * </ul>
 *
 * <p>状态判断机制：
 * <ul>
 *   <li>基于可配置的时间窗口（默认 60 秒）内的样本计算成功率</li>
 *   <li>多信号源加权平均计算综合成功率</li>
 *   <li>冷却时间机制防止状态频繁切换</li>
 *   <li>紧急状态检测（成功率为 0 时跳过冷却）</li>
 * </ul>
 *
 * <p>向后兼容：保留原有的 {@link #recordSuccess()} 和 {@link #recordFailure(String)} 方法，
 * 默认记录到 SPAN 信号。
 */
public final class OtlpHealthMonitor {

  private static final Logger logger = Logger.getLogger(OtlpHealthMonitor.class.getName());

  /** 默认时间窗口：60 秒 */
  private static final long DEFAULT_WINDOW_MILLIS = 60_000L;

  /** 默认冷却时间：5 秒 */
  private static final long DEFAULT_COOLDOWN_MILLIS = 5_000L;

  /** 默认最小样本数：5 */
  private static final int DEFAULT_MIN_SAMPLES = 5;

  /** 健康状态枚举 */
  public enum HealthState {
    /** 未知状态 (初始状态) */
    UNKNOWN,
    /** 健康状态 */
    HEALTHY,
    /** 降级状态 (部分失败) */
    DEGRADED,
    /** 不健康状态 */
    UNHEALTHY
  }

  // 核心组件
  private final Map<SignalType, SignalHealthTracker> trackers;
  private final CompositeHealthCalculator calculator;
  private final List<HealthStateListener> listeners;

  // 配置
  private final long windowMillis;
  private final double healthyThreshold;
  private final double unhealthyThreshold;
  private final long cooldownMillis;
  private final int minSamples;

  // 兼容性字段（保留原有 API 的统计）
  private final AtomicLong legacySuccessCount;
  private final AtomicLong legacyFailureCount;
  private final AtomicLong lastSuccessTimeNano;
  private final AtomicLong lastFailureTimeNano;

  /**
   * 创建 OTLP 健康监控器（使用窗口大小作为兼容参数）
   *
   * <p>这是向后兼容的构造函数，windowSize 参数会被转换为时间窗口。
   * 假设每秒约 1-2 次导出，windowSize=100 约等于 60 秒时间窗口。
   *
   * @param windowSize 滑动窗口大小（样本数，将被转换为时间窗口）
   * @param healthyThreshold 健康阈值 (成功率高于此值为健康)
   * @param unhealthyThreshold 不健康阈值 (成功率低于此值为不健康)
   */
  public OtlpHealthMonitor(int windowSize, double healthyThreshold, double unhealthyThreshold) {
    this(
        DEFAULT_WINDOW_MILLIS, // 使用默认时间窗口
        healthyThreshold,
        unhealthyThreshold,
        DEFAULT_COOLDOWN_MILLIS,
        Math.max(DEFAULT_MIN_SAMPLES, windowSize / 20)); // 从 windowSize 推导 minSamples
  }

  /**
   * 创建 OTLP 健康监控器
   *
   * @param windowMillis 时间窗口大小（毫秒）
   * @param healthyThreshold 健康阈值 (成功率高于此值为健康)
   * @param unhealthyThreshold 不健康阈值 (成功率低于此值为不健康)
   * @param cooldownMillis 冷却时间（毫秒）
   * @param minSamples 最小样本数
   */
  public OtlpHealthMonitor(
      long windowMillis,
      double healthyThreshold,
      double unhealthyThreshold,
      long cooldownMillis,
      int minSamples) {
    if (windowMillis <= 0) {
      throw new IllegalArgumentException("windowMillis must be positive");
    }
    if (healthyThreshold <= unhealthyThreshold) {
      throw new IllegalArgumentException(
          "healthyThreshold must be greater than unhealthyThreshold");
    }

    this.windowMillis = windowMillis;
    this.healthyThreshold = healthyThreshold;
    this.unhealthyThreshold = unhealthyThreshold;
    this.cooldownMillis = cooldownMillis;
    this.minSamples = minSamples;

    // 初始化信号跟踪器
    this.trackers = new EnumMap<>(SignalType.class);
    for (SignalType type : SignalType.values()) {
      trackers.put(
          type,
          new SignalHealthTracker(type, windowMillis, minSamples, type.getDefaultWeight()));
    }

    // 初始化综合健康计算器
    this.calculator =
        new CompositeHealthCalculator(trackers, healthyThreshold, unhealthyThreshold, cooldownMillis);

    // 初始化监听器列表
    this.listeners = new CopyOnWriteArrayList<>();

    // 兼容性字段
    this.legacySuccessCount = new AtomicLong(0);
    this.legacyFailureCount = new AtomicLong(0);
    this.lastSuccessTimeNano = new AtomicLong(0);
    this.lastFailureTimeNano = new AtomicLong(0);

    logger.log(
        Level.INFO,
        "OtlpHealthMonitor initialized: windowMillis={0}, healthyThreshold={1}, unhealthyThreshold={2}, cooldownMillis={3}, minSamples={4}",
        new Object[] {windowMillis, healthyThreshold, unhealthyThreshold, cooldownMillis, minSamples});
  }

  // ==================== recordSuccess 方法组（重载放在一起） ====================

  /**
   * 记录导出成功（默认记录到 SPAN 信号）
   *
   * <p>这是向后兼容的方法，等同于 {@code recordSuccess(SignalType.SPAN)}
   */
  public void recordSuccess() {
    recordSuccess(SignalType.SPAN);
  }

  /**
   * 记录指定信号类型的导出成功
   *
   * @param signalType 信号类型
   */
  public void recordSuccess(SignalType signalType) {
    SignalHealthTracker tracker = trackers.get(signalType);
    if (tracker != null) {
      tracker.recordSuccess();
    }

    // 更新兼容性统计
    if (signalType == SignalType.SPAN) {
      legacySuccessCount.incrementAndGet();
      lastSuccessTimeNano.set(System.nanoTime());
    }

    // 更新状态
    updateState();
  }

  // ==================== recordFailure 方法组（重载放在一起） ====================

  /**
   * 记录导出失败（默认记录到 SPAN 信号）
   *
   * <p>这是向后兼容的方法，等同于 {@code recordFailure(errorMessage, SignalType.SPAN)}
   *
   * @param errorMessage 错误信息
   */
  public void recordFailure(String errorMessage) {
    recordFailure(errorMessage, SignalType.SPAN);
  }

  /**
   * 记录指定信号类型的导出失败
   *
   * @param errorMessage 错误信息
   * @param signalType 信号类型
   */
  public void recordFailure(String errorMessage, SignalType signalType) {
    SignalHealthTracker tracker = trackers.get(signalType);
    if (tracker != null) {
      tracker.recordFailure(errorMessage);
    }

    // 更新兼容性统计
    if (signalType == SignalType.SPAN) {
      legacyFailureCount.incrementAndGet();
      lastFailureTimeNano.set(System.nanoTime());
    }

    // 更新状态
    updateState();
  }

  // ==================== 状态查询 API ====================

  /**
   * 获取当前健康状态
   *
   * @return 健康状态
   */
  public HealthState getState() {
    return calculator.getCurrentState();
  }

  /**
   * 检查是否健康
   *
   * @return 是否健康
   */
  public boolean isHealthy() {
    HealthState state = getState();
    return state == HealthState.HEALTHY || state == HealthState.UNKNOWN;
  }

  /**
   * 获取综合成功率
   *
   * @return 综合成功率 (0.0 ~ 1.0)
   */
  public double getSuccessRate() {
    return calculator.calculateCompositeSuccessRate();
  }

  /**
   * 获取指定信号类型的成功率
   *
   * @param signalType 信号类型
   * @return 成功率 (0.0 ~ 1.0)
   */
  public double getSuccessRate(SignalType signalType) {
    SignalHealthTracker tracker = trackers.get(signalType);
    return tracker != null ? tracker.getSuccessRate() : 1.0;
  }

  // ==================== getSuccessCount 方法组（重载放在一起） ====================

  /**
   * 获取总成功次数（仅 SPAN 信号，向后兼容）
   *
   * @return 成功次数
   */
  public long getSuccessCount() {
    return legacySuccessCount.get();
  }

  /**
   * 获取指定信号类型的总成功次数
   *
   * @param signalType 信号类型
   * @return 成功次数
   */
  public long getSuccessCount(SignalType signalType) {
    SignalHealthTracker tracker = trackers.get(signalType);
    return tracker != null ? tracker.getTotalSuccessCount() : 0;
  }

  // ==================== getFailureCount 方法组（重载放在一起） ====================

  /**
   * 获取总失败次数（仅 SPAN 信号，向后兼容）
   *
   * @return 失败次数
   */
  public long getFailureCount() {
    return legacyFailureCount.get();
  }

  /**
   * 获取指定信号类型的总失败次数
   *
   * @param signalType 信号类型
   * @return 失败次数
   */
  public long getFailureCount(SignalType signalType) {
    SignalHealthTracker tracker = trackers.get(signalType);
    return tracker != null ? tracker.getTotalFailureCount() : 0;
  }

  // ==================== 其他向后兼容的统计 API ====================

  /**
   * 获取最后一次成功时间 (纳秒)（仅 SPAN 信号，向后兼容）
   *
   * @return 最后成功时间
   */
  public long getLastSuccessTimeNano() {
    return lastSuccessTimeNano.get();
  }

  /**
   * 获取最后一次失败时间 (纳秒)（仅 SPAN 信号，向后兼容）
   *
   * @return 最后失败时间
   */
  public long getLastFailureTimeNano() {
    return lastFailureTimeNano.get();
  }

  // ==================== 多信号源统计 API ====================

  /**
   * 获取指定信号类型的健康跟踪器
   *
   * @param signalType 信号类型
   * @return 健康跟踪器，如果不存在返回 null
   */
  @Nullable
  public SignalHealthTracker getTracker(SignalType signalType) {
    return trackers.get(signalType);
  }

  /**
   * 获取指定信号类型的窗口内样本数
   *
   * @param signalType 信号类型
   * @return 样本数
   */
  public int getSampleCount(SignalType signalType) {
    SignalHealthTracker tracker = trackers.get(signalType);
    return tracker != null ? tracker.getSampleCount() : 0;
  }

  /**
   * 获取活跃信号数量
   *
   * @return 活跃信号数量
   */
  public int getActiveSignalCount() {
    return calculator.getActiveSignalCount();
  }

  /**
   * 获取状态转换次数
   *
   * @return 状态转换次数
   */
  public long getStateTransitionCount() {
    return calculator.getStateTransitionCount();
  }

  // ==================== 配置查询 API ====================

  /**
   * 获取时间窗口大小（毫秒）
   *
   * @return 时间窗口大小
   */
  public long getWindowMillis() {
    return windowMillis;
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
   * 获取最小样本数
   *
   * @return 最小样本数
   */
  public int getMinSamples() {
    return minSamples;
  }

  // ==================== 监听器 API ====================

  /**
   * 添加状态变化监听器
   *
   * @param listener 监听器
   */
  public void addListener(HealthStateListener listener) {
    listeners.add(listener);
  }

  /**
   * 移除状态变化监听器
   *
   * @param listener 监听器
   */
  public void removeListener(HealthStateListener listener) {
    listeners.remove(listener);
  }

  // ==================== 诊断 API ====================

  /**
   * 创建综合健康快照
   *
   * @return 综合健康快照
   */
  public CompositeHealthCalculator.CompositeHealthSnapshot createSnapshot() {
    return calculator.createSnapshot();
  }

  /**
   * 将 System.nanoTime() 转换为 Unix 毫秒时间戳
   *
   * @param nanoTime 纳秒时间
   * @return Unix 毫秒时间戳
   */
  public static long nanoTimeToUnixMs(long nanoTime) {
    if (nanoTime == 0) {
      return 0;
    }
    long nowNano = System.nanoTime();
    long nowMs = System.currentTimeMillis();
    long diffNano = nowNano - nanoTime;
    long diffMs = TimeUnit.NANOSECONDS.toMillis(diffNano);
    return nowMs - diffMs;
  }

  // ==================== 内部方法 ====================

  /**
   * 更新健康状态
   */
  private void updateState() {
    HealthState previousState = calculator.getCurrentState();
    HealthState newState = calculator.calculateAndUpdate();

    // 如果状态发生变化，通知监听器
    if (previousState != newState) {
      notifyListeners(previousState, newState);
    }
  }

  /**
   * 通知监听器状态变化
   */
  private void notifyListeners(HealthState previousState, HealthState newState) {
    for (HealthStateListener listener : listeners) {
      try {
        listener.onStateChanged(previousState, newState);
      } catch (RuntimeException e) {
        logger.log(Level.WARNING, "Health state listener failed", e);
      }
    }
  }

  @Override
  public String toString() {
    return String.format(
        java.util.Locale.ROOT,
        "OtlpHealthMonitor{state=%s, compositeRate=%.2f%%, activeSignals=%d, window=%dms}",
        getState(), getSuccessRate() * 100, getActiveSignalCount(), windowMillis);
  }

  /** 健康状态变化监听器 */
  @FunctionalInterface
  public interface HealthStateListener {
    /**
     * 状态变化回调
     *
     * @param previousState 之前的状态
     * @param newState 新状态
     */
    void onStateChanged(HealthState previousState, HealthState newState);
  }

  // ==================== Builder ====================

  /**
   * 创建 Builder
   *
   * @return Builder 实例
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * OtlpHealthMonitor 构建器
   */
  public static final class Builder {
    private long windowMillis = DEFAULT_WINDOW_MILLIS;
    private double healthyThreshold = 0.9;
    private double unhealthyThreshold = 0.5;
    private long cooldownMillis = DEFAULT_COOLDOWN_MILLIS;
    private int minSamples = DEFAULT_MIN_SAMPLES;

    private Builder() {}

    /**
     * 设置时间窗口大小
     *
     * @param windowMillis 时间窗口大小（毫秒）
     * @return this
     */
    public Builder windowMillis(long windowMillis) {
      this.windowMillis = windowMillis;
      return this;
    }

    /**
     * 设置健康阈值
     *
     * @param healthyThreshold 健康阈值
     * @return this
     */
    public Builder healthyThreshold(double healthyThreshold) {
      this.healthyThreshold = healthyThreshold;
      return this;
    }

    /**
     * 设置不健康阈值
     *
     * @param unhealthyThreshold 不健康阈值
     * @return this
     */
    public Builder unhealthyThreshold(double unhealthyThreshold) {
      this.unhealthyThreshold = unhealthyThreshold;
      return this;
    }

    /**
     * 设置冷却时间
     *
     * @param cooldownMillis 冷却时间（毫秒）
     * @return this
     */
    public Builder cooldownMillis(long cooldownMillis) {
      this.cooldownMillis = cooldownMillis;
      return this;
    }

    /**
     * 设置最小样本数
     *
     * @param minSamples 最小样本数
     * @return this
     */
    public Builder minSamples(int minSamples) {
      this.minSamples = minSamples;
      return this;
    }

    /**
     * 构建 OtlpHealthMonitor
     *
     * @return OtlpHealthMonitor 实例
     */
    public OtlpHealthMonitor build() {
      return new OtlpHealthMonitor(
          windowMillis, healthyThreshold, unhealthyThreshold, cooldownMillis, minSamples);
    }
  }
}
