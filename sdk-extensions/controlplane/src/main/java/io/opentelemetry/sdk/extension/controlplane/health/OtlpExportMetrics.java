/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.health;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * OTLP 导出指标收集器
 *
 * <p>通过时间窗口和多信号源统计导出成功率，仅负责指标收集和上报，不参与健康判断。
 *
 * <p>支持的信号类型：
 * <ul>
 *   <li>SPAN - Trace/Span 导出（权重 40%）</li>
 *   <li>METRIC - Metric 导出（权重 60%，因为更稳定）</li>
 * </ul>
 *
 * <p>设计原则：
 * <ul>
 *   <li><b>单一职责</b>：仅负责指标收集，健康判断由 HeartbeatReporter 负责</li>
 *   <li><b>高内聚</b>：封装时间窗口统计逻辑</li>
 * </ul>
 */
public final class OtlpExportMetrics {

  private static final Logger logger = Logger.getLogger(OtlpExportMetrics.class.getName());

  /** 默认时间窗口：60 秒 */
  private static final long DEFAULT_WINDOW_MILLIS = 60_000L;

  /** 默认最小样本数：5 */
  private static final int DEFAULT_MIN_SAMPLES = 5;

  // 核心组件
  private final Map<SignalType, SignalHealthTracker> trackers;

  // 配置
  private final long windowMillis;
  private final int minSamples;

  // 兼容性字段（保留原有 API 的统计）
  private final AtomicLong legacySuccessCount;
  private final AtomicLong legacyFailureCount;
  private final AtomicLong lastSuccessTimeNano;
  private final AtomicLong lastFailureTimeNano;

  /**
   * 创建 OTLP 导出指标收集器（使用默认配置）
   */
  public OtlpExportMetrics() {
    this(DEFAULT_WINDOW_MILLIS, DEFAULT_MIN_SAMPLES);
  }

  /**
   * 创建 OTLP 导出指标收集器
   *
   * @param windowMillis 时间窗口大小（毫秒）
   * @param minSamples 最小样本数
   */
  public OtlpExportMetrics(long windowMillis, int minSamples) {
    if (windowMillis <= 0) {
      throw new IllegalArgumentException("windowMillis must be positive");
    }

    this.windowMillis = windowMillis;
    this.minSamples = minSamples;

    // 初始化信号跟踪器
    this.trackers = new EnumMap<>(SignalType.class);
    for (SignalType type : SignalType.values()) {
      trackers.put(
          type,
          new SignalHealthTracker(type, windowMillis, minSamples, type.getDefaultWeight()));
    }

    // 兼容性字段
    this.legacySuccessCount = new AtomicLong(0);
    this.legacyFailureCount = new AtomicLong(0);
    this.lastSuccessTimeNano = new AtomicLong(0);
    this.lastFailureTimeNano = new AtomicLong(0);

    logger.log(
        Level.INFO,
        "OtlpExportMetrics initialized: windowMillis={0}, minSamples={1}",
        new Object[] {windowMillis, minSamples});
  }

  // ==================== recordSuccess 方法组 ====================

  /**
   * 记录导出成功（默认记录到 SPAN 信号）
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
  }

  // ==================== recordFailure 方法组 ====================

  /**
   * 记录导出失败（默认记录到 SPAN 信号）
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
  }

  // ==================== 指标查询 API ====================

  /**
   * 获取综合成功率（加权平均）
   *
   * @return 综合成功率 (0.0 ~ 1.0)
   */
  public double getSuccessRate() {
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

    return weightedSum / totalWeight;
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

  // ==================== getSuccessCount 方法组 ====================

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

  // ==================== getFailureCount 方法组 ====================

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

  // ==================== 时间戳 API ====================

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
    int count = 0;
    for (SignalHealthTracker tracker : trackers.values()) {
      if (tracker.isActive() && tracker.hasEnoughSamples()) {
        count++;
      }
    }
    return count;
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
   * 获取最小样本数
   *
   * @return 最小样本数
   */
  public int getMinSamples() {
    return minSamples;
  }

  // ==================== 快照 API ====================

  /**
   * 创建导出指标快照
   *
   * @return 导出指标快照
   */
  public ExportMetricsSnapshot createSnapshot() {
    EnumMap<SignalType, SignalHealthTracker.SignalHealthSnapshot> signalSnapshots =
        new EnumMap<>(SignalType.class);
    for (Map.Entry<SignalType, SignalHealthTracker> entry : trackers.entrySet()) {
      signalSnapshots.put(entry.getKey(), entry.getValue().createSnapshot());
    }

    return new ExportMetricsSnapshot(
        getSuccessRate(),
        getActiveSignalCount(),
        signalSnapshots);
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

  @Override
  public String toString() {
    return String.format(
        java.util.Locale.ROOT,
        "OtlpExportMetrics{compositeRate=%.2f%%, activeSignals=%d, window=%dms}",
        getSuccessRate() * 100, getActiveSignalCount(), windowMillis);
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
   * OtlpExportMetrics 构建器
   */
  public static final class Builder {
    private long windowMillis = DEFAULT_WINDOW_MILLIS;
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
     * 构建 OtlpExportMetrics
     *
     * @return OtlpExportMetrics 实例
     */
    public OtlpExportMetrics build() {
      return new OtlpExportMetrics(windowMillis, minSamples);
    }
  }

  // ==================== 快照类 ====================

  /**
   * 导出指标快照
   *
   * <p>用于诊断和状态上报的不可变快照。
   */
  public static final class ExportMetricsSnapshot {
    private final double compositeSuccessRate;
    private final int activeSignalCount;
    private final Map<SignalType, SignalHealthTracker.SignalHealthSnapshot> signalSnapshots;

    ExportMetricsSnapshot(
        double compositeSuccessRate,
        int activeSignalCount,
        Map<SignalType, SignalHealthTracker.SignalHealthSnapshot> signalSnapshots) {
      this.compositeSuccessRate = compositeSuccessRate;
      this.activeSignalCount = activeSignalCount;
      this.signalSnapshots = new EnumMap<>(signalSnapshots);
    }

    public double getCompositeSuccessRate() {
      return compositeSuccessRate;
    }

    public int getActiveSignalCount() {
      return activeSignalCount;
    }

    public Map<SignalType, SignalHealthTracker.SignalHealthSnapshot> getSignalSnapshots() {
      return signalSnapshots;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("ExportMetricsSnapshot{compositeRate=")
          .append(String.format(java.util.Locale.ROOT, "%.2f%%", compositeSuccessRate * 100))
          .append(", activeSignals=")
          .append(activeSignalCount)
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
