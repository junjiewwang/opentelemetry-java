/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.health;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 单信号健康跟踪器
 *
 * <p>负责跟踪单个信号类型（如 Span 或 Metric）的导出健康状态。
 * 使用 {@link TimeWindowBuffer} 存储时间窗口内的样本。
 *
 * <p>职责单一：仅负责单信号的数据收集和成功率计算，
 * 不涉及综合健康判断（由 {@link CompositeHealthCalculator} 负责）。
 */
public final class SignalHealthTracker {

  private static final Logger logger = Logger.getLogger(SignalHealthTracker.class.getName());

  /** 默认最小样本数：5 */
  private static final int DEFAULT_MIN_SAMPLES = 5;

  private final SignalType signalType;
  private final TimeWindowBuffer buffer;
  private final int minSamples;
  private final double weight;
  private final AtomicReference<String> lastError;
  private final AtomicLong lastActivityTimeMillis;

  /**
   * 创建单信号健康跟踪器（使用默认配置）
   *
   * @param signalType 信号类型
   */
  public SignalHealthTracker(SignalType signalType) {
    this(signalType, new TimeWindowBuffer(), DEFAULT_MIN_SAMPLES, signalType.getDefaultWeight());
  }

  /**
   * 创建单信号健康跟踪器
   *
   * @param signalType 信号类型
   * @param windowMillis 时间窗口大小（毫秒）
   */
  public SignalHealthTracker(SignalType signalType, long windowMillis) {
    this(
        signalType,
        new TimeWindowBuffer(windowMillis),
        DEFAULT_MIN_SAMPLES,
        signalType.getDefaultWeight());
  }

  /**
   * 创建单信号健康跟踪器
   *
   * @param signalType 信号类型
   * @param windowMillis 时间窗口大小（毫秒）
   * @param minSamples 最小样本数
   * @param weight 权重
   */
  public SignalHealthTracker(
      SignalType signalType, long windowMillis, int minSamples, double weight) {
    this(signalType, new TimeWindowBuffer(windowMillis), minSamples, weight);
  }

  /**
   * 创建单信号健康跟踪器（内部构造）
   */
  private SignalHealthTracker(
      SignalType signalType, TimeWindowBuffer buffer, int minSamples, double weight) {
    this.signalType = signalType;
    this.buffer = buffer;
    this.minSamples = minSamples;
    this.weight = weight;
    this.lastError = new AtomicReference<>();
    this.lastActivityTimeMillis = new AtomicLong(0);
  }

  /**
   * 记录导出成功
   */
  public void recordSuccess() {
    buffer.recordSuccess();
    lastActivityTimeMillis.set(System.currentTimeMillis());
    logger.log(
        Level.FINE,
        "{0} export success recorded, window successRate={1}",
        new Object[] {signalType.getName(), buffer.getSuccessRate()});
  }

  /**
   * 记录导出失败
   *
   * @param errorMessage 错误信息
   */
  public void recordFailure(@Nullable String errorMessage) {
    buffer.recordFailure();
    lastActivityTimeMillis.set(System.currentTimeMillis());
    if (errorMessage != null) {
      lastError.set(errorMessage);
    }
    logger.log(
        Level.WARNING,
        "{0} export failed: {1}, window successRate={2}",
        new Object[] {signalType.getName(), errorMessage, buffer.getSuccessRate()});
  }

  /**
   * 获取信号类型
   *
   * @return 信号类型
   */
  public SignalType getSignalType() {
    return signalType;
  }

  /**
   * 获取窗口内的成功率
   *
   * @return 成功率 (0.0 ~ 1.0)
   */
  public double getSuccessRate() {
    return buffer.getSuccessRate();
  }

  /**
   * 获取权重
   *
   * @return 权重 (0.0 ~ 1.0)
   */
  public double getWeight() {
    return weight;
  }

  /**
   * 检查是否有足够的样本进行健康判断
   *
   * @return 是否有足够样本
   */
  public boolean hasEnoughSamples() {
    return buffer.hasEnoughSamples(minSamples);
  }

  /**
   * 检查此信号是否活跃（在时间窗口内有样本）
   *
   * @return 是否活跃
   */
  public boolean isActive() {
    return buffer.getSampleCount() > 0;
  }

  /**
   * 获取窗口内样本数
   *
   * @return 样本数
   */
  public int getSampleCount() {
    return buffer.getSampleCount();
  }

  /**
   * 获取总成功次数（历史累计）
   *
   * @return 总成功次数
   */
  public long getTotalSuccessCount() {
    return buffer.getTotalSuccessCount();
  }

  /**
   * 获取总失败次数（历史累计）
   *
   * @return 总失败次数
   */
  public long getTotalFailureCount() {
    return buffer.getTotalFailureCount();
  }

  /**
   * 获取最后一次成功时间（毫秒）
   *
   * @return 最后成功时间
   */
  public long getLastSuccessTimeMillis() {
    return buffer.getLastSuccessTimeMillis();
  }

  /**
   * 获取最后一次失败时间（毫秒）
   *
   * @return 最后失败时间
   */
  public long getLastFailureTimeMillis() {
    return buffer.getLastFailureTimeMillis();
  }

  /**
   * 获取最后一次活动时间（毫秒）
   *
   * @return 最后活动时间
   */
  public long getLastActivityTimeMillis() {
    return lastActivityTimeMillis.get();
  }

  /**
   * 获取最后一次错误信息
   *
   * @return 错误信息，无错误时返回 null
   */
  @Nullable
  public String getLastError() {
    return lastError.get();
  }

  /**
   * 获取底层时间窗口缓冲区（用于高级诊断）
   *
   * @return 时间窗口缓冲区
   */
  public TimeWindowBuffer getBuffer() {
    return buffer;
  }

  /**
   * 创建健康快照
   *
   * @return 健康快照
   */
  public SignalHealthSnapshot createSnapshot() {
    return new SignalHealthSnapshot(
        signalType,
        buffer.getSuccessRate(),
        buffer.getSampleCount(),
        buffer.getTotalSuccessCount(),
        buffer.getTotalFailureCount(),
        buffer.getLastSuccessTimeMillis(),
        buffer.getLastFailureTimeMillis(),
        lastError.get(),
        weight,
        hasEnoughSamples());
  }

  @Override
  public String toString() {
    return String.format(
        java.util.Locale.ROOT,
        "SignalHealthTracker{type=%s, successRate=%.2f%%, samples=%d, weight=%.2f}",
        signalType.getName(), getSuccessRate() * 100, getSampleCount(), weight);
  }

  /**
   * 单信号健康快照
   *
   * <p>用于诊断和状态上报的不可变快照。
   */
  public static final class SignalHealthSnapshot {
    private final SignalType signalType;
    private final double successRate;
    private final int sampleCount;
    private final long totalSuccessCount;
    private final long totalFailureCount;
    private final long lastSuccessTimeMillis;
    private final long lastFailureTimeMillis;
    @Nullable private final String lastError;
    private final double weight;
    private final boolean hasEnoughSamples;

    SignalHealthSnapshot(
        SignalType signalType,
        double successRate,
        int sampleCount,
        long totalSuccessCount,
        long totalFailureCount,
        long lastSuccessTimeMillis,
        long lastFailureTimeMillis,
        @Nullable String lastError,
        double weight,
        boolean hasEnoughSamples) {
      this.signalType = signalType;
      this.successRate = successRate;
      this.sampleCount = sampleCount;
      this.totalSuccessCount = totalSuccessCount;
      this.totalFailureCount = totalFailureCount;
      this.lastSuccessTimeMillis = lastSuccessTimeMillis;
      this.lastFailureTimeMillis = lastFailureTimeMillis;
      this.lastError = lastError;
      this.weight = weight;
      this.hasEnoughSamples = hasEnoughSamples;
    }

    public SignalType getSignalType() {
      return signalType;
    }

    public double getSuccessRate() {
      return successRate;
    }

    public int getSampleCount() {
      return sampleCount;
    }

    public long getTotalSuccessCount() {
      return totalSuccessCount;
    }

    public long getTotalFailureCount() {
      return totalFailureCount;
    }

    public long getLastSuccessTimeMillis() {
      return lastSuccessTimeMillis;
    }

    public long getLastFailureTimeMillis() {
      return lastFailureTimeMillis;
    }

    @Nullable
    public String getLastError() {
      return lastError;
    }

    public double getWeight() {
      return weight;
    }

    public boolean hasEnoughSamples() {
      return hasEnoughSamples;
    }

    @Override
    public String toString() {
      return String.format(
          java.util.Locale.ROOT,
          "SignalHealthSnapshot{type=%s, successRate=%.2f%%, samples=%d, weight=%.2f, enough=%s}",
          signalType.getName(), successRate * 100, sampleCount, weight, hasEnoughSamples);
    }
  }
}
