/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.health;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 基于时间窗口的样本缓冲区
 *
 * <p>存储固定时间窗口内的导出结果样本，自动过期旧样本。
 * 相比固定数量的环形缓冲区，时间窗口缓冲区能更准确地反映实时健康状态，
 * 不受导出频率变化的影响。
 *
 * <p>线程安全：使用 {@link ConcurrentLinkedDeque} 和原子快照实现无锁读取。
 */
public final class TimeWindowBuffer {

  /** 带时间戳的样本 */
  private static final class TimestampedSample {
    final long timestampMillis;
    final boolean success;

    TimestampedSample(long timestampMillis, boolean success) {
      this.timestampMillis = timestampMillis;
      this.success = success;
    }
  }

  /** 快照：用于无锁读取 */
  private static final class Snapshot {
    final int successCount;
    final int totalCount;
    final long computedAtMillis;

    Snapshot(int successCount, int totalCount, long computedAtMillis) {
      this.successCount = successCount;
      this.totalCount = totalCount;
      this.computedAtMillis = computedAtMillis;
    }
  }

  /** 默认时间窗口：60 秒 */
  private static final long DEFAULT_WINDOW_MILLIS = 60_000L;

  /** 快照有效期：500ms（避免频繁重算） */
  private static final long SNAPSHOT_TTL_MILLIS = 500L;

  /** 最大样本数限制（防止内存泄漏） */
  private static final int MAX_SAMPLES = 10_000;

  private final long windowMillis;
  private final ConcurrentLinkedDeque<TimestampedSample> samples;
  private final AtomicReference<Snapshot> cachedSnapshot;
  private final AtomicLong totalSuccessCount;
  private final AtomicLong totalFailureCount;
  private final AtomicLong lastSuccessTimeMillis;
  private final AtomicLong lastFailureTimeMillis;
  private final AtomicInteger droppedSampleCount;

  /**
   * 创建时间窗口缓冲区（使用默认窗口大小：60 秒）
   */
  public TimeWindowBuffer() {
    this(DEFAULT_WINDOW_MILLIS);
  }

  /**
   * 创建时间窗口缓冲区
   *
   * @param windowMillis 时间窗口大小（毫秒）
   */
  public TimeWindowBuffer(long windowMillis) {
    if (windowMillis <= 0) {
      throw new IllegalArgumentException("windowMillis must be positive");
    }
    this.windowMillis = windowMillis;
    this.samples = new ConcurrentLinkedDeque<>();
    this.cachedSnapshot = new AtomicReference<>(new Snapshot(0, 0, 0));
    this.totalSuccessCount = new AtomicLong(0);
    this.totalFailureCount = new AtomicLong(0);
    this.lastSuccessTimeMillis = new AtomicLong(0);
    this.lastFailureTimeMillis = new AtomicLong(0);
    this.droppedSampleCount = new AtomicInteger(0);
  }

  /**
   * 添加成功样本
   */
  public void recordSuccess() {
    long now = System.currentTimeMillis();
    addSample(new TimestampedSample(now, /* success= */ true));
    totalSuccessCount.incrementAndGet();
    lastSuccessTimeMillis.set(now);
  }

  /**
   * 添加失败样本
   */
  public void recordFailure() {
    long now = System.currentTimeMillis();
    addSample(new TimestampedSample(now, /* success= */ false));
    totalFailureCount.incrementAndGet();
    lastFailureTimeMillis.set(now);
  }

  /**
   * 添加样本（内部方法）
   */
  private void addSample(TimestampedSample sample) {
    // 先清理过期样本
    cleanupExpiredSamples();

    // 检查样本数限制
    if (samples.size() >= MAX_SAMPLES) {
      samples.pollFirst();
      droppedSampleCount.incrementAndGet();
    }

    samples.addLast(sample);
    // 使快照失效
    cachedSnapshot.set(new Snapshot(0, 0, 0));
  }

  /**
   * 清理过期样本
   */
  private void cleanupExpiredSamples() {
    long cutoffTime = System.currentTimeMillis() - windowMillis;
    TimestampedSample head;
    while ((head = samples.peekFirst()) != null && head.timestampMillis < cutoffTime) {
      samples.pollFirst();
    }
  }

  /**
   * 获取窗口内的成功率
   *
   * @return 成功率 (0.0 ~ 1.0)，无样本时返回 1.0（乐观默认）
   */
  public double getSuccessRate() {
    Snapshot snapshot = getOrComputeSnapshot();
    if (snapshot.totalCount == 0) {
      return 1.0; // 无数据时默认健康
    }
    return (double) snapshot.successCount / snapshot.totalCount;
  }

  /**
   * 获取窗口内的样本数量
   *
   * @return 样本数量
   */
  public int getSampleCount() {
    return getOrComputeSnapshot().totalCount;
  }

  /**
   * 获取窗口内的成功样本数量
   *
   * @return 成功样本数量
   */
  public int getSuccessCount() {
    return getOrComputeSnapshot().successCount;
  }

  /**
   * 获取窗口内的失败样本数量
   *
   * @return 失败样本数量
   */
  public int getFailureCount() {
    Snapshot snapshot = getOrComputeSnapshot();
    return snapshot.totalCount - snapshot.successCount;
  }

  /**
   * 获取总成功次数（历史累计）
   *
   * @return 总成功次数
   */
  public long getTotalSuccessCount() {
    return totalSuccessCount.get();
  }

  /**
   * 获取总失败次数（历史累计）
   *
   * @return 总失败次数
   */
  public long getTotalFailureCount() {
    return totalFailureCount.get();
  }

  /**
   * 获取最后一次成功时间（毫秒）
   *
   * @return 最后成功时间，0 表示从未成功
   */
  public long getLastSuccessTimeMillis() {
    return lastSuccessTimeMillis.get();
  }

  /**
   * 获取最后一次失败时间（毫秒）
   *
   * @return 最后失败时间，0 表示从未失败
   */
  public long getLastFailureTimeMillis() {
    return lastFailureTimeMillis.get();
  }

  /**
   * 获取因超限而丢弃的样本数
   *
   * @return 丢弃的样本数
   */
  public int getDroppedSampleCount() {
    return droppedSampleCount.get();
  }

  /**
   * 获取时间窗口大小（毫秒）
   *
   * @return 时间窗口大小
   */
  public long getWindowMillis() {
    return windowMillis;
  }

  /**
   * 检查是否有足够的样本用于健康判断
   *
   * @param minSamples 最小样本数
   * @return 是否有足够样本
   */
  public boolean hasEnoughSamples(int minSamples) {
    return getSampleCount() >= minSamples;
  }

  /**
   * 获取或计算快照
   */
  private Snapshot getOrComputeSnapshot() {
    long now = System.currentTimeMillis();
    Snapshot current = cachedSnapshot.get();

    // 如果快照仍有效，直接返回
    if (current != null
        && now - current.computedAtMillis < SNAPSHOT_TTL_MILLIS
        && current.totalCount > 0) {
      return current;
    }

    // 计算新快照
    cleanupExpiredSamples();

    int successCount = 0;
    int totalCount = 0;
    for (TimestampedSample sample : samples) {
      totalCount++;
      if (sample.success) {
        successCount++;
      }
    }

    Snapshot newSnapshot = new Snapshot(successCount, totalCount, now);
    cachedSnapshot.set(newSnapshot);
    return newSnapshot;
  }

  /**
   * 清空所有样本
   */
  public void clear() {
    samples.clear();
    cachedSnapshot.set(new Snapshot(0, 0, 0));
    // 不清空历史累计统计
  }

  @Override
  public String toString() {
    Snapshot snapshot = getOrComputeSnapshot();
    return String.format(
        java.util.Locale.ROOT,
        "TimeWindowBuffer{window=%dms, samples=%d, successRate=%.2f%%, totalSuccess=%d, totalFailure=%d}",
        windowMillis,
        snapshot.totalCount,
        getSuccessRate() * 100,
        totalSuccessCount.get(),
        totalFailureCount.get());
  }
}
