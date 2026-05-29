/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.benchmark;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * 心跳滑动窗口基准测试
 *
 * <p>量化 HeartbeatReporter 中滑动窗口操作的开销：
 *
 * <ul>
 *   <li>添加记录
 *   <li>健康判断（遍历窗口计算成功率）
 *   <li>窗口清理
 * </ul>
 *
 * <p>模拟不同心跳间隔下的窗口大小（1分钟窗口内的记录数）：
 *
 * <ul>
 *   <li>2 条: 30s 间隔
 *   <li>6 条: 10s 间隔
 *   <li>60 条: 1s 间隔（极端场景）
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
public class HeartbeatSlidingWindowBenchmark {

  private static final long SLIDING_WINDOW_MS = 60_000L;

  @Param({"2", "6", "60"})
  private int windowSize;

  private Deque<HeartbeatRecord> slidingWindow;

  private static final class HeartbeatRecord {
    final long timestamp;
    final boolean success;

    HeartbeatRecord(long timestamp, boolean success) {
      this.timestamp = timestamp;
      this.success = success;
    }
  }

  @Setup
  public void setup() {
    slidingWindow = new ConcurrentLinkedDeque<>();
    long now = System.currentTimeMillis();
    // 填充窗口：80% 成功
    for (int i = 0; i < windowSize; i++) {
      long ts = now - (SLIDING_WINDOW_MS * (windowSize - i) / windowSize);
      boolean success = i % 5 != 0; // 80% success rate
      slidingWindow.addLast(new HeartbeatRecord(ts, success));
    }
  }

  @Benchmark
  public boolean isHealthy() {
    long now = System.currentTimeMillis();
    int total = 0;
    int success = 0;

    for (HeartbeatRecord record : slidingWindow) {
      if (record.timestamp > now - SLIDING_WINDOW_MS) {
        total++;
        if (record.success) {
          success++;
        }
      }
    }

    if (total == 0) {
      return true;
    }
    double rate = (double) success / total;
    return rate >= 0.8 && success >= 2;
  }

  @Benchmark
  public double getSuccessRate() {
    long now = System.currentTimeMillis();
    int total = 0;
    int success = 0;

    for (HeartbeatRecord record : slidingWindow) {
      if (record.timestamp > now - SLIDING_WINDOW_MS) {
        total++;
        if (record.success) {
          success++;
        }
      }
    }

    if (total == 0) {
      return 1.0;
    }
    return (double) success / total;
  }

  @Benchmark
  public void addRecordAndCleanup() {
    long now = System.currentTimeMillis();
    slidingWindow.addLast(new HeartbeatRecord(now, /* success= */ true));

    // 清理过期记录
    long cutoff = now - SLIDING_WINDOW_MS;
    while (!slidingWindow.isEmpty()) {
      HeartbeatRecord first = slidingWindow.peekFirst();
      if (first != null && first.timestamp < cutoff) {
        slidingWindow.pollFirst();
      } else {
        break;
      }
    }
  }
}
