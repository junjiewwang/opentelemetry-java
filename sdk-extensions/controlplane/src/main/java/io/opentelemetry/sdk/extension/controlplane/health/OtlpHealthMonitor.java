/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.health;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * OTLP 健康状态监控器
 *
 * <p>通过滑动窗口统计导出成功率，判断 OTLP 连接的健康状态。 控制平面的连接行为将与 OTLP 健康状态联动。
 */
public final class OtlpHealthMonitor {

  private static final Logger logger = Logger.getLogger(OtlpHealthMonitor.class.getName());

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

  private final int windowSize;
  private final double healthyThreshold;
  private final double unhealthyThreshold;
  private final CircularBuffer resultBuffer;
  private final AtomicReference<HealthState> currentState;
  private final AtomicLong successCount;
  private final AtomicLong failureCount;
  private final AtomicLong lastSuccessTimeNano;
  private final AtomicLong lastFailureTimeNano;
  private final List<HealthStateListener> listeners;

  /**
   * 创建 OTLP 健康监控器
   *
   * @param windowSize 滑动窗口大小
   * @param healthyThreshold 健康阈值 (成功率高于此值为健康)
   * @param unhealthyThreshold 不健康阈值 (成功率低于此值为不健康)
   */
  public OtlpHealthMonitor(int windowSize, double healthyThreshold, double unhealthyThreshold) {
    if (windowSize <= 0) {
      throw new IllegalArgumentException("windowSize must be positive");
    }
    if (healthyThreshold <= unhealthyThreshold) {
      throw new IllegalArgumentException(
          "healthyThreshold must be greater than unhealthyThreshold");
    }

    this.windowSize = windowSize;
    this.healthyThreshold = healthyThreshold;
    this.unhealthyThreshold = unhealthyThreshold;
    this.resultBuffer = new CircularBuffer(windowSize);
    this.currentState = new AtomicReference<>(HealthState.UNKNOWN);
    this.successCount = new AtomicLong(0);
    this.failureCount = new AtomicLong(0);
    this.lastSuccessTimeNano = new AtomicLong(0);
    this.lastFailureTimeNano = new AtomicLong(0);
    this.listeners = new CopyOnWriteArrayList<>();
  }

  /** 记录导出成功 */
  public void recordSuccess() {
    resultBuffer.add(true);
    successCount.incrementAndGet();
    lastSuccessTimeNano.set(System.nanoTime());
    updateState();
  }

  /**
   * 记录导出失败
   *
   * @param errorMessage 错误信息
   */
  public void recordFailure(String errorMessage) {
    resultBuffer.add(false);
    failureCount.incrementAndGet();
    lastFailureTimeNano.set(System.nanoTime());
    logger.log(Level.WARNING, "OTLP export failed: {0}", errorMessage);
    updateState();
  }

  /**
   * 获取当前健康状态
   *
   * @return 健康状态
   */
  public HealthState getState() {
    HealthState state = currentState.get();
    return state != null ? state : HealthState.UNKNOWN;
  }

  /**
   * 检查是否健康
   *
   * @return 是否健康
   */
  public boolean isHealthy() {
    HealthState state = currentState.get();
    return state == HealthState.HEALTHY || state == HealthState.UNKNOWN;
  }

  /**
   * 获取当前成功率
   *
   * @return 成功率 (0.0 ~ 1.0)
   */
  public double getSuccessRate() {
    return resultBuffer.getSuccessRate();
  }

  /**
   * 获取总成功次数
   *
   * @return 成功次数
   */
  public long getSuccessCount() {
    return successCount.get();
  }

  /**
   * 获取总失败次数
   *
   * @return 失败次数
   */
  public long getFailureCount() {
    return failureCount.get();
  }

  /**
   * 获取最后一次成功时间 (纳秒)
   *
   * @return 最后成功时间
   */
  public long getLastSuccessTimeNano() {
    return lastSuccessTimeNano.get();
  }

  /**
   * 获取最后一次失败时间 (纳秒)
   *
   * @return 最后失败时间
   */
  public long getLastFailureTimeNano() {
    return lastFailureTimeNano.get();
  }

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

  private void updateState() {
    double successRate = resultBuffer.getSuccessRate();
    int sampleCount = resultBuffer.getCount();

    // 样本数不足时保持未知状态
    if (sampleCount < windowSize / 2) {
      return;
    }

    HealthState previousState = currentState.get();
    HealthState newState;

    if (successRate >= healthyThreshold) {
      newState = HealthState.HEALTHY;
    } else if (successRate <= unhealthyThreshold) {
      newState = HealthState.UNHEALTHY;
    } else {
      newState = HealthState.DEGRADED;
    }

    if (currentState.compareAndSet(previousState, newState) && previousState != newState) {
      logger.log(
          Level.INFO,
          "OTLP health state changed: {0} -> {1} (successRate={2})",
          new Object[] {previousState, newState, successRate});
      if (previousState != null) {
        notifyListeners(previousState, newState);
      }
    }
  }

  private void notifyListeners(HealthState previousState, HealthState newState) {
    for (HealthStateListener listener : listeners) {
      try {
        listener.onStateChanged(previousState, newState);
      } catch (RuntimeException e) {
        logger.log(Level.WARNING, "Health state listener failed", e);
      }
    }
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

  /** 环形缓冲区 (用于滑动窗口) */
  private static final class CircularBuffer {
    private final boolean[] buffer;
    private int head;
    private int count;
    private int successCount;
    private final Object lock = new Object();

    CircularBuffer(int size) {
      this.buffer = new boolean[size];
      this.head = 0;
      this.count = 0;
      this.successCount = 0;
    }

    void add(boolean success) {
      synchronized (lock) {
        if (count == buffer.length) {
          // 缓冲区已满，移除最旧的元素
          if (buffer[head]) {
            successCount--;
          }
        } else {
          count++;
        }

        buffer[head] = success;
        if (success) {
          successCount++;
        }

        head = (head + 1) % buffer.length;
      }
    }

    double getSuccessRate() {
      synchronized (lock) {
        if (count == 0) {
          return 1.0; // 无数据时默认健康
        }
        return (double) successCount / count;
      }
    }

    int getCount() {
      synchronized (lock) {
        return count;
      }
    }
  }
}
