/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 指数退避计算器
 *
 * <p>用于在长轮询失败时计算重试间隔，采用指数退避策略避免服务端过载。
 */
public final class ExponentialBackoff {

  private final long minIntervalMs;
  private final long maxIntervalMs;
  private final double multiplier;
  private final AtomicLong currentIntervalMs;

  /**
   * 创建指数退避计算器
   *
   * @param config 长轮询配置
   */
  public ExponentialBackoff(LongPollConfig config) {
    this.minIntervalMs = config.getMinRetryInterval().toMillis();
    this.maxIntervalMs = config.getMaxRetryInterval().toMillis();
    this.multiplier = config.getBackoffMultiplier();
    this.currentIntervalMs = new AtomicLong(minIntervalMs);
  }

  /**
   * 创建指数退避计算器
   *
   * @param minIntervalMs 最小间隔（毫秒）
   * @param maxIntervalMs 最大间隔（毫秒）
   * @param multiplier 乘数
   */
  public ExponentialBackoff(long minIntervalMs, long maxIntervalMs, double multiplier) {
    this.minIntervalMs = minIntervalMs;
    this.maxIntervalMs = maxIntervalMs;
    this.multiplier = multiplier;
    this.currentIntervalMs = new AtomicLong(minIntervalMs);
  }

  /**
   * 获取下一个退避间隔并更新内部状态
   *
   * @return 退避间隔（毫秒）
   */
  public long nextBackoff() {
    long current = currentIntervalMs.get();
    long next = Math.min((long) (current * multiplier), maxIntervalMs);
    currentIntervalMs.set(next);
    return current;
  }

  /**
   * 获取当前退避间隔（不更新状态）
   *
   * @return 当前间隔（毫秒）
   */
  public long getCurrentInterval() {
    return currentIntervalMs.get();
  }

  /**
   * 重置退避间隔为最小值
   *
   * <p>在成功请求后调用
   */
  public void reset() {
    currentIntervalMs.set(minIntervalMs);
  }

  /**
   * 获取最小间隔
   *
   * @return 最小间隔（毫秒）
   */
  public long getMinInterval() {
    return minIntervalMs;
  }

  /**
   * 获取最大间隔
   *
   * @return 最大间隔（毫秒）
   */
  public long getMaxInterval() {
    return maxIntervalMs;
  }

  @Override
  public String toString() {
    return "ExponentialBackoff{"
        + "minIntervalMs="
        + minIntervalMs
        + ", maxIntervalMs="
        + maxIntervalMs
        + ", multiplier="
        + multiplier
        + ", currentIntervalMs="
        + currentIntervalMs.get()
        + '}';
  }
}
