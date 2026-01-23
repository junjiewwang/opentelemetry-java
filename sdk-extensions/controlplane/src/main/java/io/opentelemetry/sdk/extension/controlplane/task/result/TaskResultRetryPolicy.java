/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.result;

import java.time.Duration;

/**
 * 任务结果上传重试策略接口
 *
 * <p>定义结果上传失败后的重试行为：
 * <ul>
 *   <li>是否应该重试
 *   <li>重试间隔（支持指数退避）
 *   <li>最大重试次数
 * </ul>
 *
 * <p>可以根据任务类型实现差异化策略：
 * <ul>
 *   <li>profiling 结果：更积极的重试（数据重要）
 *   <li>thread dump：轻量重试（时效性强）
 * </ul>
 */
public interface TaskResultRetryPolicy {

  /**
   * 判断是否应该重试
   *
   * @param descriptor 结果描述符
   * @param attempt 当前尝试次数（从 1 开始）
   * @return 是否应该重试
   */
  boolean shouldRetry(TaskResultDescriptor descriptor, int attempt);

  /**
   * 计算下次重试的退避时间
   *
   * @param attempt 当前尝试次数（从 1 开始）
   * @return 退避时间
   */
  Duration nextBackoff(int attempt);

  /**
   * 获取最大重试次数
   *
   * @return 最大重试次数
   */
  int maxAttempts();

  /**
   * 创建默认重试策略
   *
   * <p>默认参数：
   * <ul>
   *   <li>最大重试次数：3
   *   <li>初始退避：1 秒
   *   <li>最大退避：30 秒
   *   <li>退避乘数：2.0
   * </ul>
   *
   * @return 默认策略
   */
  static TaskResultRetryPolicy defaultPolicy() {
    return new DefaultRetryPolicy(3, Duration.ofSeconds(1), Duration.ofSeconds(30), 2.0);
  }

  /**
   * 创建自定义重试策略
   *
   * @param maxAttempts 最大重试次数
   * @param initialBackoff 初始退避时间
   * @param maxBackoff 最大退避时间
   * @param multiplier 退避乘数
   * @return 自定义策略
   */
  static TaskResultRetryPolicy create(
      int maxAttempts,
      Duration initialBackoff,
      Duration maxBackoff,
      double multiplier) {
    return new DefaultRetryPolicy(maxAttempts, initialBackoff, maxBackoff, multiplier);
  }

  /**
   * 创建不重试策略
   *
   * @return 不重试策略
   */
  static TaskResultRetryPolicy noRetry() {
    return new TaskResultRetryPolicy() {
      @Override
      public boolean shouldRetry(TaskResultDescriptor descriptor, int attempt) {
        return false;
      }

      @Override
      public Duration nextBackoff(int attempt) {
        return Duration.ZERO;
      }

      @Override
      public int maxAttempts() {
        return 0;
      }
    };
  }

  /**
   * 默认重试策略实现（指数退避）
   */
  final class DefaultRetryPolicy implements TaskResultRetryPolicy {

    private final int maxAttempts;
    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final double multiplier;

    DefaultRetryPolicy(
        int maxAttempts,
        Duration initialBackoff,
        Duration maxBackoff,
        double multiplier) {
      if (maxAttempts < 0) {
        throw new IllegalArgumentException("maxAttempts must be >= 0");
      }
      if (multiplier < 1.0) {
        throw new IllegalArgumentException("multiplier must be >= 1.0");
      }
      this.maxAttempts = maxAttempts;
      this.initialBackoff = initialBackoff;
      this.maxBackoff = maxBackoff;
      this.multiplier = multiplier;
    }

    @Override
    public boolean shouldRetry(TaskResultDescriptor descriptor, int attempt) {
      // 已放弃的结果不再重试
      if (descriptor.getStatus() == TaskResultDescriptor.ResultStatus.ABANDONED) {
        return false;
      }
      // 已上传成功的不需要重试
      if (descriptor.getStatus() == TaskResultDescriptor.ResultStatus.UPLOADED) {
        return false;
      }
      return attempt <= maxAttempts;
    }

    @Override
    public Duration nextBackoff(int attempt) {
      if (attempt <= 0) {
        return initialBackoff;
      }
      // 计算指数退避：initialBackoff * multiplier^(attempt-1)
      long backoffMillis = (long) (initialBackoff.toMillis() * Math.pow(multiplier, attempt - 1));
      // 不超过最大退避时间
      backoffMillis = Math.min(backoffMillis, maxBackoff.toMillis());
      return Duration.ofMillis(backoffMillis);
    }

    @Override
    public int maxAttempts() {
      return maxAttempts;
    }
  }
}
