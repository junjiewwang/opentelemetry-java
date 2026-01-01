/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

import java.time.Duration;
import java.util.Objects;

/**
 * 长轮询配置
 *
 * <p>定义长轮询的超时、重试等配置参数。
 */
public final class LongPollConfig {

  // 默认值
  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration DEFAULT_MIN_RETRY_INTERVAL = Duration.ofSeconds(1);
  private static final Duration DEFAULT_MAX_RETRY_INTERVAL = Duration.ofSeconds(30);
  private static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0;
  private static final int DEFAULT_MAX_CONSECUTIVE_ERRORS = 5;

  private final Duration timeout;
  private final Duration minRetryInterval;
  private final Duration maxRetryInterval;
  private final double backoffMultiplier;
  private final int maxConsecutiveErrors;

  private LongPollConfig(Builder builder) {
    this.timeout = builder.timeout;
    this.minRetryInterval = builder.minRetryInterval;
    this.maxRetryInterval = builder.maxRetryInterval;
    this.backoffMultiplier = builder.backoffMultiplier;
    this.maxConsecutiveErrors = builder.maxConsecutiveErrors;
  }

  /**
   * 创建默认配置
   *
   * @return 默认长轮询配置
   */
  public static LongPollConfig getDefault() {
    return builder().build();
  }

  /**
   * 创建 Builder
   *
   * @return Builder 实例
   */
  public static Builder builder() {
    return new Builder();
  }

  // ===== Getters =====

  /**
   * 获取长轮询超时时间
   *
   * <p>服务端在此时间内无变更时返回空响应
   *
   * @return 超时时间
   */
  public Duration getTimeout() {
    return timeout;
  }

  /**
   * 获取超时时间（毫秒）
   *
   * @return 超时毫秒数
   */
  public long getTimeoutMillis() {
    return timeout.toMillis();
  }

  /**
   * 获取最小重试间隔
   *
   * <p>发生错误时的初始重试间隔
   *
   * @return 最小重试间隔
   */
  public Duration getMinRetryInterval() {
    return minRetryInterval;
  }

  /**
   * 获取最大重试间隔
   *
   * <p>指数退避的上限
   *
   * @return 最大重试间隔
   */
  public Duration getMaxRetryInterval() {
    return maxRetryInterval;
  }

  /**
   * 获取退避乘数
   *
   * <p>每次重试间隔乘以此值，直到达到最大间隔
   *
   * @return 退避乘数
   */
  public double getBackoffMultiplier() {
    return backoffMultiplier;
  }

  /**
   * 获取最大连续错误次数
   *
   * <p>超过此次数后进入降级模式
   *
   * @return 最大连续错误次数
   */
  public int getMaxConsecutiveErrors() {
    return maxConsecutiveErrors;
  }

  @Override
  public String toString() {
    return "LongPollConfig{"
        + "timeout="
        + timeout
        + ", minRetryInterval="
        + minRetryInterval
        + ", maxRetryInterval="
        + maxRetryInterval
        + ", backoffMultiplier="
        + backoffMultiplier
        + ", maxConsecutiveErrors="
        + maxConsecutiveErrors
        + '}';
  }

  /** Builder for LongPollConfig */
  public static final class Builder {
    private Duration timeout = DEFAULT_TIMEOUT;
    private Duration minRetryInterval = DEFAULT_MIN_RETRY_INTERVAL;
    private Duration maxRetryInterval = DEFAULT_MAX_RETRY_INTERVAL;
    private double backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;
    private int maxConsecutiveErrors = DEFAULT_MAX_CONSECUTIVE_ERRORS;

    private Builder() {}

    /**
     * 设置长轮询超时时间
     *
     * @param timeout 超时时间
     * @return this builder
     */
    public Builder setTimeout(Duration timeout) {
      this.timeout = Objects.requireNonNull(timeout, "timeout");
      return this;
    }

    /**
     * 设置最小重试间隔
     *
     * @param minRetryInterval 最小重试间隔
     * @return this builder
     */
    public Builder setMinRetryInterval(Duration minRetryInterval) {
      this.minRetryInterval = Objects.requireNonNull(minRetryInterval, "minRetryInterval");
      return this;
    }

    /**
     * 设置最大重试间隔
     *
     * @param maxRetryInterval 最大重试间隔
     * @return this builder
     */
    public Builder setMaxRetryInterval(Duration maxRetryInterval) {
      this.maxRetryInterval = Objects.requireNonNull(maxRetryInterval, "maxRetryInterval");
      return this;
    }

    /**
     * 设置退避乘数
     *
     * @param backoffMultiplier 退避乘数
     * @return this builder
     */
    public Builder setBackoffMultiplier(double backoffMultiplier) {
      if (backoffMultiplier < 1.0) {
        throw new IllegalArgumentException("backoffMultiplier must be >= 1.0");
      }
      this.backoffMultiplier = backoffMultiplier;
      return this;
    }

    /**
     * 设置最大连续错误次数
     *
     * @param maxConsecutiveErrors 最大连续错误次数
     * @return this builder
     */
    public Builder setMaxConsecutiveErrors(int maxConsecutiveErrors) {
      if (maxConsecutiveErrors < 1) {
        throw new IllegalArgumentException("maxConsecutiveErrors must be >= 1");
      }
      this.maxConsecutiveErrors = maxConsecutiveErrors;
      return this;
    }

    /**
     * 构建配置实例
     *
     * @return LongPollConfig
     */
    public LongPollConfig build() {
      validate();
      return new LongPollConfig(this);
    }

    private void validate() {
      if (minRetryInterval.compareTo(maxRetryInterval) > 0) {
        throw new IllegalArgumentException(
            "minRetryInterval must be <= maxRetryInterval");
      }
    }
  }
}
