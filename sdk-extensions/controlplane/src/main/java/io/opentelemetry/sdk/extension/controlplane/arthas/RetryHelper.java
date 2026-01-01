/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 重试工具类
 *
 * <p>提供通用的重试机制，支持：
 * <ul>
 *   <li>配置最大重试次数
 *   <li>配置重试间隔（支持指数退避）
 *   <li>配置可重试异常的判断逻辑
 *   <li>支持回调通知
 * </ul>
 */
public final class RetryHelper {

  private static final Logger logger = Logger.getLogger(RetryHelper.class.getName());

  private RetryHelper() {
    // 工具类不允许实例化
  }

  /**
   * 执行带重试的操作
   *
   * @param operation 要执行的操作
   * @param config 重试配置
   * @param <T> 返回值类型
   * @return 操作结果
   * @throws ArthasException 如果所有重试都失败
   */
  public static <T> T executeWithRetry(Callable<T> operation, RetryConfig config) {
    return executeWithRetry(operation, config, null);
  }

  /**
   * 执行带重试的操作
   *
   * @param operation 要执行的操作
   * @param config 重试配置
   * @param listener 重试监听器
   * @param <T> 返回值类型
   * @return 操作结果
   * @throws ArthasException 如果所有重试都失败
   */
  public static <T> T executeWithRetry(
      Callable<T> operation, RetryConfig config, @Nullable RetryListener listener) {

    Exception lastException = null;
    int attempt = 0;

    while (attempt <= config.maxRetries) {
      try {
        T result = operation.call();
        if (attempt > 0 && listener != null) {
          listener.onRetrySuccess(attempt);
        }
        return result;

      } catch (Exception e) {
        lastException = e;
        boolean shouldRetry = config.retryPredicate.test(e) && attempt < config.maxRetries;

        if (listener != null) {
          listener.onRetryAttempt(attempt, e, shouldRetry);
        }

        if (!shouldRetry) {
          break;
        }

        // 计算延迟时间
        long delayMs = calculateDelay(attempt, config);
        logger.log(
            Level.FINE,
            "Retry attempt {0} failed, will retry in {1}ms: {2}",
            new Object[] {attempt, delayMs, e.getMessage()});

        sleep(delayMs);
        attempt++;
      }
    }

    // 所有重试都失败
    String message =
        String.format(
            Locale.ROOT,
            "Operation failed after %d attempts: %s",
            attempt + 1, lastException != null ? lastException.getMessage() : "unknown error");

    if (lastException instanceof ArthasException) {
      throw (ArthasException) lastException;
    }

    throw new ArthasException(
        ArthasException.Type.UNKNOWN, message, lastException, /* recoverable= */ false);
  }

  /**
   * 执行带重试的操作（无返回值）
   *
   * @param operation 要执行的操作
   * @param config 重试配置
   * @throws ArthasException 如果所有重试都失败
   */
  public static void executeWithRetry(Runnable operation, RetryConfig config) {
    executeWithRetry(
        () -> {
          operation.run();
          return null;
        },
        config);
  }

  /**
   * 计算重试延迟
   *
   * @param attempt 当前重试次数
   * @param config 重试配置
   * @return 延迟时间（毫秒）
   */
  private static long calculateDelay(int attempt, RetryConfig config) {
    if (!config.exponentialBackoff) {
      return config.initialDelayMs;
    }

    // 指数退避：delay = initialDelay * (multiplier ^ attempt)
    double delay = config.initialDelayMs * Math.pow(config.backoffMultiplier, attempt);
    return Math.min((long) delay, config.maxDelayMs);
  }

  /**
   * 安全地休眠
   *
   * @param millis 毫秒数
   */
  private static void sleep(long millis) {
    try {
      TimeUnit.MILLISECONDS.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** 重试配置 */
  public static final class RetryConfig {
    final int maxRetries;
    final long initialDelayMs;
    final long maxDelayMs;
    final boolean exponentialBackoff;
    final double backoffMultiplier;
    final Predicate<Exception> retryPredicate;

    private RetryConfig(Builder builder) {
      this.maxRetries = builder.maxRetries;
      this.initialDelayMs = builder.initialDelayMs;
      this.maxDelayMs = builder.maxDelayMs;
      this.exponentialBackoff = builder.exponentialBackoff;
      this.backoffMultiplier = builder.backoffMultiplier;
      this.retryPredicate = builder.retryPredicate;
    }

    /** 创建构建器 */
    public static Builder builder() {
      return new Builder();
    }

    /** 创建默认配置（3 次重试，1 秒间隔） */
    public static RetryConfig defaultConfig() {
      return builder().build();
    }

    /** 创建无重试配置 */
    public static RetryConfig noRetry() {
      return builder().setMaxRetries(0).build();
    }

    /** Builder for {@link RetryConfig}. */
    public static final class Builder {
      private int maxRetries = 3;
      private long initialDelayMs = 1000;
      private long maxDelayMs = 30000;
      private boolean exponentialBackoff = true;
      private double backoffMultiplier = 2.0;
      private Predicate<Exception> retryPredicate = e -> true;

      private Builder() {}

      /** 设置最大重试次数 */
      public Builder setMaxRetries(int maxRetries) {
        if (maxRetries < 0) {
          throw new IllegalArgumentException("maxRetries must be >= 0");
        }
        this.maxRetries = maxRetries;
        return this;
      }

      /** 设置初始延迟时间 */
      public Builder setInitialDelay(Duration initialDelay) {
        this.initialDelayMs = initialDelay.toMillis();
        return this;
      }

      /** 设置初始延迟时间（毫秒） */
      public Builder setInitialDelayMs(long initialDelayMs) {
        this.initialDelayMs = initialDelayMs;
        return this;
      }

      /** 设置最大延迟时间 */
      public Builder setMaxDelay(Duration maxDelay) {
        this.maxDelayMs = maxDelay.toMillis();
        return this;
      }

      /** 设置最大延迟时间（毫秒） */
      public Builder setMaxDelayMs(long maxDelayMs) {
        this.maxDelayMs = maxDelayMs;
        return this;
      }

      /** 设置是否使用指数退避 */
      public Builder setExponentialBackoff(boolean exponentialBackoff) {
        this.exponentialBackoff = exponentialBackoff;
        return this;
      }

      /** 设置退避乘数 */
      public Builder setBackoffMultiplier(double backoffMultiplier) {
        if (backoffMultiplier < 1.0) {
          throw new IllegalArgumentException("backoffMultiplier must be >= 1.0");
        }
        this.backoffMultiplier = backoffMultiplier;
        return this;
      }

      /** 设置重试条件 */
      public Builder setRetryPredicate(Predicate<Exception> retryPredicate) {
        this.retryPredicate = retryPredicate;
        return this;
      }

      /** 只重试可恢复的 ArthasException */
      public Builder retryOnRecoverableArthasException() {
        this.retryPredicate =
            e -> e instanceof ArthasException && ((ArthasException) e).isRecoverable();
        return this;
      }

      /** 构建配置 */
      public RetryConfig build() {
        return new RetryConfig(this);
      }
    }
  }

  /** 重试监听器 */
  public interface RetryListener {
    /**
     * 重试尝试时调用
     *
     * @param attempt 当前尝试次数（从 0 开始）
     * @param exception 捕获的异常
     * @param willRetry 是否会继续重试
     */
    void onRetryAttempt(int attempt, Exception exception, boolean willRetry);

    /**
     * 重试成功时调用
     *
     * @param totalAttempts 总尝试次数
     */
    default void onRetrySuccess(int totalAttempts) {}
  }
}
