/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RetryHelperTest {

  @Test
  void successfulOperationDoesNotRetry() {
    AtomicInteger attempts = new AtomicInteger(0);

    String result =
        RetryHelper.executeWithRetry(
            () -> {
              attempts.incrementAndGet();
              return "success";
            },
            RetryHelper.RetryConfig.defaultConfig());

    assertThat(result).isEqualTo("success");
    assertThat(attempts.get()).isEqualTo(1);
  }

  @Test
  void retriesOnFailureUntilSuccess() {
    AtomicInteger attempts = new AtomicInteger(0);

    String result =
        RetryHelper.executeWithRetry(
            () -> {
              int attempt = attempts.incrementAndGet();
              if (attempt < 3) {
                throw new RuntimeException("Temporary failure");
              }
              return "success";
            },
            RetryHelper.RetryConfig.builder()
                .setMaxRetries(3)
                .setInitialDelayMs(10)
                .build());

    assertThat(result).isEqualTo("success");
    assertThat(attempts.get()).isEqualTo(3);
  }

  @Test
  void throwsAfterMaxRetries() {
    AtomicInteger attempts = new AtomicInteger(0);

    assertThatThrownBy(
            () ->
                RetryHelper.executeWithRetry(
                    () -> {
                      attempts.incrementAndGet();
                      throw new RuntimeException("Always fails");
                    },
                    RetryHelper.RetryConfig.builder()
                        .setMaxRetries(2)
                        .setInitialDelayMs(10)
                        .build()))
        .isInstanceOf(ArthasException.class)
        .hasMessageContaining("failed after 3 attempts");

    // 初始尝试 + 2 次重试 = 3 次
    assertThat(attempts.get()).isEqualTo(3);
  }

  @Test
  void noRetryConfigDoesNotRetry() {
    AtomicInteger attempts = new AtomicInteger(0);

    assertThatThrownBy(
            () ->
                RetryHelper.executeWithRetry(
                    () -> {
                      attempts.incrementAndGet();
                      throw new RuntimeException("Fails");
                    },
                    RetryHelper.RetryConfig.noRetry()))
        .isInstanceOf(ArthasException.class);

    assertThat(attempts.get()).isEqualTo(1);
  }

  @Test
  void retryPredicateCanFilterRetryableExceptions() {
    AtomicInteger attempts = new AtomicInteger(0);

    // 只重试 IllegalStateException
    assertThatThrownBy(
            () ->
                RetryHelper.executeWithRetry(
                    () -> {
                      attempts.incrementAndGet();
                      throw new IllegalArgumentException("Not retryable");
                    },
                    RetryHelper.RetryConfig.builder()
                        .setMaxRetries(3)
                        .setInitialDelayMs(10)
                        .setRetryPredicate(e -> e instanceof IllegalStateException)
                        .build()))
        .isInstanceOf(ArthasException.class);

    // 不应该重试
    assertThat(attempts.get()).isEqualTo(1);
  }

  @Test
  void retryPredicateAllowsRetryableExceptions() {
    AtomicInteger attempts = new AtomicInteger(0);

    assertThatThrownBy(
            () ->
                RetryHelper.executeWithRetry(
                    () -> {
                      attempts.incrementAndGet();
                      throw new IllegalStateException("Retryable");
                    },
                    RetryHelper.RetryConfig.builder()
                        .setMaxRetries(2)
                        .setInitialDelayMs(10)
                        .setRetryPredicate(e -> e instanceof IllegalStateException)
                        .build()))
        .isInstanceOf(ArthasException.class);

    // 应该重试
    assertThat(attempts.get()).isEqualTo(3);
  }

  @Test
  void retryOnRecoverableArthasExceptionFiltersCorrectly() {
    AtomicInteger attempts = new AtomicInteger(0);

    // 可恢复的异常应该重试
    assertThatThrownBy(
            () ->
                RetryHelper.executeWithRetry(
                    () -> {
                      attempts.incrementAndGet();
                      throw ArthasException.connectionFailed("Test", null);
                    },
                    RetryHelper.RetryConfig.builder()
                        .setMaxRetries(2)
                        .setInitialDelayMs(10)
                        .retryOnRecoverableArthasException()
                        .build()))
        .isInstanceOf(ArthasException.class);

    assertThat(attempts.get()).isEqualTo(3);
  }

  @Test
  void nonRecoverableArthasExceptionDoesNotRetry() {
    AtomicInteger attempts = new AtomicInteger(0);

    // 不可恢复的异常不应该重试
    assertThatThrownBy(
            () ->
                RetryHelper.executeWithRetry(
                    () -> {
                      attempts.incrementAndGet();
                      throw ArthasException.configError("Test");
                    },
                    RetryHelper.RetryConfig.builder()
                        .setMaxRetries(2)
                        .setInitialDelayMs(10)
                        .retryOnRecoverableArthasException()
                        .build()))
        .isInstanceOf(ArthasException.class);

    assertThat(attempts.get()).isEqualTo(1);
  }

  @Test
  void listenerIsNotifiedOnRetryAttempt() {
    AtomicInteger listenerCallCount = new AtomicInteger(0);
    AtomicInteger successCallCount = new AtomicInteger(0);
    AtomicInteger attempts = new AtomicInteger(0);

    String result =
        RetryHelper.executeWithRetry(
            () -> {
              int attempt = attempts.incrementAndGet();
              if (attempt < 2) {
                throw new RuntimeException("Retry");
              }
              return "success";
            },
            RetryHelper.RetryConfig.builder().setMaxRetries(3).setInitialDelayMs(10).build(),
            new RetryHelper.RetryListener() {
              @Override
              public void onRetryAttempt(int attempt, Exception exception, boolean willRetry) {
                listenerCallCount.incrementAndGet();
              }

              @Override
              public void onRetrySuccess(int totalAttempts) {
                successCallCount.incrementAndGet();
              }
            });

    assertThat(result).isEqualTo("success");
    assertThat(listenerCallCount.get()).isEqualTo(1); // 第一次失败时调用
    assertThat(successCallCount.get()).isEqualTo(1); // 成功时调用
  }

  @Test
  void runnableVersionWorks() {
    AtomicInteger attempts = new AtomicInteger(0);

    RetryHelper.executeWithRetry(
        () -> {
          int attempt = attempts.incrementAndGet();
          if (attempt < 2) {
            throw new RuntimeException("Retry");
          }
        },
        RetryHelper.RetryConfig.builder().setMaxRetries(3).setInitialDelayMs(10).build());

    assertThat(attempts.get()).isEqualTo(2);
  }

  @Test
  void configBuilderValidatesMaxRetries() {
    assertThatThrownBy(
            () -> RetryHelper.RetryConfig.builder().setMaxRetries(-1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxRetries must be >= 0");
  }

  @Test
  void configBuilderValidatesBackoffMultiplier() {
    assertThatThrownBy(
            () -> RetryHelper.RetryConfig.builder().setBackoffMultiplier(0.5).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("backoffMultiplier must be >= 1.0");
  }

  @Test
  void configBuilderAcceptsDuration() {
    RetryHelper.RetryConfig config =
        RetryHelper.RetryConfig.builder()
            .setInitialDelay(Duration.ofMillis(500))
            .setMaxDelay(Duration.ofSeconds(30))
            .build();

    assertThat(config.initialDelayMs).isEqualTo(500);
    assertThat(config.maxDelayMs).isEqualTo(30000);
  }
}
