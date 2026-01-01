/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class LongPollConfigTest {

  @Test
  void defaultValues() {
    LongPollConfig config = LongPollConfig.getDefault();

    assertThat(config.getTimeout()).isEqualTo(Duration.ofSeconds(60));
    assertThat(config.getTimeoutMillis()).isEqualTo(60000);
    assertThat(config.getMinRetryInterval()).isEqualTo(Duration.ofSeconds(1));
    assertThat(config.getMaxRetryInterval()).isEqualTo(Duration.ofSeconds(30));
    assertThat(config.getBackoffMultiplier()).isEqualTo(2.0);
    assertThat(config.getMaxConsecutiveErrors()).isEqualTo(5);
  }

  @Test
  void builderSetsValues() {
    LongPollConfig config =
        LongPollConfig.builder()
            .setTimeout(Duration.ofMinutes(2))
            .setMinRetryInterval(Duration.ofSeconds(2))
            .setMaxRetryInterval(Duration.ofMinutes(1))
            .setBackoffMultiplier(1.5)
            .setMaxConsecutiveErrors(10)
            .build();

    assertThat(config.getTimeout()).isEqualTo(Duration.ofMinutes(2));
    assertThat(config.getMinRetryInterval()).isEqualTo(Duration.ofSeconds(2));
    assertThat(config.getMaxRetryInterval()).isEqualTo(Duration.ofMinutes(1));
    assertThat(config.getBackoffMultiplier()).isEqualTo(1.5);
    assertThat(config.getMaxConsecutiveErrors()).isEqualTo(10);
  }

  @Test
  void validationFailsWhenMinRetryGreaterThanMaxRetry() {
    assertThatThrownBy(
            () ->
                LongPollConfig.builder()
                    .setMinRetryInterval(Duration.ofMinutes(1))
                    .setMaxRetryInterval(Duration.ofSeconds(30))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("minRetryInterval must be <= maxRetryInterval");
  }

  @Test
  void validationFailsWhenBackoffMultiplierLessThanOne() {
    assertThatThrownBy(() -> LongPollConfig.builder().setBackoffMultiplier(0.5).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("backoffMultiplier must be >= 1.0");
  }

  @Test
  void validationFailsWhenMaxConsecutiveErrorsLessThanOne() {
    assertThatThrownBy(() -> LongPollConfig.builder().setMaxConsecutiveErrors(0).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxConsecutiveErrors must be >= 1");
  }

  @Test
  void toStringContainsAllFields() {
    LongPollConfig config = LongPollConfig.getDefault();
    String str = config.toString();

    assertThat(str).contains("timeout");
    assertThat(str).contains("minRetryInterval");
    assertThat(str).contains("maxRetryInterval");
    assertThat(str).contains("backoffMultiplier");
    assertThat(str).contains("maxConsecutiveErrors");
  }
}
