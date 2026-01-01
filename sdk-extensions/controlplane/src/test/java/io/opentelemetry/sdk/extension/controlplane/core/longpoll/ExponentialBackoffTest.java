/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ExponentialBackoffTest {

  @Test
  void initialBackoffReturnsMinInterval() {
    ExponentialBackoff backoff = new ExponentialBackoff(1000, 30000, 2.0);

    assertThat(backoff.nextBackoff()).isEqualTo(1000);
  }

  @Test
  void backoffIncreasesExponentially() {
    ExponentialBackoff backoff = new ExponentialBackoff(1000, 30000, 2.0);

    assertThat(backoff.nextBackoff()).isEqualTo(1000);
    assertThat(backoff.nextBackoff()).isEqualTo(2000);
    assertThat(backoff.nextBackoff()).isEqualTo(4000);
    assertThat(backoff.nextBackoff()).isEqualTo(8000);
  }

  @Test
  void backoffCapsAtMaxInterval() {
    ExponentialBackoff backoff = new ExponentialBackoff(1000, 5000, 2.0);

    // 1000 -> 2000 -> 4000 -> 5000 (capped) -> 5000
    backoff.nextBackoff();
    backoff.nextBackoff();
    backoff.nextBackoff();
    assertThat(backoff.nextBackoff()).isEqualTo(5000);
    assertThat(backoff.nextBackoff()).isEqualTo(5000);
  }

  @Test
  void resetResetsToMinInterval() {
    ExponentialBackoff backoff = new ExponentialBackoff(1000, 30000, 2.0);

    backoff.nextBackoff();
    backoff.nextBackoff();
    backoff.nextBackoff();
    assertThat(backoff.getCurrentInterval()).isGreaterThan(1000);

    backoff.reset();
    assertThat(backoff.getCurrentInterval()).isEqualTo(1000);
    assertThat(backoff.nextBackoff()).isEqualTo(1000);
  }

  @Test
  void getCurrentIntervalDoesNotAdvance() {
    ExponentialBackoff backoff = new ExponentialBackoff(1000, 30000, 2.0);

    assertThat(backoff.getCurrentInterval()).isEqualTo(1000);
    assertThat(backoff.getCurrentInterval()).isEqualTo(1000);
    assertThat(backoff.getCurrentInterval()).isEqualTo(1000);
  }

  @Test
  void constructorWithLongPollConfig() {
    LongPollConfig config =
        LongPollConfig.builder()
            .setMinRetryInterval(Duration.ofSeconds(2))
            .setMaxRetryInterval(Duration.ofSeconds(60))
            .setBackoffMultiplier(1.5)
            .build();

    ExponentialBackoff backoff = new ExponentialBackoff(config);

    assertThat(backoff.getMinInterval()).isEqualTo(2000);
    assertThat(backoff.getMaxInterval()).isEqualTo(60000);
    assertThat(backoff.nextBackoff()).isEqualTo(2000);
    assertThat(backoff.nextBackoff()).isEqualTo(3000); // 2000 * 1.5
  }

  @Test
  void toStringContainsRelevantInfo() {
    ExponentialBackoff backoff = new ExponentialBackoff(1000, 30000, 2.0);
    String str = backoff.toString();

    assertThat(str).contains("minIntervalMs=1000");
    assertThat(str).contains("maxIntervalMs=30000");
    assertThat(str).contains("multiplier=2.0");
    assertThat(str).contains("currentIntervalMs=1000");
  }
}
