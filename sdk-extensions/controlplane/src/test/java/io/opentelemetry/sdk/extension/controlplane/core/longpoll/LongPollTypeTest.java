/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LongPollTypeTest {

  @Test
  void configType() {
    assertThat(LongPollType.CONFIG.getName()).isEqualTo("config");
    assertThat(LongPollType.CONFIG.toString()).isEqualTo("config");
  }

  @Test
  void taskType() {
    assertThat(LongPollType.TASK.getName()).isEqualTo("task");
    assertThat(LongPollType.TASK.toString()).isEqualTo("task");
  }

  @Test
  void combinedType() {
    assertThat(LongPollType.COMBINED.getName()).isEqualTo("combined");
    assertThat(LongPollType.COMBINED.toString()).isEqualTo("combined");
  }

  @Test
  void allTypesAreDifferent() {
    assertThat(LongPollType.CONFIG).isNotEqualTo(LongPollType.TASK);
    assertThat(LongPollType.TASK).isNotEqualTo(LongPollType.COMBINED);
    assertThat(LongPollType.CONFIG).isNotEqualTo(LongPollType.COMBINED);
  }
}
