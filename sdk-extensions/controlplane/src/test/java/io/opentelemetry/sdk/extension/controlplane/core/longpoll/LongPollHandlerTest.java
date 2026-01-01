/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LongPollHandlerTest {

  @Test
  void noChangeResult() {
    LongPollHandler.HandlerResult result = LongPollHandler.HandlerResult.noChange();

    assertThat(result.hasChanges()).isFalse();
    assertThat(result.getMessage()).isNull();
  }

  @Test
  void changedResult() {
    LongPollHandler.HandlerResult result = LongPollHandler.HandlerResult.changed();

    assertThat(result.hasChanges()).isTrue();
    assertThat(result.getMessage()).isNull();
  }

  @Test
  void changedResultWithMessage() {
    LongPollHandler.HandlerResult result = LongPollHandler.HandlerResult.changed("config updated");

    assertThat(result.hasChanges()).isTrue();
    assertThat(result.getMessage()).isEqualTo("config updated");
  }
}
