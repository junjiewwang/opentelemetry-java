/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TunnelUrlGeneratorOpsTest {

  private final TunnelUrlGenerator generator = TunnelUrlGenerator.getInstance();

  @Test
  void localExplicitEndpointHasHighestPriority() {
    String result =
        generator.resolveEffectiveEndpoint(
            "ws://local.example/v1/arthas/ws",
            "ws://ops.example/v1/arthas/ws",
            "http://collector.example:4318",
            4318);
    assertThat(result).isEqualTo("ws://local.example/v1/arthas/ws");
  }

  @Test
  void opsEndpointWinsOverDerived() {
    String result =
        generator.resolveEffectiveEndpoint(
            null, "ws://ops.example/v1/arthas/ws", "http://collector.example:4318", 4318);
    assertThat(result).isEqualTo("ws://ops.example/v1/arthas/ws");
  }

  @Test
  void missingOpsEndpointFallsBackToDerived() {
    String result =
        generator.resolveEffectiveEndpoint(null, null, "http://collector.example:4318", null);
    assertThat(result).isEqualTo("ws://collector.example:4318/v1/arthas/ws");
  }

  @Test
  void legacyOverloadsRemainBackwardCompatible() {
    assertThat(
            generator.resolveEffectiveEndpoint(
                null, "http://collector.example:4318", null))
        .isEqualTo("ws://collector.example:4318/v1/arthas/ws");
  }
}
