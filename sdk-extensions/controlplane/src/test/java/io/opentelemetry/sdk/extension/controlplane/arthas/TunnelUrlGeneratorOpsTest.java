/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.extension.controlplane.core.model.OpsTunnelOverride;
import org.junit.jupiter.api.Test;

class TunnelUrlGeneratorOpsTest {

  private final TunnelUrlGenerator generator = TunnelUrlGenerator.getInstance();

  private static ArthasConfig config(String baseOtlpEndpoint) {
    return ArthasConfig.builder().setBaseOtlpEndpoint(baseOtlpEndpoint).build();
  }

  @Test
  void localExplicitEndpointHasHighestPriority() {
    ArthasConfig local =
        ArthasConfig.builder()
            .setTunnelEndpoint("ws://local.example/v1/arthas/ws")
            .setBaseOtlpEndpoint("http://collector.example:4318")
            .build();
    String result =
        generator.resolveEffectiveEndpoint(
            local, 4318, OpsTunnelOverride.of("https://gw.example/otel"));

    assertThat(result).isEqualTo("ws://local.example/v1/arthas/ws");
  }

  @Test
  void opsBaseUrlWinsOverDerived() {
    String result =
        generator.resolveEffectiveEndpoint(
            config("http://collector.example:4318"),
            4318,
            OpsTunnelOverride.of("https://gw.example/otel"));

    assertThat(result).isEqualTo("wss://gw.example/otel/v1/arthas/ws");
  }

  @Test
  void missingOpsOverrideFallsBackToDerived() {
    String result =
        generator.resolveEffectiveEndpoint(
            config("http://collector.example:4318"), 4318, OpsTunnelOverride.empty());

    assertThat(result).isEqualTo("ws://collector.example:4318/v1/arthas/ws");
  }

  @Test
  void legacyOverloadsRemainBackwardCompatible() {
    assertThat(
            generator.resolveEffectiveEndpoint(
                null, "http://collector.example:4318", null))
        .isEqualTo("ws://collector.example:4318/v1/arthas/ws");
    assertThat(
            generator.resolveEffectiveEndpoint(
                config("http://collector.example:4318"), null))
        .isEqualTo("ws://collector.example:4318/v1/arthas/ws");
  }
}
