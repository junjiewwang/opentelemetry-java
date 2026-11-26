/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.extension.controlplane.core.model.OpsTunnelOverride;
import org.junit.jupiter.api.Test;

class TunnelUrlGeneratorDerivationTest {

  private final TunnelUrlGenerator generator = TunnelUrlGenerator.getInstance();

  private static ArthasConfig config(String baseOtlpEndpoint) {
    return ArthasConfig.builder().setBaseOtlpEndpoint(baseOtlpEndpoint).build();
  }

  private String resolve(String baseOtlpEndpoint, Integer port, OpsTunnelOverride override) {
    return generator.resolveEffectiveEndpoint(config(baseOtlpEndpoint), port, override);
  }

  // ===== derived 分支 =====

  @Test
  void derivedHttpWithExplicitPort() {
    assertThat(generator.generateDefaultEndpoint("http://10.0.0.1:4318", 4318))
        .isEqualTo("ws://10.0.0.1:4318/v1/arthas/ws");
  }

  @Test
  void derivedHttpsDomainWithoutPortDoesNotInjectPort() {
    assertThat(generator.generateDefaultEndpoint("https://collector.example", 4318))
        .isEqualTo("wss://collector.example/v1/arthas/ws");
    assertThat(resolve("https://collector.example", 4318, OpsTunnelOverride.empty()))
        .isEqualTo("wss://collector.example/v1/arthas/ws");
  }

  @Test
  void derivedGrpcWithExplicitPortIsReplaced() {
    assertThat(generator.generateDefaultEndpoint("http://10.0.0.1:4317", 4318))
        .isEqualTo("ws://10.0.0.1:4318/v1/arthas/ws");
  }

  @Test
  void derivedGrpcDomainWithoutPortDoesNotInjectPort() {
    assertThat(generator.generateDefaultEndpoint("https://grpc.example", 4318))
        .isEqualTo("wss://grpc.example/v1/arthas/ws");
  }

  @Test
  void defaultDerivationDropsOriginalPath() {
    assertThat(generator.generateDefaultEndpoint("https://collector.example/otel", 4318))
        .isEqualTo("wss://collector.example/v1/arthas/ws");
  }

  @Test
  void nonPositiveOrMissingPortIsIgnored() {
    assertThat(generator.generateDefaultEndpoint("http://10.0.0.1:4318", null))
        .isEqualTo("ws://10.0.0.1:4318/v1/arthas/ws");
    assertThat(generator.generateDefaultEndpoint("http://10.0.0.1:4318", 0))
        .isEqualTo("ws://10.0.0.1:4318/v1/arthas/ws");
    assertThat(generator.generateDefaultEndpoint("http://10.0.0.1:4318", -1))
        .isEqualTo("ws://10.0.0.1:4318/v1/arthas/ws");
  }

  // ===== base URL 覆盖分支 =====

  @Test
  void baseUrlKeepsPathPrefix() {
    assertThat(generator.generateFromBaseUrl("https://gw.example/otel"))
        .isEqualTo("wss://gw.example/otel/v1/arthas/ws");
    assertThat(
            resolve(
                "http://collector.example:4318",
                4318,
                OpsTunnelOverride.of("https://gw.example/otel")))
        .isEqualTo("wss://gw.example/otel/v1/arthas/ws");
  }

  @Test
  void baseUrlWithExplicitPortAndTrailingSlash() {
    assertThat(generator.generateFromBaseUrl("http://gw.example:8080/otel/"))
        .isEqualTo("ws://gw.example:8080/otel/v1/arthas/ws");
  }

  @Test
  void baseUrlWsSchemesArePreserved() {
    assertThat(generator.generateFromBaseUrl("ws://gw.example/otel"))
        .isEqualTo("ws://gw.example/otel/v1/arthas/ws");
    assertThat(generator.generateFromBaseUrl("wss://gw.example/otel"))
        .isEqualTo("wss://gw.example/otel/v1/arthas/ws");
  }

  @Test
  void baseUrlWithoutPath() {
    assertThat(generator.generateFromBaseUrl("https://gw.example"))
        .isEqualTo("wss://gw.example/v1/arthas/ws");
  }

  @Test
  void baseUrlDoesNotApplyServerHttpPort() {
    assertThat(
            resolve(
                "https://gw.example",
                4318,
                OpsTunnelOverride.of("https://gw.example/otel")))
        .isEqualTo("wss://gw.example/otel/v1/arthas/ws");
  }

  // ===== 优先级与回落 =====

  @Test
  void localExplicitEndpointWinsOverOpsBaseUrl() {
    ArthasConfig local =
        ArthasConfig.builder()
            .setTunnelEndpoint("ws://local.example/v1/arthas/ws")
            .setBaseOtlpEndpoint("http://collector.example:4318")
            .build();
    assertThat(
            generator.resolveEffectiveEndpoint(
                local, 4318, OpsTunnelOverride.of("https://gw.example/otel")))
        .isEqualTo("ws://local.example/v1/arthas/ws");
  }

  @Test
  void removingBaseUrlFallsBackToDerived() {
    assertThat(resolve("http://collector.example:4318", 4318, OpsTunnelOverride.empty()))
        .isEqualTo("ws://collector.example:4318/v1/arthas/ws");
  }

  @Test
  void invalidBaseUrlsFallBackToDerived() {
    String[] invalid = {
      "https://gw.example/v1/arthas/ws",
      "https://gw.example/otel?x=1",
      "https://gw.example/otel#frag",
      "https://user:secret@gw.example/otel",
      "ftp://gw.example/otel",
      "https://",
      "not a url",
      "   ",
      null
    };
    for (String baseUrl : invalid) {
      assertThat(generator.generateFromBaseUrl(baseUrl)).isNull();
      assertThat(
              resolve(
                  "http://collector.example:4318",
                  4318,
                  OpsTunnelOverride.of(baseUrl)))
          .isEqualTo("ws://collector.example:4318/v1/arthas/ws");
    }
  }

  @Test
  void fullEndpointShapedBaseUrlIsRejectedAndFallsBackToDerived() {
    // 旧的完整 endpoint 形态含固定 tunnel path，会被 base URL 校验拒绝并安全回落
    assertThat(
            resolve(
                "http://collector.example:4318",
                4318,
                OpsTunnelOverride.of("ws://ops.example/v1/arthas/ws")))
        .isEqualTo("ws://collector.example:4318/v1/arthas/ws");
  }

  @Test
  void legacyStringOverloadStillFallsBackToDerived() {
    assertThat(
            generator.resolveEffectiveEndpoint(
                null, "http://collector.example:4318", null))
        .isEqualTo("ws://collector.example:4318/v1/arthas/ws");
  }

  @Test
  void twoParamConfigOverloadFallsBackToDerived() {
    assertThat(generator.resolveEffectiveEndpoint(config("http://collector.example:4318"), null))
        .isEqualTo("ws://collector.example:4318/v1/arthas/ws");
  }
}
