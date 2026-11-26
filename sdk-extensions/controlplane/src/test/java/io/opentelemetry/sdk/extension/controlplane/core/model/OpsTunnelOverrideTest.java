/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OpsTunnelOverrideTest {

  @Test
  void emptyHasNoOverride() {
    OpsTunnelOverride override = OpsTunnelOverride.empty();

    assertThat(override.hasBaseUrl()).isFalse();
    assertThat(override.getBaseUrl()).isNull();
  }

  @Test
  void ofNormalizesBlankValues() {
    assertThat(OpsTunnelOverride.of("  ").hasBaseUrl()).isFalse();
    assertThat(OpsTunnelOverride.of(null).hasBaseUrl()).isFalse();
    assertThat(OpsTunnelOverride.of("").hasBaseUrl()).isFalse();
    assertThat(OpsTunnelOverride.of(" https://gw.example/otel ").getBaseUrl())
        .isEqualTo("https://gw.example/otel");
  }

  @Test
  void fromServerMetadataReadsBaseUrl() {
    Map<String, String> raw = new HashMap<>();
    raw.put("ops.arthas.tunnel_base_url", "https://gw.example/otel");
    raw.put("http_port", "4318");

    OpsTunnelOverride override = OpsTunnelOverride.from(ServerMetadata.fromMap(raw));

    assertThat(override.getBaseUrl()).isEqualTo("https://gw.example/otel");
    assertThat(override.hasBaseUrl()).isTrue();
  }

  @Test
  void removingKeyFallsBackToEmpty() {
    Map<String, String> raw = new HashMap<>();
    raw.put("ops.arthas.tunnel_base_url", "   ");

    assertThat(OpsTunnelOverride.from(ServerMetadata.fromMap(raw)).hasBaseUrl()).isFalse();
    assertThat(OpsTunnelOverride.from(ServerMetadata.empty()).hasBaseUrl()).isFalse();
    assertThat(OpsTunnelOverride.from(null).hasBaseUrl()).isFalse();
  }

  @Test
  void removedEndpointKeyIsIgnored() {
    Map<String, String> raw = new HashMap<>();
    raw.put("ops.arthas.tunnel_endpoint", "ws://ops.example/v1/arthas/ws");

    assertThat(OpsTunnelOverride.from(ServerMetadata.fromMap(raw)).hasBaseUrl()).isFalse();
  }

  @Test
  void unknownKeysAreIgnored() {
    Map<String, String> raw = new HashMap<>();
    raw.put("some.future.key", "value");

    assertThat(ServerMetadata.fromMap(raw).getOpsTunnelBaseUrl()).isNull();
    assertThat(OpsTunnelOverride.from(ServerMetadata.fromMap(raw)).hasBaseUrl()).isFalse();
  }

  @Test
  void toStringDoesNotLeakValues() {
    OpsTunnelOverride override = OpsTunnelOverride.of("https://gw.example/otel");

    assertThat(override.toString())
        .doesNotContain("gw.example")
        .contains("hasBaseUrl=true");
  }
}
