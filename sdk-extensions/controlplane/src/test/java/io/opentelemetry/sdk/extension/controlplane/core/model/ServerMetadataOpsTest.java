/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ServerMetadataOpsTest {

  @Test
  void parsesOpsConfigKeys() {
    Map<String, String> raw = new HashMap<>();
    raw.put("http_port", "4318");
    raw.put("ops.arthas.tunnel_base_url", "https://gw.example/otel");
    raw.put("ops.revision", "7");

    ServerMetadata metadata = ServerMetadata.fromMap(raw);
    assertThat(metadata.getHttpPort()).isEqualTo(4318);
    assertThat(metadata.getOpsTunnelBaseUrl()).isEqualTo("https://gw.example/otel");
    assertThat(metadata.getOpsRevision()).isEqualTo("7");
  }

  @Test
  void absentOrBlankOpsKeysAreNull() {
    Map<String, String> raw = new HashMap<>();
    raw.put("ops.arthas.tunnel_base_url", "   ");
    ServerMetadata metadata = ServerMetadata.fromMap(raw);
    assertThat(metadata.getOpsTunnelBaseUrl()).isNull();
    assertThat(metadata.getOpsRevision()).isNull();

    assertThat(ServerMetadata.empty().getOpsTunnelBaseUrl()).isNull();
    assertThat(ServerMetadata.fromMap(null).getOpsRevision()).isNull();
  }

  @Test
  void removedEndpointKeyIsIgnored() {
    Map<String, String> raw = new HashMap<>();
    raw.put("ops.arthas.tunnel_endpoint", "ws://ops.example/v1/arthas/ws");

    ServerMetadata metadata = ServerMetadata.fromMap(raw);
    assertThat(OpsTunnelOverride.from(metadata).hasBaseUrl()).isFalse();
    assertThat(metadata.asMap()).containsKey("ops.arthas.tunnel_endpoint");
  }
}
