/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import okhttp3.Request;
import org.junit.jupiter.api.Test;

class RequestHeadersTest {

  @Test
  void appliesAllHeadersAndProtectsReservedOnes() {
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("Authorization", "Bearer demo-token");
    headers.put("X-Tenant-Id", "tenant,1");
    headers.put("Accept-Encoding", "br");
    headers.put("content-type", "text/plain");

    Request.Builder builder =
        new Request.Builder().url("http://localhost:4318/v1/control/poll");
    RequestHeaders.apply(builder, headers);
    // 模拟 Transport 在用户 headers 之后设置协议必需头
    builder.header("Accept-Encoding", "gzip");

    Request request = builder.build();
    assertThat(request.header("Authorization")).isEqualTo("Bearer demo-token");
    assertThat(request.header("X-Tenant-Id")).isEqualTo("tenant,1");
    assertThat(request.header("Accept-Encoding")).isEqualTo("gzip");
    assertThat(request.header("content-type")).isNull();
  }

  @Test
  void emptyHeadersProducesNoAuthorization() {
    Request.Builder builder = new Request.Builder().url("http://localhost:4318/poll");
    RequestHeaders.apply(builder, Collections.emptyMap());

    assertThat(builder.build().header("Authorization")).isNull();
  }
}
