/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.transport;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** 端到端验证真实发出的 HTTP 请求头，而不只是配置对象的返回值。 */
class HttpTransportHeaderTest {

  private static final AtomicReference<String> AUTHORIZATION = new AtomicReference<>();
  private static final AtomicReference<String> TENANT_ID = new AtomicReference<>();

  @RegisterExtension
  static final ServerExtension server =
      new ServerExtension() {
        @Override
        protected void configure(ServerBuilder sb) {
          sb.service(
              "/v1/control/poll",
              (ctx, req) -> {
                AUTHORIZATION.set(req.headers().get("authorization"));
                TENANT_ID.set(req.headers().get("x-tenant-id"));
                return HttpResponse.of("ok");
              });
        }
      };

  @Test
  void sendsDecodedAuthorizationAndCustomHeaders() throws Exception {
    AUTHORIZATION.set(null);
    TENANT_ID.set(null);

    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("Authorization", "Bearer demo-token");
    headers.put("X-Tenant-Id", "tenant-1");

    TransportConfig config =
        TransportConfig.builder()
            .baseUrl(server.httpUri() + "/v1/control")
            .headers(headers)
            .build();

    HttpTransport transport = new HttpTransport(config);
    try {
      byte[] response =
          transport
              .sendUnary(Transport.Operation.UNIFIED_POLL, new byte[] {1}, 1000)
              .get(10, TimeUnit.SECONDS);
      assertThat(response).isNotEmpty();
      assertThat(AUTHORIZATION.get()).isEqualTo("Bearer demo-token");
      assertThat(TENANT_ID.get()).isEqualTo("tenant-1");
    } finally {
      transport.close();
    }
  }

  @Test
  void sendsNoAuthorizationWhenHeadersEmpty() throws Exception {
    AUTHORIZATION.set(null);

    TransportConfig config =
        TransportConfig.builder()
            .baseUrl(server.httpUri() + "/v1/control")
            .headers(Collections.emptyMap())
            .build();

    HttpTransport transport = new HttpTransport(config);
    try {
      transport
          .sendUnary(Transport.Operation.UNIFIED_POLL, new byte[] {1}, 1000)
          .get(10, TimeUnit.SECONDS);
      assertThat(AUTHORIZATION.get()).isNull();
    } finally {
      transport.close();
    }
  }
}
