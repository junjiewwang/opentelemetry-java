/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.transport;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import okhttp3.Request;

/**
 * 把已解析的控制面 headers 应用到 OkHttp 请求。
 *
 * <p>协议必需头由各 Transport 在本类之后设置，这里同时做黑名单过滤，保证用户配置的
 * headers 不会覆盖传输层自身依赖的头。HTTP 与 gRPC 两种 Transport 共用本类，避免重复实现。
 */
final class RequestHeaders {

  private static final Set<String> RESERVED =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "accept-encoding",
                  "content-encoding",
                  "content-type",
                  "content-length",
                  "host",
                  "connection",
                  "te",
                  "grpc-encoding",
                  "grpc-accept-encoding")));

  private RequestHeaders() {}

  static void apply(Request.Builder builder, Map<String, String> headers) {
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      String name = entry.getKey();
      if (name == null
          || name.isEmpty()
          || RESERVED.contains(name.toLowerCase(Locale.ROOT))
          || entry.getValue() == null) {
        continue;
      }
      builder.header(name, entry.getValue());
    }
  }
}
