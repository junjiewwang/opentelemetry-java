/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.config;

import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigurationException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * 解析 OTLP 标准 headers 配置，得到可直接发送的 HTTP 头。
 *
 * <p>语义与 SDK OTLP exporter 保持一致：{@code otel.exporter.otlp.headers} 按 W3C Baggage 的
 * key=value 列表解析，header 值必须做百分号解码。控制面不是 OTLP 信号，因此不消费
 * {@code otel.exporter.otlp.<signal>.headers} 这类信号级覆写变量。
 */
final class OtlpHeadersResolver {

  static final String HEADERS_PROPERTY = "otel.exporter.otlp.headers";
  static final String AUTHORIZATION = "Authorization";

  private OtlpHeadersResolver() {}

  /**
   * 返回解码后的、大小写不敏感去重的不可变 header map。
   *
   * @throws ConfigurationException header 值包含非法百分号编码时抛出；异常信息不含 header 值，
   *     避免凭据泄漏
   */
  static Map<String, String> resolve(ConfigProperties properties) {
    Map<String, String> raw = properties.getMap(HEADERS_PROPERTY);
    Map<String, String> decoded = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : raw.entrySet()) {
      String key = entry.getKey();
      if (key == null || key.isEmpty()) {
        continue;
      }
      String decodedValue;
      try {
        decodedValue = URLDecoder.decode(entry.getValue(), StandardCharsets.UTF_8.displayName());
      } catch (Exception e) {
        throw new ConfigurationException(
            "Cannot decode OTLP header value for header: " + key, e);
      }
      putIgnoreCase(decoded, key, decodedValue);
    }
    return Collections.unmodifiableMap(decoded);
  }

  /** 大小写不敏感地写入，重复键以最后出现者为准（与 ConfigProperties.getMap 语义一致）。 */
  static void putIgnoreCase(Map<String, String> headers, String key, String value) {
    for (String existing : new ArrayList<>(headers.keySet())) {
      if (existing.equalsIgnoreCase(key)) {
        headers.remove(existing);
      }
    }
    headers.put(key, value);
  }

  /** 大小写不敏感地查找 header 值。 */
  @Nullable
  static String find(Map<String, String> headers, String key) {
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(key)) {
        return entry.getValue();
      }
    }
    return null;
  }

  /** 去掉 Bearer scheme，得到不含前缀的纯 token（供 Arthas 链路复用）。 */
  @Nullable
  static String bearerToken(@Nullable String headerValue) {
    if (headerValue == null) {
      return null;
    }
    String value = headerValue.trim();
    if (value.toLowerCase(Locale.ROOT).startsWith("bearer ")) {
      return value.substring(7).trim();
    }
    return value;
  }
}
