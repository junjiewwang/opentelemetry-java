/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * 服务端元数据值对象 (Value Object)
 *
 * <p>职责：
 * 1. 封装原始的 key-value 数据
 * 2. 提供类型安全的访问接口
 * 3. 集中管理 Key 常量和解析逻辑
 *
 * <p>该类是不可变的。
 */
public final class ServerMetadata {

  // 1. 集中定义 Key 常量
  private static final String KEY_HTTP_PORT = "http_port";

  private final Map<String, String> rawMetadata;

  // 2. 预解析常用字段
  @Nullable private final Integer httpPort;

  private ServerMetadata(@Nullable Map<String, String> rawMetadata) {
    this.rawMetadata = rawMetadata != null
        ? Collections.unmodifiableMap(new HashMap<>(rawMetadata))
        : Collections.emptyMap();

    // 在构造时进行解析，确保后续访问的高效和一致性
    this.httpPort = parseInteger(this.rawMetadata.get(KEY_HTTP_PORT));
  }

  // 3. 静态工厂方法
  public static ServerMetadata fromMap(@Nullable Map<String, String> map) {
    return new ServerMetadata(map);
  }

  public static ServerMetadata empty() {
    return new ServerMetadata(Collections.emptyMap());
  }

  // 4. 强类型访问器
  @Nullable
  public Integer getHttpPort() {
    return httpPort;
  }

  /**
   * 获取原始 Map (仅用于调试或透传)
   */
  public Map<String, String> asMap() {
    return rawMetadata;
  }

  // 5. 内部解析工具
  @Nullable
  private static Integer parseInteger(@Nullable String value) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ServerMetadata that = (ServerMetadata) o;
    return Objects.equals(rawMetadata, that.rawMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rawMetadata);
  }

  @Override
  public String toString() {
    return "ServerMetadata{" + rawMetadata + "}";
  }
}
