/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.model;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * 运营端下发的 Arthas Tunnel base URL 覆盖（不可变值对象）。
 *
 * <p>把运营端配置收敛为一个快照，避免调用方维护多个可变字段产生中间态；同时作为
 * {@code resolveEffectiveEndpoint} 各重载的统一入参类型。
 *
 * <p>不保存、不打印任何凭据；{@link #toString()} 只输出字段存在性。
 */
public final class OpsTunnelOverride {

  private static final OpsTunnelOverride EMPTY = new OpsTunnelOverride(null);

  @Nullable private final String baseUrl;

  private OpsTunnelOverride(@Nullable String baseUrl) {
    this.baseUrl = baseUrl;
  }

  /** 无运营端覆盖。 */
  public static OpsTunnelOverride empty() {
    return EMPTY;
  }

  /** 由 base URL 构造；空串与空白串视为未配置。 */
  public static OpsTunnelOverride of(@Nullable String baseUrl) {
    String normalized = normalize(baseUrl);
    return normalized == null ? EMPTY : new OpsTunnelOverride(normalized);
  }

  /** 从服务端元数据快照投影；key 缺失或删除时回落到 empty。 */
  public static OpsTunnelOverride from(@Nullable ServerMetadata metadata) {
    return metadata == null ? EMPTY : of(metadata.getOpsTunnelBaseUrl());
  }

  /** 覆盖 scheme + host[:port] + 可选 path 前缀，最终拼接固定 tunnel path。 */
  @Nullable
  public String getBaseUrl() {
    return baseUrl;
  }

  /** base URL 命中，用于判定 {@code arthas.tunnel.source=ops}。 */
  public boolean hasBaseUrl() {
    return baseUrl != null;
  }

  @Nullable
  private static String normalize(@Nullable String value) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }
    return value.trim();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OpsTunnelOverride that = (OpsTunnelOverride) o;
    return Objects.equals(baseUrl, that.baseUrl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(baseUrl);
  }

  @Override
  public String toString() {
    return "OpsTunnelOverride{hasBaseUrl=" + hasBaseUrl() + "}";
  }
}
