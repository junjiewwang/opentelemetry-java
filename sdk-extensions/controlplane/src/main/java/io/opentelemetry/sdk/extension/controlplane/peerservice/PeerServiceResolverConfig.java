/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.peerservice;

import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * peer.service 自动解析配置
 *
 * <p>配置项：
 * <ul>
 *   <li>{@code otel.agent.peer.service.enabled} - 是否启用 peer.service 自动填充（默认 true）</li>
 *   <li>{@code otel.agent.peer.service.mapping} - 服务映射（格式: address1=service1,address2=service2）</li>
 *   <li>{@code otel.agent.peer.service.response.header.name} - 用于获取对端服务名的 Response Header 名称</li>
 *   <li>{@code otel.agent.peer.service.baggage.key} - Baggage 中传递调用方服务名的 key</li>
 * </ul>
 */
public final class PeerServiceResolverConfig {

  // 配置键常量
  private static final String PEER_SERVICE_ENABLED = "otel.agent.peer.service.enabled";
  private static final String PEER_SERVICE_MAPPING = "otel.agent.peer.service.mapping";
  private static final String PEER_SERVICE_RESPONSE_HEADER_NAME =
      "otel.agent.peer.service.response.header.name";
  private static final String PEER_SERVICE_BAGGAGE_KEY = "otel.agent.peer.service.baggage.key";

  // 默认值
  private static final boolean DEFAULT_ENABLED = true;
  private static final String DEFAULT_RESPONSE_HEADER_NAME = "x-otel-service-name";
  private static final String DEFAULT_BAGGAGE_KEY = "caller.service.name";

  private final boolean enabled;
  private final Map<String, String> serviceMapping;
  private final String responseHeaderName;
  private final String baggageKey;

  private PeerServiceResolverConfig(
      boolean enabled,
      Map<String, String> serviceMapping,
      String responseHeaderName,
      String baggageKey) {
    this.enabled = enabled;
    this.serviceMapping = Collections.unmodifiableMap(new LinkedHashMap<>(serviceMapping));
    this.responseHeaderName = responseHeaderName;
    this.baggageKey = baggageKey;
  }

  /**
   * 从 ConfigProperties 创建配置实例
   *
   * @param properties 配置属性
   * @return 配置实例
   */
  public static PeerServiceResolverConfig create(ConfigProperties properties) {
    boolean enabled = properties.getBoolean(PEER_SERVICE_ENABLED, DEFAULT_ENABLED);

    // 解析 service_mapping（格式: address1=service1,address2=service2）
    Map<String, String> mapping = new LinkedHashMap<>();
    String mappingStr = properties.getString(PEER_SERVICE_MAPPING);
    if (mappingStr != null && !mappingStr.isEmpty()) {
      for (String pair : mappingStr.split(",")) {
        String[] kv = pair.split("=", 2);
        if (kv.length == 2) {
          String key = kv[0].trim();
          String value = kv[1].trim();
          if (!key.isEmpty() && !value.isEmpty()) {
            mapping.put(key, value);
          }
        }
      }
    }

    String responseHeaderName =
        getStringOrDefault(
            properties.getString(PEER_SERVICE_RESPONSE_HEADER_NAME),
            DEFAULT_RESPONSE_HEADER_NAME);

    String baggageKey =
        getStringOrDefault(
            properties.getString(PEER_SERVICE_BAGGAGE_KEY), DEFAULT_BAGGAGE_KEY);

    return new PeerServiceResolverConfig(enabled, mapping, responseHeaderName, baggageKey);
  }

  /**
   * 创建默认配置实例（用于测试或无 ConfigProperties 场景）
   *
   * @return 默认配置实例
   */
  public static PeerServiceResolverConfig createDefault() {
    return new PeerServiceResolverConfig(
        DEFAULT_ENABLED,
        Collections.emptyMap(),
        DEFAULT_RESPONSE_HEADER_NAME,
        DEFAULT_BAGGAGE_KEY);
  }

  /** 是否启用 peer.service 自动填充 */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * 获取服务映射表
   *
   * <p>key 为地址（如 host:port），value 为服务名
   *
   * @return 不可变的服务映射表
   */
  public Map<String, String> getServiceMapping() {
    return serviceMapping;
  }

  /**
   * 获取用于获取对端服务名的 Response Header 名称
   *
   * @return Response Header 名称
   */
  public String getResponseHeaderName() {
    return responseHeaderName;
  }

  /**
   * 获取 Baggage 中传递调用方服务名的 key
   *
   * @return Baggage key
   */
  public String getBaggageKey() {
    return baggageKey;
  }

  /**
   * 获取 Response Header 对应的 Span 属性键
   *
   * <p>OTel 自动 instrumentation 捕获的 response header 会存储为
   * {@code http.response.header.<header-name>} 属性
   *
   * @return Span 属性键
   */
  public String getResponseHeaderAttributeKey() {
    return "http.response.header." + responseHeaderName;
  }

  /**
   * 根据地址在 service_mapping 中查找服务名
   *
   * @param address 地址（如 host:port）
   * @return 匹配的服务名，未找到返回 null
   */
  @Nullable
  public String resolveFromMapping(String address) {
    if (address == null || address.isEmpty()) {
      return null;
    }
    return serviceMapping.get(address);
  }

  private static String getStringOrDefault(@Nullable String value, String defaultValue) {
    return (value != null && !value.isEmpty()) ? value : defaultValue;
  }

  @Override
  public String toString() {
    return "PeerServiceResolverConfig{"
        + "enabled="
        + enabled
        + ", serviceMapping="
        + serviceMapping
        + ", responseHeaderName='"
        + responseHeaderName
        + '\''
        + ", baggageKey='"
        + baggageKey
        + '\''
        + '}';
  }
}
