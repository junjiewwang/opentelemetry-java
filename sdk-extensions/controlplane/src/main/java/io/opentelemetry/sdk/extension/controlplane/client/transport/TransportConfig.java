/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.transport;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 传输层配置
 *
 * <p>包含传输层（HTTP/gRPC）所需的所有配置参数。
 */
public final class TransportConfig {

  private final String baseUrl;
  private final Map<String, String> headers;
  private final Duration connectTimeout;
  private final Duration readTimeout;
  private final Duration writeTimeout;
  private final boolean compressionEnabled;

  private TransportConfig(Builder builder) {
    this.baseUrl = builder.baseUrl;
    this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(builder.headers));
    this.connectTimeout = builder.connectTimeout;
    this.readTimeout = builder.readTimeout;
    this.writeTimeout = builder.writeTimeout;
    this.compressionEnabled = builder.compressionEnabled;
  }

  /**
   * 创建配置构建器
   *
   * @return 配置构建器
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * 获取基础 URL
   *
   * @return 基础 URL
   */
  public String getBaseUrl() {
    return baseUrl;
  }

  /**
   * 获取解码后的完整请求头，供 HTTP/gRPC 传输一次性应用。
   *
   * @return 不可变的请求头 map
   */
  public Map<String, String> getHeaders() {
    return headers;
  }

  /**
   * 获取连接超时
   *
   * @return 连接超时
   */
  public Duration getConnectTimeout() {
    return connectTimeout;
  }

  /**
   * 获取读取超时
   *
   * @return 读取超时
   */
  public Duration getReadTimeout() {
    return readTimeout;
  }

  /**
   * 获取写入超时
   *
   * @return 写入超时
   */
  public Duration getWriteTimeout() {
    return writeTimeout;
  }

  /**
   * 检查是否启用压缩
   *
   * @return 如果启用压缩返回 true
   */
  public boolean isCompressionEnabled() {
    return compressionEnabled;
  }

  /** 配置构建器 */
  public static final class Builder {
    private String baseUrl = "";
    private Map<String, String> headers = Collections.emptyMap();
    private Duration connectTimeout = Duration.ofSeconds(30);
    private Duration readTimeout = Duration.ofSeconds(70);
    private Duration writeTimeout = Duration.ofSeconds(30);
    private boolean compressionEnabled = true;

    private Builder() {}

    /**
     * 设置基础 URL
     *
     * @param baseUrl 基础 URL
     * @return this
     */
    public Builder baseUrl(String baseUrl) {
      this.baseUrl = baseUrl;
      return this;
    }

    /**
     * 设置完整请求头。
     *
     * <p>传入的 map 会被复制为不可变副本，运行期不可修改。
     *
     * @param headers 解码后的请求头
     * @return this
     */
    public Builder headers(Map<String, String> headers) {
      this.headers = Objects.requireNonNull(headers, "headers");
      return this;
    }

    /**
     * 设置连接超时
     *
     * @param connectTimeout 连接超时
     * @return this
     */
    public Builder connectTimeout(Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    /**
     * 设置读取超时
     *
     * @param readTimeout 读取超时
     * @return this
     */
    public Builder readTimeout(Duration readTimeout) {
      this.readTimeout = readTimeout;
      return this;
    }

    /**
     * 设置写入超时
     *
     * @param writeTimeout 写入超时
     * @return this
     */
    public Builder writeTimeout(Duration writeTimeout) {
      this.writeTimeout = writeTimeout;
      return this;
    }

    /**
     * 设置是否启用压缩
     *
     * @param compressionEnabled 是否启用压缩
     * @return this
     */
    public Builder compressionEnabled(boolean compressionEnabled) {
      this.compressionEnabled = compressionEnabled;
      return this;
    }

    /**
     * 构建配置
     *
     * @return 传输配置
     */
    public TransportConfig build() {
      if (baseUrl == null || baseUrl.isEmpty()) {
        throw new IllegalArgumentException("baseUrl must not be empty");
      }
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        if (entry.getKey() == null
            || entry.getKey().isEmpty()
            || entry.getValue() == null) {
          throw new IllegalArgumentException("Transport header key/value must not be null or empty");
        }
      }
      return new TransportConfig(this);
    }
  }
}
