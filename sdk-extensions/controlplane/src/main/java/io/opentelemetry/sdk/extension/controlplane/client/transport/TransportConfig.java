/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.transport;

import java.time.Duration;
import javax.annotation.Nullable;

/**
 * 传输层配置
 *
 * <p>包含传输层（HTTP/gRPC）所需的所有配置参数。
 */
public final class TransportConfig {

  private final String baseUrl;
  @Nullable private final String authorizationHeader;
  private final Duration connectTimeout;
  private final Duration readTimeout;
  private final Duration writeTimeout;
  private final boolean compressionEnabled;

  private TransportConfig(Builder builder) {
    this.baseUrl = builder.baseUrl;
    this.authorizationHeader = builder.authorizationHeader;
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
   * 获取 Authorization Header
   *
   * @return Authorization Header，可能为 null
   */
  @Nullable
  public String getAuthorizationHeader() {
    return authorizationHeader;
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
    @Nullable private String authorizationHeader;
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
     * 设置 Authorization Header
     *
     * @param authorizationHeader Authorization Header
     * @return this
     */
    public Builder authorizationHeader(@Nullable String authorizationHeader) {
      this.authorizationHeader = authorizationHeader;
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
      return new TransportConfig(this);
    }
  }
}
