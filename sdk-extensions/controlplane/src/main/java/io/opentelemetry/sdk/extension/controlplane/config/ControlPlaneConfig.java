/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.config;

import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 控制平面配置
 *
 * <p>从 OTLP 标准配置中复用 endpoint 和 protocol 配置，并扩展控制平面特定配置。
 */
public final class ControlPlaneConfig {

  private static final Logger logger = Logger.getLogger(ControlPlaneConfig.class.getName());

  // ===== 配置键常量 =====

  // OTLP 标准配置 (复用)
  private static final String OTLP_ENDPOINT = "otel.exporter.otlp.endpoint";
  private static final String OTLP_PROTOCOL = "otel.exporter.otlp.protocol";
  private static final String OTLP_HEADERS = "otel.exporter.otlp.headers";

  // Resource Attributes 配置
  private static final String RESOURCE_ATTRIBUTES = "otel.resource.attributes";
  private static final String SERVICE_AUTH_TOKEN_KEY = "token";

  // 控制平面基础配置
  private static final String CONTROL_ENABLED = "otel.agent.control.enabled";
  private static final String CONTROL_HTTP_BASE_PATH = "otel.agent.control.http.base.path";
  private static final String CONTROL_HTTP_LONG_POLL_TIMEOUT =
      "otel.agent.control.http.long.poll.timeout";

  // 轮询配置（configPollInterval 和 taskPollInterval 已由长轮询替代）
  private static final String STATUS_REPORT_INTERVAL = "otel.agent.control.status.report.interval";

  // 重试配置
  private static final String RETRY_MAX_ATTEMPTS = "otel.agent.control.retry.max.attempts";

  private static final String RETRY_INITIAL_BACKOFF = "otel.agent.control.retry.initial.backoff";
  private static final String RETRY_MAX_BACKOFF = "otel.agent.control.retry.max.backoff";
  private static final String RETRY_BACKOFF_MULTIPLIER =
      "otel.agent.control.retry.backoff.multiplier";

  // 状态上报配置
  private static final String STATUS_INCLUDE_SYSTEM_RESOURCE =
      "otel.agent.control.status.include.system.resource";

  // 调试配置
  private static final String OTEL_JAVAAGENT_DEBUG = "otel.javaagent.debug";

  // Arthas 配置
  private static final String ARTHAS_ENABLED = "otel.agent.control.arthas.enabled";
  private static final String ASYNC_PROFILER_ENABLED = "otel.agent.control.async-profiler.enabled";

  // 存储配置
  private static final String STORAGE_DIR = "otel.agent.control.storage.dir";
  private static final String STORAGE_MAX_FILES = "otel.agent.control.storage.max.files";
  private static final String STORAGE_MAX_SIZE = "otel.agent.control.storage.max.size";



  // ===== 默认值常量 =====
  private static final String DEFAULT_PROTOCOL = "grpc";
  private static final String DEFAULT_HTTP_BASE_PATH = "/v1/control";
  private static final Duration DEFAULT_LONG_POLL_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration DEFAULT_STATUS_REPORT_INTERVAL = Duration.ofSeconds(30);
  private static final int DEFAULT_RETRY_MAX_ATTEMPTS = 5;
  private static final Duration DEFAULT_RETRY_INITIAL_BACKOFF = Duration.ofSeconds(1);
  private static final Duration DEFAULT_RETRY_MAX_BACKOFF = Duration.ofSeconds(30);
  private static final double DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0;
  private static final boolean DEFAULT_INCLUDE_SYSTEM_RESOURCE = true;
  private static final boolean DEFAULT_DEBUG_ENABLED = false;
  private static final boolean DEFAULT_ARTHAS_ENABLED = true;
  private static final boolean DEFAULT_ASYNC_PROFILER_ENABLED = true;

  // 存储默认值
  private static final String DEFAULT_STORAGE_DIR = System.getProperty("java.io.tmpdir") + "/otel-controlplane";
  private static final int DEFAULT_STORAGE_MAX_FILES = 100;
  private static final long DEFAULT_STORAGE_MAX_SIZE = 100 * 1024 * 1024; // 100MB


  // ===== 配置字段 =====
  private final boolean enabled;
  private final String endpoint;
  private final String protocol;
  private final String httpBasePath;
  private final Duration longPollTimeout;
  private final Duration statusReportInterval;
  private final int retryMaxAttempts;
  private final Duration retryInitialBackoff;
  private final Duration retryMaxBackoff;
  private final double retryBackoffMultiplier;
  private final boolean includeSystemResource;
  private final boolean debugEnabled;
  private final boolean arthasEnabled;
  private final boolean asyncProfilerEnabled;
  @Nullable private final String headers;

  // 存储配置字段
  private final String storageDir;
  private final int storageMaxFiles;
  private final long storageMaxSize;

  // Auth Token (启动时一次性解析)
  @Nullable private final String authToken;
  @Nullable private final String authTokenSource;

  private ControlPlaneConfig(Builder builder) {
    this.enabled = builder.enabled;
    this.endpoint = builder.endpoint;
    this.protocol = builder.protocol;
    this.httpBasePath = builder.httpBasePath;
    this.longPollTimeout = builder.longPollTimeout;
    this.statusReportInterval = builder.statusReportInterval;
    this.retryMaxAttempts = builder.retryMaxAttempts;
    this.retryInitialBackoff = builder.retryInitialBackoff;
    this.retryMaxBackoff = builder.retryMaxBackoff;
    this.retryBackoffMultiplier = builder.retryBackoffMultiplier;
    this.includeSystemResource = builder.includeSystemResource;
    this.debugEnabled = builder.debugEnabled;
    this.arthasEnabled = builder.arthasEnabled;
    this.asyncProfilerEnabled = builder.asyncProfilerEnabled;
    this.headers = builder.headers;
    this.storageDir = builder.storageDir;
    this.storageMaxFiles = builder.storageMaxFiles;
    this.storageMaxSize = builder.storageMaxSize;
    // 一次性解析 AuthToken
    AuthTokenResult result = resolveAuthToken(builder.resourceAttributes, builder.headers);
    this.authToken = result.token;
    this.authTokenSource = result.source;

    if (this.authToken != null) {
      logger.log(Level.INFO, "Auth token configured from source: {0}", this.authTokenSource);
    }
  }

  /**
   * 按优先级解析 AuthToken
   *
   * <p>优先级:
   * <ol>
   *   <li>Resource Attributes 中的 token</li>
   *   <li>OTLP Headers 中的 Authorization</li>
   * </ol>
   *
   * @param resourceAttributes resource attributes 字符串
   * @param otlpHeaders OTLP headers 字符串
   * @return 解析结果
   */
  private static AuthTokenResult resolveAuthToken(
      @Nullable String resourceAttributes, @Nullable String otlpHeaders) {
    // 优先级 1: Resource Attributes 中的 token
    String token = extractTokenFromResourceAttributes(resourceAttributes);
    if (token != null && !token.isEmpty()) {
      return new AuthTokenResult(token, "resource.attributes[token]");
    }

    // 优先级 2: OTLP Headers 中的 Authorization
    token = extractTokenFromOtlpHeaders(otlpHeaders);
    if (token != null && !token.isEmpty()) {
      return new AuthTokenResult(token, "otel.exporter.otlp.headers[Authorization]");
    }

    return new AuthTokenResult(null, null);
  }

  /**
   * 从 Resource Attributes 提取 Token
   *
   * @param attributes resource attributes 字符串 (格式: key1=value1,key2=value2)
   * @return token 或 null
   */
  @Nullable
  private static String extractTokenFromResourceAttributes(@Nullable String attributes) {
    if (attributes == null || attributes.isEmpty()) {
      return null;
    }

    for (String pair : attributes.split(",")) {
      String[] kv = pair.split("=", 2);
      if (kv.length == 2 && SERVICE_AUTH_TOKEN_KEY.equals(kv[0].trim())) {
        return kv[1].trim();
      }
    }
    return null;
  }

  /**
   * 从 OTLP Headers 提取 Authorization Token
   *
   * @param headers OTLP headers 字符串 (格式: Header1=Value1,Header2=Value2)
   * @return token (不含 Bearer 前缀) 或 null
   */
  @Nullable
  private static String extractTokenFromOtlpHeaders(@Nullable String headers) {
    if (headers == null || headers.isEmpty()) {
      return null;
    }

    for (String pair : headers.split(",")) {
      String[] kv = pair.split("=", 2);
      if (kv.length == 2 && "Authorization".equalsIgnoreCase(kv[0].trim())) {
        String value = kv[1].trim();
        // 移除 "Bearer " 前缀（如果有）
        if (value.toLowerCase(Locale.ROOT).startsWith("bearer ")) {
          return value.substring(7).trim();
        }
        return value;
      }
    }
    return null;
  }

  /** Token 解析结果（内部类） */
  private static class AuthTokenResult {
    @Nullable final String token;
    @Nullable final String source;

    AuthTokenResult(@Nullable String token, @Nullable String source) {
      this.token = token;
      this.source = source;
    }
  }

  /**
   * 从 ConfigProperties 创建配置实例
   *
   * @param properties 配置属性
   * @return 控制平面配置
   */
  public static ControlPlaneConfig create(ConfigProperties properties) {
    return builder().fromConfigProperties(properties).build();
  }

  /**
   * 创建构建器
   *
   * @return 构建器实例
   */
  public static Builder builder() {
    return new Builder();
  }

  // ===== Getters =====

  public boolean isEnabled() {
    return enabled;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getProtocol() {
    return protocol;
  }

  public boolean isGrpc() {
    return "grpc".equalsIgnoreCase(protocol);
  }

  public boolean isHttpProtobuf() {
    return "http/protobuf".equalsIgnoreCase(protocol);
  }

  public String getHttpBasePath() {
    return httpBasePath;
  }

  public Duration getLongPollTimeout() {
    return longPollTimeout;
  }

  public Duration getStatusReportInterval() {
    return statusReportInterval;
  }

  public int getRetryMaxAttempts() {
    return retryMaxAttempts;
  }

  public Duration getRetryInitialBackoff() {
    return retryInitialBackoff;
  }

  public Duration getRetryMaxBackoff() {
    return retryMaxBackoff;
  }

  public double getRetryBackoffMultiplier() {
    return retryBackoffMultiplier;
  }

  @Nullable
  public String getHeaders() {
    return headers;
  }

  /**
   * 获取 Auth Token（不含 Bearer 前缀）
   *
   * @return token 或 null
   */
  @Nullable
  public String getAuthToken() {
    return authToken;
  }

  /**
   * 获取完整的 Authorization Header 值
   *
   * @return "Bearer {@literal <token>}" 或 null
   */
  @Nullable
  public String getAuthorizationHeader() {
    if (authToken != null && !authToken.isEmpty()) {
      return "Bearer " + authToken;
    }
    return null;
  }

  /**
   * 是否有有效的 Auth Token
   *
   * @return 是否有 token
   */
  public boolean hasAuthToken() {
    return authToken != null && !authToken.isEmpty();
  }

  /**
   * 获取 Auth Token 来源（用于日志/调试）
   *
   * @return token 来源描述 或 null
   */
  @Nullable
  public String getAuthTokenSource() {
    return authTokenSource;
  }

  /**
   * 是否在状态上报中包含系统资源信息
   *
   * @return 是否包含系统资源
   */
  public boolean isIncludeSystemResource() {
    return includeSystemResource;
  }

  /**
   * 是否启用调试模式
   *
   * @return 是否启用调试模式
   */
  public boolean isDebugEnabled() {
    return debugEnabled;
  }

  /**
   * 是否启用 Arthas 功能
   *
   * @return 是否启用 Arthas
   */
  public boolean isArthasEnabled() {
    return arthasEnabled;
  }

  /**
   * 是否启用 AsyncProfiler 功能
   *
   * @return 是否启用 AsyncProfiler
   */
  public boolean isAsyncProfilerEnabled() {
    return asyncProfilerEnabled;
  }

  /**
   * 获取存储目录
   *
   * @return 存储目录路径
   */
  public String getStorageDir() {
    return storageDir;
  }

  /**
   * 获取存储最大文件数
   *
   * @return 最大文件数
   */
  public int getStorageMaxFiles() {
    return storageMaxFiles;
  }

  /**
   * 获取存储最大大小
   *
   * @return 最大大小（字节）
   */
  public long getStorageMaxSize() {
    return storageMaxSize;
  }

  /**
   * 获取控制平面 URL
   *
   * @return 完整的控制平面 URL
   */
  public String getControlPlaneUrl() {
    String baseEndpoint = endpoint;
    if (baseEndpoint.endsWith("/")) {
      baseEndpoint = baseEndpoint.substring(0, baseEndpoint.length() - 1);
    }
    if (isGrpc()) {
      return baseEndpoint;
    }
    return baseEndpoint + httpBasePath;
  }

  /** 构建器 */
  public static final class Builder {
    private boolean enabled = true;
    private String endpoint = "http://localhost:4317";
    private String protocol = DEFAULT_PROTOCOL;
    private String httpBasePath = DEFAULT_HTTP_BASE_PATH;
    private Duration longPollTimeout = DEFAULT_LONG_POLL_TIMEOUT;
    private Duration statusReportInterval = DEFAULT_STATUS_REPORT_INTERVAL;
    private int retryMaxAttempts = DEFAULT_RETRY_MAX_ATTEMPTS;
    private Duration retryInitialBackoff = DEFAULT_RETRY_INITIAL_BACKOFF;
    private Duration retryMaxBackoff = DEFAULT_RETRY_MAX_BACKOFF;
    private double retryBackoffMultiplier = DEFAULT_RETRY_BACKOFF_MULTIPLIER;
    private boolean includeSystemResource = DEFAULT_INCLUDE_SYSTEM_RESOURCE;
    private boolean debugEnabled = DEFAULT_DEBUG_ENABLED;
    private boolean arthasEnabled = DEFAULT_ARTHAS_ENABLED;
    private boolean asyncProfilerEnabled = DEFAULT_ASYNC_PROFILER_ENABLED;
    @Nullable private String headers;
    @Nullable private String resourceAttributes;

    // 存储配置字段
    private String storageDir = DEFAULT_STORAGE_DIR;
    private int storageMaxFiles = DEFAULT_STORAGE_MAX_FILES;
    private long storageMaxSize = DEFAULT_STORAGE_MAX_SIZE;

    private Builder() {}

    /**
     * 从 ConfigProperties 加载配置
     *
     * @param properties 配置属性
     * @return 构建器
     */
    public Builder fromConfigProperties(ConfigProperties properties) {
      this.enabled = properties.getBoolean(CONTROL_ENABLED, true);

      // 复用 OTLP 配置
      String otlpEndpoint = properties.getString(OTLP_ENDPOINT);
      if (otlpEndpoint != null) {
        this.endpoint = otlpEndpoint;
      }

      String otlpProtocol = properties.getString(OTLP_PROTOCOL);
      if (otlpProtocol != null) {
        this.protocol = otlpProtocol;
      }

      this.headers = properties.getString(OTLP_HEADERS);

      // Resource Attributes（用于解析 auth token）
      this.resourceAttributes = properties.getString(RESOURCE_ATTRIBUTES);

      // 控制平面特定配置
      String basePath = properties.getString(CONTROL_HTTP_BASE_PATH);
      if (basePath != null) {
        this.httpBasePath = basePath;
      }

      Duration longPoll = properties.getDuration(CONTROL_HTTP_LONG_POLL_TIMEOUT);
      if (longPoll != null) {
        this.longPollTimeout = longPoll;
      }

      Duration statusReport = properties.getDuration(STATUS_REPORT_INTERVAL);
      if (statusReport != null) {
        this.statusReportInterval = statusReport;
      }

      String storage = properties.getString(STORAGE_DIR);
      if (storage != null) {
        this.storageDir = storage;
      }

      Integer maxFiles = properties.getInt(STORAGE_MAX_FILES);
      if (maxFiles != null) {
        this.storageMaxFiles = maxFiles;
      }

      Long maxSizeConfig = parseSizeProperty(properties.getString(STORAGE_MAX_SIZE));
      if (maxSizeConfig != null) {
        this.storageMaxSize = maxSizeConfig;
      }

      Integer maxAttempts = properties.getInt(RETRY_MAX_ATTEMPTS);
      if (maxAttempts != null) {
        this.retryMaxAttempts = maxAttempts;
      }

      Duration initialBackoff = properties.getDuration(RETRY_INITIAL_BACKOFF);
      if (initialBackoff != null) {
        this.retryInitialBackoff = initialBackoff;
      }

      Duration maxBackoff = properties.getDuration(RETRY_MAX_BACKOFF);
      if (maxBackoff != null) {
        this.retryMaxBackoff = maxBackoff;
      }

      Double multiplier = properties.getDouble(RETRY_BACKOFF_MULTIPLIER);
      if (multiplier != null) {
        this.retryBackoffMultiplier = multiplier;
      }

      // 状态上报配置
      this.includeSystemResource =
          properties.getBoolean(STATUS_INCLUDE_SYSTEM_RESOURCE, DEFAULT_INCLUDE_SYSTEM_RESOURCE);

      // 调试配置
      this.debugEnabled = properties.getBoolean(OTEL_JAVAAGENT_DEBUG, DEFAULT_DEBUG_ENABLED);

      // Arthas 配置
      this.arthasEnabled = properties.getBoolean(ARTHAS_ENABLED, DEFAULT_ARTHAS_ENABLED);

      // AsyncProfiler 配置
      this.asyncProfilerEnabled =
          properties.getBoolean(ASYNC_PROFILER_ENABLED, DEFAULT_ASYNC_PROFILER_ENABLED);

      return this;
    }

    /**
     * 解析大小配置 (支持 KB, MB, GB 后缀)
     *
     * @param value 配置值
     * @return 字节数
     */
    @Nullable
    private static Long parseSizeProperty(@Nullable String value) {
      if (value == null || value.isEmpty()) {
        return null;
      }

      value = value.trim().toUpperCase(Locale.ROOT);
      long multiplier = 1;

      if (value.endsWith("KB")) {
        multiplier = 1024L;
        value = value.substring(0, value.length() - 2);
      } else if (value.endsWith("MB")) {
        multiplier = 1024L * 1024;
        value = value.substring(0, value.length() - 2);
      } else if (value.endsWith("GB")) {
        multiplier = 1024L * 1024 * 1024;
        value = value.substring(0, value.length() - 2);
      } else if (value.endsWith("B")) {
        value = value.substring(0, value.length() - 1);
      }

      try {
        return Long.parseLong(value.trim()) * multiplier;
      } catch (NumberFormatException e) {
        return null;
      }
    }

    public Builder setEnabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public Builder setEndpoint(String endpoint) {
      this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
      return this;
    }

    public Builder setProtocol(String protocol) {
      this.protocol = Objects.requireNonNull(protocol, "protocol");
      return this;
    }

    public Builder setHttpBasePath(String httpBasePath) {
      this.httpBasePath = Objects.requireNonNull(httpBasePath, "httpBasePath");
      return this;
    }

    public Builder setLongPollTimeout(Duration longPollTimeout) {
      this.longPollTimeout = Objects.requireNonNull(longPollTimeout, "longPollTimeout");
      return this;
    }

    public Builder setStatusReportInterval(Duration statusReportInterval) {
      this.statusReportInterval =
          Objects.requireNonNull(statusReportInterval, "statusReportInterval");
      return this;
    }

    public Builder setIncludeSystemResource(boolean includeSystemResource) {
      this.includeSystemResource = includeSystemResource;
      return this;
    }

    public Builder setDebugEnabled(boolean debugEnabled) {
      this.debugEnabled = debugEnabled;
      return this;
    }

    public Builder setArthasEnabled(boolean arthasEnabled) {
      this.arthasEnabled = arthasEnabled;
      return this;
    }

    public Builder setAsyncProfilerEnabled(boolean asyncProfilerEnabled) {
      this.asyncProfilerEnabled = asyncProfilerEnabled;
      return this;
    }

    /**
     * 构建配置实例
     *
     * @return 配置实例
     */
    public ControlPlaneConfig build() {
      validate();
      return new ControlPlaneConfig(this);
    }

    private static void validate() {
      // 预留校验扩展点
    }
  }
}
