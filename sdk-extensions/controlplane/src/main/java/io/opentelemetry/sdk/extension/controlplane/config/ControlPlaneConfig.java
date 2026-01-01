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

  // 健康监控配置
  private static final String HEALTH_WINDOW_SIZE = "otel.agent.control.health.window.size";
  private static final String HEALTH_HEALTHY_THRESHOLD =
      "otel.agent.control.health.healthy.threshold";
  private static final String HEALTH_UNHEALTHY_THRESHOLD =
      "otel.agent.control.health.unhealthy.threshold";

  // 持久化配置
  private static final String STORAGE_DIR = "otel.agent.control.storage.dir";
  private static final String STORAGE_MAX_FILES = "otel.agent.control.storage.max.files";
  private static final String STORAGE_MAX_SIZE = "otel.agent.control.storage.max.size";

  // 任务结果配置
  private static final String TASK_RESULT_MAX_RETRY = "otel.agent.control.task.result.max.retry";
  private static final String TASK_RESULT_RETRY_INTERVAL =
      "otel.agent.control.task.result.retry.interval";
  private static final String TASK_RESULT_EXPIRATION = "otel.agent.control.task.result.expiration";
  private static final String TASK_RESULT_COMPRESSION_THRESHOLD =
      "otel.agent.control.task.result.compression.threshold";
  private static final String TASK_RESULT_CHUNKED_THRESHOLD =
      "otel.agent.control.task.result.chunked.threshold";
  private static final String TASK_RESULT_CHUNK_SIZE = "otel.agent.control.task.result.chunk.size";
  private static final String TASK_RESULT_MAX_SIZE = "otel.agent.control.task.result.max.size";

  // 重试配置
  private static final String RETRY_MAX_ATTEMPTS = "otel.agent.control.retry.max.attempts";
  private static final String RETRY_INITIAL_BACKOFF = "otel.agent.control.retry.initial.backoff";
  private static final String RETRY_MAX_BACKOFF = "otel.agent.control.retry.max.backoff";
  private static final String RETRY_BACKOFF_MULTIPLIER =
      "otel.agent.control.retry.backoff.multiplier";

  // 状态上报配置
  private static final String STATUS_INCLUDE_SYSTEM_RESOURCE =
      "otel.agent.control.status.include.system.resource";

  // ===== 默认值常量 =====
  private static final String DEFAULT_PROTOCOL = "grpc";
  private static final String DEFAULT_HTTP_BASE_PATH = "/v1/control";
  private static final Duration DEFAULT_LONG_POLL_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration DEFAULT_STATUS_REPORT_INTERVAL = Duration.ofSeconds(30);
  private static final int DEFAULT_HEALTH_WINDOW_SIZE = 100;
  private static final double DEFAULT_HEALTHY_THRESHOLD = 0.9;
  private static final double DEFAULT_UNHEALTHY_THRESHOLD = 0.5;
  private static final int DEFAULT_STORAGE_MAX_FILES = 100;
  private static final long DEFAULT_STORAGE_MAX_SIZE = 50 * 1024 * 1024L; // 50MB
  private static final int DEFAULT_TASK_RESULT_MAX_RETRY = 3;
  private static final Duration DEFAULT_TASK_RESULT_RETRY_INTERVAL = Duration.ofSeconds(60);
  private static final Duration DEFAULT_TASK_RESULT_EXPIRATION = Duration.ofHours(24);
  private static final long DEFAULT_COMPRESSION_THRESHOLD = 1024L; // 1KB
  private static final long DEFAULT_CHUNKED_THRESHOLD = 50 * 1024 * 1024L; // 50MB
  private static final long DEFAULT_CHUNK_SIZE = 10 * 1024 * 1024L; // 10MB
  private static final long DEFAULT_MAX_SIZE = 200 * 1024 * 1024L; // 200MB
  private static final int DEFAULT_RETRY_MAX_ATTEMPTS = 5;
  private static final Duration DEFAULT_RETRY_INITIAL_BACKOFF = Duration.ofSeconds(1);
  private static final Duration DEFAULT_RETRY_MAX_BACKOFF = Duration.ofSeconds(30);
  private static final double DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0;
  private static final boolean DEFAULT_INCLUDE_SYSTEM_RESOURCE = true;

  // ===== 配置字段 =====
  private final boolean enabled;
  private final String endpoint;
  private final String protocol;
  private final String httpBasePath;
  private final Duration longPollTimeout;
  private final Duration statusReportInterval;
  private final int healthWindowSize;
  private final double healthyThreshold;
  private final double unhealthyThreshold;
  private final String storageDir;
  private final int storageMaxFiles;
  private final long storageMaxSize;
  private final int taskResultMaxRetry;
  private final Duration taskResultRetryInterval;
  private final Duration taskResultExpiration;
  private final long compressionThreshold;
  private final long chunkedThreshold;
  private final long chunkSize;
  private final long maxSize;
  private final int retryMaxAttempts;
  private final Duration retryInitialBackoff;
  private final Duration retryMaxBackoff;
  private final double retryBackoffMultiplier;
  private final boolean includeSystemResource;
  @Nullable private final String headers;

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
    this.healthWindowSize = builder.healthWindowSize;
    this.healthyThreshold = builder.healthyThreshold;
    this.unhealthyThreshold = builder.unhealthyThreshold;
    this.storageDir = builder.storageDir;
    this.storageMaxFiles = builder.storageMaxFiles;
    this.storageMaxSize = builder.storageMaxSize;
    this.taskResultMaxRetry = builder.taskResultMaxRetry;
    this.taskResultRetryInterval = builder.taskResultRetryInterval;
    this.taskResultExpiration = builder.taskResultExpiration;
    this.compressionThreshold = builder.compressionThreshold;
    this.chunkedThreshold = builder.chunkedThreshold;
    this.chunkSize = builder.chunkSize;
    this.maxSize = builder.maxSize;
    this.retryMaxAttempts = builder.retryMaxAttempts;
    this.retryInitialBackoff = builder.retryInitialBackoff;
    this.retryMaxBackoff = builder.retryMaxBackoff;
    this.retryBackoffMultiplier = builder.retryBackoffMultiplier;
    this.includeSystemResource = builder.includeSystemResource;
    this.headers = builder.headers;

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

  public int getHealthWindowSize() {
    return healthWindowSize;
  }

  public double getHealthyThreshold() {
    return healthyThreshold;
  }

  public double getUnhealthyThreshold() {
    return unhealthyThreshold;
  }

  public String getStorageDir() {
    return storageDir;
  }

  public int getStorageMaxFiles() {
    return storageMaxFiles;
  }

  public long getStorageMaxSize() {
    return storageMaxSize;
  }

  public int getTaskResultMaxRetry() {
    return taskResultMaxRetry;
  }

  public Duration getTaskResultRetryInterval() {
    return taskResultRetryInterval;
  }

  public Duration getTaskResultExpiration() {
    return taskResultExpiration;
  }

  public long getCompressionThreshold() {
    return compressionThreshold;
  }

  public long getChunkedThreshold() {
    return chunkedThreshold;
  }

  public long getChunkSize() {
    return chunkSize;
  }

  public long getMaxSize() {
    return maxSize;
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
    private int healthWindowSize = DEFAULT_HEALTH_WINDOW_SIZE;
    private double healthyThreshold = DEFAULT_HEALTHY_THRESHOLD;
    private double unhealthyThreshold = DEFAULT_UNHEALTHY_THRESHOLD;
    private String storageDir = "/tmp/otel-agent/control";
    private int storageMaxFiles = DEFAULT_STORAGE_MAX_FILES;
    private long storageMaxSize = DEFAULT_STORAGE_MAX_SIZE;
    private int taskResultMaxRetry = DEFAULT_TASK_RESULT_MAX_RETRY;
    private Duration taskResultRetryInterval = DEFAULT_TASK_RESULT_RETRY_INTERVAL;
    private Duration taskResultExpiration = DEFAULT_TASK_RESULT_EXPIRATION;
    private long compressionThreshold = DEFAULT_COMPRESSION_THRESHOLD;
    private long chunkedThreshold = DEFAULT_CHUNKED_THRESHOLD;
    private long chunkSize = DEFAULT_CHUNK_SIZE;
    private long maxSize = DEFAULT_MAX_SIZE;
    private int retryMaxAttempts = DEFAULT_RETRY_MAX_ATTEMPTS;
    private Duration retryInitialBackoff = DEFAULT_RETRY_INITIAL_BACKOFF;
    private Duration retryMaxBackoff = DEFAULT_RETRY_MAX_BACKOFF;
    private double retryBackoffMultiplier = DEFAULT_RETRY_BACKOFF_MULTIPLIER;
    private boolean includeSystemResource = DEFAULT_INCLUDE_SYSTEM_RESOURCE;
    @Nullable private String headers;
    @Nullable private String resourceAttributes;

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

      Integer windowSize = properties.getInt(HEALTH_WINDOW_SIZE);
      if (windowSize != null) {
        this.healthWindowSize = windowSize;
      }

      Double healthy = properties.getDouble(HEALTH_HEALTHY_THRESHOLD);
      if (healthy != null) {
        this.healthyThreshold = healthy;
      }

      Double unhealthy = properties.getDouble(HEALTH_UNHEALTHY_THRESHOLD);
      if (unhealthy != null) {
        this.unhealthyThreshold = unhealthy;
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

      Integer maxRetry = properties.getInt(TASK_RESULT_MAX_RETRY);
      if (maxRetry != null) {
        this.taskResultMaxRetry = maxRetry;
      }

      Duration retryInterval = properties.getDuration(TASK_RESULT_RETRY_INTERVAL);
      if (retryInterval != null) {
        this.taskResultRetryInterval = retryInterval;
      }

      Duration expiration = properties.getDuration(TASK_RESULT_EXPIRATION);
      if (expiration != null) {
        this.taskResultExpiration = expiration;
      }

      Long compressionThresholdConfig =
          parseSizeProperty(properties.getString(TASK_RESULT_COMPRESSION_THRESHOLD));
      if (compressionThresholdConfig != null) {
        this.compressionThreshold = compressionThresholdConfig;
      }

      Long chunkedThresholdConfig =
          parseSizeProperty(properties.getString(TASK_RESULT_CHUNKED_THRESHOLD));
      if (chunkedThresholdConfig != null) {
        this.chunkedThreshold = chunkedThresholdConfig;
      }

      Long chunkSizeConfig = parseSizeProperty(properties.getString(TASK_RESULT_CHUNK_SIZE));
      if (chunkSizeConfig != null) {
        this.chunkSize = chunkSizeConfig;
      }

      Long maxSizeResultConfig = parseSizeProperty(properties.getString(TASK_RESULT_MAX_SIZE));
      if (maxSizeResultConfig != null) {
        this.maxSize = maxSizeResultConfig;
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

    public Builder setCompressionThreshold(long compressionThreshold) {
      this.compressionThreshold = compressionThreshold;
      return this;
    }

    public Builder setChunkedThreshold(long chunkedThreshold) {
      this.chunkedThreshold = chunkedThreshold;
      return this;
    }

    public Builder setChunkSize(long chunkSize) {
      this.chunkSize = chunkSize;
      return this;
    }

    public Builder setMaxSize(long maxSize) {
      this.maxSize = maxSize;
      return this;
    }

    public Builder setIncludeSystemResource(boolean includeSystemResource) {
      this.includeSystemResource = includeSystemResource;
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

    private void validate() {
      if (compressionThreshold >= chunkedThreshold) {
        throw new IllegalArgumentException(
            "compressionThreshold must be less than chunkedThreshold");
      }
      if (chunkedThreshold >= maxSize) {
        throw new IllegalArgumentException("chunkedThreshold must be less than maxSize");
      }
      if (chunkSize > chunkedThreshold) {
        throw new IllegalArgumentException(
            "chunkSize must be less than or equal to chunkedThreshold");
      }
      if (healthyThreshold <= unhealthyThreshold) {
        throw new IllegalArgumentException(
            "healthyThreshold must be greater than unhealthyThreshold");
      }
    }
  }
}
