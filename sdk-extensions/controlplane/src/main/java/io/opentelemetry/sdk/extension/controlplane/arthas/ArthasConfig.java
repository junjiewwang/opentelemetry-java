/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 集成配置
 *
 * <p>包含 Arthas Tunnel 连接、会话管理、生命周期控制等配置项。
 */
public final class ArthasConfig {

  private static final Logger logger = Logger.getLogger(ArthasConfig.class.getName());

  // ===== 配置键常量 =====

  // 基础配置
  private static final String ARTHAS_ENABLED = "otel.agent.arthas.enabled";
  private static final String ARTHAS_VERSION = "otel.agent.arthas.version";

  // 会话管理
  private static final String ARTHAS_MAX_SESSIONS_PER_AGENT =
      "otel.agent.arthas.max.sessions.per.agent";
  private static final String ARTHAS_SESSION_IDLE_TIMEOUT =
      "otel.agent.arthas.session.idle.timeout";
  private static final String ARTHAS_SESSION_MAX_DURATION =
      "otel.agent.arthas.session.max.duration";

  // 生命周期管理
  private static final String ARTHAS_IDLE_SHUTDOWN_DELAY =
      "otel.agent.arthas.idle.shutdown.delay";
  private static final String ARTHAS_MAX_RUNNING_DURATION =
      "otel.agent.arthas.max.running.duration";

  // Tunnel 连接
  private static final String ARTHAS_TUNNEL_ENDPOINT = "otel.agent.arthas.tunnel.endpoint";
  private static final String ARTHAS_TUNNEL_RECONNECT_INTERVAL =
      "otel.agent.arthas.tunnel.reconnect.interval";
  private static final String ARTHAS_TUNNEL_MAX_RECONNECT_ATTEMPTS =
      "otel.agent.arthas.tunnel.max.reconnect.attempts";
  private static final String ARTHAS_TUNNEL_CONNECT_TIMEOUT =
      "otel.agent.arthas.tunnel.connect.timeout";
  private static final String ARTHAS_TUNNEL_PING_INTERVAL =
      "otel.agent.arthas.tunnel.ping.interval";

  // 原生库配置
  private static final String ARTHAS_LIB_PATH = "otel.agent.arthas.lib.path";

  // 安全配置
  private static final String ARTHAS_DISABLED_COMMANDS = "otel.agent.arthas.disabled.commands";
  private static final String ARTHAS_COMMAND_TIMEOUT = "otel.agent.arthas.command.timeout";

  // 输出配置
  private static final String ARTHAS_OUTPUT_BUFFER_SIZE = "otel.agent.arthas.output.buffer.size";
  private static final String ARTHAS_OUTPUT_FLUSH_INTERVAL =
      "otel.agent.arthas.output.flush.interval";

  // 认证配置（从 ControlPlaneConfig 继承）
  private static final String ARTHAS_AUTH_TOKEN = "otel.agent.arthas.auth.token";

  // ===== 默认值 =====
  private static final boolean DEFAULT_ENABLED = false;
  private static final String DEFAULT_VERSION = "4.0.3";
  private static final int DEFAULT_MAX_SESSIONS_PER_AGENT = 2;
  private static final Duration DEFAULT_SESSION_IDLE_TIMEOUT = Duration.ofMinutes(30);
  private static final Duration DEFAULT_SESSION_MAX_DURATION = Duration.ofHours(2);
  private static final Duration DEFAULT_IDLE_SHUTDOWN_DELAY = Duration.ofMinutes(5);
  private static final Duration DEFAULT_MAX_RUNNING_DURATION = Duration.ofHours(4);
  private static final Duration DEFAULT_TUNNEL_RECONNECT_INTERVAL = Duration.ofSeconds(30);
  private static final int DEFAULT_TUNNEL_MAX_RECONNECT_ATTEMPTS = 0; // 0 表示无限重连
  private static final Duration DEFAULT_TUNNEL_CONNECT_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration DEFAULT_TUNNEL_PING_INTERVAL = Duration.ofSeconds(30);
  private static final Duration DEFAULT_COMMAND_TIMEOUT = Duration.ofMinutes(5);
  private static final int DEFAULT_OUTPUT_BUFFER_SIZE = 65536; // 64KB
  private static final Duration DEFAULT_OUTPUT_FLUSH_INTERVAL = Duration.ofMillis(50);

  // 默认禁用的危险命令
  private static final Set<String> DEFAULT_DISABLED_COMMANDS =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList("stop", "reset", "shutdown", "quit")));

  // Arthas WebSocket 路径常量
  private static final String DEFAULT_ARTHAS_WS_PATH = "/v1/arthas/ws";

  // ===== 配置字段 =====
  private final boolean enabled;
  private final String version;
  private final int maxSessionsPerAgent;
  private final Duration sessionIdleTimeout;
  private final Duration sessionMaxDuration;
  private final Duration idleShutdownDelay;
  private final Duration maxRunningDuration;
  @Nullable private final String tunnelEndpoint;
  private final Duration tunnelReconnectInterval;
  private final int tunnelMaxReconnectAttempts;
  private final Duration tunnelConnectTimeout;
  private final Duration tunnelPingInterval;
  @Nullable private final String libPath;
  private final Set<String> disabledCommands;
  private final Duration commandTimeout;
  private final int outputBufferSize;
  private final Duration outputFlushInterval;

  // 用于动态生成默认 Tunnel 端点的基础 endpoint
  @Nullable private final String baseOtlpEndpoint;

  // 认证 Token（用于 Tunnel 连接）
  @Nullable private final String authToken;

  private ArthasConfig(Builder builder) {
    this.enabled = builder.enabled;
    this.version = builder.version;
    this.maxSessionsPerAgent = builder.maxSessionsPerAgent;
    this.sessionIdleTimeout = builder.sessionIdleTimeout;
    this.sessionMaxDuration = builder.sessionMaxDuration;
    this.idleShutdownDelay = builder.idleShutdownDelay;
    this.maxRunningDuration = builder.maxRunningDuration;
    this.tunnelEndpoint = builder.tunnelEndpoint;
    this.tunnelReconnectInterval = builder.tunnelReconnectInterval;
    this.tunnelMaxReconnectAttempts = builder.tunnelMaxReconnectAttempts;
    this.tunnelConnectTimeout = builder.tunnelConnectTimeout;
    this.tunnelPingInterval = builder.tunnelPingInterval;
    this.libPath = builder.libPath;
    this.disabledCommands = Collections.unmodifiableSet(new HashSet<>(builder.disabledCommands));
    this.commandTimeout = builder.commandTimeout;
    this.outputBufferSize = builder.outputBufferSize;
    this.outputFlushInterval = builder.outputFlushInterval;
    this.baseOtlpEndpoint = builder.baseOtlpEndpoint;
    this.authToken = builder.authToken;

    if (this.enabled) {
      String effectiveTunnelEndpoint = getTunnelEndpoint();
      logger.log(
          Level.INFO,
          "Arthas integration enabled, version: {0}, maxSessions: {1}, tunnelEndpoint: {2}, effectiveTunnelEndpoint: {3}",
          new Object[] {this.version, this.maxSessionsPerAgent, this.tunnelEndpoint, effectiveTunnelEndpoint});
    }
  }

  /**
   * 从 ConfigProperties 创建配置实例
   *
   * @param properties 配置属性
   * @return Arthas 配置
   */
  public static ArthasConfig create(ConfigProperties properties) {
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

  /** 是否启用 Arthas 集成 */
  public boolean isEnabled() {
    return enabled;
  }

  /** 获取 Arthas 版本 */
  public String getVersion() {
    return version;
  }

  /** 获取每个 Agent 最大并发会话数 */
  public int getMaxSessionsPerAgent() {
    return maxSessionsPerAgent;
  }

  /** 获取会话空闲超时时间 */
  public Duration getSessionIdleTimeout() {
    return sessionIdleTimeout;
  }

  /** 获取会话最大存活时间 */
  public Duration getSessionMaxDuration() {
    return sessionMaxDuration;
  }

  /** 获取空闲关闭延迟时间 */
  public Duration getIdleShutdownDelay() {
    return idleShutdownDelay;
  }

  /** 获取最大运行时长 */
  public Duration getMaxRunningDuration() {
    return maxRunningDuration;
  }

  /**
   * 获取 Tunnel 端点地址
   *
   * <p>优先级：
   * <ol>
   *   <li>显式配置的 tunnelEndpoint</li>
   *   <li>基于 OTLP endpoint 自动生成的默认地址：ws:// + host + /v1/arthas/ws</li>
   * </ol>
   *
   * @return Tunnel 端点地址，可能为 null
   */
  @Nullable
  public String getTunnelEndpoint() {
    // 如果显式配置了 tunnel endpoint，直接返回
    if (tunnelEndpoint != null && !tunnelEndpoint.isEmpty()) {
      return tunnelEndpoint;
    }

    // 否则基于 OTLP endpoint 生成默认地址
    return generateDefaultTunnelEndpoint();
  }

  /**
   * 获取显式配置的 Tunnel 端点（不包含自动生成的默认值）
   *
   * @return 显式配置的端点，或 null
   */
  @Nullable
  public String getExplicitTunnelEndpoint() {
    return tunnelEndpoint;
  }

  /**
   * 基于 OTLP endpoint 生成默认的 Arthas Tunnel 端点
   *
   * <p>转换规则：http(s)://host:port -> ws(s)://host:port/v1/arthas/ws
   *
   * @return 默认端点地址，或 null
   */
  @Nullable
  private String generateDefaultTunnelEndpoint() {
    if (baseOtlpEndpoint == null || baseOtlpEndpoint.isEmpty()) {
      return null;
    }

    String endpoint = baseOtlpEndpoint;
    String wsScheme;

    // 根据 HTTP scheme 确定 WebSocket scheme
    if (endpoint.toLowerCase(Locale.ROOT).startsWith("https://")) {
      wsScheme = "wss://";
      endpoint = endpoint.substring(8); // 移除 "https://"
    } else if (endpoint.toLowerCase(Locale.ROOT).startsWith("http://")) {
      wsScheme = "ws://";
      endpoint = endpoint.substring(7); // 移除 "http://"
    } else {
      // 未知协议，默认使用 ws
      wsScheme = "ws://";
    }

    // 移除末尾的斜杠
    if (endpoint.endsWith("/")) {
      endpoint = endpoint.substring(0, endpoint.length() - 1);
    }

    // 移除路径部分（只保留 host:port）
    int pathIndex = endpoint.indexOf('/');
    if (pathIndex > 0) {
      endpoint = endpoint.substring(0, pathIndex);
    }

    return wsScheme + endpoint + DEFAULT_ARTHAS_WS_PATH;
  }

  /** 获取 Tunnel 重连间隔 */
  public Duration getTunnelReconnectInterval() {
    return tunnelReconnectInterval;
  }

  /** 获取 Tunnel 最大重连次数，0 表示无限重连 */
  public int getTunnelMaxReconnectAttempts() {
    return tunnelMaxReconnectAttempts;
  }

  /** 获取 Tunnel 连接超时时间 */
  public Duration getTunnelConnectTimeout() {
    return tunnelConnectTimeout;
  }

  /** 获取 Tunnel Ping 间隔 */
  public Duration getTunnelPingInterval() {
    return tunnelPingInterval;
  }

  /** 获取外部原生库路径 */
  @Nullable
  public String getLibPath() {
    return libPath;
  }

  /** 获取禁用的命令集合 */
  public Set<String> getDisabledCommands() {
    return disabledCommands;
  }

  /** 检查命令是否被禁用 */
  public boolean isCommandDisabled(String command) {
    return disabledCommands.contains(command.toLowerCase(Locale.ROOT));
  }

  /** 获取命令执行超时时间 */
  public Duration getCommandTimeout() {
    return commandTimeout;
  }

  /** 获取输出缓冲区大小 */
  public int getOutputBufferSize() {
    return outputBufferSize;
  }

  /** 获取输出刷新间隔 */
  public Duration getOutputFlushInterval() {
    return outputFlushInterval;
  }

  /** 是否配置了有效的 Tunnel 端点（包含显式配置和自动生成的） */
  public boolean hasTunnelEndpoint() {
    return getTunnelEndpoint() != null;
  }

  /** 是否显式配置了 Tunnel 端点 */
  public boolean hasExplicitTunnelEndpoint() {
    return tunnelEndpoint != null && !tunnelEndpoint.isEmpty();
  }

  /** 获取用于生成默认 Tunnel 端点的 OTLP endpoint */
  @Nullable
  public String getBaseOtlpEndpoint() {
    return baseOtlpEndpoint;
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

  @Override
  public String toString() {
    return "ArthasConfig{"
        + "enabled="
        + enabled
        + ", version='"
        + version
        + '\''
        + ", maxSessionsPerAgent="
        + maxSessionsPerAgent
        + ", sessionIdleTimeout="
        + sessionIdleTimeout
        + ", tunnelEndpoint='"
        + tunnelEndpoint
        + '\''
        + '}';
  }

  /** Builder for {@link ArthasConfig}. */
  public static final class Builder {
    private boolean enabled = DEFAULT_ENABLED;
    private String version = DEFAULT_VERSION;
    private int maxSessionsPerAgent = DEFAULT_MAX_SESSIONS_PER_AGENT;
    private Duration sessionIdleTimeout = DEFAULT_SESSION_IDLE_TIMEOUT;
    private Duration sessionMaxDuration = DEFAULT_SESSION_MAX_DURATION;
    private Duration idleShutdownDelay = DEFAULT_IDLE_SHUTDOWN_DELAY;
    private Duration maxRunningDuration = DEFAULT_MAX_RUNNING_DURATION;
    @Nullable private String tunnelEndpoint;
    private Duration tunnelReconnectInterval = DEFAULT_TUNNEL_RECONNECT_INTERVAL;
    private int tunnelMaxReconnectAttempts = DEFAULT_TUNNEL_MAX_RECONNECT_ATTEMPTS;
    private Duration tunnelConnectTimeout = DEFAULT_TUNNEL_CONNECT_TIMEOUT;
    private Duration tunnelPingInterval = DEFAULT_TUNNEL_PING_INTERVAL;
    @Nullable private String libPath;
    @Nullable private String baseOtlpEndpoint;
    @Nullable private String authToken;
    private Set<String> disabledCommands = new HashSet<>(DEFAULT_DISABLED_COMMANDS);
    private Duration commandTimeout = DEFAULT_COMMAND_TIMEOUT;
    private int outputBufferSize = DEFAULT_OUTPUT_BUFFER_SIZE;
    private Duration outputFlushInterval = DEFAULT_OUTPUT_FLUSH_INTERVAL;

    private Builder() {}

    /**
     * 从 ConfigProperties 加载配置
     *
     * @param properties 配置属性
     * @return 构建器
     */
    public Builder fromConfigProperties(ConfigProperties properties) {
      this.enabled = properties.getBoolean(ARTHAS_ENABLED, DEFAULT_ENABLED);

      String ver = properties.getString(ARTHAS_VERSION);
      if (ver != null && !ver.isEmpty()) {
        this.version = ver;
      }

      Integer maxSessions = properties.getInt(ARTHAS_MAX_SESSIONS_PER_AGENT);
      if (maxSessions != null) {
        this.maxSessionsPerAgent = maxSessions;
      }

      Duration idleTimeout = properties.getDuration(ARTHAS_SESSION_IDLE_TIMEOUT);
      if (idleTimeout != null) {
        this.sessionIdleTimeout = idleTimeout;
      }

      Duration maxDuration = properties.getDuration(ARTHAS_SESSION_MAX_DURATION);
      if (maxDuration != null) {
        this.sessionMaxDuration = maxDuration;
      }

      Duration idleShutdown = properties.getDuration(ARTHAS_IDLE_SHUTDOWN_DELAY);
      if (idleShutdown != null) {
        this.idleShutdownDelay = idleShutdown;
      }

      Duration maxRunning = properties.getDuration(ARTHAS_MAX_RUNNING_DURATION);
      if (maxRunning != null) {
        this.maxRunningDuration = maxRunning;
      }

      String endpoint = properties.getString(ARTHAS_TUNNEL_ENDPOINT);
      if (endpoint != null && !endpoint.isEmpty()) {
        this.tunnelEndpoint = endpoint;
      }

      Duration reconnect = properties.getDuration(ARTHAS_TUNNEL_RECONNECT_INTERVAL);
      if (reconnect != null) {
        this.tunnelReconnectInterval = reconnect;
      }

      Integer maxReconnect = properties.getInt(ARTHAS_TUNNEL_MAX_RECONNECT_ATTEMPTS);
      if (maxReconnect != null) {
        this.tunnelMaxReconnectAttempts = maxReconnect;
      }

      Duration connectTimeout = properties.getDuration(ARTHAS_TUNNEL_CONNECT_TIMEOUT);
      if (connectTimeout != null) {
        this.tunnelConnectTimeout = connectTimeout;
      }

      Duration pingInterval = properties.getDuration(ARTHAS_TUNNEL_PING_INTERVAL);
      if (pingInterval != null) {
        this.tunnelPingInterval = pingInterval;
      }

      String lib = properties.getString(ARTHAS_LIB_PATH);
      if (lib != null && !lib.isEmpty()) {
        this.libPath = lib;
      }

      String disabledCmds = properties.getString(ARTHAS_DISABLED_COMMANDS);
      if (disabledCmds != null && !disabledCmds.isEmpty()) {
        this.disabledCommands = new HashSet<>(DEFAULT_DISABLED_COMMANDS);
        for (String cmd : disabledCmds.split(",")) {
          String trimmed = cmd.trim().toLowerCase(Locale.ROOT);
          if (!trimmed.isEmpty()) {
            this.disabledCommands.add(trimmed);
          }
        }
      }

      Duration cmdTimeout = properties.getDuration(ARTHAS_COMMAND_TIMEOUT);
      if (cmdTimeout != null) {
        this.commandTimeout = cmdTimeout;
      }

      Integer bufferSize = properties.getInt(ARTHAS_OUTPUT_BUFFER_SIZE);
      if (bufferSize != null) {
        this.outputBufferSize = bufferSize;
      }

      Duration flushInterval = properties.getDuration(ARTHAS_OUTPUT_FLUSH_INTERVAL);
      if (flushInterval != null) {
        this.outputFlushInterval = flushInterval;
      }

      // 认证 Token（可选，通常从 ControlPlaneConfig 继承）
      String token = properties.getString(ARTHAS_AUTH_TOKEN);
      if (token != null && !token.isEmpty()) {
        this.authToken = token;
      }

      return this;
    }

    public Builder setEnabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public Builder setVersion(String version) {
      this.version = Objects.requireNonNull(version, "version");
      return this;
    }

    public Builder setMaxSessionsPerAgent(int maxSessionsPerAgent) {
      if (maxSessionsPerAgent < 1) {
        throw new IllegalArgumentException("maxSessionsPerAgent must be >= 1");
      }
      this.maxSessionsPerAgent = maxSessionsPerAgent;
      return this;
    }

    public Builder setSessionIdleTimeout(Duration sessionIdleTimeout) {
      this.sessionIdleTimeout = Objects.requireNonNull(sessionIdleTimeout, "sessionIdleTimeout");
      return this;
    }

    public Builder setSessionMaxDuration(Duration sessionMaxDuration) {
      this.sessionMaxDuration = Objects.requireNonNull(sessionMaxDuration, "sessionMaxDuration");
      return this;
    }

    public Builder setIdleShutdownDelay(Duration idleShutdownDelay) {
      this.idleShutdownDelay = Objects.requireNonNull(idleShutdownDelay, "idleShutdownDelay");
      return this;
    }

    public Builder setMaxRunningDuration(Duration maxRunningDuration) {
      this.maxRunningDuration = Objects.requireNonNull(maxRunningDuration, "maxRunningDuration");
      return this;
    }

    public Builder setTunnelEndpoint(@Nullable String tunnelEndpoint) {
      this.tunnelEndpoint = tunnelEndpoint;
      return this;
    }

    public Builder setTunnelReconnectInterval(Duration tunnelReconnectInterval) {
      this.tunnelReconnectInterval =
          Objects.requireNonNull(tunnelReconnectInterval, "tunnelReconnectInterval");
      return this;
    }

    public Builder setTunnelMaxReconnectAttempts(int tunnelMaxReconnectAttempts) {
      this.tunnelMaxReconnectAttempts = tunnelMaxReconnectAttempts;
      return this;
    }

    public Builder setTunnelConnectTimeout(Duration tunnelConnectTimeout) {
      this.tunnelConnectTimeout =
          Objects.requireNonNull(tunnelConnectTimeout, "tunnelConnectTimeout");
      return this;
    }

    public Builder setTunnelPingInterval(Duration tunnelPingInterval) {
      this.tunnelPingInterval = Objects.requireNonNull(tunnelPingInterval, "tunnelPingInterval");
      return this;
    }

    public Builder setLibPath(@Nullable String libPath) {
      this.libPath = libPath;
      return this;
    }

    /**
     * 设置基础 OTLP endpoint（用于生成默认的 Arthas Tunnel 端点）
     *
     * @param baseOtlpEndpoint OTLP endpoint 地址
     * @return 构建器
     */
    public Builder setBaseOtlpEndpoint(@Nullable String baseOtlpEndpoint) {
      this.baseOtlpEndpoint = baseOtlpEndpoint;
      return this;
    }

    /**
     * 设置认证 Token
     *
     * @param authToken 认证 Token（不含 Bearer 前缀）
     * @return 构建器
     */
    public Builder setAuthToken(@Nullable String authToken) {
      this.authToken = authToken;
      return this;
    }

    public Builder setDisabledCommands(Set<String> disabledCommands) {
      this.disabledCommands = new HashSet<>(disabledCommands);
      return this;
    }

    public Builder addDisabledCommand(String command) {
      this.disabledCommands.add(command.toLowerCase(Locale.ROOT));
      return this;
    }

    public Builder setCommandTimeout(Duration commandTimeout) {
      this.commandTimeout = Objects.requireNonNull(commandTimeout, "commandTimeout");
      return this;
    }

    public Builder setOutputBufferSize(int outputBufferSize) {
      if (outputBufferSize < 1024) {
        throw new IllegalArgumentException("outputBufferSize must be >= 1024");
      }
      this.outputBufferSize = outputBufferSize;
      return this;
    }

    public Builder setOutputFlushInterval(Duration outputFlushInterval) {
      this.outputFlushInterval =
          Objects.requireNonNull(outputFlushInterval, "outputFlushInterval");
      return this;
    }

    /**
     * 构建配置实例
     *
     * @return 配置实例
     */
    public ArthasConfig build() {
      validate();
      return new ArthasConfig(this);
    }

    private void validate() {
      if (!enabled) {
        return;
      }

      // 注意：tunnelEndpoint 是“显式配置值”。即使它为 null，只要 baseOtlpEndpoint 可用，
      // getTunnelEndpoint() 仍可生成默认 tunnel ws 地址。
      if (tunnelEndpoint == null && baseOtlpEndpoint == null) {
        logger.log(
            Level.WARNING,
            "Arthas is enabled but neither tunnel endpoint nor base OTLP endpoint is configured. "
                + "Arthas will not be able to connect to server.");
      } else if (tunnelEndpoint == null) {
        // 仅提示会基于 OTLP endpoint 推导，避免误导用户以为 tunnel endpoint 一定为 null
        logger.log(
            Level.FINE,
            "Arthas tunnel endpoint not explicitly configured, will derive default from base OTLP endpoint: {0}",
            baseOtlpEndpoint);
      }
    }
  }
}
