/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;

/**
 * Arthas 状态信息 Model
 *
 * <p>封装 {@link ArthasIntegration#getStatusInfo()} 返回的所有状态字段，
 * 提供类型安全的访问方式，替代手动构建 {@code Map<String, Object>}。
 *
 * <p>使用 {@code @JsonProperty} 注解确保序列化 key 与字段定义一致，
 * 消除硬编码魔法字符串的风险。
 */
final class ArthasStatusInfo {

  @JsonProperty("enabled")
  private final boolean enabled;

  @JsonProperty("arthasState")
  private final String arthasState;

  @JsonProperty("tunnelStatus")
  private final String tunnelStatus;

  @JsonProperty("tunnelRegistered")
  private final boolean tunnelRegistered;

  @JsonProperty("tunnelReady")
  private final boolean tunnelReady;

  @JsonProperty("terminalBindable")
  private final boolean terminalBindable;

  @JsonProperty("terminalNotBindableReason")
  @Nullable
  private final String terminalNotBindableReason;

  @JsonProperty("uptimeMs")
  private final long uptimeMs;

  @JsonProperty("tunnelDisconnectedDurationMs")
  private final long tunnelDisconnectedDurationMs;

  @JsonProperty("currentEffectiveTunnelEndpoint")
  @Nullable
  private final String currentEffectiveTunnelEndpoint;

  @JsonProperty("currentServerHttpPort")
  @Nullable
  private final Integer currentServerHttpPort;

  @JsonProperty("environment")
  private final EnvironmentInfo environment;

  ArthasStatusInfo(
      boolean enabled,
      String arthasState,
      String tunnelStatus,
      boolean tunnelRegistered,
      boolean tunnelReady,
      boolean terminalBindable,
      @Nullable String terminalNotBindableReason,
      long uptimeMs,
      long tunnelDisconnectedDurationMs,
      @Nullable String currentEffectiveTunnelEndpoint,
      @Nullable Integer currentServerHttpPort,
      EnvironmentInfo environment) {
    this.enabled = enabled;
    this.arthasState = arthasState;
    this.tunnelStatus = tunnelStatus;
    this.tunnelRegistered = tunnelRegistered;
    this.tunnelReady = tunnelReady;
    this.terminalBindable = terminalBindable;
    this.terminalNotBindableReason = terminalNotBindableReason;
    this.uptimeMs = uptimeMs;
    this.tunnelDisconnectedDurationMs = tunnelDisconnectedDurationMs;
    this.currentEffectiveTunnelEndpoint = currentEffectiveTunnelEndpoint;
    this.currentServerHttpPort = currentServerHttpPort;
    this.environment = environment;
  }

  // ===== Getters =====

  public boolean isEnabled() {
    return enabled;
  }

  public String getArthasState() {
    return arthasState;
  }

  public String getTunnelStatus() {
    return tunnelStatus;
  }

  public boolean isTunnelRegistered() {
    return tunnelRegistered;
  }

  public boolean isTunnelReady() {
    return tunnelReady;
  }

  public boolean isTerminalBindable() {
    return terminalBindable;
  }

  @Nullable
  public String getTerminalNotBindableReason() {
    return terminalNotBindableReason;
  }

  public long getUptimeMs() {
    return uptimeMs;
  }

  public long getTunnelDisconnectedDurationMs() {
    return tunnelDisconnectedDurationMs;
  }

  @Nullable
  public String getCurrentEffectiveTunnelEndpoint() {
    return currentEffectiveTunnelEndpoint;
  }

  @Nullable
  public Integer getCurrentServerHttpPort() {
    return currentServerHttpPort;
  }

  public EnvironmentInfo getEnvironment() {
    return environment;
  }

  // ===== 嵌套环境信息 =====

  /**
   * 环境信息 Model
   *
   * <p>封装运行环境的操作系统、CPU 架构、libc 类型等信息。
   */
  static final class EnvironmentInfo {

    @JsonProperty("os")
    private final String os;

    @JsonProperty("arch")
    private final String arch;

    @JsonProperty("libc")
    private final String libc;

    @JsonProperty("jdkAvailable")
    private final boolean jdkAvailable;

    @JsonProperty("arthasSupported")
    private final boolean arthasSupported;

    EnvironmentInfo(String os, String arch, String libc, boolean jdkAvailable, boolean arthasSupported) {
      this.os = os;
      this.arch = arch;
      this.libc = libc;
      this.jdkAvailable = jdkAvailable;
      this.arthasSupported = arthasSupported;
    }

    public String getOs() {
      return os;
    }

    public String getArch() {
      return arch;
    }

    public String getLibc() {
      return libc;
    }

    public boolean isJdkAvailable() {
      return jdkAvailable;
    }

    public boolean isArthasSupported() {
      return arthasSupported;
    }
  }
}
