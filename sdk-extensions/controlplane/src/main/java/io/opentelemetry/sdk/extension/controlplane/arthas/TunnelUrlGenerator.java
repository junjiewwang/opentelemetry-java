/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import io.opentelemetry.sdk.extension.controlplane.core.model.OpsTunnelOverride;
import java.net.URI;
import java.util.Locale;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas Tunnel URL 生成器
 *
 * <p>封装所有关于 Tunnel 地址的推导、解析和拼接逻辑。
 * 这是一个纯函数式的工具类，不依赖任何可变状态，易于测试。
 *
 * <p><b>职责说明：</b>
 * <ul>
 *   <li>根据 OTLP Endpoint 和动态端口生成 WebSocket URL</li>
 *   <li>处理 HTTP/HTTPS 到 WS/WSS 的协议转换</li>
 *   <li>仅在原始 URL 显式带端口时应用服务端下发的 HTTP 端口覆盖</li>
 *   <li>支持运营端下发的 base URL 部分覆盖</li>
 * </ul>
 *
 * <p><b>设计原则：</b>
 * <ul>
 *   <li>单一职责：只负责 URL 生成，不管理状态</li>
 *   <li>纯函数：相同输入总是产生相同输出，无副作用</li>
 *   <li>可测试：不依赖外部状态，易于单元测试</li>
 *   <li>不在日志中输出完整 URL，避免泄漏 query 中的凭据</li>
 * </ul>
 */
public final class TunnelUrlGenerator {

  private static final Logger logger = Logger.getLogger(TunnelUrlGenerator.class.getName());

  /** Arthas WebSocket 路径常量 */
  private static final String DEFAULT_ARTHAS_WS_PATH = "/v1/arthas/ws";

  /** 单例实例 */
  private static final TunnelUrlGenerator INSTANCE = new TunnelUrlGenerator();

  private TunnelUrlGenerator() {}

  /**
   * 获取单例实例
   *
   * @return TunnelUrlGenerator 实例
   */
  public static TunnelUrlGenerator getInstance() {
    return INSTANCE;
  }

  /**
   * 计算最终有效的 Tunnel Endpoint（legacy 重载，无运营端覆盖）。
   *
   * @param explicitEndpoint 显式配置的端点（可为 null）
   * @param baseOtlpEndpoint 基础 OTLP 端点（可为 null）
   * @param serverHttpPort 服务端动态下发的 HTTP 端口（可为 null）
   * @return 有效的 WebSocket 连接地址，或 null
   */
  @Nullable
  public String resolveEffectiveEndpoint(
      @Nullable String explicitEndpoint,
      @Nullable String baseOtlpEndpoint,
      @Nullable Integer serverHttpPort) {
    return resolveCore(
        explicitEndpoint, baseOtlpEndpoint, serverHttpPort, OpsTunnelOverride.empty());
  }

  /**
   * 基于配置对象计算有效端点（便捷方法，无运营端覆盖）。
   *
   * @param config Arthas 配置
   * @param serverHttpPort 服务端动态下发的 HTTP 端口（可为 null）
   * @return 有效的 WebSocket 连接地址，或 null
   */
  @Nullable
  public String resolveEffectiveEndpoint(ArthasConfig config, @Nullable Integer serverHttpPort) {
    return resolveEffectiveEndpoint(config, serverHttpPort, OpsTunnelOverride.empty());
  }

  /**
   * 基于配置对象计算有效端点，额外接受运营端 base URL 覆盖快照。
   *
   * <p>优先级（从高到低）：
   * <ol>
   *   <li>本地 -D 显式配置（应急调试，永远最高）</li>
   *   <li>运营端下发的 tunnel base URL</li>
   *   <li>由 OTLP/control endpoint 与动态端口推导</li>
   * </ol>
   *
   * <p><b>参数顺序约定</b>：新增参数一律追加到参数列表末尾，使各重载之间的共有参数保持
   * 相同的相对顺序（ErrorProne {@code InconsistentOverloads} 要求）。
   *
   * @param config Arthas 配置
   * @param serverHttpPort 服务端动态下发的 HTTP 端口（可为 null）
   * @param opsOverride 运营端覆盖快照，不可为 null；无覆盖时传 {@link OpsTunnelOverride#empty()}
   * @return 有效的 WebSocket 连接地址，或 null
   */
  @Nullable
  public String resolveEffectiveEndpoint(
      ArthasConfig config,
      @Nullable Integer serverHttpPort,
      OpsTunnelOverride opsOverride) {
    return resolveCore(
        config.getExplicitTunnelEndpoint(),
        config.getBaseOtlpEndpoint(),
        serverHttpPort,
        opsOverride);
  }

  /** 唯一核心实现：所有重载最终委托到这里。 */
  @Nullable
  private String resolveCore(
      @Nullable String explicitEndpoint,
      @Nullable String baseOtlpEndpoint,
      @Nullable Integer serverHttpPort,
      @Nullable OpsTunnelOverride opsOverride) {

    // 1. 本地显式配置（最高优先级）
    if (explicitEndpoint != null && !explicitEndpoint.isEmpty()) {
      logger.log(Level.FINE, "Using explicit tunnel endpoint");
      return explicitEndpoint;
    }

    OpsTunnelOverride override =
        opsOverride == null ? OpsTunnelOverride.empty() : opsOverride;

    // 2. 运营端下发的 base URL（保留 path 前缀，不使用 serverHttpPort）
    if (override.hasBaseUrl()) {
      String fromBaseUrl = generateFromBaseUrl(override.getBaseUrl());
      if (fromBaseUrl != null) {
        logger.log(Level.FINE, "Using ops-managed tunnel base URL");
        return fromBaseUrl;
      }
      // 无效 base URL 视为未配置，安全回落到推导分支
      logger.log(
          Level.WARNING,
          "[TUNNEL] Invalid ops tunnel base URL, falling back to derived endpoint");
    }

    // 3. 基于 OTLP Endpoint 和动态端口生成
    return generateDefaultEndpoint(baseOtlpEndpoint, serverHttpPort);
  }

  /**
   * 纯函数：根据 OTLP Endpoint 和 HTTP 端口生成 Tunnel 地址
   *
   * <p>转换规则：
   * <ul>
   *   <li>http://host:port → ws://host:port/v1/arthas/ws</li>
   *   <li>https://host:port → wss://host:port/v1/arthas/ws</li>
   *   <li>仅当原始 endpoint 显式带端口时，才用 serverHttpPort 替换端口</li>
   *   <li>丢弃原始 path（path 前缀由 base URL 覆盖分支承载）</li>
   * </ul>
   *
   * @param baseOtlpEndpoint OTLP 端点（如 http://localhost:4317）
   * @param serverHttpPort 服务端下发的 HTTP 端口（用于覆盖 gRPC 端口）
   * @return 生成的 WebSocket 端点，或 null
   */
  @Nullable
  public String generateDefaultEndpoint(
      @Nullable String baseOtlpEndpoint, @Nullable Integer serverHttpPort) {

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

    // 仅当原始控制面 URL 显式带 port 时才做端口替换；经域名/Ingress
    // 的访问由 scheme 决定默认 80/443，不能注入监听端口 4318。
    if (serverHttpPort != null
        && serverHttpPort > 0
        && hasExplicitPort(baseOtlpEndpoint)) {
      endpoint = replacePort(endpoint, serverHttpPort);
    }

    return withTunnelPath(wsScheme + endpoint);
  }

  /**
   * 由运营端下发的 base URL 生成 Tunnel 地址。
   *
   * <p>保留 scheme 映射后的协议、host[:port] 与 path 前缀，在其后拼固定
   * {@value #DEFAULT_ARTHAS_WS_PATH}。不使用服务端下发的 {@code http_port}，
   * 以免 Ingress 域名场景被注入监听端口。
   *
   * <p>仅接受 http/https/ws/wss；拒绝 userinfo、空 host、query、fragment 以及
   * 已包含固定 tunnel path 的输入。任何无效输入返回 null，由调用方回落推导。
   *
   * @param baseUrl 运营端下发的 base URL
   * @return WebSocket 端点，或 null（输入无效）
   */
  @Nullable
  public String generateFromBaseUrl(@Nullable String baseUrl) {
    if (baseUrl == null || baseUrl.trim().isEmpty()) {
      return null;
    }

    URI uri;
    try {
      uri = URI.create(baseUrl.trim());
    } catch (IllegalArgumentException e) {
      logger.log(Level.FINE, "[TUNNEL] Unparsable ops tunnel base URL", e);
      return null;
    }

    String scheme = uri.getScheme();
    if (scheme == null
        || uri.getRawAuthority() == null
        || uri.getHost() == null
        || uri.getHost().isEmpty()
        || uri.getRawUserInfo() != null
        || uri.getRawQuery() != null
        || uri.getRawFragment() != null) {
      return null;
    }

    String wsScheme = toWebSocketScheme(scheme);
    if (wsScheme == null) {
      return null;
    }

    String path = uri.getRawPath();
    // 防御：避免把固定 tunnel path 重复拼接（collector 侧 validator 也应拦截）
    if (path != null && path.contains(DEFAULT_ARTHAS_WS_PATH)) {
      return null;
    }

    StringBuilder result =
        new StringBuilder(wsScheme).append("://").append(uri.getRawAuthority());
    if (path != null && !path.isEmpty()) {
      String prefix = path;
      while (prefix.endsWith("/")) {
        prefix = prefix.substring(0, prefix.length() - 1);
      }
      if (!prefix.isEmpty()) {
        result.append(prefix.startsWith("/") ? prefix : "/" + prefix);
      }
    }
    return withTunnelPath(result.toString());
  }

  /** scheme 白名单转换；未知协议返回 null。 */
  @Nullable
  private static String toWebSocketScheme(String scheme) {
    String lower = scheme.toLowerCase(Locale.ROOT);
    if ("http".equals(lower)) {
      return "ws";
    }
    if ("https".equals(lower)) {
      return "wss";
    }
    if ("ws".equals(lower) || "wss".equals(lower)) {
      return lower;
    }
    return null;
  }

  /** 去掉结尾斜杠后拼接固定 tunnel path，供两条生成路径共用。 */
  private static String withTunnelPath(String base) {
    String normalized = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
    return normalized + DEFAULT_ARTHAS_WS_PATH;
  }

  /** 判断原始控制面 URL 是否显式声明了端口。 */
  private static boolean hasExplicitPort(@Nullable String endpoint) {
    if (endpoint == null || endpoint.isEmpty()) {
      return false;
    }
    try {
      return URI.create(endpoint).getPort() >= 0;
    } catch (IllegalArgumentException e) {
      logger.log(Level.FINE, "[TUNNEL] Unparsable control endpoint for port detection", e);
      return false;
    }
  }

  /**
   * 替换或追加端口
   *
   * @param hostPort host:port 或 host
   * @param newPort 新端口
   * @return 替换后的 host:port
   */
  private static String replacePort(String hostPort, int newPort) {
    int colonIndex = hostPort.lastIndexOf(':');

    // 处理 IPv6 地址的情况（如 [::1]:8080）
    int bracketIndex = hostPort.lastIndexOf(']');
    if (bracketIndex > colonIndex) {
      // IPv6 地址没有端口
      return hostPort + ":" + newPort;
    }

    if (colonIndex > 0) {
      // 存在端口，替换之
      return hostPort.substring(0, colonIndex) + ":" + newPort;
    } else {
      // 不存在端口，追加之
      return hostPort + ":" + newPort;
    }
  }

  /**
   * 检查端点是否发生变更
   *
   * @param oldEndpoint 旧端点
   * @param newEndpoint 新端点
   * @return 是否发生变更
   */
  public boolean hasEndpointChanged(@Nullable String oldEndpoint, @Nullable String newEndpoint) {
    return !Objects.equals(oldEndpoint, newEndpoint);
  }
}
