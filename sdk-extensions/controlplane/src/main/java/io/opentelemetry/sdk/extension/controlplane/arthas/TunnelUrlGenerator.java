/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

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
 *   <li>支持服务端下发的 HTTP 端口覆盖（解决 gRPC 场景下端口不匹配问题）</li>
 * </ul>
 *
 * <p><b>设计原则：</b>
 * <ul>
 *   <li>单一职责：只负责 URL 生成，不管理状态</li>
 *   <li>纯函数：相同输入总是产生相同输出，无副作用</li>
 *   <li>可测试：不依赖外部状态，易于单元测试</li>
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
   * 计算最终有效的 Tunnel Endpoint
   *
   * <p>优先级：
   * <ol>
   *   <li>显式配置的 tunnelEndpoint（用户手动指定）</li>
   *   <li>基于 OTLP endpoint 和动态端口自动生成</li>
   * </ol>
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

    // 1. 优先使用显式配置
    if (explicitEndpoint != null && !explicitEndpoint.isEmpty()) {
      logger.log(Level.FINE, "Using explicit tunnel endpoint: {0}", explicitEndpoint);
      return explicitEndpoint;
    }

    // 2. 基于 OTLP Endpoint 和动态端口生成
    return generateDefaultEndpoint(baseOtlpEndpoint, serverHttpPort);
  }

  /**
   * 基于配置对象计算有效端点（便捷方法）
   *
   * @param config Arthas 配置
   * @param serverHttpPort 服务端动态下发的 HTTP 端口（可为 null）
   * @return 有效的 WebSocket 连接地址，或 null
   */
  @Nullable
  public String resolveEffectiveEndpoint(ArthasConfig config, @Nullable Integer serverHttpPort) {
    return resolveEffectiveEndpoint(
        config.getExplicitTunnelEndpoint(),
        config.getBaseOtlpEndpoint(),
        serverHttpPort);
  }

  /**
   * 纯函数：根据 OTLP Endpoint 和 HTTP 端口生成 Tunnel 地址
   *
   * <p>转换规则：
   * <ul>
   *   <li>http://host:port → ws://host:port/v1/arthas/ws</li>
   *   <li>https://host:port → wss://host:port/v1/arthas/ws</li>
   *   <li>如果提供了 serverHttpPort，则替换原端口</li>
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

    // 如果服务端下发了 HTTP 端口，替换端口部分
    if (serverHttpPort != null && serverHttpPort > 0) {
      endpoint = replacePort(endpoint, serverHttpPort);
    }

    return wsScheme + endpoint + DEFAULT_ARTHAS_WS_PATH;
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

  /**
   * 从服务端元数据中解析 HTTP 端口
   *
   * @param httpPortStr 端口字符串
   * @return 解析后的端口，或 null（解析失败时）
   */
  @Nullable
  public Integer parseHttpPort(@Nullable String httpPortStr) {
    if (httpPortStr == null || httpPortStr.isEmpty()) {
      return null;
    }

    try {
      int port = Integer.parseInt(httpPortStr);
      if (port > 0 && port <= 65535) {
        return port;
      }
      logger.log(Level.WARNING, "Invalid port number: {0}", port);
      return null;
    } catch (NumberFormatException e) {
      logger.log(Level.WARNING, "Invalid http_port format: {0}", httpPortStr);
      return null;
    }
  }
}
