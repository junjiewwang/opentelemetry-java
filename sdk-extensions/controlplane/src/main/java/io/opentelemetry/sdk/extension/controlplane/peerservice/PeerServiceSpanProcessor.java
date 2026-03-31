/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.peerservice;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.internal.ExtendedSpanProcessor;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * peer.service 自动填充处理器
 *
 * <p>实现 {@link ExtendedSpanProcessor}，根据 Span 类型自动填充 {@code peer.service} 属性，
 * 使可观测后端能正确绘制服务拓扑图。
 *
 * <h2>处理逻辑</h2>
 * <ul>
 *   <li><b>SERVER/CONSUMER（入方向）</b>：在 {@code onStart()} 阶段从 Baggage 中读取
 *       {@code caller.service.name}，直接设置为 {@code peer.service}（表示"谁调用了我"）。
 *       在 {@code onStart()} 中处理是因为此时可以访问 {@code parentContext}（Baggage），
 *       且 {@code ReadWriteSpan} 可写，无需引入临时属性。</li>
 *   <li><b>CLIENT/PRODUCER（出方向）</b>：在 {@code onEnding()} 阶段按优先级推断 peer.service：
 *     <ol>
 *       <li>已有 peer.service → 直接使用</li>
 *       <li>中间件类推断（DB: {@code db.system:server.address:server.port/db.name}，
 *           MQ: {@code messaging.system:server.address:server.port/destination}）</li>
 *       <li>Response Header（{@code x-otel-service-name}）</li>
 *       <li>service_mapping 匹配</li>
 *       <li>rpc.service</li>
 *     </ol>
 *     在 {@code onEnding()} 中处理是因为需要等待 Response Header 等属性就绪。
 *   </li>
 * </ul>
 */
public final class PeerServiceSpanProcessor implements ExtendedSpanProcessor {

  private static final Logger logger =
      Logger.getLogger(PeerServiceSpanProcessor.class.getName());

  // peer.service 属性键
  private static final AttributeKey<String> PEER_SERVICE = AttributeKey.stringKey("peer.service");

  // DB 相关语义约定属性键
  private static final AttributeKey<String> DB_SYSTEM = AttributeKey.stringKey("db.system");
  private static final AttributeKey<String> DB_NAME = AttributeKey.stringKey("db.name");
  // OTel 新版语义约定
  private static final AttributeKey<String> DB_NAMESPACE =
      AttributeKey.stringKey("db.namespace");

  // MQ 相关语义约定属性键
  private static final AttributeKey<String> MESSAGING_SYSTEM =
      AttributeKey.stringKey("messaging.system");
  private static final AttributeKey<String> MESSAGING_DESTINATION_NAME =
      AttributeKey.stringKey("messaging.destination.name");
  // 旧版语义约定
  private static final AttributeKey<String> MESSAGING_DESTINATION =
      AttributeKey.stringKey("messaging.destination");

  // 网络地址属性键（新版语义约定）
  private static final AttributeKey<String> SERVER_ADDRESS =
      AttributeKey.stringKey("server.address");
  private static final AttributeKey<Long> SERVER_PORT = AttributeKey.longKey("server.port");
  // 旧版语义约定
  private static final AttributeKey<String> NET_PEER_NAME =
      AttributeKey.stringKey("net.peer.name");
  private static final AttributeKey<Long> NET_PEER_PORT = AttributeKey.longKey("net.peer.port");


  private final PeerServiceResolverConfig config;

  /**
   * 构造函数
   *
   * @param config peer.service 解析配置
   */
  public PeerServiceSpanProcessor(PeerServiceResolverConfig config) {
    this.config = Objects.requireNonNull(config, "config");
    logger.log(Level.INFO, "PeerServiceSpanProcessor initialized with config: {0}", config);
  }

  // ===== SpanProcessor 接口方法 =====

  @Override
  public void onStart(Context parentContext, ReadWriteSpan span) {
    if (!config.isEnabled()) {
      return;
    }

    SpanKind kind = span.getKind();
    // 对于 SERVER/CONSUMER Span，直接在 onStart 中从 Baggage 读取 caller.service.name
    // 并设置 peer.service。在 onStart 中处理的原因：
    // 1. 此时可以访问 parentContext（Baggage）
    // 2. ReadWriteSpan 可写，可以直接设置 peer.service
    // 3. 无需引入临时属性（如 _internal.caller.service.name），避免属性清理问题
    //    （OTel SDK 的 SdkSpan.setAttribute(key, null) 会被忽略，无法真正删除属性）
    if (kind == SpanKind.SERVER || kind == SpanKind.CONSUMER) {
      // 如果已有 peer.service，直接返回
      String existingPeerService = span.getAttribute(PEER_SERVICE);
      if (existingPeerService != null && !existingPeerService.isEmpty()) {
        return;
      }

      Baggage baggage = Baggage.fromContext(parentContext);
      String callerService = baggage.getEntryValue(config.getBaggageKey());
      if (callerService != null && !callerService.isEmpty()) {
        span.setAttribute(PEER_SERVICE, callerService);
      }
    }
  }

  @Override
  public boolean isStartRequired() {
    return true;
  }

  @Override
  public void onEnd(ReadableSpan span) {
    // 不需要在 onEnd 中做任何处理，所有逻辑在 onEnding 中完成
  }

  @Override
  public boolean isEndRequired() {
    return false;
  }

  // ===== ExtendedSpanProcessor 接口方法 =====

  @Override
  public void onEnding(ReadWriteSpan span) {
    if (!config.isEnabled()) {
      return;
    }

    SpanKind kind = span.getKind();

    // 仅处理 CLIENT/PRODUCER（出方向）Span
    // SERVER/CONSUMER 已在 onStart() 中处理完毕
    if (kind != SpanKind.CLIENT && kind != SpanKind.PRODUCER) {
      return;
    }

    // 如果已有 peer.service，直接返回
    String existingPeerService = span.getAttribute(PEER_SERVICE);
    if (existingPeerService != null && !existingPeerService.isEmpty()) {
      return;
    }

    String peerService = resolveForOutbound(span);
    if (peerService != null && !peerService.isEmpty()) {
      span.setAttribute(PEER_SERVICE, peerService);
    }
  }

  @Override
  public boolean isOnEndingRequired() {
    return true;
  }

  // ===== 出方向解析（CLIENT/PRODUCER） =====

  /**
   * 解析出方向 Span 的 peer.service
   *
   * <p>按优先级依次尝试：
   * <ol>
   *   <li>中间件类推断（DB/MQ）</li>
   *   <li>Response Header</li>
   *   <li>service_mapping</li>
   *   <li>rpc.service</li>
   * </ol>
   */
  @Nullable
  private String resolveForOutbound(ReadWriteSpan span) {
    // 1. 中间件类推断（DB）
    String result = resolveFromDb(span);
    if (result != null) {
      return result;
    }

    // 2. 中间件类推断（MQ）
    result = resolveFromMessaging(span);
    if (result != null) {
      return result;
    }

    // 3. Response Header（精确值）
    result = resolveFromResponseHeader(span);
    if (result != null) {
      return result;
    }

    // 4. service_mapping
    result = resolveFromServiceMapping(span);
    if (result != null) {
      return result;
    }
    return "";
  }

  /**
   * 从 DB 相关属性推断 peer.service
   *
   * <p>格式: {@code db.system:server.address:server.port/db.name}
   * <br>示例: {@code mysql:10.0.0.1:3306/order_db}
   */
  @Nullable
  private static String resolveFromDb(ReadWriteSpan span) {
    String dbSystem = span.getAttribute(DB_SYSTEM);
    if (dbSystem == null || dbSystem.isEmpty()) {
      return null;
    }

    StringBuilder sb = new StringBuilder(dbSystem);

    // 追加连接地址
    String address = resolveServerAddress(span);
    if (address != null) {
      sb.append(':').append(address);
    }

    // 追加数据库名
    String dbName = span.getAttribute(DB_NAME);
    if (dbName == null || dbName.isEmpty()) {
      dbName = span.getAttribute(DB_NAMESPACE);
    }
    if (dbName != null && !dbName.isEmpty()) {
      sb.append('/').append(dbName);
    }

    return sb.toString();
  }

  /**
   * 从 MQ 相关属性推断 peer.service
   *
   * <p>格式: {@code messaging.system:server.address:server.port/destination}
   * <br>示例: {@code kafka:10.0.0.2:9092/order-topic}
   */
  @Nullable
  private static String resolveFromMessaging(ReadWriteSpan span) {
    String messagingSystem = span.getAttribute(MESSAGING_SYSTEM);
    if (messagingSystem == null || messagingSystem.isEmpty()) {
      return null;
    }

    StringBuilder sb = new StringBuilder(messagingSystem);

    // 追加连接地址
    String address = resolveServerAddress(span);
    if (address != null) {
      sb.append(':').append(address);
    }

    // 追加目标名称
    String destination = span.getAttribute(MESSAGING_DESTINATION_NAME);
    if (destination == null || destination.isEmpty()) {
      destination = span.getAttribute(MESSAGING_DESTINATION);
    }
    if (destination != null && !destination.isEmpty()) {
      sb.append('/').append(destination);
    }

    return sb.toString();
  }

  /**
   * 解析服务器地址
   *
   * <p>优先使用新版语义约定 {@code server.address:server.port}，
   * 降级到旧版 {@code net.peer.name:net.peer.port}
   *
   * @return 格式为 {@code host:port} 或 {@code host}，无地址返回 null
   */
  @Nullable
  private static String resolveServerAddress(ReadWriteSpan span) {
    // 优先新版语义约定
    String host = span.getAttribute(SERVER_ADDRESS);
    Long port = span.getAttribute(SERVER_PORT);

    // 降级到旧版语义约定
    if (host == null || host.isEmpty()) {
      host = span.getAttribute(NET_PEER_NAME);
      if (port == null) {
        port = span.getAttribute(NET_PEER_PORT);
      }
    }

    if (host == null || host.isEmpty()) {
      return null;
    }

    if (port != null && port > 0) {
      return host + ":" + port;
    }
    return host;
  }

  /**
   * 从 Response Header 中获取 peer.service
   *
   * <p>OTel HTTP instrumentation 捕获的 response header 存储为
   * {@code http.response.header.<header-name>} 属性，类型为 {@code List<String>}。
   * 本方法优先尝试 {@code List<String>} 类型读取，降级到 {@code String} 类型以兼容自定义场景。
   */
  @Nullable
  private String resolveFromResponseHeader(ReadWriteSpan span) {
    String attributeKeyName = config.getResponseHeaderAttributeKey();

    // 优先尝试 List<String> 类型（OTel HTTP instrumentation 标准行为）
    AttributeKey<List<String>> listHeaderKey = AttributeKey.stringArrayKey(attributeKeyName);
    List<String> headerValues = span.getAttribute(listHeaderKey);
    if (headerValues != null && !headerValues.isEmpty()) {
      String value = headerValues.get(headerValues.size() - 1);
      if (value != null && !value.isEmpty()) {
        // 移除临时的 response header 属性，避免导出到后端
        span.setAttribute(listHeaderKey, null);
        return value;
      }
    }

    // 降级到 String 类型（兼容自定义场景）
    AttributeKey<String> stringHeaderKey = AttributeKey.stringKey(attributeKeyName);
    String headerValue = span.getAttribute(stringHeaderKey);
    if (headerValue != null && !headerValue.isEmpty()) {
      span.setAttribute(stringHeaderKey, (String) null);
      return headerValue;
    }

    return null;
  }

  /**
   * 从 service_mapping 中匹配 peer.service
   *
   * <p>使用 {@code server.address:server.port} 作为 key 在映射表中查找
   */
  @Nullable
  private String resolveFromServiceMapping(ReadWriteSpan span) {
    if (config.getServiceMapping().isEmpty()) {
      return null;
    }

    String address = resolveServerAddress(span);
    if (address != null) {
      String mapped = config.resolveFromMapping(address);
      if (mapped != null) {
        return mapped;
      }
    }

    // 也尝试只用 host 匹配（不带端口）
    String host = span.getAttribute(SERVER_ADDRESS);
    if (host == null || host.isEmpty()) {
      host = span.getAttribute(NET_PEER_NAME);
    }
    if (host != null && !host.isEmpty()) {
      return config.resolveFromMapping(host);
    }

    return null;
  }
}
