/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.peerservice;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 调用方服务名 Baggage 注入处理器
 *
 * <p>在 CLIENT/PRODUCER 类型 Span 的 {@code onStart()} 阶段，将本服务的 {@code service.name}
 * 注入到当前 Context 的 Baggage 中。这样当 HTTP/gRPC Client 发起请求时，
 * W3CBaggagePropagator 会自动将 Baggage 序列化到请求 Header 中传递给下游服务。
 *
 * <p>下游服务的 SERVER/CONSUMER Span 可以通过 Baggage 获取调用方的服务名，
 * 从而在 {@link PeerServiceSpanProcessor} 中设置 {@code peer.service}。
 */
public final class CallerServiceBaggageSpanProcessor implements SpanProcessor {

  private static final Logger logger =
      Logger.getLogger(CallerServiceBaggageSpanProcessor.class.getName());

  private final String serviceName;
  private final String baggageKey;

  /**
   * 构造函数
   *
   * @param serviceName 本服务名称
   * @param baggageKey Baggage 中传递调用方服务名的 key
   */
  public CallerServiceBaggageSpanProcessor(String serviceName, String baggageKey) {
    this.serviceName = Objects.requireNonNull(serviceName, "serviceName");
    this.baggageKey = Objects.requireNonNull(baggageKey, "baggageKey");
    logger.log(
        Level.INFO,
        "CallerServiceBaggageSpanProcessor initialized with serviceName={0}, baggageKey={1}",
        new Object[] {serviceName, baggageKey});
  }

  @Override
  public void onStart(Context parentContext, ReadWriteSpan span) {
    if (serviceName.isEmpty()) {
      return;
    }

    SpanKind kind = span.getKind();
    // 仅对出方向 Span（CLIENT/PRODUCER）注入 Baggage
    if (kind != SpanKind.CLIENT && kind != SpanKind.PRODUCER) {
      return;
    }

    // 将 caller.service.name 注入到当前 Context 的 Baggage 中
    // W3CBaggagePropagator 会在 HTTP 请求发出时自动将 Baggage 序列化到 Header
    Baggage currentBaggage = Baggage.fromContext(parentContext);
    Baggage newBaggage =
        currentBaggage.toBuilder().put(baggageKey, serviceName).build();

    // 将新的 Baggage 设为当前 Context，使后续的 Propagator 能够读取到
    // 注意：这里使用 makeCurrent() 将 Baggage 注入到当前线程的 Context 中
    // Scope 的关闭由 Span 的 Context 生命周期管理
    @SuppressWarnings("MustBeClosedChecker")
    Scope unused = newBaggage.makeCurrent();
    // Scope 的关闭由 Span 的 Context 生命周期管理
    // 在 onStart 中 makeCurrent 后，当 Span 的 Scope 关闭时会恢复之前的 Context
  }

  @Override
  public boolean isStartRequired() {
    return true;
  }

  @Override
  public void onEnd(ReadableSpan span) {
    // 不需要在 onEnd 中做任何处理
  }

  @Override
  public boolean isEndRequired() {
    return false;
  }
}
