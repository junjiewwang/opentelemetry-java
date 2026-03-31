/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.peerservice;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.util.Collection;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 调用方服务名 Baggage 传播器（装饰器模式）
 *
 * <p>包装原始的 {@link TextMapPropagator}（通常是 {@code W3CBaggagePropagator} 的组合），
 * 在 {@code inject()} 阶段将本服务的 {@code service.name} 追加到 Context 的 Baggage 中，
 * 确保 HTTP/gRPC 请求发出时，Baggage Header 中包含 {@code caller.service.name}。
 *
 * <p>这样下游服务的 SERVER/CONSUMER Span 可以通过 Baggage 获取调用方的服务名，
 * 从而在 {@link PeerServiceSpanProcessor} 中设置 {@code peer.service}。
 *
 * <p>相比在 {@code SpanProcessor.onStart()} 中通过 {@code makeCurrent()} 注入 Baggage 的方式，
 * 本方案直接在传播阶段增强 Context，能确保 Propagator 使用的 Context 中包含正确的 Baggage。
 */
public final class CallerServiceBaggagePropagator implements TextMapPropagator {

  private static final Logger logger =
      Logger.getLogger(CallerServiceBaggagePropagator.class.getName());

  private final TextMapPropagator delegate;
  private final String serviceName;
  private final String baggageKey;

  /**
   * 构造函数
   *
   * @param delegate 被装饰的原始传播器
   * @param serviceName 本服务名称
   * @param baggageKey Baggage 中传递调用方服务名的 key
   */
  public CallerServiceBaggagePropagator(
      TextMapPropagator delegate, String serviceName, String baggageKey) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.serviceName = Objects.requireNonNull(serviceName, "serviceName");
    this.baggageKey = Objects.requireNonNull(baggageKey, "baggageKey");
    logger.log(
        Level.INFO,
        "CallerServiceBaggagePropagator initialized with serviceName={0}, baggageKey={1}",
        new Object[] {serviceName, baggageKey});
  }

  @Override
  public Collection<String> fields() {
    return delegate.fields();
  }

  /**
   * 在注入阶段，将 {@code caller.service.name} 追加到 Context 的 Baggage 中，
   * 然后委托给原始传播器执行实际的 inject 操作。
   *
   * <p>这样 {@code W3CBaggagePropagator} 在序列化 Baggage 到 Header 时，
   * 就能包含 {@code caller.service.name} 条目。
   */
  @Override
  public <C> void inject(Context context, @Nullable C carrier, TextMapSetter<C> setter) {
    if (serviceName.isEmpty()) {
      delegate.inject(context, carrier, setter);
      return;
    }

    // 将 caller.service.name 追加到当前 Context 的 Baggage 中
    Baggage currentBaggage = Baggage.fromContext(context);
    Baggage enhancedBaggage =
        currentBaggage.toBuilder().put(baggageKey, serviceName).build();
    Context enhancedContext = context.with(enhancedBaggage);

    // 使用增强后的 Context 委托给原始传播器
    delegate.inject(enhancedContext, carrier, setter);
  }

  /**
   * 提取阶段直接委托给原始传播器，不做额外处理。
   *
   * <p>下游服务收到请求后，{@code W3CBaggagePropagator.extract()} 会自动将
   * Baggage Header 解析到 Context 中，{@link PeerServiceSpanProcessor} 可以在
   * {@code onStart()} 中从 Baggage 读取 {@code caller.service.name}。
   */
  @Override
  public <C> Context extract(Context context, @Nullable C carrier, TextMapGetter<C> getter) {
    return delegate.extract(context, carrier, getter);
  }

  @Override
  public String toString() {
    return "CallerServiceBaggagePropagator{delegate=" + delegate + "}";
  }
}
