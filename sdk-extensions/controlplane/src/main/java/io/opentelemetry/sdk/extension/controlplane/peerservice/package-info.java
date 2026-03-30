/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * peer.service 自动解析模块
 *
 * <p>通过 SpanProcessor 在 Span 生命周期中自动填充 {@code peer.service} 属性，
 * 使可观测后端能正确绘制服务拓扑图。
 *
 * <p>核心组件：
 * <ul>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.peerservice.PeerServiceSpanProcessor}
 *       - 在 onEnding() 中根据 Span 类型推断并填充 peer.service</li>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.peerservice.CallerServiceBaggageSpanProcessor}
 *       - 在 onStart() 中将本服务 service.name 注入 Baggage，传递给下游</li>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.peerservice.PeerServiceResolverConfig}
 *       - 配置类，持有 service_mapping、推断策略开关等</li>
 * </ul>
 */
package io.opentelemetry.sdk.extension.controlplane.peerservice;
