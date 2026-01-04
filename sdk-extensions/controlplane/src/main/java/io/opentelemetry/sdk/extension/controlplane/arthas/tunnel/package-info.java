/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Arthas Tunnel 通信模块
 *
 * <p>【模式2架构】由 Arthas 内部 TunnelClient(Netty) 负责 tunnel 连接，
 * 此包现仅保留协议消息定义。
 *
 * <p>主要组件：
 * <ul>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage} - 消息协议定义</li>
 * </ul>
 *
 * @see io.opentelemetry.sdk.extension.controlplane.arthas.ArthasTunnelStatusBridge
 */
@javax.annotation.ParametersAreNonnullByDefault
package io.opentelemetry.sdk.extension.controlplane.arthas.tunnel;
