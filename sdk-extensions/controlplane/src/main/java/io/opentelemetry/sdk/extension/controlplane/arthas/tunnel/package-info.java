/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Arthas Tunnel 通信模块
 *
 * <p>提供与 Tunnel Server 的 WebSocket 通信能力，包括：
 *
 * <ul>
 *   <li>WebSocket 连接管理
 *   <li>消息协议定义
 *   <li>自动重连机制
 * </ul>
 *
 * @see io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.ArthasTunnelClient
 */
@javax.annotation.ParametersAreNonnullByDefault
package io.opentelemetry.sdk.extension.controlplane.arthas.tunnel;
