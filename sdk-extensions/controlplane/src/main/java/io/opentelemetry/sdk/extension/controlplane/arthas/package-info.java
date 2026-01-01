/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Arthas 集成模块
 *
 * <p>提供 Arthas 诊断工具与控制平面的集成能力，包括：
 *
 * <ul>
 *   <li>通过 WebSocket Tunnel 与服务端交互
 *   <li>按需启动和自动关闭 Arthas
 *   <li>多会话管理和安全控制
 *   <li>环境检测和自适应
 * </ul>
 *
 * @see io.opentelemetry.sdk.extension.controlplane.arthas.ArthasIntegration
 */
@javax.annotation.ParametersAreNonnullByDefault
package io.opentelemetry.sdk.extension.controlplane.arthas;
