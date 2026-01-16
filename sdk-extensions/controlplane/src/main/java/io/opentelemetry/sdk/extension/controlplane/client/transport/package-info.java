/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * 控制平面传输层
 *
 * <p>提供 HTTP 和 gRPC 两种传输协议的抽象和实现。
 *
 * <p>主要组件：
 * <ul>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.client.transport.Transport} - 传输层接口
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.client.transport.HttpTransport} - HTTP 实现
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.client.transport.TransportFactory} - 传输层工厂
 * </ul>
 */
@ParametersAreNonnullByDefault
package io.opentelemetry.sdk.extension.controlplane.client.transport;

import javax.annotation.ParametersAreNonnullByDefault;
