/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * OpenTelemetry SDK Extension - Control Plane
 *
 * <p>提供 Agent 控制平面扩展功能，包括：
 *
 * <ul>
 *   <li>配置动态下发与热更新
 *   <li>远程任务执行与结果上报
 *   <li>Agent 状态监控与上报
 *   <li>OTLP 健康状态联动
 * </ul>
 *
 * <p>启用方式：设置环境变量或系统属性 {@code otel.agent.control.enabled=true}
 *
 * @see io.opentelemetry.sdk.extension.controlplane.ControlPlaneManager
 */
@ParametersAreNonnullByDefault
package io.opentelemetry.sdk.extension.controlplane;

import javax.annotation.ParametersAreNonnullByDefault;
