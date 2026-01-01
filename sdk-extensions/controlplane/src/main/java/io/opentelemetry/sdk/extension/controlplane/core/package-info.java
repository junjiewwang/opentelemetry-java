/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * 控制平面核心组件。
 *
 * <p>包含控制平面的核心抽象和管理组件：
 *
 * <ul>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.core.ConnectionStateManager} - 连接状态管理
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.core.ScheduledTaskManager} - 调度任务管理
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.core.HealthCheckCoordinator} - 健康检查协调
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.core.ControlPlaneStatistics} - 统计信息管理
 * </ul>
 */
@ParametersAreNonnullByDefault
package io.opentelemetry.sdk.extension.controlplane.core;

import javax.annotation.ParametersAreNonnullByDefault;
