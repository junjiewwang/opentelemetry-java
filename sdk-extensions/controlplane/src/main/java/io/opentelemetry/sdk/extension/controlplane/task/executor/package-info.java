/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * 任务执行框架
 *
 * <p>提供任务执行的核心组件：
 * <ul>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutor} - 任务执行器接口
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.task.executor.TaskDispatcher} - 任务分发器
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionContext} - 执行上下文
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionResult} - 执行结果
 * </ul>
 *
 * <p>扩展支持：
 * <ul>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.task.executor.ArthasAttachExecutor} - Arthas 附加执行器
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.task.executor.ArthasDetachExecutor} - Arthas 分离执行器
 * </ul>
 */
package io.opentelemetry.sdk.extension.controlplane.task.executor;
