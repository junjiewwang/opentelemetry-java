/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * 任务结果生命周期管理模块
 *
 * <p>该包提供任务结果的完整生命周期管理能力：
 *
 * <h2>核心组件</h2>
 * <ul>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.task.result.TaskResultDescriptor}
 *       - 统一的结果描述模型
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.task.result.TaskResultLifecycleService}
 *       - 生命周期编排服务
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.task.result.TaskResultStore}
 *       - 结果存储接口
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.task.result.TaskResultUploader}
 *       - 结果上传接口
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.task.result.TaskResultRetryPolicy}
 *       - 重试策略接口
 * </ul>
 *
 * <h2>生命周期流程</h2>
 * <pre>
 * 执行器产出结果
 *     ↓
 * TaskResultSizePolicy 处理（压缩/分片/拒绝）
 *     ↓
 * TaskResultStore 持久化
 *     ↓
 * TaskResultUploader 上传
 *     ↓
 * ┌─ 成功 → TaskResultStore.delete()
 * └─ 失败 → TaskResultRetryPolicy 判断
 *          ┌─ 可重试 → 调度重试
 *          └─ 不可重试 → TaskResultStore.markAbandoned()
 * </pre>
 *
 * <h2>适用场景</h2>
 * <p>该模块适用于需要持久化和可靠上传的大型结果数据：
 * <ul>
 *   <li>CPU Profiling 结果
 *   <li>堆转储（Heap Dump）
 *   <li>线程快照（Thread Dump）
 * </ul>
 *
 * <h2>与 TaskStatusReporter 的区别</h2>
 * <ul>
 *   <li>TaskStatusReporter：负责任务执行状态上报（RUNNING/SUCCESS/FAILED 等），轻量级
 *   <li>TaskResultLifecycleService：负责结果文件的生命周期管理（存储/上传/重试），重量级
 * </ul>
 *
 * @see io.opentelemetry.sdk.extension.controlplane.task.status.TaskStatusReporter
 * @see io.opentelemetry.sdk.extension.controlplane.task.TaskResultSizePolicy
 */
@javax.annotation.ParametersAreNonnullByDefault
package io.opentelemetry.sdk.extension.controlplane.task.result;
