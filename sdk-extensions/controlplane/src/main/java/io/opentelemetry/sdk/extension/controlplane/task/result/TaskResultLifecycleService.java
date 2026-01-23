/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.result;

import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

/**
 * 任务结果生命周期管理服务
 *
 * <p>统一编排"结果落盘 → 上传 → 重试 → 成功清理 / 失败标记"的完整生命周期：
 *
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
 * <p><b>职责边界</b>：
 * <ul>
 *   <li>仅处理<b>结果文件类任务</b>（profiling / heap dump / thread dump 等）
 *   <li>不负责任务执行状态上报（由 TaskStatusReporter 处理）
 * </ul>
 */
public interface TaskResultLifecycleService {

  /**
   * 处理任务产出的结果
   *
   * <p>这是生命周期管理的入口，负责：
   * <ol>
   *   <li>应用大小策略（压缩/分片/拒绝）
   *   <li>持久化结果
   *   <li>触发上传
   *   <li>处理上传结果（成功清理/失败重试）
   * </ol>
   *
   * @param taskId 任务 ID
   * @param taskType 任务类型
   * @param data 结果数据
   * @param contentType 内容类型（MIME）
   * @return 处理结果句柄
   */
  CompletableFuture<TaskResultHandle> onResultProduced(
      String taskId,
      String taskType,
      byte[] data,
      String contentType);

  /**
   * 处理任务产出的结果（带元数据）
   *
   * @param taskId 任务 ID
   * @param taskType 任务类型
   * @param data 结果数据
   * @param contentType 内容类型（MIME）
   * @param metadata 扩展元数据
   * @return 处理结果句柄
   */
  CompletableFuture<TaskResultHandle> onResultProduced(
      String taskId,
      String taskType,
      byte[] data,
      String contentType,
      java.util.Map<String, String> metadata);

  /**
   * 重试失败的结果上传
   *
   * <p>手动触发对失败结果的重试，用于故障恢复场景
   *
   * @param taskId 任务 ID
   * @return 重试结果句柄
   */
  CompletableFuture<TaskResultHandle> retryFailed(String taskId);

  /**
   * 补偿上传所有待处理的结果
   *
   * <p>用于启动时恢复未完成的上传，或定期补偿任务
   *
   * @return 补偿处理的结果数量
   */
  CompletableFuture<Integer> compensatePending();

  /**
   * 关闭服务
   *
   * <p>停止重试调度，等待进行中的上传完成
   */
  void close();

  /**
   * 结果处理句柄
   */
  final class TaskResultHandle {

    /** 处理状态 */
    public enum Status {
      /** 处理成功，已上传并清理 */
      SUCCESS,
      /** 处理中，已入队等待上传 */
      PENDING,
      /** 被拒绝（超过最大大小限制） */
      REJECTED,
      /** 处理失败 */
      FAILED,
      /** 已放弃（超过最大重试次数） */
      ABANDONED
    }

    private final String taskId;
    private final Status status;
    @Nullable private final String failureReason;
    @Nullable private final TaskResultDescriptor descriptor;

    private TaskResultHandle(
        String taskId,
        Status status,
        @Nullable String failureReason,
        @Nullable TaskResultDescriptor descriptor) {
      this.taskId = taskId;
      this.status = status;
      this.failureReason = failureReason;
      this.descriptor = descriptor;
    }

    public static TaskResultHandle success(String taskId, TaskResultDescriptor descriptor) {
      return new TaskResultHandle(taskId, Status.SUCCESS, null, descriptor);
    }

    public static TaskResultHandle pending(String taskId, TaskResultDescriptor descriptor) {
      return new TaskResultHandle(taskId, Status.PENDING, null, descriptor);
    }

    public static TaskResultHandle rejected(String taskId, String reason) {
      return new TaskResultHandle(taskId, Status.REJECTED, reason, null);
    }

    public static TaskResultHandle failed(String taskId, String reason) {
      return new TaskResultHandle(taskId, Status.FAILED, reason, null);
    }

    public static TaskResultHandle failed(String taskId, String reason, TaskResultDescriptor descriptor) {
      return new TaskResultHandle(taskId, Status.FAILED, reason, descriptor);
    }

    public static TaskResultHandle abandoned(String taskId, String reason, TaskResultDescriptor descriptor) {
      return new TaskResultHandle(taskId, Status.ABANDONED, reason, descriptor);
    }

    public String getTaskId() {
      return taskId;
    }

    public Status getStatus() {
      return status;
    }

    @Nullable
    public String getFailureReason() {
      return failureReason;
    }

    @Nullable
    public TaskResultDescriptor getDescriptor() {
      return descriptor;
    }

    public boolean isSuccess() {
      return status == Status.SUCCESS;
    }

    public boolean isPending() {
      return status == Status.PENDING;
    }

    public boolean isRejected() {
      return status == Status.REJECTED;
    }

    public boolean isFailed() {
      return status == Status.FAILED;
    }

    public boolean isAbandoned() {
      return status == Status.ABANDONED;
    }

    @Override
    public String toString() {
      if (status == Status.SUCCESS || status == Status.PENDING) {
        return "TaskResultHandle{taskId='" + taskId + "', status=" + status + '}';
      }
      return "TaskResultHandle{taskId='" + taskId + "', status=" + status
          + ", reason='" + failureReason + "'}";
    }
  }
}
