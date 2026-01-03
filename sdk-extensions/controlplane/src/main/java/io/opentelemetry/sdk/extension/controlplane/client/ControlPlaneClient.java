/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client;

import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpHealthMonitor;
import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

/**
 * 控制平面客户端接口
 *
 * <p>定义与控制平面服务通信的标准接口，支持 HTTP/Protobuf 和 gRPC 两种协议实现。
 *
 * <p>API 端点：
 * <ul>
 *   <li>POST /v1/control/poll - 统一长轮询（配置+任务）
 *   <li>POST /v1/control/poll/config - 仅配置长轮询
 *   <li>POST /v1/control/poll/tasks - 仅任务长轮询
 * </ul>
 */
public interface ControlPlaneClient extends Closeable {

  /**
   * 统一长轮询（同时获取配置和任务）
   *
   * <p>这是推荐的主要方法，一次请求同时获取配置更新和待执行任务。
   *
   * @param request 统一轮询请求
   * @return 统一轮询响应的 CompletableFuture
   */
  CompletableFuture<UnifiedPollResponse> poll(UnifiedPollRequest request);

  /**
   * 拉取配置 (长轮询) - 仅配置
   *
   * @param request 配置请求
   * @return 配置响应的 CompletableFuture
   */
  CompletableFuture<ConfigResponse> getConfig(ConfigRequest request);

  /**
   * 拉取任务 (长轮询) - 仅任务
   *
   * @param request 任务请求
   * @return 任务响应的 CompletableFuture
   */
  CompletableFuture<TaskResponse> getTasks(TaskRequest request);

  /**
   * 上报状态
   *
   * @param request 状态请求
   * @return 状态响应的 CompletableFuture
   */
  CompletableFuture<StatusResponse> reportStatus(StatusRequest request);

  /**
   * 上传分片结果
   *
   * @param chunk 分片数据
   * @return 上传响应的 CompletableFuture
   */
  CompletableFuture<ChunkedUploadResponse> uploadChunkedResult(ChunkedTaskResult chunk);

  /**
   * 上报任务执行结果
   *
   * <p>用于向服务端上报任务的执行状态，包括成功、失败、过期、超时等情况。
   *
   * @param request 任务结果请求
   * @return 任务结果响应的 CompletableFuture
   */
  CompletableFuture<TaskResultResponse> reportTaskResult(TaskResultRequest request);

  /**
   * 检查客户端是否已关闭
   *
   * @return 是否已关闭
   */
  boolean isClosed();

  /**
   * 同步获取配置（用于连接状态检测）
   *
   * <p>该方法会阻塞直到请求完成或超时，用于验证与控制平面服务器的连接是否正常。
   *
   * @return 如果请求成功返回 true，否则返回 false
   */
  boolean fetchConfig();

  /**
   * 创建客户端实例
   *
   * @param config 配置
   * @param healthMonitor 健康监控器
   * @return 客户端实例
   */
  static ControlPlaneClient create(ControlPlaneConfig config, OtlpHealthMonitor healthMonitor) {
    if (config.isGrpc()) {
      return new GrpcControlPlaneClient(config, healthMonitor);
    } else {
      return new HttpControlPlaneClient(config, healthMonitor);
    }
  }

  // ===== 统一轮询请求/响应 DTO =====

  /** 统一轮询请求 */
  interface UnifiedPollRequest {
    String getAgentId();

    String getCurrentConfigVersion();

    String getCurrentConfigEtag();

    long getTimeoutMillis();
  }

  /** 统一轮询响应 */
  interface UnifiedPollResponse {
    boolean isSuccess();

    boolean hasAnyChanges();

    /**
     * 获取各类型的轮询结果
     *
     * @return 类型到结果的映射，key 为 "CONFIG" 或 "TASK"
     */
    Map<String, PollResult> getResults();

    String getErrorMessage();

    /** 获取配置结果（便捷方法） */
    @Nullable
    default PollResult getConfigResult() {
      return getResults().get("CONFIG");
    }

    /** 获取任务结果（便捷方法） */
    @Nullable
    default PollResult getTaskResult() {
      return getResults().get("TASK");
    }
  }

  /** 单个类型的轮询结果 */
  interface PollResult {
    String getType();

    boolean hasChanges();

    /** 配置数据（仅 CONFIG 类型有效） */
    @Nullable
    byte[] getConfigData();

    /** 配置版本（仅 CONFIG 类型有效） */
    @Nullable
    String getConfigVersion();

    /** 配置 ETag（仅 CONFIG 类型有效） */
    @Nullable
    String getConfigEtag();

    /** 任务列表（仅 TASK 类型有效） */
    @Nullable
    java.util.List<TaskInfo> getTasks();
  }

  // ===== 请求/响应 DTO =====

  /** 配置请求 */
  interface ConfigRequest {
    String getAgentId();

    String getCurrentConfigVersion();

    String getCurrentEtag();

    long getLongPollTimeoutMillis();
  }

  /** 配置响应 */
  interface ConfigResponse {
    boolean isSuccess();

    boolean hasChanges();

    String getConfigVersion();

    String getEtag();

    byte[] getConfigData();

    String getErrorMessage();

    long getSuggestedPollIntervalMillis();
  }

  /** 任务请求 */
  interface TaskRequest {
    String getAgentId();

    long getLongPollTimeoutMillis();
  }

  /** 任务响应 */
  interface TaskResponse {
    boolean isSuccess();

    java.util.List<TaskInfo> getTasks();

    String getErrorMessage();

    long getSuggestedPollIntervalMillis();
  }

  /** 任务信息 */
  interface TaskInfo {
    String getTaskId();

    String getTaskType();

    String getParametersJson();

    int getPriority();

    long getTimeoutMillis();

    /** 任务创建时间（毫秒时间戳） */
    long getCreatedAtMillis();

    /** 任务过期时间（毫秒时间戳），0 表示不过期 */
    long getExpiresAtMillis();

    /** 最大允许的任务延迟（毫秒），从服务端配置获取，0 表示使用默认值 */
    long getMaxAcceptableDelayMillis();
  }

  /** 状态请求 */
  interface StatusRequest {
    String getAgentId();

    byte[] getStatusData();
  }

  /** 状态响应 */
  interface StatusResponse {
    boolean isSuccess();

    java.util.List<String> getAcknowledgedTaskIds();

    String getErrorMessage();

    long getSuggestedReportIntervalMillis();
  }

  /** 分片任务结果 */
  interface ChunkedTaskResult {
    String getTaskId();

    String getUploadId();

    int getChunkIndex();

    int getTotalChunks();

    byte[] getChunkData();

    String getChunkChecksum();

    boolean isLastChunk();
  }

  /** 分片上传响应 */
  interface ChunkedUploadResponse {
    boolean isSuccess();

    String getUploadId();

    int getReceivedChunkIndex();

    String getStatus();

    String getErrorMessage();
  }

  // ===== 任务结果上报 DTO =====

  /**
   * 任务执行状态（稳定集合）
   *
   * <p>遵循"稳定状态 + 原因码"模式：
   * <ul>
   *   <li>status 使用稳定集合（SUCCESS/FAILED/TIMEOUT/CANCELLED/RUNNING/PENDING）
   *   <li>细分原因通过 error_code 表达（如 TASK_STALE、TASK_EXPIRED、TASK_REJECTED）
   *   <li>error_message 放可读信息
   * </ul>
   */
  enum TaskStatus {
    /** 任务待执行 */
    PENDING,
    /** 任务执行中 */
    RUNNING,
    /** 任务执行成功 */
    SUCCESS,
    /** 任务执行失败（包括过期、过旧、被拒绝等，具体原因见 error_code） */
    FAILED,
    /** 任务执行超时 */
    TIMEOUT,
    /** 任务被取消 */
    CANCELLED
  }

  /** 任务结果上报请求 */
  interface TaskResultRequest {
    /** 任务 ID */
    String getTaskId();

    /** Agent ID */
    String getAgentId();

    /** 任务执行状态 */
    TaskStatus getStatus();

    /** 错误码（失败时） */
    @Nullable
    String getErrorCode();

    /** 错误信息（失败时） */
    @Nullable
    String getErrorMessage();

    /** 任务输出结果（成功时，JSON 格式） */
    @Nullable
    String getResultJson();

    /** 任务开始执行时间（毫秒时间戳） */
    long getStartedAtMillis();

    /** 任务完成时间（毫秒时间戳） */
    long getCompletedAtMillis();

    /** 任务执行耗时（毫秒） */
    long getExecutionTimeMillis();
  }

  /** 任务结果上报响应 */
  interface TaskResultResponse {
    /** 是否成功 */
    boolean isSuccess();

    /** 错误信息 */
    String getErrorMessage();
  }
}
