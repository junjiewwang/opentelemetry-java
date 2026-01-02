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

    long getCreatedAtUnixNano();

    long getExpiresAtUnixNano();
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
}
