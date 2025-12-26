/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client;

import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpHealthMonitor;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * 控制平面客户端接口
 *
 * <p>定义与控制平面服务通信的标准接口，支持 HTTP/Protobuf 和 gRPC 两种协议实现。
 */
public interface ControlPlaneClient extends Closeable {

  /**
   * 拉取配置 (长轮询)
   *
   * @param request 配置请求
   * @return 配置响应的 CompletableFuture
   */
  CompletableFuture<ConfigResponse> getConfig(ConfigRequest request);

  /**
   * 拉取任务 (长轮询)
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
