/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client;

import io.opentelemetry.sdk.extension.controlplane.client.transport.Transport;
import io.opentelemetry.sdk.extension.controlplane.client.transport.TransportFactory;
import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpHealthMonitor;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.ConfigProtos.ConfigRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.ConfigProtos.ConfigResponse;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.PollProtos.TaskResultRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.PollProtos.TaskResultResponse;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.PollProtos.UnifiedPollRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.PollProtos.UnifiedPollResponse;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.StatusProtos.StatusRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.StatusProtos.StatusResponse;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.ChunkedTaskResult;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.ChunkedUploadResponse;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.TaskRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.TaskResponse;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * 控制平面服务接口（Protobuf-only）
 *
 * <p>定义与控制平面服务通信的业务 API，所有请求/响应均使用 Protobuf 消息类型。
 *
 *
 * <p>设计原则：
 * <ul>
 *   <li><b>协议统一</b>：入参/出参全部是 Protobuf Message
 *   <li><b>传输无关</b>：HTTP/gRPC 只是传输方式，由 {@link Transport} 实现
 *   <li><b>异步优先</b>：所有方法返回 CompletableFuture
 * </ul>
 *
 * <p>API 端点：
 * <ul>
 *   <li>POST /v1/control/poll - 统一长轮询（配置+任务）
 *   <li>POST /v1/control/poll/config - 仅配置长轮询
 *   <li>POST /v1/control/poll/tasks - 仅任务长轮询
 *   <li>POST /v1/control/status - 上报状态
 *   <li>POST /v1/control/tasks/result - 上报任务结果
 *   <li>POST /v1/control/upload-chunk - 上传分片结果
 * </ul>
 */
public interface ControlPlaneService extends Closeable {

  /**
   * 统一长轮询（配置 + 任务）
   *
   * <p>这是推荐的主要方法，一次请求同时获取配置更新和待执行任务。
   *
   * @param request 统一轮询请求（Protobuf）
   * @return 统一轮询响应的 CompletableFuture
   */
  CompletableFuture<UnifiedPollResponse> poll(UnifiedPollRequest request);

  /**
   * 拉取配置（长轮询）
   *
   * @param request 配置请求（Protobuf）
   * @return 配置响应的 CompletableFuture
   */
  CompletableFuture<ConfigResponse> getConfig(ConfigRequest request);

  /**
   * 拉取任务（长轮询）
   *
   * @param request 任务请求（Protobuf）
   * @return 任务响应的 CompletableFuture
   */
  CompletableFuture<TaskResponse> getTasks(TaskRequest request);

  /**
   * 上报状态
   *
   * @param request 状态请求（Protobuf）
   * @return 状态响应的 CompletableFuture
   */
  CompletableFuture<StatusResponse> reportStatus(StatusRequest request);

  /**
   * 上报任务执行结果
   *
   * @param request 任务结果请求（Protobuf）
   * @return 任务结果响应的 CompletableFuture
   */
  CompletableFuture<TaskResultResponse> reportTaskResult(TaskResultRequest request);

  /**
   * 上传分片结果
   *
   * @param chunk 分片数据（Protobuf）
   * @return 上传响应的 CompletableFuture
   */
  CompletableFuture<ChunkedUploadResponse> uploadChunkedResult(ChunkedTaskResult chunk);

  /**
   * 检查服务是否已关闭
   *
   * @return 如果服务已关闭返回 true
   */
  boolean isClosed();

  /**
   * 同步检查连接（用于健康检测）
   *
   * @return 如果连接正常返回 true
   */
  boolean checkConnection();

  /**
   * 获取底层传输
   *
   * @return 传输实例
   */
  Transport getTransport();

  /**
   * 关闭服务，释放资源
   */
  @Override
  void close();

  /**
   * 创建服务实例
   *
   * @param config 控制平面配置
   * @param healthMonitor OTLP 健康监控器
   * @return 服务实例
   */
  static ControlPlaneService create(ControlPlaneConfig config, OtlpHealthMonitor healthMonitor) {
    Transport transport = TransportFactory.create(config);
    return new DefaultControlPlaneService(transport, healthMonitor, config);
  }
}
