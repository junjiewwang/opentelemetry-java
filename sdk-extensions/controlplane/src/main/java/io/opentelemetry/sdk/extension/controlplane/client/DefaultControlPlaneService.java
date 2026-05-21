/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client;

import io.opentelemetry.sdk.extension.controlplane.client.codec.ProtobufCodec;
import io.opentelemetry.sdk.extension.controlplane.client.transport.Transport;
import io.opentelemetry.sdk.extension.controlplane.client.transport.Transport.Operation;
import io.opentelemetry.sdk.extension.controlplane.client.transport.TransportException;
import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.CommonProtos.ResponseStatus;
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
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.ChunkUploadStatus;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.Task;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.TaskRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.TaskResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 控制平面服务默认实现
 *
 * <p>统一的业务逻辑骨架，委托 {@link Transport} 处理传输细节。
 *
 * <p>职责：
 * <ul>
 *   <li>Protobuf 消息的编解码（委托 {@link ProtobufCodec}）
 *   <li>统一日志（委托 {@link ControlPlaneLogger}）
 *   <li>生命周期管理（closed 状态）
 *   <li>轮询计时和过快返回检测
 * </ul>
 */
public final class DefaultControlPlaneService implements ControlPlaneService {

  private final Transport transport;
  private final ControlPlaneLogger logger;
  private final AtomicBoolean closed;

  /**
   * 创建默认控制平面服务
   *
   * @param transport 传输实例
   * @param config 控制平面配置
   */
  public DefaultControlPlaneService(Transport transport, ControlPlaneConfig config) {
    this.transport = transport;
    this.logger = new ControlPlaneLogger(config.isDebugEnabled());
    this.closed = new AtomicBoolean(false);

    logger.logServiceInitialized(
        transport.getType().getValue(),
        config.getProtocol(),
        config.getControlPlaneUrl(),
        config.getAuthorizationHeader() != null);
  }

  @Override
  public CompletableFuture<UnifiedPollResponse> poll(UnifiedPollRequest request) {
    checkNotClosed();

    String agentId = request.getAgentId();
    // 协议对齐：从嵌套的 config_request 获取配置版本
    String configVersion =
        request.hasConfigRequest() && request.getConfigRequest().hasCurrentVersion()
            ? request.getConfigRequest().getCurrentVersion().getVersion()
            : "";
    long timeoutMillis = request.getTimeoutMillis();

    logger.logPollRequest(agentId, timeoutMillis, configVersion);

    long startTime = System.currentTimeMillis();
    byte[] requestBytes = ProtobufCodec.encode(request);

    return transport
        .sendUnary(Operation.UNIFIED_POLL, requestBytes, timeoutMillis)
        .thenApply(
            responseBytes -> {
              UnifiedPollResponse response =
                  ProtobufCodec.decodeSafe(
                      responseBytes, UnifiedPollResponse.parser(), "UnifiedPollResponse");
              if (response == null) {
                response = buildErrorPollResponse("Failed to parse response");
              }

              long durationMs = System.currentTimeMillis() - startTime;
              boolean success =
                  response.getStatus().getCode() == ResponseStatus.Code.CODE_OK
                      || response.getStatus().getCode() == ResponseStatus.Code.CODE_UNSPECIFIED;
              // 协议对齐：从嵌套的 task_response 获取任务数量
              int taskCount =
                  response.hasTaskResponse()
                      ? response.getTaskResponse().getTasksCount()
                      : 0;

              logger.logPollResponse(
                  durationMs, timeoutMillis, response.getHasAnyChanges(), taskCount, success);

              return response;
            })
        .exceptionally(
            e -> {
              long durationMs = System.currentTimeMillis() - startTime;
              int httpCode = extractHttpCode(e);
              logger.logPollError(durationMs, getErrorMessage(e), httpCode);
              return buildErrorPollResponse(getErrorMessage(e));
            });
  }

  @Override
  public CompletableFuture<ConfigResponse> getConfig(ConfigRequest request) {
    checkNotClosed();

    byte[] requestBytes = ProtobufCodec.encode(request);
    long timeoutMillis = request.getLongPollTimeoutMillis();

    return transport
        .sendUnary(Operation.GET_CONFIG, requestBytes, timeoutMillis)
        .thenApply(
            responseBytes -> {
              ConfigResponse response =
                  ProtobufCodec.decodeSafe(
                      responseBytes, ConfigResponse.parser(), "ConfigResponse");
              if (response == null) {
                return buildErrorConfigResponse("Failed to parse response");
              }

              // 从嵌套的 config.version 中获取版本和 etag
              String version =
                  response.hasConfig() && response.getConfig().hasVersion()
                      ? response.getConfig().getVersion().getVersion()
                      : "";
              String etag =
                  response.hasConfig() && response.getConfig().hasVersion()
                      ? response.getConfig().getVersion().getEtag()
                      : "";

              logger.logConfigReceived(version, etag, response.getHasChanges());
              return response;
            })
        .exceptionally(e -> buildErrorConfigResponse(getErrorMessage(e)));
  }

  @Override
  public CompletableFuture<TaskResponse> getTasks(TaskRequest request) {
    checkNotClosed();

    byte[] requestBytes = ProtobufCodec.encode(request);
    long timeoutMillis = request.getLongPollTimeoutMillis();

    return transport
        .sendUnary(Operation.GET_TASKS, requestBytes, timeoutMillis)
        .thenApply(
            responseBytes -> {
              TaskResponse response =
                  ProtobufCodec.decodeSafe(responseBytes, TaskResponse.parser(), "TaskResponse");
              if (response == null) {
                return buildErrorTaskResponse("Failed to parse response");
              }

              // 记录收到的任务
              for (Task task : response.getTasksList()) {
                int priorityNum = task.getPriorityNum() > 0 
                    ? task.getPriorityNum() : task.getPriority().getNumber();
                String taskType = task.getTaskTypeName().isEmpty() 
                    ? task.getType().name() : task.getTaskTypeName();
                logger.logTaskReceived(task.getTaskId(), taskType, priorityNum);
              }
              return response;
            })
        .exceptionally(e -> buildErrorTaskResponse(getErrorMessage(e)));
  }

  @Override
  public CompletableFuture<StatusResponse> reportStatus(StatusRequest request) {
    checkNotClosed();
    // 状态上报不依赖 OTLP 健康状态，始终尝试发送

    byte[] requestBytes = ProtobufCodec.encode(request);

    return transport
        .sendUnary(Operation.REPORT_STATUS, requestBytes, 0)
        .thenApply(
            responseBytes -> {
              StatusResponse response =
                  ProtobufCodec.decodeSafe(
                      responseBytes, StatusResponse.parser(), "StatusResponse");
              if (response == null) {
                return buildErrorStatusResponse("Failed to parse response");
              }
              return response;
            })
        .exceptionally(e -> buildErrorStatusResponse(getErrorMessage(e)));
  }

  @Override
  public CompletableFuture<TaskResultResponse> reportTaskResult(TaskResultRequest request) {
    checkNotClosed();

    // 协议对齐：从嵌套的 result 中获取任务信息
    String taskId = request.hasResult() ? request.getResult().getTaskId() : "";
    String statusName = request.hasResult() ? request.getResult().getStatus().name() : "UNKNOWN";
    String errorCode = request.hasResult() ? request.getResult().getErrorCode() : "";
    logger.logTaskResultReport(taskId, statusName, errorCode);

    byte[] requestBytes = ProtobufCodec.encode(request);

    return transport
        .sendUnary(Operation.REPORT_TASK_RESULT, requestBytes, 0)
        .thenApply(
            responseBytes -> {
              TaskResultResponse response =
                  ProtobufCodec.decodeSafe(
                      responseBytes, TaskResultResponse.parser(), "TaskResultResponse");
              if (response == null) {
                return buildErrorTaskResultResponse("Failed to parse response");
              }
              return response;
            })
        .exceptionally(e -> buildErrorTaskResultResponse(getErrorMessage(e)));
  }

  @Override
  public CompletableFuture<ChunkedUploadResponse> uploadChunkedResult(ChunkedTaskResult chunk) {
    checkNotClosed();

    byte[] requestBytes = ProtobufCodec.encode(chunk);

    return transport
        .sendUnary(Operation.UPLOAD_CHUNK, requestBytes, 0)
        .thenApply(
            responseBytes -> {
              ChunkedUploadResponse response =
                  ProtobufCodec.decodeSafe(
                      responseBytes, ChunkedUploadResponse.parser(), "ChunkedUploadResponse");
              if (response == null) {
                return buildErrorChunkedUploadResponse("Failed to parse response");
              }
              return response;
            })
        .exceptionally(e -> buildErrorChunkedUploadResponse(getErrorMessage(e)));
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public boolean checkConnection() {
    if (closed.get()) {
      return false;
    }
    return transport.isAvailable();
  }

  @Override
  public Transport getTransport() {
    return transport;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      transport.close();
      logger.logServiceClosed();
    }
  }

  // ===== 私有方法 =====

  private void checkNotClosed() {
    if (closed.get()) {
      throw new IllegalStateException("Service is closed");
    }
  }

  private static int extractHttpCode(Throwable e) {
    if (e instanceof TransportException) {
      return ((TransportException) e).getHttpStatusCode();
    }
    if (e.getCause() instanceof TransportException) {
      return ((TransportException) e.getCause()).getHttpStatusCode();
    }
    return 0;
  }

  /** 安全获取错误消息，避免 NullAway 警告 */
  private static String getErrorMessage(Throwable e) {
    String message = e.getMessage();
    return message != null ? message : "Unknown error";
  }

  // ===== 错误响应构建方法 =====

  private static UnifiedPollResponse buildErrorPollResponse(String message) {
    return UnifiedPollResponse.newBuilder()
        .setStatus(
            ResponseStatus.newBuilder()
                .setCode(ResponseStatus.Code.CODE_ERROR)
                .setMessage(message != null ? message : "Unknown error")
                .build())
        .setHasAnyChanges(false)
        .build();
  }

  private static ConfigResponse buildErrorConfigResponse(String message) {
    return ConfigResponse.newBuilder()
        .setStatus(
            ResponseStatus.newBuilder()
                .setCode(ResponseStatus.Code.CODE_ERROR)
                .setMessage(message != null ? message : "Unknown error")
                .build())
        .setHasChanges(false)
        .build();
  }

  private static TaskResponse buildErrorTaskResponse(String message) {
    return TaskResponse.newBuilder()
        .setStatus(
            ResponseStatus.newBuilder()
                .setCode(ResponseStatus.Code.CODE_ERROR)
                .setMessage(message != null ? message : "Unknown error")
                .build())
        .build();
  }

  private static StatusResponse buildErrorStatusResponse(String message) {
    return StatusResponse.newBuilder()
        .setStatus(
            ResponseStatus.newBuilder()
                .setCode(ResponseStatus.Code.CODE_ERROR)
                .setMessage(message != null ? message : "Unknown error")
                .build())
        .build();
  }

  private static TaskResultResponse buildErrorTaskResultResponse(String message) {
    return TaskResultResponse.newBuilder()
        .setStatus(
            ResponseStatus.newBuilder()
                .setCode(ResponseStatus.Code.CODE_ERROR)
                .setMessage(message != null ? message : "Unknown error")
                .build())
        .setAcknowledged(false)
        .build();
  }

  private static ChunkedUploadResponse buildErrorChunkedUploadResponse(String message) {
    return ChunkedUploadResponse.newBuilder()
        .setStatus(ChunkUploadStatus.CHUNK_UPLOAD_STATUS_UPLOAD_FAILED)
        .setErrorMessage(message != null ? message : "Unknown error")
        .build();
  }
}
