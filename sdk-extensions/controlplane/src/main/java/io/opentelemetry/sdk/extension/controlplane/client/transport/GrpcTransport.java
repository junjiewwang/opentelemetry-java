/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.transport;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.MetadataUtils;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.ConfigProtos.ConfigRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.ControlPlaneServiceGrpc;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.ControlPlaneServiceGrpc.ControlPlaneServiceFutureStub;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.PollProtos.TaskResultRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.PollProtos.UnifiedPollRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.StatusProtos.StatusRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.ChunkedTaskResult;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.TaskRequest;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * gRPC 传输实现
 *
 * <p>使用 gRPC stub 发送请求，支持所有控制平面操作。
 *
 * <p>特性：
 * <ul>
 *   <li>使用 FutureStub 进行异步调用
 *   <li>支持鉴权（通过 Metadata Header）
 *   <li>支持压缩
 *   <li>优雅关闭
 * </ul>
 *
 * <p><b>注意</b>：此类依赖 gRPC（compileOnly），如果 classpath 中没有 gRPC 依赖，
 * 请使用 {@link HttpTransport} 替代。
 */
public final class GrpcTransport implements Transport {

  private static final Logger logger = Logger.getLogger(GrpcTransport.class.getName());

  private static final Metadata.Key<String> AUTHORIZATION_KEY =
      Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);

  private final ManagedChannel channel;
  private final ControlPlaneServiceFutureStub futureStub;
  private final AtomicBoolean closed;
  @Nullable private final String authorizationHeader;

  /**
   * 创建 gRPC 传输
   *
   * @param config 传输配置
   */
  public GrpcTransport(TransportConfig config) {
    this.authorizationHeader = config.getAuthorizationHeader();
    this.closed = new AtomicBoolean(false);

    // 解析 baseUrl 获取 host 和 port
    URI uri = URI.create(config.getBaseUrl());
    String host = uri.getHost();
    int port = uri.getPort();
    if (port == -1) {
      // 默认端口
      port = "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 4317;
    }

    // 创建 ManagedChannel
    ManagedChannelBuilder<?> channelBuilder =
        ManagedChannelBuilder.forAddress(host, port)
            .keepAliveTime(30, TimeUnit.SECONDS)
            .keepAliveTimeout(10, TimeUnit.SECONDS);

    // 根据 scheme 决定是否使用 TLS
    if ("http".equalsIgnoreCase(uri.getScheme())) {
      channelBuilder.usePlaintext();
    }

    this.channel = channelBuilder.build();

    // 创建 FutureStub
    ControlPlaneServiceFutureStub stub = ControlPlaneServiceGrpc.newFutureStub(channel);

    // 添加鉴权 Header
    if (authorizationHeader != null) {
      Metadata metadata = new Metadata();
      metadata.put(AUTHORIZATION_KEY, authorizationHeader);
      stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    // 启用压缩
    if (config.isCompressionEnabled()) {
      stub = stub.withCompression("gzip");
    }

    this.futureStub = stub;

    logger.log(
        Level.FINE,
        "[GRPC-TRANSPORT] Initialized: target={0}:{1}, hasAuth={2}",
        new Object[] {host, port, authorizationHeader != null});
  }

  @Override
  public CompletableFuture<byte[]> sendUnary(
      Operation operation, byte[] requestBody, long timeoutMillis) {
    if (closed.get()) {
      CompletableFuture<byte[]> future = new CompletableFuture<>();
      future.completeExceptionally(new TransportException("Transport is closed"));
      return future;
    }

    CompletableFuture<byte[]> future = new CompletableFuture<>();

    try {
      // 设置超时
      ControlPlaneServiceFutureStub stubWithDeadline = futureStub;
      if (timeoutMillis > 0) {
        stubWithDeadline = futureStub.withDeadlineAfter(Duration.ofMillis(timeoutMillis));
      }

      // 根据操作类型调用对应的 gRPC 方法
      ListenableFuture<? extends MessageLite> grpcFuture =
          invokeGrpcMethod(stubWithDeadline, operation, requestBody);

      // 将 ListenableFuture 转换为 CompletableFuture
      Futures.addCallback(
          grpcFuture,
          new FutureCallback<MessageLite>() {
            @Override
            public void onSuccess(@Nullable MessageLite result) {
              if (result != null) {
                future.complete(result.toByteArray());
              } else {
                future.complete(new byte[0]);
              }
            }

            @Override
            public void onFailure(Throwable t) {
              logger.log(
                  Level.WARNING,
                  "[GRPC-TRANSPORT] Request failed: operation={0}, error={1}",
                  new Object[] {operation, t.getMessage()});
              future.completeExceptionally(convertToTransportException(t, operation));
            }
          },
          MoreExecutors.directExecutor());

    } catch (InvalidProtocolBufferException e) {
      logger.log(
          Level.WARNING,
          "[GRPC-TRANSPORT] Failed to parse request: operation={0}, error={1}",
          new Object[] {operation, e.getMessage()});
      future.completeExceptionally(
          new TransportException("Failed to parse request: " + e.getMessage(), e));
    }

    return future;
  }

  /**
   * 根据操作类型调用对应的 gRPC 方法
   */
  private static ListenableFuture<? extends MessageLite> invokeGrpcMethod(
      ControlPlaneServiceFutureStub stub, Operation operation, byte[] requestBody)
      throws InvalidProtocolBufferException {
    switch (operation) {
      case UNIFIED_POLL:
        return stub.unifiedPoll(UnifiedPollRequest.parseFrom(requestBody));
      case GET_CONFIG:
        return stub.getConfig(ConfigRequest.parseFrom(requestBody));
      case GET_TASKS:
        return stub.getTasks(TaskRequest.parseFrom(requestBody));
      case REPORT_STATUS:
        return stub.reportStatus(StatusRequest.parseFrom(requestBody));
      case REPORT_TASK_RESULT:
        return stub.reportTaskResult(TaskResultRequest.parseFrom(requestBody));
      case UPLOAD_CHUNK:
        return stub.uploadChunkedResult(ChunkedTaskResult.parseFrom(requestBody));
    }
    throw new IllegalArgumentException("Unsupported operation: " + operation);
  }

  /**
   * 将 gRPC 异常转换为 TransportException
   */
  private static TransportException convertToTransportException(Throwable t, Operation operation) {
    if (t instanceof io.grpc.StatusRuntimeException) {
      io.grpc.StatusRuntimeException sre = (io.grpc.StatusRuntimeException) t;
      Status status = sre.getStatus();
      return new TransportException(
          "gRPC " + status.getCode() + ": " + status.getDescription(),
          status.getCode().name(),
          operation);
    } else if (t instanceof io.grpc.StatusException) {
      io.grpc.StatusException se = (io.grpc.StatusException) t;
      Status status = se.getStatus();
      return new TransportException(
          "gRPC " + status.getCode() + ": " + status.getDescription(),
          status.getCode().name(),
          operation);
    } else {
      return new TransportException("gRPC request failed: " + t.getMessage(), t);
    }
  }

  @Override
  public boolean isAvailable() {
    return !closed.get() && !channel.isShutdown();
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public TransportType getType() {
    return TransportType.GRPC;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      channel.shutdown();
      try {
        if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
          channel.shutdownNow();
          if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
            logger.log(Level.WARNING, "[GRPC-TRANSPORT] Channel did not terminate");
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        channel.shutdownNow();
      }
      logger.log(Level.FINE, "[GRPC-TRANSPORT] Closed");
    }
  }
}
