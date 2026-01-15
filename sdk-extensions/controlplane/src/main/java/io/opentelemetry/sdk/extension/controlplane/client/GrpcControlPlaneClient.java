/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client;

import io.opentelemetry.sdk.extension.controlplane.client.response.DefaultChunkedUploadResponse;
import io.opentelemetry.sdk.extension.controlplane.client.response.DefaultConfigResponse;
import io.opentelemetry.sdk.extension.controlplane.client.response.DefaultStatusResponse;
import io.opentelemetry.sdk.extension.controlplane.client.response.DefaultTaskResponse;
import io.opentelemetry.sdk.extension.controlplane.client.response.DefaultTaskResultResponse;
import io.opentelemetry.sdk.extension.controlplane.client.response.DefaultUnifiedPollResponse;
import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpHealthMonitor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * gRPC 控制平面客户端实现（占位）
 *
 * <p>当前为占位实现，所有方法都将抛出 {@link UnsupportedOperationException}。
 *
 * <p>如需启用 gRPC 支持，请：
 * <ol>
 *   <li>添加 io.grpc 依赖
 *   <li>生成 proto 对应的代码
 *   <li>实现此类的各个方法
 * </ol>
 *
 * <p>注意: 如果不需要 gRPC 支持，建议在 {@link ControlPlaneConfig#isGrpc()} 返回 true 时
 * 抛出更明确的异常，而不是使用此占位实现。
 */
public final class GrpcControlPlaneClient implements ControlPlaneClient {

  private static final Logger logger = Logger.getLogger(GrpcControlPlaneClient.class.getName());
  private static final String GRPC_NOT_IMPLEMENTED = "gRPC control plane client is not implemented yet. "
      + "Please use HTTP client by setting otel.exporter.otlp.protocol=http/protobuf";

  @SuppressWarnings("UnusedVariable") // 为将来 gRPC 实现预留
  private final ControlPlaneConfig config;

  private final OtlpHealthMonitor healthMonitor;
  private final AtomicBoolean closed;

  // gRPC 相关字段 (延迟初始化)
  @Nullable private volatile Object managedChannel; // io.grpc.ManagedChannel

  /**
   * 创建 gRPC 控制平面客户端
   *
   * @param config 控制平面配置
   * @param healthMonitor OTLP 健康监控器
   */
  public GrpcControlPlaneClient(ControlPlaneConfig config, OtlpHealthMonitor healthMonitor) {
    this.config = config;
    this.healthMonitor = healthMonitor;
    this.closed = new AtomicBoolean(false);

    logger.log(
        Level.WARNING,
        "gRPC Control Plane client created but NOT IMPLEMENTED. Endpoint: {0}. "
            + "All operations will throw UnsupportedOperationException.",
        config.getEndpoint());
  }

  /**
   * 延迟初始化 gRPC 通道
   *
   * <p>在首次调用时初始化，避免在未使用 gRPC 时加载相关类。
   */
  @SuppressWarnings("UnusedMethod") // 为将来 gRPC 实现预留
  private synchronized void ensureInitialized() {
    if (managedChannel != null) {
      return;
    }

    try {
      // 使用反射加载 gRPC 类，避免硬编码依赖
      Class<?> channelBuilderClass = Class.forName("io.grpc.ManagedChannelBuilder");
      @SuppressWarnings("UnusedVariable") // 将来可能使用
      Class<?> channelClass = Class.forName("io.grpc.ManagedChannel");

      // 解析 endpoint
      String endpoint = config.getEndpoint();
      String host;
      int port;

      if (endpoint.startsWith("http://")) {
        endpoint = endpoint.substring(7);
      } else if (endpoint.startsWith("https://")) {
        endpoint = endpoint.substring(8);
      }

      int colonIndex = endpoint.indexOf(':');
      if (colonIndex > 0) {
        host = endpoint.substring(0, colonIndex);
        String portStr = endpoint.substring(colonIndex + 1);
        // 移除路径部分
        int slashIndex = portStr.indexOf('/');
        if (slashIndex > 0) {
          portStr = portStr.substring(0, slashIndex);
        }
        port = Integer.parseInt(portStr);
      } else {
        host = endpoint;
        port = 4317; // 默认 gRPC 端口
      }

      // 创建 ManagedChannel
      Object builder =
          channelBuilderClass
              .getMethod("forAddress", String.class, int.class)
              .invoke(null, host, port);

      // 设置为 plaintext (非 TLS)
      // TODO: 支持 TLS 配置
      builder = channelBuilderClass.getMethod("usePlaintext").invoke(builder);

      managedChannel = channelBuilderClass.getMethod("build").invoke(builder);

      logger.log(Level.INFO, "gRPC channel created: {0}:{1}", new Object[] {host, port});

      // TODO: 创建 stub 对象
      // 这里需要等 proto 生成的代码可用后再实现

    } catch (ClassNotFoundException e) {
      logger.log(Level.SEVERE, "gRPC classes not found, please add grpc dependencies", e);
      throw new IllegalStateException("gRPC not available", e);
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Failed to initialize gRPC channel", e);
      throw new IllegalStateException("Failed to initialize gRPC", e);
    }
  }

  @Override
  public CompletableFuture<UnifiedPollResponse> poll(UnifiedPollRequest request) {
    checkNotClosed();
    
    // 返回失败响应而非抛出异常，保持 API 兼容性
    logger.log(Level.WARNING, "gRPC poll called but not implemented");
    return CompletableFuture.completedFuture(
        DefaultUnifiedPollResponse.error(GRPC_NOT_IMPLEMENTED));
  }

  @Override
  public CompletableFuture<ConfigResponse> getConfig(ConfigRequest request) {
    checkNotClosed();
    
    logger.log(Level.WARNING, "gRPC getConfig called but not implemented");
    return CompletableFuture.completedFuture(
        DefaultConfigResponse.error(GRPC_NOT_IMPLEMENTED));
  }

  @Override
  public CompletableFuture<TaskResponse> getTasks(TaskRequest request) {
    checkNotClosed();
    
    logger.log(Level.WARNING, "gRPC getTasks called but not implemented");
    return CompletableFuture.completedFuture(
        DefaultTaskResponse.error(GRPC_NOT_IMPLEMENTED));
  }

  @Override
  public CompletableFuture<StatusResponse> reportStatus(StatusRequest request) {
    checkNotClosed();
    
    logger.log(Level.WARNING, "gRPC reportStatus called but not implemented");
    return CompletableFuture.completedFuture(
        DefaultStatusResponse.error(GRPC_NOT_IMPLEMENTED));
  }

  @Override
  public CompletableFuture<ChunkedUploadResponse> uploadChunkedResult(ChunkedTaskResult chunk) {
    checkNotClosed();
    
    logger.log(Level.WARNING, "gRPC uploadChunkedResult called but not implemented");
    return CompletableFuture.completedFuture(
        DefaultChunkedUploadResponse.error(GRPC_NOT_IMPLEMENTED));
  }

  @Override
  public CompletableFuture<TaskResultResponse> reportTaskResult(TaskResultRequest request) {
    checkNotClosed();
    
    logger.log(Level.WARNING, "gRPC reportTaskResult called but not implemented");
    return CompletableFuture.completedFuture(
        DefaultTaskResultResponse.error(GRPC_NOT_IMPLEMENTED));
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public boolean fetchConfig() {
    if (closed.get()) {
      logger.log(Level.WARNING, "Cannot fetch config: client is closed");
      return false;
    }

    // gRPC 尚未实现，返回 false
    logger.log(Level.WARNING, "gRPC fetchConfig not implemented yet");
    return false;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      if (managedChannel != null) {
        try {
          Class<?> channelClass = Class.forName("io.grpc.ManagedChannel");
          channelClass.getMethod("shutdown").invoke(managedChannel);

          // 等待关闭
          Object result =
              channelClass
                  .getMethod("awaitTermination", long.class, java.util.concurrent.TimeUnit.class)
                  .invoke(managedChannel, 5L, java.util.concurrent.TimeUnit.SECONDS);

          if (Boolean.FALSE.equals(result)) {
            channelClass.getMethod("shutdownNow").invoke(managedChannel);
          }
        } catch (Exception e) {
          logger.log(Level.WARNING, "Failed to close gRPC channel", e);
        }
      }
      logger.log(Level.INFO, "gRPC Control Plane client closed");
    }
  }

  private void checkNotClosed() {
    if (closed.get()) {
      throw new IllegalStateException("Client is closed");
    }
  }

  @SuppressWarnings("UnusedMethod") // 保留供将来实现使用
  private void checkOtlpHealth() {
    if (!healthMonitor.isHealthy()) {
      logger.log(
          Level.FINE,
          "OTLP is not healthy, control plane request may be delayed. State: {0}",
          healthMonitor.getState());
    }
  }
}
