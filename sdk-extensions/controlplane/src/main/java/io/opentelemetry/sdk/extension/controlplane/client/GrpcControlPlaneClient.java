/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client;

import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpHealthMonitor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * gRPC 控制平面客户端实现
 *
 * <p>使用 gRPC 一元长轮询方式与控制平面服务通信，预留双向流扩展能力。
 *
 * <p>注意: 此实现需要 io.grpc 依赖，在运行时按需加载。
 */
public final class GrpcControlPlaneClient implements ControlPlaneClient {

  private static final Logger logger = Logger.getLogger(GrpcControlPlaneClient.class.getName());

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
        Level.INFO,
        "gRPC Control Plane client initialized (lazy), endpoint: {0}",
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
    checkOtlpHealth();

    // TODO: 当前返回占位实现
    return CompletableFuture.completedFuture(
        new DefaultUnifiedPollResponse(
            /* success= */ false,
            /* hasAnyChanges= */ false,
            Collections.emptyMap(),
            "gRPC not implemented yet"));
  }

  @Override
  public CompletableFuture<ConfigResponse> getConfig(ConfigRequest request) {
    checkNotClosed();
    checkOtlpHealth();

    // TODO: 当前返回占位实现
    return CompletableFuture.completedFuture(
        new DefaultConfigResponse(
            /* success= */ false,
            /* hasChanges= */ false,
            "",
            "",
            new byte[0],
            "gRPC not implemented yet",
            30000));
  }

  @Override
  public CompletableFuture<TaskResponse> getTasks(TaskRequest request) {
    checkNotClosed();
    checkOtlpHealth();

    // TODO: 当前返回占位实现
    return CompletableFuture.completedFuture(
        new DefaultTaskResponse(
            /* success= */ false, Collections.emptyList(), "gRPC not implemented yet", 10000));
  }

  @Override
  public CompletableFuture<StatusResponse> reportStatus(StatusRequest request) {
    checkNotClosed();

    // TODO: 当前返回占位实现
    return CompletableFuture.completedFuture(
        new DefaultStatusResponse(
            /* success= */ false, Collections.emptyList(), "gRPC not implemented yet", 60000));
  }

  @Override
  public CompletableFuture<ChunkedUploadResponse> uploadChunkedResult(ChunkedTaskResult chunk) {
    checkNotClosed();

    // TODO: 当前返回占位实现
    return CompletableFuture.completedFuture(
        new DefaultChunkedUploadResponse(
            /* success= */ false, "", -1, "FAILED", "gRPC not implemented yet"));
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

  private void checkOtlpHealth() {
    if (!healthMonitor.isHealthy()) {
      logger.log(
          Level.FINE,
          "OTLP is not healthy, control plane request may be delayed. State: {0}",
          healthMonitor.getState());
    }
  }

  // ===== 默认响应实现 (与 HTTP 客户端共享) =====

  private static final class DefaultUnifiedPollResponse implements UnifiedPollResponse {
    private final boolean success;
    private final boolean hasAnyChanges;
    private final Map<String, PollResult> results;
    private final String errorMessage;

    DefaultUnifiedPollResponse(
        boolean success,
        boolean hasAnyChanges,
        Map<String, PollResult> results,
        String errorMessage) {
      this.success = success;
      this.hasAnyChanges = hasAnyChanges;
      this.results = results;
      this.errorMessage = errorMessage;
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    @Override
    public boolean hasAnyChanges() {
      return hasAnyChanges;
    }

    @Override
    public Map<String, PollResult> getResults() {
      return results;
    }

    @Override
    public String getErrorMessage() {
      return errorMessage;
    }
  }

  private static final class DefaultConfigResponse implements ConfigResponse {
    private final boolean success;
    private final boolean hasChanges;
    private final String configVersion;
    private final String etag;
    private final byte[] configData;
    private final String errorMessage;
    private final long suggestedPollIntervalMillis;

    DefaultConfigResponse(
        boolean success,
        boolean hasChanges,
        String configVersion,
        String etag,
        byte[] configData,
        String errorMessage,
        long suggestedPollIntervalMillis) {
      this.success = success;
      this.hasChanges = hasChanges;
      this.configVersion = configVersion;
      this.etag = etag;
      this.configData = configData;
      this.errorMessage = errorMessage;
      this.suggestedPollIntervalMillis = suggestedPollIntervalMillis;
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    @Override
    public boolean hasChanges() {
      return hasChanges;
    }

    @Override
    public String getConfigVersion() {
      return configVersion;
    }

    @Override
    public String getEtag() {
      return etag;
    }

    @Override
    public byte[] getConfigData() {
      return configData;
    }

    @Override
    public String getErrorMessage() {
      return errorMessage;
    }

    @Override
    public long getSuggestedPollIntervalMillis() {
      return suggestedPollIntervalMillis;
    }
  }

  private static final class DefaultTaskResponse implements TaskResponse {
    private final boolean success;
    private final List<TaskInfo> tasks;
    private final String errorMessage;
    private final long suggestedPollIntervalMillis;

    DefaultTaskResponse(
        boolean success,
        List<TaskInfo> tasks,
        String errorMessage,
        long suggestedPollIntervalMillis) {
      this.success = success;
      this.tasks = tasks;
      this.errorMessage = errorMessage;
      this.suggestedPollIntervalMillis = suggestedPollIntervalMillis;
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    @Override
    public List<TaskInfo> getTasks() {
      return tasks;
    }

    @Override
    public String getErrorMessage() {
      return errorMessage;
    }

    @Override
    public long getSuggestedPollIntervalMillis() {
      return suggestedPollIntervalMillis;
    }
  }

  private static final class DefaultStatusResponse implements StatusResponse {
    private final boolean success;
    private final List<String> acknowledgedTaskIds;
    private final String errorMessage;
    private final long suggestedReportIntervalMillis;

    DefaultStatusResponse(
        boolean success,
        List<String> acknowledgedTaskIds,
        String errorMessage,
        long suggestedReportIntervalMillis) {
      this.success = success;
      this.acknowledgedTaskIds = acknowledgedTaskIds;
      this.errorMessage = errorMessage;
      this.suggestedReportIntervalMillis = suggestedReportIntervalMillis;
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    @Override
    public List<String> getAcknowledgedTaskIds() {
      return acknowledgedTaskIds;
    }

    @Override
    public String getErrorMessage() {
      return errorMessage;
    }

    @Override
    public long getSuggestedReportIntervalMillis() {
      return suggestedReportIntervalMillis;
    }
  }

  private static final class DefaultChunkedUploadResponse implements ChunkedUploadResponse {
    private final boolean success;
    private final String uploadId;
    private final int receivedChunkIndex;
    private final String status;
    private final String errorMessage;

    DefaultChunkedUploadResponse(
        boolean success,
        String uploadId,
        int receivedChunkIndex,
        String status,
        String errorMessage) {
      this.success = success;
      this.uploadId = uploadId;
      this.receivedChunkIndex = receivedChunkIndex;
      this.status = status;
      this.errorMessage = errorMessage;
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    @Override
    public String getUploadId() {
      return uploadId;
    }

    @Override
    public int getReceivedChunkIndex() {
      return receivedChunkIndex;
    }

    @Override
    public String getStatus() {
      return status;
    }

    @Override
    public String getErrorMessage() {
      return errorMessage;
    }
  }
}
