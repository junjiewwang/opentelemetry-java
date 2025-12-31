/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.status;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient;
import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 心跳上报器
 *
 * <p>定时收集并上报 Agent 状态到控制平面服务器。 心跳上报的响应本身也作为健康探测的依据。
 *
 * <p>功能特性:
 * <ul>
 *   <li>定时上报 Agent 状态（身份、运行时长、健康状态等）
 *   <li>支持动态调整上报间隔（根据服务端响应）
 *   <li>记录上报成功/失败统计
 *   <li>提供心跳健康探测能力
 * </ul>
 */
public final class HeartbeatReporter {

  private static final Logger logger = Logger.getLogger(HeartbeatReporter.class.getName());

  private final ControlPlaneConfig config;
  private final ControlPlaneClient client;
  private final AgentStatusAggregator statusAggregator;
  private final ScheduledExecutorService scheduler;

  private final AtomicBoolean started;
  private final AtomicBoolean closed;
  private final AtomicLong heartbeatCount;
  private final AtomicLong successCount;
  private final AtomicLong failureCount;
  private final AtomicLong lastHeartbeatTimeMs;
  private final AtomicLong lastSuccessTimeMs;

  @Nullable private ScheduledFuture<?> heartbeatTask;
  @Nullable private volatile String lastError;

  /** 心跳状态监听器 */
  @FunctionalInterface
  public interface HeartbeatListener {
    /**
     * 心跳完成回调
     *
     * @param success 是否成功
     * @param statusData 上报的状态数据
     * @param error 错误信息（成功时为 null）
     */
    void onHeartbeat(boolean success, @Nullable Map<String, Object> statusData, @Nullable String error);
  }

  @Nullable private volatile HeartbeatListener listener;

  private HeartbeatReporter(Builder builder) {
    this.config = Objects.requireNonNull(builder.config, "config is required");
    this.client = Objects.requireNonNull(builder.client, "client is required");
    this.statusAggregator =
        builder.statusAggregator != null ? builder.statusAggregator : new AgentStatusAggregator();
    this.scheduler =
        builder.scheduler != null
            ? builder.scheduler
            : Executors.newSingleThreadScheduledExecutor(
                r -> {
                  Thread t = new Thread(r, "otel-heartbeat");
                  t.setDaemon(true);
                  return t;
                });
    this.started = new AtomicBoolean(false);
    this.closed = new AtomicBoolean(false);
    this.heartbeatCount = new AtomicLong(0);
    this.successCount = new AtomicLong(0);
    this.failureCount = new AtomicLong(0);
    this.lastHeartbeatTimeMs = new AtomicLong(0);
    this.lastSuccessTimeMs = new AtomicLong(0);
    this.listener = builder.listener;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** 启动心跳上报 */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      logger.log(Level.WARNING, "Heartbeat reporter already started");
      return;
    }

    Duration interval = config.getStatusReportInterval();
    logger.log(
        Level.INFO,
        "Starting heartbeat reporter with interval: {0}ms",
        interval.toMillis());

    // 立即执行一次心跳，然后定期执行
    heartbeatTask =
        scheduler.scheduleWithFixedDelay(
            this::sendHeartbeat,
            0, // 立即开始
            interval.toMillis(),
            TimeUnit.MILLISECONDS);
  }

  /** 停止心跳上报 */
  public void stop() {
    if (!started.get() || closed.get()) {
      return;
    }

    logger.log(Level.INFO, "Stopping heartbeat reporter...");

    if (heartbeatTask != null) {
      heartbeatTask.cancel(false);
    }

    logger.log(
        Level.INFO,
        "Heartbeat reporter stopped. Total heartbeats: {0}, success: {1}, failures: {2}",
        new Object[] {heartbeatCount.get(), successCount.get(), failureCount.get()});
  }

  /** 关闭并释放资源 */
  public void close() {
    if (closed.compareAndSet(false, true)) {
      stop();

      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        scheduler.shutdownNow();
      }

      logger.log(Level.INFO, "Heartbeat reporter closed");
    }
  }

  /** 手动触发一次心跳（用于测试或立即上报） */
  public CompletableFuture<Boolean> sendHeartbeatAsync() {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    scheduler.execute(
        () -> {
          boolean success = sendHeartbeat();
          future.complete(success);
        });
    return future;
  }

  /**
   * 设置心跳监听器
   *
   * @param listener 监听器
   */
  public void setListener(HeartbeatListener listener) {
    this.listener = listener;
  }

  // ============ 统计信息 Getters ============

  public long getHeartbeatCount() {
    return heartbeatCount.get();
  }

  public long getSuccessCount() {
    return successCount.get();
  }

  public long getFailureCount() {
    return failureCount.get();
  }

  public long getLastHeartbeatTimeMs() {
    return lastHeartbeatTimeMs.get();
  }

  public long getLastSuccessTimeMs() {
    return lastSuccessTimeMs.get();
  }

  @Nullable
  public String getLastError() {
    return lastError;
  }

  public double getSuccessRate() {
    long total = heartbeatCount.get();
    if (total == 0) {
      return 1.0;
    }
    return (double) successCount.get() / total;
  }

  public boolean isHealthy() {
    // 如果成功率大于 80% 认为健康
    return getSuccessRate() >= 0.8;
  }

  // ============ 内部实现 ============

  private boolean sendHeartbeat() {
    long count = heartbeatCount.incrementAndGet();
    long now = System.currentTimeMillis();
    lastHeartbeatTimeMs.set(now);

    logger.log(Level.FINE, "Sending heartbeat #{0}...", count);

    try {
      // 收集状态数据
      Map<String, Object> statusData = statusAggregator.collectAll();
      byte[] jsonData = statusAggregator.collectAsJsonBytes();

      // 创建状态请求
      StatusRequestImpl request =
          new StatusRequestImpl(
              (String) statusData.getOrDefault("agentId", ""), jsonData);

      // 发送状态上报
      CompletableFuture<ControlPlaneClient.StatusResponse> responseFuture =
          client.reportStatus(request);

      // 等待响应（带超时）
      ControlPlaneClient.StatusResponse response =
          responseFuture.get(30, TimeUnit.SECONDS);

      if (response.isSuccess()) {
        successCount.incrementAndGet();
        lastSuccessTimeMs.set(now);
        lastError = null;

        logger.log(Level.FINE, "Heartbeat #{0} sent successfully", count);

        // 处理服务端建议的上报间隔
        handleSuggestedInterval(response.getSuggestedReportIntervalMillis());

        notifyListener(/* success= */ true, statusData, null);
        return true;
      } else {
        failureCount.incrementAndGet();
        lastError = response.getErrorMessage();

        logger.log(
            Level.WARNING,
            "Heartbeat #{0} failed: {1}",
            new Object[] {count, response.getErrorMessage()});

        notifyListener(/* success= */ false, statusData, response.getErrorMessage());
        return false;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      failureCount.incrementAndGet();
      lastError = "Interrupted";

      logger.log(
          Level.WARNING,
          "Heartbeat #{0} interrupted",
          count);

      notifyListener(/* success= */ false, null, "Interrupted");
      return false;
    } catch (Exception e) {
      failureCount.incrementAndGet();
      lastError = e.getMessage();

      logger.log(
          Level.WARNING,
          "Heartbeat #{0} failed with exception: {1}",
          new Object[] {count, e.getMessage()});

      notifyListener(/* success= */ false, null, e.getMessage());
      return false;
    }
  }

  private void handleSuggestedInterval(long suggestedIntervalMillis) {
    if (suggestedIntervalMillis <= 0) {
      return;
    }

    // TODO: 实现动态调整上报间隔
    // 当前暂不支持动态调整，仅记录日志
    long currentInterval = config.getStatusReportInterval().toMillis();
    if (suggestedIntervalMillis != currentInterval) {
      logger.log(
          Level.FINE,
          "Server suggested report interval: {0}ms (current: {1}ms)",
          new Object[] {suggestedIntervalMillis, currentInterval});
    }
  }

  private void notifyListener(
      boolean success, @Nullable Map<String, Object> statusData, @Nullable String error) {
    HeartbeatListener l = this.listener;
    if (l != null) {
      try {
        l.onHeartbeat(success, statusData, error);
      } catch (RuntimeException e) {
        logger.log(Level.WARNING, "Heartbeat listener threw exception", e);
      }
    }
  }

  /** StatusRequest 实现 */
  private static final class StatusRequestImpl implements ControlPlaneClient.StatusRequest {
    private final String agentId;
    private final byte[] statusData;

    StatusRequestImpl(String agentId, byte[] statusData) {
      this.agentId = agentId;
      this.statusData = statusData;
    }

    @Override
    public String getAgentId() {
      return agentId;
    }

    @Override
    public byte[] getStatusData() {
      return statusData;
    }
  }

  /** Builder for HeartbeatReporter */
  public static final class Builder {
    @Nullable private ControlPlaneConfig config;
    @Nullable private ControlPlaneClient client;
    @Nullable private AgentStatusAggregator statusAggregator;
    @Nullable private ScheduledExecutorService scheduler;
    @Nullable private HeartbeatListener listener;

    private Builder() {}

    public Builder setConfig(ControlPlaneConfig config) {
      this.config = config;
      return this;
    }

    public Builder setClient(ControlPlaneClient client) {
      this.client = client;
      return this;
    }

    public Builder setStatusAggregator(AgentStatusAggregator statusAggregator) {
      this.statusAggregator = statusAggregator;
      return this;
    }

    public Builder setScheduler(ScheduledExecutorService scheduler) {
      this.scheduler = scheduler;
      return this;
    }

    public Builder setListener(HeartbeatListener listener) {
      this.listener = listener;
      return this;
    }

    public HeartbeatReporter build() {
      if (config == null) {
        throw new IllegalStateException("config is required");
      }
      if (client == null) {
        throw new IllegalStateException("client is required");
      }
      if (statusAggregator == null) {
        statusAggregator = new AgentStatusAggregator();
      }
      return new HeartbeatReporter(this);
    }
  }
}
