/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.status;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneService;
import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import io.opentelemetry.sdk.extension.controlplane.identity.AgentIdentityProvider;
import io.opentelemetry.sdk.extension.controlplane.identity.AgentIdentityProvider.AgentIdentity;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.CommonProtos.ResponseStatus;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.StatusProtos.StatusRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.StatusProtos.StatusResponse;
import java.time.Duration;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
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
 *   <li>定时上报 Agent 心跳（agentId + 时间戳）
 *   <li>记录上报成功/失败统计
 *   <li>提供心跳健康探测能力
 * </ul>
 *
 * <p><b>Phase 5 重构</b>：直接使用 {@link ControlPlaneService}（Protobuf-only），
 * 消除对旧 ControlPlaneClient 的依赖。
 *
 * <p><b>Proto 更新</b>：StatusRequest 已精简为纯心跳功能，移除了 AgentStatus、JvmMetrics 等字段。
 */
public final class HeartbeatReporter {

  private static final Logger logger = Logger.getLogger(HeartbeatReporter.class.getName());
  
  // 滑动窗口时间（1分钟）
  private static final long SLIDING_WINDOW_MS = 60_000L;

  private final ControlPlaneConfig config;
  private final ControlPlaneService service;
  private final ScheduledExecutorService scheduler;

  private final AtomicBoolean started;
  private final AtomicBoolean closed;
  // 保留总计数用于日志统计
  private final AtomicLong heartbeatCount;
  private final AtomicLong successCount;
  private final AtomicLong failureCount;
  private final AtomicLong lastHeartbeatTimeMs;
  private final AtomicLong lastSuccessTimeMs;
  
  // 滑动窗口记录
  private final Deque<HeartbeatRecord> slidingWindow = new ConcurrentLinkedDeque<>();

  private static final class HeartbeatRecord {
    final long timestamp;
    final boolean success;

    HeartbeatRecord(long timestamp, boolean success) {
      this.timestamp = timestamp;
      this.success = success;
    }
  }

  @Nullable private ScheduledFuture<?> heartbeatTask;
  @Nullable private volatile String lastError;

  /** 心跳状态监听器 */
  @FunctionalInterface
  public interface HeartbeatListener {
    /**
     * 心跳完成回调
     *
     * @param success 是否成功
     * @param error 错误信息（成功时为 null）
     */
    void onHeartbeat(boolean success, @Nullable String error);
  }

  @Nullable private volatile HeartbeatListener listener;

  private HeartbeatReporter(Builder builder) {
    this.config = Objects.requireNonNull(builder.config, "config is required");
    this.service = Objects.requireNonNull(builder.service, "service is required");
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
      
      // 清理滑动窗口，释放内存
      slidingWindow.clear();

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

  /**
   * 获取最近一分钟内的心跳成功率
   *
   * @return 成功率 (0.0 - 1.0)
   */
  public double getSuccessRate() {
    long now = System.currentTimeMillis();
    // 移除 cleanUpWindow(now) 调用，避免并发修改导致的误删风险
    // 读操作只负责过滤过期数据，写操作（sendHeartbeat）负责清理

    int total = 0;
    int success = 0;
    
    // 遍历快照
    for (HeartbeatRecord record : slidingWindow) {
      if (record.timestamp > now - SLIDING_WINDOW_MS) {
        total++;
        if (record.success) {
          success++;
        }
      }
    }

    if (total == 0) {
      return 1.0; // 无数据时默认健康
    }
    return (double) success / total;
  }

  /**
   * 判断当前是否健康
   * 
   * <p>健康标准：
   * <ul>
   *   <li>最近1分钟内成功率 >= 80%</li>
   *   <li>且最近1分钟内至少有2次成功心跳（确保样本充足且稳定）</li>
   * </ul>
   */
  public boolean isHealthy() {
    long now = System.currentTimeMillis();
    // 移除 cleanUpWindow(now) 调用，避免并发修改导致的误删风险

    int total = 0;
    int success = 0;

    for (HeartbeatRecord record : slidingWindow) {
      if (record.timestamp > now - SLIDING_WINDOW_MS) {
        total++;
        if (record.success) {
          success++;
        }
      }
    }

    // 如果没有数据，默认健康（乐观策略）
    if (total == 0) {
      return true;
    }

    double rate = (double) success / total;
    // 成功率 >= 80% 且 至少有2次成功记录（避免单次偶发成功即恢复）
    return rate >= 0.8 && success >= 2;
  }

  private void cleanUpWindow(long now) {
    long cutoff = now - SLIDING_WINDOW_MS;
    while (!slidingWindow.isEmpty()) {
      HeartbeatRecord first = slidingWindow.peekFirst();
      if (first != null && first.timestamp < cutoff) {
        slidingWindow.pollFirst();
      } else {
        break;
      }
    }
  }

  // ============ 内部实现 ============

  private boolean sendHeartbeat() {
    long count = heartbeatCount.incrementAndGet();
    long now = System.currentTimeMillis();
    lastHeartbeatTimeMs.set(now);

    logger.log(Level.FINE, "Sending heartbeat #{0}...", count);

    try {
      // 获取 Agent 身份标识
      AgentIdentity agentIdentity = AgentIdentityProvider.get();

      // Phase 5: 构建精简的心跳请求（只包含 agentId 和时间戳）
      // 使用 toProto() 方法进行转换
      StatusRequest request = StatusRequest.newBuilder()
          .setAgentIdentity(agentIdentity.toProto())
          .setAgentId(agentIdentity.getAgentId())
          .setTimestampMillis(now)
          .build();

      // 发送状态上报
      CompletableFuture<StatusResponse> responseFuture = service.reportStatus(request);

      // 等待响应（带超时）
      StatusResponse response = responseFuture.get(30, TimeUnit.SECONDS);

      // Phase 5: 直接使用 Protobuf 字段判断成功
      boolean success = response.getStatus().getCode() == ResponseStatus.Code.CODE_OK
          || response.getStatus().getCode() == ResponseStatus.Code.CODE_UNSPECIFIED;

      // 记录到滑动窗口
      slidingWindow.addLast(new HeartbeatRecord(now, success));
      cleanUpWindow(now);

      if (success) {
        successCount.incrementAndGet();
        lastSuccessTimeMs.set(now);
        lastError = null;

        logger.log(Level.FINE, "Heartbeat #{0} sent successfully", count);

        notifyListener(/* success= */ true, null);
        return true;
      } else {
        failureCount.incrementAndGet();
        lastError = response.getStatus().getMessage();

        logger.log(
            Level.WARNING,
            "Heartbeat #{0} failed: {1}",
            new Object[] {count, response.getStatus().getMessage()});

        notifyListener(/* success= */ false, response.getStatus().getMessage());
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

      notifyListener(/* success= */ false, "Interrupted");
      return false;
    } catch (Exception e) {
      failureCount.incrementAndGet();
      lastError = e.getMessage();

      logger.log(
          Level.WARNING,
          "Heartbeat #{0} failed with exception: {1}",
          new Object[] {count, e.getMessage()});

      notifyListener(/* success= */ false, e.getMessage());
      return false;
    }
  }

  private void notifyListener(
      boolean success, @Nullable String error) {
    HeartbeatListener l = this.listener;
    if (l != null) {
      try {
        l.onHeartbeat(success, error);
      } catch (RuntimeException e) {
        logger.log(Level.WARNING, "Heartbeat listener threw exception", e);
      }
    }
  }

  /** Builder for HeartbeatReporter */
  public static final class Builder {
    @Nullable private ControlPlaneConfig config;
    @Nullable private ControlPlaneService service;
    @Nullable private ScheduledExecutorService scheduler;
    @Nullable private HeartbeatListener listener;

    private Builder() {}

    public Builder setConfig(ControlPlaneConfig config) {
      this.config = config;
      return this;
    }

    /**
     * 设置控制平面服务
     *
     * <p><b>Phase 5</b>：使用 {@link ControlPlaneService}（Protobuf-only）。
     *
     * @param service 控制平面服务
     * @return this builder
     */
    public Builder setService(ControlPlaneService service) {
      this.service = service;
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
      if (service == null) {
        throw new IllegalStateException("service is required");
      }
      return new HeartbeatReporter(this);
    }
  }
}
