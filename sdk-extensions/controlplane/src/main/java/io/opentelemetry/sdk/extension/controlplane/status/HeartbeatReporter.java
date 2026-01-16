/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.status;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneService;
import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.CommonProtos.AgentIdentity;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.CommonProtos.ResponseStatus;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.StatusProtos.AgentStatus;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.StatusProtos.JvmMetrics;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.StatusProtos.OtlpExportStatus;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.StatusProtos.StatusRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.StatusProtos.StatusResponse;
import java.time.Duration;
import java.util.Locale;
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
 *
 * <p><b>Phase 5 重构</b>：直接使用 {@link ControlPlaneService}（Protobuf-only），
 * 消除对旧 ControlPlaneClient 的依赖。
 */
public final class HeartbeatReporter {

  private static final Logger logger = Logger.getLogger(HeartbeatReporter.class.getName());

  private final ControlPlaneConfig config;
  private final ControlPlaneService service;
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
    this.service = Objects.requireNonNull(builder.service, "service is required");
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
      String agentId = (String) statusData.getOrDefault("agentId", "");

      // Phase 5: 直接使用 Protobuf Builder 构建请求，填充完整状态数据
      StatusRequest.Builder requestBuilder = StatusRequest.newBuilder()
          .setAgentIdentity(buildAgentIdentity(statusData))
          .setAgentId(agentId)
          .setTimestampMillis(now);

      // 填充 Agent 状态
      requestBuilder.setAgentStatus(buildAgentStatus(statusData));

      // 填充 JVM 指标
      requestBuilder.setJvmMetrics(buildJvmMetrics(statusData));

      StatusRequest request = requestBuilder.build();

      // 发送状态上报
      CompletableFuture<StatusResponse> responseFuture = service.reportStatus(request);

      // 等待响应（带超时）
      StatusResponse response = responseFuture.get(30, TimeUnit.SECONDS);

      // Phase 5: 直接使用 Protobuf 字段判断成功
      boolean success = response.getStatus().getCode() == ResponseStatus.Code.CODE_OK
          || response.getStatus().getCode() == ResponseStatus.Code.CODE_UNSPECIFIED;

      if (success) {
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
        lastError = response.getStatus().getMessage();

        logger.log(
            Level.WARNING,
            "Heartbeat #{0} failed: {1}",
            new Object[] {count, response.getStatus().getMessage()});

        notifyListener(/* success= */ false, statusData, response.getStatus().getMessage());
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

  /**
   * 从收集的状态数据构建 AgentIdentity Protobuf 消息
   */
  @SuppressWarnings("unchecked")
  private static AgentIdentity buildAgentIdentity(Map<String, Object> statusData) {
    AgentIdentity.Builder builder = AgentIdentity.newBuilder();

    // agent_id
    String agentId = (String) statusData.get("agentId");
    if (agentId != null) {
      builder.setAgentId(agentId);
    }

    // host_name
    String hostname = (String) statusData.get("hostname");
    if (hostname != null) {
      builder.setHostName(hostname);
    }

    // process_id
    String processId = (String) statusData.get("processId");
    if (processId != null) {
      builder.setProcessId(processId);
    }

    // sdk_version
    String sdkVersion = (String) statusData.get("sdkVersion");
    if (sdkVersion != null) {
      builder.setSdkVersion(sdkVersion);
    }

    // service_name
    String serviceName = (String) statusData.get("serviceName");
    if (serviceName != null) {
      builder.setServiceName(serviceName);
    }

    // service_namespace
    String serviceNamespace = (String) statusData.get("serviceNamespace");
    if (serviceNamespace != null) {
      builder.setServiceNamespace(serviceNamespace);
    }

    // start_time_millis
    Object startupTimestamp = statusData.get("startupTimestamp");
    if (startupTimestamp instanceof Number) {
      builder.setStartTimeMillis(((Number) startupTimestamp).longValue());
    }

    // attributes (from labels)
    Object labels = statusData.get("labels");
    if (labels instanceof Map) {
      Map<String, String> labelsMap = (Map<String, String>) labels;
      builder.putAllAttributes(labelsMap);
    }

    return builder.build();
  }

  /**
   * 从收集的状态数据构建 AgentStatus Protobuf 消息
   */
  private static AgentStatus buildAgentStatus(Map<String, Object> statusData) {
    AgentStatus.Builder builder = AgentStatus.newBuilder();

    // 设置运行状态
    String runningState = (String) statusData.get("runningState");
    if (runningState != null) {
      try {
        builder.setState(AgentStatus.RunningState.valueOf("RUNNING_STATE_" + runningState.toUpperCase(Locale.ROOT)));
      } catch (IllegalArgumentException e) {
        builder.setState(AgentStatus.RunningState.RUNNING_STATE_RUNNING);
      }
    } else {
      builder.setState(AgentStatus.RunningState.RUNNING_STATE_RUNNING);
    }

    // 设置配置版本
    String configVersion = (String) statusData.get("configVersion");
    if (configVersion != null) {
      builder.setCurrentConfigVersion(configVersion);
    }

    // 设置运行时长
    Object uptimeMs = statusData.get("uptimeMs");
    if (uptimeMs instanceof Number) {
      builder.setUptimeMillis(((Number) uptimeMs).longValue());
    }

    // 设置 OTLP 导出状态
    builder.setOtlpStatus(buildOtlpExportStatus(statusData));

    return builder.build();
  }

  /**
   * 从收集的状态数据构建 OtlpExportStatus Protobuf 消息
   */
  private static OtlpExportStatus buildOtlpExportStatus(Map<String, Object> statusData) {
    OtlpExportStatus.Builder builder = OtlpExportStatus.newBuilder();

    // 设置健康状态
    String otlpHealthState = (String) statusData.get("otlpHealthState");
    if (otlpHealthState != null) {
      try {
        builder.setState(OtlpExportStatus.HealthState.valueOf("HEALTH_STATE_" + otlpHealthState.toUpperCase(Locale.ROOT)));
      } catch (IllegalArgumentException e) {
        builder.setState(OtlpExportStatus.HealthState.HEALTH_STATE_UNKNOWN);
      }
    }

    // 从 spanExportStats 提取统计信息
    @SuppressWarnings("unchecked")
    Map<String, Object> exportStats = (Map<String, Object>) statusData.get("spanExportStats");
    if (exportStats != null) {
      Object successCount = exportStats.get("successCount");
      if (successCount instanceof Number) {
        builder.setSuccessCount(((Number) successCount).longValue());
      }
      Object failureCount = exportStats.get("failureCount");
      if (failureCount instanceof Number) {
        builder.setFailureCount(((Number) failureCount).longValue());
      }
      Object lastExportTime = exportStats.get("lastExportTime");
      if (lastExportTime instanceof Number) {
        builder.setLastSuccessTimeMillis(((Number) lastExportTime).longValue());
      }
      String lastError = (String) exportStats.get("lastError");
      if (lastError != null) {
        builder.setLastErrorMessage(lastError);
      }
    }

    return builder.build();
  }

  /**
   * 从收集的状态数据构建 JvmMetrics Protobuf 消息
   */
  private static JvmMetrics buildJvmMetrics(Map<String, Object> statusData) {
    JvmMetrics.Builder builder = JvmMetrics.newBuilder();

    // 堆内存
    Object heapMemoryUsed = statusData.get("heapMemoryUsed");
    if (heapMemoryUsed instanceof Number) {
      builder.setHeapMemoryUsed(((Number) heapMemoryUsed).longValue());
    }

    Object heapMemoryMax = statusData.get("heapMemoryMax");
    if (heapMemoryMax instanceof Number) {
      builder.setHeapMemoryMax(((Number) heapMemoryMax).longValue());
    }

    // 非堆内存
    Object nonHeapMemoryUsed = statusData.get("nonHeapMemoryUsed");
    if (nonHeapMemoryUsed instanceof Number) {
      builder.setNonHeapMemoryUsed(((Number) nonHeapMemoryUsed).longValue());
    }

    // 线程
    Object threadCount = statusData.get("threadCount");
    if (threadCount instanceof Number) {
      builder.setThreadCount(((Number) threadCount).intValue());
    }

    Object daemonThreadCount = statusData.get("daemonThreadCount");
    if (daemonThreadCount instanceof Number) {
      builder.setDaemonThreadCount(((Number) daemonThreadCount).intValue());
    }

    // GC 信息
    Object gcCount = statusData.get("gcCount");
    if (gcCount instanceof Number) {
      builder.setGcCount(((Number) gcCount).longValue());
    }

    Object gcTimeMillis = statusData.get("gcTimeMillis");
    if (gcTimeMillis instanceof Number) {
      builder.setGcTimeMillis(((Number) gcTimeMillis).longValue());
    }

    // CPU
    Object cpuUsage = statusData.get("cpuUsage");
    if (cpuUsage instanceof Number) {
      builder.setCpuUsage(((Number) cpuUsage).doubleValue());
    }

    Object systemLoadAverage = statusData.get("systemLoadAverage");
    if (systemLoadAverage instanceof Number) {
      builder.setSystemLoadAverage(((Number) systemLoadAverage).doubleValue());
    }

    return builder.build();
  }

  /** Builder for HeartbeatReporter */
  public static final class Builder {
    @Nullable private ControlPlaneConfig config;
    @Nullable private ControlPlaneService service;
    @Nullable private AgentStatusAggregator statusAggregator;
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
      if (service == null) {
        throw new IllegalStateException("service is required");
      }
      if (statusAggregator == null) {
        statusAggregator = new AgentStatusAggregator();
      }
      return new HeartbeatReporter(this);
    }
  }
}
