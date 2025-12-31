/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient;
import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import io.opentelemetry.sdk.extension.controlplane.dynamic.DynamicConfigManager;
import io.opentelemetry.sdk.extension.controlplane.dynamic.DynamicSampler;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpHealthMonitor;
import io.opentelemetry.sdk.extension.controlplane.identity.AgentIdentityProvider;
import io.opentelemetry.sdk.extension.controlplane.status.AgentStatusAggregator;
import io.opentelemetry.sdk.extension.controlplane.status.ControlPlaneStateCollector;
import io.opentelemetry.sdk.extension.controlplane.status.HeartbeatReporter;
import io.opentelemetry.sdk.extension.controlplane.status.IdentityCollector;
import io.opentelemetry.sdk.extension.controlplane.status.OtlpHealthCollector;
import io.opentelemetry.sdk.extension.controlplane.status.SystemResourceCollector;
import io.opentelemetry.sdk.extension.controlplane.status.UptimeCollector;
import io.opentelemetry.sdk.extension.controlplane.task.TaskResultPersistence;
import java.io.Closeable;
import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Control plane manager.
 *
 * <p>Coordinates control plane components including:
 *
 * <ul>
 *   <li>配置轮询
 *   <li>任务轮询
 *   <li>状态上报
 *   <li>健康监控
 * </ul>
 */
public final class ControlPlaneManager implements Closeable {

  private static final Logger logger = Logger.getLogger(ControlPlaneManager.class.getName());

  private final ControlPlaneConfig config;
  private final ControlPlaneClient client;
  private final OtlpHealthMonitor healthMonitor;
  private final DynamicConfigManager configManager;
  private final DynamicSampler dynamicSampler;
  private final TaskResultPersistence resultPersistence;
  private final AgentIdentityProvider.AgentIdentity agentIdentity;

  // 状态收集和心跳上报
  private final AgentStatusAggregator statusAggregator;
  private final HeartbeatReporter heartbeatReporter;
  private final ControlPlaneStateCollector controlPlaneStateCollector;
  private final UptimeCollector uptimeCollector;

  private final ScheduledExecutorService scheduler;
  private final AtomicBoolean started;
  private final AtomicBoolean closed;

  @Nullable private ScheduledFuture<?> configPollTask;
  @Nullable private ScheduledFuture<?> taskPollTask;
  @Nullable private ScheduledFuture<?> statusReportTask;
  @Nullable private ScheduledFuture<?> cleanupTask;

  // 连接状态
  private final AtomicReference<ConnectionState> connectionState;

  // 统计计数器
  private final AtomicLong configPollCount = new AtomicLong(0);
  private final AtomicLong taskPollCount = new AtomicLong(0);
  private final AtomicLong statusReportCount = new AtomicLong(0);
  private final AtomicLong lastStatusLogTime = new AtomicLong(0);
  private static final long STATUS_LOG_INTERVAL_MS = 60_000; // 每分钟输出一次状态日志

  /** Connection state. */
  public enum ConnectionState {
    /** Connected - successfully communicated with control plane server. */
    CONNECTED,
    /** Connecting - attempting to connect but not yet verified. */
    CONNECTING,
    /** Disconnected - connection failed or not started. */
    DISCONNECTED,
    /** Waiting for OTLP recovery - paused due to OTLP health issues. */
    WAITING_FOR_OTLP,
    /** Server unavailable - server endpoint exists but control plane API not available. */
    SERVER_UNAVAILABLE
  }

  private ControlPlaneManager(Builder builder) {
    this.config = Objects.requireNonNull(builder.config, "config is required");
    this.healthMonitor = Objects.requireNonNull(builder.healthMonitor, "healthMonitor is required");
    this.configManager = Objects.requireNonNull(builder.configManager, "configManager is required");
    this.dynamicSampler =
        Objects.requireNonNull(builder.dynamicSampler, "dynamicSampler is required");
    this.resultPersistence = TaskResultPersistence.create(this.config);
    this.agentIdentity = AgentIdentityProvider.get();

    this.client = ControlPlaneClient.create(this.config, this.healthMonitor);

    this.scheduler =
        Executors.newScheduledThreadPool(
            4,
            r -> {
              Thread t = new Thread(r, "otel-controlplane");
              t.setDaemon(true);
              return t;
            });

    this.started = new AtomicBoolean(false);
    this.closed = new AtomicBoolean(false);
    this.connectionState = new AtomicReference<>(ConnectionState.DISCONNECTED);

    // 初始化状态收集器
    this.statusAggregator = new AgentStatusAggregator();
    this.controlPlaneStateCollector = new ControlPlaneStateCollector();
    this.uptimeCollector = new UptimeCollector();
    initializeStatusCollectors();

    // 初始化心跳上报器
    this.heartbeatReporter =
        HeartbeatReporter.builder()
            .setConfig(this.config)
            .setClient(this.client)
            .setStatusAggregator(this.statusAggregator)
            .setScheduler(this.scheduler)
            .setListener(this::onHeartbeatComplete)
            .build();

    // 注册健康状态监听器
    this.healthMonitor.addListener(this::onOtlpHealthStateChanged);
  }

  /**
   * Creates a new builder.
   *
   * @return the builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** 初始化状态收集器 */
  private void initializeStatusCollectors() {
    // 按优先级注册收集器
    statusAggregator.registerCollector(new IdentityCollector());        // 优先级 0
    statusAggregator.registerCollector(uptimeCollector);                 // 优先级 10
    statusAggregator.registerCollector(controlPlaneStateCollector);      // 优先级 15
    statusAggregator.registerCollector(new OtlpHealthCollector(healthMonitor)); // 优先级 20
    statusAggregator.registerCollector(new SystemResourceCollector(config.isIncludeSystemResource())); // 优先级 30

    logger.log(
        Level.FINE,
        "Initialized {0} status collectors: {1}",
        new Object[] {statusAggregator.getCollectorCount(), statusAggregator.getCollectorNames()});
  }

  /** Starts the control plane manager. */
  public void start() {
    if (!config.isEnabled()) {
      logger.log(Level.INFO, "Control plane is disabled");
      return;
    }

    if (!started.compareAndSet(false, true)) {
      logger.log(Level.WARNING, "Control plane manager already started");
      return;
    }

    logger.log(
        Level.INFO,
        "Starting control plane manager, agentId: {0}, endpoint: {1}",
        new Object[] {agentIdentity.getAgentId(), config.getEndpoint()});

    // 注册动态采样器
    configManager.registerComponent(
        "sampler", cfg -> dynamicSampler.update((io.opentelemetry.sdk.trace.samplers.Sampler) cfg));

    // 启动配置轮询
    scheduleConfigPoll();

    // 启动任务轮询
    scheduleTaskPoll();

    // 启动状态上报
    scheduleStatusReport();

    // 启动清理任务
    scheduleCleanup();

    // 启动心跳上报
    heartbeatReporter.start();

    connectionState.set(ConnectionState.CONNECTING);
    logger.log(Level.INFO, "Control plane manager started");
  }

  /** Stops the control plane manager. */
  public void stop() {
    if (!started.get() || closed.get()) {
      return;
    }

    logger.log(Level.INFO, "Stopping control plane manager...");

    // 取消定时任务
    if (configPollTask != null) {
      configPollTask.cancel(false);
    }
    if (taskPollTask != null) {
      taskPollTask.cancel(false);
    }
    if (statusReportTask != null) {
      statusReportTask.cancel(false);
    }
    if (cleanupTask != null) {
      cleanupTask.cancel(false);
    }

    // 停止心跳上报
    heartbeatReporter.stop();

    // 更新运行状态
    uptimeCollector.setRunningState(UptimeCollector.RunningState.STOPPED);

    connectionState.set(ConnectionState.DISCONNECTED);
    logger.log(Level.INFO, "Control plane manager stopped");
  }

  @Override
  public void close() throws IOException {
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

      heartbeatReporter.close();
      client.close();
      logger.log(Level.INFO, "Control plane manager closed");
    }
  }

  /**
   * Gets the connection state.
   *
   * @return the connection state
   */
  public ConnectionState getConnectionState() {
    ConnectionState state = connectionState.get();
    return state != null ? state : ConnectionState.DISCONNECTED;
  }

  /**
   * Gets the health monitor.
   *
   * @return the health monitor
   */
  public OtlpHealthMonitor getHealthMonitor() {
    return healthMonitor;
  }

  /**
   * Gets the dynamic config manager.
   *
   * @return the config manager
   */
  public DynamicConfigManager getConfigManager() {
    return configManager;
  }

  /**
   * Gets the dynamic sampler.
   *
   * @return the dynamic sampler
   */
  public DynamicSampler getDynamicSampler() {
    return dynamicSampler;
  }

  /**
   * Gets the status aggregator.
   *
   * @return the status aggregator
   */
  public AgentStatusAggregator getStatusAggregator() {
    return statusAggregator;
  }

  /**
   * Gets the heartbeat reporter.
   *
   * @return the heartbeat reporter
   */
  public HeartbeatReporter getHeartbeatReporter() {
    return heartbeatReporter;
  }

  private void scheduleConfigPoll() {
    long intervalMillis = config.getConfigPollInterval().toMillis();
    logger.log(
        Level.INFO,
        "Scheduling config poll task with interval: {0}ms",
        intervalMillis);
    configPollTask =
        scheduler.scheduleWithFixedDelay(
            this::pollConfig, 0, intervalMillis, TimeUnit.MILLISECONDS);
  }

  private void scheduleTaskPoll() {
    long intervalMillis = config.getTaskPollInterval().toMillis();
    logger.log(
        Level.INFO,
        "Scheduling task poll task with interval: {0}ms",
        intervalMillis);
    taskPollTask =
        scheduler.scheduleWithFixedDelay(
            this::pollTasks, 1000, intervalMillis, TimeUnit.MILLISECONDS);
  }

  private void scheduleStatusReport() {
    long intervalMillis = config.getStatusReportInterval().toMillis();
    logger.log(
        Level.INFO,
        "Scheduling status report task with interval: {0}ms",
        intervalMillis);
    statusReportTask =
        scheduler.scheduleWithFixedDelay(
            this::reportStatus, 5000, intervalMillis, TimeUnit.MILLISECONDS);
  }

  private void scheduleCleanup() {
    // 每小时清理一次过期结果
    logger.log(Level.INFO, "Scheduling cleanup task with interval: 1 hour");
    cleanupTask =
        scheduler.scheduleWithFixedDelay(
            () -> resultPersistence.cleanupExpired(), 1, 1, TimeUnit.HOURS);
  }

  private void pollConfig() {
    long count = configPollCount.incrementAndGet();
    controlPlaneStateCollector.setConfigPollCount(count);
    if (!shouldConnect()) {
      logPeriodicStatus();
      return;
    }

    try {
      logger.log(Level.FINE, "Polling config (count: {0})...", count);

      // 尝试从控制平面获取配置
      boolean success = client.fetchConfig();

      if (success) {
        // 请求成功，更新连接状态为 CONNECTED
        ConnectionState previousState = connectionState.getAndSet(ConnectionState.CONNECTED);
        controlPlaneStateCollector.setConnectionState(ConnectionState.CONNECTED.name());
        controlPlaneStateCollector.recordConfigFetch();
        if (previousState != ConnectionState.CONNECTED) {
          logger.log(
              Level.INFO,
              "Control plane connected, state changed: {0} -> CONNECTED",
              previousState);
        }
      } else {
        // 请求失败（如 404、500 等），说明服务端不可用
        ConnectionState previousState = connectionState.get();
        if (previousState != ConnectionState.SERVER_UNAVAILABLE) {
          connectionState.set(ConnectionState.SERVER_UNAVAILABLE);
          controlPlaneStateCollector.setConnectionState(ConnectionState.SERVER_UNAVAILABLE.name());
          logger.log(
              Level.WARNING,
              "Control plane server unavailable (API endpoint may not exist), state changed: {0} -> SERVER_UNAVAILABLE",
              previousState);
        }
      }
      logPeriodicStatus();
    } catch (RuntimeException e) {
      ConnectionState previousState = connectionState.getAndSet(ConnectionState.DISCONNECTED);
      logger.log(
          Level.WARNING,
          "Failed to poll config (count: {0}, state: {1} -> DISCONNECTED): {2}",
          new Object[] {count, previousState, e.getMessage()});
    }
  }

  private void pollTasks() {
    long count = taskPollCount.incrementAndGet();
    controlPlaneStateCollector.setTaskPollCount(count);
    // 只有在 CONNECTED 状态才轮询任务
    if (connectionState.get() != ConnectionState.CONNECTED) {
      logger.log(
          Level.FINE,
          "Skip task poll (count: {0}), not connected (state: {1})",
          new Object[] {count, connectionState.get()});
      return;
    }

    try {
      logger.log(Level.FINE, "Polling tasks (count: {0})...", count);
      // TODO: 实现任务轮询逻辑
      // client.fetchTasks();
    } catch (RuntimeException e) {
      logger.log(
          Level.WARNING,
          "Failed to poll tasks (count: {0}): {1}",
          new Object[] {count, e.getMessage()});
    }
  }

  private void reportStatus() {
    long count = statusReportCount.incrementAndGet();
    controlPlaneStateCollector.setStatusReportCount(count);
    try {
      // 状态上报不依赖连接状态，即使 SERVER_UNAVAILABLE 也尝试上报
      logger.log(Level.FINE, "Reporting status (count: {0})...", count);
      // TODO: 实现状态上报逻辑
      // client.reportStatus(agentIdentity, connectionState.get(), healthMonitor.getState());

      // 重传失败的任务结果
      retryFailedResults();
    } catch (RuntimeException e) {
      logger.log(
          Level.WARNING,
          "Failed to report status (count: {0}): {1}",
          new Object[] {count, e.getMessage()});
    }
  }

  private void retryFailedResults() {
    for (String taskId : resultPersistence.getPendingRetryTaskIds()) {
      try {
        resultPersistence
            .read(taskId)
            .ifPresent(
                data -> {
                  // TODO: 实现结果重传逻辑
                  logger.log(Level.FINE, "Retrying task result: {0}", taskId);
                });
      } catch (RuntimeException e) {
        logger.log(Level.WARNING, "Failed to retry task result: " + taskId, e);
        resultPersistence.markForRetry(taskId);
      }
    }
  }

  private boolean shouldConnect() {
    // 检查 OTLP 健康状态
    if (!healthMonitor.isHealthy()) {
      ConnectionState previousState = connectionState.get();
      if (previousState != ConnectionState.WAITING_FOR_OTLP) {
        connectionState.set(ConnectionState.WAITING_FOR_OTLP);
        logger.log(
            Level.INFO,
            "OTLP is not healthy (state: {0}), waiting for recovery before connecting to control plane",
healthMonitor.getState());
      }
      return false;
    }

    return true;
  }

  /** 心跳完成回调 */
  private void onHeartbeatComplete(
      boolean success,
      @Nullable java.util.Map<String, Object> statusData,
      @Nullable String error) {
    // 更新统计计数（引用实例变量，避免 MethodCanBeStatic 警告）
    controlPlaneStateCollector.recordStatusReport();

    if (success) {
      logger.log(Level.FINE, "Heartbeat completed successfully");
    } else {
      logger.log(Level.WARNING, "Heartbeat failed: {0}", error);
    }
  }

  /** 周期性输出状态日志，便于观察控制平面运行情况 */
  private void logPeriodicStatus() {
    long now = System.currentTimeMillis();
    long lastLog = lastStatusLogTime.get();
    if (now - lastLog >= STATUS_LOG_INTERVAL_MS && lastStatusLogTime.compareAndSet(lastLog, now)) {
      // 构建详细的 OTLP 健康信息
      String otlpHealthInfo = buildOtlpHealthInfo();
      
      logger.log(
          Level.INFO,
          "Control plane status - state: {0}, configPolls: {1}, taskPolls: {2}, statusReports: {3}, otlpHealth: [{4}], configUrl: {5}",
          new Object[] {
            connectionState.get(),
            configPollCount.get(),
            taskPollCount.get(),
            statusReportCount.get(),
            otlpHealthInfo,
            config.getControlPlaneUrl() + "/config"
          });
    }
  }

  /** 构建详细的 OTLP 健康信息 */
  private String buildOtlpHealthInfo() {
    OtlpHealthMonitor.HealthState state = healthMonitor.getState();
    long successCount = healthMonitor.getSuccessCount();
    long failureCount = healthMonitor.getFailureCount();
    double successRate = healthMonitor.getSuccessRate();
    long totalSamples = successCount + failureCount;
    
    // 当没有采样数据时，显示更友好的提示
    if (totalSamples == 0) {
      return "state=UNKNOWN, no samples yet (waiting for span exports)";
    }
    
    // 格式: state=HEALTHY, success/fail=95/5, rate=95.0%
    return String.format(
        Locale.ROOT,
        "state=%s, success/fail=%d/%d, rate=%.1f%%",
        state,
        successCount,
        failureCount,
        successRate * 100);
  }

  private void onOtlpHealthStateChanged(
      OtlpHealthMonitor.HealthState previousState, OtlpHealthMonitor.HealthState newState) {
    logger.log(
        Level.INFO,
        "OTLP health state changed: {0} -> {1}",
        new Object[] {previousState, newState});

    if (newState == OtlpHealthMonitor.HealthState.HEALTHY
        && connectionState.get() == ConnectionState.WAITING_FOR_OTLP) {
      // OTLP 恢复健康，尝试重新连接
      connectionState.set(ConnectionState.CONNECTING);
      logger.log(Level.INFO, "OTLP recovered, reconnecting to control plane");
    } else if (newState == OtlpHealthMonitor.HealthState.UNHEALTHY) {
      // OTLP 不健康，暂停连接
      connectionState.set(ConnectionState.WAITING_FOR_OTLP);
      logger.log(Level.INFO, "OTLP became unhealthy, pausing control plane connection");
    }
  }

  /** Builder for {@link ControlPlaneManager}. */
  public static final class Builder {
    @Nullable private ControlPlaneConfig config;
    @Nullable private OtlpHealthMonitor healthMonitor;
    @Nullable private DynamicConfigManager configManager;
    @Nullable private DynamicSampler dynamicSampler;

    private Builder() {}

    /**
     * Sets the configuration.
     *
     * @param config the control plane configuration
     * @return this builder
     */
    public Builder setConfig(ControlPlaneConfig config) {
      this.config = config;
      return this;
    }

    /**
     * Sets the health monitor.
     *
     * @param healthMonitor the health monitor
     * @return this builder
     */
    public Builder setHealthMonitor(OtlpHealthMonitor healthMonitor) {
      this.healthMonitor = healthMonitor;
      return this;
    }

    /**
     * Sets the config manager.
     *
     * @param configManager the config manager
     * @return this builder
     */
    public Builder setConfigManager(DynamicConfigManager configManager) {
      this.configManager = configManager;
      return this;
    }

    /**
     * Sets the dynamic sampler.
     *
     * @param dynamicSampler the dynamic sampler
     * @return this builder
     */
    public Builder setDynamicSampler(DynamicSampler dynamicSampler) {
      this.dynamicSampler = dynamicSampler;
      return this;
    }

    /**
     * Builds the control plane manager.
     *
     * @return the control plane manager
     */
    public ControlPlaneManager build() {
      if (config == null) {
        throw new IllegalStateException("config is required");
      }
      if (healthMonitor == null) {
        healthMonitor =
            new OtlpHealthMonitor(
                config.getHealthWindowSize(),
                config.getHealthyThreshold(),
                config.getUnhealthyThreshold());
      }
      if (configManager == null) {
        configManager = new DynamicConfigManager();
      }
      if (dynamicSampler == null) {
        dynamicSampler = DynamicSampler.create();
      }
      return new ControlPlaneManager(this);
    }
  }
}
