/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane;

import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasConfig;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasIntegration;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient;
import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import io.opentelemetry.sdk.extension.controlplane.core.ConnectionStateManager;
import io.opentelemetry.sdk.extension.controlplane.core.ConnectionStateManager.ConnectionState;
import io.opentelemetry.sdk.extension.controlplane.core.ControlPlaneStatistics;
import io.opentelemetry.sdk.extension.controlplane.core.HealthCheckCoordinator;
import io.opentelemetry.sdk.extension.controlplane.core.ScheduledTaskManager;
import io.opentelemetry.sdk.extension.controlplane.core.ScheduledTaskManager.TaskConfig;
import io.opentelemetry.sdk.extension.controlplane.core.tasks.CleanupTask;
import io.opentelemetry.sdk.extension.controlplane.core.tasks.ConfigPollTask;
import io.opentelemetry.sdk.extension.controlplane.core.tasks.StatusReportTask;
import io.opentelemetry.sdk.extension.controlplane.core.tasks.TaskPollTask;
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
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 控制平面管理器。
 *
 * <p>作为控制平面的门面（Facade），协调各个子组件的工作，包括：
 *
 * <ul>
 *   <li>配置轮询
 *   <li>任务轮询
 *   <li>状态上报
 *   <li>健康监控
 *   <li>Arthas 集成
 * </ul>
 *
 * <p>重构后的职责分配：
 *
 * <ul>
 *   <li>{@link ConnectionStateManager} - 连接状态管理
 *   <li>{@link ScheduledTaskManager} - 调度任务管理
 *   <li>{@link HealthCheckCoordinator} - 健康检查协调
 *   <li>{@link ControlPlaneStatistics} - 统计信息管理
 * </ul>
 */
public final class ControlPlaneManager implements Closeable {

  private static final Logger logger = Logger.getLogger(ControlPlaneManager.class.getName());

  // 任务名称常量
  private static final String TASK_CONFIG_POLL = "config-poll";
  private static final String TASK_TASK_POLL = "task-poll";
  private static final String TASK_STATUS_REPORT = "status-report";
  private static final String TASK_CLEANUP = "cleanup";

  // 配置
  private final ControlPlaneConfig config;

  // 核心组件
  private final ConnectionStateManager connectionStateManager;
  private final ScheduledTaskManager taskManager;
  private final HealthCheckCoordinator healthCheckCoordinator;
  private final ControlPlaneStatistics statistics;

  // 业务组件
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

  // Arthas 集成
  @Nullable private final ArthasIntegration arthasIntegration;

  // 生命周期状态
  private final AtomicBoolean started;
  private final AtomicBoolean closed;

  private ControlPlaneManager(Builder builder) {
    // 验证必需参数
    this.config = Objects.requireNonNull(builder.config, "config is required");
    this.healthMonitor = Objects.requireNonNull(builder.healthMonitor, "healthMonitor is required");
    this.configManager = Objects.requireNonNull(builder.configManager, "configManager is required");
    this.dynamicSampler =
        Objects.requireNonNull(builder.dynamicSampler, "dynamicSampler is required");

    // 初始化核心组件
    this.connectionStateManager = new ConnectionStateManager();
    this.taskManager = ScheduledTaskManager.createDefault();

    // 初始化业务组件
    this.resultPersistence = TaskResultPersistence.create(this.config);
    this.agentIdentity = AgentIdentityProvider.get();
    this.client = ControlPlaneClient.create(this.config, this.healthMonitor);

    // 初始化状态收集器
    this.statusAggregator = new AgentStatusAggregator();
    this.controlPlaneStateCollector = new ControlPlaneStateCollector();
    this.uptimeCollector = new UptimeCollector();
    initializeStatusCollectors();

    // 初始化健康检查协调器
    this.healthCheckCoordinator =
        new HealthCheckCoordinator(this.healthMonitor, this.connectionStateManager);

    // 初始化统计管理器
    this.statistics =
        new ControlPlaneStatistics(
            this.controlPlaneStateCollector,
            this.connectionStateManager,
            this.healthCheckCoordinator,
            this.config.getControlPlaneUrl() + "/config");

    // 初始化心跳上报器
    this.heartbeatReporter =
        HeartbeatReporter.builder()
            .setConfig(this.config)
            .setClient(this.client)
            .setStatusAggregator(this.statusAggregator)
            .setScheduler(this.taskManager.getScheduler())
            .setListener(this::onHeartbeatComplete)
            .build();

    // Arthas 集成
    this.arthasIntegration = builder.arthasIntegration;

    // 生命周期状态
    this.started = new AtomicBoolean(false);
    this.closed = new AtomicBoolean(false);
  }

  /**
   * 创建 Builder
   *
   * @return Builder 实例
   */
  public static Builder builder() {
    return new Builder();
  }

  /** 初始化状态收集器 */
  private void initializeStatusCollectors() {
    statusAggregator.registerCollector(new IdentityCollector());
    statusAggregator.registerCollector(uptimeCollector);
    statusAggregator.registerCollector(controlPlaneStateCollector);
    statusAggregator.registerCollector(new OtlpHealthCollector(healthMonitor));
    statusAggregator.registerCollector(
        new SystemResourceCollector(config.isIncludeSystemResource()));

    logger.log(
        Level.FINE,
        "Initialized {0} status collectors: {1}",
        new Object[] {statusAggregator.getCollectorCount(), statusAggregator.getCollectorNames()});
  }

  /** 启动控制平面管理器 */
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

    // 启动健康检查协调器
    healthCheckCoordinator.start();

    // 调度各项任务
    scheduleTasks();

    // 启动心跳上报
    heartbeatReporter.start();

    // 启动 Arthas 集成
    if (arthasIntegration != null) {
      arthasIntegration.start(taskManager.getScheduler());
    }

    connectionStateManager.markConnecting();
    logger.log(Level.INFO, "Control plane manager started");
  }

  /** 调度所有任务 */
  private void scheduleTasks() {
    // 配置轮询任务
    taskManager.scheduleTask(
        TaskConfig.create(
            TASK_CONFIG_POLL,
            new ConfigPollTask(
                client, connectionStateManager, healthCheckCoordinator, statistics),
            config.getConfigPollInterval()));

    // 任务轮询任务
    taskManager.scheduleTask(
        TaskConfig.create(
            TASK_TASK_POLL,
            new TaskPollTask(connectionStateManager, statistics),
            Duration.ofSeconds(1),
            config.getTaskPollInterval()));

    // 状态上报任务
    taskManager.scheduleTask(
        TaskConfig.create(
            TASK_STATUS_REPORT,
            new StatusReportTask(resultPersistence, statistics),
            Duration.ofSeconds(5),
            config.getStatusReportInterval()));

    // 清理任务（每小时）
    taskManager.scheduleTask(
        TaskConfig.create(
            TASK_CLEANUP,
            new CleanupTask(resultPersistence),
            Duration.ofHours(1),
            Duration.ofHours(1)));
  }

  /** 停止控制平面管理器 */
  public void stop() {
    if (!started.get() || closed.get()) {
      return;
    }

    logger.log(Level.INFO, "Stopping control plane manager...");

    // 取消所有任务
    taskManager.cancelAllTasks();

    // 停止心跳上报
    heartbeatReporter.stop();

    // 停止健康检查协调器
    healthCheckCoordinator.stop();

    // 停止 Arthas 集成
    if (arthasIntegration != null) {
      arthasIntegration.stop();
    }

    // 更新运行状态
    uptimeCollector.setRunningState(UptimeCollector.RunningState.STOPPED);
    connectionStateManager.setState(ConnectionState.DISCONNECTED);

    logger.log(Level.INFO, "Control plane manager stopped");
  }

  @Override
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      stop();

      taskManager.close();
      heartbeatReporter.close();
      client.close();

      if (arthasIntegration != null) {
        arthasIntegration.close();
      }

      logger.log(Level.INFO, "Control plane manager closed");
    }
  }

  // ==================== Getter 方法 ====================

  /**
   * 获取连接状态
   *
   * @return 连接状态
   */
  public ConnectionState getConnectionState() {
    return connectionStateManager.getState();
  }

  /**
   * 获取健康监控器
   *
   * @return 健康监控器
   */
  public OtlpHealthMonitor getHealthMonitor() {
    return healthMonitor;
  }

  /**
   * 获取动态配置管理器
   *
   * @return 配置管理器
   */
  public DynamicConfigManager getConfigManager() {
    return configManager;
  }

  /**
   * 获取动态采样器
   *
   * @return 动态采样器
   */
  public DynamicSampler getDynamicSampler() {
    return dynamicSampler;
  }

  /**
   * 获取状态聚合器
   *
   * @return 状态聚合器
   */
  public AgentStatusAggregator getStatusAggregator() {
    return statusAggregator;
  }

  /**
   * 获取心跳上报器
   *
   * @return 心跳上报器
   */
  public HeartbeatReporter getHeartbeatReporter() {
    return heartbeatReporter;
  }

  /**
   * 获取 Arthas 集成
   *
   * @return Arthas 集成，如果未启用则返回 null
   */
  @Nullable
  public ArthasIntegration getArthasIntegration() {
    return arthasIntegration;
  }

  /**
   * 获取连接状态管理器
   *
   * @return 连接状态管理器
   */
  public ConnectionStateManager getConnectionStateManager() {
    return connectionStateManager;
  }

  /**
   * 获取调度任务管理器
   *
   * @return 调度任务管理器
   */
  public ScheduledTaskManager getTaskManager() {
    return taskManager;
  }

  /**
   * 获取统计信息管理器
   *
   * @return 统计信息管理器
   */
  public ControlPlaneStatistics getStatistics() {
    return statistics;
  }

  // ==================== 回调方法 ====================

  /** 心跳完成回调 */
  private void onHeartbeatComplete(
      boolean success,
      @Nullable Map<String, Object> statusData,
      @Nullable String error) {
    statistics.recordStatusReport();

    if (success) {
      logger.log(Level.FINE, "Heartbeat completed successfully");
    } else {
      logger.log(Level.WARNING, "Heartbeat failed: {0}", error);
    }
  }

  // ==================== Builder ====================

  /** Builder for {@link ControlPlaneManager}. */
  public static final class Builder {
    @Nullable private ControlPlaneConfig config;
    @Nullable private OtlpHealthMonitor healthMonitor;
    @Nullable private DynamicConfigManager configManager;
    @Nullable private DynamicSampler dynamicSampler;
    @Nullable private ArthasIntegration arthasIntegration;

    private Builder() {}

    /**
     * 设置配置
     *
     * @param config 控制平面配置
     * @return this builder
     */
    public Builder setConfig(ControlPlaneConfig config) {
      this.config = config;
      return this;
    }

    /**
     * 设置健康监控器
     *
     * @param healthMonitor 健康监控器
     * @return this builder
     */
    public Builder setHealthMonitor(OtlpHealthMonitor healthMonitor) {
      this.healthMonitor = healthMonitor;
      return this;
    }

    /**
     * 设置配置管理器
     *
     * @param configManager 配置管理器
     * @return this builder
     */
    public Builder setConfigManager(DynamicConfigManager configManager) {
      this.configManager = configManager;
      return this;
    }

    /**
     * 设置动态采样器
     *
     * @param dynamicSampler 动态采样器
     * @return this builder
     */
    public Builder setDynamicSampler(DynamicSampler dynamicSampler) {
      this.dynamicSampler = dynamicSampler;
      return this;
    }

    /**
     * 设置 Arthas 集成
     *
     * @param arthasIntegration Arthas 集成
     * @return this builder
     */
    public Builder setArthasIntegration(ArthasIntegration arthasIntegration) {
      this.arthasIntegration = arthasIntegration;
      return this;
    }

    /**
     * 设置 Arthas 配置并创建集成
     *
     * <p>如果 ArthasConfig 没有显式配置 Tunnel 端点，将自动基于 ControlPlaneConfig 的 OTLP endpoint 生成默认值。
     * 默认规则：http(s)://host:port → ws(s)://host:port/v1/arthas/ws
     *
     * @param arthasConfig Arthas 配置
     * @return this builder
     */
    public Builder setArthasConfig(ArthasConfig arthasConfig) {
      if (arthasConfig != null && arthasConfig.isEnabled()) {
        // 如果没有显式配置 Tunnel 端点，注入 OTLP endpoint 用于生成默认值
        if (!arthasConfig.hasExplicitTunnelEndpoint() && this.config != null) {
          arthasConfig =
              ArthasConfig.builder()
                  .setEnabled(arthasConfig.isEnabled())
                  .setVersion(arthasConfig.getVersion())
                  .setMaxSessionsPerAgent(arthasConfig.getMaxSessionsPerAgent())
                  .setSessionIdleTimeout(arthasConfig.getSessionIdleTimeout())
                  .setSessionMaxDuration(arthasConfig.getSessionMaxDuration())
                  .setIdleShutdownDelay(arthasConfig.getIdleShutdownDelay())
                  .setMaxRunningDuration(arthasConfig.getMaxRunningDuration())
                  .setTunnelEndpoint(arthasConfig.getExplicitTunnelEndpoint())
                  .setTunnelReconnectInterval(arthasConfig.getTunnelReconnectInterval())
                  .setTunnelMaxReconnectAttempts(arthasConfig.getTunnelMaxReconnectAttempts())
                  .setTunnelConnectTimeout(arthasConfig.getTunnelConnectTimeout())
                  .setTunnelPingInterval(arthasConfig.getTunnelPingInterval())
                  .setLibPath(arthasConfig.getLibPath())
                  .setDisabledCommands(arthasConfig.getDisabledCommands())
                  .setCommandTimeout(arthasConfig.getCommandTimeout())
                  .setOutputBufferSize(arthasConfig.getOutputBufferSize())
                  .setOutputFlushInterval(arthasConfig.getOutputFlushInterval())
                  .setBaseOtlpEndpoint(this.config.getEndpoint())
                  .build();

          logger.log(
              Level.INFO,
              "Arthas tunnel endpoint not explicitly configured, using default based on OTLP endpoint: {0}",
              arthasConfig.getTunnelEndpoint());
        }
        this.arthasIntegration = ArthasIntegration.create(arthasConfig);
      }
      return this;
    }

    /**
     * 启用 Arthas 功能（使用默认配置）
     *
     * <p>Tunnel 端点将自动基于 ControlPlaneConfig 的 OTLP endpoint 生成。
     *
     * @return this builder
     */
    public Builder enableArthas() {
      return setArthasConfig(ArthasConfig.builder().setEnabled(true).build());
    }

    /**
     * 构建控制平面管理器
     *
     * @return 控制平面管理器
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
