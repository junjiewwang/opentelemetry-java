/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane;

import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasConfig;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasIntegration;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneService;
import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import io.opentelemetry.sdk.extension.controlplane.core.ConnectionStateManager;
import io.opentelemetry.sdk.extension.controlplane.core.ConnectionStateManager.ConnectionState;
import io.opentelemetry.sdk.extension.controlplane.core.ControlPlaneComponent;
import io.opentelemetry.sdk.extension.controlplane.core.ControlPlaneStatistics;
import io.opentelemetry.sdk.extension.controlplane.core.HealthCheckCoordinator;
import io.opentelemetry.sdk.extension.controlplane.core.ScheduledTaskManager;
import io.opentelemetry.sdk.extension.controlplane.core.TaskExecutorProvider;
import io.opentelemetry.sdk.extension.controlplane.core.longpoll.LongPollConfig;
import io.opentelemetry.sdk.extension.controlplane.core.longpoll.LongPollCoordinator;
import io.opentelemetry.sdk.extension.controlplane.core.longpoll.LongPollType;
import io.opentelemetry.sdk.extension.controlplane.core.longpoll.TaskLongPollHandler;
import io.opentelemetry.sdk.extension.controlplane.dynamic.DynamicConfigManager;
import io.opentelemetry.sdk.extension.controlplane.dynamic.DynamicConfigManager.ServerMetadataListener;
import io.opentelemetry.sdk.extension.controlplane.dynamic.DynamicSampler;
import io.opentelemetry.sdk.extension.controlplane.identity.AgentIdentityProvider;
import io.opentelemetry.sdk.extension.controlplane.status.ControlPlaneStateCollector;
import io.opentelemetry.sdk.extension.controlplane.status.HeartbeatReporter;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskDispatcher;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutor;
import java.io.Closeable;
import java.lang.instrument.Instrumentation;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
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
 *   <li>长轮询（配置和任务）
 *   <li>状态上报
 *   <li>健康监控
 *   <li>可扩展组件（如 Arthas 集成）
 * </ul>
 *
 * <p>重构后的架构设计：
 *
 * <ul>
 *   <li>{@link ControlPlaneComponent} - 统一的组件生命周期接口
 *   <li>{@link TaskExecutorProvider} - 任务执行器提供者接口（解耦任务注册）
 *   <li>{@link ServerMetadataListener} - 服务端元数据监听器接口
 * </ul>
 *
 * <p>遵循开闭原则（OCP）：新增组件或任务类型无需修改 Manager 代码，只需实现相应接口。
 */
public final class ControlPlaneManager implements Closeable {

  private static final Logger logger = Logger.getLogger(ControlPlaneManager.class.getName());

  // 配置
  private final ControlPlaneConfig config;

  // 核心组件
  private final ConnectionStateManager connectionStateManager;
  private final ScheduledTaskManager taskManager;
  private final HealthCheckCoordinator healthCheckCoordinator;
  private final ControlPlaneStatistics statistics;
  private final LongPollCoordinator longPollCoordinator;

  // 业务组件（Phase 5: 使用 ControlPlaneService）
  private final ControlPlaneService service;
  private final DynamicConfigManager configManager;
  private final DynamicSampler dynamicSampler;
  private final AgentIdentityProvider.AgentIdentity agentIdentity;

  // 状态收集和心跳上报
  private final HeartbeatReporter heartbeatReporter;

  // 可扩展组件列表（统一生命周期管理）
  private final List<ControlPlaneComponent> components;

  // Arthas 集成（保留直接引用以便 Getter 访问）
  @Nullable private final ArthasIntegration arthasIntegration;

  // 任务分发器
  @Nullable private TaskDispatcher taskDispatcher;

  // 生命周期状态
  private final AtomicBoolean started;
  private final AtomicBoolean closed;

  private ControlPlaneManager(Builder builder) {
    // 验证必需参数
    this.config = Objects.requireNonNull(builder.config, "config is required");
    this.configManager = Objects.requireNonNull(builder.configManager, "configManager is required");
    this.dynamicSampler =
        Objects.requireNonNull(builder.dynamicSampler, "dynamicSampler is required");

    // 初始化核心组件
    this.connectionStateManager = new ConnectionStateManager();
    this.taskManager = ScheduledTaskManager.createDefault();

    // 初始化业务组件
    this.agentIdentity = AgentIdentityProvider.get();
    // Phase 5: 直接使用 ControlPlaneService（Protobuf-only）
    this.service = ControlPlaneService.create(this.config);

    // 初始化状态收集器
    ControlPlaneStateCollector controlPlaneStateCollector = new ControlPlaneStateCollector();

    // 初始化心跳上报器（Phase 5: 使用 ControlPlaneService）
    // 注意：心跳上报器需要在健康检查协调器之前初始化
    this.heartbeatReporter =
        HeartbeatReporter.builder()
            .setConfig(this.config)
            .setService(this.service)
            .setScheduler(this.taskManager.getScheduler())
            .setListener(this::onHeartbeatComplete)
            .build();

    // 初始化健康检查协调器（使用心跳作为健康判断依据）
    this.healthCheckCoordinator =
        new HealthCheckCoordinator(this.heartbeatReporter, this.connectionStateManager);

    // 初始化统计管理器
    this.statistics =
        new ControlPlaneStatistics(
            controlPlaneStateCollector,
            this.connectionStateManager,
            this.healthCheckCoordinator,
            this.config.getControlPlaneUrl() + "/config");

    // 初始化长轮询协调器
    LongPollConfig longPollConfig =
        LongPollConfig.builder()
            .setTimeout(this.config.getLongPollTimeout())
            .setMinRetryInterval(this.config.getRetryInitialBackoff())
            .setMaxRetryInterval(this.config.getRetryMaxBackoff())
            .setBackoffMultiplier(this.config.getRetryBackoffMultiplier())
            .setMaxConsecutiveErrors(this.config.getRetryMaxAttempts())
            .build();

    // Phase 5: LongPollCoordinator 直接使用 ControlPlaneService
    this.longPollCoordinator =
        new LongPollCoordinator(
            longPollConfig,
            this.service,
            this.connectionStateManager,
            this.healthCheckCoordinator,
            this.statistics);

    // 设置 DynamicConfigManager 以便 ConfigLongPollHandler 可以应用配置
    this.longPollCoordinator.setConfigManager(this.configManager);

    // 初始化可扩展组件列表
    this.components = new CopyOnWriteArrayList<>();

    // Arthas 集成（作为可扩展组件注册）
    this.arthasIntegration = builder.arthasIntegration;
    if (this.arthasIntegration != null) {
      this.components.add(this.arthasIntegration);
    }

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
    configManager.registerComponent(DynamicConfigManager.ConfigKeys.SAMPLER, dynamicSampler);

    // 自动注册服务端元数据监听器（遍历所有实现 ServerMetadataListener 的组件）
    registerServerMetadataListeners();

    // 启动健康检查协调器
    healthCheckCoordinator.start();

    // 调度各项任务（不包括配置和任务轮询，由长轮询协调器统一处理）
    scheduleTasks();

    // 启动长轮询协调器（替代 ConfigPollTask 和 TaskPollTask）
    longPollCoordinator.start();

    // 启动心跳上报
    heartbeatReporter.start();

    // 启动所有可扩展组件（统一生命周期管理）
    startComponents();

    // 初始化并配置任务分发器（在组件启动后，因为需要从组件获取执行器）
    initializeTaskDispatcher();

    connectionStateManager.markConnecting();
    logger.log(Level.INFO, "Control plane manager started with long polling");
  }

  /**
   * 启动所有可扩展组件
   *
   * <p>遵循统一生命周期管理，遍历所有注册的 {@link ControlPlaneComponent} 并启动。
   */
  private void startComponents() {
    for (ControlPlaneComponent component : components) {
      try {
        logger.log(Level.INFO, "Starting component: {0}", component.getComponentName());
        component.start(taskManager.getScheduler());
      } catch (RuntimeException e) {
        logger.log(Level.WARNING, "Failed to start component: " + component.getComponentName(), e);
      }
    }
  }

  /**
   * 停止所有可扩展组件
   *
   * <p>遵循统一生命周期管理，遍历所有注册的 {@link ControlPlaneComponent} 并停止。
   */
  private void stopComponents() {
    for (ControlPlaneComponent component : components) {
      try {
        logger.log(Level.INFO, "Stopping component: {0}", component.getComponentName());
        component.stop();
      } catch (RuntimeException e) {
        logger.log(Level.WARNING, "Failed to stop component: " + component.getComponentName(), e);
      }
    }
  }

  /**
   * 关闭所有可扩展组件
   *
   * <p>遵循统一生命周期管理，遍历所有注册的 {@link ControlPlaneComponent} 并关闭。
   */
  private void closeComponents() {
    for (ControlPlaneComponent component : components) {
      try {
        logger.log(Level.INFO, "Closing component: {0}", component.getComponentName());
        component.close();
      } catch (Exception e) {
        logger.log(Level.WARNING, "Failed to close component: " + component.getComponentName(), e);
      }
    }
  }

  /**
   * 自动注册服务端元数据监听器
   *
   * <p>遍历所有实现 {@link ServerMetadataListener} 的组件，自动注册到 {@link DynamicConfigManager}。
   * 遵循开闭原则：新增需要监听服务端元数据的组件，只需实现 {@link ServerMetadataListener} 接口。
   */
  private void registerServerMetadataListeners() {
    int registeredCount = 0;
    for (ControlPlaneComponent component : components) {
      if (component instanceof ServerMetadataListener) {
        ServerMetadataListener listener = (ServerMetadataListener) component;
        configManager.addServerMetadataListener(listener);
        logger.log(Level.INFO, "Registered ServerMetadataListener: {0}", component.getComponentName());
        registeredCount++;
      }
    }
    logger.log(Level.INFO, "Registered {0} ServerMetadataListener(s)", registeredCount);
  }

  /**
   * 初始化任务分发器
   *
   * <p>创建 TaskDispatcher，自动发现并注册所有组件提供的任务执行器，然后配置到 TaskLongPollHandler。
   * 遵循开闭原则：新增任务类型只需让组件实现 {@link TaskExecutorProvider} 接口。
   */
  private void initializeTaskDispatcher() {
    logger.log(Level.INFO, "[TASK-DISPATCHER-INIT] Initializing TaskDispatcher");

    // 创建任务分发器（Phase 5: 使用 ControlPlaneService）
    taskDispatcher = new TaskDispatcher(
        service,
        taskManager.getScheduler());

    // 自动注册任务执行器（遍历所有实现 TaskExecutorProvider 的组件）
    registerTaskExecutors();

    // 配置到 TaskLongPollHandler
    TaskLongPollHandler taskHandler = longPollCoordinator.getHandler(LongPollType.TASK);
    if (taskHandler != null) {
      taskHandler.setTaskDispatcher(taskDispatcher);
      logger.log(
          Level.INFO,
          "[TASK-DISPATCHER-INIT] TaskDispatcher configured with {0} executor(s), registered types: {1}",
          new Object[] {taskDispatcher.getExecutorCount(), taskDispatcher.getRegisteredTaskTypes()});
    } else {
      logger.log(Level.WARNING, "[TASK-DISPATCHER-INIT] TaskLongPollHandler not found, tasks will not be executed");
    }
  }

  /**
   * 自动注册任务执行器
   *
   * <p>遍历所有实现 {@link TaskExecutorProvider} 的组件，自动发现并注册其提供的任务执行器。
   * 遵循开闭原则：新增任务类型只需让组件实现 {@link TaskExecutorProvider} 接口，无需修改 Manager 代码。
   */
  private void registerTaskExecutors() {
    if (taskDispatcher == null) {
      return;
    }

    List<String> registeredTypes = new ArrayList<>();

    for (ControlPlaneComponent component : components) {
      if (component instanceof TaskExecutorProvider) {
        TaskExecutorProvider provider = (TaskExecutorProvider) component;
        List<TaskExecutor> executors = provider.getTaskExecutors();

        for (TaskExecutor executor : executors) {
          taskDispatcher.registerExecutor(executor);
          registeredTypes.add(executor.getTaskType());
          logger.log(
              Level.INFO,
              "Registered TaskExecutor: type={0}, from={1}",
              new Object[] {executor.getTaskType(), component.getComponentName()});
        }
      }
    }

    logger.log(
        Level.INFO,
        "[TASK-EXECUTOR-REGISTER] Registered {0} executor(s) from components: {1}",
        new Object[] {registeredTypes.size(), registeredTypes});
  }

  /** 调度所有任务（不包括配置和任务轮询） */
  @SuppressWarnings("MethodCanBeStatic")
  private void scheduleTasks() {
    // 目前无需额外调度任务
  }

  /** 停止控制平面管理器 */
  public void stop() {
    if (!started.get() || closed.get()) {
      return;
    }

    logger.log(Level.INFO, "Stopping control plane manager...");

    // 停止长轮询协调器
    longPollCoordinator.stop();

    // 取消所有任务
    taskManager.cancelAllTasks();

    // 停止心跳上报
    heartbeatReporter.stop();

    // 停止健康检查协调器
    healthCheckCoordinator.stop();

    // 停止所有可扩展组件（统一生命周期管理）
    stopComponents();

    // 关闭任务分发器
    if (taskDispatcher != null) {
      taskDispatcher.close();
      taskDispatcher = null;
    }

    // 更新连接状态
    connectionStateManager.setState(ConnectionState.DISCONNECTED);

    logger.log(Level.INFO, "Control plane manager stopped");
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      stop();

      longPollCoordinator.close();
      taskManager.close();
      heartbeatReporter.close();
      service.close();

      // 关闭所有可扩展组件（统一生命周期管理）
      closeComponents();

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

  /**
   * 获取长轮询协调器
   *
   * @return 长轮询协调器
   */
  public LongPollCoordinator getLongPollCoordinator() {
    return longPollCoordinator;
  }

  /**
   * 获取已注册的组件列表
   *
   * @return 组件列表（只读）
   */
  public List<ControlPlaneComponent> getComponents() {
    return new ArrayList<>(components);
  }

  // ==================== 回调方法 ====================

  /** 心跳完成回调 */
  private void onHeartbeatComplete(
      boolean success, @Nullable String error) {
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
    @Nullable private DynamicConfigManager configManager;
    @Nullable private DynamicSampler dynamicSampler;
    @Nullable private ArthasIntegration arthasIntegration;
    @Nullable private Instrumentation instrumentation;

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
        // 如果没有显式配置 Tunnel 端点或 AuthToken，从 ControlPlaneConfig 继承
        if (this.config != null 
            && (!arthasConfig.hasExplicitTunnelEndpoint() || !arthasConfig.hasAuthToken())) {
          ArthasConfig.Builder builder = ArthasConfig.builder()
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
              .setOutputFlushInterval(arthasConfig.getOutputFlushInterval());

          // 继承 OTLP endpoint 用于生成默认 Tunnel 端点
          if (!arthasConfig.hasExplicitTunnelEndpoint()) {
            builder.setBaseOtlpEndpoint(this.config.getEndpoint());
            logger.log(
                Level.INFO,
                "Arthas tunnel endpoint not explicitly configured, will use default based on OTLP endpoint");
          }

          // 继承 AuthToken 用于 Tunnel 认证
          if (!arthasConfig.hasAuthToken() && this.config.hasAuthToken()) {
            builder.setAuthToken(this.config.getAuthToken());
            logger.log(Level.INFO, "Arthas auth token inherited from ControlPlaneConfig");
          } else if (arthasConfig.hasAuthToken()) {
            builder.setAuthToken(arthasConfig.getAuthToken());
          }

          arthasConfig = builder.build();
        }
        this.arthasIntegration = ArthasIntegration.create(arthasConfig);
        // 如果已经设置了 Instrumentation，传递给 ArthasIntegration
        if (this.instrumentation != null) {
          this.arthasIntegration.setInstrumentation(this.instrumentation);
        }
      }
      return this;
    }

    /**
     * 设置 Instrumentation 实例
     *
     * <p>Instrumentation 用于 Arthas 加载 SpyAPI 到 Bootstrap ClassLoader
     * 和进行字节码增强。
     *
     * @param instrumentation Instrumentation 实例
     * @return this builder
     */
    public Builder setInstrumentation(@Nullable Instrumentation instrumentation) {
      this.instrumentation = instrumentation;
      // 如果 ArthasIntegration 已经创建，传递 Instrumentation
      if (this.arthasIntegration != null && instrumentation != null) {
        this.arthasIntegration.setInstrumentation(instrumentation);
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
