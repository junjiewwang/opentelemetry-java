/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.io.Closeable;
import java.lang.instrument.Instrumentation;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 集成入口
 *
 * <p>协调 Arthas 生命周期管理、终端桥接等组件。
 *
 * <p>【模式2架构】由 Arthas 内部 TunnelClient(Netty) 负责 tunnel 连接：
 * <ul>
 *   <li>终端会话完全走官方 forward 通道，OTel 侧不再实现 TERMINAL_OPEN 协议</li>
 *   <li>通过 {@link ArthasTunnelStatusBridge} 观察 Arthas 内部 tunnel 状态</li>
 *   <li>保留 OTel 特有的任务状态上报、就绪判定、诊断日志等功能</li>
 * </ul>
 */
public final class ArthasIntegration
    implements Closeable,
        ArthasLifecycleManager.LifecycleEventListener,
        ArthasTerminalBridge.OutputHandler,
        ArthasTunnelStatusBridge.TunnelStatusListener {

  private static final Logger logger = Logger.getLogger(ArthasIntegration.class.getName());

  private final ArthasConfig config;
  private final ArthasLifecycleManager lifecycleManager;
  private final ArthasTerminalBridge terminalBridge;
  private final ArthasEnvironmentDetector.Environment environment;

  /** 【模式2核心】Tunnel 状态桥接器，从 Arthas 内部获取 tunnel 状态 */
  private final ArthasTunnelStatusBridge tunnelStatusBridge;

  /** Tunnel 注册状态（与 Arthas 生命周期解耦） */
  private final AtomicBoolean tunnelRegistered = new AtomicBoolean(false);

  /** 统一状态事件总线：集中 publish/subscribe/await */
  private final ArthasStateEventBus stateEventBus = new ArthasStateEventBus();

  /** 就绪门闩：集中判定/等待 Terminal 可交互能力 */
  private final ArthasReadinessGate readinessGate;

  @Nullable private ScheduledExecutorService scheduler;

  /** 供外部使用：基于 predicate 等待状态达成（事件驱动，非阻塞/非轮询） */
  public java.util.concurrent.CompletableFuture<ArthasStateEventBus.State> awaitState(
      java.util.function.Predicate<ArthasStateEventBus.State> predicate,
      java.time.Duration timeout) {
    return stateEventBus.await(predicate, timeout);
  }

  private ArthasIntegration(ArthasConfig config) {
    this.config = config;
    this.environment = ArthasEnvironmentDetector.detect();
    this.lifecycleManager = new ArthasLifecycleManager(config, this);
    this.lifecycleManager.setStateEventBus(stateEventBus);
    this.terminalBridge = new ArthasTerminalBridge(config, this);

    // readinessGate 依赖 lifecycleManager，必须在 lifecycleManager 初始化之后构造
    this.readinessGate = new ArthasReadinessGate(stateEventBus, lifecycleManager);

    // 【模式2核心】创建 Tunnel 状态桥接器（从 Arthas 内部获取 tunnel 状态）
    this.tunnelStatusBridge = new ArthasTunnelStatusBridge(
        lifecycleManager.getArthasBootstrap(),
        this, // TunnelStatusListener
        lifecycleManager.getStartupLogCollector());

    // 设置 Arthas Bootstrap 和生命周期管理器到终端桥接器
    this.terminalBridge.setArthasBootstrap(lifecycleManager.getArthasBootstrap());
    this.terminalBridge.setLifecycleManager(lifecycleManager);
  }

  /**
   * 创建 Arthas 集成实例
   *
   * @param config Arthas 配置
   * @return 集成实例
   */
  public static ArthasIntegration create(ArthasConfig config) {
    return new ArthasIntegration(config);
  }

  /**
   * 设置 Instrumentation 实例
   *
   * <p>Instrumentation 用于加载 SpyAPI 到 Bootstrap ClassLoader
   * 和传递给 Arthas 进行字节码增强。
   *
   * @param instrumentation Instrumentation 实例
   */
  public void setInstrumentation(@Nullable Instrumentation instrumentation) {
    lifecycleManager.setInstrumentation(instrumentation);
    if (instrumentation != null) {
      logger.log(Level.INFO, "Instrumentation set for Arthas integration");
    }
  }

  /**
   * 启动 Arthas 集成
   *
   * @param scheduler 调度器
   */
  public void start(ScheduledExecutorService scheduler) {
    if (!config.isEnabled()) {
      logger.log(Level.INFO, "Arthas integration is disabled");
      return;
    }

    // 检查环境支持
    if (!environment.isArthasSupported()) {
      logger.log(
          Level.WARNING,
          "Arthas not supported in current environment: {0}",
          environment.getUnsupportedReason());
      return;
    }

    this.scheduler = scheduler;

    // 启动终端桥接器
    terminalBridge.start(scheduler);

    // 【模式2核心】启动 Tunnel 状态桥接器
    // 从 Arthas 内部获取 tunnel 状态，桥接到 OTel 状态事件总线
    tunnelStatusBridge.start(scheduler, 1000); // 每秒轮询一次

    logger.log(
        Level.INFO,
        "Arthas integration started (Mode2: official tunnel), environment: {0}",
        environment);
  }

  /** 停止 Arthas 集成 */
  public void stop() {
    logger.log(Level.INFO, "Stopping Arthas integration");

    // 停止 Arthas
    lifecycleManager.stop();

    // 关闭终端桥接器
    terminalBridge.close();
  }

  @Override
  public void close() {
    stop();
    tunnelStatusBridge.stop();
    lifecycleManager.close();
    logger.log(Level.INFO, "Arthas integration closed");
  }

  // ===== Getters =====

  /** 获取配置 */
  public ArthasConfig getConfig() {
    return config;
  }

  /** 获取生命周期管理器 */
  public ArthasLifecycleManager getLifecycleManager() {
    return lifecycleManager;
  }

  /** 获取终端桥接器 */
  public ArthasTerminalBridge getTerminalBridge() {
    return terminalBridge;
  }

  /** 获取环境信息 */
  public ArthasEnvironmentDetector.Environment getEnvironment() {
    return environment;
  }

  /** 检查 Arthas 是否正在运行 */
  public boolean isArthasRunning() {
    return lifecycleManager.isRunning();
  }

  /**
   * Tunnel 是否已就绪（Arthas 内部 tunnel 已连接且已注册）
   *
   * <p>【模式2】状态来源于 Arthas 内部 TunnelClient，通过 {@link ArthasTunnelStatusBridge} 桥接
   */
  public boolean isTunnelReady() {
    return tunnelStatusBridge.isRegistered();
  }

  /**
   * 获取就绪门闩。
   *
   * <p>用于将"可交互就绪"的判断/等待收敛到统一组件，避免各处重复实现。
   */
  public ArthasReadinessGate getReadinessGate() {
    return readinessGate;
  }

  /** 终端是否可绑定到 Arthas（Capability 视角） */
  public boolean isTerminalBindable() {
    return readinessGate.evaluateNow().isTerminalReady();
  }

  /**
   * 获取终端不可绑定原因（用于拒绝/日志）
   *
   * @return 为空表示可绑定
   */
  @Nullable
  public String getTerminalNotBindableReason() {
    ArthasReadinessGate.Result r = readinessGate.evaluateNow();
    if (r.isTerminalReady()) {
      return null;
    }
    // 兼容现有 reason 语义（字符串化原因）
    return r.toErrorCode() + ":" + r.getReasonCode();
  }

  /** 获取状态信息（用于状态上报） */
  public Map<String, Object> getStatusInfo() {
    Map<String, Object> status = new LinkedHashMap<>();
    status.put("enabled", config.isEnabled());
    status.put("arthasState", lifecycleManager.getState().name());
    status.put("tunnelStatus", tunnelStatusBridge.getCurrentStatus().name());
    status.put("tunnelRegistered", tunnelRegistered.get());
    status.put("tunnelReady", isTunnelReady());
    status.put("terminalBindable", isTerminalBindable());
    status.put("terminalNotBindableReason", getTerminalNotBindableReason());
    status.put("uptimeMs", lifecycleManager.getUptimeMillis());
    status.put("activeTerminals", terminalBridge.getActiveTerminalCount());

    // 环境信息
    Map<String, Object> env = new LinkedHashMap<>();
    env.put("os", environment.getOsType().name());
    env.put("arch", environment.getCpuArch().name());
    env.put("libc", environment.getLibcType().name());
    env.put("jdkAvailable", environment.isJdkAvailable());
    env.put("arthasSupported", environment.isArthasSupported());
    status.put("environment", env);

    return Collections.unmodifiableMap(status);
  }

  // ===== LifecycleEventListener 实现 =====

  @Override
  public void onArthasStarted() {
    logger.log(Level.INFO, "Arthas started callback");
    stateEventBus.publishArthasState(lifecycleManager.getState());
  }

  @Override
  public void onArthasStopped() {
    logger.log(Level.INFO, "Arthas stopped callback");
    stateEventBus.publishArthasState(lifecycleManager.getState());
    // 关闭所有终端
    terminalBridge.closeAllTerminals();
  }

  @Override
  public void onMaxDurationExceeded() {
    logger.log(Level.WARNING, "Arthas max duration exceeded, forcing shutdown");
  }

  // ===== OutputHandler 实现（终端输出转发） =====

  @Override
  public void onOutput(String sessionId, byte[] data) {
    // 【模式2】终端输出由 Arthas 内部 tunnel 自动转发，此处仅用于本地调试
    logger.log(Level.FINE, "Terminal output for session {0}, bytes: {1}", 
        new Object[]{sessionId, data.length});
  }

  // ===== TunnelStatusListener 实现（模式2：从 Arthas 内部获取 tunnel 状态） =====

  /**
   * 【模式2】Arthas 内部 TunnelClient 连接成功
   *
   * <p>由 {@link ArthasTunnelStatusBridge} 轮询检测到状态变化后回调。
   */
  @Override
  public void onTunnelConnected() {
    logger.log(Level.INFO, "[MODE2] Arthas internal tunnel connected");
    lifecycleManager.getStartupLogCollector()
        .addLog("INFO", "Arthas internal TunnelClient connected");
    stateEventBus.publishTunnelConnected();
  }

  /**
   * 【模式2】Arthas 内部 TunnelClient 注册成功
   *
   * <p>由 {@link ArthasTunnelStatusBridge} 轮询检测到 agentId 已分配后回调。
   * 这是"tunnel 完全就绪"的标志。
   */
  @Override
  public void onTunnelRegistered() {
    logger.log(Level.INFO, "[MODE2] Arthas internal tunnel registered");
    lifecycleManager.getStartupLogCollector()
        .addLog("INFO", "Arthas internal TunnelClient registered");
    tunnelRegistered.set(true);
    stateEventBus.publishTunnelRegistered();
    lifecycleManager.markRegistered();
  }

  /**
   * 【模式2】Arthas 内部 TunnelClient 断开连接
   *
   * <p>由 {@link ArthasTunnelStatusBridge} 轮询检测到连接断开后回调。
   */
  @Override
  public void onTunnelDisconnected(String reason) {
    logger.log(Level.WARNING, "[MODE2] Arthas internal tunnel disconnected: {0}", reason);
    lifecycleManager.getStartupLogCollector()
        .addLog("WARN", "Arthas internal TunnelClient disconnected: " + reason);
    tunnelRegistered.set(false);
    stateEventBus.publishTunnelDisconnected(reason);
  }

  // ===== 便捷方法：供外部触发 Arthas 启动/停止 =====

  /**
   * 尝试启动 Arthas
   *
   * @return 启动结果
   */
  public ArthasLifecycleManager.StartResult tryStartArthas() {
    if (scheduler == null) {
      return ArthasLifecycleManager.StartResult.failed("Scheduler not available");
    }
    return lifecycleManager.tryStart(scheduler);
  }

  /**
   * 停止 Arthas
   *
   * @return 是否成功停止
   */
  public boolean stopArthas() {
    return lifecycleManager.stop();
  }

  /**
   * 获取 Arthas 状态
   *
   * @return 状态名称
   */
  public String getArthasState() {
    return lifecycleManager.getState().name();
  }

  /**
   * 获取 Arthas 运行时长
   *
   * @return 运行时长（毫秒）
   */
  public long getArthasUptimeMs() {
    return lifecycleManager.getUptimeMillis();
  }
}
