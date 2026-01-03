/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.ArthasTunnelClient;
import java.io.Closeable;
import java.lang.instrument.Instrumentation;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Arthas 集成入口
 *
 * <p>协调 Arthas 生命周期管理、会话管理、Tunnel 连接、终端桥接等组件。
 */
public final class ArthasIntegration
    implements Closeable,
        ArthasLifecycleManager.LifecycleEventListener,
        ArthasSessionManager.SessionEventListener,
        ArthasTunnelClient.TunnelEventListener,
        ArthasTerminalBridge.OutputHandler {

  private static final Logger logger = Logger.getLogger(ArthasIntegration.class.getName());

  private final ArthasConfig config;
  private final ArthasLifecycleManager lifecycleManager;
  private final ArthasSessionManager sessionManager;
  private final ArthasTunnelClient tunnelClient;
  private final ArthasTerminalBridge terminalBridge;
  private final ArthasEnvironmentDetector.Environment environment;

  /** Tunnel 注册状态（与 Arthas 生命周期解耦） */
  private final AtomicBoolean tunnelRegistered = new AtomicBoolean(false);

  /** 统一状态事件总线：集中 publish/subscribe/await */
  private final ArthasStateEventBus stateEventBus = new ArthasStateEventBus();

  @Nullable private ScheduledExecutorService scheduler;

  /** 统一状态事件监听器（轻量事件总线）：供 TaskDispatcher 等外部组件订阅 Arthas/Tunnel 状态变化 */
  private final ReplayableListeners<StatusEventListener> statusEventListeners =
      new ReplayableListeners<>(new CopyOnWriteArrayList<>());

  public interface StatusEventListener {
    void onTunnelConnected();

    void onTunnelRegistered();

    void onTunnelDisconnected(String reason);
  }

  public void addStatusEventListener(StatusEventListener listener) {
    // 向后兼容：旧接口仍保留，但底层完全由 stateEventBus 提供无竞态订阅能力。
    // 这里仍保留一份列表，仅用于 removeStatusEventListener 的语义和避免 breaking change。
    statusEventListeners.add(listener);

    stateEventBus.subscribe(
        (event, state, detail) -> {
          if (!statusEventListeners.snapshot().contains(listener)) {
            return;
          }
          switch (event) {
            case TUNNEL_CONNECTED:
              listener.onTunnelConnected();
              break;
            case TUNNEL_REGISTERED:
              listener.onTunnelRegistered();
              break;
            case TUNNEL_DISCONNECTED:
              listener.onTunnelDisconnected(detail != null ? detail : "unknown");
              break;
            default:
              break;
          }
        },
        /* replay= */ true);
  }

  public void removeStatusEventListener(StatusEventListener listener) {
    statusEventListeners.remove(listener);
  }

  /** 供外部使用：基于 predicate 等待状态达成（事件驱动，非阻塞/非轮询） */
  public java.util.concurrent.CompletableFuture<ArthasStateEventBus.State> awaitState(
      java.util.function.Predicate<ArthasStateEventBus.State> predicate,
      java.time.Duration timeout) {
    return stateEventBus.await(predicate, timeout);
  }

  /**
   * 小型工具类：监听器容器 + 订阅时无竞态回放。
   *
   * <p>目的：把“订阅后立即回放当前状态”的逻辑集中起来，避免散落在各处。
   */
  private static final class ReplayableListeners<T> {
    private final List<T> listeners;

    private ReplayableListeners(List<T> listeners) {
      this.listeners = listeners;
    }

    void add(T listener) {
      listeners.add(listener);
    }

    void remove(T listener) {
      listeners.remove(listener);
    }

    List<T> snapshot() {
      return listeners;
    }


  }

  private ArthasIntegration(ArthasConfig config) {
    this.config = config;
    this.environment = ArthasEnvironmentDetector.detect();
    this.lifecycleManager = new ArthasLifecycleManager(config, this);
    this.sessionManager = new ArthasSessionManager(config, this);
    this.tunnelClient = new ArthasTunnelClient(config, this);
    this.terminalBridge = new ArthasTerminalBridge(config, this);

    // 设置会话管理器到 Tunnel 客户端
    this.tunnelClient.setSessionManager(sessionManager);

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

    // 启动会话清理任务
    sessionManager.startCleanupTask(scheduler);

    // 启动 Tunnel 客户端
    tunnelClient.start(scheduler);

    logger.log(
        Level.INFO,
        "Arthas integration started, environment: {0}",
        environment);
  }

  /** 停止 Arthas 集成 */
  public void stop() {
    logger.log(Level.INFO, "Stopping Arthas integration");

    // 关闭所有会话
    sessionManager.closeAllSessions();

    // 停止 Arthas
    lifecycleManager.stop();

    // 停止会话清理任务
    sessionManager.stopCleanupTask();

    // 关闭终端桥接器
    terminalBridge.close();
  }

  @Override
  public void close() {
    stop();
    tunnelClient.close();
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

  /** 获取会话管理器 */
  public ArthasSessionManager getSessionManager() {
    return sessionManager;
  }

  /** 获取 Tunnel 客户端 */
  public ArthasTunnelClient getTunnelClient() {
    return tunnelClient;
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

  /** Tunnel 是否已就绪（连接成功且已注册） */
  public boolean isTunnelReady() {
    return tunnelClient.isConnected() && tunnelRegistered.get();
  }

  /**
   * 获取 Tunnel 未就绪的原因（用于诊断/日志）
   *
   * @return 为空表示已就绪
   */
  @Nullable
  public String getTunnelNotReadyReason() {
    if (!tunnelClient.isConnected()) {
      return "TUNNEL_NOT_CONNECTED";
    }
    if (!tunnelRegistered.get()) {
      return "TUNNEL_NOT_REGISTERED";
    }
    return null;
  }

  /** 终端是否可绑定到 Arthas（Capability 视角） */
  public boolean isTerminalBindable() {
    return isTunnelReady() && lifecycleManager.getState() == ArthasLifecycleManager.State.RUNNING;
  }

  /**
   * 获取终端不可绑定原因（用于拒绝/日志）
   *
   * @return 为空表示可绑定
   */
  @Nullable
  public String getTerminalNotBindableReason() {
    String tunnelReason = getTunnelNotReadyReason();
    if (tunnelReason != null) {
      return tunnelReason;
    }
    ArthasLifecycleManager.State s = lifecycleManager.getState();
    if (s != ArthasLifecycleManager.State.RUNNING) {
      return "ARTHAS_NOT_RUNNING:" + s;
    }
    return null;
  }

  /** 获取状态信息（用于状态上报） */
  public Map<String, Object> getStatusInfo() {
    Map<String, Object> status = new LinkedHashMap<>();
    status.put("enabled", config.isEnabled());
    status.put("arthasState", lifecycleManager.getState().name());
    status.put("activeSessions", sessionManager.getActiveSessionCount());
    status.put("maxSessions", config.getMaxSessionsPerAgent());
    status.put("tunnelConnected", tunnelClient.isConnected());
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
    tunnelClient.sendArthasStatus();

    // 通知所有等待的会话
    for (String sessionId : sessionManager.getSessionIds()) {
      ArthasSession session = sessionManager.getSession(sessionId);
      if (session != null) {
        terminalBridge.createSessionTerminal(session);
      }
    }
  }

  @Override
  public void onArthasStopped() {
    logger.log(Level.INFO, "Arthas stopped callback");
    stateEventBus.publishArthasState(lifecycleManager.getState());
    // 关闭所有终端
    for (String sessionId : sessionManager.getSessionIds()) {
      terminalBridge.destroySessionTerminal(sessionId);
      tunnelClient.sendTerminalClosed(sessionId, "arthas_stopped");
    }
    sessionManager.closeAllSessions();
    tunnelClient.sendArthasStatus();
  }

  @Override
  public void onMaxDurationExceeded() {
    logger.log(Level.WARNING, "Arthas max duration exceeded, forcing shutdown");
    // 通知所有会话即将关闭
    for (String sessionId : sessionManager.getSessionIds()) {
      tunnelClient.sendTerminalClosed(sessionId, "max_duration_exceeded");
    }
  }

  // ===== SessionEventListener 实现 =====

  @Override
  @Nullable
  public String canCreateSession(ArthasSessionManager.SessionCreateRequest request) {
    // 业务判断必须纯粹：不做阻塞等待。等待逻辑下沉到请求处理层（TERMINAL_OPEN）。
    String reason = getTerminalNotBindableReason();
    return reason;
  }

  @Override
  public void onSessionCreated(ArthasSession session) {
    logger.log(Level.INFO, "Session created: {0}", session.getSessionId());

    // Phase 2：Capability gating
    if (!isTerminalBindable()) {
      String reason = String.valueOf(getTerminalNotBindableReason());
      logger.log(
          Level.WARNING,
          "Terminal is not bindable right now, session will run in echo mode: sessionId={0}, reason={1}",
          new Object[] {session.getSessionId(), reason});
    }

    // 创建会话终端（内部会根据 lifecycle 状态尝试绑定，失败则 echo）
    terminalBridge.createSessionTerminal(session);
    
    // 恢复 Arthas 为活跃状态
    lifecycleManager.markActive();
  }

  @Override
  public void onSessionClosed(ArthasSession session) {
    logger.log(Level.INFO, "Session closed: {0}", session.getSessionId());
    
    // 销毁会话终端
    terminalBridge.destroySessionTerminal(session.getSessionId());
    
    // 通知 Tunnel Server
    tunnelClient.sendTerminalClosed(session.getSessionId(), "session_closed");
  }

  @Override
  public void onAllSessionsClosed() {
    logger.log(Level.INFO, "All sessions closed, marking Arthas as idle");
    if (scheduler != null) {
      lifecycleManager.markIdle(scheduler);
    }
  }

  // ===== TunnelEventListener 实现 =====

  @Override
  public void onConnected() {
    logger.log(Level.INFO, "Tunnel connected");
    tunnelRegistered.set(false);
    stateEventBus.publishTunnelConnected();
    for (StatusEventListener l : statusEventListeners.snapshot()) {
      l.onTunnelConnected();
    }
    // 发送当前状态
    tunnelClient.sendArthasStatus();
  }

  @Override
  public void onAgentRegistered() {
    logger.log(Level.INFO, "Agent registered with server successfully");
    tunnelRegistered.set(true);

    stateEventBus.publishTunnelRegistered();

    for (StatusEventListener l : statusEventListeners.snapshot()) {
      l.onTunnelRegistered();
    }

    // 【阶段性重构】不再驱动 lifecycle 状态机，仅用于诊断日志
    lifecycleManager.markRegistered();

    // 注册成功后立即上报一次状态，减少 server/agent 视图不一致窗口
    tunnelClient.sendArthasStatus();
  }

  @Override
  public void onDisconnected(String reason) {
    logger.log(Level.WARNING, "Tunnel disconnected: {0}", reason);
    tunnelRegistered.set(false);
    stateEventBus.publishTunnelDisconnected(reason);
    for (StatusEventListener l : statusEventListeners.snapshot()) {
      l.onTunnelDisconnected(reason);
    }
  }

  @Override
  public void onError(Throwable error) {
    logger.log(Level.WARNING, "Tunnel error: {0}", error.getMessage());
  }

  @Override
  public void onMaxReconnectReached() {
    logger.log(Level.SEVERE, "Max reconnect attempts reached, tunnel connection abandoned");
  }

  @Override
  public void onArthasStartRequested() {
    logger.log(Level.INFO, "Arthas start requested via tunnel");
    if (scheduler != null) {
      stateEventBus.publishArthasState(ArthasLifecycleManager.State.STARTING);
      ArthasLifecycleManager.StartResult result = lifecycleManager.tryStart(scheduler);
      if (!result.isSuccess()) {
        String errorMsg = result.getErrorMessage() != null ? result.getErrorMessage() : "Unknown error";
        logger.log(Level.WARNING, "Failed to start Arthas: {0}", errorMsg);
        tunnelClient.sendError("ARTHAS_START_FAILED", errorMsg, null);
      }
    }
  }

  @Override
  public void onArthasStopRequested(@Nullable String reason) {
    logger.log(Level.INFO, "Arthas stop requested via tunnel, reason: {0}", reason);
    stateEventBus.publishArthasState(ArthasLifecycleManager.State.STOPPING);
    lifecycleManager.stop();
  }

  @Override
  public void onTerminalInput(String sessionId, String data) {
    // 更新会话活跃时间
    sessionManager.markSessionActive(sessionId);

    // 将输入转发给终端桥接器
    terminalBridge.handleInput(sessionId, data);
    
    logger.log(Level.FINE, "Terminal input forwarded for session {0}", sessionId);
  }

  @Override
  public void onTerminalResize(String sessionId, int cols, int rows) {
    // 调整终端尺寸
    terminalBridge.handleResize(sessionId, cols, rows);
    
    logger.log(
        Level.FINE,
        "Terminal resize handled for session {0}: {1}x{2}",
        new Object[] {sessionId, cols, rows});
  }

  @Override
  public String getArthasState() {
    return lifecycleManager.getState().name();
  }

  @Override
  public long getArthasUptimeMs() {
    return lifecycleManager.getUptimeMillis();
  }

  // ===== OutputHandler 实现（终端输出转发） =====

  @Override
  public void onOutput(String sessionId, byte[] data) {
    // 将终端输出通过 WebSocket 发送回服务端
    tunnelClient.sendBinaryOutput(sessionId, data);
    
    logger.log(Level.FINE, "Terminal output sent for session {0}, bytes: {1}", 
        new Object[]{sessionId, data.length});
  }
}
