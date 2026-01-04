/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import io.opentelemetry.sdk.extension.controlplane.InstrumentationHolder;
import java.io.Closeable;
import java.lang.instrument.Instrumentation;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 生命周期管理器
 *
 * <p>负责 Arthas 的启动、停止、状态管理，实现安全的按需启动和自动关闭机制。
 *
 * <p>状态机：
 *
 * <pre>
 *   STOPPED ──────► STARTING ──────► RUNNING ──────► IDLE ──────► STOPPING ──────► STOPPED
 *      ▲                                 │            │               │
 *      │                                 │            │               │
 *      └─────────────────────────────────┴────────────┴───────────────┘
 * </pre>
 */
public final class ArthasLifecycleManager implements Closeable {

  private static final Logger logger = Logger.getLogger(ArthasLifecycleManager.class.getName());

  // 启动看门狗：防止 Arthas 启动永久停留在 STARTING（例如反射调用内部阻塞）
  private static final long STARTUP_WATCHDOG_MILLIS = 25_000;

  /** Arthas 运行状态 */
  public enum State {
    /** 已停止 */
    STOPPED,
    /** 启动中 */
    STARTING,
    /** 运行中（本地启动成功） */
    RUNNING,
    /** 空闲（无活跃会话） */
    IDLE,
    /** 停止中 */
    STOPPING
  }

  private final ArthasConfig config;
  private final LifecycleEventListener listener;
  private final AtomicReference<State> state = new AtomicReference<>(State.STOPPED);
  private final ArthasBootstrap arthasBootstrap;

  /** Tunnel 注册状态（与本地生命周期解耦，但用于任务成功判定/诊断） */
  private final AtomicBoolean registered = new AtomicBoolean(false);

  /** 启动日志收集器（用于记录启动过程中的关键事件） */
  private final StartupLogCollector startupLogCollector = new StartupLogCollector();

  @Nullable private Instant startedAt;
  @Nullable private Instant idleSince;
  @Nullable private ScheduledFuture<?> idleShutdownTask;
  @Nullable private ScheduledFuture<?> maxDurationTask;
  @Nullable private ScheduledFuture<?> startupWatchdogTask;

  // 可选：用于统一事件总线发布状态（避免由上层重复 publish STARTING/STOPPING）
  @Nullable private ArthasStateEventBus stateEventBus;

  /**
   * 创建生命周期管理器
   *
   * @param config Arthas 配置
   * @param listener 生命周期事件监听器
   */
  public ArthasLifecycleManager(ArthasConfig config, LifecycleEventListener listener) {
    this.config = config;
    this.listener = listener;
    // 在构造时尽早尝试获取 Instrumentation（如果 agent 已注入）
    // 这样即便上层 wiring 漏传，也不会导致 Arthas 永久走 legacy。
    this.arthasBootstrap = new ArthasBootstrap(config, InstrumentationHolder.get());
  }

  /**
   * 获取 Arthas Bootstrap 实例
   *
   * @return ArthasBootstrap
   */
  public ArthasBootstrap getArthasBootstrap() {
    return arthasBootstrap;
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
    // 如果调用方显式传入为空，做一次兜底尝试：可能 agent 尚未完成注入，或存在初始化时序。
    Instrumentation effective = instrumentation != null ? instrumentation : InstrumentationHolder.get();
    arthasBootstrap.setInstrumentation(effective);
    if (effective != null) {
      logger.log(Level.INFO, "Instrumentation set for Arthas lifecycle manager");
      startupLogCollector.addLog("INFO", "Instrumentation configured");
    } else {
      startupLogCollector.addLog("WARN", "Instrumentation not available");
    }
  }

  /**
   * 设置统一状态事件总线。
   *
   * <p>生命周期管理器是状态变更的唯一权威来源之一，因此在这里发布状态事件可以避免时序分散在上层。
   */
  public void setStateEventBus(@Nullable ArthasStateEventBus stateEventBus) {
    this.stateEventBus = stateEventBus;
  }

  /**
   * 尝试启动 Arthas
   *
   * @param scheduler 调度器（用于定时任务）
   * @return 启动结果
   */
  public StartResult tryStart(ScheduledExecutorService scheduler) {
    // 新一轮启动尝试，先清空上次的注册状态
    registered.set(false);

    // 检查当前状态
    State currentState = state.get();
    if (currentState == State.RUNNING || currentState == State.IDLE) {
      logger.log(Level.FINE, "Arthas already running, state: {0}", currentState);
      return StartResult.success();
    }

    if (currentState == State.STARTING) {
      logger.log(Level.FINE, "Arthas is starting, please wait");
      return StartResult.failed("Arthas is starting, please wait");
    }

    if (currentState == State.STOPPING) {
      logger.log(Level.WARNING, "Arthas is stopping, cannot start now");
      return StartResult.failed("Arthas is stopping, please try again later");
    }

    // 将启动从调用线程中剥离，避免阻塞调用方（例如 WebSocket 回调线程）
    state.set(State.STARTING);
    ArthasStateEventBus bus = this.stateEventBus;
    if (bus != null) {
      bus.publishArthasState(State.STARTING);
    }
    startupLogCollector.addLog("INFO", "Lifecycle state -> STARTING");

    // 启动看门狗：如果超过阈值仍未进入 RUNNING/IDLE，则记录诊断并回退到 STOPPED
    startupWatchdogTask =
        scheduler.schedule(
            () -> {
              State st = state.get();
              if (st == State.STARTING) {
                logger.log(
                    Level.WARNING,
                    "Arthas startup watchdog fired after {0}ms: still STARTING. This usually indicates a hang inside bootstrap.start().",
                    STARTUP_WATCHDOG_MILLIS);
                startupLogCollector.addLog(
                    "ERROR",
                    "Startup watchdog timeout after " + STARTUP_WATCHDOG_MILLIS + "ms (still STARTING)");

                // 这里保持最小侵入：仅改变状态并发布事件，避免后续 TERMINAL_OPEN 无限等待。
                state.set(State.STOPPED);
                ArthasStateEventBus b = this.stateEventBus;
                if (b != null) {
                  b.publishArthasState(State.STOPPED);
                }
                listener.onArthasStopped();
              }
            },
            STARTUP_WATCHDOG_MILLIS,
            TimeUnit.MILLISECONDS);

    scheduler.execute(
        () -> {
          try {
            doStartInternal(scheduler);
            // doStartInternal 内部会更新状态/回调
          } catch (Throwable t) {
            logger.log(Level.WARNING, "Arthas start task failed", t);
            startupLogCollector.addLog("ERROR", "Start task failed: " + t);
            state.set(State.STOPPED);
            ArthasStateEventBus b = this.stateEventBus;
            if (b != null) {
              b.publishArthasState(State.STOPPED);
            }
            listener.onArthasStopped();
          }
        });

    return StartResult.success();
  }

  // 将原有 tryStart 内的启动细节收敛到该方法，便于 watchdog/诊断复用
  private void doStartInternal(ScheduledExecutorService scheduler) {
    StartupLogCollector logs = this.startupLogCollector;

    // 在真正启动之前，做一次“可启动性”检查，避免错误状态下继续启动
    State st = state.get();
    if (st != State.STARTING) {
      logs.addLog("WARN", "Start task aborted: lifecycle state is " + st);
      return;
    }

    logs.addLog("INFO", "Bootstrap.start() begin");

    // 这里是最可能 hang 的位置：bootstrap.start 内部会进行反射 invoke(getInstance)
    ArthasBootstrap.StartResult r = arthasBootstrap.start();

    if (!r.isSuccess()) {
      logs.addLog("ERROR", "Bootstrap.start() failed: " + r.getMessage());
      // 启动失败，回退到 STOPPED，并通知 listener
      transitionToStopped("bootstrap_start_failed");
      return;
    }

    logs.addLog("INFO", "Bootstrap.start() ok: " + r.getMessage());

    // 启动成功：更新状态与时间
    startedAt = Instant.now();
    idleSince = null;

    cancelStartupWatchdog();

    state.set(State.RUNNING);
    ArthasStateEventBus bus = this.stateEventBus;
    if (bus != null) {
      bus.publishArthasState(State.RUNNING);
    }
    logs.addLog("INFO", "Lifecycle state -> RUNNING");

    // 安排最大运行时长关闭（如果配置了）
    scheduleMaxDurationShutdown(scheduler);

    // 通知 listener（由 integration 推动其他组件，如创建 terminal）
    listener.onArthasStarted();
  }

  private void scheduleMaxDurationShutdown(ScheduledExecutorService scheduler) {
    // 若未配置 maxDuration（<=0），则不安排
    long maxMillis = config.getMaxRunningDuration().toMillis();
    if (maxMillis <= 0) {
      return;
    }

    if (maxDurationTask != null) {
      maxDurationTask.cancel(false);
      maxDurationTask = null;
    }

    maxDurationTask =
        scheduler.schedule(
            () -> {
              // 超过最大时长后：通知监听器，并主动 stop
              startupLogCollector.addLog(
                  "WARN", "Max duration exceeded after " + maxMillis + "ms, stopping");
              try {
                listener.onMaxDurationExceeded();
              } catch (RuntimeException e) {
                logger.log(Level.FINE, "onMaxDurationExceeded listener failed", e);
              }
              stop();
            },
            maxMillis,
            TimeUnit.MILLISECONDS);
  }

  private void cancelStartupWatchdog() {
    if (startupWatchdogTask != null) {
      startupWatchdogTask.cancel(false);
      startupWatchdogTask = null;
    }
  }

  private void transitionToStopped(String reason) {
    cancelStartupWatchdog();

    // best-effort stop bootstrap if it thinks it's running
    try {
      arthasBootstrap.stop();
    } catch (RuntimeException e) {
      logger.log(Level.FINE, "bootstrap.stop failed during transitionToStopped", e);
    }

    startedAt = null;
    idleSince = null;
    registered.set(false);

    state.set(State.STOPPED);
    ArthasStateEventBus bus = this.stateEventBus;
    if (bus != null) {
      bus.publishArthasState(State.STOPPED);
    }

    startupLogCollector.addLog("INFO", "Lifecycle state -> STOPPED (" + reason + ")");
    listener.onArthasStopped();
  }

  /**
   * 停止 Arthas
   *
   * @return 是否成功停止
   */
  public boolean stop() {
    State currentState = state.get();
    if (currentState == State.STOPPED) {
      logger.log(Level.FINE, "Arthas already stopped");
      return true;
    }

    if (currentState == State.STOPPING) {
      logger.log(Level.FINE, "Arthas is already stopping");
      return true;
    }

    // 尝试切换到停止中状态
    if (!state.compareAndSet(currentState, State.STOPPING)) {
      logger.log(Level.WARNING, "State changed during stop attempt");
      return false;
    }

    ArthasStateEventBus bus = this.stateEventBus;
    if (bus != null) {
      bus.publishArthasState(State.STOPPING);
    }

    logger.log(Level.INFO, "Stopping Arthas...");

    try {
      // 取消定时任务
      cancelScheduledTasks();

      // 停止 Arthas
      arthasBootstrap.stop();

      // 重置状态
      startedAt = null;
      idleSince = null;
      registered.set(false);

      state.set(State.STOPPED);

      if (bus != null) {
        bus.publishArthasState(State.STOPPED);
      }

      logger.log(Level.INFO, "Arthas stopped successfully");

      // 通知监听器
      listener.onArthasStopped();

      return true;

    } catch (RuntimeException e) {
      logger.log(Level.SEVERE, "Error stopping Arthas", e);
      state.set(State.STOPPED);
      if (bus != null) {
        bus.publishArthasState(State.STOPPED);
      }
      return false;
    }
  }

  /**
   * 标记已向服务端注册成功
   *
   * <p>【阶段性重构】Tunnel 注册状态已与 Arthas 生命周期状态解耦。
   * 该回调仅用于诊断日志，不再推动本地生命周期状态机。
   */
  public void markRegistered() {
    registered.set(true);
    State currentState = state.get();
    logger.log(
        Level.FINE,
        "markRegistered received (tunnel registered), lifecycle state unchanged: {0}",
        currentState);
    startupLogCollector.addLog(
        "INFO",
        "Tunnel REGISTER_ACK received, lifecycle state unchanged: " + currentState);
  }

  /**
   * 检查是否已注册
   *
   * @return 是否已向服务端注册成功
   */
  public boolean isRegistered() {
    // Tunnel 注册与本地生命周期解耦：只有本地可用且 tunnel 已完成 REGISTER_ACK，才认为“已注册”。
    State s = state.get();
    boolean localReady = (s == State.RUNNING || s == State.IDLE);
    return localReady && registered.get();
  }

  /**
   * 标记进入空闲状态（所有会话关闭时调用）
   *
   * @param scheduler 调度器
   */
  public void markIdle(ScheduledExecutorService scheduler) {
    State currentState = state.get();
    // RUNNING 状态可以切换到 IDLE
    if (currentState != State.RUNNING) {
      return;
    }

    state.set(State.IDLE);
    idleSince = Instant.now();

    ArthasStateEventBus bus = this.stateEventBus;
    if (bus != null) {
      bus.publishArthasState(State.IDLE);
    }

    logger.log(
        Level.INFO,
        "Arthas entered idle state, will shutdown after {0}",
        config.getIdleShutdownDelay());

    // 安排空闲关闭任务
    scheduleIdleShutdown(scheduler);
  }

  /**
   * 标记恢复活跃状态（有新会话创建时调用）
   */
  public void markActive() {
    State currentState = state.get();
    if (currentState == State.IDLE) {
      // 取消空闲关闭任务
      cancelIdleShutdownTask();

      // 恢复到 RUNNING 状态
      state.set(State.RUNNING);
      idleSince = null;

      ArthasStateEventBus bus = this.stateEventBus;
      if (bus != null) {
        bus.publishArthasState(State.RUNNING);
      }

      logger.log(Level.INFO, "Arthas resumed from idle state");
    }
  }

  /** 获取当前状态 */
  public State getState() {
    State s = state.get();
    return s != null ? s : State.STOPPED;
  }

  /** 检查 Arthas 是否正在运行 */
  public boolean isRunning() {
    State s = state.get();
    return s == State.RUNNING || s == State.IDLE;
  }

  /** 获取启动时间 */
  @Nullable
  public Instant getStartedAt() {
    return startedAt;
  }

  /** 获取运行时长（毫秒） */
  public long getUptimeMillis() {
    if (startedAt == null) {
      return 0;
    }
    return System.currentTimeMillis() - startedAt.toEpochMilli();
  }

  /** 获取空闲时长（毫秒） */
  public long getIdleMillis() {
    if (idleSince == null) {
      return 0;
    }
    return System.currentTimeMillis() - idleSince.toEpochMilli();
  }

  /** 安排空闲关闭任务 */
  private void scheduleIdleShutdown(ScheduledExecutorService scheduler) {
    cancelIdleShutdownTask();

    long delayMillis = config.getIdleShutdownDelay().toMillis();
    idleShutdownTask =
        scheduler.schedule(
            () -> {
              if (state.get() == State.IDLE) {
                logger.log(Level.INFO, "Idle shutdown triggered");
                stop();
              }
            },
            delayMillis,
            TimeUnit.MILLISECONDS);

    logger.log(Level.FINE, "Scheduled idle shutdown in {0}ms", delayMillis);
  }

  /** 取消空闲关闭任务 */
  private void cancelIdleShutdownTask() {
    if (idleShutdownTask != null) {
      idleShutdownTask.cancel(false);
      idleShutdownTask = null;
    }
  }

  /** 取消所有定时任务 */
  private void cancelScheduledTasks() {
    cancelIdleShutdownTask();
    if (startupWatchdogTask != null) {
      startupWatchdogTask.cancel(false);
      startupWatchdogTask = null;
    }
    if (maxDurationTask != null) {
      maxDurationTask.cancel(false);
      maxDurationTask = null;
    }
  }

  @Override
  public void close() {
    stop();
    arthasBootstrap.destroy();
  }

  /**
   * 获取启动日志收集器
   *
   * @return 启动日志收集器
   */
  public StartupLogCollector getStartupLogCollector() {
    return startupLogCollector;
  }

  /** 启动结果 */
  public static final class StartResult {
    private final boolean success;
    @Nullable private final String errorMessage;
    private final List<String> logs;

    private StartResult(boolean success, @Nullable String errorMessage, List<String> logs) {
      this.success = success;
      this.errorMessage = errorMessage;
      this.logs = Collections.unmodifiableList(new ArrayList<>(logs));
    }

    public static StartResult success() {
      return new StartResult(/* success= */ true, /* errorMessage= */ null, Collections.emptyList());
    }

    public static StartResult success(List<String> logs) {
      return new StartResult(/* success= */ true, /* errorMessage= */ null, logs);
    }

    public static StartResult failed(String errorMessage) {
      return new StartResult(/* success= */ false, errorMessage, Collections.emptyList());
    }

    public static StartResult failed(String errorMessage, List<String> logs) {
      return new StartResult(/* success= */ false, errorMessage, logs);
    }

    public boolean isSuccess() {
      return success;
    }

    @Nullable
    public String getErrorMessage() {
      return errorMessage;
    }

    /**
     * 获取启动过程日志
     *
     * @return 日志列表
     */
    public List<String> getLogs() {
      return logs;
    }

    /**
     * 获取日志摘要（用于错误消息）
     *
     * @return 日志摘要字符串
     */
    public String getLogSummary() {
      if (logs.isEmpty()) {
        return "";
      }
      StringBuilder sb = new StringBuilder();
      sb.append(" Startup logs: ");
      for (int i = 0; i < logs.size(); i++) {
        if (i > 0) {
          sb.append("; ");
        }
        sb.append(logs.get(i));
      }
      return sb.toString();
    }
  }

  /**
   * 启动日志收集器
   *
   * <p>用于收集 Arthas 启动过程中的关键事件，便于排查问题。
   */
  public static final class StartupLogCollector {
    private final List<String> logs = new ArrayList<>();
    private static final int MAX_LOGS = 50;

    /**
     * 添加日志
     *
     * @param level 日志级别
     * @param message 日志消息
     */
    public synchronized void addLog(String level, String message) {
      if (logs.size() >= MAX_LOGS) {
        logs.remove(0);
      }
      String timestamp = String.format(Locale.ROOT, "%tT", System.currentTimeMillis());
      logs.add(String.format(Locale.ROOT, "[%s] %s: %s", timestamp, level, message));
    }

    /**
     * 获取所有日志
     *
     * @return 日志列表（副本）
     */
    public synchronized List<String> getLogs() {
      return new ArrayList<>(logs);
    }

    /**
     * 获取最近的错误日志
     *
     * @return 最近的错误日志，如果没有则返回 null
     */
    @Nullable
    public synchronized String getLastError() {
      for (int i = logs.size() - 1; i >= 0; i--) {
        String log = logs.get(i);
        if (log.contains("ERROR") || log.contains("WARN")) {
          return log;
        }
      }
      return null;
    }

    /** 清除所有日志 */
    public synchronized void clear() {
      logs.clear();
    }

    /**
     * 获取日志数量
     *
     * @return 日志数量
     */
    public synchronized int size() {
      return logs.size();
    }
  }

  /** 生命周期事件监听器 */
  public interface LifecycleEventListener {
    /** Arthas 启动时调用 */
    void onArthasStarted();

    /** Arthas 停止时调用 */
    void onArthasStopped();

    /** 超过最大运行时长时调用 */
    void onMaxDurationExceeded();
  }
}
