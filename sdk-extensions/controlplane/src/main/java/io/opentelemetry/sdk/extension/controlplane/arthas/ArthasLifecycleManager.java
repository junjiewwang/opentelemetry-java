/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.io.Closeable;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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

  /** Arthas 运行状态 */
  public enum State {
    /** 已停止 */
    STOPPED,
    /** 启动中 */
    STARTING,
    /** 运行中 */
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

  @Nullable private Instant startedAt;
  @Nullable private Instant idleSince;
  @Nullable private ScheduledFuture<?> idleShutdownTask;
  @Nullable private ScheduledFuture<?> maxDurationTask;

  /**
   * 创建生命周期管理器
   *
   * @param config Arthas 配置
   * @param listener 生命周期事件监听器
   */
  public ArthasLifecycleManager(ArthasConfig config, LifecycleEventListener listener) {
    this.config = config;
    this.listener = listener;
    this.arthasBootstrap = new ArthasBootstrap(config);
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
   * 尝试启动 Arthas
   *
   * @param scheduler 调度器（用于定时任务）
   * @return 启动结果
   */
  public StartResult tryStart(ScheduledExecutorService scheduler) {
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

    // 尝试切换到启动中状态
    if (!state.compareAndSet(State.STOPPED, State.STARTING)) {
      return StartResult.failed("State changed during start attempt");
    }

    logger.log(Level.INFO, "Starting Arthas...");

    try {
      // 1. 初始化 Arthas（加载依赖）
      if (!arthasBootstrap.isInitialized() && !arthasBootstrap.initialize()) {
        state.set(State.STOPPED);
        return StartResult.failed("Failed to initialize Arthas");
      }

      // 2. 启动 Arthas
      ArthasBootstrap.StartResult bootResult = arthasBootstrap.start();
      if (!bootResult.isSuccess()) {
        state.set(State.STOPPED);
        return StartResult.failed(bootResult.getMessage());
      }

      // 3. 启动成功，更新状态
      startedAt = Instant.now();

      // 启动最大运行时长检查任务
      scheduleMaxDurationCheck(scheduler);

      // 切换到运行中状态
      state.set(State.RUNNING);

      logger.log(Level.INFO, "Arthas started successfully");

      // 通知监听器
      listener.onArthasStarted();

      return StartResult.success();

    } catch (RuntimeException e) {
      logger.log(Level.SEVERE, "Failed to start Arthas", e);
      state.set(State.STOPPED);
      return StartResult.failed("Failed to start Arthas: " + e.getMessage());
    }
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

    logger.log(Level.INFO, "Stopping Arthas...");

    try {
      // 取消定时任务
      cancelScheduledTasks();

      // 停止 Arthas
      arthasBootstrap.stop();

      // 重置状态
      startedAt = null;
      idleSince = null;

      state.set(State.STOPPED);

      logger.log(Level.INFO, "Arthas stopped successfully");

      // 通知监听器
      listener.onArthasStopped();

      return true;

    } catch (RuntimeException e) {
      logger.log(Level.SEVERE, "Error stopping Arthas", e);
      state.set(State.STOPPED);
      return false;
    }
  }

  /**
   * 标记进入空闲状态（所有会话关闭时调用）
   *
   * @param scheduler 调度器
   */
  public void markIdle(ScheduledExecutorService scheduler) {
    State currentState = state.get();
    if (currentState != State.RUNNING) {
      return;
    }

    state.set(State.IDLE);
    idleSince = Instant.now();

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

      state.set(State.RUNNING);
      idleSince = null;

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

  /** 安排最大运行时长检查任务 */
  private void scheduleMaxDurationCheck(ScheduledExecutorService scheduler) {
    long maxDurationMillis = config.getMaxRunningDuration().toMillis();
    maxDurationTask =
        scheduler.schedule(
            () -> {
              if (isRunning()) {
                logger.log(
                    Level.WARNING,
                    "Max running duration ({0}) exceeded, forcing shutdown",
                    config.getMaxRunningDuration());
                listener.onMaxDurationExceeded();
                stop();
              }
            },
            maxDurationMillis,
            TimeUnit.MILLISECONDS);

    logger.log(Level.FINE, "Scheduled max duration check in {0}ms", maxDurationMillis);
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

  /** 启动结果 */
  public static final class StartResult {
    private final boolean success;
    @Nullable private final String errorMessage;

    private StartResult(boolean success, @Nullable String errorMessage) {
      this.success = success;
      this.errorMessage = errorMessage;
    }

    public static StartResult success() {
      return new StartResult(/* success= */ true, /* errorMessage= */ null);
    }

    public static StartResult failed(String errorMessage) {
      return new StartResult(/* success= */ false, errorMessage);
    }

    public boolean isSuccess() {
      return success;
    }

    @Nullable
    public String getErrorMessage() {
      return errorMessage;
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
