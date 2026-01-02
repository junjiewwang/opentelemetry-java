/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.PollResult;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.UnifiedPollRequest;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.UnifiedPollResponse;
import io.opentelemetry.sdk.extension.controlplane.core.ConnectionStateManager;
import io.opentelemetry.sdk.extension.controlplane.core.ConnectionStateManager.ConnectionState;
import io.opentelemetry.sdk.extension.controlplane.core.ControlPlaneStatistics;
import io.opentelemetry.sdk.extension.controlplane.core.HealthCheckCoordinator;
import io.opentelemetry.sdk.extension.controlplane.task.TaskExecutionLogger;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 长轮询协调器
 *
 * <p>协调配置和任务的长轮询请求，统一管理轮询生命周期：
 *
 * <ul>
 *   <li>管理长轮询请求的发起和响应处理
 *   <li>使用统一的 /v1/control/poll 端点同时获取配置和任务
 *   <li>通过 LongPollHandler 实现业务逻辑解耦
 *   <li>支持动态注册/注销 Handler（开闭原则）
 *   <li>实现指数退避重试机制
 *   <li>监控连接状态并自动重连
 * </ul>
 *
 * <p>采用单线程事件循环模式，避免并发问题：一个长轮询请求在飞行中时，不会发起第二个请求。
 */
public final class LongPollCoordinator implements Closeable {

  private static final Logger logger = Logger.getLogger(LongPollCoordinator.class.getName());

  /** 协调器状态 */
  public enum State {
    /** 空闲状态 */
    IDLE,
    /** 轮询中 */
    POLLING,
    /** 处理响应中 */
    PROCESSING,
    /** 等待重试 */
    WAITING_RETRY,
    /** 已停止 */
    STOPPED
  }

  // 配置
  private final LongPollConfig config;

  // 依赖组件
  private final ControlPlaneClient client;
  private final ConnectionStateManager connectionStateManager;
  private final HealthCheckCoordinator healthCheckCoordinator;
  private final ControlPlaneStatistics statistics;
  private final String agentId;

  // Handler 列表（支持动态注册，遵循开闭原则）
  private final List<LongPollHandler<?>> handlers;

  // 状态
  private final AtomicReference<State> state;
  private final AtomicBoolean running;
  private final AtomicInteger consecutiveErrors;
  private final AtomicLong pollCount;

  // 指数退避
  private final ExponentialBackoff backoff;

  // 任务日志记录器
  private final TaskExecutionLogger taskLogger;

  // 执行器（单线程）
  @Nullable private ExecutorService executor;

  /**
   * 创建长轮询协调器
   *
   * @param config 长轮询配置
   * @param client 控制平面客户端
   * @param connectionStateManager 连接状态管理器
   * @param healthCheckCoordinator 健康检查协调器
   * @param statistics 统计管理器
   * @param agentId Agent ID
   */
  public LongPollCoordinator(
      LongPollConfig config,
      ControlPlaneClient client,
      ConnectionStateManager connectionStateManager,
      HealthCheckCoordinator healthCheckCoordinator,
      ControlPlaneStatistics statistics,
      String agentId) {
    this.config = config;
    this.client = client;
    this.connectionStateManager = connectionStateManager;
    this.healthCheckCoordinator = healthCheckCoordinator;
    this.statistics = statistics;
    this.agentId = agentId;

    this.state = new AtomicReference<>(State.IDLE);
    this.running = new AtomicBoolean(false);
    this.consecutiveErrors = new AtomicInteger(0);
    this.pollCount = new AtomicLong(0);

    this.backoff = new ExponentialBackoff(config);
    this.taskLogger = TaskExecutionLogger.getInstance();

    // 初始化 Handler 列表（默认注册配置和任务处理器）
    this.handlers = new ArrayList<>();
    this.registerHandler(new ConfigLongPollHandler(client, statistics, config, agentId, running))
        .registerHandler(new TaskLongPollHandler(client, statistics, config, agentId, running));
  }

  /**
   * 注册长轮询处理器
   *
   * <p>支持动态添加新的轮询类型，遵循开闭原则
   *
   * @param handler 处理器实例
   * @return this（支持链式调用）
   */
  public LongPollCoordinator registerHandler(LongPollHandler<?> handler) {
    // 检查是否已存在同类型的 Handler
    for (LongPollHandler<?> existing : handlers) {
      if (existing.getType() == handler.getType()) {
        logger.log(
            Level.WARNING,
            "Handler for type {0} already registered, replacing",
            handler.getType());
        handlers.remove(existing);
        break;
      }
    }
    handlers.add(handler);
    logger.log(Level.INFO, "Registered handler: {0}", handler.getType());
    return this;
  }

  /**
   * 注销长轮询处理器
   *
   * @param type 处理器类型
   * @return 是否成功注销
   */
  public boolean unregisterHandler(LongPollType type) {
    for (LongPollHandler<?> handler : handlers) {
      if (handler.getType() == type) {
        handlers.remove(handler);
        logger.log(Level.INFO, "Unregistered handler: {0}", type);
        return true;
      }
    }
    return false;
  }

  /**
   * 获取已注册的处理器列表（只读）
   *
   * @return 处理器列表
   */
  public List<LongPollHandler<?>> getHandlers() {
    return Collections.unmodifiableList(handlers);
  }

  /**
   * 根据类型获取处理器
   *
   * @param type 处理器类型
   * @return 处理器实例，未找到返回 null
   */
  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
  @Nullable
  public <T extends LongPollHandler<?>> T getHandler(LongPollType type) {
    for (LongPollHandler<?> handler : handlers) {
      if (handler.getType() == type) {
        return (T) handler;
      }
    }
    return null;
  }

  /** 启动长轮询 */
  public void start() {
    if (!running.compareAndSet(false, true)) {
      logger.log(Level.WARNING, "LongPollCoordinator already started");
      return;
    }

    if (handlers.isEmpty()) {
      logger.log(Level.WARNING, "No handlers registered, long poll will not work");
    }

    executor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "otel-longpoll");
              t.setDaemon(true);
              return t;
            });

    @SuppressWarnings("FutureReturnValueIgnored")
    Object unused = executor.submit(this::pollLoop);

    logger.log(
        Level.INFO,
        "LongPollCoordinator started, timeout: {0}ms, handlers: {1}",
        new Object[] {config.getTimeoutMillis(), handlers.size()});
  }

  /** 停止长轮询 */
  public void stop() {
    if (!running.compareAndSet(true, false)) {
      return;
    }

    state.set(State.STOPPED);
    logger.log(Level.INFO, "LongPollCoordinator stopping...");
  }

  @Override
  public void close() {
    stop();

    if (executor != null) {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        executor.shutdownNow();
      }
    }

    logger.log(Level.INFO, "LongPollCoordinator closed");
  }

  /** 长轮询主循环 */
  private void pollLoop() {
    logger.log(Level.INFO, "Long poll loop started");

    while (running.get() && state.get() != State.STOPPED) {
      try {
        // 检查是否应该连接
        if (!healthCheckCoordinator.shouldConnect()) {
          state.set(State.WAITING_RETRY);
          statistics.logPeriodicStatusIfNeeded();
          Thread.sleep(backoff.nextBackoff());
          continue;
        }

        long count = pollCount.incrementAndGet();
        String taskId = "longpoll-" + count;

        // 记录任务开始
        taskLogger.logTaskReceived(
            taskId,
            "LONG_POLL",
            TaskExecutionLogger.SOURCE_SCHEDULER,
            TaskExecutionLogger.details()
                .put("pollCount", count)
                .put("timeout", config.getTimeoutMillis())
                .put("handlers", handlers.size())
                .build());

        state.set(State.POLLING);
        taskLogger.logTaskStarted(taskId, Thread.currentThread().getName());

        // 执行统一长轮询
        boolean success = executeUnifiedPoll(taskId, count);

        if (success) {
          // 成功后重置退避和连续错误计数
          backoff.reset();
          consecutiveErrors.set(0);

          // 更新连接状态
          ConnectionState previousState = connectionStateManager.markConnected();
          if (previousState != ConnectionState.CONNECTED) {
            logger.log(
                Level.INFO,
                "Control plane connected via long poll, state: {0} -> CONNECTED",
                previousState);
          }

          // 记录任务成功
          taskLogger.logTaskCompleted(taskId, "success:poll_count=" + count);

          // 根据服务端建议决定下次轮询时间（通常为 0，立即重试）
          // 这里不需要 sleep，因为长轮询本身会阻塞等待
          state.set(State.IDLE);

        } else {
          // 失败处理
          int errors = consecutiveErrors.incrementAndGet();
          long retryDelay = backoff.nextBackoff();

          // 记录任务失败
          taskLogger.logTaskFailed(
              taskId,
              TaskExecutionLogger.ERROR_NETWORK,
              "Long poll failed, consecutive errors: " + errors);

          logger.log(
              Level.WARNING,
              "Long poll failed (errors: {0}), retrying in {1}ms",
              new Object[] {errors, retryDelay});

          // 如果连续错误过多，更新连接状态
          if (errors >= config.getMaxConsecutiveErrors()) {
            ConnectionState previousState =
                connectionStateManager.markDisconnected("Too many consecutive errors");
            if (previousState != ConnectionState.DISCONNECTED) {
              logger.log(
                  Level.WARNING,
                  "Control plane disconnected due to consecutive errors, state: {0} -> DISCONNECTED",
                  previousState);
            }
          }

          state.set(State.WAITING_RETRY);
          Thread.sleep(retryDelay);
        }

        statistics.logPeriodicStatusIfNeeded();

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.log(Level.INFO, "Long poll loop interrupted");
        break;
      } catch (RuntimeException e) {
        logger.log(Level.WARNING, "Unexpected error in long poll loop: {0}", e.getMessage());
        try {
          Thread.sleep(backoff.nextBackoff());
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    state.set(State.STOPPED);
    logger.log(Level.INFO, "Long poll loop stopped");
  }

  /**
   * 执行统一长轮询（使用 /v1/control/poll 端点）
   *
   * <p>一次请求同时获取配置和任务更新，然后分发给各 Handler 处理。 遵循开闭原则：新增轮询类型只需注册
   * Handler，无需修改此方法。
   *
   * @param taskId 任务ID
   * @param count 轮询计数
   * @return 是否成功
   */
  private boolean executeUnifiedPoll(String taskId, long count) {
    if (handlers.isEmpty()) {
      logger.log(Level.WARNING, "No handlers registered, skipping poll");
      return true;
    }

    taskLogger.logTaskProgress(
        taskId,
        "polling",
        "Sending unified long poll request to /v1/control/poll");

    // 设置任务 ID 到所有 Handler（用于日志追踪）
    for (LongPollHandler<?> handler : handlers) {
      handler.setCurrentTaskId(taskId);
    }

    // 记录轮询统计
    statistics.recordConfigPoll();
    statistics.recordTaskPoll();

    // 构建统一请求
    UnifiedPollRequest request = createUnifiedPollRequest();

    try {
      // 发起统一轮询请求
      CompletableFuture<UnifiedPollResponse> future = client.poll(request);
      
      // 等待响应（带超时）
      long timeout = config.getTimeoutMillis() + 5000;
      UnifiedPollResponse response = future.get(timeout, TimeUnit.MILLISECONDS);

      // 处理响应
      return processUnifiedResponse(count, response);

    } catch (java.util.concurrent.TimeoutException e) {
      // 超时是长轮询的正常行为
      logger.log(Level.FINE, "Long poll timeout (normal behavior), count: {0}", count);
      return true;
    } catch (java.util.concurrent.ExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      logger.log(Level.WARNING, "Long poll execution failed: {0}", cause.getMessage());
      notifyHandlersError(cause);
      return false;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   * 处理统一轮询响应
   *
   * <p>将响应中的各类型结果分发给对应的 Handler 处理
   *
   * @param count 轮询计数
   * @param response 统一响应
   * @return 是否成功
   */
  private boolean processUnifiedResponse(long count, UnifiedPollResponse response) {
    
    if (!response.isSuccess()) {
      logger.log(Level.WARNING, "Unified poll failed: {0}", response.getErrorMessage());
      return false;
    }

    state.set(State.PROCESSING);
    
    int successCount = 0;
    int failureCount = 0;

    // 遍历响应中的各类型结果，分发给对应的 Handler
    for (LongPollHandler<?> handler : handlers) {
      String typeKey = handler.getType().name();
      PollResult result = response.getResults().get(typeKey);

      if (result != null) {
        try {
          // 使用类型安全的方式处理响应
          boolean processed = processHandlerResult(handler, result);
          if (processed) {
            successCount++;
          }
        } catch (RuntimeException e) {
          logger.log(
              Level.WARNING,
              "Handler {0} failed to process result: {1}",
              new Object[] {typeKey, e.getMessage()});
          handler.handleError(e);
          failureCount++;
        }
      }
    }

    logger.log(
        Level.FINE,
        "Unified poll completed (count: {0}): success={1}, failure={2}, hasAnyChanges={3}",
        new Object[] {count, successCount, failureCount, response.hasAnyChanges()});

    // 记录成功统计
    if (response.getConfigResult() != null) {
      statistics.recordConfigFetchSuccess();
    }

    // 只有当有失败且无成功时才算失败
    return failureCount == 0 || successCount > 0;
  }

  /**
   * 处理单个 Handler 的结果
   *
   * @param handler 处理器
   * @param result 轮询结果
   * @return 是否成功处理
   */
  private static boolean processHandlerResult(LongPollHandler<?> handler, PollResult result) {
    LongPollType type = handler.getType();

    if (type == LongPollType.CONFIG) {
      ConfigLongPollHandler configHandler = (ConfigLongPollHandler) handler;
      return configHandler.processUnifiedResult(result);
    } else if (type == LongPollType.TASK) {
      TaskLongPollHandler taskHandler = (TaskLongPollHandler) handler;
      return taskHandler.processUnifiedResult(result);
    }

    logger.log(Level.WARNING, "Unknown handler type: {0}", type);
    return false;
  }

  /**
   * 通知所有 Handler 发生错误
   *
   * @param error 错误
   */
  private void notifyHandlersError(Throwable error) {
    for (LongPollHandler<?> handler : handlers) {
      try {
        handler.handleError(error);
      } catch (RuntimeException e) {
        logger.log(Level.WARNING, "Handler error notification failed: {0}", e.getMessage());
      }
    }
  }

  /**
   * 创建统一轮询请求
   *
   * @return 统一轮询请求
   */
  private UnifiedPollRequest createUnifiedPollRequest() {
    // 从 ConfigHandler 获取当前配置版本和 ETag
    String configVersion = getCurrentConfigVersion();
    String configEtag = getCurrentConfigEtag();

    return new UnifiedPollRequest() {
      @Override
      public String getAgentId() {
        return agentId;
      }

      @Override
      public String getCurrentConfigVersion() {
        return configVersion;
      }

      @Override
      public String getCurrentConfigEtag() {
        return configEtag;
      }

      @Override
      public long getTimeoutMillis() {
        return config.getTimeoutMillis();
      }
    };
  }

  // ===== Getters =====

  /**
   * 获取当前状态
   *
   * @return 协调器状态
   */
  public State getState() {
    return Objects.requireNonNull(state.get(), "state");
  }

  /**
   * 是否正在运行
   *
   * @return 是否运行中
   */
  public boolean isRunning() {
    return running.get();
  }

  /**
   * 获取轮询计数
   *
   * @return 轮询次数
   */
  public long getPollCount() {
    return pollCount.get();
  }

  /**
   * 获取连续错误次数
   *
   * @return 连续错误次数
   */
  public int getConsecutiveErrors() {
    return consecutiveErrors.get();
  }

  /**
   * 获取当前配置版本（便捷方法）
   *
   * @return 配置版本，如果未注册 ConfigHandler 则返回空字符串
   */
  public String getCurrentConfigVersion() {
    ConfigLongPollHandler handler = getHandler(LongPollType.CONFIG);
    return handler != null ? handler.getCurrentConfigVersion() : "";
  }

  /**
   * 获取当前配置 ETag（便捷方法）
   *
   * @return ETag，如果未注册 ConfigHandler 则返回空字符串
   */
  public String getCurrentConfigEtag() {
    ConfigLongPollHandler handler = getHandler(LongPollType.CONFIG);
    return handler != null ? handler.getCurrentConfigEtag() : "";
  }

  /**
   * 获取配置处理器（便捷方法）
   *
   * @return 配置处理器，如果未注册返回 null
   */
  @Nullable
  public ConfigLongPollHandler getConfigHandler() {
    return getHandler(LongPollType.CONFIG);
  }

  /**
   * 获取任务处理器（便捷方法）
   *
   * @return 任务处理器，如果未注册返回 null
   */
  @Nullable
  public TaskLongPollHandler getTaskHandler() {
    return getHandler(LongPollType.TASK);
  }
}
