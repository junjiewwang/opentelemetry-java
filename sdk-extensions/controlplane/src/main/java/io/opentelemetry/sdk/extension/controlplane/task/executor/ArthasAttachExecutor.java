/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.executor;

import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasBootstrap;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasLifecycleManager;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasLifecycleManager.StartResult;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.ArthasTunnelClient;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 附加执行器
 *
 * <p>负责执行 arthas_attach 类型的任务，实现：
 * <ul>
 *   <li>启动 Arthas 服务
 *   <li>连接 Tunnel Server
 *   <li>注册 Agent 到控制平面
 *   <li>等待注册成功确认
 * </ul>
 *
 * <p>执行流程：
 * <pre>
 *   1. 检查 Arthas 当前状态
 *   2. 如果未运行，启动 Arthas
 *   3. 等待 Arthas 启动完成
 *   4. 连接 Tunnel Server
 *   5. 等待注册确认
 *   6. 返回执行结果
 * </pre>
 */
public final class ArthasAttachExecutor implements TaskExecutor {

  private static final Logger logger = Logger.getLogger(ArthasAttachExecutor.class.getName());

  /** 任务类型 */
  public static final String TASK_TYPE = "arthas_attach";

  /** 默认启动超时：30秒 */
  private static final long DEFAULT_START_TIMEOUT_MILLIS = 30 * 1000;

  /** 默认连接超时：10秒 */
  private static final long DEFAULT_CONNECT_TIMEOUT_MILLIS = 10 * 1000;

  /** 检查间隔：500ms */
  private static final long CHECK_INTERVAL_MILLIS = 500;

  /** Arthas 生命周期管理器 */
  @Nullable private final ArthasLifecycleManager lifecycleManager;

  /** Arthas Tunnel 客户端 */
  @Nullable private final ArthasTunnelClient tunnelClient;

  /** 调度器 */
  @Nullable private final ScheduledExecutorService scheduler;

  /**
   * 创建 Arthas 附加执行器
   *
   * @param lifecycleManager Arthas 生命周期管理器（可为 null，表示 Arthas 未配置）
   * @param tunnelClient Arthas Tunnel 客户端（可为 null，表示 Tunnel 未配置）
   * @param scheduler 调度器
   */
  public ArthasAttachExecutor(
      @Nullable ArthasLifecycleManager lifecycleManager,
      @Nullable ArthasTunnelClient tunnelClient,
      @Nullable ScheduledExecutorService scheduler) {
    this.lifecycleManager = lifecycleManager;
    this.tunnelClient = tunnelClient;
    this.scheduler = scheduler;
  }

  @Override
  public String getTaskType() {
    return TASK_TYPE;
  }

  @Override
  public String getDescription() {
    return "Arthas attach executor - starts Arthas and connects to tunnel server";
  }

  @Override
  public boolean isAvailable() {
    // 只要有生命周期管理器就认为可用（Tunnel 是可选的）
    return lifecycleManager != null;
  }

  @Override
  public CompletableFuture<TaskExecutionResult> execute(TaskExecutionContext context) {
    CompletableFuture<TaskExecutionResult> future = new CompletableFuture<>();

    String taskId = context.getTaskId();
    logger.log(Level.INFO, "[ARTHAS-ATTACH] Starting execution: taskId={0}", taskId);

    // 检查前置条件
    if (lifecycleManager == null) {
      logger.log(Level.WARNING, "[ARTHAS-ATTACH] ArthasLifecycleManager not configured");
      future.complete(TaskExecutionResult.failed(
          "ARTHAS_NOT_CONFIGURED",
          "ArthasLifecycleManager is not configured"));
      return future;
    }

    // 获取参数
    String action = context.getStringParameter("action", "attach");
    long startTimeout = context.getLongParameter("start_timeout_millis", DEFAULT_START_TIMEOUT_MILLIS);
    long connectTimeout = context.getLongParameter("connect_timeout_millis", DEFAULT_CONNECT_TIMEOUT_MILLIS);

    logger.log(
        Level.INFO,
        "[ARTHAS-ATTACH] Parameters: action={0}, startTimeout={1}ms, connectTimeout={2}ms",
        new Object[] {action, startTimeout, connectTimeout});

    // 异步执行
    ArthasLifecycleManager manager = lifecycleManager;
    @SuppressWarnings("FutureReturnValueIgnored")
    Object unused = CompletableFuture.runAsync(() -> {
      long startTime = System.currentTimeMillis();
      try {
        TaskExecutionResult result = executeAttach(
            context, action, startTimeout, connectTimeout, manager);
        long executionTime = System.currentTimeMillis() - startTime;

        // 更新执行时间
        if (result.isSuccess()) {
          future.complete(TaskExecutionResult.success(
              result.getResultJson(), executionTime));
        } else {
          future.complete(TaskExecutionResult.builder()
              .status(result.getStatus())
              .errorCode(result.getErrorCode())
              .errorMessage(result.getErrorMessage())
              .executionTimeMillis(executionTime)
              .completedAtMillis(System.currentTimeMillis())
              .build());
        }
      } catch (RuntimeException e) {
        long executionTime = System.currentTimeMillis() - startTime;
        logger.log(Level.WARNING, "[ARTHAS-ATTACH] Execution failed: {0}", e.getMessage());
        future.complete(TaskExecutionResult.failed(
            "ARTHAS_ATTACH_ERROR",
            "Arthas attach failed: " + e.getMessage(),
            executionTime));
      }
    });

    return future;
  }

  /**
   * 执行 Arthas 附加操作
   *
   * @param context 任务上下文
   * @param action 操作类型（预留参数，未来支持 detach 等操作）
   * @param startTimeout 启动超时
   * @param connectTimeout 连接超时
   * @param manager 生命周期管理器
   * @return 执行结果
   */
  @SuppressWarnings("UnusedVariable") // action 预留给未来扩展（如 detach 操作）
  private TaskExecutionResult executeAttach(
      TaskExecutionContext context,
      String action,
      long startTimeout,
      long connectTimeout,
      ArthasLifecycleManager manager) {

    String taskId = context.getTaskId();

    // Step 1: 检查当前状态
    ArthasLifecycleManager.State currentState = manager.getState();
    logger.log(
        Level.INFO,
        "[ARTHAS-ATTACH] Current Arthas state: {0}, taskId={1}",
        new Object[] {currentState, taskId});

    // 如果已经注册成功，直接返回成功
    if (currentState == ArthasLifecycleManager.State.REGISTERED) {
      logger.log(Level.INFO, 
          "[ARTHAS-ATTACH] Arthas already registered with server, taskId={0}", taskId);
      return buildSuccessResult("Arthas already running and registered", manager);
    }

    // 如果已经运行或空闲，直接返回成功
    if (currentState == ArthasLifecycleManager.State.RUNNING
        || currentState == ArthasLifecycleManager.State.IDLE) {
      logger.log(Level.INFO, "[ARTHAS-ATTACH] Arthas already running, taskId={0}", taskId);
      return buildSuccessResult("Arthas already running", manager);
    }

    // 如果正在启动，等待启动完成
    if (currentState == ArthasLifecycleManager.State.STARTING) {
      logger.log(Level.INFO, "[ARTHAS-ATTACH] Arthas is starting, waiting..., taskId={0}", taskId);
      return waitForArthasRunning(startTimeout, manager);
    }

    // Step 2: 启动 Arthas
    logger.log(Level.INFO, "[ARTHAS-ATTACH] Starting Arthas..., taskId={0}", taskId);

    ScheduledExecutorService effectiveScheduler = scheduler;
    if (effectiveScheduler == null) {
      effectiveScheduler = context.getScheduler();
    }

    if (effectiveScheduler == null) {
      return TaskExecutionResult.failed(
          "NO_SCHEDULER",
          "No scheduler available for Arthas startup");
    }

    StartResult startResult = manager.tryStart(effectiveScheduler);

    if (!startResult.isSuccess()) {
      logger.log(
          Level.WARNING,
          "[ARTHAS-ATTACH] Failed to start Arthas: {0}, taskId={1}",
          new Object[] {startResult.getErrorMessage(), taskId});
      return TaskExecutionResult.failed(
          "ARTHAS_START_FAILED",
          "Failed to start Arthas: " + startResult.getErrorMessage());
    }

    // Step 3: 等待 Arthas 运行
    logger.log(Level.INFO, "[ARTHAS-ATTACH] Waiting for Arthas to be running..., taskId={0}", taskId);
    TaskExecutionResult waitResult = waitForArthasRunning(startTimeout, manager);
    if (!waitResult.isSuccess()) {
      return waitResult;
    }

    // Step 4: 连接 Tunnel（如果配置了）
    if (tunnelClient != null) {
      logger.log(Level.INFO, "[ARTHAS-ATTACH] Connecting to tunnel server..., taskId={0}", taskId);
      TaskExecutionResult connectResult = connectToTunnel(connectTimeout);
      if (!connectResult.isSuccess()) {
        return connectResult;
      }
    } else {
      logger.log(Level.INFO, "[ARTHAS-ATTACH] Tunnel client not configured, skipping, taskId={0}", taskId);
    }

    // Step 5: 返回成功
    logger.log(Level.INFO, "[ARTHAS-ATTACH] Arthas attach completed successfully, taskId={0}", taskId);
    return buildSuccessResult("Arthas started and connected successfully", manager);
  }

  /**
   * 等待 Arthas 运行并注册
   *
   * <p>在超时时间内等待 Arthas 达到 REGISTERED 状态，
   * 表示已向服务端注册成功，任务可以完成。
   */
  private static TaskExecutionResult waitForArthasRunning(
      long timeoutMillis, ArthasLifecycleManager manager) {
    long deadline = System.currentTimeMillis() + timeoutMillis;
    long startTime = System.currentTimeMillis();

    while (System.currentTimeMillis() < deadline) {
      ArthasLifecycleManager.State state = manager.getState();
      
      // REGISTERED 状态表示已向服务端注册成功，任务完成
      if (state == ArthasLifecycleManager.State.REGISTERED) {
        long elapsed = System.currentTimeMillis() - startTime;
        logger.log(Level.INFO, 
            "[ARTHAS-ATTACH] Arthas registered successfully after {0}ms", elapsed);
        return TaskExecutionResult.success();
      }
      
      // IDLE 状态也表示已注册（之前注册过，现在空闲）
      if (state == ArthasLifecycleManager.State.IDLE) {
        return TaskExecutionResult.success();
      }
      
      // RUNNING 状态表示本地启动成功，但还未注册，继续等待
      // STARTING 状态表示正在启动，继续等待
      
      if (state == ArthasLifecycleManager.State.STOPPED) {
        // 收集启动过程日志
        String logSummary = getStartupLogSummary(manager);
        return TaskExecutionResult.failed(
            "ARTHAS_STOPPED",
            "Arthas stopped unexpectedly." + logSummary);
      }

      try {
        Thread.sleep(CHECK_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return TaskExecutionResult.failed(
            "INTERRUPTED",
            "Interrupted while waiting for Arthas to start");
      }
    }

    // 超时时，收集详细的错误信息
    long elapsed = System.currentTimeMillis() - startTime;
    String errorMessage = buildTimeoutErrorMessage(timeoutMillis, elapsed, manager);
    
    return TaskExecutionResult.timeout(errorMessage);
  }

  /**
   * 构建超时错误消息
   *
   * @param timeoutMillis 超时时间
   * @param elapsed 实际等待时间
   * @param manager 生命周期管理器
   * @return 详细的错误消息
   */
  private static String buildTimeoutErrorMessage(
      long timeoutMillis, long elapsed, ArthasLifecycleManager manager) {
    
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(
        Locale.ROOT,
        "Arthas startup timeout after %dms (waited %dms), current state: %s",
        timeoutMillis, elapsed, manager.getState()));

    // 添加可能的原因分析
    ArthasLifecycleManager.StartupLogCollector logCollector = manager.getStartupLogCollector();
    String lastError = logCollector.getLastError();
    
    if (lastError != null) {
      sb.append(". Last error: ").append(lastError);
    }

    // 检查 Instrumentation 状态
    ArthasBootstrap bootstrap = manager.getArthasBootstrap();
    if (bootstrap.getInstrumentation() == null) {
      sb.append(". Reason: Instrumentation not available - ");
      sb.append("SpyAPI cannot be loaded to Bootstrap ClassLoader, ");
      sb.append("this may cause Arthas to fail or work in degraded mode. ");
      sb.append("Please ensure InstrumentationHolder.set() is called in premain.");
    }

    // 添加启动日志摘要
    String logSummary = getStartupLogSummary(manager);
    if (!logSummary.isEmpty()) {
      sb.append(logSummary);
    }

    return sb.toString();
  }

  /**
   * 获取启动日志摘要
   *
   * @param manager 生命周期管理器
   * @return 日志摘要字符串
   */
  private static String getStartupLogSummary(ArthasLifecycleManager manager) {
    ArthasLifecycleManager.StartupLogCollector logCollector = manager.getStartupLogCollector();
    java.util.List<String> logs = logCollector.getLogs();
    
    if (logs.isEmpty()) {
      return "";
    }
    
    StringBuilder sb = new StringBuilder();
    sb.append(" Startup logs: [");
    
    // 只显示最近的几条日志
    int start = Math.max(0, logs.size() - 5);
    for (int i = start; i < logs.size(); i++) {
      if (i > start) {
        sb.append("; ");
      }
      sb.append(logs.get(i));
    }
    sb.append("]");
    
    return sb.toString();
  }

  /**
   * 连接到 Tunnel Server
   */
  private TaskExecutionResult connectToTunnel(long timeoutMillis) {
    if (tunnelClient == null) {
      return TaskExecutionResult.success();
    }

    // 检查是否已连接
    if (tunnelClient.isConnected()) {
      logger.log(Level.INFO, "[ARTHAS-ATTACH] Already connected to tunnel server");
      return TaskExecutionResult.success();
    }

    // 获取调度器
    ScheduledExecutorService effectiveScheduler = scheduler;
    if (effectiveScheduler == null) {
      logger.log(Level.WARNING, "[ARTHAS-ATTACH] No scheduler available for tunnel connection");
      return TaskExecutionResult.failed(
          "NO_SCHEDULER",
          "No scheduler available for tunnel connection");
    }

    // 启动 Tunnel 客户端（内部会进行连接）
    try {
      tunnelClient.start(effectiveScheduler);
    } catch (RuntimeException e) {
      logger.log(Level.WARNING, "[ARTHAS-ATTACH] Failed to start tunnel client: {0}", e.getMessage());
      return TaskExecutionResult.failed(
          "TUNNEL_START_FAILED",
          "Failed to start tunnel client: " + e.getMessage());
    }

    // 等待连接建立
    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (System.currentTimeMillis() < deadline) {
      if (tunnelClient.isConnected()) {
        logger.log(Level.INFO, "[ARTHAS-ATTACH] Successfully connected to tunnel server");
        return TaskExecutionResult.success();
      }

      // 检查是否有连接错误（提前失败，不用等到超时）
      ArthasTunnelClient.ConnectionError connectionError = tunnelClient.getLastConnectionError();
      if (connectionError != null) {
        String errorCode = connectionError.getCode();
        String errorMessage = connectionError.getMessage();
        
        // 对于认证错误，立即返回失败
        if ("UNAUTHORIZED".equals(errorCode) || "FORBIDDEN".equals(errorCode)) {
          logger.log(Level.WARNING, 
              "[ARTHAS-ATTACH] Tunnel connection failed due to authentication error: {0}", 
              errorMessage);
          return TaskExecutionResult.failed(
              "TUNNEL_" + errorCode,
              "Tunnel connection failed: " + errorMessage);
        }
      }

      try {
        Thread.sleep(CHECK_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return TaskExecutionResult.failed(
            "INTERRUPTED",
            "Interrupted while waiting for tunnel connection");
      }
    }

    // 超时时，检查是否有连接错误，返回更准确的错误信息
    ArthasTunnelClient.ConnectionError lastError = tunnelClient.getLastConnectionError();
    if (lastError != null) {
      String errorCode = "TUNNEL_" + lastError.getCode();
      String errorMessage = String.format(
          Locale.ROOT,
          "Tunnel connection failed after %dms: %s",
          timeoutMillis, lastError.getMessage());
      logger.log(Level.WARNING, "[ARTHAS-ATTACH] {0}", errorMessage);
      return TaskExecutionResult.failed(errorCode, errorMessage);
    }

    return TaskExecutionResult.timeout(String.format(
        Locale.ROOT,
        "Tunnel connection timeout after %dms",
        timeoutMillis));
  }

  /**
   * 构建成功结果
   */
  private TaskExecutionResult buildSuccessResult(
      String message, ArthasLifecycleManager manager) {
    // 构建结果 JSON
    String resultJson = String.format(
        Locale.ROOT,
        "{\"status\":\"success\",\"message\":\"%s\",\"arthas_state\":\"%s\",\"tunnel_connected\":%s}",
        message,
        manager.getState(),
        tunnelClient != null && tunnelClient.isConnected());

    return TaskExecutionResult.success(resultJson);
  }
}
