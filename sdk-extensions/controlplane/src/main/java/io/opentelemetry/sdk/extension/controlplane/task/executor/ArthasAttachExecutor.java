/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.executor;

import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasBootstrap;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasIntegration;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasLifecycleManager;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasLifecycleManager.StartResult;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.ArthasTunnelClient;
import io.opentelemetry.sdk.extension.controlplane.task.status.TaskStatusEmitter;
import java.util.Objects;
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

  /** Tunnel 注册等待的检查间隔（与连接检查共用） */
  private static final long REGISTER_CHECK_INTERVAL_MILLIS = CHECK_INTERVAL_MILLIS;

  /** Arthas 生命周期管理器 */
  @Nullable private final ArthasLifecycleManager lifecycleManager;

  /** Arthas Tunnel 客户端 */
  @Nullable private final ArthasTunnelClient tunnelClient;

  /** 调度器 */
  @Nullable private final ScheduledExecutorService scheduler;

  /** Arthas 集成（用于订阅 Tunnel REGISTER_ACK 事件，完成任务） */
  @Nullable private final ArthasIntegration arthasIntegration;

  /**
   * 创建 Arthas 附加执行器
   *
   * @param lifecycleManager Arthas 生命周期管理器（可为 null，表示 Arthas 未配置）
   * @param tunnelClient Arthas Tunnel 客户端（可为 null，表示 Tunnel 未配置）
   * @param scheduler 调度器
   * @param arthasIntegration Arthas 集成（可为 null，表示无法订阅注册事件，将回退到轮询）
   */
  public ArthasAttachExecutor(
      @Nullable ArthasLifecycleManager lifecycleManager,
      @Nullable ArthasTunnelClient tunnelClient,
      @Nullable ScheduledExecutorService scheduler,
      @Nullable ArthasIntegration arthasIntegration) {
    this.lifecycleManager = lifecycleManager;
    this.tunnelClient = tunnelClient;
    this.scheduler = scheduler;
    this.arthasIntegration = arthasIntegration;
  }

  /** 兼容旧构造签名（不带 ArthasIntegration，将回退到轮询等待 REGISTER_ACK） */
  public ArthasAttachExecutor(
      @Nullable ArthasLifecycleManager lifecycleManager,
      @Nullable ArthasTunnelClient tunnelClient,
      @Nullable ScheduledExecutorService scheduler) {
    this(lifecycleManager, tunnelClient, scheduler, null);
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

    // 事件驱动：如果 Tunnel REGISTER_ACK 事件触发，则立即上报 SUCCESS 并完成 future。
    // 否则（无 integration / 无 emitter），仍走现有同步逻辑（含轮询等待）。
    long startTime = System.currentTimeMillis();
    ArthasLifecycleManager manager = lifecycleManager;
    @Nullable TaskStatusEmitter statusEmitter = context.getStatusEmitter();

    // 只有在“需要等待注册”且“具备事件源+上报通道”时启用事件模式
    boolean enableEventMode =
        statusEmitter != null
            && arthasIntegration != null
            && tunnelClient != null;

    if (enableEventMode) {
      // 在 enableEventMode 为 true 的分支内，保证以下引用为非空，供 NullAway 识别
      TaskStatusEmitter emitter = Objects.requireNonNull(statusEmitter, "statusEmitter");
      ArthasIntegration integration = Objects.requireNonNull(arthasIntegration, "arthasIntegration");

      // 已注册则直接成功
      if (manager.isRegistered()) {
        TaskExecutionResult immediate = buildSuccessResult("Arthas already registered", manager);
        future.complete(TaskExecutionResult.success(immediate.getResultJson(), 0));
        return future;
      }

      ScheduledExecutorService effectiveScheduler = scheduler != null ? scheduler : context.getScheduler();
      if (effectiveScheduler == null) {
        future.complete(TaskExecutionResult.failed("NO_SCHEDULER", "No scheduler available for event-based attach"));
        return future;
      }

      // 事件驱动：直接等待 state predicate（REGISTER_ACK 对应 tunnelRegistered=true）
      CompletableFuture<io.opentelemetry.sdk.extension.controlplane.arthas.ArthasStateEventBus.State>
          registeredFuture =
              integration.awaitState(
                  s -> s.isTunnelRegistered(),
                  java.time.Duration.ofMillis(connectTimeout));

      @SuppressWarnings("FutureReturnValueIgnored")
      Object unusedRegistered =
          registeredFuture
              .thenAccept(
                  s -> {
                    // 通过事件完成任务：SUCCESS + resultJson
                    TaskExecutionResult ok = buildSuccessResult("Tunnel registered (REGISTER_ACK)", manager);
                    emitter.success(ok.getResultJson());
                    long executionTime = System.currentTimeMillis() - startTime;
                    future.complete(TaskExecutionResult.success(ok.getResultJson(), executionTime));
                  })
              .exceptionally(
                  e -> {
                    if (!future.isDone()) {
                      long executionTime = System.currentTimeMillis() - startTime;
                      future.complete(
                          TaskExecutionResult.timeout(
                              String.format(
                                  Locale.ROOT,
                                  "Tunnel registration timeout after %dms",
                                  connectTimeout),
                              executionTime));
                    }
                    return null;
                  });

      // 启动实际 attach 逻辑（启动 Arthas + 触发 tunnel start/connect）。
      // 该逻辑会很快返回 RUNNING；最终 SUCCESS 由 onTunnelRegistered 事件完成。
      @SuppressWarnings("FutureReturnValueIgnored")
      Object unused =
          CompletableFuture.runAsync(
              () -> {
                try {
                  TaskExecutionResult r =
                      executeAttach(context, action, startTimeout, connectTimeout, manager, statusEmitter);

                  // 若在启动阶段就失败（例如 Arthas 启动失败、连接失败、认证失败），应立即失败并解绑 listener。
                  if (r.isFailed() || r.isTimeout() || r.isCancelled()) {
                    long executionTime = System.currentTimeMillis() - startTime;
                    future.complete(
                        TaskExecutionResult.builder()
                            .status(r.getStatus())
                            .errorCode(r.getErrorCode())
                            .errorMessage(r.getErrorMessage())
                            .executionTimeMillis(executionTime)
                            .completedAtMillis(System.currentTimeMillis())
                            .build());
                  } else {
                    // r 可能是 RUNNING/成功：
                    // - RUNNING：等待 REGISTER_ACK 事件完成
                    // - SUCCESS：如果已注册，会在 executeAttach 返回 success，这里直接完成即可
                    if (r.isSuccess()) {
                      long executionTime = System.currentTimeMillis() - startTime;
                      future.complete(TaskExecutionResult.success(r.getResultJson(), executionTime));
                    } else {
                      emitter.running("Arthas started, waiting for tunnel registration");
                    }
                  }
                } catch (RuntimeException e) {
                  long executionTime = System.currentTimeMillis() - startTime;
                  future.complete(
                      TaskExecutionResult.failed(
                          "ARTHAS_ATTACH_ERROR",
                          "Arthas attach failed: " + e.getMessage(),
                          executionTime));
                }
              });

      return future;
    }

    // fallback：保留旧行为
    @SuppressWarnings("FutureReturnValueIgnored")
    Object unused =
        CompletableFuture.runAsync(
            () -> {
              long start = System.currentTimeMillis();
              try {
                TaskExecutionResult result =
                    executeAttach(context, action, startTimeout, connectTimeout, manager, statusEmitter);
                long executionTime = System.currentTimeMillis() - start;

                if (result.isSuccess()) {
                  future.complete(TaskExecutionResult.success(result.getResultJson(), executionTime));
                } else {
                  future.complete(
                      TaskExecutionResult.builder()
                          .status(result.getStatus())
                          .errorCode(result.getErrorCode())
                          .errorMessage(result.getErrorMessage())
                          .executionTimeMillis(executionTime)
                          .completedAtMillis(System.currentTimeMillis())
                          .build());
                }
              } catch (RuntimeException e) {
                long executionTime = System.currentTimeMillis() - start;
                logger.log(Level.WARNING, "[ARTHAS-ATTACH] Execution failed: {0}", e.getMessage());
                future.complete(
                    TaskExecutionResult.failed(
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
      ArthasLifecycleManager manager,
      @Nullable TaskStatusEmitter statusEmitter) {

    String taskId = context.getTaskId();

    // Step 1: 检查当前状态
    ArthasLifecycleManager.State currentState = manager.getState();
    logger.log(
        Level.INFO,
        "[ARTHAS-ATTACH] Current Arthas state: {0}, taskId={1}",
        new Object[] {currentState, taskId});

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
      TaskExecutionResult connectResult = connectToTunnel(connectTimeout, manager, statusEmitter);
      if (!connectResult.isSuccess()) {
        return connectResult;
      }
    } else {
      logger.log(Level.INFO, "[ARTHAS-ATTACH] Tunnel client not configured, skipping, taskId={0}", taskId);
    }

    // Step 5:
    // - 如果有事件发射器：此时通常还需要等待 REGISTER_ACK 才能认为真正成功，因此先返回 RUNNING。
    // - 否则：维持旧行为（阻塞等待注册完成）。
    if (statusEmitter != null && !manager.isRegistered()) {
      statusEmitter.running("Tunnel connected, waiting for REGISTER_ACK");
      return TaskExecutionResult.builder()
          .status(io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskStatus.RUNNING)
          .resultJson("{\"status\":\"running\",\"message\":\"Waiting for tunnel registration\"}")
          .build();
    }

    logger.log(Level.INFO, "[ARTHAS-ATTACH] Arthas attach completed successfully, taskId={0}", taskId);
    return buildSuccessResult("Arthas started and registered successfully", manager);
  }

  /**
   * 等待 Arthas 本地运行就绪
   *
   * <p>【阶段性重构】Tunnel 注册状态已与 Arthas 生命周期解耦。
   * 该等待仅关注 Arthas 本地状态达到 RUNNING/IDLE。
   */
  private static TaskExecutionResult waitForArthasRunning(
      long timeoutMillis, ArthasLifecycleManager manager) {
    long deadline = System.currentTimeMillis() + timeoutMillis;
    long startTime = System.currentTimeMillis();

    while (System.currentTimeMillis() < deadline) {
      ArthasLifecycleManager.State state = manager.getState();
      
      if (state == ArthasLifecycleManager.State.RUNNING
          || state == ArthasLifecycleManager.State.IDLE) {
        long elapsed = System.currentTimeMillis() - startTime;
        logger.log(Level.INFO, "[ARTHAS-ATTACH] Arthas is running after {0}ms", elapsed);
        return TaskExecutionResult.success();
      }
      
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
   * 连接到 Tunnel Server，并等待 REGISTER_ACK（如果调用方需要认为 attach 真正完成）
   */
  private TaskExecutionResult connectToTunnel(
      long timeoutMillis,
      ArthasLifecycleManager manager,
      @Nullable TaskStatusEmitter statusEmitter) {
    if (tunnelClient == null) {
      return TaskExecutionResult.success();
    }

    // 检查是否已连接
    if (tunnelClient.isConnected()) {
      logger.log(Level.INFO, "[ARTHAS-ATTACH] Already connected to tunnel server");
      // 有事件发射器时不阻塞，交给 onAgentRegistered 事件完成
      return statusEmitter != null ? TaskExecutionResult.success() : waitForTunnelRegistered(timeoutMillis, manager);
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
        // 连接成功后：有事件发射器则不阻塞；否则继续等待 REGISTER_ACK
        long remaining = deadline - System.currentTimeMillis();
        return statusEmitter != null
            ? TaskExecutionResult.success()
            : waitForTunnelRegistered(Math.max(0, remaining), manager);
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
   * 等待 Tunnel 注册成功（REGISTER_ACK）。
   *
   * <p>说明：注册 ACK 事件会通过 {@link io.opentelemetry.sdk.extension.controlplane.arthas.ArthasIntegration#onAgentRegistered()}
   * 触发，并调用 {@link ArthasLifecycleManager#markRegistered()}。
   */
  private static TaskExecutionResult waitForTunnelRegistered(
      long timeoutMillis, ArthasLifecycleManager manager) {
    if (timeoutMillis <= 0) {
      return TaskExecutionResult.timeout("Tunnel registration timeout");
    }

    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (System.currentTimeMillis() < deadline) {
      if (manager.isRegistered()) {
        logger.log(Level.INFO, "[ARTHAS-ATTACH] Tunnel registered (REGISTER_ACK received)");
        return TaskExecutionResult.success();
      }

      try {
        Thread.sleep(REGISTER_CHECK_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return TaskExecutionResult.failed("INTERRUPTED", "Interrupted while waiting for tunnel registration");
      }
    }

    return TaskExecutionResult.timeout(String.format(
        Locale.ROOT,
        "Tunnel registration timeout after %dms",
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
        "{\"status\":\"success\",\"message\":\"%s\",\"arthas_state\":\"%s\",\"tunnel_connected\":%s,\"tunnel_registered\":%s}",
        message,
        manager.getState(),
        tunnelClient != null && tunnelClient.isConnected(),
        manager.isRegistered());

    return TaskExecutionResult.success(resultJson);
  }
}
