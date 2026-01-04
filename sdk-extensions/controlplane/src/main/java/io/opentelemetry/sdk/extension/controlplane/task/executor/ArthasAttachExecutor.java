
package io.opentelemetry.sdk.extension.controlplane.task.executor;

import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasBootstrap;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasIntegration;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasLifecycleManager;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasLifecycleManager.StartResult;
import io.opentelemetry.sdk.extension.controlplane.task.status.TaskStatusEmitter;
import java.util.Locale;
import java.util.Objects;
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
 *   <li>启动 Arthas 服务（包含内部 TunnelClient）
 *   <li>等待 Arthas 内部 tunnel 注册成功
 *   <li>返回执行结果
 * </ul>
 *
 * <p>【模式2架构】由 Arthas 内部 TunnelClient(Netty) 负责 tunnel 连接：
 * <ul>
 *   <li>OTel 侧不再直接管理 tunnel 连接，通过 ArthasIntegration 观察状态</li>
 *   <li>通过 ArthasIntegration.isTunnelReady() 判断 tunnel 是否就绪</li>
 * </ul>
 *
 * <p>执行流程：
 * <pre>
 *   1. 检查 Arthas 当前状态
 *   2. 如果未运行，启动 Arthas（内部会同时启动 tunnel）
 *   3. 等待 Arthas 启动完成 + tunnel 注册成功
 *   4. 返回执行结果
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

  /** Arthas 集成（模式2核心：用于状态观测和事件订阅） */
  @Nullable private final ArthasIntegration arthasIntegration;

  /** 调度器 */
  @Nullable private final ScheduledExecutorService scheduler;

  /**
   * 创建 Arthas 附加执行器
   *
   * @param arthasIntegration Arthas 集成（模式2核心）
   * @param scheduler 调度器
   */
  public ArthasAttachExecutor(
      @Nullable ArthasIntegration arthasIntegration,
      @Nullable ScheduledExecutorService scheduler) {
    this.arthasIntegration = arthasIntegration;
    this.scheduler = scheduler;
  }

  @Override
  public String getTaskType() {
    return TASK_TYPE;
  }

  @Override
  public String getDescription() {
    return "Arthas attach executor - starts Arthas (with internal tunnel) and waits for registration";
  }

  @Override
  public boolean isAvailable() {
    // 只要有 Arthas 集成就认为可用
    return arthasIntegration != null;
  }

  @Override
  public CompletableFuture<TaskExecutionResult> execute(TaskExecutionContext context) {
    CompletableFuture<TaskExecutionResult> future = new CompletableFuture<>();

    String taskId = context.getTaskId();
    logger.log(Level.INFO, "[ARTHAS-ATTACH] Starting execution: taskId={0}", taskId);

    // 检查前置条件
    if (arthasIntegration == null) {
      logger.log(Level.WARNING, "[ARTHAS-ATTACH] ArthasIntegration not configured");
      future.complete(TaskExecutionResult.failed(
          "ARTHAS_NOT_CONFIGURED",
          "ArthasIntegration is not configured"));
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

    ArthasLifecycleManager manager = arthasIntegration.getLifecycleManager();
    @Nullable TaskStatusEmitter statusEmitter = context.getStatusEmitter();
    long startTime = System.currentTimeMillis();

    // 事件驱动模式：订阅状态变更，收到 REGISTER_ACK 时完成任务
    boolean enableEventMode = statusEmitter != null;

    if (enableEventMode) {
      TaskStatusEmitter emitter = Objects.requireNonNull(statusEmitter, "statusEmitter");

      // 已注册则直接成功
      if (arthasIntegration.isTunnelReady()) {
        TaskExecutionResult immediate = buildSuccessResult("Arthas already registered", manager);
        future.complete(TaskExecutionResult.success(immediate.getResultJson(), 0));
        return future;
      }

      ScheduledExecutorService effectiveScheduler = scheduler != null ? scheduler : context.getScheduler();
      if (effectiveScheduler == null) {
        future.complete(TaskExecutionResult.failed("NO_SCHEDULER", "No scheduler available for event-based attach"));
        return future;
      }

      // Tunnel 注册等待的有效超时：
      // - connectTimeout 是“单纯注册”窗口（默认 10s）
      // - startTimeout 是“整个启动”窗口（默认 30s）
      // 在实践中，Arthas 启动通常会消耗 1~3s，若仍使用固定 10s 很容易误判超时。
      // 因此这里取二者的较大值，并在 attach 已经运行了一段时间后扣除已消耗时间。
      long elapsedSinceStart = System.currentTimeMillis() - startTime;
      long remainingStartWindow = Math.max(0, startTimeout - elapsedSinceStart);
      long effectiveRegisterTimeout = Math.max(connectTimeout, remainingStartWindow);

      // 事件驱动：等待 tunnel 注册成功
      CompletableFuture<io.opentelemetry.sdk.extension.controlplane.arthas.ArthasStateEventBus.State>
          registeredFuture =
              arthasIntegration.awaitState(
                  s -> s.isTunnelRegistered(),
                  java.time.Duration.ofMillis(effectiveRegisterTimeout));

      // 重要：不能把“初始 STOPPED”当成启动失败。
      // 必须先观测到本次启动进入过 STARTING（或已经 RUNNING/IDLE），之后如果再回到 STOPPED 才算启动失败。
      CompletableFuture<io.opentelemetry.sdk.extension.controlplane.arthas.ArthasStateEventBus.State>
          startingObservedFuture =
              arthasIntegration.awaitState(
                  s -> {
                    ArthasLifecycleManager.State as = s.getArthasState();
                    return as == ArthasLifecycleManager.State.STARTING
                        || as == ArthasLifecycleManager.State.RUNNING
                        || as == ArthasLifecycleManager.State.IDLE;
                  },
                  java.time.Duration.ofMillis(startTimeout));

      CompletableFuture<io.opentelemetry.sdk.extension.controlplane.arthas.ArthasStateEventBus.State>
          stoppedAfterStartingFuture =
              startingObservedFuture.thenCompose(
                  ignored ->
                      arthasIntegration.awaitState(
                          s -> s.getArthasState() == ArthasLifecycleManager.State.STOPPED,
                          java.time.Duration.ofMillis(startTimeout)));

      // 任意一个先完成：registered -> SUCCESS；stoppedAfterStarting -> FAILED；timeout -> TIMEOUT
      CompletableFuture<Object> completion =
          CompletableFuture.anyOf(registeredFuture, stoppedAfterStartingFuture);

      @SuppressWarnings("FutureReturnValueIgnored")
      Object unusedCompletion =
          completion
              .thenAccept(
                  ignored -> {
                    if (future.isDone()) {
                      return;
                    }

                    long executionTime = System.currentTimeMillis() - startTime;

                    // 优先判断 STOPPED（启动失败：必须是已观测到 STARTING 之后的 STOPPED）
                    ArthasLifecycleManager.State st = manager.getState();
                    if (st == ArthasLifecycleManager.State.STOPPED) {
                      String lastError = manager.getStartupLogCollector().getLastError();
                      String msg =
                          (lastError != null && !lastError.isEmpty())
                              ? ("Arthas start failed: " + lastError)
                              : "Arthas start failed: lifecycle state STOPPED";

                      emitter.failed("ARTHAS_START_FAILED", msg);
                      future.complete(TaskExecutionResult.failed("ARTHAS_START_FAILED", msg, executionTime));
                      return;
                    }

                    // registered
                    if (arthasIntegration.isTunnelReady()) {
                      TaskExecutionResult ok = buildSuccessResult("Tunnel registered (REGISTER_ACK)", manager);
                      emitter.success(ok.getResultJson());
                      future.complete(TaskExecutionResult.success(ok.getResultJson(), executionTime));
                      return;
                    }

                    // neither stopped nor registered (shouldn't happen)
                    String msg = "Unexpected state while waiting for tunnel registration: " + manager.getState();
                    emitter.failed("ARTHAS_ATTACH_STATE_INVALID", msg);
                    future.complete(TaskExecutionResult.failed("ARTHAS_ATTACH_STATE_INVALID", msg, executionTime));
                  })
              .exceptionally(
                  e -> {
                    if (!future.isDone()) {
                      long executionTime = System.currentTimeMillis() - startTime;

                      // timeoutScheduler will wrap with CompletionException; preserve root cause
                      Throwable t = e;
                      if (t.getCause() != null) {
                        t = t.getCause();
                      }

                      // 如果此时已经 STOPPED，优先上报启动失败
                      if (manager.getState() == ArthasLifecycleManager.State.STOPPED) {
                        String lastError = manager.getStartupLogCollector().getLastError();
                        String msg =
                            (lastError != null && !lastError.isEmpty())
                                ? ("Arthas start failed: " + lastError)
                                : ("Arthas start failed: " + t.getMessage());
                        emitter.failed("ARTHAS_START_FAILED", msg);
                        future.complete(TaskExecutionResult.failed("ARTHAS_START_FAILED", msg, executionTime));
                      } else {
                        String logSummary = getStartupLogSummary(manager);
                        future.complete(
                            TaskExecutionResult.timeout(
                                String.format(
                                        Locale.ROOT,
                                        "Tunnel registration timeout after %dms (effective=%dms, elapsed=%dms), arthasState=%s%s",
                                        connectTimeout,
                                        effectiveRegisterTimeout,
                                        elapsedSinceStart,
                                        manager.getState(),
                                        logSummary),
                                executionTime));
                      }
                    }
                    return null;
                  });

      // 启动实际 attach 逻辑
      @SuppressWarnings("FutureReturnValueIgnored")
      Object unused =
          CompletableFuture.runAsync(
              () -> {
                try {
                  TaskExecutionResult r =
                      executeAttach(context, action, startTimeout, manager, emitter, effectiveScheduler);

                  // 若在启动阶段就失败，立即完成 future
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
                    // RUNNING 或 SUCCESS
                    if (r.isSuccess()) {
                      io.opentelemetry.sdk.extension.controlplane.arthas.ArthasReadinessGate.Result rr =
                          arthasIntegration.getReadinessGate().evaluateNow();
                      if (rr.isTerminalReady() || arthasIntegration.isTunnelReady()) {
                        long executionTime = System.currentTimeMillis() - startTime;
                        future.complete(TaskExecutionResult.success(r.getResultJson(), executionTime));
                      } else {
                        emitter.running("Arthas attach in progress (waiting for REGISTER_ACK)");
                      }
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

    // 无事件发射器：同步阻塞等待
    @SuppressWarnings("FutureReturnValueIgnored")
    Object unused =
        CompletableFuture.runAsync(
            () -> {
              long start = System.currentTimeMillis();
              try {
                ScheduledExecutorService effectiveScheduler = scheduler != null ? scheduler : context.getScheduler();
                TaskExecutionResult result =
                    executeAttach(context, action, startTimeout, manager, statusEmitter, effectiveScheduler);
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
   * @param action 操作类型（预留参数）
   * @param startTimeout 启动超时
   * @param manager 生命周期管理器
   * @param statusEmitter 状态发射器
   * @param effectiveScheduler 调度器
   * @return 执行结果
   */
  @SuppressWarnings("UnusedVariable")
  private TaskExecutionResult executeAttach(
      TaskExecutionContext context,
      String action,
      long startTimeout,
      ArthasLifecycleManager manager,
      @Nullable TaskStatusEmitter statusEmitter,
      @Nullable ScheduledExecutorService effectiveScheduler) {

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

    // 如果正在启动
    if (currentState == ArthasLifecycleManager.State.STARTING) {
      logger.log(Level.INFO, "[ARTHAS-ATTACH] Arthas is starting, taskId={0}", taskId);
      if (statusEmitter != null) {
        statusEmitter.running("Arthas is starting, waiting for RUNNING");
        return TaskExecutionResult.builder()
            .status(io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskStatus.RUNNING)
            .resultJson(
                "{\"status\":\"running\",\"message\":\"Arthas is starting, waiting for RUNNING\"}")
            .build();
      }
      return waitForArthasRunning(startTimeout, manager);
    }

    // Step 2: 启动 Arthas（内部会同时启动 tunnel）
    logger.log(Level.INFO, "[ARTHAS-ATTACH] Starting Arthas..., taskId={0}", taskId);

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

    if (statusEmitter != null) {
      statusEmitter.running("Arthas start requested, waiting for RUNNING");
      return TaskExecutionResult.builder()
          .status(io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskStatus.RUNNING)
          .resultJson(
              "{\"status\":\"running\",\"message\":\"Arthas start requested, waiting for RUNNING\"}")
          .build();
    }

    TaskExecutionResult waitResult = waitForArthasRunning(startTimeout, manager);
    if (!waitResult.isSuccess()) {
      return waitResult;
    }

    // Step 4: 【模式2】等待 Arthas 内部 tunnel 注册成功
    // 不再由 OTel 侧直接管理 tunnel 连接，只观察状态
    if (arthasIntegration != null && !arthasIntegration.isTunnelReady()) {
      logger.log(Level.INFO, "[ARTHAS-ATTACH] Waiting for internal tunnel registration..., taskId={0}", taskId);
      TaskExecutionResult registerResult = waitForTunnelRegistered(startTimeout);
      if (!registerResult.isSuccess()) {
        return registerResult;
      }
    }

    logger.log(Level.INFO, "[ARTHAS-ATTACH] Arthas attach completed successfully, taskId={0}", taskId);
    return buildSuccessResult("Arthas started and registered successfully", manager);
  }

  /**
   * 等待 Arthas 本地运行就绪
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
      
      if (state == ArthasLifecycleManager.State.STOPPED) {
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

    long elapsed = System.currentTimeMillis() - startTime;
    String errorMessage = buildTimeoutErrorMessage(timeoutMillis, elapsed, manager);
    
    return TaskExecutionResult.timeout(errorMessage);
  }

  /**
   * 构建超时错误消息
   */
  private static String buildTimeoutErrorMessage(
      long timeoutMillis, long elapsed, ArthasLifecycleManager manager) {
    
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(
        Locale.ROOT,
        "Arthas startup timeout after %dms (waited %dms), current state: %s",
        timeoutMillis, elapsed, manager.getState()));

    ArthasLifecycleManager.StartupLogCollector logCollector = manager.getStartupLogCollector();
    String lastError = logCollector.getLastError();
    
    if (lastError != null) {
      sb.append(". Last error: ").append(lastError);
    }

    ArthasBootstrap bootstrap = manager.getArthasBootstrap();
    if (bootstrap.getInstrumentation() == null) {
      sb.append(". Reason: Instrumentation not available - ");
      sb.append("SpyAPI cannot be loaded to Bootstrap ClassLoader, ");
      sb.append("this may cause Arthas to fail or work in degraded mode. ");
      sb.append("Please ensure InstrumentationHolder.set() is called in premain.");
    }

    String logSummary = getStartupLogSummary(manager);
    if (!logSummary.isEmpty()) {
      sb.append(logSummary);
    }

    return sb.toString();
  }

  /**
   * 获取启动日志摘要
   */
  private static String getStartupLogSummary(ArthasLifecycleManager manager) {
    ArthasLifecycleManager.StartupLogCollector logCollector = manager.getStartupLogCollector();
    java.util.List<String> logs = logCollector.getLogs();
    
    if (logs.isEmpty()) {
      return "";
    }
    
    StringBuilder sb = new StringBuilder();
    sb.append(" Startup logs: [");
    
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
   * 等待 Tunnel 注册成功（REGISTER_ACK）
   *
   * <p>【模式2】状态来源于 Arthas 内部 TunnelClient，通过 ArthasIntegration 观察
   */
  private TaskExecutionResult waitForTunnelRegistered(long timeoutMillis) {
    if (timeoutMillis <= 0) {
      return TaskExecutionResult.timeout("Tunnel registration timeout");
    }

    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (System.currentTimeMillis() < deadline) {
      // 【模式2】通过 ArthasIntegration 检查 tunnel 状态
      if (arthasIntegration != null && arthasIntegration.isTunnelReady()) {
        logger.log(Level.INFO, "[ARTHAS-ATTACH] Tunnel registered (REGISTER_ACK received via internal TunnelClient)");
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
    boolean tunnelReady = arthasIntegration != null && arthasIntegration.isTunnelReady();
    
    String resultJson = String.format(
        Locale.ROOT,
        "{\"status\":\"success\",\"message\":\"%s\",\"arthas_state\":\"%s\",\"tunnel_ready\":%s}",
        message,
        manager.getState(),
        tunnelReady);

    return TaskExecutionResult.success(resultJson);
  }
}
