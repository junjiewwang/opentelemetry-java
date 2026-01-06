package io.opentelemetry.sdk.extension.controlplane.task.executor;

import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasBootstrap;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasIntegration;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasLifecycleManager;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasLifecycleManager.StartResult;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasReadinessGate;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasStateEventBus;
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
 * <p>执行流程（事件驱动模式）：
 * <pre>
 *   1. 检查是否已就绪 → 是则直接返回成功
 *   2. 订阅状态事件：等待 tunnel 注册 或 启动失败
 *   3. 发起启动请求（如需要）
 *   4. 通过事件回调完成任务
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

  /**
   * 自愈后重启预算（毫秒）
   *
   * <p>自愈清理后，至少需要预留这么多时间用于：
   * <ul>
   *   <li>销毁旧 Arthas 实例</li>
   *   <li>启动新 Arthas 实例</li>
   *   <li>等待 tunnel 注册</li>
   * </ul>
   */
  private static final long RESTART_BUDGET_MILLIS = 15_000;

  /**
   * grace 最小值（毫秒）
   *
   * <p>避免过于激进的误杀（例如：启动慢被当成异常）
   */
  private static final long MIN_GRACE_PERIOD_MILLIS = 5_000;

  /**
   * grace 最大值（毫秒）
   *
   * <p>与被动兜底机制（5分钟超时）保持合理区间
   */
  private static final long MAX_GRACE_PERIOD_MILLIS = 30_000;

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

  // ===== 统一就绪检查 =====

  /**
   * 检查是否已就绪（Arthas 本地可用 + tunnel 已注册）
   *
   * <p>统一入口，避免多处重复调用 readinessGate.evaluateNow()
   *
   * @return 是否就绪
   */
  private boolean isReadyNow() {
    if (arthasIntegration == null) {
      return false;
    }
    ArthasReadinessGate.Result result = arthasIntegration.getReadinessGate().evaluateNow();
    return result.isTerminalReady();
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

    // 获取参数（统一计算有效超时）
    long startTimeout = context.getLongParameter("start_timeout_millis", DEFAULT_START_TIMEOUT_MILLIS);
    long connectTimeout = context.getLongParameter("connect_timeout_millis", DEFAULT_CONNECT_TIMEOUT_MILLIS);
    // 有效超时：取 startTimeout 和 connectTimeout 的较大值
    long effectiveTimeout = Math.max(startTimeout, connectTimeout);

    logger.log(
        Level.INFO,
        "[ARTHAS-ATTACH] Parameters: startTimeout={0}ms, connectTimeout={1}ms, effectiveTimeout={2}ms",
        new Object[] {startTimeout, connectTimeout, effectiveTimeout});

    // 【自愈机制】健康检查 + 清理（在发起启动请求之前，需要 effectiveTimeout 计算 grace）
    cleanupUnhealthyArthasIfNeeded(context, effectiveTimeout);

    ArthasLifecycleManager manager = arthasIntegration.getLifecycleManager();
    @Nullable TaskStatusEmitter statusEmitter = context.getStatusEmitter();
    long startTime = System.currentTimeMillis();

    // 快速路径：已就绪则直接返回成功
    if (isReadyNow()) {
      logger.log(Level.INFO, "[ARTHAS-ATTACH] Already ready, returning success immediately, taskId={0}", taskId);
      TaskExecutionResult immediate = buildSuccessResult("Arthas already ready (terminal-ready)", manager);
      future.complete(TaskExecutionResult.success(immediate.getResultJson(), 0));
      return future;
    }

    // 获取有效调度器
    ScheduledExecutorService effectiveScheduler = scheduler != null ? scheduler : context.getScheduler();
    if (effectiveScheduler == null) {
      future.complete(TaskExecutionResult.failed("NO_SCHEDULER", "No scheduler available for attach operation"));
      return future;
    }

    // 事件驱动模式（推荐）：通过状态事件驱动任务完成
    if (statusEmitter != null) {
      return executeEventDriven(context, future, manager, statusEmitter, effectiveScheduler, effectiveTimeout, startTime);
    }

    // 阻塞模式（兜底）：同步等待
    return executeBlocking(context, future, manager, effectiveScheduler, effectiveTimeout, startTime);
  }

  /**
   * 事件驱动模式执行
   *
   * <p>通过订阅状态事件来完成任务，避免阻塞线程
   */
  private CompletableFuture<TaskExecutionResult> executeEventDriven(
      TaskExecutionContext context,
      CompletableFuture<TaskExecutionResult> future,
      ArthasLifecycleManager manager,
      TaskStatusEmitter emitter,
      ScheduledExecutorService effectiveScheduler,
      long effectiveTimeout,
      long startTime) {

    String taskId = context.getTaskId();

    // 安全获取 arthasIntegration（在此方法中已由调用方确认不为 null）
    ArthasIntegration integration = Objects.requireNonNull(arthasIntegration, "arthasIntegration");

    // 订阅状态事件：等待 tunnel 注册成功
    CompletableFuture<io.opentelemetry.sdk.extension.controlplane.arthas.ArthasStateEventBus.State>
        registeredFuture =
            integration.awaitState(
                ArthasStateEventBus.State::isTunnelRegistered,
                java.time.Duration.ofMillis(effectiveTimeout));

    // 订阅状态事件：检测启动失败（观测到 STARTING 后又回到 STOPPED）
    CompletableFuture<io.opentelemetry.sdk.extension.controlplane.arthas.ArthasStateEventBus.State>
        startingObservedFuture =
            integration.awaitState(
                s -> {
                  ArthasLifecycleManager.State as = s.getArthasState();
                  return as == ArthasLifecycleManager.State.STARTING
                      || as == ArthasLifecycleManager.State.RUNNING
                      || as == ArthasLifecycleManager.State.IDLE;
                },
                java.time.Duration.ofMillis(effectiveTimeout));

    CompletableFuture<io.opentelemetry.sdk.extension.controlplane.arthas.ArthasStateEventBus.State>
        stoppedAfterStartingFuture =
            startingObservedFuture.thenCompose(
                ignored ->
                    integration.awaitState(
                        s -> s.getArthasState() == ArthasLifecycleManager.State.STOPPED,
                        java.time.Duration.ofMillis(effectiveTimeout)));

    // 任意一个先完成：registered -> SUCCESS；stoppedAfterStarting -> FAILED
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
                  handleEventCompletion(future, manager, emitter, startTime);
                })
            .exceptionally(
                e -> {
                  if (!future.isDone()) {
                    handleEventTimeout(future, manager, emitter, effectiveTimeout, startTime, e);
                  }
                  return null;
                });

    // 发起启动请求（非阻塞）
    @SuppressWarnings("FutureReturnValueIgnored")
    Object unused =
        CompletableFuture.runAsync(
            () -> {
              try {
                TaskExecutionResult startResult = tryStartArthas(taskId, manager, effectiveScheduler);

                if (startResult.isFailed()) {
                  // 启动请求本身失败（如：状态不允许启动）
                  long executionTime = System.currentTimeMillis() - startTime;
                  String errorCode = startResult.getErrorCode() != null ? startResult.getErrorCode() : "UNKNOWN_ERROR";
                  String errorMsg = startResult.getErrorMessage() != null ? startResult.getErrorMessage() : "Unknown error";
                  emitter.failed(errorCode, errorMsg);
                  future.complete(
                      TaskExecutionResult.builder()
                          .status(startResult.getStatus())
                          .errorCode(startResult.getErrorCode())
                          .errorMessage(startResult.getErrorMessage())
                          .executionTimeMillis(executionTime)
                          .completedAtMillis(System.currentTimeMillis())
                          .build());
                } else {
                  // 启动请求已发出，等待事件回调
                  emitter.running("Arthas attach in progress, waiting for tunnel registration");
                }
              } catch (RuntimeException e) {
                long executionTime = System.currentTimeMillis() - startTime;
                emitter.failed("ARTHAS_ATTACH_ERROR", "Arthas attach failed: " + e.getMessage());
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
   * 处理事件驱动模式的完成回调
   */
  private void handleEventCompletion(
      CompletableFuture<TaskExecutionResult> future,
      ArthasLifecycleManager manager,
      TaskStatusEmitter emitter,
      long startTime) {

    long executionTime = System.currentTimeMillis() - startTime;

    // 优先判断 STOPPED（启动失败）
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

    // 检查是否已就绪（方式1：完整就绪检查）
    if (isReadyNow()) {
      TaskExecutionResult ok = buildSuccessResult("Tunnel registered (REGISTER_ACK)", manager);
      emitter.success(ok.getResultJson());
      future.complete(TaskExecutionResult.success(ok.getResultJson(), executionTime));
      return;
    }

    // 检查是否已就绪（方式2：直接检查 tunnel 注册状态）
    // 说明：当 tunnel 刚注册时，readinessGate 状态可能还未同步，
    //       但只要 Arthas 运行中 + tunnel 已注册，就应视为 attach 成功
    if (arthasIntegration != null
        && (st == ArthasLifecycleManager.State.RUNNING || st == ArthasLifecycleManager.State.IDLE)
        && arthasIntegration.isTunnelReady()) {
      TaskExecutionResult ok = buildSuccessResult("Tunnel registered (direct check)", manager);
      emitter.success(ok.getResultJson());
      future.complete(TaskExecutionResult.success(ok.getResultJson(), executionTime));
      return;
    }

    // 既不是 STOPPED 也不是就绪
    // 补充诊断信息：输出当前各组件状态，便于排查
    boolean tunnelReady = arthasIntegration != null && arthasIntegration.isTunnelReady();
    String msg = String.format(
        Locale.ROOT,
        "Unexpected state while waiting for tunnel registration: arthasState=%s, tunnelReady=%s",
        manager.getState(), tunnelReady);
    emitter.failed("ARTHAS_ATTACH_STATE_INVALID", msg);
    future.complete(TaskExecutionResult.failed("ARTHAS_ATTACH_STATE_INVALID", msg, executionTime));
  }

  /**
   * 处理事件驱动模式的超时回调
   */
  private static void handleEventTimeout(
      CompletableFuture<TaskExecutionResult> future,
      ArthasLifecycleManager manager,
      TaskStatusEmitter emitter,
      long effectiveTimeout,
      long startTime,
      Throwable e) {

    long executionTime = System.currentTimeMillis() - startTime;

    // 解包异常
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
      String timeoutMsg = String.format(
          Locale.ROOT,
          "Tunnel registration timeout after %dms, arthasState=%s%s",
          effectiveTimeout,
          manager.getState(),
          logSummary);
      emitter.failed("TUNNEL_REGISTER_TIMEOUT", timeoutMsg);
      future.complete(TaskExecutionResult.timeout(timeoutMsg, executionTime));
    }
  }

  /**
   * 阻塞模式执行（兜底方案）
   *
   * <p>当没有 statusEmitter 时使用同步等待
   */
  private CompletableFuture<TaskExecutionResult> executeBlocking(
      TaskExecutionContext context,
      CompletableFuture<TaskExecutionResult> future,
      ArthasLifecycleManager manager,
      ScheduledExecutorService effectiveScheduler,
      long effectiveTimeout,
      long startTime) {

    @SuppressWarnings("FutureReturnValueIgnored")
    Object unused =
        CompletableFuture.runAsync(
            () -> {
              try {
                TaskExecutionResult result = executeAttachBlocking(
                    context.getTaskId(), manager, effectiveScheduler, effectiveTimeout);
                long executionTime = System.currentTimeMillis() - startTime;

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
                long executionTime = System.currentTimeMillis() - startTime;
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
   * 尝试启动 Arthas（非阻塞）
   *
   * <p>只负责发起启动请求，不等待启动完成
   *
   * @param taskId 任务 ID（用于日志）
   * @param manager 生命周期管理器
   * @param scheduler 调度器
   * @return 启动请求结果（SUCCESS 表示请求已接受，不代表启动完成）
   */
  private static TaskExecutionResult tryStartArthas(
      String taskId,
      ArthasLifecycleManager manager,
      ScheduledExecutorService scheduler) {

    ArthasLifecycleManager.State currentState = manager.getState();
    logger.log(
        Level.INFO,
        "[ARTHAS-ATTACH] Current Arthas state: {0}, taskId={1}",
        new Object[] {currentState, taskId});

    // 已在运行：不需要启动
    if (currentState == ArthasLifecycleManager.State.RUNNING
        || currentState == ArthasLifecycleManager.State.IDLE) {
      logger.log(Level.INFO, "[ARTHAS-ATTACH] Arthas already running, taskId={0}", taskId);
      return TaskExecutionResult.success("{\"status\":\"running\",\"message\":\"Arthas already running\"}");
    }

    // 正在启动：等待即可
    if (currentState == ArthasLifecycleManager.State.STARTING) {
      logger.log(Level.INFO, "[ARTHAS-ATTACH] Arthas is starting, taskId={0}", taskId);
      return TaskExecutionResult.success("{\"status\":\"starting\",\"message\":\"Arthas is starting\"}");
    }

    // 发起启动请求
    logger.log(Level.INFO, "[ARTHAS-ATTACH] Starting Arthas..., taskId={0}", taskId);
    StartResult startResult = manager.tryStart(scheduler);

    if (!startResult.isSuccess()) {
      logger.log(
          Level.WARNING,
          "[ARTHAS-ATTACH] Failed to start Arthas: {0}, taskId={1}",
          new Object[] {startResult.getErrorMessage(), taskId});
      return TaskExecutionResult.failed(
          "ARTHAS_START_FAILED",
          "Failed to start Arthas: " + startResult.getErrorMessage());
    }

    return TaskExecutionResult.success("{\"status\":\"starting\",\"message\":\"Arthas start requested\"}");
  }

  /**
   * 阻塞模式执行 Arthas 附加操作
   *
   * <p>包含启动 + 等待就绪的完整流程
   */
  private TaskExecutionResult executeAttachBlocking(
      String taskId,
      ArthasLifecycleManager manager,
      ScheduledExecutorService scheduler,
      long timeoutMillis) {

    // Step 1: 再次检查是否已就绪
    if (isReadyNow()) {
      logger.log(Level.INFO, "[ARTHAS-ATTACH] Already ready, taskId={0}", taskId);
      return buildSuccessResult("Arthas already ready", manager);
    }

    // Step 2: 尝试启动
    TaskExecutionResult startResult = tryStartArthas(taskId, manager, scheduler);
    if (startResult.isFailed()) {
      return startResult;
    }

    // Step 3: 等待 Arthas 本地运行
    ArthasLifecycleManager.State currentState = manager.getState();
    if (currentState != ArthasLifecycleManager.State.RUNNING
        && currentState != ArthasLifecycleManager.State.IDLE) {
      TaskExecutionResult waitResult = waitForArthasRunning(timeoutMillis, manager);
      if (!waitResult.isSuccess()) {
        return waitResult;
      }
    }

    // Step 4: 等待 tunnel 注册
    if (!isReadyNow()) {
      logger.log(Level.INFO, "[ARTHAS-ATTACH] Waiting for tunnel registration..., taskId={0}", taskId);
      TaskExecutionResult registerResult = waitForTunnelRegistered(timeoutMillis);
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
      return TaskExecutionResult.timeout("Tunnel registration timeout (no time left)");
    }

    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (System.currentTimeMillis() < deadline) {
      // 使用统一的就绪检查
      if (isReadyNow()) {
        logger.log(Level.INFO, "[ARTHAS-ATTACH] Tunnel registered (REGISTER_ACK received via internal TunnelClient)");
        return TaskExecutionResult.success();
      }

      try {
        Thread.sleep(CHECK_INTERVAL_MILLIS);
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

  // ===== 健康检查 + 清理（自愈机制） =====

  /**
   * 检查并清理不健康的 Arthas 实例
   *
   * <p>在 attach 任务开始时调用，用于：
   * <ul>
   *   <li>检测 Arthas 是否处于"运行但 tunnel 不可用"的异常状态</li>
   *   <li>如果异常状态持续超过宽限期，则主动销毁 Arthas</li>
   *   <li>允许后续流程重新启动新的 Arthas 实例</li>
   * </ul>
   *
   * <p>grace 计算策略：
   * <pre>
   *   grace = min(MAX_GRACE, max(MIN_GRACE, effectiveTimeout - RESTART_BUDGET))
   * </pre>
   * 确保自愈触发后仍有足够时间完成重启。
   *
   * @param context 任务上下文
   * @param effectiveTimeout 本次 attach 任务的有效超时（毫秒）
   */
  private void cleanupUnhealthyArthasIfNeeded(TaskExecutionContext context, long effectiveTimeout) {
    if (arthasIntegration == null) {
      return;
    }

    // 动态计算 grace（允许服务端通过参数覆盖）
    long gracePeriod = context.getLongParameter(
        "health_check_grace_period_millis",
        calculateGracePeriod(effectiveTimeout));

    // 先做诊断检查（不执行清理）
    ArthasIntegration.UnhealthyStatus unhealthyStatus =
        arthasIntegration.checkUnhealthyStatus(gracePeriod);

    if (unhealthyStatus == null) {
      // 健康或不需要清理
      logger.log(Level.FINE, "[ARTHAS-ATTACH] Health check passed, no cleanup needed");
      return;
    }

    // 记录诊断信息
    logger.log(Level.WARNING,
        "[ARTHAS-ATTACH] Detected unhealthy Arthas instance: {0}, effectiveTimeout={1}ms, gracePeriod={2}ms, taskId={3}",
        new Object[] {unhealthyStatus, effectiveTimeout, gracePeriod, context.getTaskId()});

    // 执行清理
    boolean cleaned = arthasIntegration.cleanupIfUnhealthyForAttach(
        gracePeriod,
        "attach_task:" + context.getTaskId());

    if (cleaned) {
      logger.log(Level.INFO,
          "[ARTHAS-ATTACH] Cleaned up unhealthy Arthas, will start fresh, taskId={0}",
          context.getTaskId());
    }
  }

  /**
   * 动态计算 grace 宽限期
   *
   * <p>策略：在 effectiveTimeout 中预留 RESTART_BUDGET 给重启，剩余时间用于 grace。
   * <pre>
   *   grace = min(MAX_GRACE, max(MIN_GRACE, effectiveTimeout - RESTART_BUDGET))
   * </pre>
   *
   * <p>示例：
   * <ul>
   *   <li>effectiveTimeout=30s → grace=15s（30-15=15）</li>
   *   <li>effectiveTimeout=60s → grace=30s（60-15=45，但被 MAX 限制为 30）</li>
   *   <li>effectiveTimeout=18s → grace=5s（18-15=3，但被 MIN 限制为 5）</li>
   * </ul>
   *
   * @param effectiveTimeout 本次 attach 任务的有效超时（毫秒）
   * @return 计算后的 grace 宽限期（毫秒）
   */
  private static long calculateGracePeriod(long effectiveTimeout) {
    // 预留重启预算后的剩余时间
    long remaining = effectiveTimeout - RESTART_BUDGET_MILLIS;
    // 应用上下限
    return Math.min(MAX_GRACE_PERIOD_MILLIS, Math.max(MIN_GRACE_PERIOD_MILLIS, remaining));
  }
}
