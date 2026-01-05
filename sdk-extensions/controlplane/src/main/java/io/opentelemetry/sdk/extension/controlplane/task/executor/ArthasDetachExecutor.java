/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.executor;

import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasIntegration;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasLifecycleManager;
import io.opentelemetry.sdk.extension.controlplane.task.status.TaskStatusEmitter;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 分离执行器
 *
 * <p>负责执行 arthas_detach 类型的任务，实现：
 * <ul>
 *   <li>停止 Arthas 服务（包含内部 TunnelClient）
 *   <li>等待 Arthas 完全停止
 *   <li>返回执行结果
 * </ul>
 *
 * <p>执行流程：
 * <pre>
 *   1. 检查 Arthas 当前状态
 *   2. 如果正在运行，调用停止方法
 *   3. 等待 Arthas 完全停止
 *   4. 返回执行结果
 * </pre>
 */
public final class ArthasDetachExecutor implements TaskExecutor {

  private static final Logger logger = Logger.getLogger(ArthasDetachExecutor.class.getName());

  /** 任务类型 */
  public static final String TASK_TYPE = "arthas_detach";

  /** 默认停止超时：10秒 */
  private static final long DEFAULT_STOP_TIMEOUT_MILLIS = 10 * 1000;

  /** 检查间隔：200ms */
  private static final long CHECK_INTERVAL_MILLIS = 200;

  /** Arthas 集成 */
  @Nullable private final ArthasIntegration arthasIntegration;

  /**
   * 创建 Arthas 分离执行器
   *
   * @param arthasIntegration Arthas 集成
   */
  public ArthasDetachExecutor(@Nullable ArthasIntegration arthasIntegration) {
    this.arthasIntegration = arthasIntegration;
  }

  @Override
  public String getTaskType() {
    return TASK_TYPE;
  }

  @Override
  public String getDescription() {
    return "Arthas detach executor - stops Arthas service and releases resources";
  }

  @Override
  public boolean isAvailable() {
    return arthasIntegration != null;
  }

  @Override
  public CompletableFuture<TaskExecutionResult> execute(TaskExecutionContext context) {
    CompletableFuture<TaskExecutionResult> future = new CompletableFuture<>();

    String taskId = context.getTaskId();
    logger.log(Level.INFO, "[ARTHAS-DETACH] Starting execution: taskId={0}", taskId);

    // 检查前置条件
    if (arthasIntegration == null) {
      logger.log(Level.WARNING, "[ARTHAS-DETACH] ArthasIntegration not configured");
      future.complete(TaskExecutionResult.failed(
          "ARTHAS_NOT_CONFIGURED",
          "ArthasIntegration is not configured"));
      return future;
    }

    // 获取参数
    String action = context.getStringParameter("action", "detach");
    long stopTimeout = context.getLongParameter("stop_timeout_millis", DEFAULT_STOP_TIMEOUT_MILLIS);
    boolean force = context.getBooleanParameter("force", false);

    logger.log(
        Level.INFO,
        "[ARTHAS-DETACH] Parameters: action={0}, stopTimeout={1}ms, force={2}",
        new Object[] {action, stopTimeout, force});

    ArthasLifecycleManager manager = arthasIntegration.getLifecycleManager();
    @Nullable TaskStatusEmitter statusEmitter = context.getStatusEmitter();
    long startTime = System.currentTimeMillis();

    // 异步执行停止操作
    @SuppressWarnings("FutureReturnValueIgnored")
    Object unused = CompletableFuture.runAsync(() -> {
      try {
        TaskExecutionResult result = executeDetach(
            taskId, manager, stopTimeout, force, statusEmitter);

        long executionTime = System.currentTimeMillis() - startTime;
        
        if (result.isSuccess()) {
          future.complete(TaskExecutionResult.success(result.getResultJson(), executionTime));
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
        logger.log(Level.WARNING, "[ARTHAS-DETACH] Execution failed: {0}", e.getMessage());
        future.complete(TaskExecutionResult.failed(
            "ARTHAS_DETACH_ERROR",
            "Arthas detach failed: " + e.getMessage(),
            executionTime));
      }
    });

    return future;
  }

  /**
   * 执行 Arthas 分离操作
   *
   * @param taskId 任务 ID
   * @param manager 生命周期管理器
   * @param stopTimeout 停止超时
   * @param force 是否强制停止
   * @param statusEmitter 状态发射器
   * @return 执行结果
   */
  @SuppressWarnings("MethodCanBeStatic")
  private TaskExecutionResult executeDetach(
      String taskId,
      ArthasLifecycleManager manager,
      long stopTimeout,
      boolean force,
      @Nullable TaskStatusEmitter statusEmitter) {

    // Step 1: 检查当前状态
    ArthasLifecycleManager.State currentState = manager.getState();
    logger.log(
        Level.INFO,
        "[ARTHAS-DETACH] Current Arthas state: {0}, taskId={1}",
        new Object[] {currentState, taskId});

    // 如果已经停止，直接返回成功
    if (currentState == ArthasLifecycleManager.State.STOPPED) {
      logger.log(Level.INFO, "[ARTHAS-DETACH] Arthas already stopped, taskId={0}", taskId);
      return buildSuccessResult("Arthas already stopped", manager);
    }

    // 如果正在停止，等待停止完成
    if (currentState == ArthasLifecycleManager.State.STOPPING) {
      logger.log(Level.INFO, "[ARTHAS-DETACH] Arthas is stopping, waiting for completion, taskId={0}", taskId);
      if (statusEmitter != null) {
        statusEmitter.running("Arthas is stopping, waiting for completion");
      }
      return waitForStopped(stopTimeout, manager, taskId);
    }

    // Step 2: 发起停止请求
    logger.log(Level.INFO, "[ARTHAS-DETACH] Stopping Arthas..., taskId={0}", taskId);
    if (statusEmitter != null) {
      statusEmitter.running("Stopping Arthas...");
    }

    boolean stopRequested = manager.stop();

    if (!stopRequested) {
      // stop() 返回 false 通常意味着状态在并发修改中变化
      // 再次检查当前状态
      ArthasLifecycleManager.State newState = manager.getState();
      if (newState == ArthasLifecycleManager.State.STOPPED) {
        logger.log(Level.INFO, "[ARTHAS-DETACH] Arthas stopped during stop request, taskId={0}", taskId);
        return buildSuccessResult("Arthas stopped successfully", manager);
      }

      logger.log(
          Level.WARNING,
          "[ARTHAS-DETACH] Stop request failed, current state: {0}, taskId={1}",
          new Object[] {newState, taskId});

      // 如果是强制模式，尝试通过 syncStoppedFromExternalSignal 强制停止
      if (force) {
        logger.log(Level.INFO, "[ARTHAS-DETACH] Forcing stop via syncStoppedFromExternalSignal, taskId={0}", taskId);
        manager.syncStoppedFromExternalSignal("force_detach_task:" + taskId);
        return buildSuccessResult("Arthas force stopped", manager);
      }

      return TaskExecutionResult.failed(
          "STOP_REQUEST_FAILED",
          "Failed to initiate Arthas stop, current state: " + newState);
    }

    // Step 3: 等待 Arthas 完全停止
    logger.log(Level.INFO, "[ARTHAS-DETACH] Waiting for Arthas to stop..., taskId={0}", taskId);
    return waitForStopped(stopTimeout, manager, taskId);
  }

  /**
   * 等待 Arthas 停止完成
   *
   * @param timeoutMillis 超时时间
   * @param manager 生命周期管理器
   * @param taskId 任务 ID
   * @return 执行结果
   */
  @SuppressWarnings("MethodCanBeStatic")
  private TaskExecutionResult waitForStopped(
      long timeoutMillis, ArthasLifecycleManager manager, String taskId) {

    long deadline = System.currentTimeMillis() + timeoutMillis;
    long startTime = System.currentTimeMillis();

    while (System.currentTimeMillis() < deadline) {
      ArthasLifecycleManager.State state = manager.getState();

      if (state == ArthasLifecycleManager.State.STOPPED) {
        long elapsed = System.currentTimeMillis() - startTime;
        logger.log(
            Level.INFO,
            "[ARTHAS-DETACH] Arthas stopped after {0}ms, taskId={1}",
            new Object[] {elapsed, taskId});
        return buildSuccessResult("Arthas stopped successfully", manager);
      }

      // 如果状态变回 RUNNING/IDLE，说明停止被取消或失败
      if (state == ArthasLifecycleManager.State.RUNNING
          || state == ArthasLifecycleManager.State.IDLE) {
        logger.log(
            Level.WARNING,
            "[ARTHAS-DETACH] Stop cancelled, Arthas returned to {0}, taskId={1}",
            new Object[] {state, taskId});
        return TaskExecutionResult.failed(
            "STOP_CANCELLED",
            "Arthas stop was cancelled, current state: " + state);
      }

      try {
        Thread.sleep(CHECK_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return TaskExecutionResult.failed(
            "INTERRUPTED",
            "Interrupted while waiting for Arthas to stop");
      }
    }

    // 超时
    long elapsed = System.currentTimeMillis() - startTime;
    ArthasLifecycleManager.State finalState = manager.getState();
    
    logger.log(
        Level.WARNING,
        "[ARTHAS-DETACH] Stop timeout after {0}ms, current state: {1}, taskId={2}",
        new Object[] {elapsed, finalState, taskId});

    return TaskExecutionResult.timeout(String.format(
        Locale.ROOT,
        "Arthas stop timeout after %dms, current state: %s",
        timeoutMillis, finalState));
  }

  /**
   * 构建成功结果
   *
   * @param message 消息
   * @param manager 生命周期管理器
   * @return 执行结果
   */
  @SuppressWarnings("MethodCanBeStatic")
  private TaskExecutionResult buildSuccessResult(
      String message, ArthasLifecycleManager manager) {

    String resultJson = String.format(
        Locale.ROOT,
        "{\"status\":\"success\",\"message\":\"%s\",\"arthas_state\":\"%s\"}",
        message,
        manager.getState());

    return TaskExecutionResult.success(resultJson);
  }
}
