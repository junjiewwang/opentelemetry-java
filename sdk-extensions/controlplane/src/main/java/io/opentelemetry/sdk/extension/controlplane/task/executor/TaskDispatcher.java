/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.executor;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskInfo;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskResultRequest;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskResultResponse;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskStatus;
import io.opentelemetry.sdk.extension.controlplane.task.TaskExecutionLogger;
import io.opentelemetry.sdk.extension.controlplane.task.status.TaskStatusEmitter;
import io.opentelemetry.sdk.extension.controlplane.task.status.TaskStatusEvent;
import io.opentelemetry.sdk.extension.controlplane.task.status.TaskStatusEventManager;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 任务分发器
 *
 * <p>负责将接收到的任务分发给对应的执行器执行，并管理任务的生命周期：
 * <ul>
 *   <li>任务路由：根据 taskType 查找对应的 TaskExecutor
 *   <li>执行管理：异步执行任务，支持超时控制
 *   <li>状态上报：执行完成后自动上报结果到服务端
 *   <li>幂等性：防止同一任务重复执行
 * </ul>
 *
 * <p>遵循开闭原则：通过 {@link #registerExecutor(TaskExecutor)} 注册新的执行器，
 * 无需修改分发器代码即可支持新的任务类型。
 */
public final class TaskDispatcher implements Closeable {

  /** 分发结果状态 */
  public enum DispatchStatus {
    /** 分发成功，任务正在执行 */
    SUCCESS,
    /** 任务已经在运行中（幂等性检查） */
    ALREADY_RUNNING,
    /** 分发器已关闭 */
    DISPATCHER_CLOSED,
    /** 没有对应的执行器 */
    NO_EXECUTOR,
    /** 执行器不可用 */
    EXECUTOR_UNAVAILABLE
  }

  /** 分发结果 */
  public static final class DispatchResult {
    private final DispatchStatus status;
    private final String message;

    private DispatchResult(DispatchStatus status, String message) {
      this.status = status;
      this.message = message;
    }

    public static DispatchResult success() {
      return new DispatchResult(DispatchStatus.SUCCESS, "Task dispatched successfully");
    }

    public static DispatchResult alreadyRunning() {
      return new DispatchResult(DispatchStatus.ALREADY_RUNNING, "Task already running");
    }

    public static DispatchResult dispatcherClosed() {
      return new DispatchResult(DispatchStatus.DISPATCHER_CLOSED, "TaskDispatcher is closed");
    }

    public static DispatchResult noExecutor(String taskType) {
      return new DispatchResult(DispatchStatus.NO_EXECUTOR, 
          "No executor registered for task type: " + taskType);
    }

    public static DispatchResult executorUnavailable(String taskType) {
      return new DispatchResult(DispatchStatus.EXECUTOR_UNAVAILABLE, 
          "Executor for " + taskType + " is not available");
    }

    public DispatchStatus getStatus() {
      return status;
    }

    public String getMessage() {
      return message;
    }

    public boolean isSuccess() {
      return status == DispatchStatus.SUCCESS;
    }

    public boolean isAlreadyRunning() {
      return status == DispatchStatus.ALREADY_RUNNING;
    }
  }

  private static final Logger logger = Logger.getLogger(TaskDispatcher.class.getName());

  /** 默认任务超时时间：60秒 */
  private static final long DEFAULT_TIMEOUT_MILLIS = 60 * 1000;

  /** 执行器注册表（taskType -> TaskExecutor） */
  private final Map<String, TaskExecutor> executors;

  /** 正在执行的任务（用于幂等性检查） */
  private final Set<String> runningTasks;

  /** 任务执行线程池 */
  private final ExecutorService taskExecutor;

  /** 调度器（用于超时控制） */
  private final ScheduledExecutorService scheduler;

  /** 控制平面客户端 */
  private final ControlPlaneClient client;

  /** Agent ID */
  private final String agentId;

  /** 任务日志记录器 */
  private final TaskExecutionLogger taskLogger;

  /** 是否关闭 */
  private volatile boolean closed = false;

  /** 统一任务状态事件管理器（事件驱动上报入口） */
  private final TaskStatusEventManager statusEventManager = new TaskStatusEventManager();

  /** 每个任务当前已上报到服务端的最终状态（用于去重/幂等） */
  private final Map<String, AtomicReference<TaskStatus>> reportedTerminalStatus =
      new ConcurrentHashMap<>();

  /**
   * 创建任务分发器
   *
   * @param client 控制平面客户端
   * @param agentId Agent ID
   */
  public TaskDispatcher(ControlPlaneClient client, String agentId) {
    this(client, agentId, null);
  }

  /**
   * 创建任务分发器（带自定义调度器）
   *
   * @param client 控制平面客户端
   * @param agentId Agent ID
   * @param scheduler 调度器（可选，为 null 时内部创建）
   */
  public TaskDispatcher(
      ControlPlaneClient client,
      String agentId,
      @Nullable ScheduledExecutorService scheduler) {
    this.client = client;
    this.agentId = agentId;
    this.executors = new ConcurrentHashMap<>();
    this.runningTasks = ConcurrentHashMap.newKeySet();
    this.taskLogger = TaskExecutionLogger.getInstance();

    // 创建任务执行线程池
    this.taskExecutor = Executors.newCachedThreadPool(r -> {
      Thread t = new Thread(r, "otel-task-executor");
      t.setDaemon(true);
      return t;
    });

    // 使用提供的调度器或创建新的
    this.scheduler = scheduler != null ? scheduler : Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "otel-task-timeout");
      t.setDaemon(true);
      return t;
    });

    logger.log(Level.INFO, "TaskDispatcher initialized for agent: {0}", agentId);

    // 将事件管理器的事件统一转为 reportResult 上报（实时）
    this.statusEventManager.addListener(this::onTaskStatusEvent);
  }

  private void onTaskStatusEvent(TaskStatusEvent event) {
    if (closed) {
      return;
    }
    String taskId = event.getTaskId();
    TaskExecutionResult result = event.toExecutionResult();

    // 终态幂等：SUCCESS/FAILED/TIMEOUT/CANCELLED 只上报一次；RUNNING 可重复但会被管理器做节流/合并。
    if (result.getStatus() != TaskStatus.RUNNING) {
      AtomicReference<TaskStatus> ref =
          reportedTerminalStatus.computeIfAbsent(taskId, k -> new AtomicReference<>());
      TaskStatus prev = ref.get();
      if (prev == TaskStatus.SUCCESS
          || prev == TaskStatus.FAILED
          || prev == TaskStatus.TIMEOUT
          || prev == TaskStatus.CANCELLED) {
        return;
      }
      ref.set(result.getStatus());
    }

    reportResult(taskId, result);
  }

  /**
   * 注册任务执行器
   *
   * <p>遵循开闭原则：通过注册新的执行器扩展支持的任务类型
   *
   * @param executor 任务执行器
   * @return this（支持链式调用）
   */
  public TaskDispatcher registerExecutor(TaskExecutor executor) {
    String taskType = executor.getTaskType();
    TaskExecutor existing = executors.put(taskType, executor);
    if (existing != null) {
      logger.log(
          Level.WARNING,
          "Replaced existing executor for task type: {0}",
          taskType);
    }
    logger.log(
        Level.INFO,
        "Registered executor: type={0}, description={1}",
        new Object[] {taskType, executor.getDescription()});
    return this;
  }

  /**
   * 注销任务执行器
   *
   * @param taskType 任务类型
   * @return 是否成功注销
   */
  public boolean unregisterExecutor(String taskType) {
    TaskExecutor removed = executors.remove(taskType);
    if (removed != null) {
      logger.log(Level.INFO, "Unregistered executor for task type: {0}", taskType);
      return true;
    }
    return false;
  }

  /**
   * 分发任务（返回简单的 boolean）
   *
   * <p>根据任务类型查找执行器，异步执行任务，并在完成后上报结果
   *
   * @param taskInfo 任务信息
   * @return 分发是否成功（不代表执行成功）
   * @deprecated 推荐使用 {@link #dispatchWithResult(TaskInfo)} 获取详细的分发结果
   */
  @Deprecated
  public boolean dispatch(TaskInfo taskInfo) {
    return dispatchWithResult(taskInfo).isSuccess();
  }

  /**
   * 分发任务（返回详细结果）
   *
   * <p>根据任务类型查找执行器，异步执行任务，并在完成后上报结果。
   * 返回详细的分发结果，包括状态和原因。
   *
   * @param taskInfo 任务信息
   * @return 分发结果
   */
  public DispatchResult dispatchWithResult(TaskInfo taskInfo) {
    if (closed) {
      logger.log(Level.WARNING, "TaskDispatcher is closed, rejecting task: {0}", taskInfo.getTaskId());
      return DispatchResult.dispatcherClosed();
    }

    String taskId = taskInfo.getTaskId();
    String taskType = taskInfo.getTaskType();

    // 幂等性检查
    if (!runningTasks.add(taskId)) {
      logger.log(
          Level.WARNING,
          "[TASK-DUPLICATE] Task already running, skipping: taskId={0}",
          taskId);
      return DispatchResult.alreadyRunning();
    }

    // 查找执行器
    TaskExecutor executor = executors.get(taskType);
    if (executor == null) {
      logger.log(
          Level.WARNING,
          "[TASK-NO-EXECUTOR] No executor found for task type: {0}, taskId={1}",
          new Object[] {taskType, taskId});
      runningTasks.remove(taskId);
      // 上报失败：无执行器
      reportResult(taskId, TaskExecutionResult.failed(
          "NO_EXECUTOR",
          "No executor registered for task type: " + taskType));
      return DispatchResult.noExecutor(taskType);
    }

    // 检查执行器是否可用
    if (!executor.isAvailable()) {
      logger.log(
          Level.WARNING,
          "[TASK-EXECUTOR-UNAVAILABLE] Executor not available: type={0}, taskId={1}",
          new Object[] {taskType, taskId});
      runningTasks.remove(taskId);
      // 上报失败：执行器不可用
      reportResult(taskId, TaskExecutionResult.failed(
          "EXECUTOR_UNAVAILABLE",
          "Executor for " + taskType + " is not available"));
      return DispatchResult.executorUnavailable(taskType);
    }

    // 构建执行上下文
    TaskExecutionContext context = buildContext(taskInfo);

    // 为该任务创建 emitter：执行器可在关键事件发生时实时上报状态
    TaskStatusEmitter emitter = statusEventManager.createEmitter(taskId, agentId);
    context = TaskExecutionContext.builder()
        .taskId(context.getTaskId())
        .taskType(context.getTaskType())
        .priority(context.getPriority())
        .timeoutMillis(context.getTimeoutMillis())
        .createdAtMillis(context.getCreatedAtMillis())
        .expiresAtMillis(context.getExpiresAtMillis())
        .agentId(context.getAgentId())
        .parameters(context.getParameters())
        .parametersJson(context.getParametersJson())
        .client(context.getClient())
        .scheduler(context.getScheduler())
        .receivedAtMillis(context.getReceivedAtMillis())
        .statusEmitter(emitter)
        .build();

    // 记录任务开始
    logger.log(
        Level.INFO,
        "[TASK-DISPATCH] Dispatching task: taskId={0}, type={1}, executor={2}",
        new Object[] {taskId, taskType, executor.getDescription()});
    taskLogger.logTaskStarted(taskId, Thread.currentThread().getName());

    // 异步执行任务
    long startTime = System.currentTimeMillis();

    // 事件驱动：任务一旦开始执行，立即上报 RUNNING（避免服务端误判超时/重复派发）
    emitter.running("Task started");
    
    CompletableFuture<TaskExecutionResult> executionFuture = executor.execute(context);
    
    // 添加超时控制
    long timeout = context.getEffectiveTimeoutMillis();
    if (timeout <= 0) {
      timeout = DEFAULT_TIMEOUT_MILLIS;
    }
    
    CompletableFuture<TaskExecutionResult> timeoutFuture = withTimeout(executionFuture, timeout, taskId);

    // 处理执行结果
    long finalTimeout = timeout;
    @SuppressWarnings("FutureReturnValueIgnored")
    Object unused1 = timeoutFuture.whenCompleteAsync((result, error) -> {
      long executionTime = System.currentTimeMillis() - startTime;
      runningTasks.remove(taskId);
      statusEventManager.closeEmitter(taskId);

      TaskExecutionResult finalResult;
      if (error != null) {
        if (error instanceof TimeoutException) {
          finalResult = TaskExecutionResult.timeout(
              String.format(Locale.ROOT, "Task execution timeout after %dms", finalTimeout),
              executionTime);
          logger.log(
              Level.WARNING,
              "[TASK-TIMEOUT] Task execution timeout: taskId={0}, timeout={1}ms",
              new Object[] {taskId, finalTimeout});
        } else if (error instanceof CancellationException) {
          // 取消通常来自本地超时控制或上层关闭
          finalResult = TaskExecutionResult.cancelled("Task future cancelled: " + error.getMessage());
          logger.log(
              Level.WARNING,
              "[TASK-CANCELLED] Task cancelled: taskId={0}, message={1}",
              new Object[] {taskId, error.getMessage()});
        } else {
          finalResult = TaskExecutionResult.fromException("EXECUTION_ERROR", error);
          logger.log(
              Level.WARNING,
              "[TASK-ERROR] Task execution failed: taskId={0}, error={1}",
              new Object[] {taskId, error.getMessage()});
        }
      } else {
        finalResult = result;
        if (result.isSuccess()) {
          if (result.getStatus() == TaskStatus.RUNNING) {
            logger.log(
                Level.INFO,
                "[TASK-RUNNING] Task reported running: taskId={0}, executionTime={1}ms",
                new Object[] {taskId, executionTime});
          } else {
            logger.log(
                Level.INFO,
                "[TASK-SUCCESS] Task completed successfully: taskId={0}, executionTime={1}ms",
                new Object[] {taskId, executionTime});
          }
        } else {
          logger.log(
              Level.WARNING,
              "[TASK-FAILED] Task completed with failure: taskId={0}, errorCode={1}, errorMessage={2}",
              new Object[] {taskId, result.getErrorCode(), result.getErrorMessage()});
        }
      }

      // 上报结果到服务端
      // 说明：RUNNING 不是终态，终态应由执行器通过 TaskStatusEmitter 上报。
      if (finalResult.getStatus() != TaskStatus.RUNNING) {
        reportResult(taskId, finalResult);
      }

      // 记录任务完成日志
      if (finalResult.isSuccess()) {
        if (finalResult.getStatus() != TaskStatus.RUNNING) {
          taskLogger.logTaskCompleted(taskId, finalResult.getResultJson());
        }
      } else {
        String errorCode = finalResult.getErrorCode();
        String errorMessage = finalResult.getErrorMessage();
        taskLogger.logTaskFailed(
            taskId,
            errorCode != null ? errorCode : "UNKNOWN_ERROR",
            errorMessage != null ? errorMessage : "Unknown error");
      }
    }, taskExecutor);

    return DispatchResult.success();
  }

  /**
   * 为 Future 添加超时控制
   */
  private CompletableFuture<TaskExecutionResult> withTimeout(
      CompletableFuture<TaskExecutionResult> future,
      long timeoutMillis,
      String taskId) {
    
    CompletableFuture<TaskExecutionResult> timeoutFuture = new CompletableFuture<>();
    
    // 调度超时任务
    @SuppressWarnings("FutureReturnValueIgnored")
    Object unused1 = scheduler.schedule(() -> {
      if (!timeoutFuture.isDone()) {
        // 不取消原 future：取消会导致 CancellationException，掩盖超时根因。
        timeoutFuture.completeExceptionally(
            new TimeoutException("Task " + taskId + " execution timeout"));
      }
    }, timeoutMillis, TimeUnit.MILLISECONDS);

    // 当原始 Future 完成时，完成超时 Future
    @SuppressWarnings("FutureReturnValueIgnored")
    Object unused2 = future.whenComplete((result, error) -> {
      if (!timeoutFuture.isDone()) {
        if (error != null) {
          timeoutFuture.completeExceptionally(error);
        } else {
          timeoutFuture.complete(result);
        }
      }
    });

    return timeoutFuture;
  }

  /**
   * 构建任务执行上下文
   */
  private TaskExecutionContext buildContext(TaskInfo taskInfo) {
    // 解析参数 JSON
    Map<String, Object> params = parseParameters(taskInfo.getParametersJson());

    return TaskExecutionContext.builder()
        .taskId(taskInfo.getTaskId())
        .taskType(taskInfo.getTaskType())
        .priority(taskInfo.getPriority())
        .timeoutMillis(taskInfo.getTimeoutMillis())
        .createdAtMillis(taskInfo.getCreatedAtMillis())
        .expiresAtMillis(taskInfo.getExpiresAtMillis())
        .agentId(agentId)
        .parameters(params)
        .parametersJson(taskInfo.getParametersJson())
        .client(client)
        .scheduler(scheduler)
        .receivedAtMillis(System.currentTimeMillis())
        .build();
  }

  /**
   * 解析参数 JSON
   */
  private static Map<String, Object> parseParameters(@Nullable String json) {
    Map<String, Object> params = new HashMap<>();
    if (json == null || json.isEmpty() || "{}".equals(json)) {
      return params;
    }

    // 简单的 JSON 解析（不引入外部依赖）
    // 格式：{"key1":"value1","key2":"value2"}
    try {
      String content = json.trim();
      if (content.startsWith("{") && content.endsWith("}")) {
        content = content.substring(1, content.length() - 1).trim();
        if (!content.isEmpty()) {
          // 简单分割（不处理嵌套对象）
          String[] pairs = content.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
          for (String pair : pairs) {
            int colonIndex = pair.indexOf(':');
            if (colonIndex > 0) {
              String key = pair.substring(0, colonIndex).trim();
              String value = pair.substring(colonIndex + 1).trim();
              // 去除引号
              key = removeQuotes(key);
              value = removeQuotes(value);
              params.put(key, value);
            }
          }
        }
      }
    } catch (RuntimeException e) {
      logger.log(Level.WARNING, "Failed to parse parameters JSON: {0}", e.getMessage());
    }

    return params;
  }

  private static String removeQuotes(String s) {
    if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
      return s.substring(1, s.length() - 1);
    }
    return s;
  }

  /**
   * 上报任务结果到服务端
   */
  private void reportResult(String taskId, TaskExecutionResult result) {
    TaskResultRequest request = new TaskResultRequest() {
      @Override
      public String getTaskId() {
        return taskId;
      }

      @Override
      public String getAgentId() {
        return agentId;
      }

      @Override
      public TaskStatus getStatus() {
        return result.getStatus();
      }

      @Override
      @Nullable
      public String getErrorCode() {
        return result.getErrorCode();
      }

      @Override
      @Nullable
      public String getErrorMessage() {
        return result.getErrorMessage();
      }

      @Override
      @Nullable
      public String getResultJson() {
        return result.getResultJson();
      }

      @Override
      public long getStartedAtMillis() {
        return result.getStartedAtMillis();
      }

      @Override
      public long getCompletedAtMillis() {
        return result.getCompletedAtMillis();
      }

      @Override
      public long getExecutionTimeMillis() {
        return result.getExecutionTimeMillis();
      }
    };

    CompletableFuture<TaskResultResponse> future = client.reportTaskResult(request);
    @SuppressWarnings("FutureReturnValueIgnored")
    Object unused = future.whenComplete((response, error) -> {
      if (error != null) {
        logger.log(
            Level.WARNING,
            "[TASK-REPORT-FAILED] Failed to report task result: taskId={0}, error={1}",
            new Object[] {taskId, error.getMessage()});
      } else if (response != null && !response.isSuccess()) {
        logger.log(
            Level.WARNING,
            "[TASK-REPORT-REJECTED] Server rejected task result: taskId={0}, error={1}",
            new Object[] {taskId, response.getErrorMessage()});
      } else {
        logger.log(
            Level.INFO,
            "[TASK-REPORT-SUCCESS] Task result reported: taskId={0}, status={1}",
            new Object[] {taskId, result.getStatus()});
      }
    });
  }

  /**
   * 获取已注册的执行器数量
   *
   * @return 执行器数量
   */
  public int getExecutorCount() {
    return executors.size();
  }

  /**
   * 获取已注册的任务类型列表
   *
   * @return 任务类型集合
   */
  public Set<String> getRegisteredTaskTypes() {
    return executors.keySet();
  }

  /**
   * 获取正在运行的任务数量
   *
   * @return 运行中的任务数量
   */
  public int getRunningTaskCount() {
    return runningTasks.size();
  }

  /**
   * 检查任务是否正在运行
   *
   * @param taskId 任务 ID
   * @return 是否正在运行
   */
  public boolean isTaskRunning(String taskId) {
    return runningTasks.contains(taskId);
  }

  /**
   * 检查是否存在指定类型的执行器
   *
   * @param taskType 任务类型
   * @return 是否存在
   */
  public boolean hasExecutor(String taskType) {
    return executors.containsKey(taskType);
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;

    logger.log(Level.INFO, "Closing TaskDispatcher...");

    // 关闭执行器线程池
    taskExecutor.shutdown();
    try {
      if (!taskExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        taskExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      taskExecutor.shutdownNow();
    }

    logger.log(Level.INFO, "TaskDispatcher closed");
  }
}
