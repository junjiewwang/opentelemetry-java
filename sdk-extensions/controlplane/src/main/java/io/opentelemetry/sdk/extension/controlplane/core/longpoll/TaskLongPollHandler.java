/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneService;
import io.opentelemetry.sdk.extension.controlplane.core.ControlPlaneStatistics;
import io.opentelemetry.sdk.extension.controlplane.identity.AgentIdentityProvider;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.CommonProtos.ResponseStatus;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.Task;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.AgentCapabilities;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.TaskRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.TaskResponse;
import io.opentelemetry.sdk.extension.controlplane.task.TaskExecutionLogger;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskDispatcher;
import io.opentelemetry.sdk.extension.controlplane.task.status.TaskStatusReporter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 任务长轮询处理器
 *
 * <p>负责从控制平面获取待执行的任务。
 *
 * <p>支持两种模式：
 * <ul>
 *   <li>独立模式：通过 poll() 方法直接调用 /v1/control/poll/tasks
 *   <li>统一模式：通过 processUnifiedResult() 处理 /v1/control/poll 响应中的 TASK 部分
 * </ul>
 *
 * <p><b>Phase 5 重构</b>：直接使用 {@link ControlPlaneService}（Protobuf-only），
 * 使用 Protobuf {@link Task} 代替旧的 TaskInfo DTO。
 */
public final class TaskLongPollHandler implements LongPollHandler<TaskResponse> {

  private static final Logger logger = Logger.getLogger(TaskLongPollHandler.class.getName());

  /** 默认最大允许的任务延迟：5分钟 */
  private static final long DEFAULT_MAX_ACCEPTABLE_DELAY_MILLIS = 5 * 60 * 1000;

  /** 任务验证结果 */
  public enum TaskValidationResult {
    /** 任务有效，可以执行 */
    VALID,
    /** 任务已过期（expiresAtMillis 已超过） */
    EXPIRED,
    /** 任务太旧（延迟超过 maxAcceptableDelayMillis，但未过期） */
    STALE,
    /** 任务有效但有较大延迟（警告） */
    VALID_WITH_WARNING
  }

  private final ControlPlaneService service;
  private final ControlPlaneStatistics statistics;
  private final LongPollConfig config;
  private final AtomicBoolean running;
  private final TaskExecutionLogger taskLogger;

  /** 任务分发器（可选，用于执行任务） */
  @Nullable private volatile TaskDispatcher taskDispatcher;

  // 当前任务 ID（用于日志）
  private volatile String currentTaskId = "";

  /**
   * 创建任务长轮询处理器
   *
   * @param service 控制平面服务（Protobuf-only）
   * @param statistics 统计管理器
   * @param config 长轮询配置
   * @param running 运行状态标志
   */
  public TaskLongPollHandler(
      ControlPlaneService service,
      ControlPlaneStatistics statistics,
      LongPollConfig config,
      AtomicBoolean running) {
    this.service = service;
    this.statistics = statistics;
    this.config = config;
    this.running = running;
    this.taskLogger = TaskExecutionLogger.getInstance();
  }

  @Override
  public LongPollType getType() {
    return LongPollType.TASK;
  }

  @Override
  public Map<String, Object> buildRequestParams() {
    Map<String, Object> params = new HashMap<>();
    params.put("agentId", AgentIdentityProvider.getAgentId());
    params.put("timeoutMillis", config.getTimeoutMillis());
    return params;
  }

  @Override
  public HandlerResult handleResponse(TaskResponse response) {
    // Phase 5: 直接使用 Protobuf 字段判断成功
    boolean success = response.getStatus().getCode() == ResponseStatus.Code.CODE_OK
        || response.getStatus().getCode() == ResponseStatus.Code.CODE_UNSPECIFIED;
    
    if (!success) {
      logger.log(
          Level.WARNING,
          "[TASK-ERROR] Task response error from server: {0}",
          response.getStatus().getMessage());
      taskLogger.logTaskProgress(
          currentTaskId, "task_error", "Task response error: " + response.getStatus().getMessage());
      return HandlerResult.noChange();
    }

    List<Task> taskList = response.getTasksList();
    // Protobuf getTasksList() 永远返回非 null 列表，直接检查是否为空
    if (!taskList.isEmpty()) {
      int taskCount = taskList.size();
      
      // 记录收到的任务列表
      logger.log(
          Level.INFO,
          "[TASK-RECEIVED] Received {0} task(s) from server (independent poll)",
          taskCount);
      
      taskLogger.logTaskProgress(
          currentTaskId, "tasks_received", "Received " + taskCount + " tasks");

      // 处理每个任务
      int processedCount = 0;
      for (Task task : taskList) {
        processTask(task);
        processedCount++;
      }

      // 确认接收完成
      logger.log(
          Level.INFO,
          "[TASK-ACK] Successfully acknowledged {0}/{1} task(s)",
          new Object[] {processedCount, taskCount});

      return HandlerResult.changed("tasks=" + taskCount);
    } else {
      // 无任务时也输出 INFO 级别日志，便于确认轮询正常工作
      logger.log(
          Level.INFO,
          "[TASK-POLL] No pending tasks from server (independent poll)");
      taskLogger.logTaskProgress(currentTaskId, "no_tasks", "No pending tasks");
      return HandlerResult.noChange();
    }
  }

  /**
   * 处理统一轮询响应中的任务结果
   *
   * <p>这是推荐的方式，用于处理 /v1/control/poll 统一端点返回的 TASK 部分
   *
   * <p><b>Phase 5</b>：直接使用 Protobuf TaskResponse。
   * <p><b>协议对齐</b>：复用 TaskResponse，避免字段漂移。
   *
   * @param response 任务响应（Protobuf TaskResponse）
   * @return 是否成功处理
   */
  public boolean processUnifiedResult(TaskResponse response) {
    if (response == null) {
      // 使用 INFO 级别，确保日志可见
      logger.log(Level.INFO, "[TASK-POLL] No task response in unified response (response is null)");
      return false;
    }

    // 检查响应状态
    boolean success = response.getStatus().getCode() == ResponseStatus.Code.CODE_OK
        || response.getStatus().getCode() == ResponseStatus.Code.CODE_UNSPECIFIED;
    
    if (!success) {
      logger.log(
          Level.WARNING,
          "[TASK-ERROR] Task response error in unified poll: {0}",
          response.getStatus().getMessage());
      taskLogger.logTaskProgress(
          currentTaskId, "task_error", "Task response error: " + response.getStatus().getMessage());
      return false;
    }

    // Phase 5: 直接使用 Protobuf 任务列表（永远不为 null）
    List<Task> tasks = response.getTasksList();
    
    // 增强诊断日志：无论是否有任务都输出详细信息
    logger.log(
        Level.INFO,
        "[TASK-POLL] Processing unified result: taskCount={0}",
        tasks.size());
    
    if (!tasks.isEmpty()) {
      int taskCount = tasks.size();
      
      // 记录任务列表摘要
      logger.log(
          Level.INFO,
          "[TASK-RECEIVED] Received {0} task(s) from server via unified poll",
          taskCount);
      
      taskLogger.logTaskProgress(
          currentTaskId, "tasks_received", "Received " + taskCount + " tasks via unified poll");

      // 处理每个任务并记录详情
      int processedCount = 0;
      for (Task task : tasks) {
        processTask(task);
        processedCount++;
      }

      // 确认接收完成
      logger.log(
          Level.INFO,
          "[TASK-ACK] Successfully acknowledged {0}/{1} task(s)",
          new Object[] {processedCount, taskCount});
      
      return true;
    } else {
      // 无任务时也输出 INFO 级别日志，便于确认轮询正常工作
      logger.log(
          Level.INFO,
          "[TASK-POLL] No pending tasks via unified poll: taskCount={0}",
          tasks.size());
      taskLogger.logTaskProgress(
          currentTaskId,
          "no_tasks",
          String.format(
              Locale.ROOT,
              "No pending tasks (taskCount=%d)",
              tasks.size()));
      return true;
    }
  }

  /**
   * 处理单个任务
   *
   * <p><b>Phase 5</b>：直接使用 Protobuf Task。
   *
   * @param task 任务信息（Protobuf）
   */
  private void processTask(Task task) {
    String subTaskId = task.getTaskId();
    // Phase 5: 优先使用字符串类型的 taskTypeName，兼容枚举类型
    String taskType = task.getTaskTypeName().isEmpty() 
        ? task.getType().name() : task.getTaskTypeName();
    // Phase 5: 优先使用数值类型的 priorityNum，兼容枚举类型
    int priority = task.getPriorityNum() > 0 
        ? task.getPriorityNum() : task.getPriority().getNumber();
    long timeoutMillis = task.getTimeoutMillis();
    String paramsJson = task.getParametersJson();
    long createdAtMillis = task.getCreatedAtMillis();
    long expiresAtMillis = task.getExpiresAtMillis();
    long maxAcceptableDelayMillis = task.getMaxAcceptableDelayMillis();
    
    // 验证任务时效性
    TaskValidationResult validationResult = validateTask(task);
    long nowMillis = System.currentTimeMillis();
    long delayMillis = createdAtMillis > 0 ? nowMillis - createdAtMillis : 0;
    
    // 详细记录服务端下发的任务信息
    logger.log(
        Level.INFO,
        "[TASK-DETAIL] Server dispatched task: taskId={0}, type={1}, priority={2}, timeout={3}ms, "
            + "createdAt={4}, expiresAt={5}, maxDelay={6}ms, actualDelay={7}ms, params={8}",
        new Object[] {
          subTaskId, taskType, priority, timeoutMillis,
          createdAtMillis, expiresAtMillis, maxAcceptableDelayMillis, delayMillis, paramsJson
        });
    
    // 根据验证结果处理
    switch (validationResult) {
      case EXPIRED:
        logger.log(
            Level.WARNING,
            "[TASK-REJECTED] Task rejected (EXPIRED): taskId={0}, type={1}, "
                + "expiresAt={2}, now={3}",
            new Object[] {subTaskId, taskType, expiresAtMillis, nowMillis});
        String expiredErrorMsg = String.format(
            Locale.ROOT,
            "Task expired: expiresAt=%d, now=%d",
            expiresAtMillis, nowMillis);
        taskLogger.logTaskFailed(subTaskId, "TASK_EXPIRED", expiredErrorMsg);
        // 上报服务端：使用统一的状态上报器
        reportTaskFailed(subTaskId, "TASK_EXPIRED", expiredErrorMsg);
        return;
        
      case STALE:
        long effectiveMaxDelay = maxAcceptableDelayMillis > 0 
            ? maxAcceptableDelayMillis : DEFAULT_MAX_ACCEPTABLE_DELAY_MILLIS;
        logger.log(
            Level.WARNING,
            "[TASK-REJECTED] Task rejected (STALE): taskId={0}, type={1}, "
                + "delay={2}ms > maxDelay={3}ms",
            new Object[] {subTaskId, taskType, delayMillis, effectiveMaxDelay});
        String staleErrorMsg = String.format(
            Locale.ROOT,
            "Task too old: delay=%dms > maxAcceptableDelay=%dms",
            delayMillis, effectiveMaxDelay);
        taskLogger.logTaskFailed(subTaskId, "TASK_STALE", staleErrorMsg);
        // 上报服务端：使用统一的状态上报器
        reportTaskFailed(subTaskId, "TASK_STALE", staleErrorMsg);
        return;
        
      case VALID_WITH_WARNING:
        logger.log(
            Level.WARNING,
            "[TASK-WARNING] Task has high delay but will be executed: taskId={0}, type={1}, "
                + "delay={2}ms",
            new Object[] {subTaskId, taskType, delayMillis});
        // 继续执行
        break;
        
      case VALID:
        // 正常执行
        break;
    }
    
    taskLogger.logTaskReceived(
        subTaskId,
        taskType,
        "long_poll",
        TaskExecutionLogger.details()
            .put("priority", priority)
            .put("timeout", timeoutMillis)
            .put("createdAt", createdAtMillis)
            .put("expiresAt", expiresAtMillis)
            .put("maxAcceptableDelay", maxAcceptableDelayMillis)
            .put("actualDelay", delayMillis)
            .put("params", paramsJson.isEmpty() ? "{}" : paramsJson)
            .build());

    // 确认单个任务已接收
    logger.log(
        Level.INFO,
        "[TASK-CONFIRMED] Task acknowledged: taskId={0}, type={1}, delay={2}ms",
        new Object[] {subTaskId, taskType, delayMillis});
    
    // 将任务提交到任务执行器
    dispatchTask(task);
  }

  /**
   * 分发任务到执行器
   *
   * <p><b>Phase 5</b>：直接使用 Protobuf Task。
   *
   * @param task 任务信息（Protobuf）
   */
  private void dispatchTask(Task task) {
    TaskDispatcher dispatcher = this.taskDispatcher;
    if (dispatcher == null) {
      String taskType = task.getTaskTypeName().isEmpty() 
          ? task.getType().name() : task.getTaskTypeName();
      logger.log(
          Level.WARNING,
          "[TASK-NO-DISPATCHER] TaskDispatcher not configured, task will not be executed: taskId={0}, type={1}",
          new Object[] {task.getTaskId(), taskType});
      // 上报失败：无分发器（使用统一的状态上报器）
      reportTaskFailed(task.getTaskId(), "NO_DISPATCHER", "TaskDispatcher not configured");
      return;
    }

    String taskType = task.getTaskTypeName().isEmpty() 
        ? task.getType().name() : task.getTaskTypeName();

    // 检查是否有对应的执行器
    if (!dispatcher.hasExecutor(taskType)) {
      logger.log(
          Level.WARNING,
          "[TASK-NO-EXECUTOR] No executor registered for task type: {0}, taskId={1}",
          new Object[] {taskType, task.getTaskId()});
      // TaskDispatcher.dispatchWithResult 内部会处理无执行器的情况并上报
    }

    // 分发任务（使用新的返回详细结果的方法）
    // Phase 5: 直接传递 Protobuf Task
    TaskDispatcher.DispatchResult result = dispatcher.dispatchWithResult(task);
    
    if (result.isSuccess()) {
      logger.log(
          Level.INFO,
          "[TASK-DISPATCHED] Task dispatched to executor: taskId={0}, type={1}",
          new Object[] {task.getTaskId(), taskType});
    } else if (result.isAlreadyRunning()) {
      // 任务已经在运行中，向服务端上报 RUNNING 状态
      // 这样服务端就知道任务正在执行，不会重复下发
      logger.log(
          Level.INFO,
          "[TASK-ALREADY-RUNNING] Task already running, reporting RUNNING status to server: taskId={0}, type={1}",
          new Object[] {task.getTaskId(), taskType});
      // 使用统一的状态上报器
      reportTaskRunning(task.getTaskId(), "Task is already running");
    } else {
      // 其他失败情况（NO_EXECUTOR, EXECUTOR_UNAVAILABLE, DISPATCHER_CLOSED）
      // TaskDispatcher 内部已经上报了失败状态
      logger.log(
          Level.WARNING,
          "[TASK-DISPATCH-FAILED] Failed to dispatch task: taskId={0}, type={1}, reason={2}",
          new Object[] {task.getTaskId(), taskType, result.getMessage()});
    }
  }

  /**
   * 设置任务分发器
   *
   * <p>配置任务分发器后，接收到的任务将被分发到对应的执行器执行
   *
   * @param taskDispatcher 任务分发器
   */
  public void setTaskDispatcher(@Nullable TaskDispatcher taskDispatcher) {
    this.taskDispatcher = taskDispatcher;
    if (taskDispatcher != null) {
      logger.log(
          Level.INFO,
          "TaskDispatcher configured, executors registered: {0}",
          taskDispatcher.getExecutorCount());
    } else {
      logger.log(Level.INFO, "TaskDispatcher cleared");
    }
  }

  /**
   * 获取任务分发器
   *
   * @return 任务分发器，可能为 null
   */
  @Nullable
  public TaskDispatcher getTaskDispatcher() {
    return taskDispatcher;
  }

  /**
   * 上报任务失败状态
   *
   * <p>将上报逻辑委托给统一的 {@link TaskStatusReporter}，消除重复代码。
   *
   * @param taskId 任务 ID
   * @param errorCode 错误码
   * @param errorMessage 错误信息
   */
  private void reportTaskFailed(String taskId, String errorCode, String errorMessage) {
    TaskDispatcher dispatcher = this.taskDispatcher;
    if (dispatcher != null) {
      // 使用 TaskDispatcher 中的 TaskStatusReporter（Fire-and-Forget 模式）
      dispatcher.getStatusReporter().fireFailed(taskId, errorCode, errorMessage);
    } else {
      // 无分发器时，创建临时上报器
      TaskStatusReporter reporter = new TaskStatusReporter(service, AgentIdentityProvider.getAgentId());
      reporter.fireFailed(taskId, errorCode, errorMessage);
    }
  }

  /**
   * 上报任务运行中状态
   *
   * @param taskId 任务 ID
   * @param message 状态信息
   */
  private void reportTaskRunning(String taskId, String message) {
    TaskDispatcher dispatcher = this.taskDispatcher;
    if (dispatcher != null) {
      // Fire-and-Forget 模式，非终态无需关心结果
      dispatcher.getStatusReporter().fireRunning(taskId, message);
    } else {
      TaskStatusReporter reporter = new TaskStatusReporter(service, AgentIdentityProvider.getAgentId());
      reporter.fireRunning(taskId, message);
    }
  }

  /**
   * 验证任务时效性
   *
   * <p><b>Phase 5</b>：直接使用 Protobuf Task。
   *
   * @param task 任务信息（Protobuf）
   * @return 验证结果
   */
  private static TaskValidationResult validateTask(Task task) {
    long nowMillis = System.currentTimeMillis();
    long createdAtMillis = task.getCreatedAtMillis();
    long expiresAtMillis = task.getExpiresAtMillis();
    long maxAcceptableDelayMillis = task.getMaxAcceptableDelayMillis();
    
    // 1. 检查是否已过期（严格检查）
    if (expiresAtMillis > 0 && nowMillis > expiresAtMillis) {
      return TaskValidationResult.EXPIRED;
    }
    
    // 2. 检查任务延迟是否超过最大允许值
    if (createdAtMillis > 0) {
      long delayMillis = nowMillis - createdAtMillis;
      long effectiveMaxDelay = maxAcceptableDelayMillis > 0 
          ? maxAcceptableDelayMillis : DEFAULT_MAX_ACCEPTABLE_DELAY_MILLIS;
      
      if (delayMillis > effectiveMaxDelay) {
        // 如果没有设置 expiresAtMillis，延迟超过 maxAcceptableDelay 则拒绝
        if (expiresAtMillis == 0) {
          return TaskValidationResult.STALE;
        }
        // 如果设置了 expiresAtMillis 且未过期，则警告但仍然执行
        return TaskValidationResult.VALID_WITH_WARNING;
      }
      
      // 如果延迟超过 50% 的最大允许延迟，输出警告
      if (delayMillis > effectiveMaxDelay * 0.5) {
        return TaskValidationResult.VALID_WITH_WARNING;
      }
    }
    
    return TaskValidationResult.VALID;
  }

  @Override
  public void handleError(Throwable error) {
    logger.log(
        Level.WARNING,
        "[TASK-ERROR] Task poll failed: {0}",
        error.getMessage());
    taskLogger.logTaskProgress(
        currentTaskId, "task_error", "Task poll error: " + error.getMessage());
  }

  @Override
  public boolean shouldContinue() {
    return running.get();
  }

  /**
   * 发起任务轮询请求（独立模式）
   *
   * <p>直接调用 /v1/control/poll/tasks 端点
   *
   * <p><b>Phase 5</b>：返回 Protobuf TaskResponse。
   *
   * @return 任务响应 Future（Protobuf）
   */
  @Override
  public CompletableFuture<TaskResponse> poll() {
    statistics.recordTaskPoll();
    return service.getTasks(createTaskRequest());
  }

  /**
   * 设置当前任务 ID（用于日志追踪）
   *
   * @param taskId 任务 ID
   */
  @Override
  public void setCurrentTaskId(String taskId) {
    this.currentTaskId = taskId;
  }

  /**
   * 创建任务请求
   *
   * <p><b>Phase 5</b>：直接使用 Protobuf Builder。
   */
  private TaskRequest createTaskRequest() {
    return TaskRequest.newBuilder()
        .setAgentId(AgentIdentityProvider.getAgentId())
        .setLongPollTimeoutMillis(config.getTimeoutMillis())
        .setCapabilities(AgentCapabilities.newBuilder().build())
        .build();
  }
}
