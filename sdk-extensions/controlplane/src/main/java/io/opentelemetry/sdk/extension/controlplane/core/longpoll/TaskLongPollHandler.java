/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.PollResult;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskInfo;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskRequest;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskResponse;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskResultRequest;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskResultResponse;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskStatus;
import io.opentelemetry.sdk.extension.controlplane.core.ControlPlaneStatistics;
import io.opentelemetry.sdk.extension.controlplane.task.TaskExecutionLogger;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskDispatcher;
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

  private final ControlPlaneClient client;
  private final ControlPlaneStatistics statistics;
  private final LongPollConfig config;
  private final String agentId;
  private final AtomicBoolean running;
  private final TaskExecutionLogger taskLogger;

  /** 任务分发器（可选，用于执行任务） */
  @Nullable private volatile TaskDispatcher taskDispatcher;

  // 当前任务 ID（用于日志）
  private volatile String currentTaskId = "";

  /**
   * 创建任务长轮询处理器
   *
   * @param client 控制平面客户端
   * @param statistics 统计管理器
   * @param config 长轮询配置
   * @param agentId Agent ID
   * @param running 运行状态标志
   */
  public TaskLongPollHandler(
      ControlPlaneClient client,
      ControlPlaneStatistics statistics,
      LongPollConfig config,
      String agentId,
      AtomicBoolean running) {
    this.client = client;
    this.statistics = statistics;
    this.config = config;
    this.agentId = agentId;
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
    params.put("agentId", agentId);
    params.put("timeoutMillis", config.getTimeoutMillis());
    return params;
  }

  @Override
  public HandlerResult handleResponse(TaskResponse response) {
    if (!response.isSuccess()) {
      logger.log(
          Level.WARNING,
          "[TASK-ERROR] Task response error from server: {0}",
          response.getErrorMessage());
      taskLogger.logTaskProgress(
          currentTaskId, "task_error", "Task response error: " + response.getErrorMessage());
      return HandlerResult.noChange();
    }

    List<TaskInfo> taskList = response.getTasks();
    if (taskList != null && !taskList.isEmpty()) {
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
      for (TaskInfo task : taskList) {
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
   * @param result 轮询结果
   * @return 是否成功处理
   */
  public boolean processUnifiedResult(PollResult result) {
    if (result == null) {
      // 使用 INFO 级别，确保日志可见
      logger.log(Level.INFO, "[TASK-POLL] No task result in unified response (result is null)");
      return false;
    }

    List<TaskInfo> tasks = result.getTasks();
    
    // 增强诊断日志：无论是否有任务都输出详细信息
    logger.log(
        Level.INFO,
        "[TASK-POLL] Processing unified result: hasChanges={0}, tasksNull={1}, taskCount={2}",
        new Object[] {
          result.hasChanges(),
          tasks == null,
          tasks != null ? tasks.size() : 0
        });
    
    if (result.hasChanges() && tasks != null && !tasks.isEmpty()) {
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
      for (TaskInfo task : tasks) {
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
      // 增强诊断：输出更多细节
      logger.log(
          Level.INFO,
          "[TASK-POLL] No pending tasks via unified poll: hasChanges={0}, tasksNull={1}, taskCount={2}",
          new Object[] {
            result.hasChanges(),
            tasks == null,
            tasks != null ? tasks.size() : 0
          });
      taskLogger.logTaskProgress(
          currentTaskId,
          "no_tasks",
          String.format(
              "No pending tasks (hasChanges=%s, tasksNull=%s)",
              result.hasChanges(), tasks == null));
      return true;
    }
  }

  /**
   * 处理单个任务
   *
   * @param task 任务信息
   */
  private void processTask(TaskInfo task) {
    String subTaskId = task.getTaskId();
    String taskType = task.getTaskType();
    int priority = task.getPriority();
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
        // 上报服务端：使用 FAILED + error_code 模式
        reportTaskResultToServer(
            subTaskId, TaskStatus.FAILED, "TASK_EXPIRED", expiredErrorMsg, nowMillis);
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
        // 上报服务端：使用 FAILED + error_code 模式
        reportTaskResultToServer(
            subTaskId, TaskStatus.FAILED, "TASK_STALE", staleErrorMsg, nowMillis);
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
            .put("params", paramsJson != null ? paramsJson : "{}")
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
   * @param task 任务信息
   */
  private void dispatchTask(TaskInfo task) {
    TaskDispatcher dispatcher = this.taskDispatcher;
    if (dispatcher == null) {
      logger.log(
          Level.WARNING,
          "[TASK-NO-DISPATCHER] TaskDispatcher not configured, task will not be executed: taskId={0}, type={1}",
          new Object[] {task.getTaskId(), task.getTaskType()});
      // 上报失败：无分发器
      long nowMillis = System.currentTimeMillis();
      reportTaskResultToServer(
          task.getTaskId(),
          TaskStatus.FAILED,
          "NO_DISPATCHER",
          "TaskDispatcher not configured",
          nowMillis);
      return;
    }

    // 检查是否有对应的执行器
    if (!dispatcher.hasExecutor(task.getTaskType())) {
      logger.log(
          Level.WARNING,
          "[TASK-NO-EXECUTOR] No executor registered for task type: {0}, taskId={1}",
          new Object[] {task.getTaskType(), task.getTaskId()});
      // TaskDispatcher.dispatch 内部会处理无执行器的情况并上报
    }

    // 分发任务
    boolean dispatched = dispatcher.dispatch(task);
    if (dispatched) {
      logger.log(
          Level.INFO,
          "[TASK-DISPATCHED] Task dispatched to executor: taskId={0}, type={1}",
          new Object[] {task.getTaskId(), task.getTaskType()});
    } else {
      logger.log(
          Level.WARNING,
          "[TASK-DISPATCH-FAILED] Failed to dispatch task: taskId={0}, type={1}",
          new Object[] {task.getTaskId(), task.getTaskType()});
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
   * 上报任务结果到服务端
   *
   * @param taskId 任务 ID
   * @param status 任务状态
   * @param errorCode 错误码
   * @param errorMessage 错误信息
   * @param completedAtMillis 完成时间
   */
  private void reportTaskResultToServer(
      String taskId,
      TaskStatus status,
      @Nullable String errorCode,
      @Nullable String errorMessage,
      long completedAtMillis) {
    
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
        return status;
      }

      @Override
      @Nullable
      public String getErrorCode() {
        return errorCode;
      }

      @Override
      @Nullable
      public String getErrorMessage() {
        return errorMessage;
      }

      @Override
      @Nullable
      public String getResultJson() {
        return null;
      }

      @Override
      public long getStartedAtMillis() {
        return completedAtMillis; // 对于被拒绝的任务，开始和完成时间相同
      }

      @Override
      public long getCompletedAtMillis() {
        return completedAtMillis;
      }

      @Override
      public long getExecutionTimeMillis() {
        return 0; // 被拒绝的任务没有执行时间
      }
    };

    // 异步上报，不阻塞主流程
    CompletableFuture<TaskResultResponse> future = client.reportTaskResult(request);
    @SuppressWarnings("FutureReturnValueIgnored")
    Object unused = future.whenComplete((response, error) -> {
      if (error != null) {
        logger.log(
            Level.WARNING,
            "[TASK-REPORT] Failed to report task result: taskId={0}, error={1}",
            new Object[] {taskId, error.getMessage()});
      } else if (response != null && !response.isSuccess()) {
        logger.log(
            Level.WARNING,
            "[TASK-REPORT] Server rejected task result: taskId={0}, error={1}",
            new Object[] {taskId, response.getErrorMessage()});
      } else {
        logger.log(
            Level.INFO,
            "[TASK-REPORT] Successfully reported task result: taskId={0}, status={1}",
            new Object[] {taskId, status});
      }
    });
  }

  /**
   * 验证任务时效性
   *
   * @param task 任务信息
   * @return 验证结果
   */
  private static TaskValidationResult validateTask(TaskInfo task) {
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
   * @return 任务响应 Future
   */
  @Override
  public CompletableFuture<TaskResponse> poll() {
    statistics.recordTaskPoll();
    return client.getTasks(createTaskRequest());
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

  /** 创建任务请求 */
  private TaskRequest createTaskRequest() {
    return new TaskRequest() {
      @Override
      public String getAgentId() {
        return agentId;
      }

      @Override
      public long getLongPollTimeoutMillis() {
        return config.getTimeoutMillis();
      }
    };
  }
}
