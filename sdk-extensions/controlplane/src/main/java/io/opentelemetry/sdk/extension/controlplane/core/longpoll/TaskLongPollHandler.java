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
import io.opentelemetry.sdk.extension.controlplane.core.ControlPlaneStatistics;
import io.opentelemetry.sdk.extension.controlplane.task.TaskExecutionLogger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

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

  private final ControlPlaneClient client;
  private final ControlPlaneStatistics statistics;
  private final LongPollConfig config;
  private final String agentId;
  private final AtomicBoolean running;
  private final TaskExecutionLogger taskLogger;

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
      taskLogger.logTaskProgress(
          currentTaskId, "task_error", "Task response error: " + response.getErrorMessage());
      return HandlerResult.noChange();
    }

    if (response.getTasks() != null && !response.getTasks().isEmpty()) {
      int taskCount = response.getTasks().size();
      taskLogger.logTaskProgress(
          currentTaskId, "tasks_received", "Received " + taskCount + " tasks");

      // 处理每个任务
      for (TaskInfo task : response.getTasks()) {
        processTask(task);
      }

      return HandlerResult.changed("tasks=" + taskCount);
    } else {
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
      return false;
    }

    List<TaskInfo> tasks = result.getTasks();
    
    if (result.hasChanges() && tasks != null && !tasks.isEmpty()) {
      int taskCount = tasks.size();
      taskLogger.logTaskProgress(
          currentTaskId, "tasks_received", "Received " + taskCount + " tasks via unified poll");

      // 处理每个任务
      for (TaskInfo task : tasks) {
        processTask(task);
      }

      logger.log(Level.INFO, "Received {0} tasks via unified poll", taskCount);
      return true;
    } else {
      taskLogger.logTaskProgress(currentTaskId, "no_tasks", "No pending tasks");
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
    taskLogger.logTaskReceived(
        subTaskId,
        task.getTaskType(),
        "long_poll",
        TaskExecutionLogger.details()
            .put("priority", task.getPriority())
            .put("timeout", task.getTimeoutMillis())
            .build());

    logger.log(
        Level.INFO,
        "Received task via long poll: {0}, type: {1}",
        new Object[] {subTaskId, task.getTaskType()});
    
    // TODO: 将任务提交到任务执行器
  }

  @Override
  public void handleError(Throwable error) {
    logger.log(Level.WARNING, "Task poll failed: {0}", error.getMessage());
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
