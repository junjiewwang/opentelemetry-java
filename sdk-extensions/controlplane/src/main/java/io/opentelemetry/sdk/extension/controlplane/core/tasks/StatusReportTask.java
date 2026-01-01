/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.tasks;

import io.opentelemetry.sdk.extension.controlplane.core.ControlPlaneStatistics;
import io.opentelemetry.sdk.extension.controlplane.task.TaskExecutionLogger;
import io.opentelemetry.sdk.extension.controlplane.task.TaskResultPersistence;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 状态上报任务。
 *
 * <p>负责定期上报 Agent 状态，以及重传失败的任务结果。
 */
public final class StatusReportTask implements Runnable {

  private static final Logger logger = Logger.getLogger(StatusReportTask.class.getName());

  private final TaskResultPersistence resultPersistence;
  private final ControlPlaneStatistics statistics;
  private final TaskExecutionLogger taskLogger = TaskExecutionLogger.getInstance();

  /**
   * 创建状态上报任务
   *
   * @param resultPersistence 任务结果持久化
   * @param statistics 统计管理器
   */
  public StatusReportTask(
      TaskResultPersistence resultPersistence, ControlPlaneStatistics statistics) {
    this.resultPersistence = resultPersistence;
    this.statistics = statistics;
  }

  @Override
  public void run() {
    long count = statistics.recordStatusReport();
    String taskId = "status-report-" + count;

    // 记录任务接收
    taskLogger.logTaskReceived(
        taskId,
        TaskExecutionLogger.TASK_TYPE_STATUS_REPORT,
        TaskExecutionLogger.SOURCE_SCHEDULER,
        TaskExecutionLogger.details()
            .put("reportCount", count)
            .build());

    // 记录任务开始执行
    taskLogger.logTaskStarted(taskId, Thread.currentThread().getName());

    try {
      // 状态上报不依赖连接状态，即使 SERVER_UNAVAILABLE 也尝试上报
      logger.log(Level.FINE, "Reporting status (count: {0})...", count);
      taskLogger.logTaskProgress(taskId, "reporting", "Reporting agent status");
      
      // TODO: 实现状态上报逻辑
      // client.reportStatus(agentIdentity, connectionState.get(), healthMonitor.getState());

      // 重传失败的任务结果
      taskLogger.logTaskProgress(taskId, "retrying", "Retrying failed task results");
      int retryCount = retryFailedResults();
      
      // 记录任务成功完成
      taskLogger.logTaskCompleted(
          taskId,
          "success:report_count=" + count + ",retry_count=" + retryCount);
    } catch (RuntimeException e) {
      logger.log(
          Level.WARNING,
          "Failed to report status (count: {0}): {1}",
          new Object[] {count, e.getMessage()});
      
      // 记录任务失败
      taskLogger.logTaskFailed(
          taskId,
          TaskExecutionLogger.ERROR_NETWORK,
          e.getMessage(),
          e);
    }
  }

  /**
   * 重传失败的任务结果
   *
   * @return 重传的任务数量
   */
  private int retryFailedResults() {
    List<String> pendingTaskIds = resultPersistence.getPendingRetryTaskIds();
    int retryCount = 0;

    for (String pendingTaskId : pendingTaskIds) {
      String retryTaskId = "retry-" + pendingTaskId + "-" + System.currentTimeMillis();
      
      try {
        taskLogger.logTaskReceived(
            retryTaskId,
            TaskExecutionLogger.TASK_TYPE_STATUS_REPORT,
            TaskExecutionLogger.SOURCE_SCHEDULER,
            TaskExecutionLogger.details()
                .put("originalTaskId", pendingTaskId)
                .put("retryCount", resultPersistence.getRetryCount(pendingTaskId))
                .build());
        
        taskLogger.logTaskStarted(retryTaskId, "retry_handler");

        resultPersistence
            .read(pendingTaskId)
            .ifPresent(
                data -> {
                  // TODO: 实现结果重传逻辑
                  logger.log(Level.FINE, "Retrying task result: {0}", pendingTaskId);
                  taskLogger.logTaskProgress(retryTaskId, "uploading", "Uploading task result");
                });
        
        taskLogger.logTaskCompleted(retryTaskId, "retry_success:" + pendingTaskId);
        retryCount++;
      } catch (RuntimeException e) {
        logger.log(Level.WARNING, "Failed to retry task result: " + pendingTaskId, e);
        resultPersistence.markForRetry(pendingTaskId);
        
        taskLogger.logTaskFailed(
            retryTaskId,
            TaskExecutionLogger.ERROR_NETWORK,
            "Retry failed: " + e.getMessage(),
            e);
      }
    }

    return retryCount;
  }
}
