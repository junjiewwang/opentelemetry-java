/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.tasks;

import io.opentelemetry.sdk.extension.controlplane.task.TaskExecutionLogger;
import io.opentelemetry.sdk.extension.controlplane.task.TaskResultPersistence;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 清理任务。
 *
 * <p>负责定期清理过期的任务结果。
 */
public final class CleanupTask implements Runnable {

  private static final Logger logger = Logger.getLogger(CleanupTask.class.getName());

  private final TaskResultPersistence resultPersistence;
  private final TaskExecutionLogger taskLogger = TaskExecutionLogger.getInstance();
  private final AtomicLong cleanupCount = new AtomicLong(0);

  /**
   * 创建清理任务
   *
   * @param resultPersistence 任务结果持久化
   */
  public CleanupTask(TaskResultPersistence resultPersistence) {
    this.resultPersistence = resultPersistence;
  }

  @Override
  public void run() {
    long count = cleanupCount.incrementAndGet();
    String taskId = "cleanup-" + count;

    // 记录任务接收
    taskLogger.logTaskReceived(
        taskId,
        TaskExecutionLogger.TASK_TYPE_CLEANUP,
        TaskExecutionLogger.SOURCE_SCHEDULER,
        TaskExecutionLogger.details()
            .put("cleanupCount", count)
            .put("currentFileCount", resultPersistence.getCurrentFileCount())
            .put("currentSizeBytes", resultPersistence.getCurrentSizeBytes())
            .build());

    // 记录任务开始执行
    taskLogger.logTaskStarted(taskId, Thread.currentThread().getName());

    try {
      logger.log(Level.FINE, "Running cleanup task...");
      taskLogger.logTaskProgress(taskId, "cleaning", "Cleaning expired task results");

      int beforeCount = resultPersistence.getCurrentFileCount();
      long beforeSize = resultPersistence.getCurrentSizeBytes();

      resultPersistence.cleanupExpired();

      int afterCount = resultPersistence.getCurrentFileCount();
      long afterSize = resultPersistence.getCurrentSizeBytes();

      int cleanedCount = beforeCount - afterCount;
      long cleanedSize = beforeSize - afterSize;

      logger.log(Level.FINE, "Cleanup task completed");

      // 记录任务成功完成
      taskLogger.logTaskCompleted(
          taskId,
          "success:cleaned_files=" + cleanedCount + ",cleaned_bytes=" + cleanedSize);
    } catch (RuntimeException e) {
      logger.log(Level.WARNING, "Cleanup task failed: {0}", e.getMessage());

      // 记录任务失败
      taskLogger.logTaskFailed(
          taskId,
          TaskExecutionLogger.ERROR_INTERNAL,
          e.getMessage(),
          e);
    }
  }
}
