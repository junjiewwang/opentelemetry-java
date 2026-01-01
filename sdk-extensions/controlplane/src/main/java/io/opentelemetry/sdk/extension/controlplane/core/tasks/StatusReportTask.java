/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.tasks;

import io.opentelemetry.sdk.extension.controlplane.core.ControlPlaneStatistics;
import io.opentelemetry.sdk.extension.controlplane.task.TaskResultPersistence;
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

    try {
      // 状态上报不依赖连接状态，即使 SERVER_UNAVAILABLE 也尝试上报
      logger.log(Level.FINE, "Reporting status (count: {0})...", count);
      // TODO: 实现状态上报逻辑
      // client.reportStatus(agentIdentity, connectionState.get(), healthMonitor.getState());

      // 重传失败的任务结果
      retryFailedResults();
    } catch (RuntimeException e) {
      logger.log(
          Level.WARNING,
          "Failed to report status (count: {0}): {1}",
          new Object[] {count, e.getMessage()});
    }
  }

  /** 重传失败的任务结果 */
  private void retryFailedResults() {
    for (String taskId : resultPersistence.getPendingRetryTaskIds()) {
      try {
        resultPersistence
            .read(taskId)
            .ifPresent(
                data -> {
                  // TODO: 实现结果重传逻辑
                  logger.log(Level.FINE, "Retrying task result: {0}", taskId);
                });
      } catch (RuntimeException e) {
        logger.log(Level.WARNING, "Failed to retry task result: " + taskId, e);
        resultPersistence.markForRetry(taskId);
      }
    }
  }
}
