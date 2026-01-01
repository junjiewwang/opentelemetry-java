/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.tasks;

import io.opentelemetry.sdk.extension.controlplane.core.ConnectionStateManager;
import io.opentelemetry.sdk.extension.controlplane.core.ControlPlaneStatistics;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 任务轮询任务。
 *
 * <p>负责定期从控制平面服务器拉取待执行的任务。
 */
public final class TaskPollTask implements Runnable {

  private static final Logger logger = Logger.getLogger(TaskPollTask.class.getName());

  private final ConnectionStateManager connectionStateManager;
  private final ControlPlaneStatistics statistics;

  /**
   * 创建任务轮询任务
   *
   * @param connectionStateManager 连接状态管理器
   * @param statistics 统计管理器
   */
  public TaskPollTask(
      ConnectionStateManager connectionStateManager, ControlPlaneStatistics statistics) {
    this.connectionStateManager = connectionStateManager;
    this.statistics = statistics;
  }

  @Override
  public void run() {
    long count = statistics.recordTaskPoll();

    // 只有在 CONNECTED 状态才轮询任务
    if (!connectionStateManager.isConnected()) {
      logger.log(
          Level.FINE,
          "Skip task poll (count: {0}), not connected (state: {1})",
          new Object[] {count, connectionStateManager.getState()});
      return;
    }

    try {
      logger.log(Level.FINE, "Polling tasks (count: {0})...", count);
      // TODO: 实现任务轮询逻辑
      // client.fetchTasks();
    } catch (RuntimeException e) {
      logger.log(
          Level.WARNING,
          "Failed to poll tasks (count: {0}): {1}",
          new Object[] {count, e.getMessage()});
    }
  }
}
