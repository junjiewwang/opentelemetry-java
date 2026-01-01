/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.tasks;

import io.opentelemetry.sdk.extension.controlplane.task.TaskResultPersistence;
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
    try {
      logger.log(Level.FINE, "Running cleanup task...");
      resultPersistence.cleanupExpired();
      logger.log(Level.FINE, "Cleanup task completed");
    } catch (RuntimeException e) {
      logger.log(Level.WARNING, "Cleanup task failed: {0}", e.getMessage());
    }
  }
}
