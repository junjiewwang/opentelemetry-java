/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.result;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 基于内存的任务结果存储实现
 *
 * <p>适用于以下场景：
 * <ul>
 *   <li>开发和测试环境
 *   <li>结果数据量较小的场景
 *   <li>不需要持久化恢复的场景
 * </ul>
 *
 * <p><b>注意</b>：该实现不会将数据持久化到磁盘，JVM 重启后数据会丢失。
 * 生产环境建议使用文件系统存储实现。
 */
public final class InMemoryTaskResultStore implements TaskResultStore {

  private static final Logger logger = Logger.getLogger(InMemoryTaskResultStore.class.getName());

  /** 描述符存储 */
  private final ConcurrentHashMap<String, TaskResultDescriptor> descriptors;

  /** 数据存储 */
  private final ConcurrentHashMap<String, byte[]> dataStore;

  public InMemoryTaskResultStore() {
    this.descriptors = new ConcurrentHashMap<>();
    this.dataStore = new ConcurrentHashMap<>();
  }

  @Override
  public TaskResultDescriptor save(TaskResultDescriptor descriptor, byte[] data) {
    String taskId = descriptor.getTaskId();

    // 更新描述符，设置结果路径为内存键
    TaskResultDescriptor saved = descriptor.toBuilder()
        .resultPath("memory://" + taskId)
        .build();

    descriptors.put(taskId, saved);
    dataStore.put(taskId, data);

    logger.log(
        Level.FINE,
        "[RESULT-STORE] Saved result: taskId={0}, size={1}",
        new Object[] {taskId, data.length});

    return saved;
  }

  @Override
  public boolean delete(TaskResultDescriptor descriptor) {
    String taskId = descriptor.getTaskId();
    TaskResultDescriptor removed = descriptors.remove(taskId);
    dataStore.remove(taskId);

    if (removed != null) {
      logger.log(Level.FINE, "[RESULT-STORE] Deleted result: taskId={0}", taskId);
      return true;
    }
    return false;
  }

  @Override
  public TaskResultDescriptor markFailed(TaskResultDescriptor descriptor, String reason) {
    String taskId = descriptor.getTaskId();
    TaskResultDescriptor failed = descriptor.withFailed(reason);
    descriptors.put(taskId, failed);

    logger.log(
        Level.WARNING,
        "[RESULT-STORE] Marked result as failed: taskId={0}, reason={1}",
        new Object[] {taskId, reason});

    return failed;
  }

  @Override
  public TaskResultDescriptor markAbandoned(TaskResultDescriptor descriptor, String reason) {
    String taskId = descriptor.getTaskId();
    TaskResultDescriptor abandoned = descriptor.withAbandoned(reason);
    descriptors.put(taskId, abandoned);

    logger.log(
        Level.WARNING,
        "[RESULT-STORE] Marked result as abandoned: taskId={0}, reason={1}",
        new Object[] {taskId, reason});

    return abandoned;
  }

  @Override
  public boolean updateDescriptor(TaskResultDescriptor descriptor) {
    String taskId = descriptor.getTaskId();
    if (!descriptors.containsKey(taskId)) {
      return false;
    }
    descriptors.put(taskId, descriptor);
    return true;
  }

  @Override
  public List<TaskResultDescriptor> listPending() {
    List<TaskResultDescriptor> pending = new ArrayList<>();
    for (TaskResultDescriptor desc : descriptors.values()) {
      if (desc.getStatus() == TaskResultDescriptor.ResultStatus.PENDING
          || desc.getStatus() == TaskResultDescriptor.ResultStatus.FAILED) {
        pending.add(desc);
      }
    }
    return pending;
  }

  @Override
  @Nullable
  public TaskResultDescriptor get(String taskId) {
    return descriptors.get(taskId);
  }

  @Override
  @Nullable
  public byte[] readData(TaskResultDescriptor descriptor) {
    return dataStore.get(descriptor.getTaskId());
  }

  @Override
  public int size() {
    return descriptors.size();
  }

  @Override
  public int cleanupExpired(long maxAgeMillis) {
    long now = System.currentTimeMillis();
    int cleaned = 0;

    List<String> toRemove = new ArrayList<>();
    for (TaskResultDescriptor desc : descriptors.values()) {
      // 只清理已放弃的结果
      if (desc.getStatus() == TaskResultDescriptor.ResultStatus.ABANDONED) {
        long age = now - desc.getCreatedAtMillis();
        if (age > maxAgeMillis) {
          toRemove.add(desc.getTaskId());
        }
      }
    }

    for (String taskId : toRemove) {
      descriptors.remove(taskId);
      dataStore.remove(taskId);
      cleaned++;
    }

    if (cleaned > 0) {
      logger.log(
          Level.INFO,
          "[RESULT-STORE] Cleaned up {0} expired abandoned results",
          cleaned);
    }

    return cleaned;
  }

  /**
   * 清空所有存储（仅用于测试）
   */
  public void clear() {
    descriptors.clear();
    dataStore.clear();
  }
}
