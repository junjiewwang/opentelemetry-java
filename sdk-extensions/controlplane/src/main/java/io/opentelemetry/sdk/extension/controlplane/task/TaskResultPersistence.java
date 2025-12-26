/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task;

import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 任务结果持久化存储
 *
 * <p>提供以下功能：
 *
 * <ul>
 *   <li>本地文件存储任务结果
 *   <li>LRU 淘汰策略
 *   <li>支持重传队列
 *   <li>上传成功后自动清理
 * </ul>
 */
public final class TaskResultPersistence {

  private static final Logger logger = Logger.getLogger(TaskResultPersistence.class.getName());
  private static final String FILE_EXTENSION = ".result";
  private static final String RETRY_PREFIX = "retry_";

  private final Path storageDir;
  private final int maxFiles;
  private final long maxSizeBytes;
  private final int maxRetryCount;
  private final Duration resultExpiration;

  // 内存中的结果索引
  private final Map<String, ResultMetadata> resultIndex;

  // 待重传的结果
  private final Map<String, AtomicInteger> retryCountMap;

  /**
   * 从配置创建持久化存储
   *
   * @param config 控制平面配置
   * @return 持久化存储实例
   */
  public static TaskResultPersistence create(ControlPlaneConfig config) {
    return new TaskResultPersistence(
        Paths.get(config.getStorageDir()),
        config.getStorageMaxFiles(),
        config.getStorageMaxSize(),
        config.getTaskResultMaxRetry(),
        config.getTaskResultExpiration());
  }

  /**
   * 创建持久化存储
   *
   * @param storageDir 存储目录
   * @param maxFiles 最大文件数
   * @param maxSizeBytes 最大存储大小 (字节)
   * @param maxRetryCount 最大重试次数
   * @param resultExpiration 结果过期时间
   */
  public TaskResultPersistence(
      Path storageDir,
      int maxFiles,
      long maxSizeBytes,
      int maxRetryCount,
      Duration resultExpiration) {
    this.storageDir = storageDir;
    this.maxFiles = maxFiles;
    this.maxSizeBytes = maxSizeBytes;
    this.maxRetryCount = maxRetryCount;
    this.resultExpiration = resultExpiration;
    this.resultIndex = new ConcurrentHashMap<>();
    this.retryCountMap = new ConcurrentHashMap<>();

    ensureStorageDirectory();
    loadExistingResults();
  }

  private void ensureStorageDirectory() {
    try {
      if (!Files.exists(storageDir)) {
        Files.createDirectories(storageDir);
        logger.log(Level.INFO, "Created storage directory: {0}", storageDir);
      }
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Failed to create storage directory: " + storageDir, e);
      throw new IllegalStateException("Failed to create storage directory", e);
    }
  }

  private void loadExistingResults() {
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(storageDir, "*" + FILE_EXTENSION)) {
      for (Path file : stream) {
        String fileName = file.getFileName().toString();
        String taskId = fileName.substring(0, fileName.length() - FILE_EXTENSION.length());

        // 检查是否是重试文件
        if (taskId.startsWith(RETRY_PREFIX)) {
          taskId = taskId.substring(RETRY_PREFIX.length());
        }

        long size = Files.size(file);
        Instant modifiedTime = Files.getLastModifiedTime(file).toInstant();

        // 检查是否过期
        if (Duration.between(modifiedTime, Instant.now()).compareTo(resultExpiration) > 0) {
          Files.delete(file);
          logger.log(Level.FINE, "Deleted expired result file: {0}", file);
          continue;
        }

        resultIndex.put(taskId, new ResultMetadata(taskId, file, size, modifiedTime));
      }

      logger.log(Level.INFO, "Loaded {0} existing result files", resultIndex.size());
    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to load existing results", e);
    }
  }

  /**
   * 保存任务结果
   *
   * @param taskId 任务 ID
   * @param data 结果数据
   * @return 是否保存成功
   */
  public boolean save(String taskId, byte[] data) {
    try {
      // 检查存储空间
      ensureStorageSpace(data.length);

      Path filePath = storageDir.resolve(taskId + FILE_EXTENSION);
      Files.write(filePath, data);

      ResultMetadata metadata = new ResultMetadata(taskId, filePath, data.length, Instant.now());
      resultIndex.put(taskId, metadata);

      logger.log(
          Level.FINE, "Saved task result: {0}, size: {1}", new Object[] {taskId, data.length});
      return true;
    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to save task result: " + taskId, e);
      return false;
    }
  }

  /**
   * 读取任务结果
   *
   * @param taskId 任务 ID
   * @return 结果数据 (如果存在)
   */
  public Optional<byte[]> read(String taskId) {
    ResultMetadata metadata = resultIndex.get(taskId);
    if (metadata == null) {
      return Optional.empty();
    }

    try {
      byte[] data = Files.readAllBytes(metadata.filePath);
      return Optional.of(data);
    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to read task result: " + taskId, e);
      return Optional.empty();
    }
  }

  /**
   * 删除任务结果 (上传成功后调用)
   *
   * @param taskId 任务 ID
   */
  public void delete(String taskId) {
    ResultMetadata metadata = resultIndex.remove(taskId);
    retryCountMap.remove(taskId);

    if (metadata != null) {
      try {
        Files.deleteIfExists(metadata.filePath);
        logger.log(Level.FINE, "Deleted task result: {0}", taskId);
      } catch (IOException e) {
        logger.log(Level.WARNING, "Failed to delete task result: " + taskId, e);
      }
    }
  }

  /**
   * 标记为需要重传
   *
   * @param taskId 任务 ID
   * @return 当前重试次数，如果超过最大重试次数则返回 -1
   */
  public int markForRetry(String taskId) {
    AtomicInteger count = retryCountMap.computeIfAbsent(taskId, k -> new AtomicInteger(0));
    int currentCount = count.incrementAndGet();

    if (currentCount > maxRetryCount) {
      logger.log(
          Level.WARNING,
          "Task {0} exceeded max retry count ({1}), will not retry",
          new Object[] {taskId, maxRetryCount});
      // 清理该任务
      delete(taskId);
      return -1;
    }

    logger.log(
        Level.FINE,
        "Task {0} marked for retry, count: {1}/{2}",
        new Object[] {taskId, currentCount, maxRetryCount});
    return currentCount;
  }

  /**
   * 获取待重传的任务 ID 列表
   *
   * @return 任务 ID 列表
   */
  public List<String> getPendingRetryTaskIds() {
    return new ArrayList<>(retryCountMap.keySet());
  }

  /**
   * 获取任务的重试次数
   *
   * @param taskId 任务 ID
   * @return 重试次数
   */
  public int getRetryCount(String taskId) {
    AtomicInteger count = retryCountMap.get(taskId);
    return count != null ? count.get() : 0;
  }

  /** 清理过期的结果文件 */
  public void cleanupExpired() {
    Instant now = Instant.now();
    List<String> expiredTaskIds = new ArrayList<>();

    for (Map.Entry<String, ResultMetadata> entry : resultIndex.entrySet()) {
      if (Duration.between(entry.getValue().createdTime, now).compareTo(resultExpiration) > 0) {
        expiredTaskIds.add(entry.getKey());
      }
    }

    for (String taskId : expiredTaskIds) {
      delete(taskId);
      logger.log(Level.FINE, "Cleaned up expired task result: {0}", taskId);
    }

    if (!expiredTaskIds.isEmpty()) {
      logger.log(Level.INFO, "Cleaned up {0} expired task results", expiredTaskIds.size());
    }
  }

  /**
   * 获取当前存储使用量
   *
   * @return 已使用字节数
   */
  public long getCurrentSizeBytes() {
    return resultIndex.values().stream().mapToLong(m -> m.sizeBytes).sum();
  }

  /**
   * 获取当前文件数
   *
   * @return 文件数量
   */
  public int getCurrentFileCount() {
    return resultIndex.size();
  }

  private void ensureStorageSpace(long requiredSize) throws IOException {
    // 检查文件数量限制
    while (resultIndex.size() >= maxFiles) {
      evictOldest();
    }

    // 检查存储大小限制
    while (getCurrentSizeBytes() + requiredSize > maxSizeBytes && !resultIndex.isEmpty()) {
      evictOldest();
    }
  }

  private void evictOldest() throws IOException {
    // 找到最早的文件
    Optional<ResultMetadata> oldest =
        resultIndex.values().stream().min(Comparator.comparing(m -> m.createdTime));

    if (oldest.isPresent()) {
      ResultMetadata metadata = oldest.get();
      resultIndex.remove(metadata.taskId);
      retryCountMap.remove(metadata.taskId);
      Files.deleteIfExists(metadata.filePath);
      logger.log(Level.FINE, "Evicted oldest task result: {0}", metadata.taskId);
    }
  }

  /** 结果元数据 */
  private static final class ResultMetadata {
    final String taskId;
    final Path filePath;
    final long sizeBytes;
    final Instant createdTime;

    ResultMetadata(String taskId, Path filePath, long sizeBytes, Instant createdTime) {
      this.taskId = taskId;
      this.filePath = filePath;
      this.sizeBytes = sizeBytes;
      this.createdTime = createdTime;
    }
  }
}
