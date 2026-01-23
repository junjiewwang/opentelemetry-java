/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.result;

import io.opentelemetry.sdk.extension.controlplane.task.TaskResultSizePolicy;
import io.opentelemetry.sdk.extension.controlplane.task.TaskResultSizePolicy.ChunkInfo;
import io.opentelemetry.sdk.extension.controlplane.task.TaskResultSizePolicy.TaskResultWrapper;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 任务结果生命周期管理服务的默认实现
 *
 * <p>编排结果的完整生命周期：
 * <pre>
 * 1. 应用 TaskResultSizePolicy（压缩/分片/拒绝）
 * 2. 持久化到 TaskResultStore
 * 3. 通过 TaskResultUploader 上传
 * 4. 成功：清理本地存储
 * 5. 失败：根据 TaskResultRetryPolicy 决定重试或放弃
 * </pre>
 */
public final class DefaultTaskResultLifecycleService implements TaskResultLifecycleService {

  private static final Logger logger = Logger.getLogger(DefaultTaskResultLifecycleService.class.getName());

  private final TaskResultSizePolicy sizePolicy;
  private final TaskResultStore store;
  private final TaskResultUploader uploader;
  private final TaskResultRetryPolicy retryPolicy;
  private final ScheduledExecutorService scheduler;
  private final AtomicBoolean closed;

  private DefaultTaskResultLifecycleService(Builder builder) {
    this.sizePolicy = Objects.requireNonNull(builder.sizePolicy, "sizePolicy is required");
    this.store = Objects.requireNonNull(builder.store, "store is required");
    this.uploader = Objects.requireNonNull(builder.uploader, "uploader is required");
    this.retryPolicy = builder.retryPolicy != null
        ? builder.retryPolicy : TaskResultRetryPolicy.defaultPolicy();
    this.scheduler = Objects.requireNonNull(builder.scheduler, "scheduler is required");
    this.closed = new AtomicBoolean(false);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public CompletableFuture<TaskResultHandle> onResultProduced(
      String taskId,
      String taskType,
      byte[] data,
      String contentType) {
    return onResultProduced(taskId, taskType, data, contentType, null);
  }

  @Override
  public CompletableFuture<TaskResultHandle> onResultProduced(
      String taskId,
      String taskType,
      byte[] data,
      String contentType,
      @Nullable Map<String, String> metadata) {

    if (closed.get()) {
      return CompletableFuture.completedFuture(
          TaskResultHandle.failed(taskId, "Service is closed"));
    }

    logger.log(
        Level.INFO,
        "[RESULT-LIFECYCLE] Processing result: taskId={0}, taskType={1}, size={2}",
        new Object[] {taskId, taskType, data.length});

    // 1. 应用大小策略
    TaskResultWrapper wrapper = sizePolicy.process(taskId, data, contentType);

    // 2. 处理被拒绝的结果
    if (wrapper.isRejected()) {
      String errorMsg = wrapper.getErrorMessage() != null ? wrapper.getErrorMessage() : "Result rejected";
      logger.log(
          Level.WARNING,
          "[RESULT-LIFECYCLE] Result rejected: taskId={0}, reason={1}",
          new Object[] {taskId, errorMsg});
      return CompletableFuture.completedFuture(
          TaskResultHandle.rejected(taskId, errorMsg));
    }

    // 3. 构建描述符
    TaskResultDescriptor.Builder descriptorBuilder = TaskResultDescriptor.builder()
        .taskId(taskId)
        .taskType(taskType)
        .contentType(contentType)
        .originalSize(wrapper.getOriginalSize())
        .finalSize(wrapper.getFinalSize())
        .compressed(wrapper.isCompressed());

    if (metadata != null) {
      descriptorBuilder.metadata(metadata);
    }

    // 根据处理类型设置描述符
    switch (wrapper.getType()) {
      case DIRECT:
        descriptorBuilder.type(TaskResultDescriptor.ResultType.DIRECT);
        break;
      case COMPRESSED:
        descriptorBuilder.type(TaskResultDescriptor.ResultType.COMPRESSED);
        break;
      case CHUNKED:
        descriptorBuilder.type(TaskResultDescriptor.ResultType.CHUNKED);
        List<ChunkInfo> chunks = wrapper.getChunks();
        if (chunks != null && !chunks.isEmpty()) {
          descriptorBuilder.uploadId(chunks.get(0).getUploadId());
          descriptorBuilder.totalChunks(chunks.size());
        }
        break;
      default:
        // 不应该到达这里
        return CompletableFuture.completedFuture(
            TaskResultHandle.failed(taskId, "Unknown result type: " + wrapper.getType()));
    }

    TaskResultDescriptor descriptor = descriptorBuilder.build();

    // 4. 持久化结果
    byte[] resultData = wrapper.getData();
    if (resultData == null && wrapper.getChunks() != null) {
      // 对于分片结果，需要从 chunks 中获取数据
      resultData = combineChunkData(wrapper.getChunks());
    }

    if (resultData == null) {
      return CompletableFuture.completedFuture(
          TaskResultHandle.failed(taskId, "No result data available"));
    }

    TaskResultDescriptor savedDescriptor = store.save(descriptor, resultData);

    // 5. 触发上传
    return doUpload(savedDescriptor, resultData);
  }

  @Override
  public CompletableFuture<TaskResultHandle> retryFailed(String taskId) {
    if (closed.get()) {
      return CompletableFuture.completedFuture(
          TaskResultHandle.failed(taskId, "Service is closed"));
    }

    TaskResultDescriptor descriptor = store.get(taskId);
    if (descriptor == null) {
      return CompletableFuture.completedFuture(
          TaskResultHandle.failed(taskId, "Result not found"));
    }

    if (!descriptor.isRetryable()) {
      return CompletableFuture.completedFuture(
          TaskResultHandle.failed(taskId, "Result is not retryable: status=" + descriptor.getStatus()));
    }

    byte[] data = store.readData(descriptor);
    if (data == null) {
      return CompletableFuture.completedFuture(
          TaskResultHandle.failed(taskId, "Result data not found"));
    }

    return doUpload(descriptor, data);
  }

  @Override
  public CompletableFuture<Integer> compensatePending() {
    if (closed.get()) {
      return CompletableFuture.completedFuture(0);
    }

    List<TaskResultDescriptor> pending = store.listPending();
    if (pending.isEmpty()) {
      return CompletableFuture.completedFuture(0);
    }

    logger.log(
        Level.INFO,
        "[RESULT-LIFECYCLE] Compensating {0} pending results",
        pending.size());

    CompletableFuture<Integer> result = CompletableFuture.completedFuture(0);

    for (TaskResultDescriptor descriptor : pending) {
      byte[] data = store.readData(descriptor);
      if (data != null) {
        result = result.thenCompose(count -> 
            doUpload(descriptor, data).thenApply(handle -> 
                handle.isSuccess() ? count + 1 : count));
      }
    }

    return result;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      logger.log(Level.INFO, "[RESULT-LIFECYCLE] Closing lifecycle service");
    }
  }

  /**
   * 执行上传
   */
  private CompletableFuture<TaskResultHandle> doUpload(
      TaskResultDescriptor descriptor,
      byte[] data) {

    String taskId = descriptor.getTaskId();
    int attempt = descriptor.getAttemptCount() + 1;

    // 更新状态为上传中
    TaskResultDescriptor uploading = descriptor.withUploading();
    store.updateDescriptor(uploading);

    logger.log(
        Level.INFO,
        "[RESULT-LIFECYCLE] Uploading result: taskId={0}, attempt={1}, type={2}",
        new Object[] {taskId, attempt, descriptor.getType()});

    // 根据类型选择上传方式
    CompletableFuture<TaskResultUploader.UploadResult> uploadFuture;
    if (descriptor.isChunked()) {
      uploadFuture = uploadChunked(uploading, data);
    } else {
      uploadFuture = uploader.upload(uploading, data);
    }

    return uploadFuture.thenApply(uploadResult -> {
      if (uploadResult.isSuccess()) {
        // 上传成功，清理本地存储
        store.delete(uploading);
        logger.log(
            Level.INFO,
            "[RESULT-LIFECYCLE] Upload succeeded: taskId={0}",
            taskId);
        return TaskResultHandle.success(taskId, uploading.withUploaded());

      } else {
        // 上传失败，判断是否重试
        return handleUploadFailure(uploading, uploadResult, attempt);
      }
    }).exceptionally(error -> {
      String errorMsg = error.getMessage() != null ? error.getMessage() : error.getClass().getName();
      logger.log(
          Level.WARNING,
          "[RESULT-LIFECYCLE] Upload error: taskId={0}, error={1}",
          new Object[] {taskId, errorMsg});
      return handleUploadFailure(
          uploading,
          TaskResultUploader.UploadResult.failure(errorMsg),
          attempt);
    });
  }

  /**
   * 分片上传
   */
  private CompletableFuture<TaskResultUploader.UploadResult> uploadChunked(
      TaskResultDescriptor descriptor,
      byte[] data) {

    // 重新应用大小策略获取分片信息
    TaskResultWrapper wrapper = sizePolicy.process(
        descriptor.getTaskId(),
        data,
        descriptor.getContentType());

    List<ChunkInfo> chunks = wrapper.getChunks();
    if (chunks == null || chunks.isEmpty()) {
      // 压缩后不再需要分片，直接上传
      byte[] wrapperData = wrapper.getData();
      if (wrapperData == null) {
        return CompletableFuture.completedFuture(
            TaskResultUploader.UploadResult.failure("No data available after processing"));
      }
      return uploader.upload(descriptor, wrapperData);
    }

    // 逐个上传分片
    CompletableFuture<TaskResultUploader.UploadResult> chainedFuture =
        CompletableFuture.completedFuture(TaskResultUploader.UploadResult.success());

    for (int i = 0; i < chunks.size(); i++) {
      ChunkInfo chunk = chunks.get(i);
      int chunkIndex = i;

      chainedFuture = chainedFuture.thenCompose(prevResult -> {
        if (!prevResult.isSuccess()) {
          return CompletableFuture.completedFuture(prevResult);
        }

        TaskResultDescriptor chunkDescriptor = descriptor.toBuilder()
            .chunkIndex(chunkIndex)
            .build();

        return uploader.uploadChunk(chunkDescriptor, chunk.getChunkData());
      });
    }

    // 完成分片上传
    return chainedFuture.thenCompose(lastChunkResult -> {
      if (!lastChunkResult.isSuccess()) {
        return CompletableFuture.completedFuture(lastChunkResult);
      }
      return uploader.completeChunkedUpload(descriptor);
    });
  }

  /**
   * 处理上传失败
   */
  private TaskResultHandle handleUploadFailure(
      TaskResultDescriptor descriptor,
      TaskResultUploader.UploadResult uploadResult,
      int attempt) {

    String taskId = descriptor.getTaskId();
    String reason = uploadResult.getFailureReason() != null
        ? uploadResult.getFailureReason() : "Unknown failure";

    // 判断是否应该重试
    if (uploadResult.isRetryable() && retryPolicy.shouldRetry(descriptor, attempt)) {
      // 标记为失败，调度重试
      TaskResultDescriptor failed = store.markFailed(descriptor, reason);
      scheduleRetry(failed, attempt);

      logger.log(
          Level.INFO,
          "[RESULT-LIFECYCLE] Upload failed, will retry: taskId={0}, attempt={1}, reason={2}",
          new Object[] {taskId, attempt, reason});

      return TaskResultHandle.pending(taskId, failed);
    }

    // 不可重试或超过重试次数，标记为放弃
    String abandonReason = String.format(
        Locale.ROOT, "Upload failed after %d attempts: %s", attempt, reason);
    TaskResultDescriptor abandoned = store.markAbandoned(descriptor, abandonReason);

    logger.log(
        Level.WARNING,
        "[RESULT-LIFECYCLE] Upload abandoned: taskId={0}, reason={1}",
        new Object[] {taskId, abandonReason});

    return TaskResultHandle.abandoned(taskId, abandonReason, abandoned);
  }

  /**
   * 调度重试
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  private void scheduleRetry(TaskResultDescriptor descriptor, int attempt) {
    if (closed.get()) {
      return;
    }

    Duration backoff = retryPolicy.nextBackoff(attempt);
    String taskId = descriptor.getTaskId();

    logger.log(
        Level.INFO,
        "[RESULT-LIFECYCLE] Scheduling retry: taskId={0}, attempt={1}, backoff={2}ms",
        new Object[] {taskId, attempt + 1, backoff.toMillis()});

    scheduler.schedule(() -> {
      if (!closed.get()) {
        retryFailed(taskId);
      }
    }, backoff.toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * 合并分片数据
   */
  private static byte[] combineChunkData(List<ChunkInfo> chunks) {
    int totalSize = 0;
    for (ChunkInfo chunk : chunks) {
      totalSize += chunk.getChunkData().length;
    }

    byte[] combined = new byte[totalSize];
    int offset = 0;
    for (ChunkInfo chunk : chunks) {
      byte[] chunkData = chunk.getChunkData();
      System.arraycopy(chunkData, 0, combined, offset, chunkData.length);
      offset += chunkData.length;
    }

    return combined;
  }

  // ===== Builder =====

  public static final class Builder {
    @Nullable private TaskResultSizePolicy sizePolicy;
    @Nullable private TaskResultStore store;
    @Nullable private TaskResultUploader uploader;
    @Nullable private TaskResultRetryPolicy retryPolicy;
    @Nullable private ScheduledExecutorService scheduler;

    private Builder() {}

    public Builder sizePolicy(TaskResultSizePolicy sizePolicy) {
      this.sizePolicy = sizePolicy;
      return this;
    }

    public Builder store(TaskResultStore store) {
      this.store = store;
      return this;
    }

    public Builder uploader(TaskResultUploader uploader) {
      this.uploader = uploader;
      return this;
    }

    public Builder retryPolicy(TaskResultRetryPolicy retryPolicy) {
      this.retryPolicy = retryPolicy;
      return this;
    }

    public Builder scheduler(ScheduledExecutorService scheduler) {
      this.scheduler = scheduler;
      return this;
    }

    public DefaultTaskResultLifecycleService build() {
      return new DefaultTaskResultLifecycleService(this);
    }
  }
}
