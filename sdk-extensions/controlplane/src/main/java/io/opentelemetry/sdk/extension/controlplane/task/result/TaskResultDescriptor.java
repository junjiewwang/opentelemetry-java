/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.result;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * 任务结果描述符
 *
 * <p>统一描述任务结果的元数据信息，用于结果生命周期管理的各个阶段：
 * <ul>
 *   <li>结果持久化（TaskResultStore）
 *   <li>结果上传（TaskResultUploader）
 *   <li>重试策略判断（TaskResultRetryPolicy）
 * </ul>
 *
 * <p>该类是不可变的（immutable），通过 Builder 模式创建实例。
 */
public final class TaskResultDescriptor {

  /** 结果类型 */
  public enum ResultType {
    /** 直接上传（未压缩） */
    DIRECT,
    /** 压缩后上传 */
    COMPRESSED,
    /** 分片上传 */
    CHUNKED
  }

  /** 结果状态 */
  public enum ResultStatus {
    /** 待上传 */
    PENDING,
    /** 上传中 */
    UPLOADING,
    /** 上传成功 */
    UPLOADED,
    /** 上传失败 */
    FAILED,
    /** 已放弃（超过重试次数） */
    ABANDONED
  }

  // 核心标识
  private final String taskId;
  private final String taskType;

  // 存储路径
  @Nullable private final String resultPath;

  // 内容信息
  private final String contentType;
  private final long originalSize;
  private final long finalSize;
  private final boolean compressed;
  @Nullable private final String checksum;

  // 分片信息（仅 CHUNKED 类型使用）
  @Nullable private final String uploadId;
  private final int chunkIndex;
  private final int totalChunks;

  // 状态跟踪
  private final ResultType type;
  private final ResultStatus status;
  private final int attemptCount;
  @Nullable private final String failureReason;

  // 时间戳
  private final long createdAtMillis;
  @Nullable private final Long lastAttemptAtMillis;

  // 扩展元数据
  private final Map<String, String> metadata;

  private TaskResultDescriptor(Builder builder) {
    this.taskId = Objects.requireNonNull(builder.taskId, "taskId is required");
    this.taskType = Objects.requireNonNull(builder.taskType, "taskType is required");
    this.resultPath = builder.resultPath;
    this.contentType = builder.contentType != null ? builder.contentType : "application/octet-stream";
    this.originalSize = builder.originalSize;
    this.finalSize = builder.finalSize;
    this.compressed = builder.compressed;
    this.checksum = builder.checksum;
    this.uploadId = builder.uploadId;
    this.chunkIndex = builder.chunkIndex;
    this.totalChunks = builder.totalChunks;
    this.type = builder.type != null ? builder.type : ResultType.DIRECT;
    this.status = builder.status != null ? builder.status : ResultStatus.PENDING;
    this.attemptCount = builder.attemptCount;
    this.failureReason = builder.failureReason;
    this.createdAtMillis = builder.createdAtMillis > 0 ? builder.createdAtMillis : System.currentTimeMillis();
    this.lastAttemptAtMillis = builder.lastAttemptAtMillis;
    this.metadata = builder.metadata != null
        ? Collections.unmodifiableMap(new HashMap<>(builder.metadata))
        : Collections.emptyMap();
  }

  // ===== Getters =====

  public String getTaskId() {
    return taskId;
  }

  public String getTaskType() {
    return taskType;
  }

  @Nullable
  public String getResultPath() {
    return resultPath;
  }

  public String getContentType() {
    return contentType;
  }

  public long getOriginalSize() {
    return originalSize;
  }

  public long getFinalSize() {
    return finalSize;
  }

  public boolean isCompressed() {
    return compressed;
  }

  @Nullable
  public String getChecksum() {
    return checksum;
  }

  @Nullable
  public String getUploadId() {
    return uploadId;
  }

  public int getChunkIndex() {
    return chunkIndex;
  }

  public int getTotalChunks() {
    return totalChunks;
  }

  public ResultType getType() {
    return type;
  }

  public ResultStatus getStatus() {
    return status;
  }

  public int getAttemptCount() {
    return attemptCount;
  }

  @Nullable
  public String getFailureReason() {
    return failureReason;
  }

  public long getCreatedAtMillis() {
    return createdAtMillis;
  }

  @Nullable
  public Long getLastAttemptAtMillis() {
    return lastAttemptAtMillis;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  // ===== 便捷方法 =====

  /**
   * 是否是分片上传
   */
  public boolean isChunked() {
    return type == ResultType.CHUNKED;
  }

  /**
   * 是否是最后一个分片
   */
  public boolean isLastChunk() {
    return isChunked() && chunkIndex == totalChunks - 1;
  }

  /**
   * 获取压缩比
   */
  public double getCompressionRatio() {
    return originalSize > 0 ? (double) finalSize / originalSize : 1.0;
  }

  /**
   * 是否处于终态
   */
  public boolean isTerminal() {
    return status == ResultStatus.UPLOADED
        || status == ResultStatus.ABANDONED;
  }

  /**
   * 是否可以重试
   */
  public boolean isRetryable() {
    return status == ResultStatus.FAILED;
  }

  // ===== 状态转换方法（返回新实例，保持不可变性）=====

  /**
   * 创建标记为上传中的新实例
   */
  public TaskResultDescriptor withUploading() {
    return toBuilder()
        .status(ResultStatus.UPLOADING)
        .lastAttemptAtMillis(System.currentTimeMillis())
        .attemptCount(attemptCount + 1)
        .build();
  }

  /**
   * 创建标记为上传成功的新实例
   */
  public TaskResultDescriptor withUploaded() {
    return toBuilder()
        .status(ResultStatus.UPLOADED)
        .build();
  }

  /**
   * 创建标记为上传失败的新实例
   */
  public TaskResultDescriptor withFailed(String reason) {
    return toBuilder()
        .status(ResultStatus.FAILED)
        .failureReason(reason)
        .build();
  }

  /**
   * 创建标记为已放弃的新实例
   */
  public TaskResultDescriptor withAbandoned(String reason) {
    return toBuilder()
        .status(ResultStatus.ABANDONED)
        .failureReason(reason)
        .build();
  }

  // ===== Builder =====

  public static Builder builder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder()
        .taskId(taskId)
        .taskType(taskType)
        .resultPath(resultPath)
        .contentType(contentType)
        .originalSize(originalSize)
        .finalSize(finalSize)
        .compressed(compressed)
        .checksum(checksum)
        .uploadId(uploadId)
        .chunkIndex(chunkIndex)
        .totalChunks(totalChunks)
        .type(type)
        .status(status)
        .attemptCount(attemptCount)
        .failureReason(failureReason)
        .createdAtMillis(createdAtMillis)
        .lastAttemptAtMillis(lastAttemptAtMillis)
        .metadata(metadata);
  }

  @Override
  public String toString() {
    return "TaskResultDescriptor{"
        + "taskId='" + taskId + '\''
        + ", taskType='" + taskType + '\''
        + ", type=" + type
        + ", status=" + status
        + ", originalSize=" + originalSize
        + ", finalSize=" + finalSize
        + ", compressed=" + compressed
        + ", attemptCount=" + attemptCount
        + '}';
  }

  public static final class Builder {
    @Nullable private String taskId;
    @Nullable private String taskType;
    @Nullable private String resultPath;
    @Nullable private String contentType;
    private long originalSize;
    private long finalSize;
    private boolean compressed;
    @Nullable private String checksum;
    @Nullable private String uploadId;
    private int chunkIndex;
    private int totalChunks;
    @Nullable private ResultType type;
    @Nullable private ResultStatus status;
    private int attemptCount;
    @Nullable private String failureReason;
    private long createdAtMillis;
    @Nullable private Long lastAttemptAtMillis;
    @Nullable private Map<String, String> metadata;

    private Builder() {}

    public Builder taskId(String taskId) {
      this.taskId = taskId;
      return this;
    }

    public Builder taskType(String taskType) {
      this.taskType = taskType;
      return this;
    }

    public Builder resultPath(@Nullable String resultPath) {
      this.resultPath = resultPath;
      return this;
    }

    public Builder contentType(@Nullable String contentType) {
      this.contentType = contentType;
      return this;
    }

    public Builder originalSize(long originalSize) {
      this.originalSize = originalSize;
      return this;
    }

    public Builder finalSize(long finalSize) {
      this.finalSize = finalSize;
      return this;
    }

    public Builder compressed(boolean compressed) {
      this.compressed = compressed;
      return this;
    }

    public Builder checksum(@Nullable String checksum) {
      this.checksum = checksum;
      return this;
    }

    public Builder uploadId(@Nullable String uploadId) {
      this.uploadId = uploadId;
      return this;
    }

    public Builder chunkIndex(int chunkIndex) {
      this.chunkIndex = chunkIndex;
      return this;
    }

    public Builder totalChunks(int totalChunks) {
      this.totalChunks = totalChunks;
      return this;
    }

    public Builder type(ResultType type) {
      this.type = type;
      return this;
    }

    public Builder status(ResultStatus status) {
      this.status = status;
      return this;
    }

    public Builder attemptCount(int attemptCount) {
      this.attemptCount = attemptCount;
      return this;
    }

    public Builder failureReason(@Nullable String failureReason) {
      this.failureReason = failureReason;
      return this;
    }

    public Builder createdAtMillis(long createdAtMillis) {
      this.createdAtMillis = createdAtMillis;
      return this;
    }

    public Builder lastAttemptAtMillis(@Nullable Long lastAttemptAtMillis) {
      this.lastAttemptAtMillis = lastAttemptAtMillis;
      return this;
    }

    public Builder metadata(@Nullable Map<String, String> metadata) {
      this.metadata = metadata;
      return this;
    }

    public Builder addMetadata(String key, String value) {
      if (this.metadata == null) {
        this.metadata = new HashMap<>();
      }
      this.metadata.put(key, value);
      return this;
    }

    public TaskResultDescriptor build() {
      return new TaskResultDescriptor(this);
    }
  }
}
