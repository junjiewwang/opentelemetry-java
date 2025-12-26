/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task;

import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nullable;

/**
 * 任务结果大小处理策略
 *
 * <p>根据配置的阈值对任务结果进行压缩或分片处理：
 *
 * <ul>
 *   <li>小于压缩阈值 (默认 1KB)：不压缩，直接上传
 *   <li>压缩阈值 ~ 分片阈值 (默认 50MB)：gzip 压缩后上传
 *   <li>分片阈值 ~ 最大阈值 (默认 200MB)：分片上传
 *   <li>超过最大阈值：拒绝上传，记录错误日志
 * </ul>
 */
public final class TaskResultSizePolicy {

  private static final Logger logger = Logger.getLogger(TaskResultSizePolicy.class.getName());

  private final long compressionThreshold;
  private final long chunkedThreshold;
  private final long chunkSize;
  private final long maxSize;

  /**
   * 从配置创建策略实例
   *
   * @param config 控制平面配置
   * @return 策略实例
   */
  public static TaskResultSizePolicy create(ControlPlaneConfig config) {
    return new TaskResultSizePolicy(
        config.getCompressionThreshold(),
        config.getChunkedThreshold(),
        config.getChunkSize(),
        config.getMaxSize());
  }

  /**
   * 创建策略实例
   *
   * @param compressionThreshold 压缩阈值 (字节)
   * @param chunkedThreshold 分片阈值 (字节)
   * @param chunkSize 分片大小 (字节)
   * @param maxSize 最大阈值 (字节)
   */
  public TaskResultSizePolicy(
      long compressionThreshold, long chunkedThreshold, long chunkSize, long maxSize) {
    validateConfig(compressionThreshold, chunkedThreshold, chunkSize, maxSize);
    this.compressionThreshold = compressionThreshold;
    this.chunkedThreshold = chunkedThreshold;
    this.chunkSize = chunkSize;
    this.maxSize = maxSize;
  }

  private static void validateConfig(
      long compressionThreshold, long chunkedThreshold, long chunkSize, long maxSize) {
    if (compressionThreshold >= chunkedThreshold) {
      throw new IllegalArgumentException("compressionThreshold must be less than chunkedThreshold");
    }
    if (chunkedThreshold >= maxSize) {
      throw new IllegalArgumentException("chunkedThreshold must be less than maxSize");
    }
    if (chunkSize > chunkedThreshold) {
      throw new IllegalArgumentException(
          "chunkSize must be less than or equal to chunkedThreshold");
    }
  }

  /**
   * 处理任务结果
   *
   * @param taskId 任务 ID
   * @param resultData 结果数据
   * @param dataType 数据类型 (MIME type)
   * @return 处理后的结果包装器
   */
  public TaskResultWrapper process(String taskId, byte[] resultData, String dataType) {
    Objects.requireNonNull(taskId, "taskId");
    Objects.requireNonNull(resultData, "resultData");
    Objects.requireNonNull(dataType, "dataType");

    long originalSize = resultData.length;

    // 1. 检查是否超过最大阈值
    if (originalSize > maxSize) {
      logger.log(
          Level.SEVERE,
          "Task result size {0} exceeds maximum threshold {1} for task {2}, "
              + "result will not be uploaded",
          new Object[] {formatSize(originalSize), formatSize(maxSize), taskId});

      return TaskResultWrapper.rejected(
          taskId,
          String.format(
              "Result size %s exceeds maximum allowed size %s",
              formatSize(originalSize), formatSize(maxSize)),
          originalSize);
    }

    // 2. 小于压缩阈值，不压缩
    if (originalSize <= compressionThreshold) {
      return TaskResultWrapper.direct(taskId, resultData, dataType, originalSize);
    }

    // 3. 压缩处理
    byte[] compressed = gzipCompress(resultData);
    long compressedSize = compressed.length;

    // 压缩后仍大于原始大小，不压缩
    if (compressedSize >= originalSize) {
      compressed = resultData;
      compressedSize = originalSize;
    }

    // 4. 检查是否需要分片 (基于压缩后大小)
    if (compressedSize <= chunkedThreshold) {
      // 不需要分片，直接上传
      boolean isCompressed = compressedSize < originalSize;
      return TaskResultWrapper.compressed(
          taskId, compressed, dataType, originalSize, compressedSize, isCompressed);
    }

    // 5. 需要分片上传
    List<ChunkInfo> chunks = splitIntoChunks(taskId, compressed);

    logger.log(
        Level.INFO,
        "Task {0} result split into {1} chunks, original size: {2}, compressed size: {3}",
        new Object[] {taskId, chunks.size(), formatSize(originalSize), formatSize(compressedSize)});

    return TaskResultWrapper.chunked(taskId, chunks, dataType, originalSize, compressedSize);
  }

  private static byte[] gzipCompress(byte[] data) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
        GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
      gzip.write(data);
      gzip.finish();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException("GZIP compression failed", e);
    }
  }

  private List<ChunkInfo> splitIntoChunks(String taskId, byte[] data) {
    List<ChunkInfo> chunks = new ArrayList<>();
    String uploadId = UUID.randomUUID().toString();
    int totalChunks = (int) Math.ceil((double) data.length / chunkSize);

    for (int i = 0; i < totalChunks; i++) {
      int start = (int) (i * chunkSize);
      int end = (int) Math.min(start + chunkSize, data.length);
      byte[] chunkData = Arrays.copyOfRange(data, start, end);

      chunks.add(
          new ChunkInfo(
              taskId,
              uploadId,
              i,
              totalChunks,
              chunkData,
              md5Checksum(chunkData),
              i == totalChunks - 1));
    }

    return chunks;
  }

  private static String md5Checksum(byte[] data) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      return Base64.getEncoder().encodeToString(md.digest(data));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String formatSize(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    }
    if (bytes < 1024 * 1024) {
      return String.format(Locale.ROOT, "%.2f KB", bytes / 1024.0);
    }
    if (bytes < 1024L * 1024 * 1024) {
      return String.format(Locale.ROOT, "%.2f MB", bytes / (1024.0 * 1024));
    }
    return String.format(Locale.ROOT, "%.2f GB", bytes / (1024.0 * 1024 * 1024));
  }

  /** 任务结果包装器 */
  public static final class TaskResultWrapper {

    /** 处理类型 */
    public enum Type {
      /** 直接上传 (未压缩) */
      DIRECT,
      /** 压缩后上传 */
      COMPRESSED,
      /** 分片上传 */
      CHUNKED,
      /** 拒绝上传 (超过最大阈值) */
      REJECTED
    }

    private final Type type;
    private final String taskId;
    @Nullable private final byte[] data;
    @Nullable private final List<ChunkInfo> chunks;
    @Nullable private final String dataType;
    private final long originalSize;
    private final long finalSize;
    private final boolean isCompressed;
    @Nullable private final String errorMessage;

    private TaskResultWrapper(
        Type type,
        String taskId,
        @Nullable byte[] data,
        @Nullable List<ChunkInfo> chunks,
        @Nullable String dataType,
        long originalSize,
        long finalSize,
        boolean isCompressed,
        @Nullable String errorMessage) {
      this.type = type;
      this.taskId = taskId;
      this.data = data;
      this.chunks = chunks;
      this.dataType = dataType;
      this.originalSize = originalSize;
      this.finalSize = finalSize;
      this.isCompressed = isCompressed;
      this.errorMessage = errorMessage;
    }

    static TaskResultWrapper direct(
        String taskId, byte[] data, String dataType, long originalSize) {
      return new TaskResultWrapper(
          Type.DIRECT,
          taskId,
          data,
          null,
          dataType,
          originalSize,
          originalSize,
          /* isCompressed= */ false,
          null);
    }

    static TaskResultWrapper compressed(
        String taskId,
        byte[] data,
        String dataType,
        long originalSize,
        long compressedSize,
        boolean isCompressed) {
      return new TaskResultWrapper(
          Type.COMPRESSED,
          taskId,
          data,
          null,
          dataType,
          originalSize,
          compressedSize,
          isCompressed,
          null);
    }

    static TaskResultWrapper chunked(
        String taskId,
        List<ChunkInfo> chunks,
        String dataType,
        long originalSize,
        long compressedSize) {
      return new TaskResultWrapper(
          Type.CHUNKED,
          taskId,
          null,
          chunks,
          dataType,
          originalSize,
          compressedSize,
          /* isCompressed= */ true,
          null);
    }

    static TaskResultWrapper rejected(String taskId, String errorMessage, long originalSize) {
      return new TaskResultWrapper(
          Type.REJECTED,
          taskId,
          null,
          null,
          null,
          originalSize,
          0,
          /* isCompressed= */ false,
          errorMessage);
    }

    public Type getType() {
      return type;
    }

    public String getTaskId() {
      return taskId;
    }

    @Nullable
    public byte[] getData() {
      if (data == null) {
        return null;
      }
      return data;
    }

    @Nullable
    public List<ChunkInfo> getChunks() {
      return chunks;
    }

    @Nullable
    public String getDataType() {
      return dataType;
    }

    public long getOriginalSize() {
      return originalSize;
    }

    public long getFinalSize() {
      return finalSize;
    }

    public boolean isCompressed() {
      return isCompressed;
    }

    @Nullable
    public String getErrorMessage() {
      return errorMessage;
    }

    public boolean isRejected() {
      return type == Type.REJECTED;
    }

    public double getCompressionRatio() {
      return originalSize > 0 ? (double) finalSize / originalSize : 1.0;
    }
  }

  /** 分片信息 */
  public static final class ChunkInfo {
    private final String taskId;
    private final String uploadId;
    private final int chunkIndex;
    private final int totalChunks;
    private final byte[] chunkData;
    private final String checksum;
    private final boolean isLastChunk;

    public ChunkInfo(
        String taskId,
        String uploadId,
        int chunkIndex,
        int totalChunks,
        byte[] chunkData,
        String checksum,
        boolean isLastChunk) {
      this.taskId = taskId;
      this.uploadId = uploadId;
      this.chunkIndex = chunkIndex;
      this.totalChunks = totalChunks;
      this.chunkData = chunkData;
      this.checksum = checksum;
      this.isLastChunk = isLastChunk;
    }

    public String getTaskId() {
      return taskId;
    }

    public String getUploadId() {
      return uploadId;
    }

    public int getChunkIndex() {
      return chunkIndex;
    }

    public int getTotalChunks() {
      return totalChunks;
    }

    public byte[] getChunkData() {
      return chunkData;
    }

    public String getChecksum() {
      return checksum;
    }

    public boolean isLastChunk() {
      return isLastChunk;
    }
  }
}
