/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.profiler;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneService;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.ChunkedTaskResult;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.ChunkedUploadResponse;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.ChunkUploadStatus;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 基于文件的流式结果上传器
 *
 * <p>专为文件型任务结果设计（如 profiler 输出），特点：
 * <ul>
 *   <li><b>流式读取</b>：同一时刻只有一个 chunk 在内存中，无论文件多大</li>
 *   <li><b>复用传输层</b>：直接使用 {@link ControlPlaneService#uploadChunkedResult} 上传能力</li>
 *   <li><b>不依赖外部存储</b>：文件本身即 store</li>
 *   <li><b>不做二次压缩</b>：JFR 已是压缩格式</li>
 * </ul>
 *
 * <p>未来如果有其他文件型结果（如 heap dump），可以复用本类。
 */
public final class FileStreamUploader {

  private static final Logger logger = Logger.getLogger(FileStreamUploader.class.getName());

  /** 默认分片大小：2MB */
  private static final long DEFAULT_CHUNK_SIZE = 2L * 1024 * 1024;

  /** 小文件阈值：等于 chunkSize 时走一次性上传 */
  private final ControlPlaneService service;
  private final long chunkSize;

  /**
   * 创建文件流式上传器
   *
   * @param service 控制平面服务（用于发送分片数据）
   */
  public FileStreamUploader(ControlPlaneService service) {
    this(service, DEFAULT_CHUNK_SIZE);
  }

  /**
   * 创建文件流式上传器（自定义分片大小）
   *
   * @param service 控制平面服务
   * @param chunkSize 分片大小（字节）
   */
  public FileStreamUploader(ControlPlaneService service, long chunkSize) {
    this.service = service;
    this.chunkSize = chunkSize > 0 ? chunkSize : DEFAULT_CHUNK_SIZE;
  }

  /**
   * 流式上传文件
   *
   * <p>根据文件大小自动选择上传策略：
   * <ul>
   *   <li>小文件（≤ chunkSize）：一次性读入，通过 {@code uploadChunkedResult} 单次上传</li>
   *   <li>大文件（> chunkSize）：流式分片读取，逐片上传</li>
   * </ul>
   *
   * @param taskId 任务 ID
   * @param taskType 任务类型
   * @param filePath 文件路径
   * @param contentType 内容类型
   * @param metadata 元数据（可选）
   * @return 上传结果 Future
   */
  public CompletableFuture<UploadResult> uploadFile(
      String taskId,
      String taskType,
      Path filePath,
      String contentType,
      @Nullable Map<String, String> metadata) {

    try {
      if (!Files.exists(filePath)) {
        return CompletableFuture.completedFuture(
            UploadResult.failure("File not found: " + filePath));
      }

      long fileSize = Files.size(filePath);
      if (fileSize == 0) {
        return CompletableFuture.completedFuture(
            UploadResult.failure("File is empty: " + filePath));
      }

      String uploadId = generateUploadId(taskId);
      int totalChunks = (int) Math.ceil((double) fileSize / chunkSize);

      logger.log(
          Level.INFO,
          "[FILE-UPLOAD] Starting upload: taskId={0}, file={1}, size={2}, chunks={3}",
          new Object[] {taskId, filePath.getFileName(), fileSize, totalChunks});

      String fileName = filePath.getFileName().toString();

      if (totalChunks <= 1) {
        // 小文件：一次性上传
        return uploadSingleChunk(taskId, filePath, fileSize, uploadId, fileName, contentType);
      } else {
        // 大文件：流式分片上传
        return uploadMultiChunks(
            taskId, filePath, fileSize, uploadId, totalChunks, fileName, contentType);
      }

    } catch (IOException e) {
      return CompletableFuture.completedFuture(
          UploadResult.failure("Failed to read file: " + e.getMessage()));
    }
  }

  /**
   * 小文件一次性上传
   */
  private CompletableFuture<UploadResult> uploadSingleChunk(
      String taskId,
      Path filePath,
      long fileSize,
      String uploadId,
      String fileName,
      String contentType) {

    try {
      byte[] data = Files.readAllBytes(filePath);

      ChunkedTaskResult chunk =
          buildChunkProto(taskId, uploadId, data, 0, 1, fileName, contentType);

      return service
          .uploadChunkedResult(chunk)
          .thenApply(
              response -> {
                if (isUploadSuccess(response)) {
                  logger.log(
                      Level.INFO,
                      "[FILE-UPLOAD] Single chunk upload succeeded: taskId={0}, size={1}",
                      new Object[] {taskId, fileSize});
                  return UploadResult.success(uploadId);
                } else {
                  String errorMsg = response.getErrorMessage();
                  logger.log(
                      Level.WARNING,
                      "[FILE-UPLOAD] Single chunk upload failed: taskId={0}, error={1}",
                      new Object[] {taskId, errorMsg});
                  return UploadResult.failure(
                      !errorMsg.isEmpty()
                          ? errorMsg
                          : "Upload failed with status: " + response.getStatus());
                }
              })
          .exceptionally(
              error -> {
                String msg =
                    error.getMessage() != null ? error.getMessage() : error.getClass().getName();
                logger.log(
                    Level.WARNING,
                    "[FILE-UPLOAD] Upload error: taskId={0}, error={1}",
                    new Object[] {taskId, msg});
                return UploadResult.failure("Upload error: " + msg);
              });

    } catch (IOException e) {
      return CompletableFuture.completedFuture(
          UploadResult.failure("Failed to read file: " + e.getMessage()));
    }
  }

  /**
   * 大文件流式分片上传
   *
   * <p>使用 FileChannel 逐段读取，同一时刻只有一个 chunk 在内存中。
   * 分片按顺序串行上传（保证服务端按序接收）。
   */
  private CompletableFuture<UploadResult> uploadMultiChunks(
      String taskId,
      Path filePath,
      long fileSize,
      String uploadId,
      int totalChunks,
      String fileName,
      String contentType) {

    // 串行上传分片（通过 CompletableFuture 链实现）
    CompletableFuture<UploadResult> chainFuture =
        CompletableFuture.completedFuture(UploadResult.success(uploadId));

    for (int i = 0; i < totalChunks; i++) {
      int chunkIndex = i;

      chainFuture =
          chainFuture.thenCompose(
              prevResult -> {
                if (!prevResult.isSuccess()) {
                  // 前一个 chunk 失败，快速失败
                  return CompletableFuture.completedFuture(prevResult);
                }

                try {
                  // 流式读取当前 chunk
                  byte[] chunkData = readChunk(filePath, chunkIndex, fileSize);

                  logger.log(
                      Level.FINE,
                      "[FILE-UPLOAD] Uploading chunk {0}/{1}: taskId={2}, chunkSize={3}",
                      new Object[] {
                        chunkIndex + 1, totalChunks, taskId, chunkData.length
                      });

                  ChunkedTaskResult chunk =
                      buildChunkProto(
                          taskId,
                          uploadId,
                          chunkData,
                          chunkIndex,
                          totalChunks,
                          fileName,
                          contentType);

                  return service
                      .uploadChunkedResult(chunk)
                      .thenApply(
                          response -> {
                            if (isUploadSuccess(response)) {
                              return UploadResult.success(uploadId);
                            } else {
                              String errorMsg = response.getErrorMessage();
                              return UploadResult.failure(
                                  String.format(
                                      Locale.ROOT,
                                      "Chunk %d/%d upload failed: %s",
                                      chunkIndex + 1,
                                      totalChunks,
                                      errorMsg));
                            }
                          })
                      .exceptionally(
                          error -> {
                            String msg =
                                error.getMessage() != null
                                    ? error.getMessage()
                                    : error.getClass().getName();
                            return UploadResult.failure(
                                String.format(
                                    Locale.ROOT,
                                    "Chunk %d/%d upload error: %s",
                                    chunkIndex + 1,
                                    totalChunks,
                                    msg));
                          });

                } catch (IOException e) {
                  return CompletableFuture.completedFuture(
                      UploadResult.failure(
                          String.format(
                              Locale.ROOT,
                              "Failed to read chunk %d/%d: %s",
                              chunkIndex + 1,
                              totalChunks,
                              e.getMessage())));
                }
              });
    }

    // 最终结果汇总
    return chainFuture.thenApply(
        result -> {
          if (result.isSuccess()) {
            logger.log(
                Level.INFO,
                "[FILE-UPLOAD] All {0} chunks uploaded successfully: taskId={1}",
                new Object[] {totalChunks, taskId});
          }
          return result;
        });
  }

  /**
   * 从文件中读取指定 chunk 的数据
   *
   * <p>使用 FileChannel 精确定位读取，避免读入整个文件。
   */
  private byte[] readChunk(Path filePath, int chunkIndex, long fileSize) throws IOException {
    long offset = (long) chunkIndex * chunkSize;
    int length = (int) Math.min(chunkSize, fileSize - offset);

    try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
      ByteBuffer buffer = ByteBuffer.allocate(length);
      channel.position(offset);

      int totalRead = 0;
      while (totalRead < length) {
        int bytesRead = channel.read(buffer);
        if (bytesRead == -1) {
          break;
        }
        totalRead += bytesRead;
      }

      buffer.flip();
      if (buffer.remaining() < length) {
        // 文件可能在读取过程中被截断
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
      }

      byte[] result = new byte[length];
      buffer.get(result);
      return result;
    }
  }

  /**
   * 构建 ChunkedTaskResult Protobuf 消息
   *
   * <p>仅第一个分片（chunkIndex == 0）携带 fileName 和 contentType，
   * 后续分片不重复传输元信息，以节省带宽。
   */
  private static ChunkedTaskResult buildChunkProto(
      String taskId,
      String uploadId,
      byte[] data,
      int chunkIndex,
      int totalChunks,
      String fileName,
      String contentType) {

    ChunkedTaskResult.Builder builder =
        ChunkedTaskResult.newBuilder()
            .setTaskId(taskId)
            .setUploadId(uploadId)
            .setChunkIndex(chunkIndex)
            .setTotalChunks(totalChunks)
            .setChunkData(ByteString.copyFrom(data))
            .setIsLastChunk(chunkIndex == totalChunks - 1);

    // 仅第一片携带文件元信息
    if (chunkIndex == 0) {
      if (fileName != null && !fileName.isEmpty()) {
        builder.setFileName(fileName);
      }
      if (contentType != null && !contentType.isEmpty()) {
        builder.setContentType(contentType);
      }
    }

    return builder.build();
  }

  /**
   * 判断上传响应是否成功
   */
  private static boolean isUploadSuccess(ChunkedUploadResponse response) {
    return response.getStatus() == ChunkUploadStatus.CHUNK_UPLOAD_STATUS_CHUNK_RECEIVED
        || response.getStatus() == ChunkUploadStatus.CHUNK_UPLOAD_STATUS_UPLOAD_COMPLETE;
  }

  /**
   * 生成上传 ID
   */
  private static String generateUploadId(String taskId) {
    return taskId + "-" + UUID.randomUUID().toString().substring(0, 8);
  }

  // ===== 上传结果 =====

  /**
   * 文件上传结果
   */
  public static final class UploadResult {

    private final boolean success;
    @Nullable private final String uploadId;
    @Nullable private final String failureReason;

    private UploadResult(
        boolean success, @Nullable String uploadId, @Nullable String failureReason) {
      this.success = success;
      this.uploadId = uploadId;
      this.failureReason = failureReason;
    }

    public static UploadResult success(String uploadId) {
      return new UploadResult(/* success= */ true, uploadId, /* failureReason= */ null);
    }

    public static UploadResult failure(String reason) {
      return new UploadResult(/* success= */ false, /* uploadId= */ null, reason);
    }

    public boolean isSuccess() {
      return success;
    }

    @Nullable
    public String getUploadId() {
      return uploadId;
    }

    @Nullable
    public String getFailureReason() {
      return failureReason;
    }

    @Override
    public String toString() {
      if (success) {
        return "UploadResult{success=true, uploadId='" + uploadId + "'}";
      }
      return "UploadResult{success=false, reason='" + failureReason + "'}";
    }
  }
}
