/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.result;

import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

/**
 * 任务结果上传接口
 *
 * <p>负责将结果上传到服务端：
 * <ul>
 *   <li>直接上传（小文件）
 *   <li>分片上传（大文件）
 * </ul>
 *
 * <p>该接口不负责重试逻辑，重试由 {@link TaskResultLifecycleService} 结合
 * {@link TaskResultRetryPolicy} 控制。
 */
public interface TaskResultUploader {

  /**
   * 上传结果
   *
   * @param descriptor 结果描述符
   * @param data 结果数据
   * @return 上传结果 Future
   */
  CompletableFuture<UploadResult> upload(TaskResultDescriptor descriptor, byte[] data);

  /**
   * 上传分片
   *
   * <p>用于分片上传场景，上传单个分片
   *
   * @param descriptor 结果描述符（包含分片信息）
   * @param chunkData 分片数据
   * @return 上传结果 Future
   */
  CompletableFuture<UploadResult> uploadChunk(TaskResultDescriptor descriptor, byte[] chunkData);

  /**
   * 完成分片上传
   *
   * <p>在所有分片上传完成后调用，通知服务端合并分片
   *
   * @param descriptor 结果描述符
   * @return 上传结果 Future
   */
  CompletableFuture<UploadResult> completeChunkedUpload(TaskResultDescriptor descriptor);

  /**
   * 取消分片上传
   *
   * <p>在上传失败或放弃时调用，清理服务端临时数据
   *
   * @param descriptor 结果描述符
   * @return 是否成功取消
   */
  CompletableFuture<Boolean> abortChunkedUpload(TaskResultDescriptor descriptor);

  /**
   * 上传结果
   */
  final class UploadResult {

    private final boolean success;
    @Nullable private final String uploadId;
    @Nullable private final String failureReason;
    private final int httpStatusCode;

    private UploadResult(
        boolean success,
        @Nullable String uploadId,
        @Nullable String failureReason,
        int httpStatusCode) {
      this.success = success;
      this.uploadId = uploadId;
      this.failureReason = failureReason;
      this.httpStatusCode = httpStatusCode;
    }

    /**
     * 创建成功结果
     */
    public static UploadResult success() {
      return new UploadResult(/* success= */ true, /* uploadId= */ null, /* failureReason= */ null, /* httpStatusCode= */ 200);
    }

    /**
     * 创建成功结果（带上传 ID，用于分片上传）
     */
    public static UploadResult success(String uploadId) {
      return new UploadResult(/* success= */ true, uploadId, /* failureReason= */ null, /* httpStatusCode= */ 200);
    }

    /**
     * 创建失败结果
     */
    public static UploadResult failure(String reason) {
      return new UploadResult(/* success= */ false, /* uploadId= */ null, reason, /* httpStatusCode= */ 0);
    }

    /**
     * 创建失败结果（带 HTTP 状态码）
     */
    public static UploadResult failure(String reason, int httpStatusCode) {
      return new UploadResult(/* success= */ false, /* uploadId= */ null, reason, httpStatusCode);
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

    public int getHttpStatusCode() {
      return httpStatusCode;
    }

    /**
     * 判断是否为可重试的失败
     *
     * <p>以下情况认为可重试：
     * <ul>
     *   <li>网络错误（httpStatusCode == 0）
     *   <li>服务端临时错误（5xx）
     *   <li>请求限流（429）
     * </ul>
     */
    public boolean isRetryable() {
      if (success) {
        return false;
      }
      // 网络错误
      if (httpStatusCode == 0) {
        return true;
      }
      // 服务端错误
      if (httpStatusCode >= 500 && httpStatusCode < 600) {
        return true;
      }
      // 限流
      return httpStatusCode == 429;
    }

    @Override
    public String toString() {
      if (success) {
        return "UploadResult{success=true, uploadId='" + uploadId + "'}";
      }
      return "UploadResult{success=false, reason='" + failureReason
          + "', httpStatus=" + httpStatusCode + '}';
    }
  }
}
