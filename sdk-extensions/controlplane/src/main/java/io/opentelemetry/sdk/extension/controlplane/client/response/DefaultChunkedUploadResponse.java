/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.response;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.ChunkedUploadResponse;

/**
 * 默认分片上传响应实现
 *
 * <p>供 HTTP 和 gRPC 客户端共享使用
 */
public final class DefaultChunkedUploadResponse implements ChunkedUploadResponse {
  private final boolean success;
  private final String uploadId;
  private final int receivedChunkIndex;
  private final String status;
  private final String errorMessage;

  /**
   * 创建分片上传响应
   */
  public DefaultChunkedUploadResponse(
      boolean success,
      String uploadId,
      int receivedChunkIndex,
      String status,
      String errorMessage) {
    this.success = success;
    this.uploadId = uploadId != null ? uploadId : "";
    this.receivedChunkIndex = receivedChunkIndex;
    this.status = status != null ? status : "";
    this.errorMessage = errorMessage != null ? errorMessage : "";
  }

  /**
   * 创建错误响应
   *
   * @param errorMessage 错误信息
   * @return 错误响应实例
   */
  public static ChunkedUploadResponse error(String errorMessage) {
    return new DefaultChunkedUploadResponse(
        /* success= */ false, "", -1, "FAILED", errorMessage);
  }

  @Override
  public boolean isSuccess() {
    return success;
  }

  @Override
  public String getUploadId() {
    return uploadId;
  }

  @Override
  public int getReceivedChunkIndex() {
    return receivedChunkIndex;
  }

  @Override
  public String getStatus() {
    return status;
  }

  @Override
  public String getErrorMessage() {
    return errorMessage;
  }
}
