/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.transport;

import javax.annotation.Nullable;

/**
 * 传输层异常
 *
 * <p>封装传输层（HTTP/gRPC）发生的异常，提供统一的错误处理接口。
 */
public class TransportException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /** HTTP 状态码（仅 HTTP 传输时有效） */
  private final int httpStatusCode;

  /** gRPC 状态码（仅 gRPC 传输时有效） */
  @Nullable private final String grpcStatusCode;

  /** 操作类型 */
  @Nullable private final Transport.Operation operation;

  /**
   * 创建传输异常
   *
   * @param message 错误信息
   */
  public TransportException(String message) {
    super(message);
    this.httpStatusCode = 0;
    this.grpcStatusCode = null;
    this.operation = null;
  }

  /**
   * 创建传输异常（带原因）
   *
   * @param message 错误信息
   * @param cause 原因
   */
  public TransportException(String message, Throwable cause) {
    super(message, cause);
    this.httpStatusCode = 0;
    this.grpcStatusCode = null;
    this.operation = null;
  }

  /**
   * 创建 HTTP 传输异常
   *
   * @param message 错误信息
   * @param httpStatusCode HTTP 状态码
   * @param operation 操作类型
   */
  public TransportException(String message, int httpStatusCode, Transport.Operation operation) {
    super(message);
    this.httpStatusCode = httpStatusCode;
    this.grpcStatusCode = null;
    this.operation = operation;
  }

  /**
   * 创建 gRPC 传输异常
   *
   * @param message 错误信息
   * @param grpcStatusCode gRPC 状态码
   * @param operation 操作类型
   */
  public TransportException(
      String message, String grpcStatusCode, Transport.Operation operation) {
    super(message);
    this.httpStatusCode = 0;
    this.grpcStatusCode = grpcStatusCode;
    this.operation = operation;
  }

  /**
   * 获取 HTTP 状态码
   *
   * @return HTTP 状态码，如果不是 HTTP 错误则返回 0
   */
  public int getHttpStatusCode() {
    return httpStatusCode;
  }

  /**
   * 获取 gRPC 状态码
   *
   * @return gRPC 状态码，如果不是 gRPC 错误则返回 null
   */
  @Nullable
  public String getGrpcStatusCode() {
    return grpcStatusCode;
  }

  /**
   * 获取操作类型
   *
   * @return 操作类型，可能为 null
   */
  @Nullable
  public Transport.Operation getOperation() {
    return operation;
  }

  /**
   * 检查是否为 HTTP 错误
   *
   * @return 如果是 HTTP 错误返回 true
   */
  public boolean isHttpError() {
    return httpStatusCode > 0;
  }

  /**
   * 检查是否为 gRPC 错误
   *
   * @return 如果是 gRPC 错误返回 true
   */
  public boolean isGrpcError() {
    return grpcStatusCode != null;
  }

  /**
   * 检查是否为可重试错误
   *
   * <p>以下情况认为可重试：
   * <ul>
   *   <li>HTTP 5xx 服务器错误
   *   <li>HTTP 429 Too Many Requests
   *   <li>gRPC UNAVAILABLE
   *   <li>网络连接错误
   * </ul>
   *
   * @return 如果可重试返回 true
   */
  public boolean isRetryable() {
    if (httpStatusCode >= 500 || httpStatusCode == 429) {
      return true;
    }
    if ("UNAVAILABLE".equals(grpcStatusCode) || "RESOURCE_EXHAUSTED".equals(grpcStatusCode)) {
      return true;
    }
    // 网络错误通常可重试
    Throwable cause = getCause();
    return cause instanceof java.net.SocketException
        || cause instanceof java.net.ConnectException
        || cause instanceof java.net.SocketTimeoutException;
  }
}
