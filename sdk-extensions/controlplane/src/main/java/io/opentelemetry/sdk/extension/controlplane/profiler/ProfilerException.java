/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.profiler;

/**
 * Profiler 执行异常
 *
 * <p>封装 async-profiler 执行过程中的各类异常，携带错误码便于上层分类处理。
 */
public class ProfilerException extends Exception {

  private static final long serialVersionUID = 1L;

  private final String errorCode;

  public ProfilerException(String errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public ProfilerException(String errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  /** 获取错误码 */
  public String getErrorCode() {
    return errorCode;
  }
}
