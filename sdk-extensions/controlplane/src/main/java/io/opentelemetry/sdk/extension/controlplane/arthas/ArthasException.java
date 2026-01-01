/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import javax.annotation.Nullable;

/**
 * Arthas 相关异常
 *
 * <p>用于封装 Arthas 操作过程中的各种异常情况。
 */
public class ArthasException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /** 异常类型 */
  public enum Type {
    /** 初始化失败 */
    INITIALIZATION_FAILED,
    /** 启动失败 */
    START_FAILED,
    /** 停止失败 */
    STOP_FAILED,
    /** 会话创建失败 */
    SESSION_CREATE_FAILED,
    /** 会话绑定失败 */
    SESSION_BIND_FAILED,
    /** 会话关闭失败 */
    SESSION_CLOSE_FAILED,
    /** 输入转发失败 */
    INPUT_FORWARD_FAILED,
    /** 终端调整失败 */
    RESIZE_FAILED,
    /** 连接失败 */
    CONNECTION_FAILED,
    /** 反射调用失败 */
    REFLECTION_FAILED,
    /** 配置错误 */
    CONFIG_ERROR,
    /** 超时 */
    TIMEOUT,
    /** 未知错误 */
    UNKNOWN
  }

  private final Type type;
  private final boolean recoverable;

  /**
   * 创建 Arthas 异常
   *
   * @param type 异常类型
   * @param message 异常消息
   */
  public ArthasException(Type type, String message) {
    this(type, message, null, isRecoverableType(type));
  }

  /**
   * 创建 Arthas 异常
   *
   * @param type 异常类型
   * @param message 异常消息
   * @param cause 原始异常
   */
  public ArthasException(Type type, String message, @Nullable Throwable cause) {
    this(type, message, cause, isRecoverableType(type));
  }

  /**
   * 创建 Arthas 异常
   *
   * @param type 异常类型
   * @param message 异常消息
   * @param cause 原始异常
   * @param recoverable 是否可恢复
   */
  public ArthasException(
      Type type, String message, @Nullable Throwable cause, boolean recoverable) {
    super(formatMessage(type, message), cause);
    this.type = type;
    this.recoverable = recoverable;
  }

  /**
   * 获取异常类型
   *
   * @return 异常类型
   */
  public Type getType() {
    return type;
  }

  /**
   * 检查异常是否可恢复（可重试）
   *
   * @return 是否可恢复
   */
  public boolean isRecoverable() {
    return recoverable;
  }

  /**
   * 判断异常类型是否默认可恢复
   *
   * @param type 异常类型
   * @return 是否可恢复
   */
  private static boolean isRecoverableType(Type type) {
    switch (type) {
      case CONNECTION_FAILED:
      case TIMEOUT:
      case SESSION_CREATE_FAILED:
      case SESSION_BIND_FAILED:
        return true;
      case INITIALIZATION_FAILED:
      case START_FAILED:
      case CONFIG_ERROR:
      case REFLECTION_FAILED:
      default:
        return false;
    }
  }

  /**
   * 格式化异常消息
   *
   * @param type 异常类型
   * @param message 原始消息
   * @return 格式化后的消息
   */
  private static String formatMessage(Type type, String message) {
    return "[" + type.name() + "] " + message;
  }

  // ===== 便捷工厂方法 =====

  public static ArthasException initializationFailed(String message, Throwable cause) {
    return new ArthasException(Type.INITIALIZATION_FAILED, message, cause);
  }

  public static ArthasException startFailed(String message, Throwable cause) {
    return new ArthasException(Type.START_FAILED, message, cause);
  }

  public static ArthasException sessionCreateFailed(String message, @Nullable Throwable cause) {
    return new ArthasException(Type.SESSION_CREATE_FAILED, message, cause);
  }

  public static ArthasException sessionBindFailed(String message, @Nullable Throwable cause) {
    return new ArthasException(Type.SESSION_BIND_FAILED, message, cause);
  }

  public static ArthasException connectionFailed(String message, @Nullable Throwable cause) {
    return new ArthasException(Type.CONNECTION_FAILED, message, cause);
  }

  public static ArthasException reflectionFailed(String message, Throwable cause) {
    return new ArthasException(Type.REFLECTION_FAILED, message, cause);
  }

  public static ArthasException timeout(String message) {
    return new ArthasException(Type.TIMEOUT, message);
  }

  public static ArthasException configError(String message) {
    return new ArthasException(Type.CONFIG_ERROR, message);
  }
}
