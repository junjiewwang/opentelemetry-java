/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.codec;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Protobuf 编解码器
 *
 * <p>统一的序列化/反序列化入口，所有传输层都使用此编码器。
 *
 * <p>特性：
 * <ul>
 *   <li>类型安全的编解码
 *   <li>统一的错误处理和日志
 *   <li>支持安全解码（返回 null 而非抛出异常）
 * </ul>
 */
public final class ProtobufCodec {

  private static final Logger logger = Logger.getLogger(ProtobufCodec.class.getName());

  private ProtobufCodec() {}

  /**
   * 编码 Protobuf 消息为字节数组
   *
   * @param message 要编码的消息
   * @return 编码后的字节数组
   */
  public static byte[] encode(Message message) {
    return message.toByteArray();
  }

  /**
   * 解码字节数组为 Protobuf 消息
   *
   * @param data 要解码的字节数组
   * @param parser 消息解析器
   * @param <T> 消息类型
   * @return 解码后的消息
   * @throws InvalidProtocolBufferException 如果解码失败
   */
  public static <T extends Message> T decode(byte[] data, Parser<T> parser)
      throws InvalidProtocolBufferException {
    return parser.parseFrom(data);
  }

  /**
   * 安全解码字节数组为 Protobuf 消息
   *
   * <p>如果解码失败，返回 null 而非抛出异常，并记录警告日志。
   *
   * @param data 要解码的字节数组
   * @param parser 消息解析器
   * @param messageType 消息类型名称（用于日志）
   * @param <T> 消息类型
   * @return 解码后的消息，解码失败时返回 null
   */
  @Nullable
  public static <T extends Message> T decodeSafe(
      byte[] data, Parser<T> parser, String messageType) {
    try {
      return parser.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      logger.log(
          Level.WARNING,
          "[CODEC-ERROR] Failed to parse Protobuf message: type={0}, error={1}, dataLength={2}",
          new Object[] {messageType, e.getMessage(), data != null ? data.length : 0});
      return null;
    }
  }

  /**
   * 安全解码字节数组为 Protobuf 消息（带默认值）
   *
   * <p>如果解码失败，返回指定的默认值而非抛出异常，并记录警告日志。
   *
   * @param data 要解码的字节数组
   * @param parser 消息解析器
   * @param defaultValue 解码失败时返回的默认值
   * @param messageType 消息类型名称（用于日志）
   * @param <T> 消息类型
   * @return 解码后的消息，解码失败时返回默认值
   */
  public static <T extends Message> T decodeOrDefault(
      byte[] data, Parser<T> parser, T defaultValue, String messageType) {
    T result = decodeSafe(data, parser, messageType);
    return result != null ? result : defaultValue;
  }

  /**
   * 检查字节数组是否为有效的 Protobuf 消息
   *
   * @param data 要检查的字节数组
   * @param parser 消息解析器
   * @param <T> 消息类型
   * @return 如果是有效的 Protobuf 消息返回 true
   */
  public static <T extends Message> boolean isValid(byte[] data, Parser<T> parser) {
    if (data == null || data.length == 0) {
      return false;
    }
    try {
      parser.parseFrom(data);
      return true;
    } catch (InvalidProtocolBufferException e) {
      return false;
    }
  }
}
