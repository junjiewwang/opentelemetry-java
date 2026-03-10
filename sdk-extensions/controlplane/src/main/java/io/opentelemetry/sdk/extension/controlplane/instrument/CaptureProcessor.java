/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 采集数据安全提取处理器
 *
 * <p>在运行时从方法参数和返回值中安全地提取数据，设置为 Span Attribute。
 * 所有方法均有完善的异常保护，确保不会因为数据提取失败而影响目标方法的执行。
 *
 * <p>设计为静态方法容器，线程安全。
 *
 * @see CaptureConfig
 */
final class CaptureProcessor {

  private static final Logger logger =
      DynamicInstrumentLogger.getLogger("capture-processor");

  /** Span Attribute key 前缀 */
  static final String ATTR_PREFIX_ARGS = "code.function.args.";
  static final String ATTR_PREFIX_RETURN = "code.function.return";

  private CaptureProcessor() {}

  /**
   * 将对象安全转换为字符串
   *
   * <p>处理 null、toString() 异常、超长截断等边界情况。
   *
   * @param obj 要转换的对象（可能为 null）
   * @param maxLength 最大长度，超过将被截断
   * @return 字符串表示
   */
  static String safeToString(@Nullable Object obj, int maxLength) {
    if (obj == null) {
      return "null";
    }
    try {
      String str = obj.toString();
      if (str == null) {
        return "null";
      }
      if (str.length() > maxLength) {
        return str.substring(0, maxLength) + "...(truncated)";
      }
      return str;
    } catch (Throwable e) {
      // toString() 可能抛出任何异常
      return "<error:" + e.getClass().getSimpleName() + ">";
    }
  }

  /**
   * 通过反射从对象中提取指定字段的值
   *
   * <p>优先使用 getter 方法（getXxx() / isXxx()），fallback 到 public field。
   *
   * @param obj 目标对象（可能为 null）
   * @param fieldName 字段名称
   * @return 字段值，提取失败返回 null
   */
  @Nullable
  static Object extractField(@Nullable Object obj, String fieldName) {
    if (obj == null || fieldName == null || fieldName.isEmpty()) {
      return null;
    }

    try {
      // 1. 尝试 getter 方法：getXxx()
      String getterName = "get" + Character.toUpperCase(fieldName.charAt(0))
          + fieldName.substring(1);
      try {
        Method getter = obj.getClass().getMethod(getterName);
        return getter.invoke(obj);
      } catch (NoSuchMethodException ignored) {
        // getter 不存在，继续尝试
      }

      // 2. 尝试 boolean getter：isXxx()
      String isGetterName = "is" + Character.toUpperCase(fieldName.charAt(0))
          + fieldName.substring(1);
      try {
        Method isGetter = obj.getClass().getMethod(isGetterName);
        return isGetter.invoke(obj);
      } catch (NoSuchMethodException ignored) {
        // is getter 不存在，继续尝试
      }

      // 3. 尝试直接同名方法（如 Kotlin 的属性访问器）
      try {
        Method directMethod = obj.getClass().getMethod(fieldName);
        return directMethod.invoke(obj);
      } catch (NoSuchMethodException ignored) {
        // 直接方法不存在，继续尝试
      }

      // 4. Fallback 到 public field
      try {
        Field field = obj.getClass().getField(fieldName);
        return field.get(obj);
      } catch (NoSuchFieldException ignored) {
        // public field 也不存在
      }

      // 5. 最后尝试 declared field（包括 private）
      try {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(obj);
      } catch (NoSuchFieldException ignored) {
        // declared field 也不存在
      }

      logger.log(Level.FINE,
          "[CAPTURE-PROCESSOR] Field ''{0}'' not found in class {1}",
          new Object[] {fieldName, obj.getClass().getName()});
      return null;

    } catch (Throwable e) {
      logger.log(Level.FINE,
          "[CAPTURE-PROCESSOR] Failed to extract field ''{0}'' from {1}: {2}",
          new Object[] {fieldName, obj.getClass().getName(), e.getMessage()});
      return null;
    }
  }
}
