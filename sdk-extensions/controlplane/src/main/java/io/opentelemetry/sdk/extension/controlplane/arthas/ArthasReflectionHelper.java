/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 反射工具类
 *
 * <p>提供反射调用 Arthas API 的便捷方法，包括：
 * <ul>
 *   <li>方法缓存，提升反射性能
 *   <li>统一的异常处理
 *   <li>类型安全的调用封装
 * </ul>
 */
public final class ArthasReflectionHelper {

  private static final Logger logger = Logger.getLogger(ArthasReflectionHelper.class.getName());

  // 方法缓存，key 格式为 "className#methodName#paramTypes"
  private static final Map<String, Method> methodCache = new ConcurrentHashMap<>();

  // Arthas 类名常量
  public static final String ARTHAS_BOOTSTRAP_CLASS =
      "com.taobao.arthas.core.server.ArthasBootstrap";
  public static final String SHELL_SERVER_CLASS = "com.taobao.arthas.core.shell.ShellServer";
  public static final String SHELL_SESSION_CLASS = "com.taobao.arthas.core.shell.session.Session";
  public static final String TERM_CLASS = "com.taobao.arthas.core.shell.term.Term";
  public static final String HANDLER_CLASS = "io.termd.core.function.Consumer";

  private ArthasReflectionHelper() {
    // 工具类不允许实例化
  }

  /**
   * 查找方法（带缓存）
   *
   * @param clazz 类
   * @param methodName 方法名
   * @param parameterTypes 参数类型
   * @return 方法，如果未找到返回 null
   */
  @Nullable
  public static Method findMethod(Class<?> clazz, String methodName, Class<?>... parameterTypes) {
    String cacheKey = buildCacheKey(clazz, methodName, parameterTypes);
    return methodCache.computeIfAbsent(
        cacheKey,
        key -> {
          try {
            return clazz.getMethod(methodName, parameterTypes);
          } catch (NoSuchMethodException e) {
            // 尝试查找任意参数签名的方法
            return findMethodByName(clazz, methodName);
          }
        });
  }

  /**
   * 根据方法名查找方法（忽略参数类型）
   *
   * @param clazz 类
   * @param methodName 方法名
   * @return 找到的第一个匹配方法，如果未找到返回 null
   */
  @Nullable
  public static Method findMethodByName(Class<?> clazz, String methodName) {
    for (Method method : clazz.getMethods()) {
      if (method.getName().equals(methodName)) {
        return method;
      }
    }
    return null;
  }

  /**
   * 安全地调用方法
   *
   * @param target 目标对象（静态方法传 null）
   * @param method 方法
   * @param args 参数
   * @return 返回值
   * @throws ArthasException 如果调用失败
   */
  @Nullable
  public static Object invokeMethod(@Nullable Object target, Method method, Object... args) {
    try {
      return method.invoke(target, args);
    } catch (IllegalAccessException e) {
      throw ArthasException.reflectionFailed(
          "Cannot access method: " + method.getName(), e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw ArthasException.reflectionFailed(
          "Method invocation failed: " + method.getName(), cause != null ? cause : e);
    }
  }

  /**
   * 安全地调用方法（忽略异常）
   *
   * @param target 目标对象
   * @param method 方法
   * @param args 参数
   * @return 返回值，如果调用失败返回 null
   */
  @Nullable
  public static Object invokeMethodQuietly(@Nullable Object target, Method method, Object... args) {
    try {
      return method.invoke(target, args);
    } catch (ReflectiveOperationException e) {
      logger.log(Level.FINE, "Method invocation failed: " + method.getName(), e);
      return null;
    }
  }

  /**
   * 获取属性值
   *
   * @param target 目标对象
   * @param propertyName 属性名（会自动添加 get 前缀）
   * @return 属性值
   */
  @Nullable
  public static Object getProperty(Object target, String propertyName) {
    String getterName = "get" + capitalize(propertyName);
    Method getter = findMethod(target.getClass(), getterName);
    if (getter != null) {
      return invokeMethodQuietly(target, getter);
    }
    return null;
  }

  /**
   * 获取 int 属性值
   *
   * @param target 目标对象
   * @param propertyName 属性名
   * @param defaultValue 默认值
   * @return 属性值
   */
  public static int getIntProperty(Object target, String propertyName, int defaultValue) {
    Object value = getProperty(target, propertyName);
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    return defaultValue;
  }

  /**
   * 获取 String 属性值
   *
   * @param target 目标对象
   * @param propertyName 属性名
   * @return 属性值
   */
  @Nullable
  public static String getStringProperty(Object target, String propertyName) {
    Object value = getProperty(target, propertyName);
    return value != null ? value.toString() : null;
  }

  /**
   * 检查类是否存在
   *
   * @param className 类名
   * @param classLoader 类加载器
   * @return 是否存在
   */
  public static boolean isClassAvailable(String className, ClassLoader classLoader) {
    try {
      Class.forName(className, false, classLoader);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * 加载类（不初始化）
   *
   * @param className 类名
   * @param classLoader 类加载器
   * @return 类对象
   * @throws ArthasException 如果类不存在
   */
  public static Class<?> loadClass(String className, ClassLoader classLoader) {
    try {
      return Class.forName(className, false, classLoader);
    } catch (ClassNotFoundException e) {
      throw ArthasException.reflectionFailed("Class not found: " + className, e);
    }
  }

  /**
   * 尝试加载类
   *
   * @param className 类名
   * @param classLoader 类加载器
   * @return 类对象，如果加载失败返回 null
   */
  @Nullable
  public static Class<?> tryLoadClass(String className, ClassLoader classLoader) {
    try {
      return Class.forName(className, false, classLoader);
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  /**
   * 构建方法缓存 key
   *
   * @param clazz 类
   * @param methodName 方法名
   * @param parameterTypes 参数类型
   * @return 缓存 key
   */
  private static String buildCacheKey(
      Class<?> clazz, String methodName, Class<?>... parameterTypes) {
    StringBuilder sb = new StringBuilder();
    sb.append(clazz.getName()).append("#").append(methodName);
    for (Class<?> type : parameterTypes) {
      sb.append("#").append(type.getName());
    }
    return sb.toString();
  }

  /**
   * 首字母大写
   *
   * @param str 字符串
   * @return 首字母大写的字符串
   */
  private static String capitalize(String str) {
    if (str == null || str.isEmpty()) {
      return str;
    }
    return Character.toUpperCase(str.charAt(0)) + str.substring(1);
  }

  /** 清除方法缓存（主要用于测试） */
  public static void clearCache() {
    methodCache.clear();
  }
}
