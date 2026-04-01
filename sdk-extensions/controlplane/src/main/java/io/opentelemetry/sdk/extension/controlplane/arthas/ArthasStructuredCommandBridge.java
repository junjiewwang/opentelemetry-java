/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import io.opentelemetry.sdk.extension.controlplane.util.JsonUtils;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 结构化命令桥接层。
 *
 * <p>通过反射复用 Arthas 内部 {@code CommandExecutorImpl}，并在 Arthas ClassLoader 内完成 JSON 序列化，
 * 避免 Arthas 内部类型泄漏到业务层。
 */
public final class ArthasStructuredCommandBridge {

  private static final Logger logger =
      Logger.getLogger(ArthasStructuredCommandBridge.class.getName());

  private static final String SESSION_MANAGER_CLASS =
      "com.taobao.arthas.core.shell.session.SessionManager";
  private static final String COMMAND_EXECUTOR_IMPL_CLASS =
      "com.taobao.arthas.core.command.CommandExecutorImpl";
  private static final String[] FASTJSON2_JSON_CLASSES = {
    "com.alibaba.fastjson2.JSON", "com.alibaba.arthas.deps.com.alibaba.fastjson2.JSON"
  };

  private final ArthasBootstrap arthasBootstrap;

  @Nullable private volatile ExecutorHandle cachedHandle;

  public ArthasStructuredCommandBridge(ArthasBootstrap arthasBootstrap) {
    this.arthasBootstrap = Objects.requireNonNull(arthasBootstrap, "arthasBootstrap");
  }

  public StructuredExecResult executeSync(
      String command,
      long timeoutMillis,
      @Nullable String sessionId,
      @Nullable Object authSubject,
      @Nullable String userId) {
    long initStart = System.nanoTime();
    ExecutorHandle handle = ensureHandle();
    long initCost = nanosToMillis(System.nanoTime() - initStart);

    Object rawResult;
    long invokeStart = System.nanoTime();
    try {
      rawResult =
          handle.executeSyncMethod.invoke(
              handle.commandExecutor, command, timeoutMillis, sessionId, authSubject, userId);
    } catch (IllegalAccessException e) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED,
          "调用 Arthas executeSync 失败",
          e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw arthasFailure(
          classifyInvokeError(cause),
          "调用 Arthas executeSync 失败: " + safeMessage(cause),
          cause);
    }
    long invokeCost = nanosToMillis(System.nanoTime() - invokeStart);

    String rawJson;
    long serializationStart = System.nanoTime();
    try {
      rawJson = handle.toJson(rawResult);
    } catch (IllegalAccessException e) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_JSON_SERIALIZATION_FAILED,
          "Arthas 结果 JSON 序列化失败",
          e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_JSON_SERIALIZATION_FAILED,
          "Arthas 结果 JSON 序列化失败: " + safeMessage(cause),
          cause);
    }
    long serializationCost = nanosToMillis(System.nanoTime() - serializationStart);

    Map<String, Object> payload = JsonUtils.parseSimpleObject(rawJson);
    if (payload.isEmpty() && rawJson != null && !rawJson.trim().isEmpty() && !"{}".equals(rawJson.trim())) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_JSON_PARSE_FAILED,
          "Arthas 结构化结果 JSON 解析失败",
          null);
    }

    boolean success = toBoolean(payload.get(ArthasTaskProtocol.ResultField.SUCCESS), true);
    boolean timeout = toBoolean(payload.get(ArthasTaskProtocol.ResultField.TIMEOUT), false);
    String resolvedSessionId =
        firstNonBlank(
            toNullableString(payload.get(ArthasTaskProtocol.ResultField.SESSION_ID)), sessionId);
    String errorCode =
        firstNonBlank(
            toNullableString(payload.get(ArthasTaskProtocol.ResultField.ERROR_CODE)),
            timeout ? ArthasTaskProtocol.ErrorCode.COMMAND_TIMEOUT : null);
    String errorMessage =
        firstNonBlank(
            toNullableString(payload.get(ArthasTaskProtocol.ResultField.ERROR_MESSAGE)),
            success ? null : "Arthas command execution failed");

    logger.log(
        Level.FINE,
        "[ARTHAS-EXEC-SYNC] Bridge executeSync completed: command={0}, success={1}, timeout={2}, initMs={3}, invokeMs={4}, jsonMs={5}",
        new Object[] {command, success, timeout, initCost, invokeCost, serializationCost});

    return StructuredExecResult.builder(command)
        .success(success)
        .timeout(timeout)
        .sessionId(resolvedSessionId)
        .errorCode(errorCode)
        .errorMessage(errorMessage)
        .payload(payload)
        .rawJson(rawJson)
        .bridgeInitTimeMillis(initCost)
        .invokeTimeMillis(invokeCost)
        .serializationTimeMillis(serializationCost)
        .build();
  }

  private ExecutorHandle ensureHandle() {
    ExecutorHandle local = cachedHandle;
    if (local != null && local.isStillBoundTo(arthasBootstrap)) {
      return local;
    }

    synchronized (this) {
      local = cachedHandle;
      if (local != null && local.isStillBoundTo(arthasBootstrap)) {
        return local;
      }
      ExecutorHandle rebuilt = buildHandle();
      cachedHandle = rebuilt;
      return rebuilt;
    }
  }

  private ExecutorHandle buildHandle() {
    ClassLoader arthasCl = arthasBootstrap.getArthasClassLoader();
    if (arthasCl == null) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.ARTHAS_CLASSLOADER_UNAVAILABLE,
          "Arthas ClassLoader 不存在",
          null);
    }

    Object bootstrapInstance = arthasBootstrap.getBootstrapInstance();
    if (bootstrapInstance == null) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.ARTHAS_BOOTSTRAP_UNAVAILABLE,
          "Arthas Bootstrap 实例不存在",
          null);
    }

    try {
      Method getSessionManagerMethod = bootstrapInstance.getClass().getMethod("getSessionManager");
      Object sessionManager = getSessionManagerMethod.invoke(bootstrapInstance);
      if (sessionManager == null) {
        throw arthasFailure(
            ArthasTaskProtocol.ErrorCode.SESSION_MANAGER_UNAVAILABLE,
            "Arthas SessionManager 不存在",
            null);
      }

      Class<?> sessionManagerClass =
          loadRequiredClass(arthasCl, "SessionManager", SESSION_MANAGER_CLASS);
      Class<?> executorImplClass =
          loadRequiredClass(arthasCl, "CommandExecutorImpl", COMMAND_EXECUTOR_IMPL_CLASS);
      Class<?> jsonClass = loadRequiredClass(arthasCl, "fastjson2 JSON", FASTJSON2_JSON_CLASSES);

      Constructor<?> ctor = executorImplClass.getConstructor(sessionManagerClass);
      Object commandExecutor = ctor.newInstance(sessionManager);
      Method executeSyncMethod =
          executorImplClass.getMethod(
              "executeSync", String.class, long.class, String.class, Object.class, String.class);
      Method toJsonMethod = jsonClass.getMethod("toJSONString", Object.class);

      logger.log(Level.FINE, "[ARTHAS-EXEC-SYNC] CommandExecutorImpl handle initialized successfully");
      return new ExecutorHandle(
          arthasCl,
          bootstrapInstance,
          sessionManager,
          commandExecutor,
          executeSyncMethod,
          toJsonMethod);
    } catch (NoSuchMethodException e) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTOR_INIT_FAILED,
          "初始化 Arthas CommandExecutorImpl 失败: 方法签名不存在",
          e);
    } catch (ClassNotFoundException e) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTOR_INIT_FAILED,
          "初始化 Arthas CommandExecutorImpl 失败: " + safeMessage(e),
          e);
    } catch (InstantiationException e) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTOR_INIT_FAILED,
          "初始化 Arthas CommandExecutorImpl 失败: 无法实例化",
          e);
    } catch (IllegalAccessException e) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTOR_INIT_FAILED,
          "初始化 Arthas CommandExecutorImpl 失败: 无法访问目标成员",
          e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTOR_INIT_FAILED,
          "初始化 Arthas CommandExecutorImpl 失败: " + safeMessage(cause),
          cause);
    }
  }

  static Class<?> loadRequiredClass(
      ClassLoader classLoader, String componentName, String... candidateClassNames)
      throws ClassNotFoundException {
    ClassNotFoundException last = null;
    for (String candidateClassName : candidateClassNames) {
      try {
        return classLoader.loadClass(candidateClassName);
      } catch (ClassNotFoundException e) {
        last = e;
      }
    }

    String message =
        componentName
            + " 类不存在，可选类名="
            + Arrays.toString(candidateClassNames);
    throw last != null ? new ClassNotFoundException(message, last) : new ClassNotFoundException(message);
  }

  private static long nanosToMillis(long nanos) {
    return nanos / 1_000_000L;
  }

  private static boolean toBoolean(@Nullable Object value, boolean defaultValue) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    }
    return defaultValue;
  }

  @Nullable
  private static String toNullableString(@Nullable Object value) {
    if (value == null) {
      return null;
    }
    String s = String.valueOf(value);
    return s.isEmpty() ? null : s;
  }

  @Nullable
  private static String firstNonBlank(@Nullable String first, @Nullable String second) {
    if (first != null && !first.trim().isEmpty()) {
      return first;
    }
    return second;
  }

  private static String classifyInvokeError(Throwable throwable) {
    if (isTimeoutThrowable(throwable)) {
      return ArthasTaskProtocol.ErrorCode.COMMAND_TIMEOUT;
    }
    return ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED;
  }

  private static boolean isTimeoutThrowable(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof java.util.concurrent.TimeoutException) {
        return true;
      }
      String message = current.getMessage();
      if (message != null && message.toLowerCase(java.util.Locale.ROOT).contains("timeout")) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static String safeMessage(Throwable throwable) {
    String message = throwable.getMessage();
    return message != null && !message.trim().isEmpty()
        ? message
        : throwable.getClass().getSimpleName();
  }

  private static IllegalStateException arthasFailure(
      String errorCode, String message, @Nullable Throwable cause) {
    String fullMessage = errorCode + ": " + message;
    return cause != null ? new IllegalStateException(fullMessage, cause) : new IllegalStateException(fullMessage);
  }

  private static final class ExecutorHandle {
    private final ClassLoader arthasClassLoader;
    private final Object bootstrapInstance;
    @SuppressWarnings("unused")
    private final Object sessionManager;
    private final Object commandExecutor;
    private final Method executeSyncMethod;
    private final Method toJsonMethod;

    private ExecutorHandle(
        ClassLoader arthasClassLoader,
        Object bootstrapInstance,
        Object sessionManager,
        Object commandExecutor,
        Method executeSyncMethod,
        Method toJsonMethod) {
      this.arthasClassLoader = arthasClassLoader;
      this.bootstrapInstance = bootstrapInstance;
      this.sessionManager = sessionManager;
      this.commandExecutor = commandExecutor;
      this.executeSyncMethod = executeSyncMethod;
      this.toJsonMethod = toJsonMethod;
    }

    private boolean isStillBoundTo(ArthasBootstrap bootstrap) {
      return arthasClassLoader == bootstrap.getArthasClassLoader()
          && bootstrapInstance == bootstrap.getBootstrapInstance();
    }

    private String toJson(@Nullable Object value)
        throws InvocationTargetException, IllegalAccessException {
      Object json = toJsonMethod.invoke(null, value);
      return json != null ? String.valueOf(json) : JsonUtils.toJsonString(Collections.emptyMap());
    }
  }
}
