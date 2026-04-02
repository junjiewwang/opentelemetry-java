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
import java.util.Locale;
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
  private static final String SESSION_CLASS = "com.taobao.arthas.core.shell.session.Session";
  private static final String JOB_CLASS = "com.taobao.arthas.core.shell.system.Job";
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

    if (sessionId != null) {
      configureSessionContext(handle, sessionId, authSubject, userId);
    }

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

    return buildSyncResult(rawResult, command, sessionId, initCost, invokeCost);
  }

  public StructuredAsyncResult openSession(
      @Nullable String userId, @Nullable Object authSubject) {
    ExecutorHandle handle = ensureHandle();
    long initCost = handle.lastInitCostMillis;
    Object rawResult;
    long invokeStart = System.nanoTime();
    try {
      rawResult = handle.createSessionMethod.invoke(handle.commandExecutor);
    } catch (IllegalAccessException e) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED,
          "调用 Arthas createSession 失败",
          e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED,
          "调用 Arthas createSession 失败: " + safeMessage(cause),
          cause);
    }
    long invokeCost = nanosToMillis(System.nanoTime() - invokeStart);

    StructuredAsyncResult result = buildAsyncResult(rawResult, initCost, invokeCost);
    if (!result.isSuccess()) {
      return result;
    }
    String sessionId = result.getSessionId();
    if (sessionId != null) {
      configureSessionContext(handle, sessionId, authSubject, userId);
    }
    return result;
  }

  public StructuredAsyncResult executeAsync(
      String command,
      String sessionId,
      @Nullable String userId,
      @Nullable Object authSubject) {
    ExecutorHandle handle = ensureHandle();
    long initCost = handle.lastInitCostMillis;
    configureSessionContext(handle, sessionId, authSubject, userId);
    Object rawResult;
    long invokeStart = System.nanoTime();
    try {
      rawResult = handle.executeAsyncMethod.invoke(handle.commandExecutor, command, sessionId);
    } catch (IllegalAccessException e) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED,
          "调用 Arthas executeAsync 失败",
          e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED,
          "调用 Arthas executeAsync 失败: " + safeMessage(cause),
          cause);
    }
    long invokeCost = nanosToMillis(System.nanoTime() - invokeStart);
    return buildAsyncResult(rawResult, initCost, invokeCost);
  }

  @Nullable
  public StructuredAsyncResult pullResults(String sessionId, String consumerId) {
    ExecutorHandle handle = ensureHandle();
    long initCost = handle.lastInitCostMillis;
    Object rawResult;
    long invokeStart = System.nanoTime();
    try {
      rawResult = handle.pullResultsMethod.invoke(handle.commandExecutor, sessionId, consumerId);
    } catch (IllegalAccessException e) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.PULL_RESULT_FAILED,
          "调用 Arthas pullResults 失败",
          e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.PULL_RESULT_FAILED,
          "调用 Arthas pullResults 失败: " + safeMessage(cause),
          cause);
    }
    long invokeCost = nanosToMillis(System.nanoTime() - invokeStart);
    if (rawResult == null) {
      return null;
    }
    return buildAsyncResult(rawResult, initCost, invokeCost);
  }

  public StructuredAsyncResult interruptJob(String sessionId) {
    ExecutorHandle handle = ensureHandle();
    long initCost = handle.lastInitCostMillis;
    Object rawResult;
    long invokeStart = System.nanoTime();
    try {
      rawResult = handle.interruptJobMethod.invoke(handle.commandExecutor, sessionId);
    } catch (IllegalAccessException e) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.ASYNC_JOB_INTERRUPTED,
          "调用 Arthas interruptJob 失败",
          e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.ASYNC_JOB_INTERRUPTED,
          "调用 Arthas interruptJob 失败: " + safeMessage(cause),
          cause);
    }
    long invokeCost = nanosToMillis(System.nanoTime() - invokeStart);
    return buildAsyncResult(rawResult, initCost, invokeCost);
  }

  public StructuredAsyncResult closeSession(String sessionId) {
    ExecutorHandle handle = ensureHandle();
    long initCost = handle.lastInitCostMillis;
    Object rawResult;
    long invokeStart = System.nanoTime();
    try {
      rawResult = handle.closeSessionMethod.invoke(handle.commandExecutor, sessionId);
    } catch (IllegalAccessException e) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED,
          "调用 Arthas closeSession 失败",
          e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED,
          "调用 Arthas closeSession 失败: " + safeMessage(cause),
          cause);
    }
    long invokeCost = nanosToMillis(System.nanoTime() - invokeStart);
    return buildAsyncResult(rawResult, initCost, invokeCost);
  }

  public ArthasSessionInspection inspectSession(String sessionId) {
    ExecutorHandle handle = ensureHandle();
    Object session;
    try {
      session = handle.getSessionMethod.invoke(handle.sessionManager, sessionId);
    } catch (IllegalAccessException e) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.SESSION_NOT_FOUND,
          "读取 Arthas session 失败",
          e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.SESSION_NOT_FOUND,
          "读取 Arthas session 失败: " + safeMessage(cause),
          cause);
    }
    if (session == null) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.SESSION_NOT_FOUND,
          "Arthas session 不存在: " + sessionId,
          null);
    }

    try {
      Object foregroundJob = handle.getForegroundJobMethod.invoke(session);
      if (foregroundJob == null) {
        return new ArthasSessionInspection(sessionId, /* hasForegroundJob= */ false, null, null);
      }
      Integer jobId = ((Number) handle.jobIdMethod.invoke(foregroundJob)).intValue();
      Object jobStatus = handle.jobStatusMethod.invoke(foregroundJob);
      return new ArthasSessionInspection(
          sessionId,
          /* hasForegroundJob= */ true,
          jobId,
          jobStatus != null ? String.valueOf(jobStatus) : null);
    } catch (IllegalAccessException e) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.SESSION_NOT_FOUND,
          "读取 Arthas session job 状态失败",
          e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.SESSION_NOT_FOUND,
          "读取 Arthas session job 状态失败: " + safeMessage(cause),
          cause);
    }
  }

  private StructuredExecResult buildSyncResult(
      Object rawResult,
      String command,
      @Nullable String sessionId,
      long initCost,
      long invokeCost) {
    JsonConversion conversion = convertRawResultToJson(rawResult);
    Map<String, Object> payload = conversion.payload;

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
            firstNonBlank(
                toNullableString(payload.get("error")),
                success ? null : "Arthas command execution failed"));

    logger.log(
        Level.FINE,
        "[ARTHAS-EXEC-SYNC] Bridge executeSync completed: command={0}, success={1}, timeout={2}, initMs={3}, invokeMs={4}, jsonMs={5}",
        new Object[] {
          command, success, timeout, initCost, invokeCost, conversion.serializationCostMillis
        });

    return StructuredExecResult.builder(command)
        .success(success)
        .timeout(timeout)
        .sessionId(resolvedSessionId)
        .errorCode(errorCode)
        .errorMessage(errorMessage)
        .payload(payload)
        .rawJson(conversion.rawJson)
        .bridgeInitTimeMillis(initCost)
        .invokeTimeMillis(invokeCost)
        .serializationTimeMillis(conversion.serializationCostMillis)
        .build();
  }

  private StructuredAsyncResult buildAsyncResult(
      Object rawResult, long initCost, long invokeCost) {
    JsonConversion conversion = convertRawResultToJson(rawResult);
    Map<String, Object> payload = conversion.payload;
    boolean success = toBoolean(payload.get(ArthasTaskProtocol.ResultField.SUCCESS), true);
    String sessionId = toNullableString(payload.get(ArthasTaskProtocol.ResultField.SESSION_ID));
    String consumerId = toNullableString(payload.get(ArthasTaskProtocol.ResultField.CONSUMER_ID));
    String errorMessage =
        firstNonBlank(
            toNullableString(payload.get(ArthasTaskProtocol.ResultField.ERROR_MESSAGE)),
            toNullableString(payload.get("error")));
    String errorCode = success ? null : classifyAsyncPayloadError(errorMessage);

    return StructuredAsyncResult.builder()
        .success(success)
        .sessionId(sessionId)
        .consumerId(consumerId)
        .errorCode(errorCode)
        .errorMessage(errorMessage)
        .payload(payload)
        .rawJson(conversion.rawJson)
        .bridgeInitTimeMillis(initCost)
        .invokeTimeMillis(invokeCost)
        .serializationTimeMillis(conversion.serializationCostMillis)
        .build();
  }

  private JsonConversion convertRawResultToJson(@Nullable Object rawResult) {
    ExecutorHandle handle = ensureHandle();
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
    if (payload.isEmpty()
        && rawJson != null
        && !rawJson.trim().isEmpty()
        && !"{}".equals(rawJson.trim())) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_JSON_PARSE_FAILED,
          "Arthas 结构化结果 JSON 解析失败",
          null);
    }
    return new JsonConversion(rawJson, payload, serializationCost);
  }

  private static void configureSessionContext(
      ExecutorHandle handle,
      String sessionId,
      @Nullable Object authSubject,
      @Nullable String userId) {

    try {
      if (authSubject != null) {
        handle.setSessionAuthMethod.invoke(handle.commandExecutor, sessionId, authSubject);
      }
      if (userId != null && !userId.trim().isEmpty()) {
        handle.setSessionUserIdMethod.invoke(handle.commandExecutor, sessionId, userId);
      }
    } catch (IllegalAccessException e) {
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED,
          "配置 Arthas session 上下文失败",
          e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw arthasFailure(
          ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED,
          "配置 Arthas session 上下文失败: " + safeMessage(cause),
          cause);
    }
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
    long initStart = System.nanoTime();
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
      Class<?> sessionClass = loadRequiredClass(arthasCl, "Session", SESSION_CLASS);
      Class<?> jobClass = loadRequiredClass(arthasCl, "Job", JOB_CLASS);
      Class<?> executorImplClass =
          loadRequiredClass(arthasCl, "CommandExecutorImpl", COMMAND_EXECUTOR_IMPL_CLASS);
      Class<?> jsonClass = loadRequiredClass(arthasCl, "fastjson2 JSON", FASTJSON2_JSON_CLASSES);

      Constructor<?> ctor = executorImplClass.getConstructor(sessionManagerClass);
      Object commandExecutor = ctor.newInstance(sessionManager);
      Method executeSyncMethod =
          executorImplClass.getMethod(
              "executeSync", String.class, long.class, String.class, Object.class, String.class);
      Method createSessionMethod = executorImplClass.getMethod("createSession");
      Method executeAsyncMethod =
          executorImplClass.getMethod("executeAsync", String.class, String.class);
      Method pullResultsMethod =
          executorImplClass.getMethod("pullResults", String.class, String.class);
      Method interruptJobMethod = executorImplClass.getMethod("interruptJob", String.class);
      Method closeSessionMethod = executorImplClass.getMethod("closeSession", String.class);
      Method setSessionAuthMethod =
          executorImplClass.getMethod("setSessionAuth", String.class, Object.class);
      Method setSessionUserIdMethod =
          executorImplClass.getMethod("setSessionUserId", String.class, String.class);
      Method getSessionMethod = sessionManagerClass.getMethod("getSession", String.class);
      Method getForegroundJobMethod = sessionClass.getMethod("getForegroundJob");
      Method jobIdMethod = jobClass.getMethod("id");
      Method jobStatusMethod = jobClass.getMethod("status");
      Method toJsonMethod = jsonClass.getMethod("toJSONString", Object.class);

      long initCost = nanosToMillis(System.nanoTime() - initStart);
      logger.log(Level.FINE, "[ARTHAS-BRIDGE] CommandExecutorImpl handle initialized successfully");
      return new ExecutorHandle(
          arthasCl,
          bootstrapInstance,
          sessionManager,
          commandExecutor,
          executeSyncMethod,
          createSessionMethod,
          executeAsyncMethod,
          pullResultsMethod,
          interruptJobMethod,
          closeSessionMethod,
          setSessionAuthMethod,
          setSessionUserIdMethod,
          getSessionMethod,
          getForegroundJobMethod,
          jobIdMethod,
          jobStatusMethod,
          toJsonMethod,
          initCost);
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
        componentName + " 类不存在，可选类名=" + Arrays.toString(candidateClassNames);
    throw last != null
        ? new ClassNotFoundException(message, last)
        : new ClassNotFoundException(message);
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

  private static String classifyAsyncPayloadError(@Nullable String errorMessage) {
    if (errorMessage == null || errorMessage.trim().isEmpty()) {
      return ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED;
    }
    String lower = errorMessage.toLowerCase(Locale.ROOT);
    if (lower.contains("session") && lower.contains("not found")) {
      return ArthasTaskProtocol.ErrorCode.SESSION_NOT_FOUND;
    }
    if (lower.contains("another command is executing") || lower.contains("another job is running")) {
      return ArthasTaskProtocol.ErrorCode.SESSION_NOT_IDLE;
    }
    if (lower.contains("interrupt")) {
      return ArthasTaskProtocol.ErrorCode.ASYNC_JOB_INTERRUPTED;
    }
    if (lower.contains("consumer")) {
      return ArthasTaskProtocol.ErrorCode.PULL_RESULT_FAILED;
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
      if (message != null && message.toLowerCase(Locale.ROOT).contains("timeout")) {
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

  private static final class JsonConversion {
    private final String rawJson;
    private final Map<String, Object> payload;
    private final long serializationCostMillis;

    private JsonConversion(String rawJson, Map<String, Object> payload, long serializationCostMillis) {
      this.rawJson = rawJson;
      this.payload = payload;
      this.serializationCostMillis = serializationCostMillis;
    }
  }

  private static final class ExecutorHandle {
    private final ClassLoader arthasClassLoader;
    private final Object bootstrapInstance;
    private final Object sessionManager;
    private final Object commandExecutor;
    private final Method executeSyncMethod;
    private final Method createSessionMethod;
    private final Method executeAsyncMethod;
    private final Method pullResultsMethod;
    private final Method interruptJobMethod;
    private final Method closeSessionMethod;
    private final Method setSessionAuthMethod;
    private final Method setSessionUserIdMethod;
    private final Method getSessionMethod;
    private final Method getForegroundJobMethod;
    private final Method jobIdMethod;
    private final Method jobStatusMethod;
    private final Method toJsonMethod;
    private final long lastInitCostMillis;

    private ExecutorHandle(
        ClassLoader arthasClassLoader,
        Object bootstrapInstance,
        Object sessionManager,
        Object commandExecutor,
        Method executeSyncMethod,
        Method createSessionMethod,
        Method executeAsyncMethod,
        Method pullResultsMethod,
        Method interruptJobMethod,
        Method closeSessionMethod,
        Method setSessionAuthMethod,
        Method setSessionUserIdMethod,
        Method getSessionMethod,
        Method getForegroundJobMethod,
        Method jobIdMethod,
        Method jobStatusMethod,
        Method toJsonMethod,
        long lastInitCostMillis) {
      this.arthasClassLoader = arthasClassLoader;
      this.bootstrapInstance = bootstrapInstance;
      this.sessionManager = sessionManager;
      this.commandExecutor = commandExecutor;
      this.executeSyncMethod = executeSyncMethod;
      this.createSessionMethod = createSessionMethod;
      this.executeAsyncMethod = executeAsyncMethod;
      this.pullResultsMethod = pullResultsMethod;
      this.interruptJobMethod = interruptJobMethod;
      this.closeSessionMethod = closeSessionMethod;
      this.setSessionAuthMethod = setSessionAuthMethod;
      this.setSessionUserIdMethod = setSessionUserIdMethod;
      this.getSessionMethod = getSessionMethod;
      this.getForegroundJobMethod = getForegroundJobMethod;
      this.jobIdMethod = jobIdMethod;
      this.jobStatusMethod = jobStatusMethod;
      this.toJsonMethod = toJsonMethod;
      this.lastInitCostMillis = lastInitCostMillis;
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
