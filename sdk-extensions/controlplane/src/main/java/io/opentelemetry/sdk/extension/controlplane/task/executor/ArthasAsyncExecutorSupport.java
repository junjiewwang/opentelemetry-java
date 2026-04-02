package io.opentelemetry.sdk.extension.controlplane.task.executor;

import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasAsyncSessionSnapshot;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasIntegration;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasLifecycleManager;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasReadinessGate;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasSessionState;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasTaskProtocol;
import io.opentelemetry.sdk.extension.controlplane.util.JsonUtils;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

final class ArthasAsyncExecutorSupport {

  private static final Logger logger = Logger.getLogger(ArthasAsyncExecutorSupport.class.getName());

  static final long DEFAULT_EXEC_TIMEOUT_MILLIS = 30_000;
  static final long DEFAULT_WAIT_TIMEOUT_MILLIS = 10_000;
  static final long DEFAULT_PULL_MAX_ITEMS = 50;
  static final long DEFAULT_PULL_MAX_BYTES = 512 * 1024;
  static final long DEFAULT_SESSION_TTL_MILLIS = 300_000;
  static final long DEFAULT_IDLE_TIMEOUT_MILLIS = 60_000;
  static final long CHECK_INTERVAL_MILLIS = 200;
  static final long RESTART_BUDGET_MILLIS = 15_000;
  static final long MIN_GRACE_PERIOD_MILLIS = 5_000;
  static final long MAX_GRACE_PERIOD_MILLIS = 30_000;
  static final boolean DEFAULT_REQUIRE_LOCAL_READY = false;
  static final boolean DEFAULT_REQUIRE_TUNNEL_READY = true;

  private ArthasAsyncExecutorSupport() {}

  static CompletableFuture<TaskExecutionResult> executeAsync(
      TaskExecutionContext context, String taskType, InternalExecutor executor) {
    Executor taskExecutor = context.getTaskExecutor();
    if (taskExecutor != null) {
      return CompletableFuture.supplyAsync(
          () -> executeSafely(context, taskType, executor), taskExecutor);
    }
    return CompletableFuture.supplyAsync(() -> executeSafely(context, taskType, executor));
  }

  static TaskExecutionResult executeSafely(
      TaskExecutionContext context,
      String taskType,
      InternalExecutor executor) {
    long startTime = System.currentTimeMillis();
    try {
      return executor.execute(startTime);
    } catch (RuntimeException e) {
      String errorCode = parseBridgeErrorCode(e);
      String errorMessage = parseBridgeErrorMessage(e);
      logger.log(Level.WARNING, "[ARTHAS-ASYNC] Execution failed unexpectedly: taskType=" + taskType, e);
      return buildFailureResult(
          taskType,
          errorCode,
          errorMessage,
          null,
          null,
          null,
          null,
          null,
          elapsed(startTime),
          0,
          0,
          0,
          null,
          /* tunnelReady= */ false);
    }
  }

  static ReadyCheck ensureReady(
      ArthasIntegration integration,
      TaskExecutionContext context,
      String taskType,
      @Nullable String command,
      long timeoutMillis,
      boolean autoAttach,
      boolean requireTunnelReady) {
    ArthasLifecycleManager manager = integration.getLifecycleManager();
    if (!requireTunnelReady && manager.isRunning()) {
      return ReadyCheck.ok();
    }

    ArthasReadinessGate.Result readiness = integration.getReadinessGate().evaluateNow();
    if (requireTunnelReady && readiness.isTerminalReady()) {
      return ReadyCheck.ok();
    }

    if (!requireTunnelReady && !manager.isRunning()) {
      if (!autoAttach) {
        return ReadyCheck.fail(
            buildFailureResult(
                taskType,
                ArthasTaskProtocol.ErrorCode.ARTHAS_NOT_RUNNING,
                "Arthas is not running and auto_attach is disabled",
                command,
                null,
                null,
                null,
                null,
                0,
                0,
                0,
                0,
                currentArthasState(integration),
                integration.isTunnelReady()));
      }
      return waitForLocalRunning(integration, context, taskType, command, timeoutMillis);
    }

    if (!autoAttach) {
      return ReadyCheck.fail(
          buildFailureResult(
              taskType,
              mapReadinessErrorCode(readiness),
              readiness.toHumanMessage(),
              command,
              null,
              null,
              null,
              null,
              0,
              0,
              0,
              0,
              currentArthasState(integration),
              integration.isTunnelReady()));
    }

    cleanupUnhealthyForAttach(integration, timeoutMillis, taskType + ":" + context.getTaskId());
    return waitForTerminalReady(integration, context, taskType, command, timeoutMillis);
  }

  static Map<String, Object> snapshotToMap(ArthasAsyncSessionSnapshot snapshot) {
    Map<String, Object> session = new LinkedHashMap<>();
    session.put(ArthasTaskProtocol.ResultField.SESSION_ID, snapshot.getSessionId());
    session.put(ArthasTaskProtocol.ResultField.CONSUMER_ID, snapshot.getConsumerId());
    session.put(ArthasTaskProtocol.ResultField.STATE, snapshot.getState().name());
    session.put("ttlMs", snapshot.getTtlMillis());
    session.put("idleTimeoutMs", snapshot.getIdleTimeoutMillis());
    session.put("createdAt", snapshot.getCreatedAtMillis());
    session.put("lastAccessAt", snapshot.getLastAccessAtMillis());
    if (snapshot.getCurrentCommand() != null) {
      session.put("currentCommand", snapshot.getCurrentCommand());
    }
    if (snapshot.getCurrentJobId() != null) {
      session.put("currentJobId", snapshot.getCurrentJobId());
    }
    if (snapshot.getCurrentJobStatus() != null) {
      session.put("currentJobStatus", snapshot.getCurrentJobStatus());
    }
    session.put("endOfStream", snapshot.isEndOfStream());
    return session;
  }

  static TaskExecutionResult buildSuccessResult(
      String taskType,
      @Nullable String command,
      @Nullable String sessionId,
      @Nullable String consumerId,
      @Nullable Map<String, Object> session,
      @Nullable Map<String, Object> job,
      @Nullable Map<String, Object> delta,
      @Nullable Map<String, Object> payload,
      @Nullable String rawJson,
      long executionTimeMillis,
      long bridgeInitTimeMillis,
      long invokeTimeMillis,
      long serializationTimeMillis,
      @Nullable String arthasState,
      boolean tunnelReady) {
    return TaskExecutionResult.success(
        buildEnvelopeJson(
            taskType,
            /* success= */ true,
            /* timeout= */ false,
            null,
            null,
            command,
            sessionId,
            consumerId,
            session,
            job,
            delta,
            payload,
            rawJson,
            executionTimeMillis,
            bridgeInitTimeMillis,
            invokeTimeMillis,
            serializationTimeMillis,
            arthasState,
            tunnelReady),
        executionTimeMillis);
  }

  static TaskExecutionResult buildFailureResult(
      String taskType,
      String errorCode,
      String errorMessage,
      @Nullable String command,
      @Nullable String sessionId,
      @Nullable String consumerId,
      @Nullable Map<String, Object> session,
      @Nullable Map<String, Object> payload,
      long executionTimeMillis,
      long bridgeInitTimeMillis,
      long invokeTimeMillis,
      long serializationTimeMillis,
      @Nullable String arthasState,
      boolean tunnelReady) {
    String resultJson =
        buildEnvelopeJson(
            taskType,
            /* success= */ false,
            /* timeout= */ false,
            errorCode,
            errorMessage,
            command,
            sessionId,
            consumerId,
            session,
            null,
            null,
            payload,
            null,
            executionTimeMillis,
            bridgeInitTimeMillis,
            invokeTimeMillis,
            serializationTimeMillis,
            arthasState,
            tunnelReady);
    return TaskExecutionResult.builder()
        .status(TaskExecutionResult.Status.FAILED)
        .errorCode(errorCode)
        .errorMessage(errorMessage)
        .resultJson(resultJson)
        .executionTimeMillis(executionTimeMillis)
        .completedAtMillis(System.currentTimeMillis())
        .build();
  }

  static TaskExecutionResult buildTimeoutResult(
      String taskType,
      String errorCode,
      String errorMessage,
      @Nullable String command,
      @Nullable String sessionId,
      @Nullable String consumerId,
      @Nullable Map<String, Object> session,
      long executionTimeMillis,
      long bridgeInitTimeMillis,
      long invokeTimeMillis,
      long serializationTimeMillis,
      @Nullable String arthasState,
      boolean tunnelReady) {
    String resultJson =
        buildEnvelopeJson(
            taskType,
            /* success= */ false,
            /* timeout= */ true,
            errorCode,
            errorMessage,
            command,
            sessionId,
            consumerId,
            session,
            null,
            null,
            null,
            null,
            executionTimeMillis,
            bridgeInitTimeMillis,
            invokeTimeMillis,
            serializationTimeMillis,
            arthasState,
            tunnelReady);
    return TaskExecutionResult.builder()
        .status(TaskExecutionResult.Status.TIMEOUT)
        .errorCode(errorCode)
        .errorMessage(errorMessage)
        .resultJson(resultJson)
        .executionTimeMillis(executionTimeMillis)
        .completedAtMillis(System.currentTimeMillis())
        .build();
  }

  static Map<String, Object> buildJobMap(
      boolean accepted,
      ArthasSessionState state,
      @Nullable String command,
      @Nullable Integer jobId,
      @Nullable String jobStatus) {
    Map<String, Object> job = new LinkedHashMap<>();
    job.put("accepted", accepted);
    job.put(ArthasTaskProtocol.ResultField.STATE, state.name());
    if (command != null) {
      job.put(ArthasTaskProtocol.ResultField.COMMAND, command);
    }
    if (jobId != null) {
      job.put("jobId", jobId);
    }
    if (jobStatus != null) {
      job.put("jobStatus", jobStatus);
    }
    return job;
  }

  static Map<String, Object> buildDeltaMap(
      List<Object> items,
      boolean hasMore,
      boolean endOfStream,
      @Nullable String nextCursor,
      long waitedMillis) {
    Map<String, Object> delta = new LinkedHashMap<>();
    delta.put("items", items);
    delta.put("count", items.size());
    delta.put("hasMore", hasMore);
    delta.put("endOfStream", endOfStream);
    delta.put("nextCursor", nextCursor);
    Map<String, Object> meta = new LinkedHashMap<>();
    meta.put("waitedMs", waitedMillis);
    delta.put("meta", meta);
    return delta;
  }

  static long resolveEffectiveTimeout(TaskExecutionContext context, String parameterKey, long defaultValue) {
    long requested = context.getLongParameter(parameterKey, defaultValue);
    long taskLimit = context.getEffectiveTimeoutMillis();
    if (taskLimit > 0) {
      return Math.min(requested, taskLimit);
    }
    return requested;
  }

  static long resolvePositiveLong(TaskExecutionContext context, String parameterKey, long defaultValue) {
    long value = context.getLongParameter(parameterKey, defaultValue);
    return value > 0 ? value : defaultValue;
  }

  @Nullable
  static String emptyToNull(@Nullable String value) {
    return value == null || value.trim().isEmpty() ? null : value;
  }

  static String parseBridgeErrorCode(RuntimeException e) {
    String message = e.getMessage();
    if (message != null) {
      int idx = message.indexOf(": ");
      if (idx > 0) {
        return message.substring(0, idx);
      }
    }
    return ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED;
  }

  static String parseBridgeErrorMessage(RuntimeException e) {
    String message = e.getMessage();
    if (message != null) {
      int idx = message.indexOf(": ");
      if (idx > 0 && idx + 2 < message.length()) {
        return message.substring(idx + 2);
      }
      return message;
    }
    return e.getClass().getSimpleName();
  }

  static long elapsed(long startTimeMillis) {
    return Math.max(0, System.currentTimeMillis() - startTimeMillis);
  }

  static boolean isOverLimit(@Nullable String rawJson, long limitBytes) {
    return rawJson != null && rawJson.getBytes(StandardCharsets.UTF_8).length > limitBytes;
  }

  static boolean isTerminalJobStatus(@Nullable String jobStatus) {
    if (jobStatus == null) {
      return false;
    }
    return "STOPPED".equalsIgnoreCase(jobStatus) || "TERMINATED".equalsIgnoreCase(jobStatus);
  }

  static String currentArthasState(ArthasIntegration integration) {
    return integration.getLifecycleManager().getState().name();
  }

  static List<Object> trimItemsToLimits(
      List<Object> originalItems, long maxItems, long maxBytes, String taskType) {
    if (originalItems.isEmpty()) {
      return originalItems;
    }
    List<Object> trimmed = new ArrayList<>();
    long totalBytes = 0;
    for (Object item : originalItems) {
      if (trimmed.size() >= maxItems) {
        break;
      }
      String json = JsonUtils.toJsonString(item);
      long nextBytes = json.getBytes(StandardCharsets.UTF_8).length;
      if (!trimmed.isEmpty() && totalBytes + nextBytes > maxBytes) {
        break;
      }
      trimmed.add(item);
      totalBytes += nextBytes;
    }
    if (trimmed.size() != originalItems.size()) {
      logger.log(
          Level.FINE,
          "[ARTHAS-ASYNC] Trimmed delta items for {0}: original={1}, trimmed={2}",
          new Object[] {taskType, originalItems.size(), trimmed.size()});
    }
    return trimmed;
  }

  static ReadyCheck waitForLocalRunning(
      ArthasIntegration integration,
      TaskExecutionContext context,
      String taskType,
      @Nullable String command,
      long timeoutMillis) {
    ReadyCheck startCheck =
        startIfNeeded(
            integration,
            context,
            taskType,
            command,
            /* requireTunnelReady= */ false);
    if (!startCheck.proceed()) {
      return startCheck;
    }

    ArthasLifecycleManager manager = integration.getLifecycleManager();
    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (System.currentTimeMillis() < deadline) {
      if (manager.isRunning()) {
        return ReadyCheck.ok();
      }
      if (manager.getState() == ArthasLifecycleManager.State.STOPPED) {
        return ReadyCheck.fail(
            buildFailureResult(
                taskType,
                ArthasTaskProtocol.ErrorCode.ARTHAS_START_FAILED,
                "Arthas failed to enter RUNNING state",
                command,
                null,
                null,
                null,
                null,
                0,
                0,
                0,
                0,
                currentArthasState(integration),
                integration.isTunnelReady()));
      }
      sleepQuietly();
    }

    return ReadyCheck.fail(
        buildTimeoutResult(
            taskType,
            ArthasTaskProtocol.ErrorCode.COMMAND_TIMEOUT,
            String.format(Locale.ROOT, "Waiting Arthas running timed out after %dms", timeoutMillis),
            command,
            null,
            null,
            null,
            0,
            0,
            0,
            0,
            currentArthasState(integration),
            integration.isTunnelReady()));
  }

  static ReadyCheck waitForTerminalReady(
      ArthasIntegration integration,
      TaskExecutionContext context,
      String taskType,
      @Nullable String command,
      long timeoutMillis) {
    ReadyCheck startCheck =
        startIfNeeded(
            integration,
            context,
            taskType,
            command,
            /* requireTunnelReady= */ true);
    if (!startCheck.proceed()) {
      return startCheck;
    }

    ArthasReadinessGate.Result result;
    try {
      result = integration.getReadinessGate().awaitTerminalReady(Duration.ofMillis(timeoutMillis)).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return ReadyCheck.fail(
          buildFailureResult(
              taskType,
              ArthasTaskProtocol.ErrorCode.INTERRUPTED,
              "Interrupted while waiting for Arthas to become ready",
              command,
              null,
              null,
              null,
              null,
              0,
              0,
              0,
              0,
              currentArthasState(integration),
              integration.isTunnelReady()));
    } catch (java.util.concurrent.ExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      if (cause instanceof TimeoutException) {
        return ReadyCheck.fail(
            buildTimeoutResult(
                taskType,
                ArthasTaskProtocol.ErrorCode.COMMAND_TIMEOUT,
                String.format(Locale.ROOT, "Waiting Arthas ready timed out after %dms", timeoutMillis),
                command,
                null,
                null,
                null,
                0,
                0,
                0,
                0,
                currentArthasState(integration),
                integration.isTunnelReady()));
      }
      return ReadyCheck.fail(
          buildFailureResult(
              taskType,
              ArthasTaskProtocol.ErrorCode.ARTHAS_START_FAILED,
              "Failed while waiting for Arthas ready: " + safeMessage(cause),
              command,
              null,
              null,
              null,
              null,
              0,
              0,
              0,
              0,
              currentArthasState(integration),
              integration.isTunnelReady()));
    }

    if (result.isTerminalReady()) {
      return ReadyCheck.ok();
    }

    return ReadyCheck.fail(
        buildFailureResult(
            taskType,
            mapReadinessErrorCode(result),
            result.toHumanMessage(),
            command,
            null,
            null,
            null,
            null,
            0,
            0,
            0,
            0,
            currentArthasState(integration),
            integration.isTunnelReady()));
  }

  private static ReadyCheck startIfNeeded(
      ArthasIntegration integration,
      TaskExecutionContext context,
      String taskType,
      @Nullable String command,
      boolean requireTunnelReady) {
    ArthasLifecycleManager manager = integration.getLifecycleManager();
    ArthasLifecycleManager.State state = manager.getState();
    if (state == ArthasLifecycleManager.State.RUNNING || state == ArthasLifecycleManager.State.IDLE) {
      return ReadyCheck.ok();
    }
    if (state == ArthasLifecycleManager.State.STARTING) {
      return ReadyCheck.ok();
    }
    if (state == ArthasLifecycleManager.State.STOPPING) {
      return ReadyCheck.fail(
          buildFailureResult(
              taskType,
              ArthasTaskProtocol.ErrorCode.ARTHAS_NOT_READY,
              "Arthas is stopping, cannot execute command now",
              command,
              null,
              null,
              null,
              null,
              0,
              0,
              0,
              0,
              currentArthasState(integration),
              integration.isTunnelReady()));
    }

    ScheduledExecutorService scheduler = context.getScheduler();
    if (scheduler == null) {
      return ReadyCheck.fail(
          buildFailureResult(
              taskType,
              ArthasTaskProtocol.ErrorCode.NO_SCHEDULER,
              "No scheduler available for Arthas auto_attach",
              command,
              null,
              null,
              null,
              null,
              0,
              0,
              0,
              0,
              currentArthasState(integration),
              integration.isTunnelReady()));
    }

    ArthasLifecycleManager.StartResult startResult = manager.tryStart(scheduler);
    if (!startResult.isSuccess() && manager.getState() != ArthasLifecycleManager.State.STARTING) {
      return ReadyCheck.fail(
          buildFailureResult(
              taskType,
              ArthasTaskProtocol.ErrorCode.ARTHAS_START_FAILED,
              "Failed to start Arthas: " + startResult.getErrorMessage(),
              command,
              null,
              null,
              null,
              null,
              0,
              0,
              0,
              0,
              currentArthasState(integration),
              integration.isTunnelReady()));
    }

    logger.log(
        Level.INFO,
        "[ARTHAS-ASYNC] Auto-attach requested for {0}, waiting for {1}",
        new Object[] {taskType, requireTunnelReady ? "terminal ready" : "local running"});
    return ReadyCheck.ok();
  }

  private static void cleanupUnhealthyForAttach(
      ArthasIntegration integration, long timeoutMillis, String reason) {
    long remaining = timeoutMillis - RESTART_BUDGET_MILLIS;
    long gracePeriod = Math.min(MAX_GRACE_PERIOD_MILLIS, Math.max(MIN_GRACE_PERIOD_MILLIS, remaining));
    integration.cleanupIfUnhealthyForAttach(gracePeriod, reason);
  }

  private static String buildEnvelopeJson(
      String taskType,
      boolean success,
      boolean timeout,
      @Nullable String errorCode,
      @Nullable String errorMessage,
      @Nullable String command,
      @Nullable String sessionId,
      @Nullable String consumerId,
      @Nullable Map<String, Object> session,
      @Nullable Map<String, Object> job,
      @Nullable Map<String, Object> delta,
      @Nullable Map<String, Object> payload,
      @Nullable String rawJson,
      long executionTimeMillis,
      long bridgeInitTimeMillis,
      long invokeTimeMillis,
      long serializationTimeMillis,
      @Nullable String arthasState,
      boolean tunnelReady) {
    Map<String, Object> root = new LinkedHashMap<>();
    root.put(ArthasTaskProtocol.ResultField.SUCCESS, success);
    root.put(ArthasTaskProtocol.ResultField.TASK_TYPE, taskType);
    root.put(ArthasTaskProtocol.ResultField.COMMAND, command);
    root.put(ArthasTaskProtocol.ResultField.SESSION_ID, sessionId);
    root.put(ArthasTaskProtocol.ResultField.CONSUMER_ID, consumerId);
    root.put(ArthasTaskProtocol.ResultField.TIMEOUT, timeout);
    root.put(ArthasTaskProtocol.ResultField.ERROR_CODE, valueOrEmpty(errorCode));
    root.put(ArthasTaskProtocol.ResultField.ERROR_MESSAGE, valueOrEmpty(errorMessage));
    if (session != null) {
      root.put("session", session);
    }
    if (job != null) {
      root.put("job", job);
    }
    if (delta != null) {
      root.put(ArthasTaskProtocol.ResultField.DELTA, delta);
    }
    if (payload != null) {
      root.put(ArthasTaskProtocol.ResultField.PAYLOAD, payload);
    }
    root.put(ArthasTaskProtocol.ResultField.RAW_JSON, rawJson);

    Map<String, Object> meta = new LinkedHashMap<>();
    meta.put("executionTimeMs", executionTimeMillis);
    meta.put("bridgeInitTimeMs", bridgeInitTimeMillis);
    meta.put("invokeTimeMs", invokeTimeMillis);
    meta.put("serializationTimeMs", serializationTimeMillis);
    meta.put("arthasState", arthasState);
    meta.put("tunnelReady", tunnelReady);
    root.put(ArthasTaskProtocol.ResultField.META, meta);
    return JsonUtils.toJsonString(root);
  }

  private static String mapReadinessErrorCode(ArthasReadinessGate.Result readiness) {
    switch (readiness.getReasonCode()) {
      case TUNNEL_NOT_CONNECTED:
      case TUNNEL_NOT_REGISTERED:
        return ArthasTaskProtocol.ErrorCode.TUNNEL_NOT_READY;
      case ARTHAS_STARTING:
      case ARTHAS_STOPPING:
        return ArthasTaskProtocol.ErrorCode.ARTHAS_NOT_READY;
      case ARTHAS_STOPPED:
        return ArthasTaskProtocol.ErrorCode.ARTHAS_NOT_RUNNING;
      case NONE:
        return ArthasTaskProtocol.ErrorCode.ARTHAS_NOT_READY;
    }
    return ArthasTaskProtocol.ErrorCode.ARTHAS_NOT_READY;
  }

  private static String valueOrEmpty(@Nullable String value) {
    return value != null ? value : "";
  }

  private static String safeMessage(Throwable throwable) {
    String message = throwable.getMessage();
    return message != null && !message.trim().isEmpty()
        ? message
        : throwable.getClass().getSimpleName();
  }

  private static void sleepQuietly() {
    try {
      Thread.sleep(CHECK_INTERVAL_MILLIS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  interface InternalExecutor {
    TaskExecutionResult execute(long startTime);
  }

  static final class ReadyCheck {
    @Nullable private final TaskExecutionResult result;

    private ReadyCheck(@Nullable TaskExecutionResult result) {
      this.result = result;
    }

    static ReadyCheck ok() {
      return new ReadyCheck(null);
    }

    static ReadyCheck fail(TaskExecutionResult result) {
      return new ReadyCheck(Objects.requireNonNull(result, "result"));
    }

    boolean proceed() {
      return result == null;
    }

    TaskExecutionResult getFailureResult() {
      return Objects.requireNonNull(result, "result");
    }
  }
}
