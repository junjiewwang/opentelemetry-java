/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.executor;

import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasIntegration;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasLifecycleManager;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasReadinessGate;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasStructuredCommandBridge;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasTaskProtocol;
import io.opentelemetry.sdk.extension.controlplane.arthas.StructuredExecResult;
import io.opentelemetry.sdk.extension.controlplane.task.status.TaskStatusEmitter;
import io.opentelemetry.sdk.extension.controlplane.util.JsonUtils;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 同步结构化执行器。
 *
 * <p>负责处理 {@code arthas_exec_sync} 任务，必要时先启动 Arthas，再通过桥接层执行
 * {@code CommandExecutorImpl.executeSync(...)} 并返回稳定 JSON。
 */
public final class ArthasExecSyncExecutor implements TaskExecutor {

  private static final Logger logger = Logger.getLogger(ArthasExecSyncExecutor.class.getName());

  public static final String TASK_TYPE = ArthasTaskProtocol.TaskType.EXEC_SYNC;

  private static final long DEFAULT_EXEC_TIMEOUT_MILLIS = 30_000;
  private static final long DEFAULT_RESULT_LIMIT_BYTES = 1024 * 1024;
  private static final long CHECK_INTERVAL_MILLIS = 200;
  private static final long RESTART_BUDGET_MILLIS = 15_000;
  private static final long MIN_GRACE_PERIOD_MILLIS = 5_000;
  private static final long MAX_GRACE_PERIOD_MILLIS = 30_000;
  private static final boolean DEFAULT_REQUIRE_TUNNEL_READY = false;

  @Nullable private final ArthasIntegration arthasIntegration;

  public ArthasExecSyncExecutor(@Nullable ArthasIntegration arthasIntegration) {
    this.arthasIntegration = arthasIntegration;
  }

  @Override
  public String getTaskType() {
    return TASK_TYPE;
  }

  @Override
  public String getDescription() {
    return "Arthas exec sync executor - executes structured Arthas sync commands via CommandExecutorImpl";
  }

  @Override
  public boolean isAvailable() {
    return arthasIntegration != null;
  }

  @Override
  public CompletableFuture<TaskExecutionResult> execute(TaskExecutionContext context) {
    CompletableFuture<TaskExecutionResult> future = new CompletableFuture<>();
    long startTime = System.currentTimeMillis();

    @SuppressWarnings("FutureReturnValueIgnored")
    Object unused =
        CompletableFuture.runAsync(
            () -> {
              TaskExecutionResult result;
              try {
                result = executeInternal(context, startTime);
              } catch (RuntimeException e) {
                long executionTime = System.currentTimeMillis() - startTime;
                logger.log(Level.WARNING, "[ARTHAS-EXEC-SYNC] Execution failed unexpectedly", e);
                result =
                    buildFailureResult(
                        ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED,
                        "Arthas exec_sync failed: " + safeMessage(e),
                        null,
                        null,
                        executionTime,
                        null,
                        null,
                        0,
                        0,
                        0,
                        currentArthasState(),
                        isTunnelReady());

              }
              future.complete(result);
            });

    return future;
  }

  private TaskExecutionResult executeInternal(TaskExecutionContext context, long startTime) {
    if (arthasIntegration == null) {
      return buildFailureResult(
          ArthasTaskProtocol.ErrorCode.ARTHAS_NOT_CONFIGURED,
          "ArthasIntegration is not configured",
          null,
          null,
          elapsed(startTime),
          null,
          null,
          0,
          0,
          0,
          currentArthasState(),
          /* tunnelReady= */ false);
    }

    ArthasIntegration integration = requireIntegration();
    String command = context.getStringParameter(ArthasTaskProtocol.ParameterKey.COMMAND, "").trim();

    if (command.isEmpty()) {
      return buildFailureResult(
          ArthasTaskProtocol.ErrorCode.INVALID_PARAMETERS,
          "Parameter 'command' is required",
          command,
          null,
          elapsed(startTime),
          null,
          null,
          0,
          0,
          0,
          currentArthasState(),
          isTunnelReady());
    }

    long timeoutMillis = resolveEffectiveTimeout(context);
    if (timeoutMillis <= 0) {
      return buildTimeoutResult(
          ArthasTaskProtocol.ErrorCode.COMMAND_TIMEOUT,
          "No execution time left for arthas_exec_sync",
          command,
          null,
          elapsed(startTime),
          null,
          null,
          0,
          0,
          0,
          currentArthasState(),
          isTunnelReady());
    }

    long resultLimitBytes =
        context.getLongParameter(
            ArthasTaskProtocol.ParameterKey.RESULT_LIMIT_BYTES, DEFAULT_RESULT_LIMIT_BYTES);
    if (resultLimitBytes <= 0) {
      return buildFailureResult(
          ArthasTaskProtocol.ErrorCode.INVALID_PARAMETERS,
          "Parameter 'result_limit_bytes' must be greater than 0",
          command,
          null,
          elapsed(startTime),
          null,
          null,
          0,
          0,
          0,
          currentArthasState(),
          isTunnelReady());
    }

    boolean autoAttach =
        context.getBooleanParameter(ArthasTaskProtocol.ParameterKey.AUTO_ATTACH, true);
    boolean requireTunnelReady =
        context.getBooleanParameter(
            ArthasTaskProtocol.ParameterKey.REQUIRE_TUNNEL_READY,
            DEFAULT_REQUIRE_TUNNEL_READY);
    @Nullable String sessionId =
        emptyToNull(context.getStringParameter(ArthasTaskProtocol.ParameterKey.SESSION_ID, ""));
    @Nullable String userId =
        emptyToNull(context.getStringParameter(ArthasTaskProtocol.ParameterKey.USER_ID, ""));
    @Nullable Object authSubject =
        context.getParameters().get(ArthasTaskProtocol.ParameterKey.AUTH_SUBJECT);
    @Nullable TaskStatusEmitter statusEmitter = context.getStatusEmitter();

    logger.log(
        Level.INFO,
        "[ARTHAS-EXEC-SYNC] Starting execution: taskId={0}, command={1}, timeoutMs={2}, autoAttach={3}, requireTunnelReady={4}",
        new Object[] {
          context.getTaskId(), abbreviate(command), timeoutMillis, autoAttach, requireTunnelReady
        });

    if (statusEmitter != null) {
      statusEmitter.running("Arthas exec_sync is preparing execution context");
    }

    ReadyCheck readyCheck = ensureReady(context, command, timeoutMillis, autoAttach, requireTunnelReady);
    if (!readyCheck.proceed()) {
      return readyCheck.getFailureResult();
    }

    if (statusEmitter != null) {
      statusEmitter.running("Arthas exec_sync is invoking structured bridge");
    }

    ArthasStructuredCommandBridge bridge = integration.getStructuredCommandBridge();

    StructuredExecResult bridgeResult;
    try {
      bridgeResult = bridge.executeSync(command, timeoutMillis, sessionId, authSubject, userId);
    } catch (RuntimeException e) {
      String errorCode = parseBridgeErrorCode(e);
      String errorMessage = parseBridgeErrorMessage(e);
      if (ArthasTaskProtocol.ErrorCode.COMMAND_TIMEOUT.equals(errorCode)) {
        return buildTimeoutResult(
            errorCode,
            errorMessage,
            command,
            sessionId,
            elapsed(startTime),
            null,
            null,
            0,
            0,
            0,
            currentArthasState(),
            isTunnelReady());
      }
      return buildFailureResult(
          errorCode,
          errorMessage,
          command,
          sessionId,
          elapsed(startTime),
          null,
          null,
          0,
          0,
          0,
          currentArthasState(),
          isTunnelReady());
    }

    if (isOverLimit(bridgeResult.getRawJson(), resultLimitBytes)) {
      return buildFailureResult(
          ArthasTaskProtocol.ErrorCode.RESULT_TOO_LARGE,
          String.format(
              Locale.ROOT,
              "Arthas result exceeds limit: limit=%d bytes",
              resultLimitBytes),
          command,
          bridgeResult.getSessionId(),
          elapsed(startTime),
          Collections.emptyMap(),
          null,
          bridgeResult.getBridgeInitTimeMillis(),
          bridgeResult.getInvokeTimeMillis(),
          bridgeResult.getSerializationTimeMillis(),
          currentArthasState(),
          isTunnelReady());

    }

    long executionTime = elapsed(startTime);
    if (bridgeResult.isTimeout()) {
      return buildTimeoutResult(
          Objects.requireNonNull(
              firstNonBlank(bridgeResult.getErrorCode(), ArthasTaskProtocol.ErrorCode.COMMAND_TIMEOUT)),
          Objects.requireNonNull(
              firstNonBlank(bridgeResult.getErrorMessage(), "Arthas command timeout")),
          command,
          bridgeResult.getSessionId(),
          executionTime,
          bridgeResult.getPayload(),
          bridgeResult.getRawJson(),
          bridgeResult.getBridgeInitTimeMillis(),
          bridgeResult.getInvokeTimeMillis(),
          bridgeResult.getSerializationTimeMillis(),
          currentArthasState(),
          isTunnelReady());
    }

    if (!bridgeResult.isSuccess()) {
      return buildFailureResult(
          Objects.requireNonNull(
              firstNonBlank(
                  bridgeResult.getErrorCode(),
                  ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED)),
          Objects.requireNonNull(
              firstNonBlank(bridgeResult.getErrorMessage(), "Arthas command execution failed")),
          command,
          bridgeResult.getSessionId(),
          executionTime,
          bridgeResult.getPayload(),
          bridgeResult.getRawJson(),
          bridgeResult.getBridgeInitTimeMillis(),
          bridgeResult.getInvokeTimeMillis(),
          bridgeResult.getSerializationTimeMillis(),
          currentArthasState(),
          isTunnelReady());
    }

    String resultJson =
        buildEnvelopeJson(
            /* success= */ true,
            /* timeout= */ false,
            null,
            null,
            command,
            bridgeResult.getSessionId(),
            bridgeResult.getPayload(),
            bridgeResult.getRawJson(),
            bridgeResult.getBridgeInitTimeMillis(),
            bridgeResult.getInvokeTimeMillis(),
            bridgeResult.getSerializationTimeMillis(),
            executionTime,
            currentArthasState(),
            isTunnelReady());

    return TaskExecutionResult.success(resultJson, executionTime);
  }

  private ReadyCheck ensureReady(
      TaskExecutionContext context,
      String command,
      long timeoutMillis,
      boolean autoAttach,
      boolean requireTunnelReady) {
    ArthasIntegration integration = requireIntegration();
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
                ArthasTaskProtocol.ErrorCode.ARTHAS_NOT_RUNNING,
                "Arthas is not running and auto_attach is disabled",
                command,
                null,
                0,
                null,
                null,
                0,
                0,
                0,
                currentArthasState(),
                isTunnelReady()));

      }
      return waitForLocalRunning(context, command, timeoutMillis);
    }

    if (!autoAttach) {
      return ReadyCheck.fail(
          buildFailureResult(
              mapReadinessErrorCode(readiness),
              buildReadinessMessage(readiness),
              command,
              null,
              0,
              null,
              null,
              0,
              0,
              0,
              currentArthasState(),
              isTunnelReady()));

    }

    cleanupUnhealthyForAttach(timeoutMillis, context.getTaskId());
    return waitForTerminalReady(context, command, timeoutMillis);
  }

  private ReadyCheck waitForLocalRunning(
      TaskExecutionContext context, String command, long timeoutMillis) {
    ArthasIntegration integration = requireIntegration();
    ArthasLifecycleManager manager = integration.getLifecycleManager();
    ReadyCheck startCheck = startIfNeeded(context, command, /* requireTunnelReady= */ false);

    if (!startCheck.proceed()) {
      return startCheck;
    }

    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (System.currentTimeMillis() < deadline) {
      if (manager.isRunning()) {
        return ReadyCheck.ok();
      }
      if (manager.getState() == ArthasLifecycleManager.State.STOPPED) {
        return ReadyCheck.fail(
            buildFailureResult(
                ArthasTaskProtocol.ErrorCode.ARTHAS_START_FAILED,
                "Arthas failed to enter RUNNING state",
                command,
                null,
                0,
                null,
                null,
                0,
                0,
                0,
                currentArthasState(),
                isTunnelReady()));

      }
      sleepQuietly();
    }

    return ReadyCheck.fail(
        buildTimeoutResult(
            ArthasTaskProtocol.ErrorCode.COMMAND_TIMEOUT,
            String.format(Locale.ROOT, "Waiting Arthas running timed out after %dms", timeoutMillis),
            command,
            null,
            0,
            null,
            null,
            0,
            0,
            0,
            currentArthasState(),
            isTunnelReady()));
  }

  private ReadyCheck waitForTerminalReady(
      TaskExecutionContext context, String command, long timeoutMillis) {
    ArthasIntegration integration = requireIntegration();
    ReadyCheck startCheck = startIfNeeded(context, command, /* requireTunnelReady= */ true);
    if (!startCheck.proceed()) {
      return startCheck;
    }

    ArthasReadinessGate.Result result;
    try {
      result =
          integration
              .getReadinessGate()
              .awaitTerminalReady(Duration.ofMillis(timeoutMillis))
              .get();

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return ReadyCheck.fail(
          buildFailureResult(
              ArthasTaskProtocol.ErrorCode.INTERRUPTED,
              "Interrupted while waiting for Arthas to become ready",
              command,
              null,
              0,
              null,
              null,
              0,
              0,
              0,
              currentArthasState(),
              isTunnelReady()));

    } catch (java.util.concurrent.ExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      if (cause instanceof TimeoutException) {
        return ReadyCheck.fail(
            buildTimeoutResult(
                ArthasTaskProtocol.ErrorCode.COMMAND_TIMEOUT,
                String.format(Locale.ROOT, "Waiting Arthas ready timed out after %dms", timeoutMillis),
                command,
                null,
                0,
                null,
                null,
                0,
                0,
                0,
                currentArthasState(),
                isTunnelReady()));
      }
      return ReadyCheck.fail(
          buildFailureResult(
              ArthasTaskProtocol.ErrorCode.ARTHAS_START_FAILED,
              "Failed while waiting for Arthas ready: " + safeMessage(cause),
              command,
              null,
              0,
              null,
              null,
              0,
              0,
              0,
              currentArthasState(),
              isTunnelReady()));

    }

    if (result.isTerminalReady()) {
      return ReadyCheck.ok();
    }

    String errorCode = mapReadinessErrorCode(result);
    String errorMessage = buildReadinessMessage(result);
    if (ArthasTaskProtocol.ErrorCode.TUNNEL_NOT_READY.equals(errorCode)
        || ArthasTaskProtocol.ErrorCode.ARTHAS_NOT_READY.equals(errorCode)
        || ArthasTaskProtocol.ErrorCode.ARTHAS_NOT_RUNNING.equals(errorCode)) {
      return ReadyCheck.fail(
          buildFailureResult(
              errorCode,
              errorMessage,
              command,
              null,
              0,
              null,
              null,
              0,
              0,
              0,
              currentArthasState(),
              isTunnelReady()));

    }

    return ReadyCheck.fail(
        buildFailureResult(
            ArthasTaskProtocol.ErrorCode.ARTHAS_NOT_READY,
            errorMessage,
            command,
            null,
            0,
            null,
            null,
            0,
            0,
            0,
            currentArthasState(),
            isTunnelReady()));

  }

  private ReadyCheck startIfNeeded(
      TaskExecutionContext context, String command, boolean requireTunnelReady) {
    ArthasIntegration integration = requireIntegration();
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
              ArthasTaskProtocol.ErrorCode.ARTHAS_NOT_READY,
              "Arthas is stopping, cannot execute command now",
              command,
              null,
              0,
              null,
              null,
              0,
              0,
              0,
              currentArthasState(),
              isTunnelReady()));

    }

    ScheduledExecutorService scheduler = context.getScheduler();
    if (scheduler == null) {
      return ReadyCheck.fail(
          buildFailureResult(
              ArthasTaskProtocol.ErrorCode.NO_SCHEDULER,
              "No scheduler available for Arthas auto_attach",
              command,
              null,
              0,
              null,
              null,
              0,
              0,
              0,
              currentArthasState(),
              isTunnelReady()));

    }

    ArthasLifecycleManager.StartResult startResult = manager.tryStart(scheduler);
    if (!startResult.isSuccess() && manager.getState() != ArthasLifecycleManager.State.STARTING) {
      return ReadyCheck.fail(
          buildFailureResult(
              ArthasTaskProtocol.ErrorCode.ARTHAS_START_FAILED,
              "Failed to start Arthas for exec_sync: " + startResult.getErrorMessage(),
              command,
              null,
              0,
              null,
              null,
              0,
              0,
              0,
              currentArthasState(),
              isTunnelReady()));

    }

    logger.log(
        Level.INFO,
        "[ARTHAS-EXEC-SYNC] Auto-attach requested, waiting for {0}",
        requireTunnelReady ? "terminal ready" : "local running");
    return ReadyCheck.ok();
  }

  private void cleanupUnhealthyForAttach(long timeoutMillis, String taskId) {
    long gracePeriod = calculateGracePeriod(timeoutMillis);
    requireIntegration().cleanupIfUnhealthyForAttach(gracePeriod, "exec_sync:" + taskId);
  }

  private static long calculateGracePeriod(long effectiveTimeout) {
    long remaining = effectiveTimeout - RESTART_BUDGET_MILLIS;
    return Math.min(MAX_GRACE_PERIOD_MILLIS, Math.max(MIN_GRACE_PERIOD_MILLIS, remaining));
  }

  private static long resolveEffectiveTimeout(TaskExecutionContext context) {
    long requested =
        context.getLongParameter(
            ArthasTaskProtocol.ParameterKey.TIMEOUT_MILLIS, DEFAULT_EXEC_TIMEOUT_MILLIS);
    long taskLimit = context.getEffectiveTimeoutMillis();
    if (taskLimit > 0) {
      return Math.min(requested, taskLimit);
    }
    return requested;
  }

  private static TaskExecutionResult buildFailureResult(
      String errorCode,
      String errorMessage,
      @Nullable String command,
      @Nullable String sessionId,
      long executionTime,
      @Nullable Map<String, Object> payload,
      @Nullable String rawJson,
      long bridgeInitTimeMillis,
      long invokeTimeMillis,
      long serializationTimeMillis,
      @Nullable String arthasState,
      boolean tunnelReady) {

    String resultJson =
        buildEnvelopeJson(
            /* success= */ false,
            /* timeout= */ false,
            errorCode,
            errorMessage,
            command,
            sessionId,
            payload,
            rawJson,
            bridgeInitTimeMillis,
            invokeTimeMillis,
            serializationTimeMillis,
            executionTime,
            arthasState,
            tunnelReady);

    return TaskExecutionResult.builder()
        .status(TaskExecutionResult.Status.FAILED)
        .errorCode(errorCode)
        .errorMessage(errorMessage)
        .resultJson(resultJson)
        .executionTimeMillis(executionTime)
        .completedAtMillis(System.currentTimeMillis())
        .build();
  }

  private static TaskExecutionResult buildTimeoutResult(
      String errorCode,
      String errorMessage,
      @Nullable String command,
      @Nullable String sessionId,
      long executionTime,
      @Nullable Map<String, Object> payload,
      @Nullable String rawJson,
      long bridgeInitTimeMillis,
      long invokeTimeMillis,
      long serializationTimeMillis,
      @Nullable String arthasState,
      boolean tunnelReady) {

    String resultJson =
        buildEnvelopeJson(
            /* success= */ false,
            /* timeout= */ true,
            errorCode,
            errorMessage,
            command,
            sessionId,
            payload,
            rawJson,
            bridgeInitTimeMillis,
            invokeTimeMillis,
            serializationTimeMillis,
            executionTime,
            arthasState,
            tunnelReady);

    return TaskExecutionResult.builder()
        .status(TaskExecutionResult.Status.TIMEOUT)
        .errorCode(errorCode)
        .errorMessage(errorMessage)
        .resultJson(resultJson)
        .executionTimeMillis(executionTime)
        .completedAtMillis(System.currentTimeMillis())
        .build();
  }

  private static String buildEnvelopeJson(
      boolean success,
      boolean timeout,
      @Nullable String errorCode,
      @Nullable String errorMessage,
      @Nullable String command,
      @Nullable String sessionId,
      @Nullable Map<String, Object> payload,
      @Nullable String rawJson,
      long bridgeInitTimeMillis,
      long invokeTimeMillis,
      long serializationTimeMillis,
      long executionTimeMillis,
      @Nullable String arthasState,
      boolean tunnelReady) {

    Map<String, Object> root = new LinkedHashMap<>();
    root.put(ArthasTaskProtocol.ResultField.SUCCESS, success);
    root.put(ArthasTaskProtocol.ResultField.TASK_TYPE, TASK_TYPE);
    root.put(ArthasTaskProtocol.ResultField.COMMAND, command);
    root.put(ArthasTaskProtocol.ResultField.SESSION_ID, sessionId);
    root.put(ArthasTaskProtocol.ResultField.TIMEOUT, timeout);
    root.put(ArthasTaskProtocol.ResultField.ERROR_CODE, valueOrEmpty(errorCode));
    root.put(ArthasTaskProtocol.ResultField.ERROR_MESSAGE, valueOrEmpty(errorMessage));
    root.put(
        ArthasTaskProtocol.ResultField.PAYLOAD,
        payload != null ? payload : new LinkedHashMap<String, Object>());
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

  private static boolean isOverLimit(@Nullable String rawJson, long limitBytes) {
    return rawJson != null && rawJson.getBytes(StandardCharsets.UTF_8).length > limitBytes;
  }

  private static long elapsed(long startTime) {
    return Math.max(0, System.currentTimeMillis() - startTime);
  }

  private static void sleepQuietly() {
    try {
      Thread.sleep(CHECK_INTERVAL_MILLIS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static String parseBridgeErrorCode(RuntimeException e) {
    String message = e.getMessage();
    if (message != null) {
      int idx = message.indexOf(": ");
      if (idx > 0) {
        return message.substring(0, idx);
      }
    }
    return ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED;
  }

  private static String parseBridgeErrorMessage(RuntimeException e) {
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

  private static String buildReadinessMessage(ArthasReadinessGate.Result readiness) {
    return readiness.toHumanMessage();
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

  @Nullable
  private static String emptyToNull(@Nullable String value) {
    return value == null || value.trim().isEmpty() ? null : value;
  }

  private ArthasIntegration requireIntegration() {
    return Objects.requireNonNull(arthasIntegration, "arthasIntegration");
  }

  @Nullable
  private String currentArthasState() {
    return arthasIntegration != null ? arthasIntegration.getLifecycleManager().getState().name() : null;
  }

  private boolean isTunnelReady() {
    return arthasIntegration != null && arthasIntegration.isTunnelReady();
  }

  private static String valueOrEmpty(@Nullable String value) {
    return value != null ? value : "";
  }

  @Nullable
  private static String firstNonBlank(@Nullable String first, @Nullable String second) {
    if (first != null && !first.trim().isEmpty()) {
      return first;
    }
    return second;
  }

  private static String safeMessage(Throwable throwable) {
    String message = throwable.getMessage();
    return message != null && !message.trim().isEmpty()
        ? message
        : throwable.getClass().getSimpleName();
  }

  private static String abbreviate(String command) {
    if (command.length() <= 160) {
      return command;
    }
    return command.substring(0, 160) + "...";
  }

  private static final class ReadyCheck {
    @Nullable private final TaskExecutionResult result;

    private ReadyCheck(@Nullable TaskExecutionResult result) {
      this.result = result;
    }

    private static ReadyCheck ok() {
      return new ReadyCheck(null);
    }

    private static ReadyCheck fail(TaskExecutionResult result) {
      return new ReadyCheck(Objects.requireNonNull(result, "result"));
    }

    private boolean proceed() {
      return result == null;
    }

    private TaskExecutionResult getFailureResult() {
      return Objects.requireNonNull(result, "result");
    }
  }
}
