package io.opentelemetry.sdk.extension.controlplane.task.executor;

import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasAsyncSessionSnapshot;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasIntegration;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasSessionInspection;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasStructuredCommandBridge;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasTaskProtocol;
import io.opentelemetry.sdk.extension.controlplane.arthas.StructuredAsyncResult;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

/** Arthas 异步会话执行器。 */
public final class ArthasSessionExecExecutor implements TaskExecutor {

  public static final String TASK_TYPE = ArthasTaskProtocol.TaskType.SESSION_EXEC;

  @Nullable private final ArthasIntegration arthasIntegration;

  public ArthasSessionExecExecutor(@Nullable ArthasIntegration arthasIntegration) {
    this.arthasIntegration = arthasIntegration;
  }

  @Override
  public String getTaskType() {
    return TASK_TYPE;
  }

  @Override
  public String getDescription() {
    return "Arthas session exec executor - launches an async Arthas command in an existing session";
  }

  @Override
  public boolean isAvailable() {
    return arthasIntegration != null;
  }

  @Override
  public CompletableFuture<TaskExecutionResult> execute(TaskExecutionContext context) {
    return ArthasAsyncExecutorSupport.executeAsync(
        context,
        TASK_TYPE,
        startTime -> executeInternal(requireIntegration(), context, startTime));
  }

  private static TaskExecutionResult executeInternal(
      ArthasIntegration integration, TaskExecutionContext context, long startTime) {

    String command = context.getStringParameter(ArthasTaskProtocol.ParameterKey.COMMAND, "").trim();
    if (command.isEmpty()) {
      return ArthasAsyncExecutorSupport.buildFailureResult(
          TASK_TYPE,
          ArthasTaskProtocol.ErrorCode.INVALID_PARAMETERS,
          "Parameter 'command' is required",
          command,
          null,
          null,
          null,
          null,
          ArthasAsyncExecutorSupport.elapsed(startTime),
          0,
          0,
          0,
          ArthasAsyncExecutorSupport.currentArthasState(integration),
          integration.isTunnelReady());
    }

    String sessionId =
        ArthasAsyncExecutorSupport.emptyToNull(
            context.getStringParameter(ArthasTaskProtocol.ParameterKey.SESSION_ID, ""));
    if (sessionId == null) {
      return ArthasAsyncExecutorSupport.buildFailureResult(
          TASK_TYPE,
          ArthasTaskProtocol.ErrorCode.INVALID_PARAMETERS,
          "Parameter 'session_id' is required",
          command,
          null,
          null,
          null,
          null,
          ArthasAsyncExecutorSupport.elapsed(startTime),
          0,
          0,
          0,
          ArthasAsyncExecutorSupport.currentArthasState(integration),
          integration.isTunnelReady());
    }

    long timeoutMillis =
        ArthasAsyncExecutorSupport.resolveEffectiveTimeout(
            context,
            ArthasTaskProtocol.ParameterKey.TIMEOUT_MILLIS,
            ArthasAsyncExecutorSupport.DEFAULT_EXEC_TIMEOUT_MILLIS);
    if (timeoutMillis <= 0) {
      return ArthasAsyncExecutorSupport.buildTimeoutResult(
          TASK_TYPE,
          ArthasTaskProtocol.ErrorCode.COMMAND_TIMEOUT,
          "No execution time left for arthas_session_exec",
          command,
          sessionId,
          null,
          null,
          ArthasAsyncExecutorSupport.elapsed(startTime),
          0,
          0,
          0,
          ArthasAsyncExecutorSupport.currentArthasState(integration),
          integration.isTunnelReady());
    }

    boolean autoAttach =
        context.getBooleanParameter(ArthasTaskProtocol.ParameterKey.AUTO_ATTACH, true);
    boolean requireTunnelReady =
        context.getBooleanParameter(
            ArthasTaskProtocol.ParameterKey.REQUIRE_TUNNEL_READY,
            ArthasAsyncExecutorSupport.DEFAULT_REQUIRE_LOCAL_READY);
    @Nullable String userId =
        ArthasAsyncExecutorSupport.emptyToNull(
            context.getStringParameter(ArthasTaskProtocol.ParameterKey.USER_ID, ""));
    @Nullable Object authSubject =
        context.getParameters().get(ArthasTaskProtocol.ParameterKey.AUTH_SUBJECT);

    ArthasAsyncExecutorSupport.ReadyCheck readyCheck =
        ArthasAsyncExecutorSupport.ensureReady(
            integration,
            context,
            TASK_TYPE,
            command,
            timeoutMillis,
            autoAttach,
            requireTunnelReady);
    if (!readyCheck.proceed()) {
      return readyCheck.getFailureResult();
    }

    ArthasAsyncSessionSnapshot sessionSnapshot;
    try {
      sessionSnapshot = integration.getSessionRegistry().requireActiveSession(sessionId);
    } catch (RuntimeException e) {
      return ArthasAsyncExecutorSupport.buildFailureResult(
          TASK_TYPE,
          ArthasAsyncExecutorSupport.parseBridgeErrorCode(e),
          ArthasAsyncExecutorSupport.parseBridgeErrorMessage(e),
          command,
          sessionId,
          null,
          null,
          null,
          ArthasAsyncExecutorSupport.elapsed(startTime),
          0,
          0,
          0,
          ArthasAsyncExecutorSupport.currentArthasState(integration),
          integration.isTunnelReady());
    }

    ArthasStructuredCommandBridge bridge = integration.getStructuredCommandBridge();
    StructuredAsyncResult bridgeResult = bridge.executeAsync(command, sessionId, userId, authSubject);
    if (!bridgeResult.isSuccess()) {
      return ArthasAsyncExecutorSupport.buildFailureResult(
          TASK_TYPE,
          bridgeResult.getErrorCode() != null
              ? bridgeResult.getErrorCode()
              : ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED,
          bridgeResult.getErrorMessage() != null
              ? bridgeResult.getErrorMessage()
              : "Failed to execute Arthas async command",
          command,
          sessionId,
          sessionSnapshot.getConsumerId(),
          ArthasAsyncExecutorSupport.snapshotToMap(sessionSnapshot),
          bridgeResult.getPayload(),
          ArthasAsyncExecutorSupport.elapsed(startTime),
          bridgeResult.getBridgeInitTimeMillis(),
          bridgeResult.getInvokeTimeMillis(),
          bridgeResult.getSerializationTimeMillis(),
          ArthasAsyncExecutorSupport.currentArthasState(integration),
          integration.isTunnelReady());
    }

    ArthasSessionInspection inspection = bridge.inspectSession(sessionId);
    ArthasAsyncSessionSnapshot updatedSnapshot =
        integration
            .getSessionRegistry()
            .markExecuting(
                sessionId, command, inspection.getJobId(), inspection.getJobStatus());

    Map<String, Object> job =
        ArthasAsyncExecutorSupport.buildJobMap(
            /* accepted= */ true,
            updatedSnapshot.getState(),
            command,
            inspection.getJobId(),
            inspection.getJobStatus());

    return ArthasAsyncExecutorSupport.buildSuccessResult(
        TASK_TYPE,
        command,
        sessionId,
        updatedSnapshot.getConsumerId(),
        ArthasAsyncExecutorSupport.snapshotToMap(updatedSnapshot),
        job,
        null,
        bridgeResult.getPayload(),
        bridgeResult.getRawJson(),
        ArthasAsyncExecutorSupport.elapsed(startTime),
        bridgeResult.getBridgeInitTimeMillis(),
        bridgeResult.getInvokeTimeMillis(),
        bridgeResult.getSerializationTimeMillis(),
        ArthasAsyncExecutorSupport.currentArthasState(integration),
        integration.isTunnelReady());
  }

  private ArthasIntegration requireIntegration() {
    return Objects.requireNonNull(arthasIntegration, "arthasIntegration");
  }
}
