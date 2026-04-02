package io.opentelemetry.sdk.extension.controlplane.task.executor;

import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasAsyncSessionSnapshot;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasIntegration;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasStructuredCommandBridge;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasTaskProtocol;
import io.opentelemetry.sdk.extension.controlplane.arthas.StructuredAsyncResult;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

/** Arthas 异步会话关闭执行器。 */
public final class ArthasSessionCloseExecutor implements TaskExecutor {

  public static final String TASK_TYPE = ArthasTaskProtocol.TaskType.SESSION_CLOSE;

  @Nullable private final ArthasIntegration arthasIntegration;

  public ArthasSessionCloseExecutor(@Nullable ArthasIntegration arthasIntegration) {
    this.arthasIntegration = arthasIntegration;
  }

  @Override
  public String getTaskType() {
    return TASK_TYPE;
  }

  @Override
  public String getDescription() {
    return "Arthas session close executor - closes an async Arthas session and cleans local state";
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

    String sessionId =
        ArthasAsyncExecutorSupport.emptyToNull(
            context.getStringParameter(ArthasTaskProtocol.ParameterKey.SESSION_ID, ""));
    if (sessionId == null) {
      return ArthasAsyncExecutorSupport.buildFailureResult(
          TASK_TYPE,
          ArthasTaskProtocol.ErrorCode.INVALID_PARAMETERS,
          "Parameter 'session_id' is required",
          null,
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
          "No execution time left for arthas_session_close",
          null,
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

    ArthasAsyncSessionSnapshot snapshot = null;
    try {
      snapshot = integration.getSessionRegistry().requireActiveSession(sessionId);
    } catch (RuntimeException e) {
      String errorCode = ArthasAsyncExecutorSupport.parseBridgeErrorCode(e);
      if (!ArthasTaskProtocol.ErrorCode.SESSION_NOT_FOUND.equals(errorCode)) {
        return ArthasAsyncExecutorSupport.buildFailureResult(
            TASK_TYPE,
            errorCode,
            ArthasAsyncExecutorSupport.parseBridgeErrorMessage(e),
            null,
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
    }

    ArthasStructuredCommandBridge bridge = integration.getStructuredCommandBridge();
    StructuredAsyncResult bridgeResult = bridge.closeSession(sessionId);
    if (!bridgeResult.isSuccess()) {
      return ArthasAsyncExecutorSupport.buildFailureResult(
          TASK_TYPE,
          bridgeResult.getErrorCode() != null
              ? bridgeResult.getErrorCode()
              : ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED,
          bridgeResult.getErrorMessage() != null
              ? bridgeResult.getErrorMessage()
              : "Failed to close Arthas session",
          snapshot != null ? snapshot.getCurrentCommand() : null,
          sessionId,
          snapshot != null ? snapshot.getConsumerId() : null,
          snapshot != null ? ArthasAsyncExecutorSupport.snapshotToMap(snapshot) : null,
          bridgeResult.getPayload(),
          ArthasAsyncExecutorSupport.elapsed(startTime),
          bridgeResult.getBridgeInitTimeMillis(),
          bridgeResult.getInvokeTimeMillis(),
          bridgeResult.getSerializationTimeMillis(),
          ArthasAsyncExecutorSupport.currentArthasState(integration),
          integration.isTunnelReady());
    }

    ArthasAsyncSessionSnapshot closedSnapshot = integration.getSessionRegistry().markClosed(sessionId);
    integration.getSessionRegistry().remove(sessionId);

    Map<String, Object> job =
        ArthasAsyncExecutorSupport.buildJobMap(
            /* accepted= */ false,
            closedSnapshot.getState(),
            closedSnapshot.getCurrentCommand(),
            closedSnapshot.getCurrentJobId(),
            closedSnapshot.getCurrentJobStatus());

    job.put(ArthasTaskProtocol.ResultField.CLOSED, true);

    return ArthasAsyncExecutorSupport.buildSuccessResult(
        TASK_TYPE,
        closedSnapshot.getCurrentCommand(),
        sessionId,
        closedSnapshot.getConsumerId(),
        ArthasAsyncExecutorSupport.snapshotToMap(closedSnapshot),
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
