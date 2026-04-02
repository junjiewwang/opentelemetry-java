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

/** Arthas 异步会话中断执行器。 */
public final class ArthasSessionInterruptExecutor implements TaskExecutor {

  public static final String TASK_TYPE = ArthasTaskProtocol.TaskType.SESSION_INTERRUPT;

  @Nullable private final ArthasIntegration arthasIntegration;

  public ArthasSessionInterruptExecutor(@Nullable ArthasIntegration arthasIntegration) {
    this.arthasIntegration = arthasIntegration;
  }

  @Override
  public String getTaskType() {
    return TASK_TYPE;
  }

  @Override
  public String getDescription() {
    return "Arthas session interrupt executor - interrupts the current async Arthas job in a session";
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
          "No execution time left for arthas_session_interrupt",
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

    ArthasAsyncSessionSnapshot snapshot;
    try {
      snapshot = integration.getSessionRegistry().requireActiveSession(sessionId);
    } catch (RuntimeException e) {
      return ArthasAsyncExecutorSupport.buildFailureResult(
          TASK_TYPE,
          ArthasAsyncExecutorSupport.parseBridgeErrorCode(e),
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

    ArthasStructuredCommandBridge bridge = integration.getStructuredCommandBridge();
    StructuredAsyncResult bridgeResult = bridge.interruptJob(sessionId);
    if (!bridgeResult.isSuccess()) {
      return ArthasAsyncExecutorSupport.buildFailureResult(
          TASK_TYPE,
          bridgeResult.getErrorCode() != null
              ? bridgeResult.getErrorCode()
              : ArthasTaskProtocol.ErrorCode.ASYNC_JOB_INTERRUPTED,
          bridgeResult.getErrorMessage() != null
              ? bridgeResult.getErrorMessage()
              : "Failed to interrupt Arthas async job",
          snapshot.getCurrentCommand(),
          sessionId,
          snapshot.getConsumerId(),
          ArthasAsyncExecutorSupport.snapshotToMap(snapshot),
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
            .markInterrupted(sessionId, inspection.getJobId(), inspection.getJobStatus());

    Map<String, Object> job =
        ArthasAsyncExecutorSupport.buildJobMap(
            /* accepted= */ false,
            updatedSnapshot.getState(),
            updatedSnapshot.getCurrentCommand(),
            inspection.getJobId(),
            inspection.getJobStatus());

    job.put(ArthasTaskProtocol.ResultField.INTERRUPTED, true);

    return ArthasAsyncExecutorSupport.buildSuccessResult(
        TASK_TYPE,
        updatedSnapshot.getCurrentCommand(),
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
