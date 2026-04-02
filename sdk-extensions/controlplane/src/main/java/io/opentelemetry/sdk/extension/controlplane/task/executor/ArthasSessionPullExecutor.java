package io.opentelemetry.sdk.extension.controlplane.task.executor;

import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasAsyncSessionSnapshot;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasIntegration;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasSessionInspection;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasStructuredCommandBridge;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasTaskProtocol;
import io.opentelemetry.sdk.extension.controlplane.arthas.StructuredAsyncResult;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

/** Arthas 异步会话拉取执行器。 */
public final class ArthasSessionPullExecutor implements TaskExecutor {

  public static final String TASK_TYPE = ArthasTaskProtocol.TaskType.SESSION_PULL;

  @Nullable private final ArthasIntegration arthasIntegration;

  public ArthasSessionPullExecutor(@Nullable ArthasIntegration arthasIntegration) {
    this.arthasIntegration = arthasIntegration;
  }

  @Override
  public String getTaskType() {
    return TASK_TYPE;
  }

  @Override
  public String getDescription() {
    return "Arthas session pull executor - fetches async Arthas result deltas for a session";
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
    String consumerId =
        ArthasAsyncExecutorSupport.emptyToNull(
            context.getStringParameter(ArthasTaskProtocol.ParameterKey.CONSUMER_ID, ""));
    if (sessionId == null || consumerId == null) {
      return ArthasAsyncExecutorSupport.buildFailureResult(
          TASK_TYPE,
          ArthasTaskProtocol.ErrorCode.INVALID_PARAMETERS,
          "Parameters 'session_id' and 'consumer_id' are required",
          null,
          sessionId,
          consumerId,
          null,
          null,
          ArthasAsyncExecutorSupport.elapsed(startTime),
          0,
          0,
          0,
          ArthasAsyncExecutorSupport.currentArthasState(integration),
          integration.isTunnelReady());
    }

    long waitTimeoutMillis =
        ArthasAsyncExecutorSupport.resolveEffectiveTimeout(
            context,
            ArthasTaskProtocol.ParameterKey.WAIT_TIMEOUT_MILLIS,
            ArthasAsyncExecutorSupport.DEFAULT_WAIT_TIMEOUT_MILLIS);
    long maxItems =
        ArthasAsyncExecutorSupport.resolvePositiveLong(
            context,
            ArthasTaskProtocol.ParameterKey.MAX_ITEMS,
            ArthasAsyncExecutorSupport.DEFAULT_PULL_MAX_ITEMS);
    long maxBytes =
        ArthasAsyncExecutorSupport.resolvePositiveLong(
            context,
            ArthasTaskProtocol.ParameterKey.MAX_BYTES,
            ArthasAsyncExecutorSupport.DEFAULT_PULL_MAX_BYTES);

    ArthasAsyncSessionSnapshot snapshot;
    try {
      snapshot = integration.getSessionRegistry().requireActiveSession(sessionId, consumerId);
    } catch (RuntimeException e) {
      return ArthasAsyncExecutorSupport.buildFailureResult(
          TASK_TYPE,
          ArthasAsyncExecutorSupport.parseBridgeErrorCode(e),
          ArthasAsyncExecutorSupport.parseBridgeErrorMessage(e),
          null,
          sessionId,
          consumerId,
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
    StructuredAsyncResult bridgeResult = pollUntilAvailable(bridge, sessionId, consumerId, waitTimeoutMillis);
    if (bridgeResult != null && !bridgeResult.isSuccess()) {
      return ArthasAsyncExecutorSupport.buildFailureResult(
          TASK_TYPE,
          bridgeResult.getErrorCode() != null
              ? bridgeResult.getErrorCode()
              : ArthasTaskProtocol.ErrorCode.PULL_RESULT_FAILED,
          bridgeResult.getErrorMessage() != null
              ? bridgeResult.getErrorMessage()
              : "Failed to pull Arthas async results",
          snapshot.getCurrentCommand(),
          sessionId,
          consumerId,
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
    boolean endOfStream = !inspection.hasForegroundJob() || ArthasAsyncExecutorSupport.isTerminalJobStatus(inspection.getJobStatus());
    List<Object> items = extractItems(bridgeResult);
    List<Object> trimmedItems =
        ArthasAsyncExecutorSupport.trimItemsToLimits(items, maxItems, maxBytes, TASK_TYPE);
    boolean hasMore = items.size() > trimmedItems.size();
    String nextCursor = hasMore ? String.valueOf(System.currentTimeMillis()) : null;

    ArthasAsyncSessionSnapshot updatedSnapshot =
        integration
            .getSessionRegistry()
            .updateAfterPull(sessionId, inspection.getJobId(), inspection.getJobStatus(), endOfStream);

    Map<String, Object> delta =
        ArthasAsyncExecutorSupport.buildDeltaMap(
            trimmedItems,
            hasMore,
            updatedSnapshot.isEndOfStream(),
            nextCursor,
            waitTimeoutMillis);

    return ArthasAsyncExecutorSupport.buildSuccessResult(
        TASK_TYPE,
        updatedSnapshot.getCurrentCommand(),
        sessionId,
        consumerId,
        ArthasAsyncExecutorSupport.snapshotToMap(updatedSnapshot),
        null,
        delta,
        bridgeResult != null ? bridgeResult.getPayload() : Collections.emptyMap(),
        bridgeResult != null ? bridgeResult.getRawJson() : null,
        ArthasAsyncExecutorSupport.elapsed(startTime),
        bridgeResult != null ? bridgeResult.getBridgeInitTimeMillis() : 0,
        bridgeResult != null ? bridgeResult.getInvokeTimeMillis() : 0,
        bridgeResult != null ? bridgeResult.getSerializationTimeMillis() : 0,
        ArthasAsyncExecutorSupport.currentArthasState(integration),
        integration.isTunnelReady());
  }

  @Nullable
  private static StructuredAsyncResult pollUntilAvailable(
      ArthasStructuredCommandBridge bridge,
      String sessionId,
      String consumerId,
      long waitTimeoutMillis) {

    long deadline = System.currentTimeMillis() + waitTimeoutMillis;
    StructuredAsyncResult lastResult = null;
    while (System.currentTimeMillis() <= deadline) {
      StructuredAsyncResult current = bridge.pullResults(sessionId, consumerId);
      lastResult = current;
      if (current != null) {
        return current;
      }
      try {
        Thread.sleep(ArthasAsyncExecutorSupport.CHECK_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    return lastResult;
  }

  @SuppressWarnings("unchecked")
  private static List<Object> extractItems(@Nullable StructuredAsyncResult bridgeResult) {
    if (bridgeResult == null) {
      return Collections.emptyList();
    }
    Object results = bridgeResult.getPayload().get("results");
    if (results instanceof List) {
      return (List<Object>) results;
    }
    return Collections.emptyList();
  }

  private ArthasIntegration requireIntegration() {
    return Objects.requireNonNull(arthasIntegration, "arthasIntegration");
  }
}
