package io.opentelemetry.sdk.extension.controlplane.task.executor;

import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasAsyncSessionSnapshot;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasIntegration;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasSessionRegistry;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasStructuredCommandBridge;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasTaskProtocol;
import io.opentelemetry.sdk.extension.controlplane.arthas.StructuredAsyncResult;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

/** Arthas 异步会话创建执行器。 */
public final class ArthasSessionOpenExecutor implements TaskExecutor {

  public static final String TASK_TYPE = ArthasTaskProtocol.TaskType.SESSION_OPEN;

  @Nullable private final ArthasIntegration arthasIntegration;

  public ArthasSessionOpenExecutor(@Nullable ArthasIntegration arthasIntegration) {
    this.arthasIntegration = arthasIntegration;
  }

  @Override
  public String getTaskType() {
    return TASK_TYPE;
  }

  @Override
  public String getDescription() {
    return "Arthas session open executor - creates a local async Arthas session";
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

    long timeoutMillis =
        ArthasAsyncExecutorSupport.resolveEffectiveTimeout(
            context,
            ArthasTaskProtocol.ParameterKey.TIMEOUT_MILLIS,
            ArthasAsyncExecutorSupport.DEFAULT_EXEC_TIMEOUT_MILLIS);
    if (timeoutMillis <= 0) {
      return ArthasAsyncExecutorSupport.buildTimeoutResult(
          TASK_TYPE,
          ArthasTaskProtocol.ErrorCode.COMMAND_TIMEOUT,
          "No execution time left for arthas_session_open",
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

    boolean autoAttach =
        context.getBooleanParameter(ArthasTaskProtocol.ParameterKey.AUTO_ATTACH, true);
    boolean requireTunnelReady =
        context.getBooleanParameter(
            ArthasTaskProtocol.ParameterKey.REQUIRE_TUNNEL_READY,
            ArthasAsyncExecutorSupport.DEFAULT_REQUIRE_LOCAL_READY);
    long ttlMillis =
        ArthasAsyncExecutorSupport.resolvePositiveLong(
            context,
            ArthasTaskProtocol.ParameterKey.TTL_MILLIS,
            ArthasAsyncExecutorSupport.DEFAULT_SESSION_TTL_MILLIS);
    long idleTimeoutMillis =
        ArthasAsyncExecutorSupport.resolvePositiveLong(
            context,
            ArthasTaskProtocol.ParameterKey.IDLE_TIMEOUT_MILLIS,
            ArthasAsyncExecutorSupport.DEFAULT_IDLE_TIMEOUT_MILLIS);
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
            null,
            timeoutMillis,
            autoAttach,
            requireTunnelReady);
    if (!readyCheck.proceed()) {
      return readyCheck.getFailureResult();
    }

    ArthasStructuredCommandBridge bridge = integration.getStructuredCommandBridge();
    StructuredAsyncResult bridgeResult = bridge.openSession(userId, authSubject);
    if (!bridgeResult.isSuccess()) {
      return ArthasAsyncExecutorSupport.buildFailureResult(
          TASK_TYPE,
          bridgeResult.getErrorCode() != null
              ? bridgeResult.getErrorCode()
              : ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED,
          bridgeResult.getErrorMessage() != null
              ? bridgeResult.getErrorMessage()
              : "Failed to create Arthas async session",
          null,
          bridgeResult.getSessionId(),
          bridgeResult.getConsumerId(),
          null,
          bridgeResult.getPayload(),
          ArthasAsyncExecutorSupport.elapsed(startTime),
          bridgeResult.getBridgeInitTimeMillis(),
          bridgeResult.getInvokeTimeMillis(),
          bridgeResult.getSerializationTimeMillis(),
          ArthasAsyncExecutorSupport.currentArthasState(integration),
          integration.isTunnelReady());
    }

    String sessionId = bridgeResult.getSessionId();
    String consumerId = bridgeResult.getConsumerId();
    if (sessionId == null || consumerId == null) {
      return ArthasAsyncExecutorSupport.buildFailureResult(
          TASK_TYPE,
          ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED,
          "Arthas createSession did not return sessionId or consumerId",
          null,
          sessionId,
          consumerId,
          null,
          bridgeResult.getPayload(),
          ArthasAsyncExecutorSupport.elapsed(startTime),
          bridgeResult.getBridgeInitTimeMillis(),
          bridgeResult.getInvokeTimeMillis(),
          bridgeResult.getSerializationTimeMillis(),
          ArthasAsyncExecutorSupport.currentArthasState(integration),
          integration.isTunnelReady());
    }

    ArthasSessionRegistry registry = integration.getSessionRegistry();
    ArthasAsyncSessionSnapshot snapshot =
        registry.registerSession(sessionId, consumerId, ttlMillis, idleTimeoutMillis);

    return ArthasAsyncExecutorSupport.buildSuccessResult(
        TASK_TYPE,
        null,
        sessionId,
        consumerId,
        ArthasAsyncExecutorSupport.snapshotToMap(snapshot),
        null,
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
    return java.util.Objects.requireNonNull(arthasIntegration, "arthasIntegration");
  }
}
