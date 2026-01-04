package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Arthas 就绪门闩（Readiness Gate）。
 *
 * <p>目的：将“可交互就绪”的判定/等待从各个调用点（如 `TERMINAL_OPEN`、`arthas_attach`）收敛到单一位置，
 * 避免重复 if/else、避免不同模块对“成功/就绪”的语义不一致。
 *
 * <p>该类只依赖 `ArthasStateEventBus` 的事实快照，不直接操作 Arthas 启停（遵循单一职责）。
 */
public final class ArthasReadinessGate {

  /**
   * 就绪等级。
   *
   * <p>注意：此处表达的是“能力（Capability）就绪”，而非任务成功与否。
   */
  public enum Readiness {
    /** Arthas 本地可用 + Tunnel 已注册，可创建 Terminal session */
    TERMINAL_READY,
    /** Arthas 本地可用，但 Tunnel 未就绪（未连接/未注册） */
    ARTHAS_READY_BUT_TUNNEL_NOT_READY,
    /** Tunnel 已就绪，但 Arthas 本地不可用（STARTING/STOPPED/STOPPING） */
    TUNNEL_READY_BUT_ARTHAS_NOT_READY,
    /** 两者都不可用 */
    NOT_READY
  }

  /** 不可用原因（用于拒绝/诊断）。 */
  public enum ReasonCode {
    NONE,
    TUNNEL_NOT_CONNECTED,
    TUNNEL_NOT_REGISTERED,
    ARTHAS_STARTING,
    ARTHAS_STOPPING,
    ARTHAS_STOPPED
  }

  /** 就绪快照（用于日志/诊断，避免到处拼字符串）。 */
  public static final class Snapshot {
    private final boolean tunnelConnected;
    private final boolean tunnelRegistered;
    private final ArthasLifecycleManager.State arthasState;

    Snapshot(boolean tunnelConnected, boolean tunnelRegistered, ArthasLifecycleManager.State arthasState) {
      this.tunnelConnected = tunnelConnected;
      this.tunnelRegistered = tunnelRegistered;
      this.arthasState = arthasState;
    }

    public boolean isTunnelConnected() {
      return tunnelConnected;
    }

    public boolean isTunnelRegistered() {
      return tunnelRegistered;
    }

    public ArthasLifecycleManager.State getArthasState() {
      return arthasState;
    }

    @Override
    public String toString() {
      return "Snapshot{tunnelConnected="
          + tunnelConnected
          + ", tunnelRegistered="
          + tunnelRegistered
          + ", arthasState="
          + arthasState
          + '}';
    }
  }

  /** 评估结果。 */
  public static final class Result {
    private final Readiness readiness;
    private final ReasonCode reasonCode;
    private final Snapshot snapshot;

    Result(Readiness readiness, ReasonCode reasonCode, Snapshot snapshot) {
      this.readiness = readiness;
      this.reasonCode = reasonCode;
      this.snapshot = snapshot;
    }

    public Readiness getReadiness() {
      return readiness;
    }

    public ReasonCode getReasonCode() {
      return reasonCode;
    }

    public Snapshot getSnapshot() {
      return snapshot;
    }

    public boolean isTerminalReady() {
      return readiness == Readiness.TERMINAL_READY;
    }

    /**
     * 用于向上游返回的统一错误码（便于服务端聚合统计）。
     */
    public String toErrorCode() {
      if (isTerminalReady()) {
        return "OK";
      }
      switch (reasonCode) {
        case TUNNEL_NOT_CONNECTED:
          return "TUNNEL_NOT_CONNECTED";
        case TUNNEL_NOT_REGISTERED:
          return "TUNNEL_NOT_REGISTERED";
        case ARTHAS_STARTING:
        case ARTHAS_STOPPING:
        case ARTHAS_STOPPED:
          return "ARTHAS_NOT_RUNNING";
        case NONE:
          return "NOT_READY";
      }
      throw new AssertionError("Unexpected reasonCode: " + reasonCode);
    }

    public String toHumanMessage() {
      if (isTerminalReady()) {
        return "Terminal ready";
      }
      return String.format(Locale.ROOT, "Terminal not ready: %s, snapshot=%s", reasonCode, snapshot);
    }
  }

  private final ArthasStateEventBus stateEventBus;

  public ArthasReadinessGate(ArthasStateEventBus stateEventBus, ArthasLifecycleManager lifecycleManager) {
    this.stateEventBus = Objects.requireNonNull(stateEventBus, "stateEventBus");
    // lifecycleManager 当前仅用于构造期的依赖校验：ReadinessGate 的事实来源是 stateEventBus。
    Objects.requireNonNull(lifecycleManager, "lifecycleManager");
  }

  public Result evaluateNow() {
    ArthasStateEventBus.State s = stateEventBus.getState();

    boolean tunnelConnected = s.isTunnelConnected();
    boolean tunnelRegistered = s.isTunnelRegistered();
    ArthasLifecycleManager.State arthasState = s.getArthasState();

    Snapshot snapshot = new Snapshot(tunnelConnected, tunnelRegistered, arthasState);

    boolean tunnelReady = tunnelConnected && tunnelRegistered;
    boolean arthasReady = arthasState == ArthasLifecycleManager.State.RUNNING || arthasState == ArthasLifecycleManager.State.IDLE;

    if (tunnelReady && arthasReady) {
      return new Result(Readiness.TERMINAL_READY, ReasonCode.NONE, snapshot);
    }

    // 细分原因，优先给出最贴近用户动作的原因
    ReasonCode reason = ReasonCode.NONE;

    if (!tunnelConnected) {
      reason = ReasonCode.TUNNEL_NOT_CONNECTED;
    } else if (!tunnelRegistered) {
      reason = ReasonCode.TUNNEL_NOT_REGISTERED;
    } else {
      // tunnel ready but arthas not ready
      switch (arthasState) {
        case STARTING:
          reason = ReasonCode.ARTHAS_STARTING;
          break;
        case STOPPING:
          reason = ReasonCode.ARTHAS_STOPPING;
          break;
        case STOPPED:
        case RUNNING:
        case IDLE:
          // 走到这里说明 arthasReady=false，因此 RUNNING/IDLE 理论上不会出现，保守兜底到 STOPPED
          reason = ReasonCode.ARTHAS_STOPPED;
          break;
      }
    }

    Readiness readiness;
    if (arthasReady && !tunnelReady) {
      readiness = Readiness.ARTHAS_READY_BUT_TUNNEL_NOT_READY;
    } else if (tunnelReady && !arthasReady) {
      readiness = Readiness.TUNNEL_READY_BUT_ARTHAS_NOT_READY;
    } else {
      readiness = Readiness.NOT_READY;
    }

    return new Result(readiness, reason, snapshot);
  }

  /**
   * 等待 Terminal 就绪。
   *
   * <p>设计约束：
   * <ul>
   *   <li>不负责触发启动（由调用方负责，如 `onArthasStartRequested()`）</li>
   *   <li>只等待就绪事实达成，且基于事件驱动（不轮询）</li>
   * </ul>
   */
  public CompletableFuture<Result> awaitTerminalReady(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");

    Result now = evaluateNow();
    if (now.isTerminalReady()) {
      return CompletableFuture.completedFuture(now);
    }

    // 终止条件：1) 达到 ready；2) 进入明确失败态（STOPPED/STOPPING 且 tunnel ready 已经满足或一直不满足）
    // 这里的策略：只要 Arthas 进入 STOPPED/STOPPING，就认为“本轮请求不可达”，直接结束等待。
    return stateEventBus
        .await(
            s -> {
              ArthasLifecycleManager.State st = s.getArthasState();
              boolean tunnelReady = s.isTunnelConnected() && s.isTunnelRegistered();
              boolean arthasReady = st == ArthasLifecycleManager.State.RUNNING || st == ArthasLifecycleManager.State.IDLE;
              if (tunnelReady && arthasReady) {
                return true;
              }
              // 快速失败：如果 Arthas 已 STOPPED/STOPPING，认为无法在当前窗口内变为可用
              return st == ArthasLifecycleManager.State.STOPPED || st == ArthasLifecycleManager.State.STOPPING;
            },
            timeout)
        .handle(
            (ok, err) -> {
              if (err == null) {
                return evaluateNow();
              }

              // timeout / other
              Result latest = evaluateNow();
              if (err instanceof java.util.concurrent.CompletionException && err.getCause() != null) {
                err = err.getCause();
              }
              if (err instanceof TimeoutException) {
                // 超时：返回最新快照，供上游决定如何 reject
                return latest;
              }
              return latest;
            });
  }

  /**
   * 便捷：用于 attach 任务判定“真正完成”的一致性语义。
   */
  public boolean isTunnelReadyNow() {
    ArthasStateEventBus.State s = stateEventBus.getState();
    return s.isTunnelConnected() && s.isTunnelRegistered();
  }

  /**
   * 便捷：用于 attach 任务判定“本地 Arthas ready”。
   */
  public boolean isArthasReadyNow() {
    ArthasLifecycleManager.State st = stateEventBus.getState().getArthasState();
    return st == ArthasLifecycleManager.State.RUNNING || st == ArthasLifecycleManager.State.IDLE;
  }

  @Nullable
  public String buildDiagnosticDetail(@Nullable String prefix) {
    Result r = evaluateNow();
    String p = prefix == null ? "" : (prefix + ": ");
    return p + r.toHumanMessage();
  }
}
