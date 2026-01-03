/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 统一状态事件总线。
 *
 * <p>目标：
 *
 * <ul>
 *   <li>publish：集中发布状态事件
 *   <li>subscribe：订阅状态事件，支持无竞态 replay
 *   <li>await(predicate)：基于事件完成 CompletableFuture，替代 wait/notify 或轮询
 * </ul>
 */
public final class ArthasStateEventBus {

  private static final Logger logger = Logger.getLogger(ArthasStateEventBus.class.getName());

  /** 统一事件模型（后续可按需细分更多事件） */
  public enum Event {
    TUNNEL_CONNECTED,
    TUNNEL_DISCONNECTED,
    TUNNEL_REGISTERED,

    ARTHAS_STARTING,
    ARTHAS_STARTED,
    ARTHAS_STOPPING,
    ARTHAS_STOPPED
  }

  /**
   * 状态快照。
   *
   * <p>只保留 await/predicate 需要的最小集合，避免让总线变成“上帝对象”。
   */
  public static final class State {
    private final boolean tunnelConnected;
    private final boolean tunnelRegistered;
    private final ArthasLifecycleManager.State arthasState;

    private State(boolean tunnelConnected, boolean tunnelRegistered, ArthasLifecycleManager.State arthasState) {
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
      return "State{tunnelConnected="
          + tunnelConnected
          + ", tunnelRegistered="
          + tunnelRegistered
          + ", arthasState="
          + arthasState
          + '}';
    }

    private State withTunnelConnected(boolean connected) {
      return new State(connected, tunnelRegistered, arthasState);
    }

    private State withTunnelRegistered(boolean registered) {
      return new State(tunnelConnected, registered, arthasState);
    }

    private State withArthasState(ArthasLifecycleManager.State state) {
      return new State(tunnelConnected, tunnelRegistered, state);
    }
  }

  public interface Subscription extends AutoCloseable {
    @Override
    void close();
  }

  public interface Listener {
    void onEvent(Event event, State state, @Nullable String detail);
  }

  private final AtomicReference<State> state =
      new AtomicReference<>(
          new State(/* tunnelConnected= */ false,
              /* tunnelRegistered= */ false,
              /* arthasState= */ ArthasLifecycleManager.State.STOPPED));

  private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

  // 仅用于 timeout 调度；不依赖调用方的 scheduler，以免耦合。
  private final ScheduledExecutorService timeoutScheduler =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "otel-arthas-statebus");
            t.setDaemon(true);
            return t;
          });

  public State getState() {
    return Objects.requireNonNull(state.get(), "state");
  }

  /**
   * 订阅事件。
   *
   * @param listener 监听器
   * @param replay 是否回放当前状态（无竞态订阅的关键）
   */
  public Subscription subscribe(Listener listener, boolean replay) {
    Objects.requireNonNull(listener, "listener");
    listeners.add(listener);

    if (replay) {
      // 回放并非“把历史事件都重放”，而是基于 state 快照触发等价事件，让订阅不依赖时序。
      replayTo(listener);
    }

    return () -> listeners.remove(listener);
  }

  private void replayTo(Listener listener) {
    State s = getState();
    if (s.isTunnelConnected()) {
      safeInvoke(listener, Event.TUNNEL_CONNECTED, s, null);
    }
    if (s.isTunnelRegistered()) {
      safeInvoke(listener, Event.TUNNEL_REGISTERED, s, null);
    }

    ArthasLifecycleManager.State as = s.getArthasState();
    // 只回放“结果态”或关键态，避免噪声
    if (as == ArthasLifecycleManager.State.STARTING) {
      safeInvoke(listener, Event.ARTHAS_STARTING, s, null);
    } else if (as == ArthasLifecycleManager.State.RUNNING || as == ArthasLifecycleManager.State.IDLE) {
      safeInvoke(listener, Event.ARTHAS_STARTED, s, null);
    } else if (as == ArthasLifecycleManager.State.STOPPING) {
      safeInvoke(listener, Event.ARTHAS_STOPPING, s, null);
    } else if (as == ArthasLifecycleManager.State.STOPPED) {
      safeInvoke(listener, Event.ARTHAS_STOPPED, s, null);
    }
  }

  /** 发布 Tunnel connected */
  public void publishTunnelConnected() {
    updateAndPublish(Event.TUNNEL_CONNECTED, s -> s.withTunnelConnected(true), null);
  }

  public void publishTunnelDisconnected(@Nullable String reason) {
    updateAndPublish(
        Event.TUNNEL_DISCONNECTED,
        s -> s.withTunnelConnected(false).withTunnelRegistered(false),
        reason);
  }

  public void publishTunnelRegistered() {
    updateAndPublish(Event.TUNNEL_REGISTERED, s -> s.withTunnelRegistered(true), null);
  }

  public void publishArthasState(ArthasLifecycleManager.State newState) {
    Objects.requireNonNull(newState, "newState");

    Event event;
    switch (newState) {
      case STARTING:
        event = Event.ARTHAS_STARTING;
        break;
      case RUNNING:
      case IDLE:
        event = Event.ARTHAS_STARTED;
        break;
      case STOPPING:
        event = Event.ARTHAS_STOPPING;
        break;
      case STOPPED:
      default:
        event = Event.ARTHAS_STOPPED;
        break;
    }

    updateAndPublish(event, s -> s.withArthasState(newState), null);
  }

  private interface StateUpdater {
    State apply(State previous);
  }

  private void updateAndPublish(Event event, StateUpdater updater, @Nullable String detail) {
    State next = state.updateAndGet(updater::apply);
    for (Listener l : listeners) {
      safeInvoke(l, event, next, detail);
    }
  }

  private static void safeInvoke(Listener listener, Event event, State state, @Nullable String detail) {
    try {
      listener.onEvent(event, state, detail);
    } catch (RuntimeException e) {
      logger.log(Level.FINE, "ArthasStateEventBus listener threw", e);
    }
  }

  /**
   * await：等待 predicate 成立。
   *
   * <p>实现要点：
   * <ul>
   *   <li>先检查当前 state（避免已经达成但仍等待）
   *   <li>通过订阅事件驱动完成 future（不轮询）
   *   <li>timeout 到期则异常完成并解除订阅
   * </ul>
   */
  public CompletableFuture<State> await(Predicate<State> predicate, Duration timeout) {
    Objects.requireNonNull(predicate, "predicate");
    Objects.requireNonNull(timeout, "timeout");

    State current = state.get();
    if (predicate.test(current)) {
      return CompletableFuture.completedFuture(current);
    }

    CompletableFuture<State> future = new CompletableFuture<>();

    Listener listener =
        (event, s, detail) -> {
          if (!future.isDone() && predicate.test(s)) {
            future.complete(s);
          }
        };

    Subscription subscription = subscribe(listener, /* replay= */ false);

    // 防止并发：订阅后立刻再 check 一次（事件可能在 subscribe 前后之间发生）
    State after = state.get();
    if (predicate.test(after)) {
      subscription.close();
      future.complete(after);
      return future;
    }

    long timeoutMillis = Math.max(1, timeout.toMillis());
    @SuppressWarnings("FutureReturnValueIgnored")
    Object unusedTimeoutFuture =
        timeoutScheduler.schedule(
            () -> {
              if (future.completeExceptionally(
                  new TimeoutException("Timeout waiting for state predicate"))) {
                subscription.close();
              }
            },
            timeoutMillis,
            TimeUnit.MILLISECONDS);

    @SuppressWarnings("FutureReturnValueIgnored")
    Object unusedWhenComplete =
        future.whenComplete(
            (ok, err) -> {
              // best-effort 解绑
              try {
                subscription.close();
              } catch (RuntimeException e) {
                logger.log(Level.FINE, "Failed to close subscription", e);
              }
            });

    return future;
  }
}
