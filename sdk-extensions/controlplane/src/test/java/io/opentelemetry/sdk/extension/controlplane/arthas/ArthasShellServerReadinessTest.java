/*
 * Copyright The Open Telemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasReadinessGate.Readiness;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasReadinessGate.ReasonCode;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasStateEventBus.State;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArthasShellServerReadinessTest {

  private ArthasStateEventBus bus;
  private ArthasReadinessGate gate;
  @Nullable private String savedProbeEnabled;

  @BeforeEach
  void setUp() {
    bus = new ArthasStateEventBus();
    // Gate 构造需要 lifecycleManager，仅做非空校验；这里传一个轻量实例
    gate = new ArthasReadinessGate(bus,
        new ArthasLifecycleManager(ArthasConfig.builder().build(),
            new NoopLifecycleListener()));
    savedProbeEnabled = System.getProperty(ShellServerReadinessProbe.PROP_ENABLED);
    System.clearProperty(ShellServerReadinessProbe.PROP_ENABLED);
  }

  @AfterEach
  void tearDown() {
    if (savedProbeEnabled == null) {
      System.clearProperty(ShellServerReadinessProbe.PROP_ENABLED);
    } else {
      System.setProperty(ShellServerReadinessProbe.PROP_ENABLED, savedProbeEnabled);
    }
  }

  /** 隧道连接+注册 + Arthas 运行中，但不含 ShellServer listen。 */
  private void publishTunnelAndArthasReady() {
    bus.publishTunnelConnected();
    bus.publishTunnelRegistered();
    bus.publishArthasState(ArthasLifecycleManager.State.RUNNING);
  }

  @Test
  void notTerminalReadyWhenShellServerNotListening() {
    publishTunnelAndArthasReady();

    ArthasReadinessGate.Result result = gate.evaluateNow();

    assertThat(result.isTerminalReady()).isFalse();
    assertThat(result.getReadiness()).isEqualTo(Readiness.TERMINAL_NOT_LISTENING);
    assertThat(result.getReasonCode()).isEqualTo(ReasonCode.TERMINAL_NOT_LISTENING);
    assertThat(result.toErrorCode()).isEqualTo("TERMINAL_NOT_LISTENING");
    assertThat(result.getSnapshot().isShellServerReady()).isFalse();
  }

  @Test
  void terminalReadyWhenAllFourDimensionsSatisfied() {
    publishTunnelAndArthasReady();
    bus.publishShellServerReady(true);

    ArthasReadinessGate.Result result = gate.evaluateNow();

    assertThat(result.isTerminalReady()).isTrue();
    assertThat(result.getReadiness()).isEqualTo(Readiness.TERMINAL_READY);
    assertThat(result.getReasonCode()).isEqualTo(ReasonCode.NONE);
    assertThat(result.toErrorCode()).isEqualTo("OK");
  }

  @Test
  void tunnelRegisteredImpliesConnected() {
    // 现场 Bridge 会直接从 DISCONNECTED 跳到 REGISTERED，不发 CONNECTED 事件
    bus.publishTunnelRegistered();
    bus.publishArthasState(ArthasLifecycleManager.State.RUNNING);
    bus.publishShellServerReady(true);

    State s = bus.getState();
    assertThat(s.isTunnelConnected()).isTrue();
    assertThat(gate.evaluateNow().isTerminalReady()).isTrue();
  }

  @Test
  void awaitIsWokenByShellServerReadyPublish() throws Exception {
    publishTunnelAndArthasReady();
    CompletableFuture<ArthasReadinessGate.Result> waiting =
        gate.awaitTerminalReady(Duration.ofSeconds(3));
    assertThat(waiting).isNotDone();

    // 关键回归点：tunnel 状态稳定不变时，只有 publish 新事件才能唤醒 await
    bus.publishShellServerReady(true);

    assertThat(waiting.get(2, TimeUnit.SECONDS).isTerminalReady()).isTrue();
  }

  @Test
  void disconnectClearsShellServerReady() {
    publishTunnelAndArthasReady();
    bus.publishShellServerReady(true);
    assertThat(gate.evaluateNow().isTerminalReady()).isTrue();

    bus.publishTunnelDisconnected("test");

    assertThat(bus.getState().isShellServerReady()).isFalse();
    assertThat(gate.evaluateNow().isTerminalReady()).isFalse();
  }

  @Test
  void stoppingAndStartingClearStaleReady() {
    publishTunnelAndArthasReady();
    bus.publishShellServerReady(true);

    bus.publishArthasState(ArthasLifecycleManager.State.STOPPED);
    assertThat(bus.getState().isShellServerReady()).isFalse();

    // 新一轮启动不能残留上一次的 true
    bus.publishArthasState(ArthasLifecycleManager.State.STARTING);
    assertThat(bus.getState().isShellServerReady()).isFalse();
  }

  @Test
  void otherDimensionsPreservedWhenShellServerReadyChanges() {
    bus.publishTunnelRegistered();
    bus.publishArthasState(ArthasLifecycleManager.State.RUNNING);
    bus.publishShellServerReady(true);

    State s = bus.getState();
    assertThat(s.isTunnelConnected()).isTrue();
    assertThat(s.isTunnelRegistered()).isTrue();
    assertThat(s.getArthasState()).isEqualTo(ArthasLifecycleManager.State.RUNNING);
    assertThat(s.isShellServerReady()).isTrue();
  }

  @Test
  void existingReadinessSemanticsUnchanged() {
    // 隧道未连接、Arthas 运行中
    bus.publishArthasState(ArthasLifecycleManager.State.RUNNING);
    assertThat(gate.evaluateNow().getReadiness())
        .isEqualTo(Readiness.ARTHAS_READY_BUT_TUNNEL_NOT_READY);
    assertThat(gate.evaluateNow().getReasonCode())
        .isEqualTo(ReasonCode.TUNNEL_NOT_CONNECTED);

    // 隧道已就绪但 Arthas 未运行
    ArthasStateEventBus other = new ArthasStateEventBus();
    other.publishTunnelRegistered();
    other.publishArthasState(ArthasLifecycleManager.State.STOPPED);
    ArthasReadinessGate otherGate = new ArthasReadinessGate(other,
        new ArthasLifecycleManager(ArthasConfig.builder().build(),
            new NoopLifecycleListener()));
    assertThat(otherGate.evaluateNow().getReadiness())
        .isEqualTo(Readiness.TUNNEL_READY_BUT_ARTHAS_NOT_READY);
  }

  @Test
  void bridgePublishesShellServerReadyChange() {
    // 用可控桩验证：轮询时探测值变化会驱动监听器回调
    AtomicReference<Boolean> observed = new AtomicReference<>();
    ShellServerReadinessProbe fake = () -> true;
    ArthasTunnelStatusBridge.TunnelStatusListener listener =
        new ArthasTunnelStatusBridge.TunnelStatusListener() {
          @Override
          public void onTunnelConnected() {}

          @Override
          public void onTunnelRegistered() {}

          @Override
          public void onTunnelDisconnected(String reason) {}

          @Override
          public void onShellServerReady(boolean ready) {
            observed.set(ready);
          }
        };
    ArthasBootstrap bootstrap = new ArthasBootstrap(ArthasConfig.builder().build());
    ArthasLifecycleManager.StartupLogCollector logs =
        new ArthasLifecycleManager.StartupLogCollector();
    ArthasTunnelStatusBridge bridge =
        new ArthasTunnelStatusBridge(bootstrap, listener, logs, fake);

    try {
      java.lang.reflect.Field runningField =
          ArthasTunnelStatusBridge.class.getDeclaredField("running");
      runningField.setAccessible(true);
      ((java.util.concurrent.atomic.AtomicBoolean) runningField.get(bridge)).set(true);
      java.lang.reflect.Method poll =
          ArthasTunnelStatusBridge.class.getDeclaredMethod("pollTunnelStatus");
      poll.setAccessible(true);
      poll.invoke(bridge);
    } catch (ReflectiveOperationException e) {
      throw new LinkageError(e.getMessage(), e);
    }

    assertThat(observed.get()).isTrue();
  }

  @Test
  void reconnectRepublishesShellServerReady() {
    // 用真实 Bridge + 可控桩模拟：断连清场后，下一轮轮询必须重新发布 true
    RecordingListener listener = new RecordingListener(bus);
    ShellServerReadinessProbe alwaysListening = () -> true;
    ArthasBootstrap bootstrap = new ArthasBootstrap(ArthasConfig.builder().build());
    ArthasTunnelStatusBridge bridge = new ArthasTunnelStatusBridge(
        bootstrap, listener, new ArthasLifecycleManager.StartupLogCollector(),
        alwaysListening);
    setRunning(bridge);
    invokePoll(bridge); // 发布 true
    assertThat(bus.getState().isShellServerReady()).isTrue();

    bus.publishTunnelDisconnected("reconnect-test");
    assertThat(bus.getState().isShellServerReady()).isFalse();

    // 本地探测值仍是 true（ShellServer 仍 listen），但总线已被清场；
    // 下一次轮询必须能重新发布，否则重连后永久卡在 false
    invokePoll(bridge);

    assertThat(bus.getState().isShellServerReady()).isTrue();
  }

  private static void setRunning(ArthasTunnelStatusBridge bridge) {
    try {
      java.lang.reflect.Field f = ArthasTunnelStatusBridge.class.getDeclaredField("running");
      f.setAccessible(true);
      ((java.util.concurrent.atomic.AtomicBoolean) f.get(bridge)).set(true);
    } catch (ReflectiveOperationException e) {
      throw new LinkageError(e.getMessage(), e);
    }
  }

  private static void invokePoll(ArthasTunnelStatusBridge bridge) {
    try {
      java.lang.reflect.Method m =
          ArthasTunnelStatusBridge.class.getDeclaredMethod("pollTunnelStatus");
      m.setAccessible(true);
      m.invoke(bridge);
    } catch (ReflectiveOperationException e) {
      throw new LinkageError(e.getMessage(), e);
    }
  }

  private static final class RecordingListener
      implements ArthasTunnelStatusBridge.TunnelStatusListener {
    private final ArthasStateEventBus bus;

    RecordingListener(ArthasStateEventBus bus) {
      this.bus = bus;
    }

    @Override
    public void onTunnelConnected() {
      bus.publishTunnelConnected();
    }

    @Override
    public void onTunnelRegistered() {
      bus.publishTunnelRegistered();
    }

    @Override
    public void onTunnelDisconnected(String reason) {
      bus.publishTunnelDisconnected(reason);
    }

    @Override
    public void onShellServerReady(boolean ready) {
      bus.publishShellServerReady(ready);
    }
  }

  @Test
  void probeDisabledRollsBackToLegacyBehaviour() {
    System.setProperty(ShellServerReadinessProbe.PROP_ENABLED, "false");
    // 关闭探测时反射实现恒返回 true，等价于不追加这一层条件
    ShellServerReadinessProbe probe = new ShellServerReadinessProbe.Reflective(
        new ArthasBootstrap(ArthasConfig.builder().build()),
        new ArthasLifecycleManager.StartupLogCollector());
    assertThat(probe.isListening()).isTrue();
  }

  /** 生命周期监听器空实现，仅用于构造依赖。 */
  private static final class NoopLifecycleListener
      implements ArthasLifecycleManager.LifecycleEventListener {
    @Override
    public void onArthasStarted() {}

    @Override
    public void onArthasStopped() {}

    @Override
    public void onMaxDurationExceeded() {}
  }
}
