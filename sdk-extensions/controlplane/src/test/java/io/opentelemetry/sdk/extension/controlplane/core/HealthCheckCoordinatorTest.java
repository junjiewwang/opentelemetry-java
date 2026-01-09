/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.sdk.extension.controlplane.core.ConnectionStateManager.ConnectionState;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpHealthMonitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HealthCheckCoordinatorTest {

  private OtlpHealthMonitor healthMonitor;
  private ConnectionStateManager connectionStateManager;
  private HealthCheckCoordinator coordinator;

  @BeforeEach
  void setUp() {
    healthMonitor = mock(OtlpHealthMonitor.class);
    connectionStateManager = new ConnectionStateManager();
    coordinator = new HealthCheckCoordinator(healthMonitor, connectionStateManager);
  }

  @Test
  void startRegistersListener() {
    coordinator.start();
    verify(healthMonitor).addListener(coordinator);
  }

  @Test
  void stopRemovesListener() {
    coordinator.stop();
    verify(healthMonitor).removeListener(coordinator);
  }

  @Test
  void shouldConnectReturnsTrueWhenHealthy() {
    // 新实现通过 gatePolicy.decide() 判断，需 mock getState() 返回 HEALTHY
    when(healthMonitor.getState()).thenReturn(OtlpHealthMonitor.HealthState.HEALTHY);
    assertThat(coordinator.shouldConnect()).isTrue();
  }

  @Test
  void shouldConnectReturnsFalseWhenUnhealthy() {
    when(healthMonitor.isHealthy()).thenReturn(false);
    when(healthMonitor.getState()).thenReturn(OtlpHealthMonitor.HealthState.UNHEALTHY);
    
    assertThat(coordinator.shouldConnect()).isFalse();
    assertThat(connectionStateManager.getState()).isEqualTo(ConnectionState.WAITING_FOR_OTLP);
  }

  @Test
  void isOtlpHealthyDelegatesToMonitor() {
    when(healthMonitor.isHealthy()).thenReturn(true);
    assertThat(coordinator.isOtlpHealthy()).isTrue();
    
    when(healthMonitor.isHealthy()).thenReturn(false);
    assertThat(coordinator.isOtlpHealthy()).isFalse();
  }

  @Test
  void getOtlpHealthStateDelegatesToMonitor() {
    when(healthMonitor.getState()).thenReturn(OtlpHealthMonitor.HealthState.HEALTHY);
    assertThat(coordinator.getOtlpHealthState()).isEqualTo(OtlpHealthMonitor.HealthState.HEALTHY);
  }

  @Test
  void buildOtlpHealthInfoWithNoSamples() {
    when(healthMonitor.getState()).thenReturn(OtlpHealthMonitor.HealthState.UNKNOWN);
    when(healthMonitor.getSuccessCount()).thenReturn(0L);
    when(healthMonitor.getFailureCount()).thenReturn(0L);
    
    String info = coordinator.buildOtlpHealthInfo();
    assertThat(info).contains("no samples yet");
  }

  @Test
  void buildOtlpHealthInfoWithSamples() {
    when(healthMonitor.getState()).thenReturn(OtlpHealthMonitor.HealthState.HEALTHY);
    when(healthMonitor.getSuccessCount()).thenReturn(95L);
    when(healthMonitor.getFailureCount()).thenReturn(5L);
    when(healthMonitor.getSuccessRate()).thenReturn(0.95);

    String info = coordinator.buildOtlpHealthInfo();
    assertThat(info).contains("state=HEALTHY");
    assertThat(info).contains("success/fail=95/5");
    assertThat(info).contains("rate=95.0%");
    assertThat(info).contains("gate=");
  }

  @Test
  void shouldConnectFastOpensWhenDegradedButRecentSuccess() {
    // 使用 builder 精确控制配置：
    // - windowMillis=60000: 时间窗口
    // - healthyThreshold=0.8, unhealthyThreshold=0.3: DEGRADED 在 [0.3, 0.8) 之间
    // - cooldownMillis=0: 禁用冷却，状态立即生效
    // - minSamples=1: 最小样本数为 1，避免样本不足导致 UNKNOWN
    OtlpHealthMonitor realMonitor = OtlpHealthMonitor.builder()
        .windowMillis(60_000)
        .healthyThreshold(0.8)
        .unhealthyThreshold(0.3)
        .cooldownMillis(0)
        .minSamples(1)
        .build();
    
    // 构造 DEGRADED 状态：5 success + 5 failure => 0.5 (满足 0.3 <= 0.5 < 0.8)
    for (int i = 0; i < 5; i++) {
      realMonitor.recordSuccess();
    }
    for (int i = 0; i < 5; i++) {
      realMonitor.recordFailure("x");
    }
    // 最后再记录一次成功，确保 lastSuccessTimeNano 有值且为最近
    realMonitor.recordSuccess();
    // 现在 6/11 ≈ 0.545，满足 DEGRADED 条件
    assertThat(realMonitor.getState()).isEqualTo(OtlpHealthMonitor.HealthState.DEGRADED);

    HealthCheckCoordinator c =
        new HealthCheckCoordinator(realMonitor, new ConnectionStateManager());

    assertThat(c.shouldConnect()).isTrue();
    assertThat(c.getLastGateDecision().isAllowed()).isTrue();
  }

  @Test
  void shouldConnectBlocksWhenUnhealthy() {
    // 使用 builder 精确控制配置：禁用冷却，最小样本数为 1
    OtlpHealthMonitor realMonitor = OtlpHealthMonitor.builder()
        .windowMillis(60_000)
        .healthyThreshold(0.8)
        .unhealthyThreshold(0.3)
        .cooldownMillis(0)
        .minSamples(1)
        .build();
    for (int i = 0; i < 6; i++) {
      realMonitor.recordFailure("x");
    }
    assertThat(realMonitor.getState()).isEqualTo(OtlpHealthMonitor.HealthState.UNHEALTHY);

    ConnectionStateManager sm = new ConnectionStateManager();
    HealthCheckCoordinator c = new HealthCheckCoordinator(realMonitor, sm);

    assertThat(c.shouldConnect()).isFalse();
    assertThat(sm.getState()).isEqualTo(ConnectionState.WAITING_FOR_OTLP);
    assertThat(c.getLastGateDecision().isAllowed()).isFalse();
  }

  @Test
  void onStateChangedToHealthyWhenWaitingReconnects() {
    connectionStateManager.setState(ConnectionState.WAITING_FOR_OTLP);
    // 新实现在 onStateChanged 中通过 gatePolicy.decide() 判断是否开闸
    // 需 mock getState() 返回 HEALTHY 使 gatePolicy 允许连接
    when(healthMonitor.getState()).thenReturn(OtlpHealthMonitor.HealthState.HEALTHY);
    
    coordinator.onStateChanged(
        OtlpHealthMonitor.HealthState.UNHEALTHY,
        OtlpHealthMonitor.HealthState.HEALTHY);
    
    assertThat(connectionStateManager.getState()).isEqualTo(ConnectionState.CONNECTING);
  }

  @Test
  void onStateChangedToUnhealthyPausesConnection() {
    connectionStateManager.setState(ConnectionState.CONNECTED);
    // 新实现中 onStateChanged 不再直接暂停连接，而是依赖 shouldConnect() 调用时判断。
    // 修改测试：变为 UNHEALTHY 后，调用 shouldConnect() 才会切换到 WAITING_FOR_OTLP。
    when(healthMonitor.getState()).thenReturn(OtlpHealthMonitor.HealthState.UNHEALTHY);
    
    coordinator.onStateChanged(
        OtlpHealthMonitor.HealthState.HEALTHY,
        OtlpHealthMonitor.HealthState.UNHEALTHY);
    
    // onStateChanged 本身不会切换到 WAITING_FOR_OTLP（因为当前是 CONNECTED 不是 WAITING_FOR_OTLP）
    // 但调用 shouldConnect() 时会触发状态切换
    assertThat(coordinator.shouldConnect()).isFalse();
    assertThat(connectionStateManager.getState()).isEqualTo(ConnectionState.WAITING_FOR_OTLP);
  }

  @Test
  void onStateChangedToHealthyWhenConnectedNoChange() {
    connectionStateManager.setState(ConnectionState.CONNECTED);
    
    coordinator.onStateChanged(
        OtlpHealthMonitor.HealthState.DEGRADED,
        OtlpHealthMonitor.HealthState.HEALTHY);
    
    // 如果不是 WAITING_FOR_OTLP 状态，不会改变连接状态
    assertThat(connectionStateManager.getState()).isEqualTo(ConnectionState.CONNECTED);
  }
}
