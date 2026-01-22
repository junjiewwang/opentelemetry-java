/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentelemetry.sdk.extension.controlplane.core.ConnectionStateManager.ConnectionState;
import io.opentelemetry.sdk.extension.controlplane.status.HeartbeatReporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class HealthCheckCoordinatorTest {

  @Nested
  @DisplayName("HeartbeatOnly Tests")
  class HeartbeatOnlyTests {

    private HeartbeatReporter heartbeatReporter;
    private ConnectionStateManager connectionStateManager;
    private HealthCheckCoordinator coordinator;

    @BeforeEach
    void setUp() {
      heartbeatReporter = mock(HeartbeatReporter.class);
      connectionStateManager = new ConnectionStateManager();
      coordinator = new HealthCheckCoordinator(heartbeatReporter, connectionStateManager);
    }

    @Test
    void shouldConnectReturnsTrueWhenHeartbeatHealthy() {
      when(heartbeatReporter.isHealthy()).thenReturn(true);
      when(heartbeatReporter.getHeartbeatCount()).thenReturn(5L);
      when(heartbeatReporter.getSuccessRate()).thenReturn(0.9);
      when(heartbeatReporter.getLastHeartbeatTimeMs()).thenReturn(System.currentTimeMillis());

      assertThat(coordinator.shouldConnect()).isTrue();
      assertThat(coordinator.getLastGateDecision().isAllowed()).isTrue();
      assertThat(coordinator.getLastGateDecision().getReason()).contains("heartbeat_healthy");
    }

    @Test
    void shouldConnectReturnsFalseWhenHeartbeatUnhealthy() {
      when(heartbeatReporter.isHealthy()).thenReturn(false);
      when(heartbeatReporter.getHeartbeatCount()).thenReturn(10L);
      when(heartbeatReporter.getSuccessRate()).thenReturn(0.3); // < 0.5 阈值
      when(heartbeatReporter.getLastHeartbeatTimeMs()).thenReturn(System.currentTimeMillis());

      assertThat(coordinator.shouldConnect()).isFalse();
      assertThat(connectionStateManager.getState()).isEqualTo(ConnectionState.WAITING_FOR_OTLP);
      assertThat(coordinator.getLastGateDecision().isAllowed()).isFalse();
      assertThat(coordinator.getLastGateDecision().getReason()).contains("heartbeat_unhealthy");
    }

    @Test
    void shouldConnectAllowsWhenHeartbeatDegraded() {
      // 中间状态（50%~80%）→ 乐观允许连接
      when(heartbeatReporter.isHealthy()).thenReturn(false);
      when(heartbeatReporter.getHeartbeatCount()).thenReturn(10L);
      when(heartbeatReporter.getSuccessRate()).thenReturn(0.6); // 50% < 0.6 < 80%
      when(heartbeatReporter.getLastHeartbeatTimeMs()).thenReturn(System.currentTimeMillis());

      assertThat(coordinator.shouldConnect()).isTrue();
      assertThat(coordinator.getLastGateDecision().isAllowed()).isTrue();
      assertThat(coordinator.getLastGateDecision().getReason()).contains("heartbeat_degraded");
      assertThat(coordinator.getLastGateDecision().getReason()).contains("optimistic");
    }

    @Test
    void shouldConnectAllowsWhenNoHeartbeatYet() {
      // 无心跳记录，乐观允许连接
      when(heartbeatReporter.getHeartbeatCount()).thenReturn(0L);
      when(heartbeatReporter.getLastHeartbeatTimeMs()).thenReturn(0L);

      assertThat(coordinator.shouldConnect()).isTrue();
      assertThat(coordinator.getLastGateDecision().getReason()).contains("no_heartbeat_yet");
    }

    @Test
    void shouldConnectAllowsWhenHeartbeatStale() {
      // 心跳超过2个周期未更新，乐观允许连接
      when(heartbeatReporter.getHeartbeatCount()).thenReturn(5L);
      // 模拟心跳卡住：最后心跳时间是 70 秒前（超过 2 * 30s）
      when(heartbeatReporter.getLastHeartbeatTimeMs())
          .thenReturn(System.currentTimeMillis() - 70_000);

      assertThat(coordinator.shouldConnect()).isTrue();
      assertThat(coordinator.getLastGateDecision().getReason()).contains("heartbeat_stale");
    }

    @Test
    void isHeartbeatHealthyDelegatesToReporter() {
      when(heartbeatReporter.isHealthy()).thenReturn(true);
      assertThat(coordinator.isHeartbeatHealthy()).isTrue();

      when(heartbeatReporter.isHealthy()).thenReturn(false);
      assertThat(coordinator.isHeartbeatHealthy()).isFalse();
    }

    @Test
    void getHeartbeatReporterReturnsReporter() {
      assertThat(coordinator.getHeartbeatReporter()).isSameAs(heartbeatReporter);
    }

    @Test
    void buildHealthInfoContainsHeartbeatInfo() {
      when(heartbeatReporter.isHealthy()).thenReturn(true);
      when(heartbeatReporter.getSuccessRate()).thenReturn(0.95);
      when(heartbeatReporter.getHeartbeatCount()).thenReturn(100L);

      String info = coordinator.buildHealthInfo();
      assertThat(info).contains("heartbeat=");
      assertThat(info).contains("healthy=true");
      assertThat(info).contains("rate=95.0%");
      assertThat(info).contains("count=100");
      assertThat(info).contains("gate=");
    }
  }

  @Nested
  @DisplayName("ConnectionGatePolicy Tests")
  class PolicyTests {

    @Test
    void heartbeatOnlyPolicyCreatesCorrectPolicy() {
      HealthCheckCoordinator.ConnectionGatePolicy policy =
          HealthCheckCoordinator.ConnectionGatePolicies.heartbeatOnlyPolicy();
      assertThat(policy).isNotNull();
      assertThat(policy).isInstanceOf(HealthCheckCoordinator.HeartbeatOnlyGatePolicy.class);
    }

    @Test
    void heartbeatOnlyPolicyWithIntervalCreatesCorrectPolicy() {
      HealthCheckCoordinator.ConnectionGatePolicy policy =
          HealthCheckCoordinator.ConnectionGatePolicies.heartbeatOnlyPolicy(60_000L);
      assertThat(policy).isNotNull();
      assertThat(policy).isInstanceOf(HealthCheckCoordinator.HeartbeatOnlyGatePolicy.class);
    }

    @Test
    void heartbeatOnlyPolicyWithIntervalAndThresholdCreatesCorrectPolicy() {
      HealthCheckCoordinator.ConnectionGatePolicy policy =
          HealthCheckCoordinator.ConnectionGatePolicies.heartbeatOnlyPolicy(60_000L, 0.4);
      assertThat(policy).isNotNull();
      assertThat(policy).isInstanceOf(HealthCheckCoordinator.HeartbeatOnlyGatePolicy.class);
    }
  }

  @Nested
  @DisplayName("GateDecision Tests")
  class GateDecisionTests {

    @Test
    void allowedDecisionHasCorrectProperties() {
      HealthCheckCoordinator.GateDecision decision =
          HealthCheckCoordinator.GateDecision.allowed("test_reason");
      assertThat(decision.isAllowed()).isTrue();
      assertThat(decision.getReason()).isEqualTo("test_reason");
      assertThat(decision.toString()).contains("OPEN");
      assertThat(decision.toString()).contains("test_reason");
    }

    @Test
    void blockedDecisionHasCorrectProperties() {
      HealthCheckCoordinator.GateDecision decision =
          HealthCheckCoordinator.GateDecision.blocked("test_reason");
      assertThat(decision.isAllowed()).isFalse();
      assertThat(decision.getReason()).isEqualTo("test_reason");
      assertThat(decision.toString()).contains("CLOSED");
      assertThat(decision.toString()).contains("test_reason");
    }
  }

  @Nested
  @DisplayName("Lifecycle Tests")
  class LifecycleTests {

    @Test
    void startAndStopDoNotThrow() {
      HeartbeatReporter heartbeatReporter = mock(HeartbeatReporter.class);
      ConnectionStateManager connectionStateManager = new ConnectionStateManager();
      HealthCheckCoordinator coordinator =
          new HealthCheckCoordinator(heartbeatReporter, connectionStateManager);

      // start 和 stop 不应该抛异常
      coordinator.start();
      coordinator.stop();
    }
  }
}
