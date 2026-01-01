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
    when(healthMonitor.isHealthy()).thenReturn(true);
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
  }

  @Test
  void onStateChangedToHealthyWhenWaitingReconnects() {
    connectionStateManager.setState(ConnectionState.WAITING_FOR_OTLP);
    
    coordinator.onStateChanged(
        OtlpHealthMonitor.HealthState.UNHEALTHY,
        OtlpHealthMonitor.HealthState.HEALTHY);
    
    assertThat(connectionStateManager.getState()).isEqualTo(ConnectionState.CONNECTING);
  }

  @Test
  void onStateChangedToUnhealthyPausesConnection() {
    connectionStateManager.setState(ConnectionState.CONNECTED);
    
    coordinator.onStateChanged(
        OtlpHealthMonitor.HealthState.HEALTHY,
        OtlpHealthMonitor.HealthState.UNHEALTHY);
    
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
