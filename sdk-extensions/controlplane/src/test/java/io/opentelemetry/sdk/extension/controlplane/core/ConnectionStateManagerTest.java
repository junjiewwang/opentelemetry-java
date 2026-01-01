/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.extension.controlplane.core.ConnectionStateManager.ConnectionState;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectionStateManagerTest {

  private ConnectionStateManager manager;

  @BeforeEach
  void setUp() {
    manager = new ConnectionStateManager();
  }

  @Test
  void initialStateIsDisconnected() {
    assertThat(manager.getState()).isEqualTo(ConnectionState.DISCONNECTED);
  }

  @Test
  void setStateUpdatesCurrentState() {
    ConnectionState previous = manager.setState(ConnectionState.CONNECTED);
    assertThat(previous).isEqualTo(ConnectionState.DISCONNECTED);
    assertThat(manager.getState()).isEqualTo(ConnectionState.CONNECTED);
  }

  @Test
  void compareAndSetStateSucceedsWhenStateMatches() {
    boolean result = manager.compareAndSetState(ConnectionState.DISCONNECTED, ConnectionState.CONNECTING);
    assertThat(result).isTrue();
    assertThat(manager.getState()).isEqualTo(ConnectionState.CONNECTING);
  }

  @Test
  void compareAndSetStateFailsWhenStateDoesNotMatch() {
    boolean result = manager.compareAndSetState(ConnectionState.CONNECTED, ConnectionState.DISCONNECTED);
    assertThat(result).isFalse();
    assertThat(manager.getState()).isEqualTo(ConnectionState.DISCONNECTED);
  }

  @Test
  void markConnectedUpdatesState() {
    manager.markConnected();
    assertThat(manager.getState()).isEqualTo(ConnectionState.CONNECTED);
    assertThat(manager.isConnected()).isTrue();
  }

  @Test
  void markDisconnectedUpdatesState() {
    manager.setState(ConnectionState.CONNECTED);
    manager.markDisconnected("test reason");
    assertThat(manager.getState()).isEqualTo(ConnectionState.DISCONNECTED);
    assertThat(manager.isConnected()).isFalse();
  }

  @Test
  void markServerUnavailableUpdatesState() {
    manager.markServerUnavailable();
    assertThat(manager.getState()).isEqualTo(ConnectionState.SERVER_UNAVAILABLE);
  }

  @Test
  void markWaitingForOtlpUpdatesState() {
    manager.markWaitingForOtlp();
    assertThat(manager.getState()).isEqualTo(ConnectionState.WAITING_FOR_OTLP);
  }

  @Test
  void markConnectingUpdatesState() {
    manager.markConnecting();
    assertThat(manager.getState()).isEqualTo(ConnectionState.CONNECTING);
  }

  @Test
  void canPerformOperationsReturnsTrueWhenConnected() {
    manager.markConnected();
    assertThat(manager.canPerformOperations()).isTrue();
  }

  @Test
  void canPerformOperationsReturnsFalseWhenNotConnected() {
    assertThat(manager.canPerformOperations()).isFalse();
    
    manager.markConnecting();
    assertThat(manager.canPerformOperations()).isFalse();
  }

  @Test
  void listenerNotifiedOnStateChange() {
    List<ConnectionState[]> notifications = new ArrayList<>();
    manager.addListener((prev, next) -> notifications.add(new ConnectionState[] {prev, next}));

    manager.setState(ConnectionState.CONNECTING);
    manager.setState(ConnectionState.CONNECTED);

    assertThat(notifications).hasSize(2);
    assertThat(notifications.get(0)).containsExactly(ConnectionState.DISCONNECTED, ConnectionState.CONNECTING);
    assertThat(notifications.get(1)).containsExactly(ConnectionState.CONNECTING, ConnectionState.CONNECTED);
  }

  @Test
  void listenerNotNotifiedWhenStateSame() {
    List<ConnectionState[]> notifications = new ArrayList<>();
    manager.addListener((prev, next) -> notifications.add(new ConnectionState[] {prev, next}));

    manager.setState(ConnectionState.DISCONNECTED);
    assertThat(notifications).isEmpty();
  }

  @Test
  void removeListenerStopsNotifications() {
    List<ConnectionState[]> notifications = new ArrayList<>();
    ConnectionStateManager.StateChangeListener listener =
        (prev, next) -> notifications.add(new ConnectionState[] {prev, next});
    
    manager.addListener(listener);
    manager.setState(ConnectionState.CONNECTING);
    
    manager.removeListener(listener);
    manager.setState(ConnectionState.CONNECTED);

    assertThat(notifications).hasSize(1);
  }

  @Test
  void nullListenerIsIgnored() {
    manager.addListener(null);
    // 不应抛出异常
    manager.setState(ConnectionState.CONNECTED);
  }
}
