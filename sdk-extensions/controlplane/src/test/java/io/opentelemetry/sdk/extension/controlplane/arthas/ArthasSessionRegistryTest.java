/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ArthasSessionRegistryTest {

  @Test
  void registerSessionCreatesOpenSnapshot() {
    ArthasSessionRegistry registry = new ArthasSessionRegistry();

    ArthasAsyncSessionSnapshot snapshot = registry.registerSession("session-1", "consumer-1", 1000, 500);

    assertThat(snapshot.getSessionId()).isEqualTo("session-1");
    assertThat(snapshot.getConsumerId()).isEqualTo("consumer-1");
    assertThat(snapshot.getState()).isEqualTo(ArthasSessionState.OPEN);
    assertThat(snapshot.getTtlMillis()).isEqualTo(1000);
    assertThat(snapshot.getIdleTimeoutMillis()).isEqualTo(500);
    assertThat(snapshot.isEndOfStream()).isFalse();
    assertThat(snapshot.getCreatedAtMillis()).isPositive();
    assertThat(snapshot.getLastAccessAtMillis()).isGreaterThanOrEqualTo(snapshot.getCreatedAtMillis());
  }

  @Test
  void requireActiveSessionRejectsMismatchedConsumer() {
    ArthasSessionRegistry registry = new ArthasSessionRegistry();
    registry.registerSession("session-1", "consumer-1", 0, 0);

    assertThatThrownBy(() -> registry.requireActiveSession("session-1", "consumer-2"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(ArthasTaskProtocol.ErrorCode.SESSION_NOT_FOUND)
        .hasMessageContaining("Consumer does not match session");
  }

  @Test
  void markExecutingAndUpdateAfterPullTransitionSessionState() {
    ArthasSessionRegistry registry = new ArthasSessionRegistry();
    registry.registerSession("session-1", "consumer-1", 0, 0);

    ArthasAsyncSessionSnapshot executing =
        registry.markExecuting("session-1", "watch demo", 101, "RUNNING");

    assertThat(executing.getState()).isEqualTo(ArthasSessionState.EXECUTING);
    assertThat(executing.getCurrentCommand()).isEqualTo("watch demo");
    assertThat(executing.getCurrentJobId()).isEqualTo(101);
    assertThat(executing.getCurrentJobStatus()).isEqualTo("RUNNING");
    assertThat(executing.isEndOfStream()).isFalse();

    ArthasAsyncSessionSnapshot stillExecuting =
        registry.updateAfterPull("session-1", 101, "RUNNING", /* endOfStream= */ false);

    assertThat(stillExecuting.getState()).isEqualTo(ArthasSessionState.EXECUTING);
    assertThat(stillExecuting.getCurrentJobStatus()).isEqualTo("RUNNING");
    assertThat(stillExecuting.isEndOfStream()).isFalse();

    ArthasAsyncSessionSnapshot idle =
        registry.updateAfterPull("session-1", 101, "STOPPED", /* endOfStream= */ true);

    assertThat(idle.getState()).isEqualTo(ArthasSessionState.IDLE);
    assertThat(idle.getCurrentCommand()).isEqualTo("watch demo");
    assertThat(idle.getCurrentJobId()).isEqualTo(101);
    assertThat(idle.getCurrentJobStatus()).isEqualTo("STOPPED");
    assertThat(idle.isEndOfStream()).isTrue();
  }

  @Test
  void markExecutingTwiceFailsWithSessionNotIdle() {
    ArthasSessionRegistry registry = new ArthasSessionRegistry();
    registry.registerSession("session-1", "consumer-1", 0, 0);
    registry.markExecuting("session-1", "watch demo", 101, "RUNNING");

    assertThatThrownBy(() -> registry.markExecuting("session-1", "trace demo", 102, "RUNNING"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(ArthasTaskProtocol.ErrorCode.SESSION_NOT_IDLE);
  }

  @Test
  void markInterruptedSetsInterruptedStateAndEndOfStream() {
    ArthasSessionRegistry registry = new ArthasSessionRegistry();
    registry.registerSession("session-1", "consumer-1", 0, 0);
    registry.markExecuting("session-1", "watch demo", 101, "RUNNING");

    ArthasAsyncSessionSnapshot interrupted = registry.markInterrupted("session-1", 101, "STOPPED");

    assertThat(interrupted.getState()).isEqualTo(ArthasSessionState.INTERRUPTED);
    assertThat(interrupted.getCurrentJobId()).isEqualTo(101);
    assertThat(interrupted.getCurrentJobStatus()).isEqualTo("STOPPED");
    assertThat(interrupted.isEndOfStream()).isTrue();
  }

  @Test
  void markClosedPreventsFurtherAccess() {
    ArthasSessionRegistry registry = new ArthasSessionRegistry();
    registry.registerSession("session-1", "consumer-1", 0, 0);

    ArthasAsyncSessionSnapshot closed = registry.markClosed("session-1");

    assertThat(closed.getState()).isEqualTo(ArthasSessionState.CLOSED);
    assertThat(closed.isEndOfStream()).isTrue();
    assertThatThrownBy(() -> registry.requireActiveSession("session-1"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(ArthasTaskProtocol.ErrorCode.SESSION_ALREADY_CLOSED);
  }

  @Test
  void markFailedSetsFailedStateAndEndOfStream() {
    ArthasSessionRegistry registry = new ArthasSessionRegistry();
    registry.registerSession("session-1", "consumer-1", 0, 0);

    ArthasAsyncSessionSnapshot failed = registry.markFailed("session-1", "FAILED");

    assertThat(failed.getState()).isEqualTo(ArthasSessionState.FAILED);
    assertThat(failed.getCurrentJobStatus()).isEqualTo("FAILED");
    assertThat(failed.isEndOfStream()).isTrue();
  }

  @Test
  void requireActiveSessionFailsWithTtlExceededAndRemovesSession() throws Exception {
    ArthasSessionRegistry registry = new ArthasSessionRegistry();
    registry.registerSession("ttl-session", "consumer-1", 20, 0);

    Thread.sleep(60);

    assertThatThrownBy(() -> registry.requireActiveSession("ttl-session", "consumer-1"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(ArthasTaskProtocol.ErrorCode.SESSION_TTL_EXCEEDED);
    assertThatThrownBy(() -> registry.requireActiveSession("ttl-session", "consumer-1"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(ArthasTaskProtocol.ErrorCode.SESSION_NOT_FOUND);
  }

  @Test
  void requireActiveSessionFailsWithIdleTimeout() throws Exception {
    ArthasSessionRegistry registry = new ArthasSessionRegistry();
    registry.registerSession("idle-session", "consumer-1", 0, 20);

    Thread.sleep(60);

    assertThatThrownBy(() -> registry.requireActiveSession("idle-session", "consumer-1"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(ArthasTaskProtocol.ErrorCode.SESSION_IDLE_TIMEOUT);
  }

  @Test
  void cleanupExpiredSessionsRemovesExpiredEntriesOnly() throws Exception {
    ArthasSessionRegistry registry = new ArthasSessionRegistry();
    registry.registerSession("expired-session", "consumer-1", 20, 0);
    registry.registerSession("active-session", "consumer-2", 0, 0);

    Thread.sleep(60);

    assertThat(registry.cleanupExpiredSessions()).containsExactly("expired-session");
    assertThat(registry.listSnapshots())
        .extracting(ArthasAsyncSessionSnapshot::getSessionId)
        .containsExactly("active-session");
  }
}
