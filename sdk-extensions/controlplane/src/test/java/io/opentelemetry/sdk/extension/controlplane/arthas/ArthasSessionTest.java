/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class ArthasSessionTest {

  @Test
  void builderCreatesSessionWithDefaultValues() {
    ArthasSession session = ArthasSession.builder().build();

    assertThat(session.getSessionId()).isNotNull().isNotEmpty();
    assertThat(session.getArthasSessionId()).isNull();
    assertThat(session.getUserId()).isNull();
    assertThat(session.getCreatedAt()).isNotNull();
    assertThat(session.getLastActiveAt()).isEqualTo(session.getCreatedAt());
    assertThat(session.getCols()).isEqualTo(120);
    assertThat(session.getRows()).isEqualTo(40);
  }

  @Test
  void builderCanSetAllValues() {
    Instant now = Instant.now();
    ArthasSession session =
        ArthasSession.builder()
            .setSessionId("test-session-id")
            .setArthasSessionId("arthas-123")
            .setUserId("user-456")
            .setCreatedAt(now)
            .setCols(80)
            .setRows(24)
            .build();

    assertThat(session.getSessionId()).isEqualTo("test-session-id");
    assertThat(session.getArthasSessionId()).isEqualTo("arthas-123");
    assertThat(session.getUserId()).isEqualTo("user-456");
    assertThat(session.getCreatedAt()).isEqualTo(now);
    assertThat(session.getCols()).isEqualTo(80);
    assertThat(session.getRows()).isEqualTo(24);
  }

  @Test
  void markActiveUpdatesLastActiveTime() throws InterruptedException {
    ArthasSession session = ArthasSession.builder().build();
    Instant initialActive = session.getLastActiveAt();

    // 等待一小段时间
    Thread.sleep(10);

    session.markActive();
    assertThat(session.getLastActiveAt()).isAfter(initialActive);
  }

  @Test
  void isIdleTimeoutReturnsTrueWhenIdle() throws InterruptedException {
    ArthasSession session = ArthasSession.builder().build();

    // 设置一个很短的超时时间
    Thread.sleep(50);
    assertThat(session.isIdleTimeout(10)).isTrue();
  }

  @Test
  void isIdleTimeoutReturnsFalseWhenActive() {
    ArthasSession session = ArthasSession.builder().build();
    session.markActive();

    // 使用一个很长的超时时间
    assertThat(session.isIdleTimeout(100000)).isFalse();
  }

  @Test
  void isExpiredReturnsTrueWhenOld() throws InterruptedException {
    ArthasSession session = ArthasSession.builder().build();

    // 等待一小段时间
    Thread.sleep(50);
    assertThat(session.isExpired(10)).isTrue();
  }

  @Test
  void isExpiredReturnsFalseWhenYoung() {
    ArthasSession session = ArthasSession.builder().build();

    // 使用一个很长的存活时间
    assertThat(session.isExpired(100000)).isFalse();
  }

  @Test
  void getAgeMillisReturnsPositiveValue() throws InterruptedException {
    ArthasSession session = ArthasSession.builder().build();

    Thread.sleep(10);
    assertThat(session.getAgeMillis()).isGreaterThan(0);
  }

  @Test
  void getIdleMillisReturnsPositiveValue() throws InterruptedException {
    ArthasSession session = ArthasSession.builder().build();

    Thread.sleep(10);
    assertThat(session.getIdleMillis()).isGreaterThan(0);
  }

  @Test
  void getIdleMillisResetsAfterMarkActive() throws InterruptedException {
    ArthasSession session = ArthasSession.builder().build();

    Thread.sleep(50);
    long idleBefore = session.getIdleMillis();

    session.markActive();
    long idleAfter = session.getIdleMillis();

    assertThat(idleBefore).isGreaterThan(idleAfter);
  }

  @Test
  void equalsAndHashCodeBasedOnSessionId() {
    ArthasSession session1 =
        ArthasSession.builder().setSessionId("same-id").setUserId("user1").build();

    ArthasSession session2 =
        ArthasSession.builder().setSessionId("same-id").setUserId("user2").build();

    ArthasSession session3 =
        ArthasSession.builder().setSessionId("different-id").setUserId("user1").build();

    assertThat(session1).isEqualTo(session2);
    assertThat(session1.hashCode()).isEqualTo(session2.hashCode());
    assertThat(session1).isNotEqualTo(session3);
  }

  @Test
  void toStringContainsKeyFields() {
    ArthasSession session =
        ArthasSession.builder()
            .setSessionId("test-session")
            .setUserId("test-user")
            .setCols(100)
            .setRows(30)
            .build();

    String str = session.toString();
    assertThat(str).contains("sessionId='test-session'");
    assertThat(str).contains("userId='test-user'");
    assertThat(str).contains("cols=100");
    assertThat(str).contains("rows=30");
  }

  @Test
  void colsMustBeAtLeastOne() {
    assertThatThrownBy(() -> ArthasSession.builder().setCols(0).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cols must be >= 1");
  }

  @Test
  void rowsMustBeAtLeastOne() {
    assertThatThrownBy(() -> ArthasSession.builder().setRows(0).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rows must be >= 1");
  }

  @Test
  void sessionIdCannotBeNull() {
    assertThatThrownBy(() -> ArthasSession.builder().setSessionId(null).build())
        .isInstanceOf(NullPointerException.class);
  }
}
