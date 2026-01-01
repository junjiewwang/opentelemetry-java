/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ArthasExceptionTest {

  @Test
  void basicConstructorCreatesException() {
    ArthasException exception =
        new ArthasException(ArthasException.Type.CONNECTION_FAILED, "Connection lost");

    assertThat(exception.getType()).isEqualTo(ArthasException.Type.CONNECTION_FAILED);
    assertThat(exception.getMessage()).contains("CONNECTION_FAILED");
    assertThat(exception.getMessage()).contains("Connection lost");
    assertThat(exception.getCause()).isNull();
  }

  @Test
  void constructorWithCauseCreatesException() {
    RuntimeException cause = new RuntimeException("Root cause");
    ArthasException exception =
        new ArthasException(ArthasException.Type.START_FAILED, "Cannot start", cause);

    assertThat(exception.getType()).isEqualTo(ArthasException.Type.START_FAILED);
    assertThat(exception.getCause()).isEqualTo(cause);
  }

  @Test
  void connectionFailedIsRecoverable() {
    ArthasException exception = ArthasException.connectionFailed("Network error", null);

    assertThat(exception.getType()).isEqualTo(ArthasException.Type.CONNECTION_FAILED);
    assertThat(exception.isRecoverable()).isTrue();
  }

  @Test
  void timeoutIsRecoverable() {
    ArthasException exception = ArthasException.timeout("Operation timed out");

    assertThat(exception.getType()).isEqualTo(ArthasException.Type.TIMEOUT);
    assertThat(exception.isRecoverable()).isTrue();
  }

  @Test
  void sessionCreateFailedIsRecoverable() {
    ArthasException exception = ArthasException.sessionCreateFailed("Too many sessions", null);

    assertThat(exception.getType()).isEqualTo(ArthasException.Type.SESSION_CREATE_FAILED);
    assertThat(exception.isRecoverable()).isTrue();
  }

  @Test
  void sessionBindFailedIsRecoverable() {
    ArthasException exception =
        ArthasException.sessionBindFailed("Bind error", new RuntimeException());

    assertThat(exception.getType()).isEqualTo(ArthasException.Type.SESSION_BIND_FAILED);
    assertThat(exception.isRecoverable()).isTrue();
  }

  @Test
  void initializationFailedIsNotRecoverable() {
    ArthasException exception =
        ArthasException.initializationFailed("Init error", new RuntimeException());

    assertThat(exception.getType()).isEqualTo(ArthasException.Type.INITIALIZATION_FAILED);
    assertThat(exception.isRecoverable()).isFalse();
  }

  @Test
  void startFailedIsNotRecoverable() {
    ArthasException exception = ArthasException.startFailed("Start error", new RuntimeException());

    assertThat(exception.getType()).isEqualTo(ArthasException.Type.START_FAILED);
    assertThat(exception.isRecoverable()).isFalse();
  }

  @Test
  void configErrorIsNotRecoverable() {
    ArthasException exception = ArthasException.configError("Invalid config");

    assertThat(exception.getType()).isEqualTo(ArthasException.Type.CONFIG_ERROR);
    assertThat(exception.isRecoverable()).isFalse();
  }

  @Test
  void reflectionFailedIsNotRecoverable() {
    ArthasException exception =
        ArthasException.reflectionFailed("Method not found", new NoSuchMethodException());

    assertThat(exception.getType()).isEqualTo(ArthasException.Type.REFLECTION_FAILED);
    assertThat(exception.isRecoverable()).isFalse();
  }

  @Test
  void messageFormatIncludesTypeAndMessage() {
    ArthasException exception = new ArthasException(ArthasException.Type.UNKNOWN, "Test message");

    assertThat(exception.getMessage()).isEqualTo("[UNKNOWN] Test message");
  }

  @Test
  void fullConstructorAllowsOverridingRecoverable() {
    // 默认 CONFIG_ERROR 不可恢复
    ArthasException defaultException = ArthasException.configError("Test");
    assertThat(defaultException.isRecoverable()).isFalse();

    // 可以显式设置为可恢复
    ArthasException recoverableException =
        new ArthasException(
            ArthasException.Type.CONFIG_ERROR, "Test", null, /* recoverable= */ true);
    assertThat(recoverableException.isRecoverable()).isTrue();
  }

  @Test
  void allTypesAreDefined() {
    // 确保所有类型都已定义
    ArthasException.Type[] types = ArthasException.Type.values();
    assertThat(types).contains(
        ArthasException.Type.INITIALIZATION_FAILED,
        ArthasException.Type.START_FAILED,
        ArthasException.Type.STOP_FAILED,
        ArthasException.Type.SESSION_CREATE_FAILED,
        ArthasException.Type.SESSION_BIND_FAILED,
        ArthasException.Type.SESSION_CLOSE_FAILED,
        ArthasException.Type.INPUT_FORWARD_FAILED,
        ArthasException.Type.RESIZE_FAILED,
        ArthasException.Type.CONNECTION_FAILED,
        ArthasException.Type.REFLECTION_FAILED,
        ArthasException.Type.CONFIG_ERROR,
        ArthasException.Type.TIMEOUT,
        ArthasException.Type.UNKNOWN);
  }
}
