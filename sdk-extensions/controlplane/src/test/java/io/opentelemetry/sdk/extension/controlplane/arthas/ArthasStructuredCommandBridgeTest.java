/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class ArthasStructuredCommandBridgeTest {

  @Test
  void loadRequiredClassFallsBackToShadedFastjson2Class() throws Exception {
    URL arthasCoreJar =
        ArthasStructuredCommandBridgeTest.class.getResource("/arthas/arthas-core.jar");
    assertThat(arthasCoreJar).isNotNull();

    try (@SuppressWarnings("BanClassLoader")
        URLClassLoader loader = new URLClassLoader(new URL[] {arthasCoreJar}, null)) {
      Class<?> jsonClass =
          ArthasStructuredCommandBridge.loadRequiredClass(
              loader,
              "fastjson2 JSON",
              "com.alibaba.fastjson2.JSON",
              "com.alibaba.arthas.deps.com.alibaba.fastjson2.JSON");

      assertThat(jsonClass.getName())
          .isEqualTo("com.alibaba.arthas.deps.com.alibaba.fastjson2.JSON");
    }
  }

  @Test
  void loadRequiredClassIncludesComponentNameAndCandidatesWhenMissing() {
    ClassLoader emptyLoader =
        new ClassLoader(null) {
          @Override
          protected Class<?> findClass(String name) throws ClassNotFoundException {
            throw new ClassNotFoundException(name);
          }
        };

    assertThatThrownBy(
            () ->
                ArthasStructuredCommandBridge.loadRequiredClass(
                    emptyLoader,
                    "fastjson2 JSON",
                    "com.alibaba.fastjson2.JSON",
                    "com.alibaba.arthas.deps.com.alibaba.fastjson2.JSON"))
        .isInstanceOf(ClassNotFoundException.class)
        .hasMessageContaining("fastjson2 JSON 类不存在")
        .hasMessageContaining("com.alibaba.fastjson2.JSON")
        .hasMessageContaining("com.alibaba.arthas.deps.com.alibaba.fastjson2.JSON");
  }

  @Test
  void classifyAsyncPayloadErrorMapsKnownMessagesToProtocolCodes() throws Exception {
    assertThat(invokePrivateStaticString("classifyAsyncPayloadError", "Session not found"))
        .isEqualTo(ArthasTaskProtocol.ErrorCode.SESSION_NOT_FOUND);
    assertThat(
            invokePrivateStaticString(
                "classifyAsyncPayloadError", "another command is executing in this session"))
        .isEqualTo(ArthasTaskProtocol.ErrorCode.SESSION_NOT_IDLE);
    assertThat(invokePrivateStaticString("classifyAsyncPayloadError", "Interrupt requested"))
        .isEqualTo(ArthasTaskProtocol.ErrorCode.ASYNC_JOB_INTERRUPTED);
    assertThat(invokePrivateStaticString("classifyAsyncPayloadError", "Consumer mismatch"))
        .isEqualTo(ArthasTaskProtocol.ErrorCode.PULL_RESULT_FAILED);
  }

  @Test
  void classifyAsyncPayloadErrorFallsBackToCommandExecutionFailed() throws Exception {
    assertThat(invokePrivateStaticString("classifyAsyncPayloadError", ""))
        .isEqualTo(ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED);
    assertThat(invokePrivateStaticString("classifyAsyncPayloadError", "unexpected boom"))
        .isEqualTo(ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED);
  }

  @Test
  void classifyInvokeErrorMapsTimeoutSignalsToTimeoutErrorCode() throws Exception {
    assertThat(invokePrivateStaticString("classifyInvokeError", new TimeoutException("timed out")))
        .isEqualTo(ArthasTaskProtocol.ErrorCode.COMMAND_TIMEOUT);
    assertThat(
            invokePrivateStaticString(
                "classifyInvokeError", new IllegalStateException("operation timeout")))
        .isEqualTo(ArthasTaskProtocol.ErrorCode.COMMAND_TIMEOUT);
    assertThat(
            invokePrivateStaticString(
                "classifyInvokeError",
                new RuntimeException("wrapper", new TimeoutException("nested timeout"))))
        .isEqualTo(ArthasTaskProtocol.ErrorCode.COMMAND_TIMEOUT);
  }

  @Test
  void classifyInvokeErrorFallsBackToExecutionFailedForGenericException() throws Exception {
    assertThat(invokePrivateStaticString("classifyInvokeError", new IllegalArgumentException("boom")))
        .isEqualTo(ArthasTaskProtocol.ErrorCode.COMMAND_EXECUTION_FAILED);
  }

  @Test
  void sessionTasksDefaultToLocalReadyInsteadOfTunnelReady() throws Exception {
    assertThat(readStaticBooleanConstant(
            "io.opentelemetry.sdk.extension.controlplane.task.executor.ArthasAsyncExecutorSupport",
            "DEFAULT_REQUIRE_LOCAL_READY"))
        .isFalse();
    assertThat(readStaticBooleanConstant(
            "io.opentelemetry.sdk.extension.controlplane.task.executor.ArthasAsyncExecutorSupport",
            "DEFAULT_REQUIRE_TUNNEL_READY"))
        .isTrue();
  }

  private static String invokePrivateStaticString(String methodName, Object argument)
      throws Exception {
    Class<?> argumentType = Throwable.class;
    if ("classifyAsyncPayloadError".equals(methodName)) {
      argumentType = String.class;
    }

    Method method = ArthasStructuredCommandBridge.class.getDeclaredMethod(methodName, argumentType);
    method.setAccessible(true);
    return (String) method.invoke(null, argument);
  }

  private static boolean readStaticBooleanConstant(String className, String fieldName)
      throws Exception {
    Class<?> clazz = Class.forName(className);
    Field field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.getBoolean(null);
  }
}
