/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ControlPlaneConfigTest {

  @Test
  void defaultValues() {
    ControlPlaneConfig config = ControlPlaneConfig.builder().build();

    assertThat(config.isEnabled()).isTrue(); // 默认启用控制平面
    assertThat(config.getProtocol()).isEqualTo("grpc");
    assertThat(config.getHttpBasePath()).isEqualTo("/v1/control");
    assertThat(config.getLongPollTimeout()).isEqualTo(Duration.ofSeconds(60));
    assertThat(config.getConfigPollInterval()).isEqualTo(Duration.ofSeconds(30));
    assertThat(config.getTaskPollInterval()).isEqualTo(Duration.ofSeconds(10));
    assertThat(config.getStatusReportInterval()).isEqualTo(Duration.ofSeconds(30));
    assertThat(config.getCompressionThreshold()).isEqualTo(1024L);
    assertThat(config.getChunkedThreshold()).isEqualTo(50 * 1024 * 1024L);
    assertThat(config.getMaxSize()).isEqualTo(200 * 1024 * 1024L);
  }

  @Test
  void builderSetsValues() {
    ControlPlaneConfig config =
        ControlPlaneConfig.builder()
            .setEnabled(true)
            .setEndpoint("http://localhost:4318")
            .setProtocol("http/protobuf")
            .setHttpBasePath("/custom/path")
            .setLongPollTimeout(Duration.ofSeconds(120))
            .setConfigPollInterval(Duration.ofSeconds(60))
            .setCompressionThreshold(2048)
            .setChunkedThreshold(100 * 1024 * 1024L)
            .setMaxSize(500 * 1024 * 1024L)
            .build();

    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getEndpoint()).isEqualTo("http://localhost:4318");
    assertThat(config.getProtocol()).isEqualTo("http/protobuf");
    assertThat(config.isHttpProtobuf()).isTrue();
    assertThat(config.isGrpc()).isFalse();
    assertThat(config.getHttpBasePath()).isEqualTo("/custom/path");
    assertThat(config.getLongPollTimeout()).isEqualTo(Duration.ofMinutes(2));
    assertThat(config.getCompressionThreshold()).isEqualTo(2048);
    assertThat(config.getChunkedThreshold()).isEqualTo(100 * 1024 * 1024L);
    assertThat(config.getMaxSize()).isEqualTo(500 * 1024 * 1024L);
  }

  @Test
  void controlPlaneUrlForGrpc() {
    ControlPlaneConfig config =
        ControlPlaneConfig.builder()
            .setEndpoint("http://localhost:4317")
            .setProtocol("grpc")
            .build();

    assertThat(config.getControlPlaneUrl()).isEqualTo("http://localhost:4317");
  }

  @Test
  void controlPlaneUrlForHttp() {
    ControlPlaneConfig config =
        ControlPlaneConfig.builder()
            .setEndpoint("http://localhost:4318")
            .setProtocol("http/protobuf")
            .setHttpBasePath("/v1/control")
            .build();

    assertThat(config.getControlPlaneUrl()).isEqualTo("http://localhost:4318/v1/control");
  }

  @Test
  void controlPlaneUrlRemovesTrailingSlash() {
    ControlPlaneConfig config =
        ControlPlaneConfig.builder()
            .setEndpoint("http://localhost:4318/")
            .setProtocol("http/protobuf")
            .setHttpBasePath("/v1/control")
            .build();

    assertThat(config.getControlPlaneUrl()).isEqualTo("http://localhost:4318/v1/control");
  }

  @Test
  void validationFailsWhenCompressionThresholdGreaterThanChunkedThreshold() {
    assertThatThrownBy(
            () ->
                ControlPlaneConfig.builder()
                    .setCompressionThreshold(100 * 1024 * 1024L)
                    .setChunkedThreshold(50 * 1024 * 1024L)
                    .setMaxSize(200 * 1024 * 1024L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("compressionThreshold must be less than chunkedThreshold");
  }

  @Test
  void validationFailsWhenChunkedThresholdGreaterThanMaxSize() {
    assertThatThrownBy(
            () ->
                ControlPlaneConfig.builder()
                    .setCompressionThreshold(1024L)
                    .setChunkedThreshold(300 * 1024 * 1024L)
                    .setMaxSize(200 * 1024 * 1024L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("chunkedThreshold must be less than maxSize");
  }
}
