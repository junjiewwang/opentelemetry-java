/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.config;

import static org.assertj.core.api.Assertions.assertThat;

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
    // configPollInterval 和 taskPollInterval 已由长轮询替代
    assertThat(config.getStatusReportInterval()).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void builderSetsValues() {
    ControlPlaneConfig config =
        ControlPlaneConfig.builder()
            .setEnabled(true)
            .setEndpoint("http://localhost:4318")
            .setProtocol("http/protobuf")
            .setHttpBasePath("/custom/path")
            .setLongPollTimeout(Duration.ofMinutes(2))
            .build();

    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getEndpoint()).isEqualTo("http://localhost:4318");
    assertThat(config.getProtocol()).isEqualTo("http/protobuf");
    assertThat(config.isHttpProtobuf()).isTrue();
    assertThat(config.isGrpc()).isFalse();
    assertThat(config.getHttpBasePath()).isEqualTo("/custom/path");
    assertThat(config.getLongPollTimeout()).isEqualTo(Duration.ofMinutes(2));
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


}
