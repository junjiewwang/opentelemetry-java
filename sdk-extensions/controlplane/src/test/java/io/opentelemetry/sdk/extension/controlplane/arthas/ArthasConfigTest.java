/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

class ArthasConfigTest {

  @Test
  void defaultConfigHasExpectedValues() {
    ArthasConfig config = ArthasConfig.builder().build();

    assertThat(config.isEnabled()).isFalse();
    assertThat(config.getVersion()).isEqualTo("4.0.3");
    assertThat(config.getMaxSessionsPerAgent()).isEqualTo(2);
    assertThat(config.getSessionIdleTimeout()).isEqualTo(Duration.ofMinutes(30));
    assertThat(config.getSessionMaxDuration()).isEqualTo(Duration.ofHours(2));
    assertThat(config.getIdleShutdownDelay()).isEqualTo(Duration.ofMinutes(5));
    assertThat(config.getMaxRunningDuration()).isEqualTo(Duration.ofHours(4));
    assertThat(config.getTunnelEndpoint()).isNull();
    assertThat(config.getTunnelReconnectInterval()).isEqualTo(Duration.ofSeconds(30));
    assertThat(config.getTunnelMaxReconnectAttempts()).isEqualTo(0);
    assertThat(config.getTunnelConnectTimeout()).isEqualTo(Duration.ofSeconds(30));
    assertThat(config.getTunnelPingInterval()).isEqualTo(Duration.ofSeconds(30));
    assertThat(config.getLibPath()).isNull();
    assertThat(config.getCommandTimeout()).isEqualTo(Duration.ofMinutes(5));
    assertThat(config.getOutputBufferSize()).isEqualTo(65536);
    assertThat(config.getOutputFlushInterval()).isEqualTo(Duration.ofMillis(50));
  }

  @Test
  void defaultDisabledCommandsIncludeDangerousCommands() {
    ArthasConfig config = ArthasConfig.builder().build();

    assertThat(config.getDisabledCommands()).containsExactlyInAnyOrder("stop", "reset", "shutdown", "quit");
  }

  @Test
  void isCommandDisabledIsCaseInsensitive() {
    ArthasConfig config = ArthasConfig.builder().build();

    assertThat(config.isCommandDisabled("stop")).isTrue();
    assertThat(config.isCommandDisabled("STOP")).isTrue();
    assertThat(config.isCommandDisabled("Stop")).isTrue();
    assertThat(config.isCommandDisabled("help")).isFalse();
  }

  @Test
  void builderCanSetAllValues() {
    ArthasConfig config =
        ArthasConfig.builder()
            .setEnabled(true)
            .setVersion("3.8.0")
            .setMaxSessionsPerAgent(5)
            .setSessionIdleTimeout(Duration.ofMinutes(15))
            .setSessionMaxDuration(Duration.ofHours(1))
            .setIdleShutdownDelay(Duration.ofMinutes(10))
            .setMaxRunningDuration(Duration.ofHours(8))
            .setTunnelEndpoint("ws://localhost:8080/ws")
            .setTunnelReconnectInterval(Duration.ofSeconds(10))
            .setTunnelMaxReconnectAttempts(5)
            .setTunnelConnectTimeout(Duration.ofSeconds(15))
            .setTunnelPingInterval(Duration.ofSeconds(20))
            .setLibPath("/opt/arthas/lib")
            .setCommandTimeout(Duration.ofMinutes(10))
            .setOutputBufferSize(131072)
            .setOutputFlushInterval(Duration.ofMillis(100))
            .build();

    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getVersion()).isEqualTo("3.8.0");
    assertThat(config.getMaxSessionsPerAgent()).isEqualTo(5);
    assertThat(config.getSessionIdleTimeout()).isEqualTo(Duration.ofMinutes(15));
    assertThat(config.getSessionMaxDuration()).isEqualTo(Duration.ofHours(1));
    assertThat(config.getIdleShutdownDelay()).isEqualTo(Duration.ofMinutes(10));
    assertThat(config.getMaxRunningDuration()).isEqualTo(Duration.ofHours(8));
    assertThat(config.getTunnelEndpoint()).isEqualTo("ws://localhost:8080/ws");
    assertThat(config.getTunnelReconnectInterval()).isEqualTo(Duration.ofSeconds(10));
    assertThat(config.getTunnelMaxReconnectAttempts()).isEqualTo(5);
    assertThat(config.getTunnelConnectTimeout()).isEqualTo(Duration.ofSeconds(15));
    assertThat(config.getTunnelPingInterval()).isEqualTo(Duration.ofSeconds(20));
    assertThat(config.getLibPath()).isEqualTo("/opt/arthas/lib");
    assertThat(config.getCommandTimeout()).isEqualTo(Duration.ofMinutes(10));
    assertThat(config.getOutputBufferSize()).isEqualTo(131072);
    assertThat(config.getOutputFlushInterval()).isEqualTo(Duration.ofMillis(100));
  }

  @Test
  void hasTunnelEndpointReturnsFalseWhenNotSet() {
    ArthasConfig config = ArthasConfig.builder().build();
    assertThat(config.hasTunnelEndpoint()).isFalse();
    assertThat(config.hasExplicitTunnelEndpoint()).isFalse();
  }

  @Test
  void hasTunnelEndpointReturnsTrueWhenSet() {
    ArthasConfig config =
        ArthasConfig.builder().setTunnelEndpoint("ws://localhost:8080/ws").build();
    assertThat(config.hasTunnelEndpoint()).isTrue();
    assertThat(config.hasExplicitTunnelEndpoint()).isTrue();
  }

  @Test
  void hasTunnelEndpointReturnsFalseWhenEmpty() {
    ArthasConfig config = ArthasConfig.builder().setTunnelEndpoint("").build();
    assertThat(config.hasTunnelEndpoint()).isFalse();
    assertThat(config.hasExplicitTunnelEndpoint()).isFalse();
  }

  @Test
  void defaultTunnelEndpointGeneratedFromOtlpEndpoint() {
    // 测试基于 OTLP endpoint 自动生成默认 Tunnel 端点
    ArthasConfig config =
        ArthasConfig.builder()
            .setBaseOtlpEndpoint("http://localhost:4317")
            .build();

    assertThat(config.getTunnelEndpoint()).isEqualTo("ws://localhost:4317/v1/arthas/ws");
    assertThat(config.hasExplicitTunnelEndpoint()).isFalse();
    assertThat(config.hasTunnelEndpoint()).isTrue();
  }

  @Test
  void defaultTunnelEndpointHttps() {
    // 测试 HTTPS 转换为 WSS
    ArthasConfig config =
        ArthasConfig.builder()
            .setBaseOtlpEndpoint("https://otel.example.com:443")
            .build();

    assertThat(config.getTunnelEndpoint()).isEqualTo("wss://otel.example.com:443/v1/arthas/ws");
  }

  @Test
  void explicitTunnelEndpointOverridesDefault() {
    // 显式配置的 Tunnel 端点优先级更高
    ArthasConfig config =
        ArthasConfig.builder()
            .setBaseOtlpEndpoint("http://localhost:4317")
            .setTunnelEndpoint("ws://custom:8080/ws")
            .build();

    assertThat(config.getTunnelEndpoint()).isEqualTo("ws://custom:8080/ws");
    assertThat(config.hasExplicitTunnelEndpoint()).isTrue();
  }

  @Test
  void addDisabledCommandAddsToExistingSet() {
    ArthasConfig config =
        ArthasConfig.builder().addDisabledCommand("dangerous").addDisabledCommand("unsafe").build();

    assertThat(config.getDisabledCommands()).contains("dangerous", "unsafe", "stop", "reset");
  }

  @Test
  void setDisabledCommandsReplacesDefaultSet() {
    ArthasConfig config =
        ArthasConfig.builder()
            .setDisabledCommands(new HashSet<>(Arrays.asList("custom1", "custom2")))
            .build();

    assertThat(config.getDisabledCommands()).containsExactlyInAnyOrder("custom1", "custom2");
    assertThat(config.getDisabledCommands()).doesNotContain("stop");
  }

  @Test
  void maxSessionsPerAgentMustBeAtLeastOne() {
    assertThatThrownBy(() -> ArthasConfig.builder().setMaxSessionsPerAgent(0).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxSessionsPerAgent must be >= 1");
  }

  @Test
  void outputBufferSizeMustBeAtLeast1024() {
    assertThatThrownBy(() -> ArthasConfig.builder().setOutputBufferSize(512).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("outputBufferSize must be >= 1024");
  }

  @Test
  void toStringContainsKeyFields() {
    ArthasConfig config =
        ArthasConfig.builder()
            .setEnabled(true)
            .setVersion("3.8.0")
            .setTunnelEndpoint("ws://localhost:8080/ws")
            .build();

    String str = config.toString();
    assertThat(str).contains("enabled=true");
    assertThat(str).contains("version='3.8.0'");
    assertThat(str).contains("tunnelEndpoint='ws://localhost:8080/ws'");
  }
}
