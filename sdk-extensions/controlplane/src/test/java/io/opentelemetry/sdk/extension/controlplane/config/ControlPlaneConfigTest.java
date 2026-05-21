/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.autoconfigure.spi.internal.DefaultConfigProperties;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ControlPlaneConfigTest {

  @Test
  void defaultValues() {
    ControlPlaneConfig config = ControlPlaneConfig.builder().build();

    assertThat(config.isEnabled()).isTrue(); // 默认启用控制平面
    // 未配置 otel.exporter.otlp.protocol 时，fallback 为 http/protobuf（与 javaagent 一致）
    assertThat(config.getProtocol()).isEqualTo("http/protobuf");
    assertThat(config.isHttpProtobuf()).isTrue();
    assertThat(config.isGrpc()).isFalse();
    // endpoint 根据协议自动选择：http/protobuf → 4318
    assertThat(config.getEndpoint()).isEqualTo("http://localhost:4318");
    assertThat(config.getHttpBasePath()).isEqualTo("/v1/control");
    assertThat(config.getLongPollTimeout()).isEqualTo(Duration.ofSeconds(60));
    assertThat(config.getStatusReportInterval()).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void defaultValuesWithGrpcProtocol() {
    // 显式设置 grpc 协议时，endpoint 自动选择 4317
    ControlPlaneConfig config = ControlPlaneConfig.builder().setProtocol("grpc").build();

    assertThat(config.getProtocol()).isEqualTo("grpc");
    assertThat(config.isGrpc()).isTrue();
    assertThat(config.getEndpoint()).isEqualTo("http://localhost:4317");
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

  // ===== 统一端点：优先级覆盖模式测试 =====

  @Test
  void dedicatedEndpointOverridesOtlpEndpoint() {
    // 场景：控制平面和遥测分离部署
    Map<String, String> props = new HashMap<>();
    props.put("otel.exporter.otlp.endpoint", "http://collector:4318");
    props.put("otel.exporter.otlp.protocol", "http/protobuf");
    props.put("otel.agent.control.endpoint", "http://control-server:8080");
    ConfigProperties properties = DefaultConfigProperties.createFromMap(props);

    ControlPlaneConfig config = ControlPlaneConfig.create(properties);

    // 控制平面专属 endpoint 生效
    assertThat(config.getEndpoint()).isEqualTo("http://control-server:8080");
    assertThat(config.getProtocol()).isEqualTo("http/protobuf");
    assertThat(config.getControlPlaneUrl()).isEqualTo("http://control-server:8080/v1/control");
  }

  @Test
  void dedicatedProtocolOverridesOtlpProtocol() {
    // 场景：遥测用 gRPC，控制平面用 HTTP
    Map<String, String> props = new HashMap<>();
    props.put("otel.exporter.otlp.protocol", "grpc");
    props.put("otel.exporter.otlp.endpoint", "http://collector:4317");
    props.put("otel.agent.control.protocol", "http/protobuf");
    props.put("otel.agent.control.endpoint", "http://control-server:4318");
    ConfigProperties properties = DefaultConfigProperties.createFromMap(props);

    ControlPlaneConfig config = ControlPlaneConfig.create(properties);

    // 控制平面专属 protocol 和 endpoint 生效
    assertThat(config.getProtocol()).isEqualTo("http/protobuf");
    assertThat(config.getEndpoint()).isEqualTo("http://control-server:4318");
    assertThat(config.getControlPlaneUrl()).isEqualTo("http://control-server:4318/v1/control");
  }

  @Test
  void fallbackToOtlpWhenNoDedicatedConfig() {
    // 场景：统一部署，只配 OTLP endpoint（最常见用法）
    Map<String, String> props = new HashMap<>();
    props.put("otel.exporter.otlp.endpoint", "http://collector:4318");
    props.put("otel.exporter.otlp.protocol", "http/protobuf");
    ConfigProperties properties = DefaultConfigProperties.createFromMap(props);

    ControlPlaneConfig config = ControlPlaneConfig.create(properties);

    // 复用 OTLP 共享配置
    assertThat(config.getEndpoint()).isEqualTo("http://collector:4318");
    assertThat(config.getProtocol()).isEqualTo("http/protobuf");
    assertThat(config.getControlPlaneUrl()).isEqualTo("http://collector:4318/v1/control");
  }

  @Test
  void onlyDedicatedEndpointWithSharedProtocol() {
    // 场景：只覆盖 endpoint，协议继续共享
    Map<String, String> props = new HashMap<>();
    props.put("otel.exporter.otlp.protocol", "http/protobuf");
    props.put("otel.agent.control.endpoint", "http://control-server:9090");
    ConfigProperties properties = DefaultConfigProperties.createFromMap(props);

    ControlPlaneConfig config = ControlPlaneConfig.create(properties);

    assertThat(config.getEndpoint()).isEqualTo("http://control-server:9090");
    assertThat(config.getProtocol()).isEqualTo("http/protobuf");
    assertThat(config.getControlPlaneUrl()).isEqualTo("http://control-server:9090/v1/control");
  }

  @Test
  void onlyDedicatedProtocolWithDefaultEndpoint() {
    // 场景：只覆盖 protocol，endpoint 根据 protocol 推导
    Map<String, String> props = new HashMap<>();
    props.put("otel.agent.control.protocol", "grpc");
    ConfigProperties properties = DefaultConfigProperties.createFromMap(props);

    ControlPlaneConfig config = ControlPlaneConfig.create(properties);

    assertThat(config.getProtocol()).isEqualTo("grpc");
    // endpoint 根据 protocol=grpc 推导为 4317
    assertThat(config.getEndpoint()).isEqualTo("http://localhost:4317");
    assertThat(config.getControlPlaneUrl()).isEqualTo("http://localhost:4317");
  }

  // ===== 统一端点：Token 优先级覆盖测试 =====

  @Test
  void dedicatedTokenOverridesResourceAttributesToken() {
    // 场景：控制平面有专属 token，同时 resource.attributes 也有 token
    Map<String, String> props = new HashMap<>();
    props.put("otel.exporter.otlp.endpoint", "http://collector:4318");
    props.put("otel.exporter.otlp.protocol", "http/protobuf");
    props.put("otel.agent.control.token", "dedicated-secret-token");
    props.put("otel.resource.attributes", "service.name=myapp,token=shared-token");
    ConfigProperties properties = DefaultConfigProperties.createFromMap(props);

    ControlPlaneConfig config = ControlPlaneConfig.create(properties);

    // 控制平面专属 token 生效（最高优先级）
    assertThat(config.hasAuthToken()).isTrue();
    assertThat(config.getAuthToken()).isEqualTo("dedicated-secret-token");
    assertThat(config.getAuthTokenSource()).isEqualTo("otel.agent.control.token (dedicated)");
    assertThat(config.getAuthorizationHeader()).isEqualTo("Bearer dedicated-secret-token");
  }

  @Test
  void dedicatedTokenOverridesOtlpHeadersToken() {
    // 场景：控制平面有专属 token，同时 OTLP headers 也有 Authorization
    Map<String, String> props = new HashMap<>();
    props.put("otel.exporter.otlp.endpoint", "http://collector:4318");
    props.put("otel.exporter.otlp.protocol", "http/protobuf");
    props.put("otel.agent.control.token", "control-plane-token");
    props.put("otel.exporter.otlp.headers", "Authorization=Bearer otlp-token");
    ConfigProperties properties = DefaultConfigProperties.createFromMap(props);

    ControlPlaneConfig config = ControlPlaneConfig.create(properties);

    assertThat(config.hasAuthToken()).isTrue();
    assertThat(config.getAuthToken()).isEqualTo("control-plane-token");
    assertThat(config.getAuthTokenSource()).isEqualTo("otel.agent.control.token (dedicated)");
  }

  @Test
  void fallbackToResourceAttributesTokenWhenNoDedicatedToken() {
    // 场景：无专属 token，fallback 到 resource.attributes 中的 token
    Map<String, String> props = new HashMap<>();
    props.put("otel.exporter.otlp.endpoint", "http://collector:4318");
    props.put("otel.exporter.otlp.protocol", "http/protobuf");
    props.put("otel.resource.attributes", "service.name=myapp,token=resource-token");
    ConfigProperties properties = DefaultConfigProperties.createFromMap(props);

    ControlPlaneConfig config = ControlPlaneConfig.create(properties);

    assertThat(config.hasAuthToken()).isTrue();
    assertThat(config.getAuthToken()).isEqualTo("resource-token");
    assertThat(config.getAuthTokenSource()).isEqualTo("resource.attributes[token]");
  }

  @Test
  void fallbackToOtlpHeadersTokenWhenNoOtherTokenSources() {
    // 场景：无专属 token 也无 resource.attributes token，fallback 到 OTLP headers
    Map<String, String> props = new HashMap<>();
    props.put("otel.exporter.otlp.endpoint", "http://collector:4318");
    props.put("otel.exporter.otlp.protocol", "http/protobuf");
    props.put("otel.exporter.otlp.headers", "Authorization=Bearer header-token");
    ConfigProperties properties = DefaultConfigProperties.createFromMap(props);

    ControlPlaneConfig config = ControlPlaneConfig.create(properties);

    assertThat(config.hasAuthToken()).isTrue();
    assertThat(config.getAuthToken()).isEqualTo("header-token");
    assertThat(config.getAuthTokenSource()).isEqualTo("otel.exporter.otlp.headers[Authorization]");
  }

}
