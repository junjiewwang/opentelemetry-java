/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.transport;

import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import java.time.Duration;

/**
 * 传输层工厂
 *
 * <p>根据配置创建对应的传输实现。
 */
public final class TransportFactory {

  /** 默认连接超时时间（秒） */
  private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(30);

  /** 默认写入超时时间（秒） */
  private static final Duration DEFAULT_WRITE_TIMEOUT = Duration.ofSeconds(30);

  /** 读超时额外缓冲时间（秒），确保读超时大于长轮询超时 */
  private static final int READ_TIMEOUT_BUFFER_SECONDS = 10;

  private TransportFactory() {}

  /**
   * 根据配置创建传输实例
   *
   * @param config 控制平面配置
   * @return 传输实例
   */
  public static Transport create(ControlPlaneConfig config) {
    TransportConfig transportConfig =
        TransportConfig.builder()
            .baseUrl(config.getControlPlaneUrl())
            .authorizationHeader(config.getAuthorizationHeader())
            .connectTimeout(DEFAULT_CONNECT_TIMEOUT)
            .readTimeout(config.getLongPollTimeout().plusSeconds(READ_TIMEOUT_BUFFER_SECONDS))
            .writeTimeout(DEFAULT_WRITE_TIMEOUT)
            .compressionEnabled(true)
            .build();

    if (config.isGrpc()) {
      if (!isGrpcAvailable()) {
        throw new IllegalStateException(
            "gRPC transport requested but gRPC dependencies are not available. "
                + "Please add io.grpc:grpc-api, io.grpc:grpc-protobuf, and io.grpc:grpc-stub "
                + "to your classpath, or use HTTP transport instead.");
      }
      return new GrpcTransport(transportConfig);
    } else {
      return new HttpTransport(transportConfig);
    }
  }

  /**
   * 检查 gRPC 依赖是否可用
   *
   * <p>由于 gRPC 是 compileOnly 依赖，运行时可能不存在。
   *
   * @return 如果 gRPC 依赖可用返回 true
   */
  public static boolean isGrpcAvailable() {
    try {
      Class.forName("io.grpc.ManagedChannel");
      Class.forName("io.grpc.stub.AbstractFutureStub");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
