/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.transport;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * 控制平面传输层接口
 *
 * <p>抽象底层传输协议（HTTP/gRPC），只负责字节流的收发。 所有序列化/反序列化由上层 Codec 处理。
 *
 * <p>设计原则：
 * <ul>
 *   <li>Transport 不理解业务含义，只负责传输
 *   <li>使用 {@link Operation} 枚举而非字符串路径，便于 gRPC 映射
 *   <li>HTTP 实现做 operation -> path 映射
 *   <li>gRPC 实现做 operation -> stub method 映射
 * </ul>
 */
public interface Transport extends Closeable {

  /**
   * 发送一元请求
   *
   * @param operation 操作类型（用于路由到正确的端点/方法）
   * @param requestBody 请求体（已序列化的 Protobuf 字节）
   * @param timeoutMillis 超时时间（毫秒），0 表示使用默认超时
   * @return 响应体的 CompletableFuture
   * @throws TransportException 如果传输失败
   */
  CompletableFuture<byte[]> sendUnary(Operation operation, byte[] requestBody, long timeoutMillis);

  /**
   * 检查传输层是否可用
   *
   * @return 如果传输层可用返回 true
   */
  boolean isAvailable();

  /**
   * 检查传输层是否已关闭
   *
   * @return 如果传输层已关闭返回 true
   */
  boolean isClosed();

  /**
   * 获取传输协议类型
   *
   * @return 传输协议类型
   */
  TransportType getType();

  /**
   * 关闭传输层，释放资源
   */
  @Override
  void close();

  /**
   * 控制平面操作枚举
   *
   * <p>使用枚举而非字符串路径，便于：
   * <ul>
   *   <li>类型安全（编译期检查）
   *   <li>统一日志/指标标签
   *   <li>gRPC 实现可以 switch 到不同 stub 方法
   *   <li>HTTP 实现做 operation -> path 映射
   * </ul>
   */
  enum Operation {
    /** 统一长轮询（配置 + 任务） */
    UNIFIED_POLL("/poll"),

    /** 仅配置长轮询 */
    GET_CONFIG("/poll/config"),

    /** 仅任务长轮询 */
    GET_TASKS("/poll/tasks"),

    /** 上报状态 */
    REPORT_STATUS("/status"),

    /** 上报任务结果 */
    REPORT_TASK_RESULT("/tasks/result"),

    /** 上传分片结果 */
    UPLOAD_CHUNK("/upload-chunk");

    private final String httpPath;

    Operation(String httpPath) {
      this.httpPath = httpPath;
    }

    /**
     * 获取 HTTP 路径（相对于 baseUrl）
     *
     * @return HTTP 路径
     */
    public String getHttpPath() {
      return httpPath;
    }
  }

  /**
   * 传输协议类型枚举
   */
  enum TransportType {
    /** HTTP/Protobuf 传输 */
    HTTP_PROTOBUF("http/protobuf"),

    /** gRPC 传输 */
    GRPC("grpc");

    private final String value;

    TransportType(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }
}
