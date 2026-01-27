/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.transport;

import io.opentelemetry.exporter.internal.compression.Compressor;
import io.opentelemetry.exporter.internal.compression.GzipCompressor;
import io.opentelemetry.exporter.sender.okhttp.internal.GrpcRequestBody;
import io.opentelemetry.exporter.internal.marshal.Marshaler;
import io.opentelemetry.exporter.internal.marshal.Serializer;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.ConnectionSpec;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

/**
 * gRPC 传输实现（OkHttp 路线，不依赖 grpc-java）
 *
 * <p>使用 OkHttp 直接发送 gRPC wire format（HTTP/2 + gRPC framing），并解析响应得到 protobuf bytes。
 *
 * <p>特性：
 * <ul>
 *   <li>支持鉴权（Authorization Header）
 *   <li>支持 gzip（gRPC message-level 压缩）
 *   <li>异步非阻塞请求
 *   <li>优雅关闭
 * </ul>
 */
public final class GrpcTransport implements Transport {

  private static final Logger logger = Logger.getLogger(GrpcTransport.class.getName());

  private static final String HEADER_AUTHORIZATION = "Authorization";
  private static final String HEADER_TE = "te";
  private static final String HEADER_GRPC_STATUS = "grpc-status";
  private static final String HEADER_GRPC_MESSAGE = "grpc-message";

  private static final String TE_TRAILERS = "trailers";

  private static final int GRPC_FRAME_HEADER_LENGTH = 5;

  /**
   * Per-call 超时额外缓冲时间（毫秒）。
   *
   * <p>对于长轮询场景，客户端 per-call 超时必须大于服务端 hold 超时 + 网络延迟，
   * 否则会出现"服务端刚准备返回，客户端先超时"的问题。
   *
   * <p>此值与 TransportFactory 中 readTimeout 的缓冲时间保持一致（10秒）。
   */
  private static final long CALL_TIMEOUT_BUFFER_MILLIS = 10_000L;

  private static final String SERVICE_NAME =
      "io.opentelemetry.extension.controlplane.proto.v1.ControlPlaneService";

  private final OkHttpClient httpClient;
  private final String baseUrl;
  @Nullable private final String authorizationHeader;
  @Nullable private final Compressor compressor;
  private final AtomicBoolean closed;

  public GrpcTransport(TransportConfig config) {
    this.baseUrl = config.getBaseUrl();
    this.authorizationHeader = config.getAuthorizationHeader();
    this.closed = new AtomicBoolean(false);
    this.compressor = config.isCompressionEnabled() ? GzipCompressor.getInstance() : null;

    // 对齐 OTLP OkHttpGrpcSender 的协议选择：
    // - http:// 走 h2c prior knowledge
    // - https:// 走 HTTP_2 + HTTP_1_1
    boolean isPlainHttp = baseUrl.startsWith("http://");

    OkHttpClient.Builder builder =
        new OkHttpClient.Builder()
            .connectTimeout(config.getConnectTimeout())
            .readTimeout(config.getReadTimeout())
            .writeTimeout(config.getWriteTimeout())
            // 单次调用总超时：避免长轮询被 callTimeout 意外打断，这里不设置 callTimeout
            .retryOnConnectionFailure(true);

    if (isPlainHttp) {
      builder.connectionSpecs(java.util.Collections.singletonList(ConnectionSpec.CLEARTEXT));
      builder.protocols(java.util.Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE));
    } else {
      builder.protocols(java.util.Arrays.asList(Protocol.HTTP_2, Protocol.HTTP_1_1));
    }

    this.httpClient = builder.build();

    URI uri = URI.create(baseUrl);
    logger.log(
        Level.FINE,
        "[GRPC-TRANSPORT] Initialized: baseUrl={0}, scheme={1}, hasAuth={2}, compression={3}",
        new Object[] {baseUrl, uri.getScheme(), authorizationHeader != null, compressor != null});
  }

  @Override
  public CompletableFuture<byte[]> sendUnary(Operation operation, byte[] requestBody, long timeoutMillis) {
    if (closed.get()) {
      CompletableFuture<byte[]> future = new CompletableFuture<>();
      future.completeExceptionally(new TransportException("Transport is closed"));
      return future;
    }

    CompletableFuture<byte[]> future = new CompletableFuture<>();

    String url = baseUrl + grpcPath(operation);

    RequestBody grpcBody = new GrpcRequestBody(new ByteArrayMarshaler(requestBody), compressor);

    Request.Builder requestBuilder =
        new Request.Builder()
            .url(url)
            .post(grpcBody)
            .header(HEADER_TE, TE_TRAILERS);

    // 如果启用了压缩，需要告知服务端使用的压缩算法
    if (compressor != null) {
      requestBuilder.header("grpc-encoding", compressor.getEncoding());
    }

    if (authorizationHeader != null) {
      requestBuilder.header(HEADER_AUTHORIZATION, authorizationHeader);
    }

    // 每次请求都设置一次 per-call timeout，避免影响 OkHttpClient 全局设置
    // 对于长轮询请求，需要添加 buffer 确保：客户端超时 > 服务端 hold 时间 + 网络延迟
    Call call = httpClient.newCall(requestBuilder.build());
    if (timeoutMillis > 0) {
      long effectiveTimeout = timeoutMillis + CALL_TIMEOUT_BUFFER_MILLIS;
      call.timeout().timeout(effectiveTimeout, TimeUnit.MILLISECONDS);
      logger.log(
          Level.FINE,
          "[GRPC-TRANSPORT] Set per-call timeout: operation={0}, requestedTimeout={1}ms, effectiveTimeout={2}ms",
          new Object[] {operation, timeoutMillis, effectiveTimeout});
    }

    call.enqueue(
        new Callback() {
          @Override
          public void onFailure(Call call, IOException e) {
            logger.log(
                Level.WARNING,
                "[GRPC-TRANSPORT] Request failed: operation={0}, error={1}",
                new Object[] {operation, e.getMessage()});
            future.completeExceptionally(new TransportException("gRPC request failed: " + e.getMessage(), e));
          }

          @Override
          public void onResponse(Call call, Response response) {
            try (ResponseBody body = response.body()) {
              byte[] rawBody = body.bytes();

              // 先解析 grpc-status / grpc-message
              GrpcStatus grpcStatus = readGrpcStatus(response);

              if (grpcStatus.statusCodeString == null) {
                // grpc-status 取不到通常意味着 HTTP 层错误或 trailers 不可读
                future.completeExceptionally(
                    new TransportException(
                        "gRPC status missing. HTTP " + response.code() + ": " + response.message(),
                        response.code(),
                        operation));
                return;
              }

              int statusCodeInt;
              try {
                statusCodeInt = Integer.parseInt(grpcStatus.statusCodeString);
              } catch (NumberFormatException ex) {
                statusCodeInt = -1;
              }

              if (statusCodeInt != 0) {
                String codeName = grpcCodeName(statusCodeInt);
                future.completeExceptionally(
                    new TransportException(
                        "gRPC " + codeName + ": " + grpcStatus.message,
                        codeName,
                        operation));
                return;
              }

              // 成功时解析 gRPC framing，提取 message bytes
              byte[] messageBytes;
              try {
                messageBytes = decodeSingleMessage(rawBody);
              } catch (IllegalArgumentException ex) {
                future.completeExceptionally(
                    new TransportException("Invalid gRPC response frame: " + ex.getMessage(), ex));
                return;
              }

              future.complete(messageBytes);
            } catch (IOException e) {
              logger.log(
                  Level.WARNING,
                  "[GRPC-TRANSPORT] Failed to read response: operation={0}, error={1}",
                  new Object[] {operation, e.getMessage()});
              future.completeExceptionally(new TransportException("Failed to read response: " + e.getMessage(), e));
            }
          }
        });

    return future;
  }

  @Override
  public boolean isAvailable() {
    return !closed.get();
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public TransportType getType() {
    return TransportType.GRPC;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      httpClient.dispatcher().executorService().shutdown();
      try {
        if (!httpClient.dispatcher().executorService().awaitTermination(5, TimeUnit.SECONDS)) {
          httpClient.dispatcher().executorService().shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        httpClient.dispatcher().executorService().shutdownNow();
      }
      httpClient.connectionPool().evictAll();
      logger.log(Level.FINE, "[GRPC-TRANSPORT] Closed");
    }
  }

  private static String grpcPath(Operation operation) {
    String method;
    switch (operation) {
      case UNIFIED_POLL:
        method = "UnifiedPoll";
        break;
      case GET_CONFIG:
        method = "GetConfig";
        break;
      case GET_TASKS:
        method = "GetTasks";
        break;
      case REPORT_STATUS:
        method = "ReportStatus";
        break;
      case REPORT_TASK_RESULT:
        method = "ReportTaskResult";
        break;
      case UPLOAD_CHUNK:
        method = "UploadChunkedResult";
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + operation);
    }
    return "/" + SERVICE_NAME + "/" + method;
  }

  private static byte[] decodeSingleMessage(byte[] grpcResponseBody) {
    if (grpcResponseBody.length < GRPC_FRAME_HEADER_LENGTH) {
      throw new IllegalArgumentException("Body too small: " + grpcResponseBody.length);
    }

    ByteBuffer buffer = ByteBuffer.wrap(grpcResponseBody).order(ByteOrder.BIG_ENDIAN);
    byte compressedFlag = buffer.get();
    int messageLength = buffer.getInt();

    if (messageLength < 0) {
      throw new IllegalArgumentException("Negative message length: " + messageLength);
    }

    int remaining = buffer.remaining();
    if (remaining < messageLength) {
      throw new IllegalArgumentException(
          "Not enough bytes. expected=" + messageLength + ", remaining=" + remaining);
    }

    byte[] message = new byte[messageLength];
    buffer.get(message);

    if (compressedFlag == 0) {
      return message;
    }

    // 服务端如果返回 compressed flag=1，则需要解压。这里按 gzip 处理。
    // 当前控制平面默认只启用 gzip，所以直接使用 GZIPInputStream。
    try {
      return HttpGzip.decompress(message);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to decompress message", e);
    }
  }

  private static GrpcStatus readGrpcStatus(Response response) {
    String status = response.header(HEADER_GRPC_STATUS);
    String message = response.header(HEADER_GRPC_MESSAGE);

    if (status == null || message == null) {
      // 尝试 trailers
      try {
        Headers trailers = response.trailers();
        if (status == null) {
          status = trailers.get(HEADER_GRPC_STATUS);
        }
        if (message == null) {
          message = trailers.get(HEADER_GRPC_MESSAGE);
        }
      } catch (IOException e) {
        // ignore; will be handled by caller
      }
    }

    if (message != null) {
      message = unescapeGrpcMessage(message);
    } else {
      message = response.message();
    }

    return new GrpcStatus(status, message);
  }

  private static String grpcCodeName(int grpcStatusCode) {
    // 只需要满足 TransportException.isRetryable 的判断场景
    switch (grpcStatusCode) {
      case 1:
        return "CANCELLED";
      case 2:
        return "UNKNOWN";
      case 3:
        return "INVALID_ARGUMENT";
      case 4:
        return "DEADLINE_EXCEEDED";
      case 5:
        return "NOT_FOUND";
      case 6:
        return "ALREADY_EXISTS";
      case 7:
        return "PERMISSION_DENIED";
      case 8:
        return "RESOURCE_EXHAUSTED";
      case 9:
        return "FAILED_PRECONDITION";
      case 10:
        return "ABORTED";
      case 11:
        return "OUT_OF_RANGE";
      case 12:
        return "UNIMPLEMENTED";
      case 13:
        return "INTERNAL";
      case 14:
        return "UNAVAILABLE";
      case 15:
        return "DATA_LOSS";
      case 16:
        return "UNAUTHENTICATED";
      case 0:
      default:
        return "OK";
    }
  }

  // From grpc-java / OkHttpGrpcSender
  private static String unescapeGrpcMessage(String value) {
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c < ' ' || c >= '~' || (c == '%' && i + 2 < value.length())) {
        return doUnescape(value.getBytes(StandardCharsets.US_ASCII));
      }
    }
    return value;
  }

  private static String doUnescape(byte[] value) {
    ByteBuffer buf = ByteBuffer.allocate(value.length);
    for (int i = 0; i < value.length; ) {
      if (value[i] == '%' && i + 2 < value.length) {
        try {
          buf.put((byte) Integer.parseInt(new String(value, i + 1, 2, StandardCharsets.UTF_8), 16));
          i += 3;
          continue;
        } catch (NumberFormatException e) {
          // ignore
        }
      }
      buf.put(value[i]);
      i += 1;
    }
    return new String(buf.array(), 0, buf.position(), StandardCharsets.UTF_8);
  }

  private static final class GrpcStatus {
    @Nullable private final String statusCodeString;
    private final String message;

    private GrpcStatus(@Nullable String statusCodeString, String message) {
      this.statusCodeString = statusCodeString;
      this.message = message;
    }
  }

  private static final class ByteArrayMarshaler extends Marshaler {
    private final byte[] bytes;

    private ByteArrayMarshaler(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public int getBinarySerializedSize() {
      return bytes.length;
    }

    @Override
    protected void writeTo(Serializer output) throws IOException {
      output.writeSerializedMessage(bytes, "");
    }
  }

  /**
   * 仅用于处理 gRPC response message-level gzip 解压（当 compressed flag=1）。
   */
  private static final class HttpGzip {
    private static byte[] decompress(byte[] compressed) throws IOException {
      try (java.util.zip.GZIPInputStream gis =
              new java.util.zip.GZIPInputStream(new java.io.ByteArrayInputStream(compressed));
          java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream()) {
        byte[] buffer = new byte[1024];
        int len;
        while ((len = gis.read(buffer)) > 0) {
          baos.write(buffer, 0, len);
        }
        return baos.toByteArray();
      }
    }
  }
}
