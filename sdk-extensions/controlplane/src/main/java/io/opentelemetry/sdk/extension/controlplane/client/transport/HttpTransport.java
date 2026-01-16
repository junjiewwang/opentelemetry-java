/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.transport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import javax.annotation.Nullable;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

/**
 * HTTP/Protobuf 传输实现
 *
 * <p>使用 OkHttp 发送 HTTP 请求，支持 Protobuf 和 JSON 两种格式。
 *
 * <p>特性：
 * <ul>
 *   <li>支持 gzip 压缩响应解码
 *   <li>支持鉴权（Authorization Header）
 *   <li>异步非阻塞请求
 *   <li>优雅关闭
 * </ul>
 */
public final class HttpTransport implements Transport {

  private static final Logger logger = Logger.getLogger(HttpTransport.class.getName());

  private static final MediaType PROTOBUF_TYPE = MediaType.parse("application/x-protobuf");
  private static final String HEADER_CONTENT_ENCODING = "Content-Encoding";
  private static final String HEADER_ACCEPT_ENCODING = "Accept-Encoding";
  private static final String GZIP = "gzip";

  private final OkHttpClient httpClient;
  private final String baseUrl;
  @Nullable private final String authorizationHeader;
  private final AtomicBoolean closed;

  /**
   * 创建 HTTP 传输
   *
   * @param config 传输配置
   */
  public HttpTransport(TransportConfig config) {
    this.baseUrl = config.getBaseUrl();
    this.authorizationHeader = config.getAuthorizationHeader();
    this.closed = new AtomicBoolean(false);

    this.httpClient =
        new OkHttpClient.Builder()
            .connectTimeout(config.getConnectTimeout())
            .readTimeout(config.getReadTimeout())
            .writeTimeout(config.getWriteTimeout())
            .retryOnConnectionFailure(true)
            .build();

    logger.log(
        Level.FINE,
        "[HTTP-TRANSPORT] Initialized: baseUrl={0}, hasAuth={1}",
        new Object[] {baseUrl, authorizationHeader != null});
  }

  @Override
  public CompletableFuture<byte[]> sendUnary(
      Operation operation, byte[] requestBody, long timeoutMillis) {
    if (closed.get()) {
      CompletableFuture<byte[]> future = new CompletableFuture<>();
      future.completeExceptionally(new TransportException("Transport is closed"));
      return future;
    }

    CompletableFuture<byte[]> future = new CompletableFuture<>();

    String url = baseUrl + operation.getHttpPath();
    Request.Builder requestBuilder =
        new Request.Builder()
            .url(url)
            .post(RequestBody.create(requestBody, PROTOBUF_TYPE))
            .header(HEADER_ACCEPT_ENCODING, GZIP);

    if (authorizationHeader != null) {
      requestBuilder.header("Authorization", authorizationHeader);
    }

    Request httpRequest = requestBuilder.build();

    httpClient
        .newCall(httpRequest)
        .enqueue(
            new Callback() {
              @Override
              public void onFailure(Call call, IOException e) {
                logger.log(
                    Level.WARNING,
                    "[HTTP-TRANSPORT] Request failed: operation={0}, error={1}",
                    new Object[] {operation, e.getMessage()});
                future.completeExceptionally(
                    new TransportException(
                        "HTTP request failed: " + e.getMessage(), e));
              }

              @Override
              public void onResponse(Call call, Response response) {
                try (ResponseBody body = response.body()) {
                  if (!response.isSuccessful()) {
                    int code = response.code();
                    String message = response.message();
                    logger.log(
                        Level.WARNING,
                        "[HTTP-TRANSPORT] Request unsuccessful: operation={0}, code={1}, message={2}",
                        new Object[] {operation, code, message});
                    future.completeExceptionally(
                        new TransportException(
                            "HTTP " + code + ": " + message, code, operation));
                    return;
                  }

                  byte[] responseBody = readResponseBody(response, body);
                  future.complete(responseBody);
                } catch (IOException e) {
                  logger.log(
                      Level.WARNING,
                      "[HTTP-TRANSPORT] Failed to read response: operation={0}, error={1}",
                      new Object[] {operation, e.getMessage()});
                  future.completeExceptionally(
                      new TransportException("Failed to read response: " + e.getMessage(), e));
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
    return TransportType.HTTP_PROTOBUF;
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
      logger.log(Level.FINE, "[HTTP-TRANSPORT] Closed");
    }
  }

  /**
   * 读取响应体，支持 gzip 解压
   */
  private static byte[] readResponseBody(Response response, ResponseBody body) throws IOException {
    byte[] data = body.bytes();

    // 处理 gzip 压缩
    String encoding = response.header(HEADER_CONTENT_ENCODING);
    if (GZIP.equalsIgnoreCase(encoding)) {
      data = decompress(data);
    }

    return data;
  }

  /**
   * gzip 解压
   */
  private static byte[] decompress(byte[] compressed) throws IOException {
    try (GZIPInputStream gis = new GZIPInputStream(new java.io.ByteArrayInputStream(compressed));
        ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[1024];
      int len;
      while ((len = gis.read(buffer)) > 0) {
        baos.write(buffer, 0, len);
      }
      return baos.toByteArray();
    }
  }
}
