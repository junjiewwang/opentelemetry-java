/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client;

import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpHealthMonitor;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
 * HTTP/Protobuf 控制平面客户端实现
 *
 * <p>使用 HTTP 长轮询方式与控制平面服务通信，数据格式为 Protobuf。
 *
 * <p>API 端点：
 * <ul>
 *   <li>POST /v1/control/poll - 统一长轮询（配置+任务）
 *   <li>POST /v1/control/poll/config - 仅配置长轮询
 *   <li>POST /v1/control/poll/tasks - 仅任务长轮询
 * </ul>
 */
public final class HttpControlPlaneClient implements ControlPlaneClient {

  private static final Logger logger = Logger.getLogger(HttpControlPlaneClient.class.getName());
  private static final MediaType PROTOBUF_TYPE = MediaType.parse("application/x-protobuf");
  private static final MediaType JSON_TYPE = MediaType.parse("application/json");
  private static final String HEADER_CONTENT_ENCODING = "Content-Encoding";
  private static final String HEADER_ACCEPT_ENCODING = "Accept-Encoding";
  private static final String GZIP = "gzip";

  // API 路径常量（相对于 baseUrl，baseUrl 应为 http://host:port/v1/control）
  private static final String PATH_UNIFIED_POLL = "/poll";
  private static final String PATH_CONFIG_POLL = "/poll/config";
  private static final String PATH_TASKS_POLL = "/poll/tasks";
  private static final String PATH_STATUS = "/status";
  private static final String PATH_UPLOAD_CHUNK = "/upload-chunk";

  private final OtlpHealthMonitor healthMonitor;
  private final OkHttpClient httpClient;
  private final String baseUrl;
  private final AtomicBoolean closed;

  // 缓存的 Authorization Header（final，启动时设置）
  @Nullable private final String authorizationHeader;

  /**
   * 创建 HTTP 控制平面客户端
   *
   * @param config 控制平面配置
   * @param healthMonitor OTLP 健康监控器
   */
  public HttpControlPlaneClient(ControlPlaneConfig config, OtlpHealthMonitor healthMonitor) {
    this.healthMonitor = healthMonitor;
    this.baseUrl = config.getControlPlaneUrl();
    this.closed = new AtomicBoolean(false);

    // 直接从 config 获取预解析的 Authorization Header
    this.authorizationHeader = config.getAuthorizationHeader();

    // 构建 OkHttpClient，支持长轮询超时
    Duration longPollTimeout = config.getLongPollTimeout();
    this.httpClient =
        new OkHttpClient.Builder()
            .connectTimeout(Duration.ofSeconds(30))
            .readTimeout(longPollTimeout.plusSeconds(10)) // 比长轮询超时多一点
            .writeTimeout(Duration.ofSeconds(30))
            .retryOnConnectionFailure(true)
            .build();

    if (this.authorizationHeader != null) {
      logger.log(
          Level.INFO,
          "HTTP Control Plane client initialized with authentication, baseUrl: {0}, tokenSource: {1}",
          new Object[] {baseUrl, config.getAuthTokenSource()});
    } else {
      logger.log(
          Level.INFO,
          "HTTP Control Plane client initialized without authentication, baseUrl: {0}",
          baseUrl);
    }
  }

  @Override
  public CompletableFuture<UnifiedPollResponse> poll(UnifiedPollRequest request) {
    checkNotClosed();
    checkOtlpHealth();

    CompletableFuture<UnifiedPollResponse> future = new CompletableFuture<>();

    // 构建请求体
    byte[] requestBody = serializeUnifiedPollRequest(request);

    Request httpRequest =
        buildRequest(baseUrl + PATH_UNIFIED_POLL)
            .post(RequestBody.create(requestBody, JSON_TYPE))
            .build();

    executeAsync(
        httpRequest,
        future,
        responseBody -> parseUnifiedPollResponse(responseBody),
        DefaultUnifiedPollResponse::error);

    return future;
  }

  @Override
  public CompletableFuture<ConfigResponse> getConfig(ConfigRequest request) {
    checkNotClosed();
    checkOtlpHealth();

    CompletableFuture<ConfigResponse> future = new CompletableFuture<>();

    // 构建请求体 (简化版本，实际应使用 Protobuf 序列化)
    byte[] requestBody = serializeConfigRequest(request);

    Request httpRequest =
        buildRequest(baseUrl + PATH_CONFIG_POLL)
            .post(RequestBody.create(requestBody, PROTOBUF_TYPE))
            .build();

    executeAsync(
        httpRequest,
        future,
        responseBody -> parseConfigResponse(responseBody),
        DefaultConfigResponse::error);

    return future;
  }

  @Override
  public CompletableFuture<TaskResponse> getTasks(TaskRequest request) {
    checkNotClosed();
    checkOtlpHealth();

    CompletableFuture<TaskResponse> future = new CompletableFuture<>();

    byte[] requestBody = serializeTaskRequest(request);

    Request httpRequest =
        buildRequest(baseUrl + PATH_TASKS_POLL)
            .post(RequestBody.create(requestBody, PROTOBUF_TYPE))
            .build();

    executeAsync(
        httpRequest,
        future,
        responseBody -> parseTaskResponse(responseBody),
        DefaultTaskResponse::error);

    return future;
  }

  @Override
  public CompletableFuture<StatusResponse> reportStatus(StatusRequest request) {
    checkNotClosed();
    // 状态上报不依赖 OTLP 健康状态，始终尝试发送

    CompletableFuture<StatusResponse> future = new CompletableFuture<>();

    byte[] requestBody = request.getStatusData();

    Request httpRequest =
        buildRequest(baseUrl + PATH_STATUS)
            .post(RequestBody.create(requestBody, PROTOBUF_TYPE))
            .build();

    executeAsync(
        httpRequest,
        future,
        responseBody -> parseStatusResponse(responseBody),
        DefaultStatusResponse::error);

    return future;
  }

  @Override
  public CompletableFuture<ChunkedUploadResponse> uploadChunkedResult(ChunkedTaskResult chunk) {
    checkNotClosed();

    CompletableFuture<ChunkedUploadResponse> future = new CompletableFuture<>();

    byte[] requestBody = serializeChunkedTaskResult(chunk);

    Request httpRequest =
        buildRequest(baseUrl + PATH_UPLOAD_CHUNK)
            .post(RequestBody.create(requestBody, PROTOBUF_TYPE))
            .build();

    executeAsync(
        httpRequest,
        future,
        responseBody -> parseChunkedUploadResponse(responseBody),
        DefaultChunkedUploadResponse::error);

    return future;
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public boolean fetchConfig() {
    if (closed.get()) {
      logger.log(Level.WARNING, "Cannot fetch config: client is closed");
      return false;
    }

    // 构建 POST 请求来获取配置（服务端只支持 POST 方法）
    // 使用空的请求体作为简单的连接检查
    String emptyRequest = "{}";
    Request httpRequest =
        buildRequest(baseUrl + PATH_UNIFIED_POLL)
            .post(RequestBody.create(emptyRequest.getBytes(StandardCharsets.UTF_8), JSON_TYPE))
            .build();

    try (Response response = httpClient.newCall(httpRequest).execute()) {
      int code = response.code();
      if (response.isSuccessful()) {
        logger.log(Level.FINE, "Control plane config fetch successful (HTTP {0})", code);
        return true;
      } else if (code == 404) {
        // API 端点不存在
        logger.log(
            Level.WARNING,
            "Control plane API endpoint not found (HTTP 404), server may not support control plane");
        return false;
      } else if (code == 405) {
        // 方法不允许（理论上不应该再出现此错误）
        logger.log(
            Level.WARNING,
            "Control plane API method not allowed (HTTP 405), please check server configuration");
        return false;
      } else if (code >= 500) {
        // 服务器错误
        logger.log(Level.WARNING, "Control plane server error (HTTP {0})", code);
        return false;
      } else {
        // 其他错误（如 401, 403 等）
        logger.log(
            Level.WARNING,
            "Control plane request failed (HTTP {0}): {1}",
            new Object[] {code, response.message()});
        return false;
      }
    } catch (IOException e) {
      // 网络错误
      logger.log(Level.WARNING, "Control plane connection failed: {0}", e.getMessage());
      return false;
    }
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
      logger.log(Level.INFO, "HTTP Control Plane client closed");
    }
  }

  private void checkNotClosed() {
    if (closed.get()) {
      throw new IllegalStateException("Client is closed");
    }
  }

  private void checkOtlpHealth() {
    if (!healthMonitor.isHealthy()) {
      logger.log(
          Level.FINE,
          "OTLP is not healthy, control plane request may be delayed. State: {0}",
          healthMonitor.getState());
    }
  }

  /**
   * 构建带鉴权的 Request.Builder
   *
   * @param url 请求 URL
   * @return 配置好的 Request.Builder
   */
  private Request.Builder buildRequest(String url) {
    Request.Builder builder =
        new Request.Builder()
            .url(url)
            .header(HEADER_ACCEPT_ENCODING, GZIP);

    // 添加缓存的 Authorization Header
    if (authorizationHeader != null) {
      builder.header("Authorization", authorizationHeader);
    }

    return builder;
  }

  private <T> void executeAsync(
      Request request,
      CompletableFuture<T> future,
      ResponseParser<T> parser,
      ErrorResponseFactory<T> errorFactory) {

    httpClient
        .newCall(request)
        .enqueue(
            new Callback() {
              @Override
              public void onFailure(Call call, IOException e) {
                String endpoint = request.url().encodedPath();
                logger.log(
                    Level.WARNING,
                    "HTTP request failed: endpoint={0}, error={1}",
                    new Object[] {endpoint, e.getMessage()});
                String errorMsg =
                    "endpoint=" + endpoint + ", " + (e.getMessage() != null ? e.getMessage() : "Unknown error");
                future.complete(errorFactory.create(errorMsg));
              }

              @Override
              public void onResponse(Call call, Response response) {
                try (ResponseBody body = response.body()) {
                  if (!response.isSuccessful()) {
                    String endpoint = request.url().encodedPath();
                    String errorMsg =
                        "HTTP " + response.code() + ": " + response.message() + " (endpoint: " + endpoint + ")";
                    logger.log(
                        Level.FINE,
                        "HTTP request unsuccessful: endpoint={0}, code={1}, message={2}",
                        new Object[] {endpoint, response.code(), response.message()});
                    future.complete(errorFactory.create(errorMsg));
                    return;
                  }

                  byte[] responseBody = readResponseBody(response, body);
                  future.complete(parser.parse(responseBody));
                } catch (Exception e) {
                  logger.log(Level.WARNING, "Failed to parse response", e);
                  String errorMsg = e.getMessage() != null ? e.getMessage() : "Unknown error";
                  future.complete(errorFactory.create(errorMsg));
                }
              }
            });
  }

  private static byte[] readResponseBody(Response response, ResponseBody body) throws IOException {
    byte[] data = body.bytes();

    // 处理 gzip 压缩
    String encoding = response.header(HEADER_CONTENT_ENCODING);
    if (GZIP.equalsIgnoreCase(encoding)) {
      data = decompress(data);
    }

    return data;
  }

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

  // ===== 序列化方法 =====

  private static byte[] serializeUnifiedPollRequest(UnifiedPollRequest request) {
    // 按照新的 API 格式序列化
    String json =
        String.format(
            Locale.ROOT,
            "{\"agent_id\":\"%s\",\"current_config_version\":\"%s\",\"current_config_etag\":\"%s\",\"timeout_millis\":%d}",
            request.getAgentId(),
            request.getCurrentConfigVersion(),
            request.getCurrentConfigEtag(),
            request.getTimeoutMillis());
    return json.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] serializeConfigRequest(ConfigRequest request) {
    // TODO: 使用 Protobuf 序列化
    // 这里返回简化的 JSON 格式作为临时实现
    String json =
        String.format(
            Locale.ROOT,
            "{\"agent_id\":\"%s\",\"current_config_version\":\"%s\",\"current_config_etag\":\"%s\",\"timeout_millis\":%d}",
            request.getAgentId(),
            request.getCurrentConfigVersion(),
            request.getCurrentEtag(),
            request.getLongPollTimeoutMillis());
    return json.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] serializeTaskRequest(TaskRequest request) {
    // TODO: 使用 Protobuf 序列化
    String json =
        String.format(
            Locale.ROOT,
            "{\"agent_id\":\"%s\",\"timeout_millis\":%d}",
            request.getAgentId(),
            request.getLongPollTimeoutMillis());
    return json.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] serializeChunkedTaskResult(ChunkedTaskResult chunk) {
    // TODO: 使用 Protobuf 序列化
    return chunk.getChunkData();
  }

  // ===== 解析方法 =====

  @SuppressWarnings("UnusedVariable")
  private static UnifiedPollResponse parseUnifiedPollResponse(byte[] data) {
    // TODO: 使用 JSON/Protobuf 反序列化
    // 这里返回简化的实现
    String json = new String(data, StandardCharsets.UTF_8);
    
    // 简化解析（实际应使用 JSON 解析器）
    boolean hasAnyChanges = json.contains("\"has_any_changes\":true") 
        || json.contains("\"has_any_changes\": true");
    
    Map<String, PollResult> results = new HashMap<>();
    
    // 解析 CONFIG 结果
    if (json.contains("\"CONFIG\"")) {
      boolean configHasChanges = json.contains("\"has_changes\":true") 
          || json.contains("\"has_changes\": true");
      results.put("CONFIG", new DefaultPollResult("CONFIG", configHasChanges, null, null, null, null));
    }
    
    // 解析 TASK 结果
    if (json.contains("\"TASK\"")) {
      results.put("TASK", new DefaultPollResult("TASK", /* hasChanges= */ false, null, null, null, Collections.emptyList()));
    }
    
    return new DefaultUnifiedPollResponse(/* success= */ true, hasAnyChanges, results, "");
  }

  @SuppressWarnings("UnusedVariable")
  private static ConfigResponse parseConfigResponse(byte[] data) {
    // TODO: 使用 Protobuf 反序列化
    return new DefaultConfigResponse(
        /* success= */ true, /* hasChanges= */ false, "", "", data, "", 30000);
  }

  @SuppressWarnings("UnusedVariable")
  private static TaskResponse parseTaskResponse(byte[] data) {
    // TODO: 使用 Protobuf 反序列化
    return new DefaultTaskResponse(/* success= */ true, Collections.emptyList(), "", 10000);
  }

  @SuppressWarnings("UnusedVariable")
  private static StatusResponse parseStatusResponse(byte[] data) {
    // TODO: 使用 Protobuf 反序列化
    return new DefaultStatusResponse(/* success= */ true, Collections.emptyList(), "", 60000);
  }

  @SuppressWarnings("UnusedVariable")
  private static ChunkedUploadResponse parseChunkedUploadResponse(byte[] data) {
    // TODO: 使用 Protobuf 反序列化
    return new DefaultChunkedUploadResponse(/* success= */ true, "", 0, "CHUNK_RECEIVED", "");
  }

  // ===== 函数式接口 =====

  @FunctionalInterface
  private interface ResponseParser<T> {
    T parse(byte[] data) throws Exception;
  }

  @FunctionalInterface
  private interface ErrorResponseFactory<T> {
    T create(String errorMessage);
  }

  // ===== 默认响应实现 =====

  /** 默认统一轮询响应实现 */
  private static final class DefaultUnifiedPollResponse implements UnifiedPollResponse {
    private final boolean success;
    private final boolean hasAnyChanges;
    private final Map<String, PollResult> results;
    private final String errorMessage;

    DefaultUnifiedPollResponse(
        boolean success,
        boolean hasAnyChanges,
        Map<String, PollResult> results,
        String errorMessage) {
      this.success = success;
      this.hasAnyChanges = hasAnyChanges;
      this.results = results;
      this.errorMessage = errorMessage;
    }

    static UnifiedPollResponse error(String errorMessage) {
      return new DefaultUnifiedPollResponse(
          /* success= */ false, /* hasAnyChanges= */ false, Collections.emptyMap(), errorMessage);
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    @Override
    public boolean hasAnyChanges() {
      return hasAnyChanges;
    }

    @Override
    public Map<String, PollResult> getResults() {
      return results;
    }

    @Override
    public String getErrorMessage() {
      return errorMessage;
    }
  }

  /** 默认轮询结果实现 */
  private static final class DefaultPollResult implements PollResult {
    private final String type;
    private final boolean hasChanges;
    @Nullable private final byte[] configData;
    @Nullable private final String configVersion;
    @Nullable private final String configEtag;
    @Nullable private final List<TaskInfo> tasks;

    DefaultPollResult(
        String type,
        boolean hasChanges,
        @Nullable byte[] configData,
        @Nullable String configVersion,
        @Nullable String configEtag,
        @Nullable List<TaskInfo> tasks) {
      this.type = type;
      this.hasChanges = hasChanges;
      this.configData = configData;
      this.configVersion = configVersion;
      this.configEtag = configEtag;
      this.tasks = tasks;
    }

    @Override
    public String getType() {
      return type;
    }

    @Override
    public boolean hasChanges() {
      return hasChanges;
    }

    @Override
    @Nullable
    public byte[] getConfigData() {
      return configData;
    }

    @Override
    @Nullable
    public String getConfigVersion() {
      return configVersion;
    }

    @Override
    @Nullable
    public String getConfigEtag() {
      return configEtag;
    }

    @Override
    @Nullable
    public List<TaskInfo> getTasks() {
      return tasks;
    }
  }

  private static final class DefaultConfigResponse implements ConfigResponse {
    private final boolean success;
    private final boolean hasChanges;
    private final String configVersion;
    private final String etag;
    private final byte[] configData;
    private final String errorMessage;
    private final long suggestedPollIntervalMillis;

    DefaultConfigResponse(
        boolean success,
        boolean hasChanges,
        String configVersion,
        String etag,
        byte[] configData,
        String errorMessage,
        long suggestedPollIntervalMillis) {
      this.success = success;
      this.hasChanges = hasChanges;
      this.configVersion = configVersion;
      this.etag = etag;
      this.configData = configData;
      this.errorMessage = errorMessage;
      this.suggestedPollIntervalMillis = suggestedPollIntervalMillis;
    }

    static ConfigResponse error(String errorMessage) {
      return new DefaultConfigResponse(
          /* success= */ false, /* hasChanges= */ false, "", "", new byte[0], errorMessage, 30000);
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    @Override
    public boolean hasChanges() {
      return hasChanges;
    }

    @Override
    public String getConfigVersion() {
      return configVersion;
    }

    @Override
    public String getEtag() {
      return etag;
    }

    @Override
    public byte[] getConfigData() {
      return configData;
    }

    @Override
    public String getErrorMessage() {
      return errorMessage;
    }

    @Override
    public long getSuggestedPollIntervalMillis() {
      return suggestedPollIntervalMillis;
    }
  }

  private static final class DefaultTaskResponse implements TaskResponse {
    private final boolean success;
    private final List<TaskInfo> tasks;
    private final String errorMessage;
    private final long suggestedPollIntervalMillis;

    DefaultTaskResponse(
        boolean success,
        List<TaskInfo> tasks,
        String errorMessage,
        long suggestedPollIntervalMillis) {
      this.success = success;
      this.tasks = tasks;
      this.errorMessage = errorMessage;
      this.suggestedPollIntervalMillis = suggestedPollIntervalMillis;
    }

    static TaskResponse error(String errorMessage) {
      return new DefaultTaskResponse(
          /* success= */ false, Collections.emptyList(), errorMessage, 10000);
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    @Override
    public List<TaskInfo> getTasks() {
      return tasks;
    }

    @Override
    public String getErrorMessage() {
      return errorMessage;
    }

    @Override
    public long getSuggestedPollIntervalMillis() {
      return suggestedPollIntervalMillis;
    }
  }

  private static final class DefaultStatusResponse implements StatusResponse {
    private final boolean success;
    private final List<String> acknowledgedTaskIds;
    private final String errorMessage;
    private final long suggestedReportIntervalMillis;

    DefaultStatusResponse(
        boolean success,
        List<String> acknowledgedTaskIds,
        String errorMessage,
        long suggestedReportIntervalMillis) {
      this.success = success;
      this.acknowledgedTaskIds = acknowledgedTaskIds;
      this.errorMessage = errorMessage;
      this.suggestedReportIntervalMillis = suggestedReportIntervalMillis;
    }

    static StatusResponse error(String errorMessage) {
      return new DefaultStatusResponse(
          /* success= */ false, Collections.emptyList(), errorMessage, 60000);
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    @Override
    public List<String> getAcknowledgedTaskIds() {
      return acknowledgedTaskIds;
    }

    @Override
    public String getErrorMessage() {
      return errorMessage;
    }

    @Override
    public long getSuggestedReportIntervalMillis() {
      return suggestedReportIntervalMillis;
    }
  }

  private static final class DefaultChunkedUploadResponse implements ChunkedUploadResponse {
    private final boolean success;
    private final String uploadId;
    private final int receivedChunkIndex;
    private final String status;
    private final String errorMessage;

    DefaultChunkedUploadResponse(
        boolean success,
        String uploadId,
        int receivedChunkIndex,
        String status,
        String errorMessage) {
      this.success = success;
      this.uploadId = uploadId;
      this.receivedChunkIndex = receivedChunkIndex;
      this.status = status;
      this.errorMessage = errorMessage;
    }

    static ChunkedUploadResponse error(String errorMessage) {
      return new DefaultChunkedUploadResponse(/* success= */ false, "", -1, "FAILED", errorMessage);
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    @Override
    public String getUploadId() {
      return uploadId;
    }

    @Override
    public int getReceivedChunkIndex() {
      return receivedChunkIndex;
    }

    @Override
    public String getStatus() {
      return status;
    }

    @Override
    public String getErrorMessage() {
      return errorMessage;
    }
  }
}
