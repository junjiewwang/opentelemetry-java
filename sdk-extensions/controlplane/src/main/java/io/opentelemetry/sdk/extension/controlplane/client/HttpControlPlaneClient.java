/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client;

import io.opentelemetry.sdk.extension.controlplane.client.dto.ConfigPollRequestDto;
import io.opentelemetry.sdk.extension.controlplane.client.dto.PollResultDto;
import io.opentelemetry.sdk.extension.controlplane.client.dto.TaskInfoDto;
import io.opentelemetry.sdk.extension.controlplane.client.dto.TaskPollRequestDto;
import io.opentelemetry.sdk.extension.controlplane.client.dto.TaskResultRequestDto;
import io.opentelemetry.sdk.extension.controlplane.client.dto.UnifiedPollRequestDto;
import io.opentelemetry.sdk.extension.controlplane.client.dto.UnifiedPollResponseDto;
import io.opentelemetry.sdk.extension.controlplane.client.response.DefaultChunkedUploadResponse;
import io.opentelemetry.sdk.extension.controlplane.client.response.DefaultConfigResponse;
import io.opentelemetry.sdk.extension.controlplane.client.response.DefaultPollResult;
import io.opentelemetry.sdk.extension.controlplane.client.response.DefaultStatusResponse;
import io.opentelemetry.sdk.extension.controlplane.client.response.DefaultTaskInfo;
import io.opentelemetry.sdk.extension.controlplane.client.response.DefaultTaskResponse;
import io.opentelemetry.sdk.extension.controlplane.client.response.DefaultTaskResultResponse;
import io.opentelemetry.sdk.extension.controlplane.client.response.DefaultUnifiedPollResponse;
import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpHealthMonitor;
import io.opentelemetry.sdk.extension.controlplane.util.JsonUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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

  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration WRITE_TIMEOUT = Duration.ofSeconds(30);
  private static final long READ_TIMEOUT_BUFFER_SECONDS = 10;

  private static final long QUICK_RESPONSE_THRESHOLD_MS = 5_000;
  private static final long MIN_EXPECTED_TIMEOUT_MS = 30_000;

  // API 路径常量（相对于 baseUrl，baseUrl 应为 http://host:port/v1/control）
  private static final String PATH_UNIFIED_POLL = "/poll";
  private static final String PATH_CONFIG_POLL = "/poll/config";
  private static final String PATH_TASKS_POLL = "/poll/tasks";
  private static final String PATH_STATUS = "/status";
  private static final String PATH_UPLOAD_CHUNK = "/upload-chunk";
  private static final String PATH_TASK_RESULT = "/tasks/result";

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
            .connectTimeout(CONNECT_TIMEOUT)
            .readTimeout(longPollTimeout.plusSeconds(READ_TIMEOUT_BUFFER_SECONDS)) // 比长轮询超时多一点
            .writeTimeout(WRITE_TIMEOUT)
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

    // 使用 DTO 构建请求体（类型安全，字段名与服务端 Go 结构体匹配）
    UnifiedPollRequestDto requestDto = UnifiedPollRequestDto.from(request);
    byte[] requestBody = JsonUtils.toBytes(requestDto);

    // 调试日志：记录实际发送的请求体，确认 timeout_millis 是否正确
    if (logger.isLoggable(Level.INFO)) {
      String requestJson = new String(requestBody, StandardCharsets.UTF_8);
      logger.log(Level.INFO,
          "[POLL-REQUEST] Sending long poll request: url={0}, timeout_millis={1}, requestBody={2}",
          new Object[] {baseUrl + PATH_UNIFIED_POLL, request.getTimeoutMillis(), requestJson});
    }

    // 记录请求开始时间（用于计算实际等待时间）
    long pollStartTime = System.currentTimeMillis();

    Request httpRequest =
        buildRequest(baseUrl + PATH_UNIFIED_POLL)
            .post(RequestBody.create(requestBody, JSON_TYPE))
            .build();

    // 包装 future，在响应返回时记录实际等待时间
    CompletableFuture<UnifiedPollResponse> wrappedFuture = new CompletableFuture<>();

    executeAsync(
        httpRequest,
        wrappedFuture,
        HttpControlPlaneClient::parseUnifiedPollResponse,
        DefaultUnifiedPollResponse::error);

    // 当收到响应时，记录实际等待时间
    @SuppressWarnings("FutureReturnValueIgnored")
    Object unused = wrappedFuture.whenComplete((response, error) -> {
      long duration = System.currentTimeMillis() - pollStartTime;
      if (logger.isLoggable(Level.INFO)) {
        if (error != null) {
          logger.log(Level.INFO,
              "[POLL-TIMING] Long poll failed after {0}ms, expectedTimeout={1}ms, error={2}",
              new Object[] {duration, request.getTimeoutMillis(), error.getMessage()});
        } else {
          boolean success = response != null && response.isSuccess();
          boolean hasChanges = response != null && response.hasAnyChanges();
          logger.log(Level.INFO,
              "[POLL-TIMING] Long poll completed in {0}ms, expectedTimeout={1}ms, success={2}, hasChanges={3}",
              new Object[] {duration, request.getTimeoutMillis(), success, hasChanges});

          // 如果实际等待时间远小于预期超时时间，输出警告
          if (duration < QUICK_RESPONSE_THRESHOLD_MS && request.getTimeoutMillis() >= MIN_EXPECTED_TIMEOUT_MS) {
            logger.log(Level.WARNING,
                "[POLL-TIMING-WARN] Server responded too quickly! duration={0}ms, expectedTimeout={1}ms. " +
                "This may indicate server is not honoring timeout_millis parameter.",
                new Object[] {duration, request.getTimeoutMillis()});
          }
        }
      }
      // 将结果传递给原始 future
      if (error != null) {
        future.completeExceptionally(error);
      } else {
        future.complete(response);
      }
    });

    return future;
  }

  @Override
  public CompletableFuture<ConfigResponse> getConfig(ConfigRequest request) {
    checkNotClosed();
    checkOtlpHealth();

    // 使用 DTO 构建请求体（类型安全，字段名与服务端 Go 结构体匹配）
    ConfigPollRequestDto requestDto = ConfigPollRequestDto.from(request);

    return executePost(
        PATH_CONFIG_POLL,
        requestDto,
        JSON_TYPE,
        /* checkOtlpHealth= */ false,
        HttpControlPlaneClient::parseConfigResponse,
        DefaultConfigResponse::error);
  }

  @Override
  public CompletableFuture<TaskResponse> getTasks(TaskRequest request) {
    checkNotClosed();
    checkOtlpHealth();

    // 使用 DTO 构建请求体（类型安全，字段名与服务端 Go 结构体匹配）
    TaskPollRequestDto requestDto = TaskPollRequestDto.from(request);

    return executePost(
        PATH_TASKS_POLL,
        requestDto,
        JSON_TYPE,
        /* checkOtlpHealth= */ false,
        HttpControlPlaneClient::parseTaskResponse,
        DefaultTaskResponse::error);
  }

  @Override
  public CompletableFuture<StatusResponse> reportStatus(StatusRequest request) {
    checkNotClosed();
    // 状态上报不依赖 OTLP 健康状态，始终尝试发送

    return executePostBytes(
        PATH_STATUS,
        request.getStatusData(),
        PROTOBUF_TYPE,
        HttpControlPlaneClient::parseStatusResponse,
        DefaultStatusResponse::error);
  }

  @Override
  public CompletableFuture<ChunkedUploadResponse> uploadChunkedResult(ChunkedTaskResult chunk) {
    checkNotClosed();

    return executePostBytes(
        PATH_UPLOAD_CHUNK,
        chunk.getChunkData(),
        PROTOBUF_TYPE,
        HttpControlPlaneClient::parseChunkedUploadResponse,
        DefaultChunkedUploadResponse::error);
  }

  @Override
  public CompletableFuture<TaskResultResponse> reportTaskResult(TaskResultRequest request) {
    checkNotClosed();

    // 使用 DTO 构建请求体（类型安全，字段名与服务端 Go 结构体匹配）
    TaskResultRequestDto requestDto = TaskResultRequestDto.from(request);

    logger.log(
        Level.FINE,
        "[TASK-RESULT] Reporting task result: taskId={0}, status={1}",
        new Object[] {request.getTaskId(), request.getStatus()});

    return executePost(
        PATH_TASK_RESULT,
        requestDto,
        JSON_TYPE,
        /* checkOtlpHealth= */ false,
        HttpControlPlaneClient::parseTaskResultResponse,
        DefaultTaskResultResponse::error);
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

  private <ReqT, RespT> CompletableFuture<RespT> executePost(
      String path,
      ReqT requestDto,
      MediaType mediaType,
      boolean checkOtlpHealth,
      ResponseParser<RespT> parser,
      ErrorResponseFactory<RespT> errorFactory) {

    if (checkOtlpHealth) {
      checkOtlpHealth();
    }

    CompletableFuture<RespT> future = new CompletableFuture<>();
    byte[] requestBody = JsonUtils.toBytes(requestDto);

    Request httpRequest =
        buildRequest(baseUrl + path)
            .post(RequestBody.create(requestBody, mediaType))
            .build();

    executeAsync(httpRequest, future, parser, errorFactory);
    return future;
  }

  private <RespT> CompletableFuture<RespT> executePostBytes(
      String path,
      byte[] requestBody,
      MediaType mediaType,
      ResponseParser<RespT> parser,
      ErrorResponseFactory<RespT> errorFactory) {

    CompletableFuture<RespT> future = new CompletableFuture<>();
    Request httpRequest =
        buildRequest(baseUrl + path)
            .post(RequestBody.create(requestBody, mediaType))
            .build();

    executeAsync(httpRequest, future, parser, errorFactory);
    return future;
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

  // ===== 解析方法 =====

  /**
   * 解析统一轮询响应
   *
   * <p>使用 Jackson DTO 进行反序列化，简化解析逻辑并提升可维护性。
   */
  private static UnifiedPollResponse parseUnifiedPollResponse(byte[] data) {
    // 使用 Jackson 直接反序列化为 DTO
    UnifiedPollResponseDto dto = JsonUtils.parseObjectSafe(data, UnifiedPollResponseDto.class);
    if (dto == null) {
      logger.log(Level.WARNING, "[POLL-PARSE] Failed to parse unified poll response");
      return DefaultUnifiedPollResponse.error("JSON parse failed");
    }

    // 调试级别日志：输出响应摘要
    if (logger.isLoggable(Level.FINE)) {
      logger.log(Level.FINE, "[POLL-PARSE] Received response: hasAnyChanges={0}, hasCONFIG={1}, hasTASK={2}",
          new Object[] {dto.isHasAnyChanges(), dto.hasConfigResult(), dto.hasTaskResult()});
    }

    Map<String, PollResult> results = new HashMap<>();

    // 转换 CONFIG 结果
    if (dto.hasConfigResult()) {
      PollResultDto configDto = dto.getConfigResult();
      // 关键调试日志：输出从服务端收到的 CONFIG 详细信息
      logger.log(Level.INFO,
          "[POLL-PARSE] CONFIG result from server: hasChanges={0}, version={1}, etag={2}, type={3}, message={4}",
          new Object[] {
            configDto != null ? configDto.isHasChanges() : "null",
            configDto != null ? configDto.getConfigVersion() : "null",
            configDto != null ? configDto.getConfigEtag() : "null",
            configDto != null ? configDto.getType() : "null",
            configDto != null ? configDto.getMessage() : "null"
          });
      
      PollResult configResult = convertToPollResult(configDto, "CONFIG");
      if (configResult != null) {
        results.put("CONFIG", configResult);
      }
    } else {
      logger.log(Level.INFO, "[POLL-PARSE] No CONFIG result in server response");
    }

    // 转换 TASK 结果
    if (dto.hasTaskResult()) {
      PollResult taskResult = convertToPollResult(dto.getTaskResult(), "TASK");
      if (taskResult != null) {
        results.put("TASK", taskResult);
        logTaskResultSummary(taskResult);
      }
    } else {
      logger.log(Level.FINE, "[POLL-PARSE] No TASK block in response");
    }

    return new DefaultUnifiedPollResponse(/* success= */ true, dto.isHasAnyChanges(), results, "");
  }

  /**
   * 将 PollResultDto 转换为 PollResult
   */
  @Nullable
  private static PollResult convertToPollResult(@Nullable PollResultDto dto, String type) {
    if (dto == null) {
      return null;
    }

    if ("CONFIG".equals(type)) {
      return DefaultPollResult.config(
          dto.isHasChanges(),
          dto.getConfigData(),
          dto.getConfigVersion(),
          dto.getConfigEtag());
    } else if ("TASK".equals(type)) {
      List<TaskInfo> tasks = convertToTaskInfoList(dto.getTasks());
      return DefaultPollResult.task(dto.isHasChanges(), tasks);
    }

    return null;
  }

  /**
   * 将 TaskInfoDto 列表转换为 TaskInfo 列表
   */
  private static List<TaskInfo> convertToTaskInfoList(List<TaskInfoDto> dtos) {
    if (dtos == null || dtos.isEmpty()) {
      return Collections.emptyList();
    }

    List<TaskInfo> tasks = new ArrayList<>(dtos.size());
    for (TaskInfoDto dto : dtos) {
      if (dto.isValid()) {
        // isValid() 确保 taskId 不为空
        String taskId = dto.getTaskId();
        if (taskId == null) {
          continue; // 理论上不会发生，但满足 NullAway 检查
        }
        TaskInfo taskInfo = new DefaultTaskInfo(
            taskId,
            dto.getTaskType() != null ? dto.getTaskType() : "UNKNOWN",
            dto.getParametersJson(),
            dto.getPriority(),
            dto.getTimeoutMillis(),
            dto.getCreatedAtMillis(),
            dto.getExpiresAtMillis(),
            dto.getMaxAcceptableDelayMillis());
        tasks.add(taskInfo);

        // FINEST 级别日志：单个任务解析详情
        if (logger.isLoggable(Level.FINEST)) {
          logger.log(Level.FINEST, "[POLL-PARSE] Parsed task: id={0}, type={1}, priority={2}",
              new Object[] {taskId, dto.getTaskType(), dto.getPriority()});
        }
      }
    }
    // 返回不可变列表，避免 MixedMutabilityReturnType 警告
    return Collections.unmodifiableList(tasks);
  }

  /**
   * 输出任务结果摘要日志
   */
  private static void logTaskResultSummary(PollResult taskResult) {
    int taskCount = taskResult.getTasks() != null ? taskResult.getTasks().size() : 0;
    logger.log(Level.FINE, "[POLL-PARSE] Parsed TASK result: hasChanges={0}, taskCount={1}",
        new Object[] {taskResult.hasChanges(), taskCount});

    // FINEST 级别：输出每个任务的摘要信息
    if (logger.isLoggable(Level.FINEST) && taskResult.getTasks() != null) {
      for (TaskInfo task : taskResult.getTasks()) {
        logger.log(Level.FINEST,
            "[POLL-PARSE] Task: taskId={0}, type={1}, priority={2}",
            new Object[] {task.getTaskId(), task.getTaskType(), task.getPriority()});
      }
    }
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
    return new DefaultTaskResponse(
        /* success= */ true, Collections.emptyList(), "", 10000);
  }

  @SuppressWarnings("UnusedVariable")
  private static StatusResponse parseStatusResponse(byte[] data) {
    // TODO: 使用 Protobuf 反序列化
    return new DefaultStatusResponse(
        /* success= */ true, Collections.emptyList(), "", 60000);
  }

  @SuppressWarnings("UnusedVariable")
  private static ChunkedUploadResponse parseChunkedUploadResponse(byte[] data) {
    // TODO: 使用 Protobuf 反序列化
    return new DefaultChunkedUploadResponse(
        /* success= */ true, "", 0, "CHUNK_RECEIVED", "");
  }

  private static TaskResultResponse parseTaskResultResponse(byte[] data) {
    String json = new String(data, StandardCharsets.UTF_8);
    
    // 使用 JsonUtils 解析响应
    boolean success = JsonUtils.extractBoolean(json, "success", false);
    String errorMessage = JsonUtils.extractString(json, "error_message");
    if (errorMessage == null) {
      errorMessage = JsonUtils.extractString(json, "message");
    }
    
    return new DefaultTaskResultResponse(success, errorMessage != null ? errorMessage : "");
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
}
