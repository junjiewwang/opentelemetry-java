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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
  public CompletableFuture<TaskResultResponse> reportTaskResult(TaskResultRequest request) {
    checkNotClosed();

    CompletableFuture<TaskResultResponse> future = new CompletableFuture<>();

    byte[] requestBody = serializeTaskResultRequest(request);

    Request httpRequest =
        buildRequest(baseUrl + PATH_TASK_RESULT)
            .post(RequestBody.create(requestBody, JSON_TYPE))
            .build();

    logger.log(
        Level.FINE,
        "[TASK-RESULT] Reporting task result: taskId={0}, status={1}",
        new Object[] {request.getTaskId(), request.getStatus()});

    executeAsync(
        httpRequest,
        future,
        responseBody -> parseTaskResultResponse(responseBody),
        DefaultTaskResultResponse::error);

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

  private static byte[] serializeTaskResultRequest(TaskResultRequest request) {
    // 使用 JSON 序列化
    StringBuilder json = new StringBuilder();
    json.append("{");
    json.append(String.format(Locale.ROOT, "\"task_id\":\"%s\"", escapeJson(request.getTaskId())));
    json.append(String.format(Locale.ROOT, ",\"agent_id\":\"%s\"", escapeJson(request.getAgentId())));
    json.append(String.format(Locale.ROOT, ",\"status\":\"%s\"", request.getStatus().name()));
    
    if (request.getErrorCode() != null) {
      json.append(String.format(Locale.ROOT, ",\"error_code\":\"%s\"", escapeJson(request.getErrorCode())));
    }
    if (request.getErrorMessage() != null) {
      json.append(String.format(Locale.ROOT, ",\"error_message\":\"%s\"", escapeJson(request.getErrorMessage())));
    }
    if (request.getResultJson() != null) {
      // resultJson 已经是 JSON，不需要额外转义
      json.append(String.format(Locale.ROOT, ",\"result\":%s", request.getResultJson()));
    }
    
    json.append(String.format(Locale.ROOT, ",\"started_at_millis\":%d", request.getStartedAtMillis()));
    json.append(String.format(Locale.ROOT, ",\"completed_at_millis\":%d", request.getCompletedAtMillis()));
    json.append(String.format(Locale.ROOT, ",\"execution_time_millis\":%d", request.getExecutionTimeMillis()));
    json.append("}");
    
    return json.toString().getBytes(StandardCharsets.UTF_8);
  }

  /** JSON 字符串转义 */
  private static String escapeJson(String value) {
    if (value == null) {
      return "";
    }
    return value
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }

  // ===== 解析方法 =====

  private static UnifiedPollResponse parseUnifiedPollResponse(byte[] data) {
    String json = new String(data, StandardCharsets.UTF_8);
    
    // 增强日志：使用 INFO 级别输出响应摘要，便于诊断
    int jsonLen = json.length();
    String jsonPreview = jsonLen > 500 ? json.substring(0, 500) + "..." : json;
    logger.log(Level.INFO, "[POLL-PARSE] Received unified poll response: length={0}, preview={1}", 
        new Object[] {jsonLen, jsonPreview});
    
    // 解析顶层字段
    boolean hasAnyChanges = json.contains("\"has_any_changes\":true") 
        || json.contains("\"has_any_changes\": true");
    
    logger.log(Level.INFO, "[POLL-PARSE] Top-level: hasAnyChanges={0}, hasCONFIG={1}, hasTASK={2}",
        new Object[] {hasAnyChanges, json.contains("\"CONFIG\""), json.contains("\"TASK\"")});
    
    Map<String, PollResult> results = new HashMap<>();
    
    // 解析 CONFIG 结果
    if (json.contains("\"CONFIG\"")) {
      PollResult configResult = parseConfigResult(json);
      if (configResult != null) {
        results.put("CONFIG", configResult);
      }
    }
    
    // 解析 TASK 结果
    if (json.contains("\"TASK\"")) {
      PollResult taskResult = parseTaskResult(json);
      if (taskResult != null) {
        results.put("TASK", taskResult);
        int taskCount = taskResult.getTasks() != null ? taskResult.getTasks().size() : 0;
        logger.log(Level.INFO, "[POLL-PARSE] Parsed TASK result: hasChanges={0}, taskCount={1}",
            new Object[] {taskResult.hasChanges(), taskCount});
        
        // 输出每个任务的摘要信息
        if (taskResult.getTasks() != null) {
          for (TaskInfo task : taskResult.getTasks()) {
            logger.log(Level.INFO, 
                "[POLL-PARSE] Task in response: taskId={0}, type={1}, priority={2}, params={3}",
                new Object[] {
                  task.getTaskId(),
                  task.getTaskType(),
                  task.getPriority(),
                  task.getParametersJson()
                });
          }
        }
      } else {
        logger.log(Level.WARNING, "[POLL-PARSE] TASK block found but parseTaskResult returned null");
      }
    } else {
      logger.log(Level.INFO, "[POLL-PARSE] No TASK block in response");
    }
    
    return new DefaultUnifiedPollResponse(/* success= */ true, hasAnyChanges, results, "");
  }

  /**
   * 解析 CONFIG 类型的结果
   */
  @Nullable
  private static PollResult parseConfigResult(String json) {
    // 查找 CONFIG 块
    int configStart = json.indexOf("\"CONFIG\"");
    if (configStart < 0) {
      return null;
    }
    
    // 提取 CONFIG 对象（简化解析，假设结构正确）
    int blockStart = json.indexOf("{", configStart);
    int blockEnd = findMatchingBrace(json, blockStart);
    if (blockStart < 0 || blockEnd < 0) {
      return new DefaultPollResult("CONFIG", /* hasChanges= */ false, null, null, null, null);
    }
    
    String configBlock = json.substring(blockStart, blockEnd + 1);
    
    // 解析 has_changes
    boolean hasChanges = configBlock.contains("\"has_changes\":true") 
        || configBlock.contains("\"has_changes\": true");
    
    // 解析 config_version
    String configVersion = extractStringField(configBlock, "config_version");
    
    // 解析 config_etag
    String configEtag = extractStringField(configBlock, "config_etag");
    
    return new DefaultPollResult("CONFIG", hasChanges, null, configVersion, configEtag, null);
  }

  /**
   * 解析 TASK 类型的结果
   */
  @Nullable
  private static PollResult parseTaskResult(String json) {
    // 查找 TASK 块
    int taskStart = json.indexOf("\"TASK\"");
    if (taskStart < 0) {
      return null;
    }
    
    // 提取 TASK 对象
    int blockStart = json.indexOf("{", taskStart);
    int blockEnd = findMatchingBrace(json, blockStart);
    if (blockStart < 0 || blockEnd < 0) {
      return new DefaultPollResult("TASK", /* hasChanges= */ false, null, null, null, Collections.emptyList());
    }
    
    String taskBlock = json.substring(blockStart, blockEnd + 1);
    
    // 解析 has_changes
    boolean hasChanges = taskBlock.contains("\"has_changes\":true") 
        || taskBlock.contains("\"has_changes\": true");
    
    // 解析 tasks 数组
    List<TaskInfo> tasks = parseTasksArray(taskBlock);
    
    logger.log(Level.FINE, "[POLL-PARSE] TASK block: hasChanges={0}, tasks={1}",
        new Object[] {hasChanges, tasks.size()});
    
    return new DefaultPollResult("TASK", hasChanges, null, null, null, tasks);
  }

  /**
   * 解析 tasks 数组
   */
  private static List<TaskInfo> parseTasksArray(String taskBlock) {
    List<TaskInfo> tasks = new ArrayList<>();
    
    // 查找 tasks 数组
    int tasksStart = taskBlock.indexOf("\"tasks\"");
    if (tasksStart < 0) {
      return tasks;
    }
    
    // 找到数组开始位置
    int arrayStart = taskBlock.indexOf("[", tasksStart);
    int arrayEnd = findMatchingBracket(taskBlock, arrayStart);
    if (arrayStart < 0 || arrayEnd < 0) {
      return tasks;
    }
    
    String tasksArray = taskBlock.substring(arrayStart + 1, arrayEnd);
    
    // 解析数组中的每个任务对象
    int pos = 0;
    while (pos < tasksArray.length()) {
      int objStart = tasksArray.indexOf("{", pos);
      if (objStart < 0) {
        break;
      }
      
      int objEnd = findMatchingBrace(tasksArray, objStart);
      if (objEnd < 0) {
        break;
      }
      
      String taskObj = tasksArray.substring(objStart, objEnd + 1);
      TaskInfo taskInfo = parseTaskInfo(taskObj);
      if (taskInfo != null) {
        tasks.add(taskInfo);
      }
      
      pos = objEnd + 1;
    }
    
    return tasks;
  }

  /**
   * 解析单个任务对象
   */
  @Nullable
  private static TaskInfo parseTaskInfo(String taskObj) {
    String taskId = extractStringField(taskObj, "task_id");
    if (taskId == null || taskId.isEmpty()) {
      return null;
    }
    
    String taskTypeRaw = extractStringField(taskObj, "task_type");
    String taskType = taskTypeRaw != null ? taskTypeRaw : "UNKNOWN";
    String parametersJsonRaw = extractObjectField(taskObj, "parameters");
    String parametersJson = parametersJsonRaw != null ? parametersJsonRaw : "{}";
    int priority = extractIntField(taskObj, "priority", 0);
    long timeoutMillis = extractLongField(taskObj, "timeout_millis", 60000);
    long createdAtMillis = extractLongField(taskObj, "created_at_millis", 0);
    long expiresAtMillis = extractLongField(taskObj, "expires_at_millis", 0);
    long maxAcceptableDelayMillis = extractLongField(taskObj, "max_acceptable_delay_millis", 0);
    
    logger.log(Level.FINE, "[POLL-PARSE] Parsed task: id={0}, type={1}, priority={2}",
        new Object[] {taskId, taskType, priority});
    
    return new DefaultTaskInfo(
        taskId, taskType, parametersJson, priority, timeoutMillis, 
        createdAtMillis, expiresAtMillis, maxAcceptableDelayMillis);
  }

  // ===== JSON 解析辅助方法 =====

  /**
   * 提取字符串字段值
   */
  @Nullable
  private static String extractStringField(String json, String fieldName) {
    Pattern pattern = Pattern.compile("\"" + fieldName + "\"\\s*:\\s*\"([^\"]*)\"");
    Matcher matcher = pattern.matcher(json);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }

  /**
   * 提取嵌套对象字段（返回 JSON 字符串）
   */
  @Nullable
  private static String extractObjectField(String json, String fieldName) {
    int fieldStart = json.indexOf("\"" + fieldName + "\"");
    if (fieldStart < 0) {
      return null;
    }
    
    int objStart = json.indexOf("{", fieldStart);
    if (objStart < 0) {
      return null;
    }
    
    int objEnd = findMatchingBrace(json, objStart);
    if (objEnd < 0) {
      return null;
    }
    
    return json.substring(objStart, objEnd + 1);
  }

  /**
   * 提取整数字段值
   */
  private static int extractIntField(String json, String fieldName, int defaultValue) {
    Pattern pattern = Pattern.compile("\"" + fieldName + "\"\\s*:\\s*(-?\\d+)");
    Matcher matcher = pattern.matcher(json);
    if (matcher.find()) {
      try {
        return Integer.parseInt(matcher.group(1));
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  /**
   * 提取长整数字段值
   */
  private static long extractLongField(String json, String fieldName, long defaultValue) {
    Pattern pattern = Pattern.compile("\"" + fieldName + "\"\\s*:\\s*(-?\\d+)");
    Matcher matcher = pattern.matcher(json);
    if (matcher.find()) {
      try {
        return Long.parseLong(matcher.group(1));
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  /**
   * 查找匹配的右花括号
   */
  private static int findMatchingBrace(String json, int start) {
    if (start < 0 || start >= json.length() || json.charAt(start) != '{') {
      return -1;
    }
    
    int depth = 0;
    boolean inString = false;
    
    for (int i = start; i < json.length(); i++) {
      char c = json.charAt(i);
      
      if (c == '"' && (i == 0 || json.charAt(i - 1) != '\\')) {
        inString = !inString;
      } else if (!inString) {
        if (c == '{') {
          depth++;
        } else if (c == '}') {
          depth--;
          if (depth == 0) {
            return i;
          }
        }
      }
    }
    
    return -1;
  }

  /**
   * 查找匹配的右方括号
   */
  private static int findMatchingBracket(String json, int start) {
    if (start < 0 || start >= json.length() || json.charAt(start) != '[') {
      return -1;
    }
    
    int depth = 0;
    boolean inString = false;
    
    for (int i = start; i < json.length(); i++) {
      char c = json.charAt(i);
      
      if (c == '"' && (i == 0 || json.charAt(i - 1) != '\\')) {
        inString = !inString;
      } else if (!inString) {
        if (c == '[') {
          depth++;
        } else if (c == ']') {
          depth--;
          if (depth == 0) {
            return i;
          }
        }
      }
    }
    
    return -1;
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

  private static TaskResultResponse parseTaskResultResponse(byte[] data) {
    String json = new String(data, StandardCharsets.UTF_8);
    
    // 解析响应
    boolean success = json.contains("\"success\":true") || json.contains("\"success\": true");
    String errorMessage = extractStringField(json, "error_message");
    if (errorMessage == null) {
      errorMessage = extractStringField(json, "message");
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

  /** 默认任务信息实现 */
  private static final class DefaultTaskInfo implements TaskInfo {
    private final String taskId;
    private final String taskType;
    private final String parametersJson;
    private final int priority;
    private final long timeoutMillis;
    private final long createdAtMillis;
    private final long expiresAtMillis;
    private final long maxAcceptableDelayMillis;

    DefaultTaskInfo(
        String taskId,
        String taskType,
        @Nullable String parametersJson,
        int priority,
        long timeoutMillis,
        long createdAtMillis,
        long expiresAtMillis,
        long maxAcceptableDelayMillis) {
      this.taskId = taskId != null ? taskId : "";
      this.taskType = taskType != null ? taskType : "UNKNOWN";
      this.parametersJson = parametersJson != null ? parametersJson : "{}";
      this.priority = priority;
      this.timeoutMillis = timeoutMillis;
      this.createdAtMillis = createdAtMillis;
      this.expiresAtMillis = expiresAtMillis;
      this.maxAcceptableDelayMillis = maxAcceptableDelayMillis;
    }

    @Override
    public String getTaskId() {
      return taskId;
    }

    @Override
    public String getTaskType() {
      return taskType;
    }

    @Override
    public String getParametersJson() {
      return parametersJson;
    }

    @Override
    public int getPriority() {
      return priority;
    }

    @Override
    public long getTimeoutMillis() {
      return timeoutMillis;
    }

    @Override
    public long getCreatedAtMillis() {
      return createdAtMillis;
    }

    @Override
    public long getExpiresAtMillis() {
      return expiresAtMillis;
    }

    @Override
    public long getMaxAcceptableDelayMillis() {
      return maxAcceptableDelayMillis;
    }

    @Override
    public String toString() {
      return String.format(
          Locale.ROOT,
          "TaskInfo{id=%s, type=%s, priority=%d, timeout=%dms, maxDelay=%dms}",
          taskId, taskType, priority, timeoutMillis, maxAcceptableDelayMillis);
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

  /** 默认任务结果响应实现 */
  private static final class DefaultTaskResultResponse implements TaskResultResponse {
    private final boolean success;
    private final String errorMessage;

    DefaultTaskResultResponse(boolean success, String errorMessage) {
      this.success = success;
      this.errorMessage = errorMessage;
    }

    static TaskResultResponse error(String errorMessage) {
      return new DefaultTaskResultResponse(/* success= */ false, errorMessage);
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    @Override
    public String getErrorMessage() {
      return errorMessage;
    }
  }
}
