/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas.tunnel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasConfig;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasSession;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasSessionManager;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasSessionManager.SessionCreateRequest;
import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasSessionManager.SessionCreateResult;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.ArthasStartMessage;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.ArthasStatusMessage;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.ArthasStatusPayload;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.ArthasStopMessage;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.ErrorMessage;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.ErrorPayload;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.MessageType;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.PingMessage;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.PongMessage;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.RegisterMessage;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.RegisterPayload;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.TerminalCloseMessage;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.TerminalClosedMessage;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.TerminalClosedPayload;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.TerminalInputMessage;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.TerminalOpenMessage;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.TerminalReadyMessage;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.TerminalReadyPayload;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.TerminalRejectedMessage;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.TerminalRejectedPayload;
import io.opentelemetry.sdk.extension.controlplane.arthas.tunnel.TunnelMessage.TerminalResizeMessage;
import io.opentelemetry.sdk.extension.controlplane.identity.AgentIdentityProvider;
import io.opentelemetry.sdk.extension.controlplane.task.TaskExecutionLogger;
import io.opentelemetry.sdk.extension.controlplane.task.TaskExecutionLogger.TaskContext;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

/**
 * Arthas Tunnel WebSocket 客户端
 *
 * <p>负责与 Tunnel Server 建立 WebSocket 连接，处理消息交互。
 * 使用 OkHttp WebSocket 实现，兼容 Java 8+。
 */
public final class ArthasTunnelClient implements Closeable {

  private static final Logger logger = Logger.getLogger(ArthasTunnelClient.class.getName());

  private static final ObjectMapper objectMapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  /** 连接状态 */
  public enum ConnectionState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    RECONNECTING,
    CLOSED
  }

  private final ArthasConfig config;
  private final TunnelEventListener eventListener;
  private final AgentIdentityProvider.AgentIdentity agentIdentity;
  private final OkHttpClient httpClient;

  private final AtomicReference<ConnectionState> connectionState =
      new AtomicReference<>(ConnectionState.DISCONNECTED);
  private final AtomicReference<WebSocket> webSocketRef = new AtomicReference<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicInteger reconnectAttempts = new AtomicInteger(0);

  @Nullable private ScheduledExecutorService scheduler;
  @Nullable private ScheduledFuture<?> pingTask;
  @Nullable private ScheduledFuture<?> reconnectTask;
  @Nullable private ArthasSessionManager sessionManager;

  /** 任务执行日志记录器 */
  private final TaskExecutionLogger taskLogger = TaskExecutionLogger.getInstance();

  // 消息处理器（用于处理二进制帧输出，预留给未来扩展使用）
  @SuppressWarnings("UnusedVariable")
  @Nullable private BinaryOutputHandler binaryOutputHandler;

  /**
   * 创建 Tunnel 客户端
   *
   * @param config Arthas 配置
   * @param eventListener 事件监听器
   */
  public ArthasTunnelClient(ArthasConfig config, TunnelEventListener eventListener) {
    this.config = config;
    this.eventListener = eventListener;
    this.agentIdentity = AgentIdentityProvider.get();
    
    // 创建 OkHttpClient
    this.httpClient = new OkHttpClient.Builder()
        .connectTimeout(config.getTunnelConnectTimeout())
        .readTimeout(config.getTunnelConnectTimeout())
        .writeTimeout(config.getTunnelConnectTimeout())
        .pingInterval(config.getTunnelPingInterval())
        .build();
  }

  /**
   * 设置会话管理器
   *
   * @param sessionManager 会话管理器
   */
  public void setSessionManager(ArthasSessionManager sessionManager) {
    this.sessionManager = sessionManager;
  }

  /**
   * 设置二进制输出处理器
   *
   * @param handler 处理器
   */
  public void setBinaryOutputHandler(BinaryOutputHandler handler) {
    this.binaryOutputHandler = handler;
  }

  /**
   * 启动客户端
   *
   * @param scheduler 调度器
   */
  public void start(ScheduledExecutorService scheduler) {
    if (!config.isEnabled() || !config.hasTunnelEndpoint()) {
      logger.log(Level.INFO, "Arthas tunnel client disabled or no endpoint configured");
      return;
    }

    this.scheduler = scheduler;
    connect();
  }

  /** 连接到 Tunnel Server */
  private void connect() {
    if (closed.get()) {
      return;
    }

    String endpoint = config.getTunnelEndpoint();
    if (endpoint == null || endpoint.isEmpty()) {
      logger.log(Level.WARNING, "Tunnel endpoint not configured");
      return;
    }

    connectionState.set(ConnectionState.CONNECTING);
    logger.log(Level.INFO, "Connecting to tunnel server: {0}", endpoint);

    try {
      Request request = new Request.Builder()
          .url(endpoint)
          .build();

      WebSocket ws = httpClient.newWebSocket(request, new TunnelWebSocketListener());
      webSocketRef.set(ws);
      
    } catch (RuntimeException e) {
      logger.log(Level.SEVERE, "Error creating WebSocket connection", e);
      connectionState.set(ConnectionState.DISCONNECTED);
      scheduleReconnect();
    }
  }

  /** 发送注册消息 */
  private void sendRegisterMessage() {
    RegisterPayload payload =
        new RegisterPayload(
            agentIdentity.getAgentId(),
            agentIdentity.getServiceName(),
            agentIdentity.getSdkVersion(),
            agentIdentity.getHostName(),
            agentIdentity.getIp());

    RegisterMessage message = RegisterMessage.create(payload);
    sendTextMessage(message);
  }

  /** 发送文本消息 */
  private void sendTextMessage(TunnelMessage message) {
    WebSocket ws = webSocketRef.get();
    if (ws == null || connectionState.get() != ConnectionState.CONNECTED) {
      logger.log(Level.WARNING, "Cannot send message, not connected");
      return;
    }

    try {
      String json = objectMapper.writeValueAsString(message);
      ws.send(json);
      logger.log(Level.FINE, "Sent message: {0}", message.getType());
    } catch (JsonProcessingException e) {
      logger.log(Level.WARNING, "Failed to serialize message", e);
    }
  }

  /**
   * 发送二进制帧（终端输出）
   *
   * @param sessionId 会话 ID
   * @param data 输出数据
   */
  public void sendBinaryOutput(String sessionId, byte[] data) {
    WebSocket ws = webSocketRef.get();
    if (ws == null || connectionState.get() != ConnectionState.CONNECTED) {
      return;
    }

    // 构建带 sessionId 前缀的二进制帧
    // 格式: [sessionId 长度 (1 byte)] [sessionId (N bytes)] [数据]
    byte[] sessionIdBytes = sessionId.getBytes(StandardCharsets.UTF_8);
    byte[] buffer = new byte[1 + sessionIdBytes.length + data.length];
    buffer[0] = (byte) sessionIdBytes.length;
    System.arraycopy(sessionIdBytes, 0, buffer, 1, sessionIdBytes.length);
    System.arraycopy(data, 0, buffer, 1 + sessionIdBytes.length, data.length);

    ws.send(ByteString.of(buffer));
  }

  /** 启动心跳任务 */
  private void startPingTask() {
    if (scheduler == null) {
      return;
    }

    stopPingTask();

    long intervalMillis = config.getTunnelPingInterval().toMillis();
    pingTask =
        scheduler.scheduleWithFixedDelay(
            () -> {
              if (connectionState.get() == ConnectionState.CONNECTED) {
                sendTextMessage(new PingMessage(TunnelMessage.generateId(), TunnelMessage.currentTimestamp()));
              }
            },
            intervalMillis,
            intervalMillis,
            TimeUnit.MILLISECONDS);
  }

  /** 停止心跳任务 */
  private void stopPingTask() {
    if (pingTask != null) {
      pingTask.cancel(false);
      pingTask = null;
    }
  }

  /** 安排重连 */
  private void scheduleReconnect() {
    if (closed.get() || scheduler == null) {
      return;
    }

    int maxAttempts = config.getTunnelMaxReconnectAttempts();
    int attempts = reconnectAttempts.incrementAndGet();

    if (maxAttempts > 0 && attempts > maxAttempts) {
      logger.log(Level.WARNING, "Max reconnect attempts ({0}) reached, giving up", maxAttempts);
      connectionState.set(ConnectionState.CLOSED);
      eventListener.onMaxReconnectReached();
      return;
    }

    long delayMillis = config.getTunnelReconnectInterval().toMillis();
    connectionState.set(ConnectionState.RECONNECTING);

    logger.log(
        Level.INFO,
        "Scheduling reconnect in {0}ms (attempt {1})",
        new Object[] {delayMillis, attempts});

    reconnectTask =
        scheduler.schedule(
            this::connect,
            delayMillis,
            TimeUnit.MILLISECONDS);
  }

  /** 处理收到的文本消息 */
  private void handleTextMessage(String text) {
    try {
      // 首先解析基础消息获取类型
      TunnelMessage baseMessage = objectMapper.readValue(text, TunnelMessage.class);
      MessageType type = baseMessage.getType();

      logger.log(Level.FINE, "Received message type: {0}", type);

      switch (type) {
        case PING:
          handlePing(objectMapper.readValue(text, PingMessage.class));
          break;
        case ARTHAS_START:
          handleArthasStart(objectMapper.readValue(text, ArthasStartMessage.class));
          break;
        case ARTHAS_STOP:
          handleArthasStop(objectMapper.readValue(text, ArthasStopMessage.class));
          break;
        case TERMINAL_OPEN:
          handleTerminalOpen(objectMapper.readValue(text, TerminalOpenMessage.class));
          break;
        case TERMINAL_INPUT:
          handleTerminalInput(objectMapper.readValue(text, TerminalInputMessage.class));
          break;
        case TERMINAL_RESIZE:
          handleTerminalResize(objectMapper.readValue(text, TerminalResizeMessage.class));
          break;
        case TERMINAL_CLOSE:
          handleTerminalClose(objectMapper.readValue(text, TerminalCloseMessage.class));
          break;
        default:
          logger.log(Level.FINE, "Unhandled message type: {0}", type);
      }
    } catch (JsonProcessingException e) {
      logger.log(Level.WARNING, "Failed to parse message: {0}", e.getMessage());
    }
  }

  /** 处理 Ping 消息 */
  @SuppressWarnings("UnusedVariable")
  private void handlePing(PingMessage message) {
    sendTextMessage(PongMessage.create());
  }

  /** 处理 Arthas 启动请求 */
  @SuppressWarnings("UnusedVariable")
  private void handleArthasStart(ArthasStartMessage message) {
    String taskId = "arthas-start-" + System.currentTimeMillis();

    // 记录任务接收
    TaskContext context = taskLogger.logTaskReceived(
        taskId,
        TaskExecutionLogger.TASK_TYPE_ARTHAS_START,
        TaskExecutionLogger.SOURCE_TUNNEL_SERVER);

    taskLogger.logTaskStarted(taskId, "tunnel_client");

    logger.log(Level.INFO, "Received ARTHAS_START request");
    eventListener.onArthasStartRequested();
    // 发送状态响应
    sendArthasStatus();

    // 记录任务完成
    taskLogger.logTaskCompleted(taskId, "arthas_start_requested");
  }

  /** 处理 Arthas 停止请求 */
  private void handleArthasStop(ArthasStopMessage message) {
    String taskId = "arthas-stop-" + System.currentTimeMillis();
    String reason = message.getPayload() != null ? message.getPayload().getReason() : null;

    // 记录任务接收
    taskLogger.logTaskReceived(
        taskId,
        TaskExecutionLogger.TASK_TYPE_ARTHAS_STOP,
        TaskExecutionLogger.SOURCE_TUNNEL_SERVER,
        TaskExecutionLogger.details()
            .put("reason", reason)
            .build());

    taskLogger.logTaskStarted(taskId, "tunnel_client");

    logger.log(Level.INFO, "Received ARTHAS_STOP request, reason: {0}", reason);
    eventListener.onArthasStopRequested(reason);
    // 发送状态响应
    sendArthasStatus();

    // 记录任务完成
    taskLogger.logTaskCompleted(taskId, "arthas_stop_requested:" + reason);
  }

  /** 处理终端打开请求 */
  private void handleTerminalOpen(TerminalOpenMessage message) {
    String requestId = message.getPayload().getRequestId();
    String userId = message.getPayload().getUserId();
    int cols = message.getPayload().getCols();
    int rows = message.getPayload().getRows();

    // 记录任务接收
    taskLogger.logTaskReceived(
        requestId,
        TaskExecutionLogger.TASK_TYPE_ARTHAS_SESSION,
        TaskExecutionLogger.SOURCE_TUNNEL_SERVER,
        TaskExecutionLogger.details()
            .put("userId", userId)
            .put("cols", cols)
            .put("rows", rows)
            .build());

    if (sessionManager == null) {
      taskLogger.logTaskFailed(
          requestId,
          TaskExecutionLogger.ERROR_INTERNAL,
          "Session manager not initialized");
      sendTerminalRejected(requestId, "INTERNAL_ERROR", "Session manager not initialized");
      return;
    }

    logger.log(
        Level.INFO,
        "Received TERMINAL_OPEN request: requestId={0}, user={1}, cols={2}, rows={3}",
        new Object[] {requestId, userId, cols, rows});

    taskLogger.logTaskStarted(requestId, "session_manager");

    // 请求启动 Arthas（如果尚未启动）
    taskLogger.logTaskProgress(requestId, "arthas_check", "Ensuring Arthas is started");
    eventListener.onArthasStartRequested();

    // 创建会话
    taskLogger.logTaskProgress(requestId, "session_create", "Creating terminal session");
    SessionCreateRequest request = new SessionCreateRequest(userId, cols, rows);
    SessionCreateResult result = sessionManager.tryCreateSession(request);

    if (result.isSuccess()) {
      ArthasSession session = result.getSession();
      if (session != null) {
        TerminalReadyPayload payload =
            new TerminalReadyPayload(
                session.getSessionId(),
                requestId,
                config.getVersion(),
                sessionManager.getActiveSessionCount(),
                config.getMaxSessionsPerAgent());
        sendTextMessage(TerminalReadyMessage.create(payload));
        logger.log(Level.INFO, "Terminal session created: {0}", session.getSessionId());

        // 记录任务成功完成
        taskLogger.logTaskCompleted(
            requestId,
            "session_created:" + session.getSessionId(),
            0);
      }
    } else {
      String rejectReason = result.getRejectReason() != null 
          ? result.getRejectReason().name() : "UNKNOWN";
      String rejectMsg = result.getMessage() != null ? result.getMessage() : "Unknown error";
      sendTerminalRejected(requestId, rejectReason, rejectMsg);
      logger.log(Level.WARNING, "Terminal session rejected: {0}", rejectMsg);

      // 记录任务失败
      taskLogger.logTaskFailed(requestId, rejectReason, rejectMsg);
    }
  }

  /** 发送终端拒绝消息 */
  private void sendTerminalRejected(String requestId, String reason, String message) {
    TerminalRejectedPayload payload = new TerminalRejectedPayload(requestId, reason, message);
    sendTextMessage(TerminalRejectedMessage.create(payload));
  }

  /** 处理终端输入 */
  private void handleTerminalInput(TerminalInputMessage message) {
    String sessionId = message.getPayload().getSessionId();
    String data = message.getPayload().getData();

    // 仅对包含换行符的输入（表示完整命令）进行任务日志记录
    boolean isCommand = data.contains("\r") || data.contains("\n");
    String commandTaskId = null;

    if (isCommand && data.trim().length() > 0) {
      commandTaskId = "cmd-" + sessionId + "-" + System.currentTimeMillis();
      taskLogger.logTaskReceived(
          commandTaskId,
          TaskExecutionLogger.TASK_TYPE_ARTHAS_COMMAND,
          TaskExecutionLogger.SOURCE_TERMINAL_INPUT,
          TaskExecutionLogger.details()
              .put("sessionId", sessionId)
              .put("commandLength", data.trim().length())
              .build());
      taskLogger.logCommandExecution(commandTaskId, data.trim(), sessionId);
      taskLogger.logTaskStarted(commandTaskId, "terminal:" + sessionId);
    }

    if (sessionManager != null) {
      sessionManager.markSessionActive(sessionId);
    }

    // 通知监听器处理输入
    eventListener.onTerminalInput(sessionId, data);

    // 对于命令输入，记录已转发（实际执行结果由 Arthas 异步返回）
    if (commandTaskId != null) {
      taskLogger.logTaskProgress(commandTaskId, "forwarded", "Command forwarded to terminal");
      // 注意：命令的完成状态由输出回调来记录
    }
  }

  /** 处理终端尺寸调整 */
  private void handleTerminalResize(TerminalResizeMessage message) {
    String sessionId = message.getPayload().getSessionId();
    int cols = message.getPayload().getCols();
    int rows = message.getPayload().getRows();

    logger.log(
        Level.FINE,
        "Terminal resize: session={0}, cols={1}, rows={2}",
        new Object[] {sessionId, cols, rows});

    eventListener.onTerminalResize(sessionId, cols, rows);
  }

  /** 处理终端关闭 */
  private void handleTerminalClose(TerminalCloseMessage message) {
    String sessionId = message.getPayload().getSessionId();
    String taskId = "close-" + sessionId + "-" + System.currentTimeMillis();

    // 记录任务接收
    taskLogger.logTaskReceived(
        taskId,
        TaskExecutionLogger.TASK_TYPE_ARTHAS_SESSION,
        TaskExecutionLogger.SOURCE_TUNNEL_SERVER,
        TaskExecutionLogger.details()
            .put("sessionId", sessionId)
            .put("action", "close")
            .build());

    taskLogger.logTaskStarted(taskId, "session_manager");

    logger.log(Level.INFO, "Received TERMINAL_CLOSE: session={0}", sessionId);

    if (sessionManager != null) {
      sessionManager.closeSession(sessionId);
    }

    // 发送关闭确认
    TerminalClosedPayload payload = new TerminalClosedPayload(sessionId, "user_request");
    sendTextMessage(TerminalClosedMessage.create(payload));

    // 记录任务完成
    taskLogger.logTaskCompleted(taskId, "session_closed:" + sessionId);
  }

  /** 发送 Arthas 状态 */
  public void sendArthasStatus() {
    String state = eventListener.getArthasState();
    int activeSessions = sessionManager != null ? sessionManager.getActiveSessionCount() : 0;
    long uptimeMs = eventListener.getArthasUptimeMs();

    ArthasStatusPayload payload =
        new ArthasStatusPayload(
            state, config.getVersion(), activeSessions, config.getMaxSessionsPerAgent(), uptimeMs);

    sendTextMessage(ArthasStatusMessage.create(payload));
  }

  /** 发送终端关闭消息 */
  public void sendTerminalClosed(String sessionId, String reason) {
    TerminalClosedPayload payload = new TerminalClosedPayload(sessionId, reason);
    sendTextMessage(TerminalClosedMessage.create(payload));
  }

  /** 发送错误消息 */
  public void sendError(String code, String message, @Nullable String details) {
    ErrorPayload payload = new ErrorPayload(code, message, details);
    sendTextMessage(ErrorMessage.create(payload));
  }

  /** 获取连接状态 */
  public ConnectionState getConnectionState() {
    ConnectionState state = connectionState.get();
    return state != null ? state : ConnectionState.DISCONNECTED;
  }

  /** 是否已连接 */
  public boolean isConnected() {
    return connectionState.get() == ConnectionState.CONNECTED;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      logger.log(Level.INFO, "Closing tunnel client");

      stopPingTask();

      if (reconnectTask != null) {
        reconnectTask.cancel(false);
        reconnectTask = null;
      }

      WebSocket ws = webSocketRef.getAndSet(null);
      if (ws != null) {
        ws.close(1000, "client closing");
      }

      connectionState.set(ConnectionState.CLOSED);
      
      // 关闭 OkHttpClient
      httpClient.dispatcher().executorService().shutdown();
      httpClient.connectionPool().evictAll();
    }
  }

  /** OkHttp WebSocket 监听器 */
  private class TunnelWebSocketListener extends WebSocketListener {

    @Override
    public void onOpen(WebSocket webSocket, Response response) {
      logger.log(Level.INFO, "WebSocket opened");
      connectionState.set(ConnectionState.CONNECTED);
      reconnectAttempts.set(0);

      // 发送注册消息
      sendRegisterMessage();

      // 启动心跳
      startPingTask();

      // 通知监听器
      eventListener.onConnected();
    }

    @Override
    public void onMessage(WebSocket webSocket, String text) {
      handleTextMessage(text);
    }

    @Override
    public void onMessage(WebSocket webSocket, ByteString bytes) {
      // Agent 端一般不接收二进制消息，但保留处理能力
      logger.log(Level.FINE, "Received binary message, size: {0}", bytes.size());
    }

    @Override
    public void onClosing(WebSocket webSocket, int code, String reason) {
      logger.log(Level.INFO, "WebSocket closing: code={0}, reason={1}", new Object[] {code, reason});
      webSocket.close(code, reason);
    }

    @Override
    public void onClosed(WebSocket webSocket, int code, String reason) {
      logger.log(Level.INFO, "WebSocket closed: code={0}, reason={1}", new Object[] {code, reason});
      connectionState.set(ConnectionState.DISCONNECTED);
      webSocketRef.set(null);

      if (!closed.get()) {
        eventListener.onDisconnected(reason);
        scheduleReconnect();
      }
    }

    @Override
    public void onFailure(WebSocket webSocket, Throwable t, @Nullable Response response) {
      logger.log(Level.WARNING, "WebSocket error: {0}", t.getMessage());
      connectionState.set(ConnectionState.DISCONNECTED);
      webSocketRef.set(null);

      if (!closed.get()) {
        eventListener.onError(t);
        scheduleReconnect();
      }
    }
  }

  /** Tunnel 事件监听器 */
  public interface TunnelEventListener {
    /** 连接成功 */
    void onConnected();

    /** 断开连接 */
    void onDisconnected(String reason);

    /** 连接错误 */
    void onError(Throwable error);

    /** 达到最大重连次数 */
    void onMaxReconnectReached();

    /** 收到 Arthas 启动请求 */
    void onArthasStartRequested();

    /** 收到 Arthas 停止请求 */
    void onArthasStopRequested(@Nullable String reason);

    /** 收到终端输入 */
    void onTerminalInput(String sessionId, String data);

    /** 收到终端尺寸调整 */
    void onTerminalResize(String sessionId, int cols, int rows);

    /** 获取 Arthas 状态 */
    String getArthasState();

    /** 获取 Arthas 运行时长 */
    long getArthasUptimeMs();
  }

  /** 二进制输出处理器（用于发送终端输出） */
  public interface BinaryOutputHandler {
    /** 发送二进制输出 */
    void sendOutput(String sessionId, byte[] data);
  }
}
