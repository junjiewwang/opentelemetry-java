/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas.tunnel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;

/**
 * Tunnel 消息基类
 *
 * <p>所有 Tunnel 消息的基类，包含消息类型、ID 和时间戳。
 */
public class TunnelMessage {

  private final MessageType type;
  private final String id;
  private final long timestamp;

  @JsonCreator
  public TunnelMessage(
      @JsonProperty("type") MessageType type,
      @JsonProperty("id") String id,
      @JsonProperty("timestamp") long timestamp) {
    this.type = type;
    this.id = id;
    this.timestamp = timestamp;
  }

  public MessageType getType() {
    return type;
  }

  public String getId() {
    return id;
  }

  public long getTimestamp() {
    return timestamp;
  }

  /** 消息类型枚举 */
  public enum MessageType {
    // ===== Server -> Agent (控制指令) =====

    /** 启动 Arthas */
    ARTHAS_START,
    /** 停止 Arthas */
    ARTHAS_STOP,
    /** 打开终端会话 */
    TERMINAL_OPEN,
    /** 终端输入 */
    TERMINAL_INPUT,
    /** 调整终端尺寸 */
    TERMINAL_RESIZE,
    /** 关闭终端会话 */
    TERMINAL_CLOSE,
    /** Ping（心跳） */
    PING,

    // ===== Agent -> Server (响应/数据) =====

    /** Arthas 状态 */
    ARTHAS_STATUS,
    /** 终端就绪 */
    TERMINAL_READY,
    /** 终端拒绝 */
    TERMINAL_REJECTED,
    /** 终端关闭确认 */
    TERMINAL_CLOSED,
    /** Pong（心跳响应） */
    PONG,
    /** 错误 */
    ERROR,
    /** 注册 */
    REGISTER,
    /** 注册响应 */
    REGISTER_ACK
  }

  /** 创建消息 ID */
  public static String generateId() {
    return "msg-" + System.currentTimeMillis() + "-" + (int) (Math.random() * 10000);
  }

  /** 获取当前时间戳 */
  public static long currentTimestamp() {
    return System.currentTimeMillis();
  }

  // ===== 具体消息类型 =====

  /** 注册消息 (Agent -> Server) */
  public static class RegisterMessage extends TunnelMessage {
    private final RegisterPayload payload;

    @JsonCreator
    public RegisterMessage(
        @JsonProperty("id") String id,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("payload") RegisterPayload payload) {
      super(MessageType.REGISTER, id, timestamp);
      this.payload = payload;
    }

    public static RegisterMessage create(RegisterPayload payload) {
      return new RegisterMessage(generateId(), currentTimestamp(), payload);
    }

    public RegisterPayload getPayload() {
      return payload;
    }
  }

  /** 注册消息载荷 */
  public static class RegisterPayload {
    private final String agentId;
    @Nullable private final String serviceName;
    @Nullable private final String version;
    @Nullable private final String hostname;
    @Nullable private final String ip;

    @JsonCreator
    public RegisterPayload(
        @JsonProperty("agentId") String agentId,
        @JsonProperty("serviceName") @Nullable String serviceName,
        @JsonProperty("version") @Nullable String version,
        @JsonProperty("hostname") @Nullable String hostname,
        @JsonProperty("ip") @Nullable String ip) {
      this.agentId = agentId;
      this.serviceName = serviceName;
      this.version = version;
      this.hostname = hostname;
      this.ip = ip;
    }

    public String getAgentId() {
      return agentId;
    }

    @Nullable
    public String getServiceName() {
      return serviceName;
    }

    @Nullable
    public String getVersion() {
      return version;
    }

    @Nullable
    public String getHostname() {
      return hostname;
    }

    @Nullable
    public String getIp() {
      return ip;
    }
  }

  /** 注册确认消息 (Server -> Agent) */
  public static class RegisterAckMessage extends TunnelMessage {
    private final RegisterAckPayload payload;

    @JsonCreator
    public RegisterAckMessage(
        @JsonProperty("id") String id,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("payload") RegisterAckPayload payload) {
      super(MessageType.REGISTER_ACK, id, timestamp);
      this.payload = payload;
    }

    public RegisterAckPayload getPayload() {
      return payload;
    }
  }

  /** 注册确认载荷 */
  public static class RegisterAckPayload {
    private final boolean success;
    @Nullable private final String message;

    @JsonCreator
    public RegisterAckPayload(
        @JsonProperty("success") boolean success, @JsonProperty("message") @Nullable String message) {
      this.success = success;
      this.message = message;
    }

    public boolean isSuccess() {
      return success;
    }

    @Nullable
    public String getMessage() {
      return message;
    }
  }

  /** Arthas 启动消息 (Server -> Agent) */
  public static class ArthasStartMessage extends TunnelMessage {
    @Nullable private final ArthasStartPayload payload;

    @JsonCreator
    public ArthasStartMessage(
        @JsonProperty("id") String id,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("payload") @Nullable ArthasStartPayload payload) {
      super(MessageType.ARTHAS_START, id, timestamp);
      this.payload = payload;
    }

    @Nullable
    public ArthasStartPayload getPayload() {
      return payload;
    }
  }

  /** Arthas 启动载荷 */
  public static class ArthasStartPayload {
    @Nullable private final String userId;

    @JsonCreator
    public ArthasStartPayload(@JsonProperty("userId") @Nullable String userId) {
      this.userId = userId;
    }

    @Nullable
    public String getUserId() {
      return userId;
    }
  }

  /** Arthas 停止消息 (Server -> Agent) */
  public static class ArthasStopMessage extends TunnelMessage {
    @Nullable private final ArthasStopPayload payload;

    @JsonCreator
    public ArthasStopMessage(
        @JsonProperty("id") String id,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("payload") @Nullable ArthasStopPayload payload) {
      super(MessageType.ARTHAS_STOP, id, timestamp);
      this.payload = payload;
    }

    @Nullable
    public ArthasStopPayload getPayload() {
      return payload;
    }
  }

  /** Arthas 停止载荷 */
  public static class ArthasStopPayload {
    @Nullable private final String reason;

    @JsonCreator
    public ArthasStopPayload(@JsonProperty("reason") @Nullable String reason) {
      this.reason = reason;
    }

    @Nullable
    public String getReason() {
      return reason;
    }
  }

  /** Arthas 状态消息 (Agent -> Server) */
  public static class ArthasStatusMessage extends TunnelMessage {
    private final ArthasStatusPayload payload;

    @JsonCreator
    public ArthasStatusMessage(
        @JsonProperty("id") String id,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("payload") ArthasStatusPayload payload) {
      super(MessageType.ARTHAS_STATUS, id, timestamp);
      this.payload = payload;
    }

    public static ArthasStatusMessage create(ArthasStatusPayload payload) {
      return new ArthasStatusMessage(generateId(), currentTimestamp(), payload);
    }

    public ArthasStatusPayload getPayload() {
      return payload;
    }
  }

  /** Arthas 状态载荷 */
  public static class ArthasStatusPayload {
    private final String state;
    @Nullable private final String arthasVersion;
    private final int activeSessions;
    private final int maxSessions;
    private final long uptimeMs;

    @JsonCreator
    public ArthasStatusPayload(
        @JsonProperty("state") String state,
        @JsonProperty("arthasVersion") @Nullable String arthasVersion,
        @JsonProperty("activeSessions") int activeSessions,
        @JsonProperty("maxSessions") int maxSessions,
        @JsonProperty("uptimeMs") long uptimeMs) {
      this.state = state;
      this.arthasVersion = arthasVersion;
      this.activeSessions = activeSessions;
      this.maxSessions = maxSessions;
      this.uptimeMs = uptimeMs;
    }

    public String getState() {
      return state;
    }

    @Nullable
    public String getArthasVersion() {
      return arthasVersion;
    }

    public int getActiveSessions() {
      return activeSessions;
    }

    public int getMaxSessions() {
      return maxSessions;
    }

    public long getUptimeMs() {
      return uptimeMs;
    }
  }

  /** 终端打开消息 (Server -> Agent) */
  public static class TerminalOpenMessage extends TunnelMessage {
    private final TerminalOpenPayload payload;

    @JsonCreator
    public TerminalOpenMessage(
        @JsonProperty("id") String id,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("payload") TerminalOpenPayload payload) {
      super(MessageType.TERMINAL_OPEN, id, timestamp);
      this.payload = payload;
    }

    public TerminalOpenPayload getPayload() {
      return payload;
    }
  }

  /** 终端打开载荷 */
  public static class TerminalOpenPayload {
    private final String requestId;
    @Nullable private final String userId;
    private final int cols;
    private final int rows;

    @JsonCreator
    public TerminalOpenPayload(
        @JsonProperty("requestId") String requestId,
        @JsonProperty("userId") @Nullable String userId,
        @JsonProperty("cols") int cols,
        @JsonProperty("rows") int rows) {
      this.requestId = requestId;
      this.userId = userId;
      this.cols = cols > 0 ? cols : 120;
      this.rows = rows > 0 ? rows : 40;
    }

    public String getRequestId() {
      return requestId;
    }

    @Nullable
    public String getUserId() {
      return userId;
    }

    public int getCols() {
      return cols;
    }

    public int getRows() {
      return rows;
    }
  }

  /** 终端就绪消息 (Agent -> Server) */
  public static class TerminalReadyMessage extends TunnelMessage {
    private final TerminalReadyPayload payload;

    @JsonCreator
    public TerminalReadyMessage(
        @JsonProperty("id") String id,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("payload") TerminalReadyPayload payload) {
      super(MessageType.TERMINAL_READY, id, timestamp);
      this.payload = payload;
    }

    public static TerminalReadyMessage create(TerminalReadyPayload payload) {
      return new TerminalReadyMessage(generateId(), currentTimestamp(), payload);
    }

    public TerminalReadyPayload getPayload() {
      return payload;
    }
  }

  /** 终端就绪载荷 */
  public static class TerminalReadyPayload {
    private final String sessionId;
    private final String requestId;
    @Nullable private final String arthasVersion;
    private final int currentSessions;
    private final int maxSessions;

    @JsonCreator
    public TerminalReadyPayload(
        @JsonProperty("sessionId") String sessionId,
        @JsonProperty("requestId") String requestId,
        @JsonProperty("arthasVersion") @Nullable String arthasVersion,
        @JsonProperty("currentSessions") int currentSessions,
        @JsonProperty("maxSessions") int maxSessions) {
      this.sessionId = sessionId;
      this.requestId = requestId;
      this.arthasVersion = arthasVersion;
      this.currentSessions = currentSessions;
      this.maxSessions = maxSessions;
    }

    public String getSessionId() {
      return sessionId;
    }

    public String getRequestId() {
      return requestId;
    }

    @Nullable
    public String getArthasVersion() {
      return arthasVersion;
    }

    public int getCurrentSessions() {
      return currentSessions;
    }

    public int getMaxSessions() {
      return maxSessions;
    }
  }

  /** 终端拒绝消息 (Agent -> Server) */
  public static class TerminalRejectedMessage extends TunnelMessage {
    private final TerminalRejectedPayload payload;

    @JsonCreator
    public TerminalRejectedMessage(
        @JsonProperty("id") String id,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("payload") TerminalRejectedPayload payload) {
      super(MessageType.TERMINAL_REJECTED, id, timestamp);
      this.payload = payload;
    }

    public static TerminalRejectedMessage create(TerminalRejectedPayload payload) {
      return new TerminalRejectedMessage(generateId(), currentTimestamp(), payload);
    }

    public TerminalRejectedPayload getPayload() {
      return payload;
    }
  }

  /** 终端拒绝载荷 */
  public static class TerminalRejectedPayload {
    private final String requestId;
    private final String reason;
    @Nullable private final String message;

    @JsonCreator
    public TerminalRejectedPayload(
        @JsonProperty("requestId") String requestId,
        @JsonProperty("reason") String reason,
        @JsonProperty("message") @Nullable String message) {
      this.requestId = requestId;
      this.reason = reason;
      this.message = message;
    }

    public String getRequestId() {
      return requestId;
    }

    public String getReason() {
      return reason;
    }

    @Nullable
    public String getMessage() {
      return message;
    }
  }

  /** 终端输入消息 (Server -> Agent) */
  public static class TerminalInputMessage extends TunnelMessage {
    private final TerminalInputPayload payload;

    @JsonCreator
    public TerminalInputMessage(
        @JsonProperty("id") String id,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("payload") TerminalInputPayload payload) {
      super(MessageType.TERMINAL_INPUT, id, timestamp);
      this.payload = payload;
    }

    public TerminalInputPayload getPayload() {
      return payload;
    }
  }

  /** 终端输入载荷 */
  public static class TerminalInputPayload {
    private final String sessionId;
    private final String data;

    @JsonCreator
    public TerminalInputPayload(
        @JsonProperty("sessionId") String sessionId, @JsonProperty("data") String data) {
      this.sessionId = sessionId;
      this.data = data;
    }

    public String getSessionId() {
      return sessionId;
    }

    public String getData() {
      return data;
    }
  }

  /** 终端尺寸调整消息 (Server -> Agent) */
  public static class TerminalResizeMessage extends TunnelMessage {
    private final TerminalResizePayload payload;

    @JsonCreator
    public TerminalResizeMessage(
        @JsonProperty("id") String id,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("payload") TerminalResizePayload payload) {
      super(MessageType.TERMINAL_RESIZE, id, timestamp);
      this.payload = payload;
    }

    public TerminalResizePayload getPayload() {
      return payload;
    }
  }

  /** 终端尺寸调整载荷 */
  public static class TerminalResizePayload {
    private final String sessionId;
    private final int cols;
    private final int rows;

    @JsonCreator
    public TerminalResizePayload(
        @JsonProperty("sessionId") String sessionId,
        @JsonProperty("cols") int cols,
        @JsonProperty("rows") int rows) {
      this.sessionId = sessionId;
      this.cols = cols;
      this.rows = rows;
    }

    public String getSessionId() {
      return sessionId;
    }

    public int getCols() {
      return cols;
    }

    public int getRows() {
      return rows;
    }
  }

  /** 终端关闭消息 (Server -> Agent) */
  public static class TerminalCloseMessage extends TunnelMessage {
    private final TerminalClosePayload payload;

    @JsonCreator
    public TerminalCloseMessage(
        @JsonProperty("id") String id,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("payload") TerminalClosePayload payload) {
      super(MessageType.TERMINAL_CLOSE, id, timestamp);
      this.payload = payload;
    }

    public TerminalClosePayload getPayload() {
      return payload;
    }
  }

  /** 终端关闭载荷 */
  public static class TerminalClosePayload {
    private final String sessionId;

    @JsonCreator
    public TerminalClosePayload(@JsonProperty("sessionId") String sessionId) {
      this.sessionId = sessionId;
    }

    public String getSessionId() {
      return sessionId;
    }
  }

  /** 终端已关闭消息 (Agent -> Server) */
  public static class TerminalClosedMessage extends TunnelMessage {
    private final TerminalClosedPayload payload;

    @JsonCreator
    public TerminalClosedMessage(
        @JsonProperty("id") String id,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("payload") TerminalClosedPayload payload) {
      super(MessageType.TERMINAL_CLOSED, id, timestamp);
      this.payload = payload;
    }

    public static TerminalClosedMessage create(TerminalClosedPayload payload) {
      return new TerminalClosedMessage(generateId(), currentTimestamp(), payload);
    }

    public TerminalClosedPayload getPayload() {
      return payload;
    }
  }

  /** 终端已关闭载荷 */
  public static class TerminalClosedPayload {
    private final String sessionId;
    @Nullable private final String reason;

    @JsonCreator
    public TerminalClosedPayload(
        @JsonProperty("sessionId") String sessionId,
        @JsonProperty("reason") @Nullable String reason) {
      this.sessionId = sessionId;
      this.reason = reason;
    }

    public String getSessionId() {
      return sessionId;
    }

    @Nullable
    public String getReason() {
      return reason;
    }
  }

  /** Ping 消息 (Server -> Agent) */
  public static class PingMessage extends TunnelMessage {
    @JsonCreator
    public PingMessage(
        @JsonProperty("id") String id, @JsonProperty("timestamp") long timestamp) {
      super(MessageType.PING, id, timestamp);
    }
  }

  /** Pong 消息 (Agent -> Server) */
  public static class PongMessage extends TunnelMessage {
    @JsonCreator
    public PongMessage(
        @JsonProperty("id") String id, @JsonProperty("timestamp") long timestamp) {
      super(MessageType.PONG, id, timestamp);
    }

    public static PongMessage create() {
      return new PongMessage(generateId(), currentTimestamp());
    }
  }

  /** 错误消息 */
  public static class ErrorMessage extends TunnelMessage {
    private final ErrorPayload payload;

    @JsonCreator
    public ErrorMessage(
        @JsonProperty("id") String id,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("payload") ErrorPayload payload) {
      super(MessageType.ERROR, id, timestamp);
      this.payload = payload;
    }

    public static ErrorMessage create(ErrorPayload payload) {
      return new ErrorMessage(generateId(), currentTimestamp(), payload);
    }

    public ErrorPayload getPayload() {
      return payload;
    }
  }

  /** 错误载荷 */
  public static class ErrorPayload {
    private final String code;
    private final String message;
    @Nullable private final String details;

    @JsonCreator
    public ErrorPayload(
        @JsonProperty("code") String code,
        @JsonProperty("message") String message,
        @JsonProperty("details") @Nullable String details) {
      this.code = code;
      this.message = message;
      this.details = details;
    }

    public String getCode() {
      return code;
    }

    public String getMessage() {
      return message;
    }

    @Nullable
    public String getDetails() {
      return details;
    }
  }
}
