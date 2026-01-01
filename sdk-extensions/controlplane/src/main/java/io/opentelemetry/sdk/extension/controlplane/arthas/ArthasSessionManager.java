/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Arthas 会话管理器
 *
 * <p>管理当前 Agent 的所有 Arthas 终端会话，包括：
 *
 * <ul>
 *   <li>会话创建与销毁
 *   <li>会话数量限制
 *   <li>空闲超时检测
 *   <li>过期会话清理
 * </ul>
 */
public final class ArthasSessionManager implements Closeable {

  private static final Logger logger = Logger.getLogger(ArthasSessionManager.class.getName());

  private final ArthasConfig config;
  private final Map<String, ArthasSession> sessions = new ConcurrentHashMap<>();
  private final AtomicInteger sessionCounter = new AtomicInteger(0);
  private final SessionEventListener listener;

  @Nullable private ScheduledFuture<?> cleanupTask;

  /**
   * 创建会话管理器
   *
   * @param config Arthas 配置
   * @param listener 会话事件监听器
   */
  public ArthasSessionManager(ArthasConfig config, SessionEventListener listener) {
    this.config = config;
    this.listener = listener;
  }

  /**
   * 启动会话清理任务
   *
   * @param scheduler 调度器
   */
  public void startCleanupTask(ScheduledExecutorService scheduler) {
    // 每 30 秒检查一次过期会话
    cleanupTask =
        scheduler.scheduleWithFixedDelay(this::cleanupExpiredSessions, 30, 30, TimeUnit.SECONDS);
    logger.log(Level.FINE, "Session cleanup task started");
  }

  /**
   * 停止会话清理任务
   */
  public void stopCleanupTask() {
    if (cleanupTask != null) {
      cleanupTask.cancel(false);
      cleanupTask = null;
    }
  }

  /**
   * 尝试创建新会话
   *
   * @param request 会话创建请求
   * @return 创建结果
   */
  public SessionCreateResult tryCreateSession(SessionCreateRequest request) {
    // 1. 检查会话数量限制
    if (sessions.size() >= config.getMaxSessionsPerAgent()) {
      logger.log(
          Level.WARNING,
          "Session creation rejected: max sessions ({0}) reached",
          config.getMaxSessionsPerAgent());
      return SessionCreateResult.rejected(
          RejectReason.MAX_SESSIONS_REACHED,
          "Maximum " + config.getMaxSessionsPerAgent() + " sessions allowed per agent");
    }

    // 2. 创建新会话
    ArthasSession session =
        ArthasSession.builder()
            .setUserId(request.getUserId())
            .setCols(request.getCols())
            .setRows(request.getRows())
            .build();

    // 3. 注册会话
    sessions.put(session.getSessionId(), session);
    sessionCounter.incrementAndGet();

    logger.log(
        Level.INFO,
        "Session created: {0}, user: {1}, total sessions: {2}",
        new Object[] {session.getSessionId(), request.getUserId(), sessions.size()});

    // 4. 通知监听器
    listener.onSessionCreated(session);

    return SessionCreateResult.success(session);
  }

  /**
   * 关闭指定会话
   *
   * @param sessionId 会话 ID
   * @return 是否成功关闭
   */
  public boolean closeSession(String sessionId) {
    ArthasSession session = sessions.remove(sessionId);
    if (session == null) {
      logger.log(Level.WARNING, "Session not found for close: {0}", sessionId);
      return false;
    }

    logger.log(
        Level.INFO,
        "Session closed: {0}, user: {1}, age: {2}ms, remaining sessions: {3}",
        new Object[] {
          sessionId, session.getUserId(), session.getAgeMillis(), sessions.size()
        });

    // 通知监听器
    listener.onSessionClosed(session);

    // 检查是否需要触发 Arthas 关闭
    if (sessions.isEmpty()) {
      listener.onAllSessionsClosed();
    }

    return true;
  }

  /**
   * 获取指定会话
   *
   * @param sessionId 会话 ID
   * @return 会话，如果不存在返回 null
   */
  @Nullable
  public ArthasSession getSession(String sessionId) {
    return sessions.get(sessionId);
  }

  /**
   * 更新会话活跃时间
   *
   * @param sessionId 会话 ID
   */
  public void markSessionActive(String sessionId) {
    ArthasSession session = sessions.get(sessionId);
    if (session != null) {
      session.markActive();
    }
  }

  /**
   * 获取当前活跃会话数
   *
   * @return 会话数
   */
  public int getActiveSessionCount() {
    return sessions.size();
  }

  /**
   * 获取所有会话的信息（用于状态上报）
   *
   * @return 会话信息列表
   */
  public List<SessionInfo> getSessionInfos() {
    return sessions.values().stream()
        .map(
            s ->
                new SessionInfo(
                    s.getSessionId(),
                    s.getUserId(),
                    s.getCreatedAt(),
                    s.getLastActiveAt(),
                    s.getCols(),
                    s.getRows()))
        .collect(Collectors.toList());
  }

  /**
   * 获取所有会话 ID
   *
   * @return 会话 ID 列表
   */
  public List<String> getSessionIds() {
    return new ArrayList<>(sessions.keySet());
  }

  /**
   * 检查是否还有活跃会话
   *
   * @return 如果有活跃会话返回 true
   */
  public boolean hasActiveSessions() {
    return !sessions.isEmpty();
  }

  /** 清理过期会话 */
  private void cleanupExpiredSessions() {
    long idleTimeoutMillis = config.getSessionIdleTimeout().toMillis();
    long maxDurationMillis = config.getSessionMaxDuration().toMillis();

    List<String> expiredSessionIds = new ArrayList<>();

    for (ArthasSession session : sessions.values()) {
      boolean expired = false;
      String reason = null;

      if (session.isExpired(maxDurationMillis)) {
        expired = true;
        reason = "max duration exceeded";
      } else if (session.isIdleTimeout(idleTimeoutMillis)) {
        expired = true;
        reason = "idle timeout";
      }

      if (expired) {
        expiredSessionIds.add(session.getSessionId());
        logger.log(
            Level.INFO,
            "Session expired: {0}, reason: {1}, age: {2}ms, idle: {3}ms",
            new Object[] {
              session.getSessionId(), reason, session.getAgeMillis(), session.getIdleMillis()
            });
      }
    }

    // 批量关闭过期会话
    for (String sessionId : expiredSessionIds) {
      closeSession(sessionId);
    }

    if (!expiredSessionIds.isEmpty()) {
      logger.log(
          Level.INFO,
          "Cleaned up {0} expired sessions, remaining: {1}",
          new Object[] {expiredSessionIds.size(), sessions.size()});
    }
  }

  /** 关闭所有会话 */
  public void closeAllSessions() {
    List<String> sessionIds = new ArrayList<>(sessions.keySet());
    for (String sessionId : sessionIds) {
      closeSession(sessionId);
    }
    logger.log(Level.INFO, "All sessions closed");
  }

  @Override
  public void close() {
    stopCleanupTask();
    closeAllSessions();
  }

  /**
   * 获取会话统计信息
   *
   * @return 统计信息 Map
   */
  public Map<String, Object> getStatistics() {
    Map<String, Object> stats = new java.util.LinkedHashMap<>();
    stats.put("activeSessions", sessions.size());
    stats.put("maxSessions", config.getMaxSessionsPerAgent());
    stats.put("totalCreated", sessionCounter.get());
    return Collections.unmodifiableMap(stats);
  }

  // ===== 内部类 =====

  /** 会话创建请求 */
  public static final class SessionCreateRequest {
    @Nullable private final String userId;
    private final int cols;
    private final int rows;

    public SessionCreateRequest(@Nullable String userId, int cols, int rows) {
      this.userId = userId;
      this.cols = cols > 0 ? cols : 120;
      this.rows = rows > 0 ? rows : 40;
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

  /** 会话创建结果 */
  public static final class SessionCreateResult {
    private final boolean success;
    @Nullable private final ArthasSession session;
    @Nullable private final RejectReason rejectReason;
    @Nullable private final String message;

    private SessionCreateResult(
        boolean success,
        @Nullable ArthasSession session,
        @Nullable RejectReason rejectReason,
        @Nullable String message) {
      this.success = success;
      this.session = session;
      this.rejectReason = rejectReason;
      this.message = message;
    }

    public static SessionCreateResult success(ArthasSession session) {
      return new SessionCreateResult(
          /* success= */ true, session, /* rejectReason= */ null, /* message= */ null);
    }

    public static SessionCreateResult rejected(RejectReason reason, String message) {
      return new SessionCreateResult(
          /* success= */ false, /* session= */ null, reason, message);
    }

    public boolean isSuccess() {
      return success;
    }

    @Nullable
    public ArthasSession getSession() {
      return session;
    }

    @Nullable
    public RejectReason getRejectReason() {
      return rejectReason;
    }

    @Nullable
    public String getMessage() {
      return message;
    }
  }

  /** 拒绝原因 */
  public enum RejectReason {
    /** 达到最大会话数 */
    MAX_SESSIONS_REACHED,
    /** Arthas 启动失败 */
    ARTHAS_START_FAILED,
    /** Arthas 未运行 */
    ARTHAS_NOT_RUNNING,
    /** 内部错误 */
    INTERNAL_ERROR
  }

  /** 会话信息（用于状态上报） */
  public static final class SessionInfo {
    private final String sessionId;
    @Nullable private final String userId;
    private final java.time.Instant createdAt;
    private final java.time.Instant lastActiveAt;
    private final int cols;
    private final int rows;

    public SessionInfo(
        String sessionId,
        @Nullable String userId,
        java.time.Instant createdAt,
        java.time.Instant lastActiveAt,
        int cols,
        int rows) {
      this.sessionId = sessionId;
      this.userId = userId;
      this.createdAt = createdAt;
      this.lastActiveAt = lastActiveAt;
      this.cols = cols;
      this.rows = rows;
    }

    public String getSessionId() {
      return sessionId;
    }

    @Nullable
    public String getUserId() {
      return userId;
    }

    public java.time.Instant getCreatedAt() {
      return createdAt;
    }

    public java.time.Instant getLastActiveAt() {
      return lastActiveAt;
    }

    public int getCols() {
      return cols;
    }

    public int getRows() {
      return rows;
    }
  }

  /** 会话事件监听器 */
  public interface SessionEventListener {
    /** 会话创建时调用 */
    void onSessionCreated(ArthasSession session);

    /** 会话关闭时调用 */
    void onSessionClosed(ArthasSession session);

    /** 所有会话关闭时调用 */
    void onAllSessionsClosed();
  }
}
