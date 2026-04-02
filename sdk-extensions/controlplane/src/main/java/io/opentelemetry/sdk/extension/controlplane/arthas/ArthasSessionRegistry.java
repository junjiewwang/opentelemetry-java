package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/** Arthas 异步会话注册表。 */
public final class ArthasSessionRegistry {

  private static final Logger logger = Logger.getLogger(ArthasSessionRegistry.class.getName());

  private final ConcurrentMap<String, SessionEntry> sessions = new ConcurrentHashMap<>();

  public ArthasAsyncSessionSnapshot registerSession(
      String sessionId, String consumerId, long ttlMillis, long idleTimeoutMillis) {
    long now = System.currentTimeMillis();
    SessionEntry entry =
        new SessionEntry(sessionId, consumerId, ttlMillis, idleTimeoutMillis, now, now);
    sessions.put(sessionId, entry);
    return entry.snapshot();
  }

  public ArthasAsyncSessionSnapshot requireActiveSession(String sessionId, String consumerId) {
    SessionEntry entry = requireEntry(sessionId);
    synchronized (entry) {
      if (!entry.consumerId.equals(consumerId)) {
        throw sessionFailure(
            ArthasTaskProtocol.ErrorCode.SESSION_NOT_FOUND,
            "Consumer does not match session: " + sessionId);
      }
      ensureNotExpired(entry);
      ensureNotClosed(entry);
      entry.touch();
      return entry.snapshot();
    }
  }

  public ArthasAsyncSessionSnapshot requireActiveSession(String sessionId) {
    SessionEntry entry = requireEntry(sessionId);
    synchronized (entry) {
      ensureNotExpired(entry);
      ensureNotClosed(entry);
      entry.touch();
      return entry.snapshot();
    }
  }

  public ArthasAsyncSessionSnapshot markExecuting(
      String sessionId, String command, @Nullable Integer jobId, @Nullable String jobStatus) {
    SessionEntry entry = requireEntry(sessionId);
    synchronized (entry) {
      ensureNotExpired(entry);
      ensureNotClosed(entry);
      if (entry.state == ArthasSessionState.EXECUTING) {
        throw sessionFailure(
            ArthasTaskProtocol.ErrorCode.SESSION_NOT_IDLE,
            "Session is already executing: " + sessionId);
      }
      entry.state = ArthasSessionState.EXECUTING;
      entry.currentCommand = command;
      entry.currentJobId = jobId;
      entry.currentJobStatus = jobStatus;
      entry.endOfStream = false;
      entry.touch();
      return entry.snapshot();
    }
  }

  public ArthasAsyncSessionSnapshot updateAfterPull(
      String sessionId,
      @Nullable Integer jobId,
      @Nullable String jobStatus,
      boolean endOfStream) {
    SessionEntry entry = requireEntry(sessionId);
    synchronized (entry) {
      ensureNotExpired(entry);
      ensureNotClosed(entry);
      entry.currentJobId = jobId;
      entry.currentJobStatus = jobStatus;
      entry.endOfStream = endOfStream;
      if (endOfStream) {
        entry.state = ArthasSessionState.IDLE;
      } else if (entry.state != ArthasSessionState.INTERRUPTED) {
        entry.state = ArthasSessionState.EXECUTING;
      }
      entry.touch();
      return entry.snapshot();
    }
  }

  public ArthasAsyncSessionSnapshot markInterrupted(
      String sessionId, @Nullable Integer jobId, @Nullable String jobStatus) {
    SessionEntry entry = requireEntry(sessionId);
    synchronized (entry) {
      ensureNotExpired(entry);
      ensureNotClosed(entry);
      entry.state = ArthasSessionState.INTERRUPTED;
      entry.currentJobId = jobId;
      entry.currentJobStatus = jobStatus;
      entry.endOfStream = true;
      entry.touch();
      return entry.snapshot();
    }
  }

  public ArthasAsyncSessionSnapshot markClosed(String sessionId) {
    SessionEntry entry = requireEntry(sessionId);
    synchronized (entry) {
      entry.state = ArthasSessionState.CLOSED;
      entry.endOfStream = true;
      entry.touch();
      return entry.snapshot();
    }
  }

  public ArthasAsyncSessionSnapshot markFailed(String sessionId, @Nullable String jobStatus) {
    SessionEntry entry = requireEntry(sessionId);
    synchronized (entry) {
      ensureNotExpired(entry);
      ensureNotClosed(entry);
      entry.state = ArthasSessionState.FAILED;
      entry.currentJobStatus = jobStatus;
      entry.endOfStream = true;
      entry.touch();
      return entry.snapshot();
    }
  }

  public void remove(String sessionId) {
    sessions.remove(sessionId);
  }

  public Collection<ArthasAsyncSessionSnapshot> listSnapshots() {
    List<ArthasAsyncSessionSnapshot> snapshots = new ArrayList<>();
    for (SessionEntry entry : sessions.values()) {
      synchronized (entry) {
        snapshots.add(entry.snapshot());
      }
    }
    return Collections.unmodifiableList(snapshots);
  }

  public List<String> cleanupExpiredSessions() {
    long now = System.currentTimeMillis();
    List<String> removed = new ArrayList<>();
    for (Map.Entry<String, SessionEntry> candidate : sessions.entrySet()) {
      SessionEntry entry = candidate.getValue();
      boolean expired;
      synchronized (entry) {
        expired = isExpired(entry, now);
        if (expired) {
          entry.state = ArthasSessionState.EXPIRED;
          entry.endOfStream = true;
        }
      }
      if (expired && sessions.remove(candidate.getKey(), entry)) {
        removed.add(candidate.getKey());
      }
    }
    if (!removed.isEmpty()) {
      logger.log(Level.FINE, "[ARTHAS-SESSION] Cleaned expired sessions: {0}", removed);
    }
    return removed;
  }

  private SessionEntry requireEntry(String sessionId) {
    SessionEntry entry = sessions.get(sessionId);
    if (entry == null) {
      throw sessionFailure(
          ArthasTaskProtocol.ErrorCode.SESSION_NOT_FOUND, "Session not found: " + sessionId);
    }
    return entry;
  }

  private static void ensureNotClosed(SessionEntry entry) {
    if (entry.state == ArthasSessionState.CLOSED) {
      throw sessionFailure(
          ArthasTaskProtocol.ErrorCode.SESSION_ALREADY_CLOSED,
          "Session already closed: " + entry.sessionId);
    }
  }

  private void ensureNotExpired(SessionEntry entry) {
    if (isExpired(entry, System.currentTimeMillis())) {
      entry.state = ArthasSessionState.EXPIRED;
      entry.endOfStream = true;
      sessions.remove(entry.sessionId, entry);
      throw sessionFailure(resolveExpiredError(entry), "Session expired: " + entry.sessionId);
    }
  }

  private static String resolveExpiredError(SessionEntry entry) {
    long now = System.currentTimeMillis();
    if (entry.ttlMillis > 0 && now - entry.createdAtMillis >= entry.ttlMillis) {
      return ArthasTaskProtocol.ErrorCode.SESSION_TTL_EXCEEDED;
    }
    return ArthasTaskProtocol.ErrorCode.SESSION_IDLE_TIMEOUT;
  }

  private static boolean isExpired(SessionEntry entry, long now) {
    if (entry.ttlMillis > 0 && now - entry.createdAtMillis >= entry.ttlMillis) {
      return true;
    }
    return entry.idleTimeoutMillis > 0 && now - entry.lastAccessAtMillis >= entry.idleTimeoutMillis;
  }

  private static IllegalStateException sessionFailure(String errorCode, String message) {
    return new IllegalStateException(errorCode + ": " + message);
  }

  private static final class SessionEntry {
    private final String sessionId;
    private final String consumerId;
    private final long ttlMillis;
    private final long idleTimeoutMillis;
    private final long createdAtMillis;
    private long lastAccessAtMillis;
    private ArthasSessionState state = ArthasSessionState.OPEN;
    @Nullable private String currentCommand;
    @Nullable private Integer currentJobId;
    @Nullable private String currentJobStatus;
    private boolean endOfStream;

    private SessionEntry(
        String sessionId,
        String consumerId,
        long ttlMillis,
        long idleTimeoutMillis,
        long createdAtMillis,
        long lastAccessAtMillis) {
      this.sessionId = Objects.requireNonNull(sessionId, "sessionId");
      this.consumerId = Objects.requireNonNull(consumerId, "consumerId");
      this.ttlMillis = ttlMillis;
      this.idleTimeoutMillis = idleTimeoutMillis;
      this.createdAtMillis = createdAtMillis;
      this.lastAccessAtMillis = lastAccessAtMillis;
    }

    private void touch() {
      this.lastAccessAtMillis = System.currentTimeMillis();
    }

    private ArthasAsyncSessionSnapshot snapshot() {
      return ArthasAsyncSessionSnapshot.builder()
          .sessionId(sessionId)
          .consumerId(consumerId)
          .state(state)
          .createdAtMillis(createdAtMillis)
          .lastAccessAtMillis(lastAccessAtMillis)
          .ttlMillis(ttlMillis)
          .idleTimeoutMillis(idleTimeoutMillis)
          .currentCommand(currentCommand)
          .currentJobId(currentJobId)
          .currentJobStatus(currentJobStatus)
          .endOfStream(endOfStream)
          .build();
    }

  }
}
