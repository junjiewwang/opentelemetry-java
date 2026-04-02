package io.opentelemetry.sdk.extension.controlplane.arthas;

import javax.annotation.Nullable;

/** Arthas 异步会话快照。 */
public final class ArthasAsyncSessionSnapshot {

  private final String sessionId;
  private final String consumerId;
  private final ArthasSessionState state;
  private final long createdAtMillis;
  private final long lastAccessAtMillis;
  private final long ttlMillis;
  private final long idleTimeoutMillis;
  @Nullable private final String currentCommand;
  @Nullable private final Integer currentJobId;
  @Nullable private final String currentJobStatus;
  private final boolean endOfStream;

  private ArthasAsyncSessionSnapshot(Builder builder) {
    this.sessionId = builder.sessionId;
    this.consumerId = builder.consumerId;
    this.state = builder.state;
    this.createdAtMillis = builder.createdAtMillis;
    this.lastAccessAtMillis = builder.lastAccessAtMillis;
    this.ttlMillis = builder.ttlMillis;
    this.idleTimeoutMillis = builder.idleTimeoutMillis;
    this.currentCommand = builder.currentCommand;
    this.currentJobId = builder.currentJobId;
    this.currentJobStatus = builder.currentJobStatus;
    this.endOfStream = builder.endOfStream;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getConsumerId() {
    return consumerId;
  }

  public ArthasSessionState getState() {
    return state;
  }

  public long getCreatedAtMillis() {
    return createdAtMillis;
  }

  public long getLastAccessAtMillis() {
    return lastAccessAtMillis;
  }

  public long getTtlMillis() {
    return ttlMillis;
  }

  public long getIdleTimeoutMillis() {
    return idleTimeoutMillis;
  }

  @Nullable
  public String getCurrentCommand() {
    return currentCommand;
  }

  @Nullable
  public Integer getCurrentJobId() {
    return currentJobId;
  }

  @Nullable
  public String getCurrentJobStatus() {
    return currentJobStatus;
  }

  public boolean isEndOfStream() {
    return endOfStream;
  }

  public static final class Builder {
    private String sessionId = "";
    private String consumerId = "";
    private ArthasSessionState state = ArthasSessionState.OPEN;
    private long createdAtMillis;
    private long lastAccessAtMillis;
    private long ttlMillis;
    private long idleTimeoutMillis;
    @Nullable private String currentCommand;
    @Nullable private Integer currentJobId;
    @Nullable private String currentJobStatus;
    private boolean endOfStream;

    public Builder sessionId(String sessionId) {
      this.sessionId = sessionId;
      return this;
    }

    public Builder consumerId(String consumerId) {
      this.consumerId = consumerId;
      return this;
    }

    public Builder state(ArthasSessionState state) {
      this.state = state;
      return this;
    }

    public Builder createdAtMillis(long createdAtMillis) {
      this.createdAtMillis = createdAtMillis;
      return this;
    }

    public Builder lastAccessAtMillis(long lastAccessAtMillis) {
      this.lastAccessAtMillis = lastAccessAtMillis;
      return this;
    }

    public Builder ttlMillis(long ttlMillis) {
      this.ttlMillis = ttlMillis;
      return this;
    }

    public Builder idleTimeoutMillis(long idleTimeoutMillis) {
      this.idleTimeoutMillis = idleTimeoutMillis;
      return this;
    }

    public Builder currentCommand(@Nullable String currentCommand) {
      this.currentCommand = currentCommand;
      return this;
    }

    public Builder currentJobId(@Nullable Integer currentJobId) {
      this.currentJobId = currentJobId;
      return this;
    }

    public Builder currentJobStatus(@Nullable String currentJobStatus) {
      this.currentJobStatus = currentJobStatus;
      return this;
    }

    public Builder endOfStream(boolean endOfStream) {
      this.endOfStream = endOfStream;
      return this;
    }

    public ArthasAsyncSessionSnapshot build() {
      return new ArthasAsyncSessionSnapshot(this);
    }
  }
}
