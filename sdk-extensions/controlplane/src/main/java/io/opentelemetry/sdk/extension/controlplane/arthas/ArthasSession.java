/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Arthas 会话信息
 *
 * <p>封装单个 Arthas 终端会话的信息，包括会话 ID、用户、创建时间、终端尺寸等。
 */
public final class ArthasSession {

  private final String sessionId;
  @Nullable private final String arthasSessionId;
  @Nullable private final String userId;
  private final Instant createdAt;
  private volatile Instant lastActiveAt;
  private final int cols;
  private final int rows;

  private ArthasSession(Builder builder) {
    this.sessionId = builder.sessionId;
    this.arthasSessionId = builder.arthasSessionId;
    this.userId = builder.userId;
    this.createdAt = builder.createdAt;
    this.lastActiveAt = builder.createdAt;
    this.cols = builder.cols;
    this.rows = builder.rows;
  }

  /**
   * 创建构建器
   *
   * @return 新的构建器实例
   */
  public static Builder builder() {
    return new Builder();
  }

  /** 获取会话 ID（Tunnel 层面的会话标识） */
  public String getSessionId() {
    return sessionId;
  }

  /** 获取 Arthas 原生会话 ID */
  @Nullable
  public String getArthasSessionId() {
    return arthasSessionId;
  }

  /** 获取用户标识 */
  @Nullable
  public String getUserId() {
    return userId;
  }

  /** 获取会话创建时间 */
  public Instant getCreatedAt() {
    return createdAt;
  }

  /** 获取最后活跃时间 */
  public Instant getLastActiveAt() {
    return lastActiveAt;
  }

  /** 获取终端列数 */
  public int getCols() {
    return cols;
  }

  /** 获取终端行数 */
  public int getRows() {
    return rows;
  }

  /** 更新活跃时间 */
  public void markActive() {
    this.lastActiveAt = Instant.now();
  }

  /**
   * 检查会话是否空闲超时
   *
   * @param idleTimeoutMillis 空闲超时时间（毫秒）
   * @return 如果空闲超时返回 true
   */
  public boolean isIdleTimeout(long idleTimeoutMillis) {
    return System.currentTimeMillis() - lastActiveAt.toEpochMilli() > idleTimeoutMillis;
  }

  /**
   * 检查会话是否超过最大存活时间
   *
   * @param maxDurationMillis 最大存活时间（毫秒）
   * @return 如果超过最大存活时间返回 true
   */
  public boolean isExpired(long maxDurationMillis) {
    return System.currentTimeMillis() - createdAt.toEpochMilli() > maxDurationMillis;
  }

  /**
   * 获取会话已存活时间（毫秒）
   *
   * @return 存活时间
   */
  public long getAgeMillis() {
    return System.currentTimeMillis() - createdAt.toEpochMilli();
  }

  /**
   * 获取空闲时间（毫秒）
   *
   * @return 空闲时间
   */
  public long getIdleMillis() {
    return System.currentTimeMillis() - lastActiveAt.toEpochMilli();
  }

  @Override
  public String toString() {
    return "ArthasSession{"
        + "sessionId='"
        + sessionId
        + '\''
        + ", userId='"
        + userId
        + '\''
        + ", createdAt="
        + createdAt
        + ", lastActiveAt="
        + lastActiveAt
        + ", cols="
        + cols
        + ", rows="
        + rows
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ArthasSession that = (ArthasSession) o;
    return Objects.equals(sessionId, that.sessionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sessionId);
  }

  /** Builder for {@link ArthasSession}. */
  public static final class Builder {
    private String sessionId = UUID.randomUUID().toString();
    @Nullable private String arthasSessionId;
    @Nullable private String userId;
    private Instant createdAt = Instant.now();
    private int cols = 120;
    private int rows = 40;

    private Builder() {}

    public Builder setSessionId(String sessionId) {
      this.sessionId = Objects.requireNonNull(sessionId, "sessionId");
      return this;
    }

    public Builder setArthasSessionId(@Nullable String arthasSessionId) {
      this.arthasSessionId = arthasSessionId;
      return this;
    }

    public Builder setUserId(@Nullable String userId) {
      this.userId = userId;
      return this;
    }

    public Builder setCreatedAt(Instant createdAt) {
      this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
      return this;
    }

    public Builder setCols(int cols) {
      if (cols < 1) {
        throw new IllegalArgumentException("cols must be >= 1");
      }
      this.cols = cols;
      return this;
    }

    public Builder setRows(int rows) {
      if (rows < 1) {
        throw new IllegalArgumentException("rows must be >= 1");
      }
      this.rows = rows;
      return this;
    }

    /**
     * 构建会话实例
     *
     * @return 会话实例
     */
    public ArthasSession build() {
      return new ArthasSession(this);
    }
  }
}
