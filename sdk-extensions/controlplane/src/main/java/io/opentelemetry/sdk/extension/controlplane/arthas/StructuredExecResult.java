/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Arthas 结构化同步执行结果。
 *
 * <p>用于承接桥接层返回的稳定数据，避免将 Arthas 内部类或反射细节泄漏到业务层。
 */
public final class StructuredExecResult {

  private final boolean success;
  private final boolean timeout;
  private final String command;
  @Nullable private final String sessionId;
  @Nullable private final String errorCode;
  @Nullable private final String errorMessage;
  private final Map<String, Object> payload;
  @Nullable private final String rawJson;
  private final long bridgeInitTimeMillis;
  private final long invokeTimeMillis;
  private final long serializationTimeMillis;

  private StructuredExecResult(Builder builder) {
    this.success = builder.success;
    this.timeout = builder.timeout;
    this.command = builder.command;
    this.sessionId = builder.sessionId;
    this.errorCode = builder.errorCode;
    this.errorMessage = builder.errorMessage;
    this.payload = Collections.unmodifiableMap(new LinkedHashMap<>(builder.payload));
    this.rawJson = builder.rawJson;
    this.bridgeInitTimeMillis = builder.bridgeInitTimeMillis;
    this.invokeTimeMillis = builder.invokeTimeMillis;
    this.serializationTimeMillis = builder.serializationTimeMillis;
  }

  public static Builder builder(String command) {
    return new Builder(command);
  }

  public boolean isSuccess() {
    return success;
  }

  public boolean isTimeout() {
    return timeout;
  }

  public String getCommand() {
    return command;
  }

  @Nullable
  public String getSessionId() {
    return sessionId;
  }

  @Nullable
  public String getErrorCode() {
    return errorCode;
  }

  @Nullable
  public String getErrorMessage() {
    return errorMessage;
  }

  public Map<String, Object> getPayload() {
    return payload;
  }

  @Nullable
  public String getRawJson() {
    return rawJson;
  }

  public long getBridgeInitTimeMillis() {
    return bridgeInitTimeMillis;
  }

  public long getInvokeTimeMillis() {
    return invokeTimeMillis;
  }

  public long getSerializationTimeMillis() {
    return serializationTimeMillis;
  }

  public static final class Builder {
    private final String command;
    private boolean success;
    private boolean timeout;
    @Nullable private String sessionId;
    @Nullable private String errorCode;
    @Nullable private String errorMessage;
    private Map<String, Object> payload = Collections.emptyMap();
    @Nullable private String rawJson;
    private long bridgeInitTimeMillis;
    private long invokeTimeMillis;
    private long serializationTimeMillis;

    private Builder(String command) {
      this.command = command;
    }

    public Builder success(boolean success) {
      this.success = success;
      return this;
    }

    public Builder timeout(boolean timeout) {
      this.timeout = timeout;
      return this;
    }

    public Builder sessionId(@Nullable String sessionId) {
      this.sessionId = sessionId;
      return this;
    }

    public Builder errorCode(@Nullable String errorCode) {
      this.errorCode = errorCode;
      return this;
    }

    public Builder errorMessage(@Nullable String errorMessage) {
      this.errorMessage = errorMessage;
      return this;
    }

    public Builder payload(Map<String, Object> payload) {
      this.payload = payload != null ? payload : Collections.emptyMap();
      return this;
    }

    public Builder rawJson(@Nullable String rawJson) {
      this.rawJson = rawJson;
      return this;
    }

    public Builder bridgeInitTimeMillis(long bridgeInitTimeMillis) {
      this.bridgeInitTimeMillis = bridgeInitTimeMillis;
      return this;
    }

    public Builder invokeTimeMillis(long invokeTimeMillis) {
      this.invokeTimeMillis = invokeTimeMillis;
      return this;
    }

    public Builder serializationTimeMillis(long serializationTimeMillis) {
      this.serializationTimeMillis = serializationTimeMillis;
      return this;
    }

    public StructuredExecResult build() {
      return new StructuredExecResult(this);
    }
  }
}
