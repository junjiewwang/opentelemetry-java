package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;

/** Arthas 异步结构化结果。 */
public final class StructuredAsyncResult {

  private final boolean success;
  @Nullable private final String sessionId;
  @Nullable private final String consumerId;
  @Nullable private final String errorCode;
  @Nullable private final String errorMessage;
  private final Map<String, Object> payload;
  @Nullable private final String rawJson;
  private final long bridgeInitTimeMillis;
  private final long invokeTimeMillis;
  private final long serializationTimeMillis;

  private StructuredAsyncResult(Builder builder) {
    this.success = builder.success;
    this.sessionId = builder.sessionId;
    this.consumerId = builder.consumerId;
    this.errorCode = builder.errorCode;
    this.errorMessage = builder.errorMessage;
    this.payload = Collections.unmodifiableMap(new LinkedHashMap<>(builder.payload));
    this.rawJson = builder.rawJson;
    this.bridgeInitTimeMillis = builder.bridgeInitTimeMillis;
    this.invokeTimeMillis = builder.invokeTimeMillis;
    this.serializationTimeMillis = builder.serializationTimeMillis;
  }

  public static Builder builder() {
    return new Builder();
  }

  public boolean isSuccess() {
    return success;
  }

  @Nullable
  public String getSessionId() {
    return sessionId;
  }

  @Nullable
  public String getConsumerId() {
    return consumerId;
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
    private boolean success;
    @Nullable private String sessionId;
    @Nullable private String consumerId;
    @Nullable private String errorCode;
    @Nullable private String errorMessage;
    private Map<String, Object> payload = Collections.emptyMap();
    @Nullable private String rawJson;
    private long bridgeInitTimeMillis;
    private long invokeTimeMillis;
    private long serializationTimeMillis;

    public Builder success(boolean success) {
      this.success = success;
      return this;
    }

    public Builder sessionId(@Nullable String sessionId) {
      this.sessionId = sessionId;
      return this;
    }

    public Builder consumerId(@Nullable String consumerId) {
      this.consumerId = consumerId;
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

    public StructuredAsyncResult build() {
      return new StructuredAsyncResult(this);
    }
  }
}
