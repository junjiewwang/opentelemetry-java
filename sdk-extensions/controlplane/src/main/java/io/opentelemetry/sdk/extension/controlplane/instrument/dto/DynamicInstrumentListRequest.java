/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument.dto;

import io.opentelemetry.sdk.extension.controlplane.instrument.EnhancementState;
import io.opentelemetry.sdk.extension.controlplane.instrument.InstrumentationType;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionContext;
import java.util.Locale;
import javax.annotation.Nullable;

/**
 * `dynamic_instrument_list` 任务入参模型。
 *
 * <p>统一封装查询过滤条件、分页参数以及参数校验逻辑，避免在执行器中散落字符串键和校验代码。
 */
public final class DynamicInstrumentListRequest {

  static final int DEFAULT_LIMIT = 100;
  static final int MAX_LIMIT = 500;

  private final String ruleId;
  private final String className;
  private final String methodName;
  @Nullable private final InstrumentationType type;
  @Nullable private final EnhancementState.Status status;
  private final boolean activeOnly;
  private final boolean includeConfig;
  private final int limit;
  private final int offset;

  private DynamicInstrumentListRequest(Builder builder) {
    this.ruleId = builder.ruleId;
    this.className = builder.className;
    this.methodName = builder.methodName;
    this.type = builder.type;
    this.status = builder.status;
    this.activeOnly = builder.activeOnly;
    this.includeConfig = builder.includeConfig;
    this.limit = builder.limit;
    this.offset = builder.offset;
  }

  public static DynamicInstrumentListRequest fromContext(TaskExecutionContext context) {
    String typeStr = trimToEmpty(context.getStringParameter("type", ""));
    InstrumentationType type = null;
    if (!typeStr.isEmpty()) {
      type = InstrumentationType.fromString(typeStr);
      if (type == null) {
        throw new IllegalArgumentException(
            "Unsupported instrumentation type: "
                + typeStr
                + ", supported: "
                + InstrumentationType.supportedValues());
      }
    }

    String statusStr = trimToEmpty(context.getStringParameter("status", ""));
    EnhancementState.Status status = null;
    if (!statusStr.isEmpty()) {
      try {
        status = EnhancementState.Status.valueOf(statusStr.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Unsupported status: "
                + statusStr
                + ", supported: pending, active, reverting, reverted, failed",
            e);
      }
    }

    int limit = context.getIntParameter("limit", DEFAULT_LIMIT);
    int offset = context.getIntParameter("offset", 0);
    if (limit <= 0 || limit > MAX_LIMIT) {
      throw new IllegalArgumentException(
          "Parameter 'limit' must be between 1 and " + MAX_LIMIT + ": " + limit);
    }
    if (offset < 0) {
      throw new IllegalArgumentException("Parameter 'offset' must be >= 0: " + offset);
    }

    return builder()
        .ruleId(trimToEmpty(context.getStringParameter("rule_id", "")))
        .className(trimToEmpty(context.getStringParameter("class_name", "")))
        .methodName(trimToEmpty(context.getStringParameter("method_name", "")))
        .type(type)
        .status(status)
        .activeOnly(context.getBooleanParameter("active_only", false))
        .includeConfig(context.getBooleanParameter("include_config", false))
        .limit(limit)
        .offset(offset)
        .build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getRuleId() {
    return ruleId;
  }

  public String getClassName() {
    return className;
  }

  public String getMethodName() {
    return methodName;
  }

  @Nullable
  public InstrumentationType getType() {
    return type;
  }

  @Nullable
  public EnhancementState.Status getStatus() {
    return status;
  }

  public boolean isActiveOnly() {
    return activeOnly;
  }

  public boolean isIncludeConfig() {
    return includeConfig;
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  @Override
  public String toString() {
    return "DynamicInstrumentListRequest{" 
        + "ruleId='" + ruleId + '\''
        + ", className='" + className + '\''
        + ", methodName='" + methodName + '\''
        + ", type=" + (type != null ? type.getValue() : "*")
        + ", status=" + (status != null ? status.name().toLowerCase(Locale.ROOT) : "*")
        + ", activeOnly=" + activeOnly
        + ", includeConfig=" + includeConfig
        + ", limit=" + limit
        + ", offset=" + offset
        + '}';
  }

  private static String trimToEmpty(@Nullable String value) {
    return value != null ? value.trim() : "";
  }

  public static final class Builder {
    private String ruleId = "";
    private String className = "";
    private String methodName = "";
    @Nullable private InstrumentationType type;
    @Nullable private EnhancementState.Status status;
    private boolean activeOnly;
    private boolean includeConfig;
    private int limit = DEFAULT_LIMIT;
    private int offset;

    private Builder() {}

    public Builder ruleId(String ruleId) {
      this.ruleId = ruleId != null ? ruleId : "";
      return this;
    }

    public Builder className(String className) {
      this.className = className != null ? className : "";
      return this;
    }

    public Builder methodName(String methodName) {
      this.methodName = methodName != null ? methodName : "";
      return this;
    }

    public Builder type(@Nullable InstrumentationType type) {
      this.type = type;
      return this;
    }

    public Builder status(@Nullable EnhancementState.Status status) {
      this.status = status;
      return this;
    }

    public Builder activeOnly(boolean activeOnly) {
      this.activeOnly = activeOnly;
      return this;
    }

    public Builder includeConfig(boolean includeConfig) {
      this.includeConfig = includeConfig;
      return this;
    }

    public Builder limit(int limit) {
      this.limit = limit;
      return this;
    }

    public Builder offset(int offset) {
      this.offset = offset;
      return this;
    }

    public DynamicInstrumentListRequest build() {
      return new DynamicInstrumentListRequest(this);
    }
  }
}
