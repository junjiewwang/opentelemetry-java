/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.opentelemetry.sdk.extension.controlplane.core.InstrumentationSnapshot;
import io.opentelemetry.sdk.extension.controlplane.instrument.EnhancementState;
import io.opentelemetry.sdk.extension.controlplane.instrument.EnhancementStateRegistry;
import io.opentelemetry.sdk.extension.controlplane.instrument.InstrumentationRule;
import io.opentelemetry.sdk.extension.controlplane.instrument.TransformerManager;
import io.opentelemetry.sdk.extension.controlplane.util.JsonUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;

/**
 * `dynamic_instrument_list` 任务出参模型。
 *
 * <p>统一管理返回字段定义和序列化逻辑，消除执行器中的散落 `Map` 拼装逻辑。
 */
@SuppressWarnings("UnusedVariable")
public final class DynamicInstrumentListResponse {

  @JsonProperty("summary")
  private final Summary summary;

  @JsonProperty("items")
  private final List<Item> items;

  @JsonProperty("paging")
  private final Paging paging;

  private DynamicInstrumentListResponse(Summary summary, List<Item> items, Paging paging) {
    this.summary = summary;
    this.items = Collections.unmodifiableList(new ArrayList<>(items));
    this.paging = paging;
  }

  public static DynamicInstrumentListResponse of(Summary summary, List<Item> items, Paging paging) {
    return new DynamicInstrumentListResponse(summary, items, paging);
  }

  public String toJson() {
    return JsonUtils.toJsonString(this);
  }

  public static Summary buildSummary(
      List<EnhancementState> matchedStates,
      EnhancementStateRegistry stateRegistry,
      TransformerManager transformerManager,
      InstrumentationSnapshot snapshot) {
    int pending = 0;
    int active = 0;
    int reverting = 0;
    int reverted = 0;
    int failed = 0;
    int effective = 0;

    for (EnhancementState state : matchedStates) {
      switch (state.getStatus()) {
        case PENDING:
          pending++;
          break;
        case ACTIVE:
          active++;
          break;
        case REVERTING:
          reverting++;
          break;
        case REVERTED:
          reverted++;
          break;
        case FAILED:
          failed++;
          break;
      }
      if (Item.isEffective(state, transformerManager.isApplied(state.getRuleId()))) {
        effective++;
      }
    }

    return new Summary(
        stateRegistry.size(),
        matchedStates.size(),
        pending,
        active,
        reverting,
        reverted,
        failed,
        effective,
        transformerManager.getActiveCount(),
        snapshot.isAvailable(),
        snapshot.hasEnhancementCapability(),
        snapshot.supportsRetransform(),
        snapshot.supportsRedefine(),
        snapshot.getSource().name().toLowerCase(Locale.ROOT),
        snapshot.getDiagnosticMessage());
  }

  public static Paging buildPaging(DynamicInstrumentListRequest request, int total, int returned) {
    return new Paging(
        request.getOffset(),
        request.getLimit(),
        returned,
        request.getOffset() + returned < total);
  }

  public static Item buildItem(
      EnhancementState state, boolean includeConfig, TransformerManager transformerManager) {
    return Item.from(state, includeConfig, transformerManager);
  }

  @SuppressWarnings("UnusedVariable")
  public static final class Summary {

    @JsonProperty("registered_total")
    private final int registeredTotal;

    @JsonProperty("total")
    private final int total;

    @JsonProperty("pending")
    private final int pending;

    @JsonProperty("active")
    private final int active;

    @JsonProperty("reverting")
    private final int reverting;

    @JsonProperty("reverted")
    private final int reverted;

    @JsonProperty("failed")
    private final int failed;

    @JsonProperty("effective")
    private final int effective;

    @JsonProperty("active_transformer_count")
    private final int activeTransformerCount;

    @JsonProperty("instrumentation_available")
    private final boolean instrumentationAvailable;

    @JsonProperty("enhancement_capability")
    private final boolean enhancementCapability;

    @JsonProperty("supports_retransform")
    private final boolean supportsRetransform;

    @JsonProperty("supports_redefine")
    private final boolean supportsRedefine;

    @JsonProperty("instrumentation_source")
    private final String instrumentationSource;

    @JsonProperty("diagnostic_message")
    @Nullable
    private final String diagnosticMessage;

    private Summary(
        int registeredTotal,
        int total,
        int pending,
        int active,
        int reverting,
        int reverted,
        int failed,
        int effective,
        int activeTransformerCount,
        boolean instrumentationAvailable,
        boolean enhancementCapability,
        boolean supportsRetransform,
        boolean supportsRedefine,
        String instrumentationSource,
        @Nullable String diagnosticMessage) {
      this.registeredTotal = registeredTotal;
      this.total = total;
      this.pending = pending;
      this.active = active;
      this.reverting = reverting;
      this.reverted = reverted;
      this.failed = failed;
      this.effective = effective;
      this.activeTransformerCount = activeTransformerCount;
      this.instrumentationAvailable = instrumentationAvailable;
      this.enhancementCapability = enhancementCapability;
      this.supportsRetransform = supportsRetransform;
      this.supportsRedefine = supportsRedefine;
      this.instrumentationSource = instrumentationSource;
      this.diagnosticMessage = diagnosticMessage;
    }
  }

  @SuppressWarnings("UnusedVariable")
  public static final class Item {

    @JsonProperty("rule_id")
    private final String ruleId;

    @JsonProperty("class_name")
    private final String className;

    @JsonProperty("method_name")
    private final String methodName;

    @JsonProperty("method_descriptor")
    @Nullable
    private final String methodDescriptor;

    @JsonProperty("parameter_types")
    private final List<String> parameterTypes;

    @JsonProperty("type")
    private final String type;

    @JsonProperty("span_name")
    private final String spanName;

    @JsonProperty("status")
    private final String status;

    @JsonProperty("runtime_status")
    private final String runtimeStatus;

    @JsonProperty("is_applied")
    private final boolean applied;

    @JsonProperty("is_effective")
    private final boolean effective;

    @JsonProperty("created_at_millis")
    private final long createdAtMillis;

    @JsonProperty("activated_at_millis")
    private final long activatedAtMillis;

    @JsonProperty("reverted_at_millis")
    private final long revertedAtMillis;

    @JsonProperty("error_message")
    @Nullable
    private final String errorMessage;

    @JsonProperty("enhanced_class_name")
    @Nullable
    private final String enhancedClassName;

    @JsonProperty("config")
    @Nullable
    private final Object config;

    private Item(
        String ruleId,
        String className,
        String methodName,
        @Nullable String methodDescriptor,
        List<String> parameterTypes,
        String type,
        String spanName,
        String status,
        String runtimeStatus,
        boolean applied,
        boolean effective,
        long createdAtMillis,
        long activatedAtMillis,
        long revertedAtMillis,
        @Nullable String errorMessage,
        @Nullable String enhancedClassName,
        @Nullable Object config) {
      this.ruleId = ruleId;
      this.className = className;
      this.methodName = methodName;
      this.methodDescriptor = methodDescriptor;
      this.parameterTypes = Collections.unmodifiableList(new ArrayList<>(parameterTypes));
      this.type = type;
      this.spanName = spanName;
      this.status = status;
      this.runtimeStatus = runtimeStatus;
      this.applied = applied;
      this.effective = effective;
      this.createdAtMillis = createdAtMillis;
      this.activatedAtMillis = activatedAtMillis;
      this.revertedAtMillis = revertedAtMillis;
      this.errorMessage = errorMessage;
      this.enhancedClassName = enhancedClassName;
      this.config = config;
    }

    static Item from(
        EnhancementState state,
        boolean includeConfig,
        TransformerManager transformerManager) {
      InstrumentationRule rule = state.getRule();
      boolean applied = transformerManager.isApplied(rule.getRuleId());
      List<String> parameterTypes =
          rule.getParameterTypes() != null ? rule.getParameterTypes() : Collections.emptyList();
      return new Item(
          rule.getRuleId(),
          rule.getClassName(),
          rule.getMethodName(),
          rule.getMethodDescriptor(),
          parameterTypes,
          rule.getType().getValue(),
          rule.getEffectiveSpanName(),
          normalizeStatus(state.getStatus()),
          normalizeStatus(state.getStatus()),
          applied,
          isEffective(state, applied),
          state.getCreatedAtMillis(),
          state.getActivatedAtMillis(),
          state.getRevertedAtMillis(),
          state.getErrorMessage(),
          state.getEnhancedClassName(),
          includeConfig ? rule.getConfig() : null);
    }

    private static boolean isEffective(EnhancementState state, boolean applied) {
      return applied && state.getStatus() == EnhancementState.Status.ACTIVE;
    }

    private static String normalizeStatus(EnhancementState.Status status) {
      return status.name().toLowerCase(Locale.ROOT);
    }
  }

  @SuppressWarnings("UnusedVariable")
  public static final class Paging {

    @JsonProperty("offset")
    private final int offset;

    @JsonProperty("limit")
    private final int limit;

    @JsonProperty("returned")
    private final int returned;

    @JsonProperty("has_more")
    private final boolean hasMore;

    private Paging(int offset, int limit, int returned, boolean hasMore) {
      this.offset = offset;
      this.limit = limit;
      this.returned = returned;
      this.hasMore = hasMore;
    }
  }
}
