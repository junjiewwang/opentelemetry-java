/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * METRIC 类型的动态增强 Advice 桥接
 *
 * <p>在方法入口记录起始时间，方法出口记录耗时（直方图）和调用次数（计数器）。
 *
 * <p>生成的指标：
 * <ul>
 *   <li>{@code dynamic.method.duration} — 方法耗时直方图（毫秒）</li>
 *   <li>{@code dynamic.method.invocations} — 方法调用次数计数器</li>
 * </ul>
 *
 * <p>属性：
 * <ul>
 *   <li>{@code code.namespace} — 类名</li>
 *   <li>{@code code.function} — 方法名</li>
 *   <li>{@code dynamic.instrumentation.rule_id} — 规则 ID</li>
 *   <li>{@code error} — 是否异常（"true" / "false"）</li>
 * </ul>
 */
final class DynamicMetricAdvice {

  private static final Logger logger =
      DynamicInstrumentLogger.getLogger("metric");

  private static final String INSTRUMENTATION_SCOPE = "io.opentelemetry.dynamic-instrumentation";

  private static final AttributeKey<String> ATTR_CLASS =
      AttributeKey.stringKey("code.namespace");
  private static final AttributeKey<String> ATTR_METHOD =
      AttributeKey.stringKey("code.function");
  private static final AttributeKey<String> ATTR_RULE_ID =
      AttributeKey.stringKey("dynamic.instrumentation.rule_id");
  private static final AttributeKey<String> ATTR_ERROR =
      AttributeKey.stringKey("error");

  /** ruleId -> MetricInstruments */
  private static final ConcurrentHashMap<String, MetricInstruments> INSTRUMENTS_REGISTRY =
      new ConcurrentHashMap<>();

  /** ruleId -> InstrumentationRule */
  private static final ConcurrentHashMap<String, InstrumentationRule> RULE_REGISTRY =
      new ConcurrentHashMap<>();

  private DynamicMetricAdvice() {}

  /**
   * 注册规则并创建 Metric Instruments
   *
   * @param rule 增强规则
   */
  static void registerRule(InstrumentationRule rule) {
    RULE_REGISTRY.put(rule.getRuleId(), rule);

    Meter meter = GlobalOpenTelemetry.getMeter(INSTRUMENTATION_SCOPE);

    String metricPrefix = rule.getConfig().getOrDefault("metric_prefix", "dynamic.method");

    LongHistogram durationHistogram = meter
        .histogramBuilder(metricPrefix + ".duration")
        .setDescription("Dynamic instrumented method duration")
        .setUnit("ms")
        .ofLongs()
        .build();

    LongCounter invocationCounter = meter
        .counterBuilder(metricPrefix + ".invocations")
        .setDescription("Dynamic instrumented method invocation count")
        .setUnit("{invocations}")
        .build();

    INSTRUMENTS_REGISTRY.put(rule.getRuleId(),
        new MetricInstruments(durationHistogram, invocationCounter));
  }

  /**
   * 注销规则
   *
   * @param ruleId 规则 ID
   */
  static void unregisterRule(String ruleId) {
    RULE_REGISTRY.remove(ruleId);
    INSTRUMENTS_REGISTRY.remove(ruleId);
  }

  /**
   * 方法入口回调：记录起始时间
   *
   * @param ruleId 规则 ID
   * @return 起始时间（纳秒），失败返回 null
   */
  @Nullable
  static Long onMethodEnter(String ruleId) {
    try {
      if (!INSTRUMENTS_REGISTRY.containsKey(ruleId)) {
        return null;
      }
      return System.nanoTime();
    } catch (RuntimeException e) {
      logger.log(Level.FINE,
          "[DYNAMIC-METRIC] Error in onMethodEnter for rule: " + ruleId, e);
      return null;
    }
  }

  /**
   * 方法出口回调：记录耗时和调用次数
   *
   * @param ruleId 规则 ID
   * @param startTimeNanos 起始时间（纳秒，可能为 null）
   * @param thrown 方法抛出的异常（可能为 null）
   */
  static void onMethodExit(
      String ruleId, @Nullable Long startTimeNanos, @Nullable Throwable thrown) {
    try {
      InstrumentationRule rule = RULE_REGISTRY.get(ruleId);
      MetricInstruments instruments = INSTRUMENTS_REGISTRY.get(ruleId);
      if (rule == null || instruments == null) {
        return;
      }

      Attributes attrs = Attributes.of(
          ATTR_CLASS, rule.getClassName(),
          ATTR_METHOD, rule.getMethodName(),
          ATTR_RULE_ID, ruleId,
          ATTR_ERROR, thrown != null ? "true" : "false");

      // 记录调用次数
      instruments.invocationCounter.add(1, attrs);

      // 记录耗时
      if (startTimeNanos != null) {
        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
        instruments.durationHistogram.record(durationMs, attrs);
      }
    } catch (RuntimeException e) {
      logger.log(Level.FINE,
          "[DYNAMIC-METRIC] Error in onMethodExit for rule: " + ruleId, e);
    }
  }

  /** Metric instruments 持有者 */
  private static final class MetricInstruments {
    final LongHistogram durationHistogram;
    final LongCounter invocationCounter;

    MetricInstruments(LongHistogram durationHistogram, LongCounter invocationCounter) {
      this.durationHistogram = durationHistogram;
      this.invocationCounter = invocationCounter;
    }
  }
}
