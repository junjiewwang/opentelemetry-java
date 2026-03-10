/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * TRACE 类型的动态增强 Advice 桥接
 *
 * <p>在方法入口创建 OTel {@link Span}，在方法出口结束 Span。
 * 支持自动异常记录和状态标记。
 *
 * <p>设计为<b>静态方法容器</b>，通过 {@code InstrumentationRule.getRuleId()} 关联的
 * 配置信息（spanName 等）从全局注册表中查找。
 *
 * <p>线程安全：所有方法均为静态方法，使用 {@link ThreadLocal} 和线程安全的 OTel API。
 *
 * <p><b>注意</b>：此类不使用 ByteBuddy {@code @Advice} 注解，而是作为
 * {@link DynamicClassFileTransformer} 的回调委托，在 transform 方法中被直接调用。
 */
final class DynamicTraceAdvice {

  private static final Logger logger =
      DynamicInstrumentLogger.getLogger("trace");

  /** 动态增强 Tracer 的 instrumentation scope 名称 */
  private static final String INSTRUMENTATION_SCOPE = "io.opentelemetry.dynamic-instrumentation";

  /** 方法名属性键 */
  private static final AttributeKey<String> ATTR_METHOD =
      AttributeKey.stringKey("code.function");

  /** 类名属性键 */
  private static final AttributeKey<String> ATTR_CLASS =
      AttributeKey.stringKey("code.namespace");

  /** ruleId 属性键 */
  private static final AttributeKey<String> ATTR_RULE_ID =
      AttributeKey.stringKey("dynamic.instrumentation.rule_id");

  /** 规则配置注册表：ruleId -> InstrumentationRule */
  private static final ConcurrentHashMap<String, InstrumentationRule> RULE_REGISTRY =
      new ConcurrentHashMap<>();

  private DynamicTraceAdvice() {}

  /**
   * 注册规则配置
   *
   * @param rule 增强规则
   */
  static void registerRule(InstrumentationRule rule) {
    RULE_REGISTRY.put(rule.getRuleId(), rule);
  }

  /**
   * 注销规则配置
   *
   * @param ruleId 规则 ID
   */
  static void unregisterRule(String ruleId) {
    RULE_REGISTRY.remove(ruleId);
  }

  /**
   * 方法入口回调：创建并激活 Span
   *
   * @param ruleId 规则 ID
   * @return 用于方法出口的上下文数组：[Span, Scope, startTimeNanos]，失败返回 null
   */
  @Nullable
  static Object[] onMethodEnter(String ruleId) {
    try {
      InstrumentationRule rule = RULE_REGISTRY.get(ruleId);
      if (rule == null) {
        return null;
      }

      Tracer tracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_SCOPE);
      String spanName = rule.getEffectiveSpanName();

      Span span = tracer.spanBuilder(spanName)
          .setAttribute(ATTR_CLASS, rule.getClassName())
          .setAttribute(ATTR_METHOD, rule.getMethodName())
          .setAttribute(ATTR_RULE_ID, ruleId)
          .startSpan();

      @SuppressWarnings("MustBeClosedChecker")
      Scope scope = span.makeCurrent();

      return new Object[] {span, scope, Long.valueOf(System.nanoTime())};
    } catch (RuntimeException e) {
      logger.log(Level.FINE,
          "[DYNAMIC-TRACE] Error in onMethodEnter for rule: " + ruleId, e);
      return null;
    }
  }

  /**
   * 方法出口回调：结束 Span
   *
   * @param enterContext 方法入口返回的上下文（可能为 null）
   * @param thrown 方法抛出的异常（可能为 null）
   */
  static void onMethodExit(@Nullable Object[] enterContext, @Nullable Throwable thrown) {
    if (enterContext == null) {
      return;
    }
    try {
      Span span = (Span) enterContext[0];
      Scope scope = (Scope) enterContext[1];

      if (thrown != null) {
        String errorMsg = thrown.getMessage() != null ? thrown.getMessage() : thrown.getClass().getName();
        span.setStatus(StatusCode.ERROR, errorMsg);
        span.recordException(thrown);
      }

      scope.close();
      span.end();
    } catch (RuntimeException e) {
      logger.log(Level.FINE,
          "[DYNAMIC-TRACE] Error in onMethodExit", e);
    }
  }

  /**
   * 获取已注册的规则数量（用于测试和诊断）
   *
   * @return 已注册规则数量
   */
  static int getRegisteredRuleCount() {
    return RULE_REGISTRY.size();
  }
}
