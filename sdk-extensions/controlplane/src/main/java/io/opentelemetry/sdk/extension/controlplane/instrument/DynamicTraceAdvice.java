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

  /** 预解析的 CaptureConfig 缓存：ruleId -> CaptureConfig（在增强阶段通过反射预解析，运行时零开销） */
  private static final ConcurrentHashMap<String, CaptureConfig> CAPTURE_CONFIG_CACHE =
      new ConcurrentHashMap<>();

  private DynamicTraceAdvice() {}

  /**
   * 注册规则配置及其预解析的 CaptureConfig
   *
   * <p>统一注册入口。在增强阶段（非热路径）通过反射获取目标 Method 后预解析
   * CaptureConfig，运行时直接从缓存中取用，避免每次方法调用都重新 parse。
   * 无采集配置时 captureConfig 传 null 即可。
   *
   * @param rule 增强规则
   * @param captureConfig 预解析的采集配置（可以为 null）
   */
  static void registerRule(InstrumentationRule rule, @Nullable CaptureConfig captureConfig) {
    RULE_REGISTRY.put(rule.getRuleId(), rule);
    if (captureConfig != null && captureConfig.hasCaptureConfig()) {
      CAPTURE_CONFIG_CACHE.put(rule.getRuleId(), captureConfig);
    }
  }

  /**
   * 注销规则配置
   *
   * @param ruleId 规则 ID
   */
  static void unregisterRule(String ruleId) {
    RULE_REGISTRY.remove(ruleId);
    CAPTURE_CONFIG_CACHE.remove(ruleId);
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
   * 方法入口回调（带参数采集）：创建并激活 Span，按配置采集方法参数
   *
   * <p>在 {@link #onMethodEnter(String)} 的基础上，额外将配置指定的方法参数
   * 设置为 Span Attribute。参数采集按 {@link CaptureConfig} 中预解析的索引和名称执行。
   *
   * @param ruleId 规则 ID
   * @param args 方法所有参数（来自 @AllArguments）
   * @return 用于方法出口的上下文数组：[Span, Scope, startTimeNanos, ruleId]，失败返回 null
   */
  @Nullable
  static Object[] onMethodEnterWithCapture(String ruleId, @Nullable Object[] args) {
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

      // 按配置采集方法参数（优先使用预解析的缓存，避免运行时重复 parse）
      if (args != null) {
        CaptureConfig captureConfig = CAPTURE_CONFIG_CACHE.get(ruleId);
        if (captureConfig == null) {
          captureConfig = CaptureConfig.parse(rule.getConfig());
        }
        int[] argIndices = captureConfig.getArgIndices();
        for (int i = 0; i < argIndices.length; i++) {
          int idx = argIndices[i];
          if (idx < args.length) {
            String keyName = captureConfig.getArgKeyName(i);
            String value = CaptureProcessor.safeToString(args[idx], captureConfig.getMaxLength());
            span.setAttribute(
                AttributeKey.stringKey(CaptureProcessor.ATTR_PREFIX_ARGS + keyName), value);
          }
        }
      }

      @SuppressWarnings("MustBeClosedChecker")
      Scope scope = span.makeCurrent();

      // 上下文中额外携带 ruleId，供 exit 阶段查找 CaptureConfig
      return new Object[] {span, scope, Long.valueOf(System.nanoTime()), ruleId};
    } catch (RuntimeException e) {
      logger.log(Level.FINE,
          "[DYNAMIC-TRACE] Error in onMethodEnterWithCapture for rule: " + ruleId, e);
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
   * 方法出口回调（带返回值采集）：结束 Span，按配置采集返回值
   *
   * <p>在 {@link #onMethodExit(Object[], Throwable)} 的基础上，额外将配置指定的返回值
   * 设置为 Span Attribute。支持返回值的 toString() 和指定字段提取。
   *
   * @param enterContext 方法入口返回的上下文（可能为 null）
   * @param thrown 方法抛出的异常（可能为 null）
   * @param returnValue 方法返回值（可能为 null）
   */
  static void onMethodExitWithCapture(
      @Nullable Object[] enterContext, @Nullable Throwable thrown,
      @Nullable Object returnValue) {
    if (enterContext == null) {
      return;
    }
    try {
      Span span = (Span) enterContext[0];
      Scope scope = (Scope) enterContext[1];

      // 采集返回值（优先使用预解析的缓存，避免运行时重复 parse）
      if (enterContext.length > 3 && enterContext[3] instanceof String) {
        String ruleId = (String) enterContext[3];
        InstrumentationRule rule = RULE_REGISTRY.get(ruleId);
        if (rule != null && returnValue != null) {
          CaptureConfig captureConfig = CAPTURE_CONFIG_CACHE.get(ruleId);
          if (captureConfig == null) {
            captureConfig = CaptureConfig.parse(rule.getConfig());
          }
          if (captureConfig.isCaptureReturn()) {
            String[] returnFields = captureConfig.getReturnFields();
            if (returnFields.length == 0) {
              // "*" 模式：仅采集返回值 toString()
              span.setAttribute(
                  AttributeKey.stringKey(CaptureProcessor.ATTR_PREFIX_RETURN),
                  CaptureProcessor.safeToString(returnValue, captureConfig.getMaxLength()));
            } else {
              // 指定字段模式：仅提取指定字段，不采集 toString()
              for (String field : returnFields) {
                Object fieldValue = CaptureProcessor.extractField(returnValue, field);
                if (fieldValue != null) {
                  span.setAttribute(
                      AttributeKey.stringKey(CaptureProcessor.ATTR_PREFIX_RETURN + "." + field),
                      CaptureProcessor.safeToString(fieldValue, captureConfig.getMaxLength()));
                }
              }
            }
          }
        }
      }

      if (thrown != null) {
        String errorMsg = thrown.getMessage() != null
            ? thrown.getMessage() : thrown.getClass().getName();
        span.setStatus(StatusCode.ERROR, errorMsg);
        span.recordException(thrown);
      }

      scope.close();
      span.end();
    } catch (RuntimeException e) {
      logger.log(Level.FINE,
          "[DYNAMIC-TRACE] Error in onMethodExitWithCapture", e);
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
