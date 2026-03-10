/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * LOG 类型的动态增强 Advice 桥接
 *
 * <p>在方法入口和出口记录日志，包含方法名、参数概要和执行耗时。
 *
 * <p>使用 {@link java.util.logging.Logger} 输出日志（JUL），
 * 在 OTel Java Agent 环境中，JUL 日志会自动桥接到 OTel LoggerProvider。
 *
 * <p>日志格式：
 * <ul>
 *   <li>入口：{@code [DYNAMIC-LOG] ENTER ClassName.methodName [ruleId]}</li>
 *   <li>出口：{@code [DYNAMIC-LOG] EXIT ClassName.methodName [ruleId] duration=Xms}</li>
 *   <li>异常：{@code [DYNAMIC-LOG] ERROR ClassName.methodName [ruleId] exception=...}</li>
 * </ul>
 */
final class DynamicLogAdvice {

  private static final Logger logger =
      DynamicInstrumentLogger.getLogger("log");

  /** ruleId -> InstrumentationRule */
  private static final ConcurrentHashMap<String, InstrumentationRule> RULE_REGISTRY =
      new ConcurrentHashMap<>();

  private DynamicLogAdvice() {}

  /**
   * 注册规则
   *
   * @param rule 增强规则
   */
  static void registerRule(InstrumentationRule rule) {
    RULE_REGISTRY.put(rule.getRuleId(), rule);
  }

  /**
   * 注销规则
   *
   * @param ruleId 规则 ID
   */
  static void unregisterRule(String ruleId) {
    RULE_REGISTRY.remove(ruleId);
  }

  /**
   * 方法入口回调：记录入口日志和起始时间
   *
   * @param ruleId 规则 ID
   * @return 起始时间（纳秒），失败返回 null
   */
  @Nullable
  static Long onMethodEnter(String ruleId) {
    try {
      InstrumentationRule rule = RULE_REGISTRY.get(ruleId);
      if (rule == null) {
        return null;
      }

      logger.log(Level.INFO,
          "[DYNAMIC-LOG] ENTER {0}.{1} [ruleId={2}]",
          new Object[] {rule.getClassName(), rule.getMethodName(), ruleId});

      return System.nanoTime();
    } catch (RuntimeException e) {
      // 日志增强不应影响业务方法
      return null;
    }
  }

  /**
   * 方法出口回调：记录出口日志
   *
   * @param ruleId 规则 ID
   * @param startTimeNanos 起始时间（纳秒，可能为 null）
   * @param thrown 方法抛出的异常（可能为 null）
   */
  static void onMethodExit(
      String ruleId, @Nullable Long startTimeNanos, @Nullable Throwable thrown) {
    try {
      InstrumentationRule rule = RULE_REGISTRY.get(ruleId);
      if (rule == null) {
        return;
      }

      long durationMs = 0;
      if (startTimeNanos != null) {
        durationMs = (System.nanoTime() - startTimeNanos) / 1_000_000;
      }

      if (thrown != null) {
        logger.log(Level.WARNING,
            "[DYNAMIC-LOG] ERROR {0}.{1} [ruleId={2}] duration={3}ms exception={4}",
            new Object[] {
              rule.getClassName(), rule.getMethodName(), ruleId,
              Long.valueOf(durationMs), thrown.getClass().getName() + ": " + thrown.getMessage()
            });
      } else {
        logger.log(Level.INFO,
            "[DYNAMIC-LOG] EXIT {0}.{1} [ruleId={2}] duration={3}ms",
            new Object[] {
              rule.getClassName(), rule.getMethodName(), ruleId, Long.valueOf(durationMs)
            });
      }
    } catch (RuntimeException e) {
      // 日志增强不应影响业务方法
    }
  }
}
