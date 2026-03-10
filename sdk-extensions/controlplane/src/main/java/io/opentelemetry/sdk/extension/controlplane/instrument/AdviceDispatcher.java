/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;


/**
 * Advice 统一分发器
 *
 * <p>根据 {@link InstrumentationType} 将方法入口/出口的回调分发到对应的 Advice 实现：
 * <ul>
 *   <li>{@link InstrumentationType#TRACE} → {@link DynamicTraceAdvice}</li>
 *   <li>{@link InstrumentationType#METRIC} → {@link DynamicMetricAdvice}</li>
 *   <li>{@link InstrumentationType#LOG} → {@link DynamicLogAdvice}</li>
 * </ul>
 *
 * <p>作为 {@link DynamicClassFileTransformer} 织入的桥接方法的实际调用目标。
 * 字节码织入在目标方法的入口插入 {@code AdviceDispatcher.onEnter(ruleId, type)}，
 * 在出口插入 {@code AdviceDispatcher.onExit(ruleId, type, enterContext, thrown)}。
 */
// 使用 if-else 而非 switch(enum)，避免编译器为 switch 自动生成 synthetic $SwitchMap 内部类（$1），
// 该类不在 BootstrapClassInjector 注入列表中会导致运行时 NoClassDefFoundError。
@SuppressWarnings("UseEnumSwitch")
public final class AdviceDispatcher {

  private static final Logger logger =
      DynamicInstrumentLogger.getLogger();

  private AdviceDispatcher() {}

  /**
   * 注册规则到对应的 Advice
   *
   * @param rule 增强规则
   */
  static void registerRule(InstrumentationRule rule) {
    InstrumentationType type = rule.getType();
    if (type == InstrumentationType.TRACE) {
      DynamicTraceAdvice.registerRule(rule);
    } else if (type == InstrumentationType.METRIC) {
      DynamicMetricAdvice.registerRule(rule);
    } else if (type == InstrumentationType.LOG) {
      DynamicLogAdvice.registerRule(rule);
    }
  }

  /**
   * 注销规则
   *
   * @param ruleId 规则 ID
   * @param type 增强类型
   */
  static void unregisterRule(String ruleId, InstrumentationType type) {
    if (type == InstrumentationType.TRACE) {
      DynamicTraceAdvice.unregisterRule(ruleId);
    } else if (type == InstrumentationType.METRIC) {
      DynamicMetricAdvice.unregisterRule(ruleId);
    } else if (type == InstrumentationType.LOG) {
      DynamicLogAdvice.unregisterRule(ruleId);
    }
  }

  /**
   * 方法入口分发
   *
   * <p>此方法会被织入到目标方法的最开始处。
   *
   * @param ruleId 规则 ID
   * @param typeValue 增强类型字符串值
   * @return 入口上下文（传递给 onExit 使用），各类型含义不同
   */
  @Nullable
  public static Object onEnter(String ruleId, String typeValue) {
    try {
      InstrumentationType type = InstrumentationType.fromString(typeValue);
      if (type == null) {
        return null;
      }
      if (type == InstrumentationType.TRACE) {
        return DynamicTraceAdvice.onMethodEnter(ruleId);
      } else if (type == InstrumentationType.METRIC) {
        return DynamicMetricAdvice.onMethodEnter(ruleId);
      } else if (type == InstrumentationType.LOG) {
        return DynamicLogAdvice.onMethodEnter(ruleId);
      }
      return null;
    } catch (RuntimeException e) {
      logger.log(Level.WARNING,
          "[ADVICE-DISPATCHER] Error in onEnter for rule: " + ruleId, e);
      return null;
    }
  }

  /**
   * 方法出口分发
   *
   * <p>此方法会被织入到目标方法的所有出口处（包括正常返回和异常抛出）。
   *
   * @param ruleId 规则 ID
   * @param typeValue 增强类型字符串值
   * @param enterContext 方法入口返回的上下文
   * @param thrown 方法抛出的异常（可能为 null）
   */
  public static void onExit(
      String ruleId, String typeValue,
      @Nullable Object enterContext, @Nullable Throwable thrown) {
    try {
      InstrumentationType type = InstrumentationType.fromString(typeValue);
      if (type == null) {
        return;
      }
      if (type == InstrumentationType.TRACE) {
        DynamicTraceAdvice.onMethodExit(
            enterContext instanceof Object[] ? (Object[]) enterContext : null,
            thrown);
      } else if (type == InstrumentationType.METRIC) {
        DynamicMetricAdvice.onMethodExit(
            ruleId,
            enterContext instanceof Long ? (Long) enterContext : null,
            thrown);
      } else if (type == InstrumentationType.LOG) {
        DynamicLogAdvice.onMethodExit(
            ruleId,
            enterContext instanceof Long ? (Long) enterContext : null,
            thrown);
      }
    } catch (RuntimeException e) {
      logger.log(Level.WARNING,
          "[ADVICE-DISPATCHER] Error in onExit for rule: " + ruleId, e);
    }
  }
}
