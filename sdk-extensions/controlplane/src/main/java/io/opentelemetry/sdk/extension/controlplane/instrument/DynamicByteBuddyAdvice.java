/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import javax.annotation.Nullable;
import net.bytebuddy.asm.Advice;

/**
 * ByteBuddy Advice 类：动态增强的字节码织入模板
 *
 * <p>此类定义了 ByteBuddy {@link Advice} 的入口和出口方法。ByteBuddy 在编译后
 * 会将这些方法的字节码<b>内联</b>（inline）到目标方法中，而非通过方法调用。
 *
 * <p>实际的增强逻辑委托给 {@link AdviceDispatcher}，它会根据增强类型
 * （TRACE / METRIC / LOG）分发到对应的 Advice 实现。
 *
 * <p><b>关键设计</b>：
 * <ul>
 *   <li>{@code @RuleId} 和 {@code @TypeValue} 是自定义绑定注解，
 *       通过 {@code Advice.withCustomMapping()} 在创建时注入固定值</li>
 *   <li>入口方法返回的上下文对象通过 {@code @Advice.Enter} 传递给出口方法</li>
 *   <li>异常通过 {@code @Advice.Thrown} 捕获但不抑制</li>
 *   <li>内联代码中添加 try-catch 并通过 {@link DynamicInstrumentLogger#logAdviceError}
 *       记录异常到独立日志文件，避免 {@code suppress} 吞掉异常后无法排查</li>
 * </ul>
 *
 * @see ByteBuddyTransformerFactory
 * @see AdviceDispatcher
 */
final class DynamicByteBuddyAdvice {

  private DynamicByteBuddyAdvice() {}

  /**
   * 方法入口 Advice
   *
   * <p>ByteBuddy 会将此方法的字节码内联到目标方法的最开始处。
   *
   * @param ruleId 规则 ID（编译时通过 @RuleId 绑定）
   * @param typeValue 增强类型值（编译时通过 @TypeValue 绑定）
   * @return 入口上下文，传递给出口方法
   */
  @Nullable
  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static Object onEnter(
      @RuleId String ruleId,
      @TypeValue String typeValue) {
    try {
      return AdviceDispatcher.onEnter(ruleId, typeValue);
    } catch (Throwable t) {
      // 此 catch 块会被内联到目标类中，即使 AdviceDispatcher 不可见导致 NoClassDefFoundError
      // 也能捕获。通过独立日志文件记录，避免 suppress 吞掉异常后无法排查。
      DynamicInstrumentLogger.logAdviceError("onEnter", ruleId, t);
      return null;
    }
  }

  /**
   * 方法出口 Advice
   *
   * <p>ByteBuddy 会将此方法的字节码内联到目标方法的所有出口处。
   *
   * @param ruleId 规则 ID（编译时通过 @RuleId 绑定）
   * @param typeValue 增强类型值（编译时通过 @TypeValue 绑定）
   * @param enterContext 方法入口返回的上下文
   * @param thrown 方法抛出的异常（不抑制）
   */
  @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
  public static void onExit(
      @RuleId String ruleId,
      @TypeValue String typeValue,
      @Advice.Enter Object enterContext,
      @Advice.Thrown Throwable thrown) {
    try {
      AdviceDispatcher.onExit(ruleId, typeValue, enterContext, thrown);
    } catch (Throwable t) {
      // 同 onEnter 的 catch 设计，确保 suppress 吞掉的异常可被诊断
      DynamicInstrumentLogger.logAdviceError("onExit", ruleId, t);
    }
  }
}
