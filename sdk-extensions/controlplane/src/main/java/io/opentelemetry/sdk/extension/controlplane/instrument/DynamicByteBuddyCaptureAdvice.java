/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import javax.annotation.Nullable;
import net.bytebuddy.asm.Advice;

/**
 * ByteBuddy Advice 类：带参数/返回值采集的动态增强字节码织入模板
 *
 * <p>与 {@link DynamicByteBuddyAdvice} 类似，但额外捕获方法参数（{@code @Advice.AllArguments}）
 * 和返回值（{@code @Advice.Return}），用于支持用户配置的参数/返回值采集功能。
 *
 * <p><b>设计原则（方案 B - 零开销设计）</b>：
 * <ul>
 *   <li>仅当规则配置了 {@code capture_args} 或 {@code capture_return} 时，
 *       {@link ByteBuddyTransformerFactory} 才会选择此 Advice 模板</li>
 *   <li>未配置采集的规则仍使用轻量级的 {@link DynamicByteBuddyAdvice}，
 *       避免 {@code @AllArguments} 带来的参数数组创建和自动装箱开销</li>
 * </ul>
 *
 * <p><b>关键差异</b>（对比 {@link DynamicByteBuddyAdvice}）：
 * <ul>
 *   <li>onEnter 额外绑定 {@code @Advice.AllArguments Object[] args}</li>
 *   <li>onExit 额外绑定 {@code @Advice.Return(readOnly=true) Object returnValue}</li>
 *   <li>调用 {@link AdviceDispatcher} 的带采集能力的方法</li>
 * </ul>
 *
 * @see DynamicByteBuddyAdvice
 * @see ByteBuddyTransformerFactory
 * @see AdviceDispatcher#onEnterWithCapture(String, String, Object[])
 */
final class DynamicByteBuddyCaptureAdvice {

  private DynamicByteBuddyCaptureAdvice() {}

  /**
   * 方法入口 Advice（带参数采集）
   *
   * <p>ByteBuddy 会将此方法的字节码内联到目标方法的最开始处。
   * 与 {@link DynamicByteBuddyAdvice#onEnter} 相比，额外捕获所有方法参数。
   *
   * @param ruleId 规则 ID（编译时通过 @RuleId 绑定）
   * @param typeValue 增强类型值（编译时通过 @TypeValue 绑定）
   * @param args 方法所有参数（ByteBuddy 自动创建 Object[] 数组）
   * @return 入口上下文，传递给出口方法
   */
  @Nullable
  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static Object onEnter(
      @RuleId String ruleId,
      @TypeValue String typeValue,
      @Advice.AllArguments Object[] args) {
    try {
      return AdviceDispatcher.onEnterWithCapture(ruleId, typeValue, args);
    } catch (Throwable t) {
      DynamicInstrumentLogger.logAdviceError("onEnterWithCapture", ruleId, t);
      return null;
    }
  }

  /**
   * 方法出口 Advice（带返回值采集）
   *
   * <p>ByteBuddy 会将此方法的字节码内联到目标方法的所有出口处。
   * 与 {@link DynamicByteBuddyAdvice#onExit} 相比，额外捕获方法返回值。
   *
   * @param ruleId 规则 ID（编译时通过 @RuleId 绑定）
   * @param typeValue 增强类型值（编译时通过 @TypeValue 绑定）
   * @param enterContext 方法入口返回的上下文
   * @param thrown 方法抛出的异常（不抑制）
   * @param returnValue 方法返回值（只读，不修改）
   */
  @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
  public static void onExit(
      @RuleId String ruleId,
      @TypeValue String typeValue,
      @Advice.Enter Object enterContext,
      @Advice.Thrown Throwable thrown,
      @Advice.Return(readOnly = true) Object returnValue) {
    try {
      AdviceDispatcher.onExitWithCapture(ruleId, typeValue, enterContext, thrown, returnValue);
    } catch (Throwable t) {
      DynamicInstrumentLogger.logAdviceError("onExitWithCapture", ruleId, t);
    }
  }
}
