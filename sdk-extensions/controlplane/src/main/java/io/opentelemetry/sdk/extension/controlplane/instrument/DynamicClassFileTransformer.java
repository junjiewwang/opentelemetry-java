/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import java.lang.instrument.ClassFileTransformer;
import java.security.ProtectionDomain;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 降级用标记式 ClassFileTransformer
 *
 * <p>当 ByteBuddy core 不在 classpath 中时（非 OTel Java Agent 环境），
 * 作为 {@link ByteBuddyTransformerFactory} 的降级替代方案。
 *
 * <p>匹配目标类时记录日志并返回原始字节码不变。虽然不修改字节码，
 * 但 transformer 仍被注册到 {@link java.lang.instrument.Instrumentation}，
 * 移除后 retransform 会恢复类到原始状态。
 *
 * <p>当 ByteBuddy 可用时，{@link TransformerManager} 会使用
 * {@link ByteBuddyTransformerFactory} 创建真正的字节码增强 transformer。
 *
 * <p>线程安全：transform 方法可能被多个 ClassLoader 并发调用。
 *
 * @see ByteBuddyTransformerFactory
 */
final class DynamicClassFileTransformer implements ClassFileTransformer {

  private static final Logger logger =
      DynamicInstrumentLogger.getLogger("transformer");

  private final InstrumentationRule rule;
  private final String targetInternalName;

  DynamicClassFileTransformer(InstrumentationRule rule) {
    this.rule = rule;
    // JVM 传入的 className 是内部格式（/分隔），需要转换
    this.targetInternalName = rule.getClassName().replace('.', '/');
  }

  @Override
  @Nullable
  public byte[] transform(
      ClassLoader loader,
      String className,
      Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain,
      byte[] classfileBuffer) {

    // 只处理目标类
    if (!targetInternalName.equals(className)) {
      return null; // 不修改
    }

    logger.log(Level.INFO,
        "[DYNAMIC-TRANSFORMER] Marker-only transform for class: {0}, rule: {1}, type: {2}",
        new Object[] {
          rule.getClassName(), rule.getRuleId(), rule.getType().getValue()
        });

    // 降级模式：不修改字节码，返回 null 表示不变更。
    // 当 ByteBuddy 可用时不会使用此 Transformer，而是使用 ByteBuddyTransformerFactory。
    // AdviceDispatcher 的回调仍会被触发（通过 TransformerManager 注册），
    // 但由于字节码未被修改，回调不会被执行。

    return null;
  }

  /**
   * 获取关联的增强规则
   *
   * @return 增强规则
   */
  InstrumentationRule getRule() {
    return rule;
  }

  @Override
  public String toString() {
    return String.format(
        "DynamicClassFileTransformer{ruleId='%s', target='%s.%s', type=%s}",
        rule.getRuleId(), rule.getClassName(), rule.getMethodName(), rule.getType().getValue());
  }
}
