/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import java.lang.instrument.ClassFileTransformer;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.JavaModule;

/**
 * ByteBuddy Transformer 工厂
 *
 * <p>使用 ByteBuddy 的 {@link AgentBuilder} + {@link Advice} 机制创建
 * {@link ClassFileTransformer}，实现真正的字节码修改。
 *
 * <p>在目标方法的入口和出口织入对 {@link AdviceDispatcher} 的调用：
 * <ul>
 *   <li>入口：{@code Object enterContext = AdviceDispatcher.onEnter(ruleId, typeValue)}</li>
 *   <li>出口：{@code AdviceDispatcher.onExit(ruleId, typeValue, enterContext, thrown)}</li>
 * </ul>
 *
 * <p><b>注意</b>：此类依赖 ByteBuddy core（{@code net.bytebuddy:byte-buddy}），
 * 在 OTel Java Agent 环境中始终可用。如果 ByteBuddy 不在 classpath 中，
 * {@link TransformerManager} 会捕获 {@link NoClassDefFoundError} 并降级到标记式 Transformer。
 *
 * @see AdviceDispatcher
 * @see TransformerManager#createTransformer
 */
final class ByteBuddyTransformerFactory {

  private static final Logger logger =
      DynamicInstrumentLogger.getLogger("transformer-factory");

  private ByteBuddyTransformerFactory() {}

  /**
   * 检测 ByteBuddy 是否可用
   *
   * @return 是否可用
   */
  static boolean isAvailable() {
    try {
      Class.forName("net.bytebuddy.agent.builder.AgentBuilder");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * 使用 ByteBuddy 创建 ClassFileTransformer
   *
   * <p>创建的 transformer 会匹配目标类和目标方法，在方法入口和出口织入
   * {@link AdviceDispatcher#onEnter} 和 {@link AdviceDispatcher#onExit} 调用。
   *
   * @param rule 增强规则
   * @return ClassFileTransformer
   * @throws RuntimeException 如果 ByteBuddy 创建失败
   */
  static ClassFileTransformer create(InstrumentationRule rule) {
    logger.log(Level.INFO,
        "[BYTEBUDDY-FACTORY] Creating transformer for rule: {0}, target: {1}.{2}",
        new Object[] {rule.getRuleId(), rule.getClassName(), rule.getMethodName()});

    // 构建方法匹配器
    net.bytebuddy.matcher.ElementMatcher.Junction<MethodDescription> methodMatcher =
        ElementMatchers.named(rule.getMethodName());

    // 如果指定了方法描述符（JVM 格式），优先使用精确匹配
    if (rule.getMethodDescriptor() != null) {
      methodMatcher = methodMatcher.and(
          ElementMatchers.hasDescriptor(rule.getMethodDescriptor()));
    } else if (rule.getParameterTypes() != null) {
      // 使用参数类型列表匹配（Java 风格，用户友好）
      methodMatcher = methodMatcher.and(
          buildParameterTypesMatcher(rule.getParameterTypes()));
    }

    // 使用 AgentBuilder 创建 Transformer
    // 注意：这里使用 makeRaw() 获取原始的 ClassFileTransformer，
    // 而不是使用 installOn(inst)，因为我们需要手动管理 transformer 的生命周期
    AgentBuilder builder = new AgentBuilder.Default()
        .disableClassFormatChanges()
        .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
        .with(new LoggingListener(rule.getRuleId()))
        .type(ElementMatchers.named(rule.getClassName()))
        .transform(new DynamicAdviceTransformer(rule, methodMatcher));

    ClassFileTransformer transformer = builder.makeRaw();

    logger.log(Level.INFO,
        "[BYTEBUDDY-FACTORY] Transformer created successfully for rule: {0}",
        rule.getRuleId());

    return transformer;
  }

  /**
   * ByteBuddy 转换器：使用 Advice 织入 AdviceDispatcher 的调用
   */
  private static final class DynamicAdviceTransformer implements AgentBuilder.Transformer {

    private final InstrumentationRule rule;
    private final net.bytebuddy.matcher.ElementMatcher<? super MethodDescription> methodMatcher;

    DynamicAdviceTransformer(
        InstrumentationRule rule,
        net.bytebuddy.matcher.ElementMatcher<? super MethodDescription> methodMatcher) {
      this.rule = rule;
      this.methodMatcher = methodMatcher;
    }

    @Override
    public DynamicType.Builder<?> transform(
        DynamicType.Builder<?> builder,
        TypeDescription typeDescription,
        @Nullable ClassLoader classLoader,
        @Nullable JavaModule module,
        @Nullable java.security.ProtectionDomain protectionDomain) {

      logger.log(Level.INFO,
          "[BYTEBUDDY-FACTORY] Applying advice to: {0}, method: {1}, type: {2}",
          new Object[] {
            typeDescription.getName(), rule.getMethodName(), rule.getType().getValue()
          });

      return builder.visit(
          Advice.withCustomMapping()
              .bind(RuleId.class, rule.getRuleId())
              .bind(TypeValue.class, rule.getType().getValue())
              .to(DynamicByteBuddyAdvice.class)
              .on(methodMatcher));
    }
  }

  /**
   * 构建基于参数类型列表的方法匹配器
   *
   * <p>支持两种匹配模式：
   * <ul>
   *   <li><b>简单类名（尾部匹配）</b>：如 "String" 匹配 "java.lang.String"、"Wrapper" 匹配
   *       "com.baomidou.mybatisplus.core.conditions.Wrapper"</li>
   *   <li><b>全限定名（精确匹配）</b>：如 "java.lang.String" 精确匹配</li>
   * </ul>
   *
   * <p>Java 基本类型名（int, long, boolean 等）自动转换为对应的全限定名。
   *
   * @param parameterTypes 参数类型列表，空列表匹配无参方法
   * @return 方法匹配器
   */
  private static net.bytebuddy.matcher.ElementMatcher.Junction<MethodDescription>
      buildParameterTypesMatcher(List<String> parameterTypes) {
    // 先匹配参数个数
    net.bytebuddy.matcher.ElementMatcher.Junction<MethodDescription> matcher =
        ElementMatchers.takesArguments(parameterTypes.size());

    // 逐个参数位置匹配类型名
    for (int i = 0; i < parameterTypes.size(); i++) {
      String typeName = parameterTypes.get(i);
      int index = i;

      if (typeName.contains(".")) {
        // 全限定名 → 精确匹配
        matcher = matcher.and(
            ElementMatchers.takesArgument(index,
                ElementMatchers.named(typeName)));
      } else {
        // 简单类名 → 尾部匹配（支持基本类型和简短类名）
        String resolvedName = resolvePrimitiveType(typeName);
        if (resolvedName != null) {
          // 基本类型精确匹配
          matcher = matcher.and(
              ElementMatchers.takesArgument(index,
                  ElementMatchers.named(resolvedName)));
        } else {
          // 简单类名尾部匹配：类全限定名以 ".TypeName" 结尾 或等于 "TypeName"
          String suffix = "." + typeName;
          matcher = matcher.and(
              ElementMatchers.takesArgument(index,
                  new net.bytebuddy.matcher.ElementMatcher<TypeDescription>() {
                    @Override
                    public boolean matches(TypeDescription target) {
                      String fullName = target.getName();
                      return fullName.endsWith(suffix) || fullName.equals(typeName);
                    }
                  }));
        }
      }
    }

    return matcher;
  }

  /**
   * 将 Java 基本类型名解析为 JVM 内部类型名
   *
   * @param typeName 类型名
   * @return JVM 类型全限定名，非基本类型返回 null
   */
  @Nullable
  private static String resolvePrimitiveType(String typeName) {
    switch (typeName) {
      case "int":     return "int";
      case "long":    return "long";
      case "boolean": return "boolean";
      case "double":  return "double";
      case "float":   return "float";
      case "short":   return "short";
      case "byte":    return "byte";
      case "char":    return "char";
      case "void":    return "void";
      default:        return null;
    }
  }

  /**
   * AgentBuilder 日志监听器
   */
  private static final class LoggingListener implements AgentBuilder.Listener {
    private final String ruleId;

    LoggingListener(String ruleId) {
      this.ruleId = ruleId;
    }

    @Override
    public void onDiscovery(
        String typeName, @Nullable ClassLoader classLoader,
        @Nullable JavaModule module, boolean loaded) {
      // 不记录发现日志（太多）
    }

    @Override
    public void onTransformation(
        TypeDescription typeDescription, @Nullable ClassLoader classLoader,
        @Nullable JavaModule module, boolean loaded,
        DynamicType dynamicType) {
      logger.log(Level.INFO,
          "[BYTEBUDDY-FACTORY] Transformed: {0} for rule: {1}",
          new Object[] {typeDescription.getName(), ruleId});
    }

    @Override
    public void onIgnored(
        TypeDescription typeDescription, @Nullable ClassLoader classLoader,
        @Nullable JavaModule module, boolean loaded) {
      // 不记录忽略日志
    }

    @Override
    public void onError(
        String typeName, @Nullable ClassLoader classLoader,
        @Nullable JavaModule module, boolean loaded, Throwable throwable) {
      logger.log(Level.WARNING,
          "[BYTEBUDDY-FACTORY] Error transforming: " + typeName
              + " for rule: " + ruleId, throwable);
    }

    @Override
    public void onComplete(
        String typeName, @Nullable ClassLoader classLoader,
        @Nullable JavaModule module, boolean loaded) {
      // 不记录完成日志
    }
  }
}
