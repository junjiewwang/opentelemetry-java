/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import io.opentelemetry.sdk.extension.controlplane.core.InstrumentationProvider;
import io.opentelemetry.sdk.extension.controlplane.core.InstrumentationSnapshot;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Transformer 生命周期管理器
 *
 * <p>动态增强的<b>核心组件</b>，负责：
 * <ul>
 *   <li>使用 ByteBuddy 创建 {@link ClassFileTransformer}</li>
 *   <li>通过 {@link Instrumentation#addTransformer} 注册 transformer</li>
 *   <li>通过 {@link Instrumentation#retransformClasses} 触发已加载类的重新转换</li>
 *   <li>通过 {@link Instrumentation#removeTransformer} + retransform 还原增强</li>
 *   <li>追踪 ruleId 到 transformer 的映射关系</li>
 * </ul>
 *
 * <p>技术要点：
 * <ul>
 *   <li>JVM 的 {@code retransformClasses} 在移除 transformer 后再次触发 retransform 时，
 *       类会恢复到"当前所有剩余 transformer 处理后的状态"</li>
 *   <li>使用规则级别的锁，防止同一规则的增强和还原操作产生竞态</li>
 *   <li>ByteBuddy 的使用通过反射实现，避免 compileOnly 依赖在运行时不存在时的类加载问题</li>
 * </ul>
 *
 * @see EnhancementStateRegistry
 */
public final class TransformerManager {

  private static final Logger logger =
      DynamicInstrumentLogger.getLogger("transformer-manager");

  /** 安全限制：最大同时活跃的增强数量 */
  private static final int MAX_ACTIVE_ENHANCEMENTS = 50;

  /** Bootstrap CL 中 AdviceDispatcher 的 registerRule 方法缓存 */
  @Nullable private static volatile Method bootstrapRegisterRule;

  /** Bootstrap CL 中 AdviceDispatcher 的 unregisterRule 方法缓存 */
  @Nullable private static volatile Method bootstrapUnregisterRule;

  /** Bootstrap CL 中 InstrumentationType 的 fromString 方法缓存 */
  @Nullable private static volatile Method bootstrapTypeFromString;

  /** ruleId -> ManagedTransformer */
  private final ConcurrentHashMap<String, ManagedTransformer> transformers =
      new ConcurrentHashMap<>();

  /** 目标方法(className#methodName) -> ruleId 的映射，防止不同 ruleId 增强同一方法 */
  private final ConcurrentHashMap<String, String> targetMethodToRuleId =
      new ConcurrentHashMap<>();

  /** 规则级别的锁，防止同一规则的增强和还原操作竞态 */
  private final ConcurrentHashMap<String, ReentrantLock> ruleLocks =
      new ConcurrentHashMap<>();

  private final InstrumentationProvider instrumentationProvider;
  private final EnhancementStateRegistry stateRegistry;

  /**
   * 创建 TransformerManager
   *
   * @param instrumentationProvider Instrumentation 提供者
   * @param stateRegistry 增强状态注册表
   */
  public TransformerManager(
      InstrumentationProvider instrumentationProvider,
      EnhancementStateRegistry stateRegistry) {
    this.instrumentationProvider = instrumentationProvider;
    this.stateRegistry = stateRegistry;
  }

  /**
   * 应用增强规则
   *
   * <p>流程：
   * <ol>
   *   <li>校验规则参数和 Instrumentation 能力</li>
   *   <li>查找目标类</li>
   *   <li>创建 {@link ClassFileTransformer}（通过 ByteBuddy Advice）</li>
   *   <li>注册 transformer 到 Instrumentation</li>
   *   <li>触发目标类的 retransform</li>
   *   <li>更新状态注册表</li>
   * </ol>
   *
   * @param rule 增强规则
   * @return 增强结果
   */
  public EnhancementResult applyRule(InstrumentationRule rule) {
    String ruleId = rule.getRuleId();
    ReentrantLock lock = ruleLocks.computeIfAbsent(ruleId, k -> new ReentrantLock());
    lock.lock();
    try {
      return doApplyRule(rule);
    } finally {
      lock.unlock();
    }
  }

  /**
   * 还原增强
   *
   * <p>流程：
   * <ol>
   *   <li>查找 ruleId 对应的 ManagedTransformer</li>
   *   <li>从 Instrumentation 中移除 transformer</li>
   *   <li>触发目标类的 retransform（恢复字节码）</li>
   *   <li>更新状态注册表</li>
   * </ol>
   *
   * @param ruleId 规则 ID
   * @return 还原结果
   */
  public EnhancementResult revertRule(String ruleId) {
    ReentrantLock lock = ruleLocks.computeIfAbsent(ruleId, k -> new ReentrantLock());
    lock.lock();
    try {
      return doRevertRule(ruleId);
    } finally {
      lock.unlock();
    }
  }

  /**
   * 还原所有增强（用于优雅关闭）
   *
   * @return 还原结果列表
   */
  public List<EnhancementResult> revertAll() {
    List<EnhancementResult> results = new ArrayList<>();
    for (String ruleId : new ArrayList<>(transformers.keySet())) {
      try {
        results.add(revertRule(ruleId));
      } catch (RuntimeException e) {
        logger.log(Level.WARNING, "Failed to revert rule: " + ruleId, e);
        results.add(EnhancementResult.failed(ruleId, "REVERT_ERROR", e.getMessage()));
      }
    }
    return results;
  }

  /**
   * 获取当前活跃的增强数量
   *
   * @return 活跃增强数量
   */
  public int getActiveCount() {
    return transformers.size();
  }

  /**
   * 检查指定规则是否已被应用
   *
   * @param ruleId 规则 ID
   * @return 是否已应用
   */
  public boolean isApplied(String ruleId) {
    return transformers.containsKey(ruleId);
  }

  // ===== 私有方法 =====

  private EnhancementResult doApplyRule(InstrumentationRule rule) {
    String ruleId = rule.getRuleId();

    // 1. 校验规则
    String validationError = rule.validate();
    if (validationError != null) {
      return EnhancementResult.failed(ruleId, "INVALID_RULE", validationError);
    }

    // 2. 检查是否已存在
    if (transformers.containsKey(ruleId)) {
      return EnhancementResult.failed(ruleId, "ALREADY_APPLIED",
          "Rule already applied: " + ruleId);
    }

    // 3. 检查同一 class+method 是否已被其他规则增强
    String targetKey = buildTargetKey(rule.getClassName(), rule.getMethodName());
    String existingRuleId = targetMethodToRuleId.get(targetKey);
    if (existingRuleId != null) {
      return EnhancementResult.failed(ruleId, "DUPLICATE_TARGET",
          "Method " + rule.getClassName() + "#" + rule.getMethodName()
              + " already enhanced by rule: " + existingRuleId);
    }

    // 4. 检查数量限制
    if (transformers.size() >= MAX_ACTIVE_ENHANCEMENTS) {
      return EnhancementResult.failed(ruleId, "LIMIT_EXCEEDED",
          "Maximum active enhancements reached: " + MAX_ACTIVE_ENHANCEMENTS);
    }

    // 5. 检查 Instrumentation 能力
    InstrumentationSnapshot snapshot = instrumentationProvider.getSnapshot();
    if (!snapshot.hasEnhancementCapability()) {
      return EnhancementResult.failed(ruleId, "NO_INSTRUMENTATION",
          "Instrumentation not available or does not support retransform: "
              + snapshot.getDiagnosticMessage());
    }

    Instrumentation inst = snapshot.getInstrumentation();
    if (inst == null) {
      return EnhancementResult.failed(ruleId, "NO_INSTRUMENTATION",
          "Instrumentation is null");
    }

    // 6. 注册状态
    EnhancementState state = stateRegistry.register(rule);

    // 7. 查找目标类
    Class<?> targetClass = findLoadedClass(inst, rule.getClassName());
    if (targetClass == null) {
      state.markFailed("Target class not loaded: " + rule.getClassName());
      return EnhancementResult.failed(ruleId, "CLASS_NOT_FOUND",
          "Target class not loaded: " + rule.getClassName());
    }

    // 8. 创建 Transformer（通过 ByteBuddy）
    ClassFileTransformer transformer;
    try {
      transformer = createTransformer(rule);
    } catch (RuntimeException e) {
      state.markFailed("Failed to create transformer: " + e.getMessage());
      return EnhancementResult.failed(ruleId, "TRANSFORMER_CREATE_ERROR", e.getMessage());
    }

    // 9. 确保 Advice 相关类已注入到 Bootstrap ClassLoader
    //    必须在 addTransformer + retransform 之前执行，否则 Advice 内联代码运行时找不到类
    if (!BootstrapClassInjector.isInjected()) {
      boolean injected = BootstrapClassInjector.inject(inst);
      if (!injected) {
        state.markFailed("Failed to inject advice classes to Bootstrap ClassLoader");
        return EnhancementResult.failed(ruleId, "BOOTSTRAP_INJECT_ERROR",
            "Failed to inject advice classes to Bootstrap ClassLoader. "
                + "Dynamic instrumentation requires Bootstrap CL visibility for advice classes.");
      }
    }

    // 10. 注册 transformer
    inst.addTransformer(transformer, /* canRetransform= */ true);

    // 11. 触发 retransform
    try {
      inst.retransformClasses(targetClass);
    } catch (UnmodifiableClassException e) {
      // 回滚：移除 transformer
      inst.removeTransformer(transformer);
      state.markFailed("Class not modifiable: " + e.getMessage());
      return EnhancementResult.failed(ruleId, "CLASS_NOT_MODIFIABLE", e.getMessage());
    } catch (RuntimeException e) {
      // 回滚：移除 transformer
      inst.removeTransformer(transformer);
      state.markFailed("Retransform failed: " + e.getMessage());
      return EnhancementResult.failed(ruleId, "RETRANSFORM_ERROR", e.getMessage());
    }

    // 12. 注册 Advice 规则到 Bootstrap CL 中的 AdviceDispatcher
    //     注意：必须通过反射调用 Bootstrap CL 中的类，因为 Agent CL 和 Bootstrap CL
    //     中的 AdviceDispatcher 是两个不同的类（注入发生在 Agent CL 加载之后）。
    //     Advice 内联代码在目标类中运行时，访问的是 Bootstrap CL 中的静态字段。
    registerRuleToBootstrapCl(rule);

    // 13. 记录映射关系
    ManagedTransformer managed = new ManagedTransformer(ruleId, rule, transformer, targetClass);
    transformers.put(ruleId, managed);
    targetMethodToRuleId.put(targetKey, ruleId);

    // 14. 更新状态
    state.markActive(targetClass.getName());

    logger.log(Level.INFO,
        "[TRANSFORMER-MANAGER] Applied rule: {0}, target: {1}.{2}",
        new Object[] {ruleId, rule.getClassName(), rule.getMethodName()});

    return EnhancementResult.success(ruleId);
  }

  private EnhancementResult doRevertRule(String ruleId) {
    // 1. 查找 managed transformer
    ManagedTransformer managed = transformers.get(ruleId);
    if (managed == null) {
      return EnhancementResult.failed(ruleId, "NOT_FOUND",
          "Rule not found or not applied: " + ruleId);
    }

    // 2. 更新状态为 REVERTING
    EnhancementState state = stateRegistry.get(ruleId);
    if (state != null) {
      state.markReverting();
    }

    // 3. 获取 Instrumentation
    Instrumentation inst = instrumentationProvider.getInstrumentation();
    if (inst == null) {
      if (state != null) {
        state.markFailed("Instrumentation not available for revert");
      }
      return EnhancementResult.failed(ruleId, "NO_INSTRUMENTATION",
          "Instrumentation not available for revert");
    }

    // 4. 移除 transformer
    boolean removed = inst.removeTransformer(managed.transformer);
    if (!removed) {
      logger.log(Level.WARNING,
          "[TRANSFORMER-MANAGER] Transformer was not registered (already removed?): {0}",
          ruleId);
    }

    // 5. 触发 retransform 恢复原始字节码
    try {
      inst.retransformClasses(managed.targetClass);
    } catch (UnmodifiableClassException e) {
      if (state != null) {
        state.markFailed("Failed to retransform for revert: " + e.getMessage());
      }
      return EnhancementResult.failed(ruleId, "REVERT_RETRANSFORM_ERROR", e.getMessage());
    }

    // 6. 注销 Advice（通过反射操作 Bootstrap CL 中的 AdviceDispatcher）
    unregisterRuleFromBootstrapCl(ruleId, managed.rule.getType());

    // 7. 清理映射
    transformers.remove(ruleId);
    String revertTargetKey = buildTargetKey(
        managed.rule.getClassName(), managed.rule.getMethodName());
    targetMethodToRuleId.remove(revertTargetKey);

    // 8. 更新状态
    if (state != null) {
      state.markReverted();
    }

    logger.log(Level.INFO,
        "[TRANSFORMER-MANAGER] Reverted rule: {0}, target: {1}",
        new Object[] {ruleId, managed.targetClass.getName()});

    return EnhancementResult.success(ruleId);
  }

  /**
   * 在已加载的类中查找目标类
   *
   * @param inst Instrumentation 实例
   * @param className 全限定类名
   * @return 目标类，未找到返回 null
   */
  @Nullable
  private static Class<?> findLoadedClass(Instrumentation inst, String className) {
    for (Class<?> clazz : inst.getAllLoadedClasses()) {
      if (clazz.getName().equals(className)) {
        if (inst.isModifiableClass(clazz)) {
          return clazz;
        }
        logger.log(Level.WARNING,
            "[TRANSFORMER-MANAGER] Class found but not modifiable: {0}", className);
        return null;
      }
    }
    return null;
  }

  /**
   * 创建 ClassFileTransformer
   *
   * <p>优先使用 ByteBuddy 的 {@link net.bytebuddy.asm.Advice} 机制创建 transformer，
   * 实现真正的字节码修改。如果 ByteBuddy core 不在 classpath 中（非 OTel Java Agent 环境），
   * 降级到标记式 Transformer（仅匹配类不修改字节码）。
   *
   * <p>ByteBuddy 模式下的增强流程：
   * <ol>
   *   <li>使用 {@link ByteBuddyTransformerFactory} 创建 transformer</li>
   *   <li>ByteBuddy 在目标方法入口织入 {@code AdviceDispatcher.onEnter(ruleId, type)}</li>
   *   <li>ByteBuddy 在目标方法出口织入 {@code AdviceDispatcher.onExit(ruleId, type, ctx, thrown)}</li>
   *   <li>{@link AdviceDispatcher} 根据类型分发到 DynamicTraceAdvice / DynamicMetricAdvice / DynamicLogAdvice</li>
   * </ol>
   *
   * @param rule 增强规则
   * @return ClassFileTransformer
   */
  private static ClassFileTransformer createTransformer(InstrumentationRule rule) {
    // 优先使用 ByteBuddy 创建真正的字节码增强 transformer
    try {
      if (ByteBuddyTransformerFactory.isAvailable()) {
        ClassFileTransformer transformer = ByteBuddyTransformerFactory.create(rule);
        logger.log(Level.INFO,
            "[TRANSFORMER-MANAGER] Created ByteBuddy transformer for rule: {0}",
            rule.getRuleId());
        return transformer;
      }
    } catch (NoClassDefFoundError | RuntimeException e) {
      logger.log(Level.WARNING,
          "[TRANSFORMER-MANAGER] ByteBuddy not available, falling back to marker transformer: "
              + e.getMessage());
    }

    // 降级到标记式 Transformer（仅匹配类，不修改字节码）
    logger.log(Level.INFO,
        "[TRANSFORMER-MANAGER] Using marker transformer for rule: {0}", rule.getRuleId());
    return new DynamicClassFileTransformer(rule);
  }

  /**
   * 通过反射将规则注册到 Bootstrap CL 中的 AdviceDispatcher
   *
   * <p>由于 Bootstrap 注入发生在 Agent CL 已加载这些类之后，Agent CL 中的
   * {@link AdviceDispatcher} 和 Bootstrap CL 中的 AdviceDispatcher 是两个不同的类。
   * Advice 内联代码在目标类的 ClassLoader 中运行时，通过双亲委派找到的是 Bootstrap CL
   * 中的版本。因此规则必须注册到 Bootstrap CL 的 AdviceDispatcher 中。
   *
   * @param rule 增强规则
   */
  private static void registerRuleToBootstrapCl(InstrumentationRule rule) {
    try {
      // 缓存反射方法引用
      if (bootstrapRegisterRule == null) {
        Class<?> bootstrapDispatcher = Class.forName(
            AdviceDispatcher.class.getName(), true, null);
        Class<?> bootstrapRuleClass = Class.forName(
            InstrumentationRule.class.getName(), true, null);
        bootstrapRegisterRule = bootstrapDispatcher.getDeclaredMethod(
            "registerRule", bootstrapRuleClass);
        bootstrapRegisterRule.setAccessible(true);
      }

      // 将 Agent CL 中的 InstrumentationRule 序列化为 Bootstrap CL 中的版本
      // 由于两者是不同的类，不能直接传递，需要通过 Builder 重建
      Object bootstrapRule = rebuildRuleInBootstrapCl(rule);
      bootstrapRegisterRule.invoke(null, bootstrapRule);

      logger.log(Level.FINE,
          "[TRANSFORMER-MANAGER] Registered rule to Bootstrap CL AdviceDispatcher: {0}",
          rule.getRuleId());
    } catch (Exception e) {
      logger.log(Level.SEVERE,
          "[TRANSFORMER-MANAGER] Failed to register rule to Bootstrap CL: "
              + rule.getRuleId(), e);
    }
  }

  /**
   * 通过反射从 Bootstrap CL 中的 AdviceDispatcher 注销规则
   *
   * @param ruleId 规则 ID
   * @param type 增强类型
   */
  private static void unregisterRuleFromBootstrapCl(
      String ruleId, InstrumentationType type) {
    try {
      if (bootstrapUnregisterRule == null) {
        Class<?> bootstrapDispatcher = Class.forName(
            AdviceDispatcher.class.getName(), true, null);
        Class<?> bootstrapTypeClass = Class.forName(
            InstrumentationType.class.getName(), true, null);
        bootstrapUnregisterRule = bootstrapDispatcher.getDeclaredMethod(
            "unregisterRule", String.class, bootstrapTypeClass);
        bootstrapUnregisterRule.setAccessible(true);
      }

      // 将 Agent CL 的 InstrumentationType 转换为 Bootstrap CL 的版本
      Object bootstrapType = resolveBootstrapType(type.getValue());
      bootstrapUnregisterRule.invoke(null, ruleId, bootstrapType);

      logger.log(Level.FINE,
          "[TRANSFORMER-MANAGER] Unregistered rule from Bootstrap CL AdviceDispatcher: {0}",
          ruleId);
    } catch (Exception e) {
      logger.log(Level.SEVERE,
          "[TRANSFORMER-MANAGER] Failed to unregister rule from Bootstrap CL: " + ruleId, e);
    }
  }

  /**
   * 在 Bootstrap CL 中重建 InstrumentationRule 对象
   *
   * <p>Agent CL 中的 {@link InstrumentationRule} 和 Bootstrap CL 中的同名类是不同的类，
   * 不能直接传递。通过反射调用 Bootstrap CL 中的 Builder 来重建对象。
   *
   * @param rule Agent CL 中的规则对象
   * @return Bootstrap CL 中的规则对象
   */
  private static Object rebuildRuleInBootstrapCl(InstrumentationRule rule) throws Exception {
    Class<?> bootstrapRuleClass = Class.forName(
        InstrumentationRule.class.getName(), true, null);

    // 调用 InstrumentationRule.builder()
    Method builderMethod = bootstrapRuleClass.getMethod("builder");
    Object builder = builderMethod.invoke(null);
    Class<?> builderClass = builder.getClass();

    // 设置各字段
    builderClass.getMethod("ruleId", String.class).invoke(builder, rule.getRuleId());
    builderClass.getMethod("className", String.class).invoke(builder, rule.getClassName());
    builderClass.getMethod("methodName", String.class).invoke(builder, rule.getMethodName());
    if (rule.getMethodDescriptor() != null) {
      builderClass.getMethod("methodDescriptor", String.class)
          .invoke(builder, rule.getMethodDescriptor());
    }
    if (rule.getSpanName() != null) {
      builderClass.getMethod("spanName", String.class)
          .invoke(builder, rule.getSpanName());
    }

    // 设置 type（需要转换为 Bootstrap CL 中的枚举值）
    Object bootstrapType = resolveBootstrapType(rule.getType().getValue());
    Class<?> bootstrapTypeClass = Class.forName(
        InstrumentationType.class.getName(), true, null);
    builderClass.getMethod("type", bootstrapTypeClass).invoke(builder, bootstrapType);

    // 设置 config
    builderClass.getMethod("config", java.util.Map.class)
        .invoke(builder, rule.getConfig());

    // build()
    return builderClass.getMethod("build").invoke(builder);
  }

  /**
   * 通过反射获取 Bootstrap CL 中的 InstrumentationType 枚举值
   *
   * @param typeValue 类型字符串值（如 "trace"）
   * @return Bootstrap CL 中的 InstrumentationType 枚举值
   */
  private static Object resolveBootstrapType(String typeValue) throws Exception {
    if (bootstrapTypeFromString == null) {
      Class<?> bootstrapTypeClass = Class.forName(
          InstrumentationType.class.getName(), true, null);
      bootstrapTypeFromString = bootstrapTypeClass.getMethod("fromString", String.class);
    }
    return bootstrapTypeFromString.invoke(null, typeValue);
  }

  /**
   * 构建目标方法的唯一标识 key
   *
   * @param className 全限定类名
   * @param methodName 方法名
   * @return className#methodName 格式的 key
   */
  private static String buildTargetKey(String className, String methodName) {
    return className + "#" + methodName;
  }

  // ===== 内部类 =====

  /** 管理中的 Transformer 信息 */
  @SuppressWarnings("UnusedVariable")
  private static final class ManagedTransformer {
    final String ruleId;
    final InstrumentationRule rule;
    final ClassFileTransformer transformer;
    final Class<?> targetClass;

    ManagedTransformer(
        String ruleId, InstrumentationRule rule,
        ClassFileTransformer transformer, Class<?> targetClass) {
      this.ruleId = ruleId;
      this.rule = rule;
      this.transformer = transformer;
      this.targetClass = targetClass;
    }
  }

  /**
   * 增强操作结果
   */
  public static final class EnhancementResult {
    private final String ruleId;
    private final boolean success;
    @Nullable private final String errorCode;
    @Nullable private final String errorMessage;

    private EnhancementResult(String ruleId, boolean success,
        @Nullable String errorCode, @Nullable String errorMessage) {
      this.ruleId = ruleId;
      this.success = success;
      this.errorCode = errorCode;
      this.errorMessage = errorMessage;
    }

    public static EnhancementResult success(String ruleId) {
      return new EnhancementResult(ruleId, /* success= */ true, null, null);
    }

    public static EnhancementResult failed(
        String ruleId, String errorCode, @Nullable String errorMessage) {
      return new EnhancementResult(ruleId, /* success= */ false, errorCode, errorMessage);
    }

    public String getRuleId() {
      return ruleId;
    }

    public boolean isSuccess() {
      return success;
    }

    @Nullable
    public String getErrorCode() {
      return errorCode;
    }

    @Nullable
    public String getErrorMessage() {
      return errorMessage;
    }

    @Override
    public String toString() {
      if (success) {
        return "EnhancementResult{ruleId='" + ruleId + "', success=true}";
      }
      return "EnhancementResult{ruleId='" + ruleId + "', success=false"
          + ", errorCode='" + errorCode + "', errorMessage='" + errorMessage + "'}";
    }
  }
}
