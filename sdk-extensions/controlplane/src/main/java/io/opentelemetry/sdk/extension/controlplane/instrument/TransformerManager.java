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
import java.util.Collections;
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

  /** Bootstrap CL 中 AdviceDispatcher 的 registerRule(InstrumentationRule, CaptureConfig) 方法缓存 */
  @Nullable private static volatile Method bootstrapRegisterRule;

  /** Bootstrap CL 中 AdviceDispatcher 的 unregisterRule 方法缓存 */
  @Nullable private static volatile Method bootstrapUnregisterRule;

  /** Bootstrap CL 中 InstrumentationType 的 fromString 方法缓存 */
  @Nullable private static volatile Method bootstrapTypeFromString;

  /** ruleId -> ManagedTransformer */
  private final ConcurrentHashMap<String, ManagedTransformer> transformers =
      new ConcurrentHashMap<>();

  /**
   * 目标方法(className#methodName#type) -> 已增强条目列表 的映射
   *
   * <p>key 包含 type 维度，允许同一方法被不同类型（trace/metric/log）分别增强，
   * 但同一类型内通过 {@link #checkTargetConflict} 进行精细化冲突检测：
   * <ul>
   *   <li>完全相同的重载签名 → DUPLICATE_TARGET</li>
   *   <li>有一方覆盖全部重载（未指定 parameterTypes）而另一方有交叉 → OVERLAPPING_TARGET</li>
   * </ul>
   */
  private final ConcurrentHashMap<String, List<TargetEntry>> targetMethodToRules =
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

  /**
   * 根据目标方法信息查找对应的 ruleId 列表
   *
   * <p>用于还原时无需指定 rule_id，通过 class_name + method_name + type 查找。
   * 如果同一目标方法有多条规则（不同重载），返回所有匹配的 ruleId。
   *
   * @param className 全限定类名
   * @param methodName 方法名
   * @param type 增强类型
   * @return 匹配的 ruleId 列表，无匹配返回空列表
   */
  public List<String> findRuleIdsByTarget(
      String className, String methodName, InstrumentationType type) {
    String targetKey = buildTargetKey(className, methodName, type);
    List<TargetEntry> entries = targetMethodToRules.get(targetKey);
    if (entries == null || entries.isEmpty()) {
      return Collections.emptyList();
    }
    List<String> result = new ArrayList<>(entries.size());
    for (TargetEntry entry : entries) {
      result.add(entry.ruleId);
    }
    return Collections.unmodifiableList(result);
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

    // 3. 检查同一 class+method+type 是否与已有规则冲突
    String targetKey = buildTargetKey(rule.getClassName(), rule.getMethodName(), rule.getType());
    String conflictError = checkTargetConflict(targetKey, rule);
    if (conflictError != null) {
      return EnhancementResult.failed(ruleId, conflictError.startsWith("OVERLAPPING")
          ? "OVERLAPPING_TARGET" : "DUPLICATE_TARGET", conflictError);
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

    // 12. 预解析 CaptureConfig（在增强阶段反射获取目标 Method，运行时零开销）
    List<String> warnings = new ArrayList<>();
    CaptureConfig captureConfig = resolveCaptureConfig(rule, targetClass, warnings);

    // 13. 注册 Advice 规则到 Bootstrap CL 中的 AdviceDispatcher
    //     注意：必须通过反射调用 Bootstrap CL 中的类，因为 Agent CL 和 Bootstrap CL
    //     中的 AdviceDispatcher 是两个不同的类（注入发生在 Agent CL 加载之后）。
    //     Advice 内联代码在目标类中运行时，访问的是 Bootstrap CL 中的静态字段。
    registerRuleToBootstrapCl(rule, captureConfig);

    // 14. 记录映射关系
    ManagedTransformer managed = new ManagedTransformer(ruleId, rule, transformer, targetClass);
    transformers.put(ruleId, managed);
    targetMethodToRules.computeIfAbsent(targetKey, k -> new ArrayList<>())
        .add(new TargetEntry(ruleId, rule.getParameterTypes()));

    // 15. 更新状态
    state.markActive(targetClass.getName());

    logger.log(Level.INFO,
        "[TRANSFORMER-MANAGER] Applied rule: {0}, target: {1}.{2}",
        new Object[] {ruleId, rule.getClassName(), rule.getMethodName()});

    if (!warnings.isEmpty()) {
      logger.log(Level.WARNING,
          "[TRANSFORMER-MANAGER] Rule {0} applied with warnings: {1}",
          new Object[] {ruleId, warnings});
      return EnhancementResult.successWithWarnings(ruleId, warnings);
    }
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
        managed.rule.getClassName(), managed.rule.getMethodName(), managed.rule.getType());
    removeTargetEntry(revertTargetKey, ruleId);

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
   * 在增强阶段反射获取目标 Method 并预解析 CaptureConfig
   *
   * <p>通过 targetClass 反射获取与规则匹配的 Method 对象，然后调用
   * {@link CaptureConfig#resolve(java.util.Map, Method)} 做完整解析（含参数名→索引映射）。
   * 这样运行时采集时直接使用缓存的 CaptureConfig，不需要再做反射，真正实现零开销。
   *
   * @param rule 增强规则
   * @param targetClass 已加载的目标类
   * @param warnings 收集警告信息的列表（调用方传入）
   * @return 预解析的 CaptureConfig，无采集配置时返回 {@link CaptureConfig#NONE}
   */
  private static CaptureConfig resolveCaptureConfig(
      InstrumentationRule rule, Class<?> targetClass, List<String> warnings) {
    if (rule.getConfig() == null || rule.getConfig().isEmpty()) {
      return CaptureConfig.NONE;
    }

    // 检查是否有采集配置（capture_return 非空且非 "false" 即表示需要采集）
    String captureReturnVal = rule.getConfig().get(CaptureConfig.KEY_CAPTURE_RETURN);
    boolean hasCaptureConfig = rule.getConfig().containsKey(CaptureConfig.KEY_CAPTURE_ARGS)
        || (captureReturnVal != null
            && !captureReturnVal.trim().isEmpty()
            && !"false".equalsIgnoreCase(captureReturnVal.trim()));
    if (!hasCaptureConfig) {
      return CaptureConfig.NONE;
    }

    // 尝试反射获取目标 Method（带详细失败原因）
    MethodMatchResult matchResult = findTargetMethodWithReason(targetClass, rule);
    Method resolvedMethod = matchResult.getMethod();
    if (matchResult.isSuccess() && resolvedMethod != null) {
      logger.log(Level.FINE,
          "[TRANSFORMER-MANAGER] Resolved target method for capture: {0}.{1}, params={2}",
          new Object[] {targetClass.getName(), rule.getMethodName(),
              resolvedMethod.getParameterCount()});
    } else {
      // 将详细的失败原因作为 warning 收集
      String reason = matchResult.getFailureReason();
      warnings.add(reason);
      logger.log(Level.WARNING,
          "[TRANSFORMER-MANAGER] Could not resolve target method for capture: {0}",
          reason);
    }

    CaptureConfig captureConfig = CaptureConfig.resolve(rule.getConfig(), resolvedMethod);
    logger.log(Level.FINE,
        "[TRANSFORMER-MANAGER] Pre-resolved CaptureConfig for rule {0}: {1}",
        new Object[] {rule.getRuleId(), captureConfig});
    return captureConfig;
  }

  /**
   * 通过反射查找目标类中与规则匹配的 Method（带详细失败原因）
   *
   * <p>匹配策略：
   * <ol>
   *   <li>如果指定了 parameterTypes，按参数类型精确匹配</li>
   *   <li>否则查找所有同名方法，如果仅有一个则返回，多个则返回失败（无法确定）</li>
   * </ol>
   *
   * @param targetClass 目标类
   * @param rule 增强规则
   * @return 包含匹配结果和失败原因的 MethodMatchResult
   */
  private static MethodMatchResult findTargetMethodWithReason(
      Class<?> targetClass, InstrumentationRule rule) {
    try {
      Method[] allMethods = targetClass.getDeclaredMethods();
      List<Method> candidates = new ArrayList<>();
      List<Method> allSameNameMethods = new ArrayList<>();

      for (Method m : allMethods) {
        if (!m.getName().equals(rule.getMethodName())) {
          continue;
        }
        allSameNameMethods.add(m);

        // 如果指定了参数类型列表，按参数类型匹配
        if (rule.getParameterTypes() != null) {
          if (matchesParameterTypes(m, rule.getParameterTypes())) {
            candidates.add(m);
          }
        } else {
          candidates.add(m);
        }
      }

      // 没有找到同名方法
      if (allSameNameMethods.isEmpty()) {
        return MethodMatchResult.methodNotFound(
            targetClass.getSimpleName(), rule.getMethodName());
      }

      // 精确匹配成功
      if (candidates.size() == 1) {
        return MethodMatchResult.success(candidates.get(0));
      }

      // 有多个重载但未指定 parameter_types
      if (candidates.size() > 1 && rule.getParameterTypes() == null) {
        return MethodMatchResult.ambiguousOverloads(
            targetClass.getSimpleName(), rule.getMethodName(),
            candidates.size(), formatMethodSignatures(candidates));
      }

      // 指定了 parameter_types 但没有匹配到
      if (candidates.isEmpty() && rule.getParameterTypes() != null) {
        return MethodMatchResult.parameterTypesMismatch(
            targetClass.getSimpleName(), rule.getMethodName(),
            rule.getParameterTypes(), formatMethodSignatures(allSameNameMethods));
      }

      // 未指定 parameter_types 且没有同名方法（理论上不会到这，前面已覆盖）
      return MethodMatchResult.methodNotFound(
          targetClass.getSimpleName(), rule.getMethodName());
    } catch (RuntimeException e) {
      return MethodMatchResult.error(
          targetClass.getSimpleName(), rule.getMethodName(), String.valueOf(e.getMessage()));
    }
  }

  /**
   * 格式化方法签名列表，用于 warning 消息
   *
   * @param methods 方法列表
   * @return 格式化的签名字符串，如 "[setIfAbsent(Object,Object), setIfAbsent(Object,Object,long,TimeUnit)]"
   */
  private static String formatMethodSignatures(List<Method> methods) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < methods.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      Method m = methods.get(i);
      sb.append(m.getName()).append('(');
      Class<?>[] paramTypes = m.getParameterTypes();
      for (int j = 0; j < paramTypes.length; j++) {
        if (j > 0) {
          sb.append(',');
        }
        sb.append(paramTypes[j].getSimpleName());
      }
      sb.append(')');
    }
    sb.append(']');
    return sb.toString();
  }

  /**
   * 检查 Method 的参数类型是否与指定的参数类型列表匹配
   *
   * <p>支持简单类名尾部匹配和全限定名精确匹配（与 ByteBuddy 匹配逻辑一致）。
   *
   * @param method 方法
   * @param parameterTypes 参数类型列表
   * @return 是否匹配
   */
  private static boolean matchesParameterTypes(Method method, java.util.List<String> parameterTypes) {
    Class<?>[] params = method.getParameterTypes();
    if (params.length != parameterTypes.size()) {
      return false;
    }
    for (int i = 0; i < params.length; i++) {
      String expected = parameterTypes.get(i);
      String actual = params[i].getName();
      if (expected.contains(".")) {
        // 全限定名精确匹配
        if (!actual.equals(expected)) {
          return false;
        }
      } else {
        // 简单类名：尾部匹配或基本类型精确匹配
        if (!actual.endsWith("." + expected) && !actual.equals(expected)) {
          return false;
        }
      }
    }
    return true;
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
   * @param captureConfig 预解析的采集配置
   */
  private static void registerRuleToBootstrapCl(
      InstrumentationRule rule, CaptureConfig captureConfig) {
    try {
      Object bootstrapRule = rebuildRuleInBootstrapCl(rule);

      // 统一走双参数注册，无采集配置时传 null
      if (bootstrapRegisterRule == null) {
        Class<?> bootstrapDispatcher = Class.forName(
            AdviceDispatcher.class.getName(), true, null);
        Class<?> bootstrapRuleClass = Class.forName(
            InstrumentationRule.class.getName(), true, null);
        Class<?> bootstrapCaptureConfigClass = Class.forName(
            CaptureConfig.class.getName(), true, null);
        bootstrapRegisterRule = bootstrapDispatcher.getDeclaredMethod(
            "registerRule", bootstrapRuleClass, bootstrapCaptureConfigClass);
        bootstrapRegisterRule.setAccessible(true);
      }

      Object bootstrapCaptureConfig =
          (captureConfig != null && captureConfig.hasCaptureConfig())
              ? rebuildCaptureConfigInBootstrapCl(captureConfig)
              : null;
      bootstrapRegisterRule.invoke(null, bootstrapRule, bootstrapCaptureConfig);

      logger.log(Level.FINE,
          "[TRANSFORMER-MANAGER] Registered rule to Bootstrap CL AdviceDispatcher: {0}, "
              + "captureConfig: {1}",
          new Object[] {rule.getRuleId(), captureConfig});
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
   * 在 Bootstrap CL 中重建 CaptureConfig 对象
   *
   * <p>Agent CL 中的 {@link CaptureConfig} 和 Bootstrap CL 中的同名类是不同的类，
   * 不能直接传递。通过反射调用 Bootstrap CL 中的 {@code CaptureConfig.resolve()} 来重建。
   *
   * <p>由于 CaptureConfig 是不可变对象且已预解析完成，这里使用 config Map 和 resolve
   * 方法重新构建 Bootstrap CL 版本，但此时不需要 Method 参数，因为预解析阶段已经将
   * 参数名解析为索引。因此直接通过 create 静态工厂方法传入预解析的数据。
   *
   * @param captureConfig Agent CL 中已预解析的 CaptureConfig
   * @return Bootstrap CL 中的 CaptureConfig 对象
   */
  private static Object rebuildCaptureConfigInBootstrapCl(CaptureConfig captureConfig)
      throws Exception {
    Class<?> bootstrapCaptureConfigClass = Class.forName(
        CaptureConfig.class.getName(), true, null);

    // 使用 createResolved 静态工厂方法重建
    Method createResolved = bootstrapCaptureConfigClass.getDeclaredMethod(
        "createResolved",
        int[].class, String[].class, boolean.class, String[].class, int.class);
    createResolved.setAccessible(true);

    return createResolved.invoke(null,
        captureConfig.getArgIndices(),
        captureConfig.getArgNames(),
        captureConfig.isCaptureReturn(),
        captureConfig.getReturnFields(),
        captureConfig.getMaxLength());
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
   * 构建目标方法的唯一标识 key（包含 type 维度）
   *
   * <p>包含 type 维度使得同一方法可以被不同类型分别增强（如 trace + metric），
   * 而同一类型内通过 {@link #checkTargetConflict} 做精细冲突检测。
   *
   * @param className 全限定类名
   * @param methodName 方法名
   * @param type 增强类型
   * @return className#methodName#type 格式的 key
   */
  private static String buildTargetKey(
      String className, String methodName, InstrumentationType type) {
    return className + "#" + methodName + "#" + type.getValue();
  }

  /**
   * 检查新规则是否与已有规则存在目标冲突
   *
   * <p>冲突检测矩阵：
   * <ul>
   *   <li>完全相同的签名（parameterTypes 相等） → DUPLICATE_TARGET</li>
   *   <li>新规则或已有规则覆盖全部重载（parameterTypes == null）而另一方存在 → OVERLAPPING_TARGET</li>
   *   <li>不同重载签名 → 允许</li>
   * </ul>
   *
   * @param targetKey className#methodName#type 格式的 key
   * @param newRule 新增强规则
   * @return 冲突错误描述，无冲突返回 null
   */
  @Nullable
  private String checkTargetConflict(String targetKey, InstrumentationRule newRule) {
    List<TargetEntry> existing = targetMethodToRules.get(targetKey);
    if (existing == null || existing.isEmpty()) {
      return null;
    }

    String target = newRule.getClassName() + "#" + newRule.getMethodName();
    for (TargetEntry entry : existing) {
      // 新规则覆盖全部重载，但已有精确匹配的规则 → 重叠
      if (newRule.getParameterTypes() == null) {
        return "OVERLAPPING: " + target + " (all overloads) overlaps with existing rule '"
            + entry.ruleId + "'" + formatEntryParams(entry)
            + ". Revert the existing rule first, or specify 'parameter_types' to target a specific overload.";
      }
      // 已有规则覆盖全部重载，新规则是精确匹配 → 重叠
      if (entry.parameterTypes == null) {
        return "OVERLAPPING: " + target + "(" + String.join(",", newRule.getParameterTypes())
            + ") overlaps with existing rule '" + entry.ruleId + "' (all overloads)"
            + ". Revert the existing rule first.";
      }
      // 两者都是精确匹配，且签名相同 → 重复
      if (newRule.getParameterTypes().equals(entry.parameterTypes)) {
        return "DUPLICATE: " + target + "(" + String.join(",", newRule.getParameterTypes())
            + ") already enhanced by rule: " + entry.ruleId;
      }
    }
    // 不同重载签名，无冲突
    return null;
  }

  /**
   * 格式化 TargetEntry 的参数类型信息，用于错误消息
   */
  private static String formatEntryParams(TargetEntry entry) {
    if (entry.parameterTypes == null) {
      return " (all overloads)";
    }
    return "(" + String.join(",", entry.parameterTypes) + ")";
  }

  /**
   * 从目标映射中移除指定 ruleId 的条目
   *
   * @param targetKey className#methodName#type 格式的 key
   * @param ruleId 要移除的规则 ID
   */
  private void removeTargetEntry(String targetKey, String ruleId) {
    List<TargetEntry> entries = targetMethodToRules.get(targetKey);
    if (entries != null) {
      entries.removeIf(e -> e.ruleId.equals(ruleId));
      if (entries.isEmpty()) {
        targetMethodToRules.remove(targetKey);
      }
    }
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
    @Nullable private final List<String> warnings;

    @SuppressWarnings("BooleanParameter")
    private EnhancementResult(String ruleId, boolean success,
        @Nullable String errorCode, @Nullable String errorMessage,
        @Nullable List<String> warnings) {
      this.ruleId = ruleId;
      this.success = success;
      this.errorCode = errorCode;
      this.errorMessage = errorMessage;
      this.warnings = warnings;
    }

    public static EnhancementResult success(String ruleId) {
      return new EnhancementResult(ruleId, /* success= */ true, null, null, null);
    }

    public static EnhancementResult successWithWarnings(String ruleId, List<String> warnings) {
      return new EnhancementResult(ruleId, /* success= */ true, null, null,
          Collections.unmodifiableList(new ArrayList<>(warnings)));
    }

    public static EnhancementResult failed(
        String ruleId, String errorCode, @Nullable String errorMessage) {
      return new EnhancementResult(ruleId, /* success= */ false, errorCode, errorMessage, null);
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

    /**
     * 获取警告列表（成功但存在注意事项时不为空）
     *
     * <p>典型场景：增强成功但无法解析目标方法用于 capture 通配符展开，
     * 此时 ByteBuddy 层面的增强是成功的，但 capture_args/capture_return 的 '*' 通配符
     * 无法展开为具体参数列表。
     *
     * @return 警告列表，无警告时返回 null
     */
    @Nullable
    public List<String> getWarnings() {
      return warnings;
    }

    /**
     * 是否存在警告
     */
    public boolean hasWarnings() {
      return warnings != null && !warnings.isEmpty();
    }

    @Override
    public String toString() {
      if (success) {
        if (hasWarnings()) {
          return "EnhancementResult{ruleId='" + ruleId + "', success=true"
              + ", warnings=" + warnings + "}";
        }
        return "EnhancementResult{ruleId='" + ruleId + "', success=true}";
      }
      return "EnhancementResult{ruleId='" + ruleId + "', success=false"
          + ", errorCode='" + errorCode + "', errorMessage='" + errorMessage + "'}";
    }
  }

  /**
   * 目标方法增强条目，记录一条已增强规则的 ruleId 和参数类型
   *
   * <p>用于精细化冲突检测：同一 className#methodName#type 下可以存在多个不同重载的条目。
   */
  private static final class TargetEntry {
    final String ruleId;
    @Nullable final List<String> parameterTypes;

    TargetEntry(String ruleId, @Nullable List<String> parameterTypes) {
      this.ruleId = ruleId;
      this.parameterTypes = parameterTypes;
    }
  }

  /**
   * 方法匹配结果，包含匹配到的 Method 或详细的失败原因
   *
   * <p>用于 {@link #findTargetMethodWithReason} 返回，替代之前返回 {@code @Nullable Method}
   * 的方式，提供对用户友好的失败诊断信息。
   */
  private static final class MethodMatchResult {
    @Nullable private final Method method;
    @Nullable private final String failureReason;

    private MethodMatchResult(@Nullable Method method, @Nullable String failureReason) {
      this.method = method;
      this.failureReason = failureReason;
    }

    /** 匹配成功 */
    static MethodMatchResult success(Method method) {
      return new MethodMatchResult(method, null);
    }

    /** 方法名在目标类中不存在 */
    static MethodMatchResult methodNotFound(String simpleClassName, String methodName) {
      return new MethodMatchResult(null,
          "No method named '" + methodName + "' found in class " + simpleClassName
              + ". Wildcard '*' capture_args/capture_return cannot be resolved.");
    }

    /** 存在多个重载，未指定 parameter_types 无法确定唯一目标 */
    static MethodMatchResult ambiguousOverloads(
        String simpleClassName, String methodName, int count, String signatures) {
      return new MethodMatchResult(null,
          "Multiple overloads found for " + simpleClassName + "." + methodName
              + " (" + count + " candidates: " + signatures
              + "). Specify 'parameter_types' to disambiguate."
              + " Wildcard '*' capture_args cannot be resolved to parameter names.");
    }

    /** 指定了 parameter_types 但没有匹配到 */
    static MethodMatchResult parameterTypesMismatch(
        String simpleClassName, String methodName,
        List<String> parameterTypes, String availableSignatures) {
      return new MethodMatchResult(null,
          "No method matches parameter_types=" + parameterTypes
              + " for " + simpleClassName + "." + methodName
              + ". Available: " + availableSignatures
              + ". Tip: generic types are erased at runtime, use actual types like 'Object'.");
    }

    /** 反射异常 */
    static MethodMatchResult error(
        String simpleClassName, String methodName, String errorMessage) {
      return new MethodMatchResult(null,
          "Error finding method '" + methodName + "' in class " + simpleClassName
              + ": " + errorMessage);
    }

    boolean isSuccess() {
      return method != null;
    }

    @Nullable
    Method getMethod() {
      return method;
    }

    @Nullable
    String getFailureReason() {
      return failureReason;
    }
  }
}
