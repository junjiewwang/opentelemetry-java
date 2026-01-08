/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Instrumentation 提供者
 *
 * <p>负责从多种来源获取 Instrumentation 实例，并提供统一的诊断信息。
 * 这是平台基础设施层的组件，不属于任何特定的功能域（如 Arthas）。
 *
 * <p>设计原则：
 * <ul>
 *   <li><b>单一职责</b>：只负责 Instrumentation 的获取和诊断，不负责使用</li>
 *   <li><b>开闭原则</b>：通过 {@link #addFallbackLoader} 扩展获取策略</li>
 *   <li><b>依赖倒置</b>：上层组件依赖本类获取 Instrumentation，而非直接依赖具体来源</li>
 * </ul>
 *
 * <p>获取优先级（按顺序尝试）：
 * <ol>
 *   <li>显式设置的 Instrumentation（通过 {@link #setInstrumentation}）</li>
 *   <li>javaagent-bootstrap 模块的 InstrumentationHolder（多种 ClassLoader 尝试）</li>
 *   <li>ByteBuddy Agent 动态获取</li>
 *   <li>自定义的 fallback loader</li>
 * </ol>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 获取 Instrumentation 快照
 * InstrumentationSnapshot snapshot = InstrumentationProvider.getInstance().getSnapshot();
 *
 * if (snapshot.isAvailable()) {
 *     Instrumentation inst = snapshot.getInstrumentation();
 *     if (snapshot.hasEnhancementCapability()) {
 *         // 可以进行字节码增强
 *     }
 * } else {
 *     logger.warning("Instrumentation not available: " + snapshot.getDiagnosticMessage());
 * }
 * }</pre>
 */
public final class InstrumentationProvider {

  private static final Logger logger = Logger.getLogger(InstrumentationProvider.class.getName());

  /** 单例实例 */
  private static final InstrumentationProvider INSTANCE = new InstrumentationProvider();

  /** 显式设置的 Instrumentation */
  @Nullable private volatile Instrumentation explicitInstrumentation;

  /** 缓存的快照（懒加载） */
  @Nullable private volatile InstrumentationSnapshot cachedSnapshot;

  /** 自定义的 fallback loader */
  private final List<FallbackLoader> fallbackLoaders = new ArrayList<>();

  /** 诊断日志 */
  private final List<String> diagnosticLogs = new ArrayList<>();

  private InstrumentationProvider() {}

  /**
   * 获取单例实例
   *
   * @return 单例实例
   */
  public static InstrumentationProvider getInstance() {
    return INSTANCE;
  }

  /**
   * 显式设置 Instrumentation
   *
   * <p>通常在 Agent 的 premain 方法中调用。设置后会清除缓存的快照。
   *
   * @param instrumentation Instrumentation 实例
   */
  public void setInstrumentation(@Nullable Instrumentation instrumentation) {
    this.explicitInstrumentation = instrumentation;
    this.cachedSnapshot = null; // 清除缓存，强制重新计算
    if (instrumentation != null) {
      logger.log(Level.INFO, "[InstrumentationProvider] Instrumentation set explicitly");
    }
  }

  /**
   * 添加 fallback loader
   *
   * <p>用于扩展 Instrumentation 的获取策略。
   *
   * @param loader fallback loader
   */
  public void addFallbackLoader(FallbackLoader loader) {
    synchronized (fallbackLoaders) {
      fallbackLoaders.add(loader);
    }
    this.cachedSnapshot = null; // 清除缓存
  }

  /**
   * 获取 Instrumentation 快照
   *
   * <p>快照包含 Instrumentation 实例、能力信息和诊断信息。
   * 结果会被缓存，直到调用 {@link #setInstrumentation} 或 {@link #invalidateCache}。
   *
   * @return Instrumentation 快照
   */
  public InstrumentationSnapshot getSnapshot() {
    InstrumentationSnapshot snapshot = this.cachedSnapshot;
    if (snapshot != null) {
      return snapshot;
    }

    synchronized (this) {
      snapshot = this.cachedSnapshot;
      if (snapshot != null) {
        return snapshot;
      }

      snapshot = resolveInstrumentation();
      this.cachedSnapshot = snapshot;

      // 记录结果
      if (snapshot.isAvailable()) {
        logger.log(Level.INFO, 
            "[InstrumentationProvider] Resolved: {0}", snapshot.toSummary());
      } else {
        logger.log(Level.WARNING, 
            "[InstrumentationProvider] NOT available: {0}", snapshot.getDiagnosticMessage());
      }

      return snapshot;
    }
  }

  /**
   * 获取 Instrumentation 实例
   *
   * <p>便捷方法，等价于 {@code getSnapshot().getInstrumentation()}。
   *
   * @return Instrumentation 实例，可能为 null
   */
  @Nullable
  public Instrumentation getInstrumentation() {
    return getSnapshot().getInstrumentation();
  }

  /**
   * 检查是否可用
   *
   * <p>便捷方法，等价于 {@code getSnapshot().isAvailable()}。
   *
   * @return 是否可用
   */
  public boolean isAvailable() {
    return getSnapshot().isAvailable();
  }

  /**
   * 检查是否具备增强能力
   *
   * <p>便捷方法，等价于 {@code getSnapshot().hasEnhancementCapability()}。
   *
   * @return 是否具备增强能力
   */
  public boolean hasEnhancementCapability() {
    return getSnapshot().hasEnhancementCapability();
  }

  /**
   * 获取诊断日志
   *
   * @return 诊断日志列表
   */
  public List<String> getDiagnosticLogs() {
    synchronized (diagnosticLogs) {
      return new ArrayList<>(diagnosticLogs);
    }
  }

  /**
   * 使缓存失效，强制下次调用重新解析
   */
  public void invalidateCache() {
    this.cachedSnapshot = null;
  }

  /**
   * 清除所有状态（仅用于测试）
   */
  void clearForTesting() {
    this.explicitInstrumentation = null;
    this.cachedSnapshot = null;
    synchronized (fallbackLoaders) {
      fallbackLoaders.clear();
    }
    synchronized (diagnosticLogs) {
      diagnosticLogs.clear();
    }
  }

  // ===== 私有方法 =====

  /**
   * 解析 Instrumentation（核心逻辑）
   */
  private InstrumentationSnapshot resolveInstrumentation() {
    StringBuilder diagBuilder = new StringBuilder();

    // 1. 显式设置的 Instrumentation（最高优先级）
    Instrumentation inst = explicitInstrumentation;
    if (inst != null) {
      addDiag(diagBuilder, "Found explicit Instrumentation");
      return InstrumentationSnapshot.of(inst, InstrumentationSnapshot.Source.AGENT_PREMAIN);
    }
    addDiag(diagBuilder, "No explicit Instrumentation set");

    // 2. 从 javaagent-bootstrap InstrumentationHolder 获取（多种 ClassLoader 尝试）
    inst = tryGetFromJavaagentBootstrap(diagBuilder);
    if (inst != null) {
      return InstrumentationSnapshot.of(inst, InstrumentationSnapshot.Source.JAVAAGENT_BOOTSTRAP);
    }

    // 3. 通过 ByteBuddy Agent 获取
    inst = tryGetFromByteBuddyAgent(diagBuilder);
    if (inst != null) {
      return InstrumentationSnapshot.of(inst, InstrumentationSnapshot.Source.BYTEBUDDY_AGENT);
    }

    // 4. 自定义 fallback loader
    synchronized (fallbackLoaders) {
      for (FallbackLoader loader : fallbackLoaders) {
        try {
          inst = loader.load();
          if (inst != null) {
            addDiag(diagBuilder, "Found via fallback loader: " + loader.getName());
            return InstrumentationSnapshot.of(inst, InstrumentationSnapshot.Source.UNKNOWN);
          }
        } catch (RuntimeException e) {
          addDiag(diagBuilder, "Fallback loader " + loader.getName() + " failed: " + e.getMessage());
        }
      }
    }

    // 全部失败
    String finalDiag = diagBuilder.toString();
    synchronized (diagnosticLogs) {
      diagnosticLogs.add(finalDiag);
    }
    return InstrumentationSnapshot.notAvailable(finalDiag);
  }

  /**
   * 从 javaagent-bootstrap InstrumentationHolder 获取
   *
   * <p>【关键修复】使用多种 ClassLoader 尝试，而非仅用 null（Bootstrap ClassLoader）。
   * javaagent-bootstrap 的类通常不在 Bootstrap ClassLoader 中，而在 Agent ClassLoader 中。
   */
  @Nullable
  private static Instrumentation tryGetFromJavaagentBootstrap(StringBuilder diagBuilder) {
    String holderClassName = "io.opentelemetry.javaagent.bootstrap.InstrumentationHolder";
    String getterMethodName = "getInstrumentation";

    // 按优先级尝试多种 ClassLoader
    ClassLoader[] loaders = {
        Thread.currentThread().getContextClassLoader(),
        InstrumentationProvider.class.getClassLoader(),
        ClassLoader.getSystemClassLoader(),
        null // Bootstrap ClassLoader
    };

    for (ClassLoader loader : loaders) {
      String loaderName = loader != null ? loader.getClass().getName() : "Bootstrap";
      try {
        Class<?> holder = Class.forName(holderClassName, false, loader);
        Method getter = holder.getMethod(getterMethodName);
        Object value = getter.invoke(null);
        if (value instanceof Instrumentation) {
          addDiag(diagBuilder, 
              "Found from javaagent-bootstrap via ClassLoader: " + loaderName);
          logger.log(Level.INFO, 
              "[InstrumentationProvider] Got Instrumentation from javaagent-bootstrap via {0}",
              loaderName);
          return (Instrumentation) value;
        }
        addDiag(diagBuilder, 
            "javaagent-bootstrap found via " + loaderName + " but returned null");
      } catch (ClassNotFoundException e) {
        // 继续尝试下一个 loader
        addDiag(diagBuilder, 
            "javaagent-bootstrap not found via " + loaderName);
      } catch (ReflectiveOperationException e) {
        addDiag(diagBuilder, 
            "javaagent-bootstrap reflection error via " + loaderName + ": " + e.getMessage());
      }
    }

    return null;
  }

  /**
   * 通过 ByteBuddy Agent 获取
   */
  @Nullable
  private static Instrumentation tryGetFromByteBuddyAgent(StringBuilder diagBuilder) {
    try {
      Class<?> byteBuddyAgentClass = Class.forName("net.bytebuddy.agent.ByteBuddyAgent");
      Method installMethod = byteBuddyAgentClass.getMethod("install");
      Object result = installMethod.invoke(null);
      if (result instanceof Instrumentation) {
        addDiag(diagBuilder, "Found via ByteBuddy Agent");
        return (Instrumentation) result;
      }
      addDiag(diagBuilder, "ByteBuddy Agent install() returned non-Instrumentation");
    } catch (ClassNotFoundException e) {
      addDiag(diagBuilder, "ByteBuddy Agent not in classpath");
    } catch (ReflectiveOperationException e) {
      addDiag(diagBuilder, "ByteBuddy Agent error: " + e.getMessage());
    }
    return null;
  }

  private static void addDiag(StringBuilder builder, String message) {
    if (builder.length() > 0) {
      builder.append(" -> ");
    }
    builder.append(message);
  }

  /**
   * Fallback loader 接口
   *
   * <p>允许外部扩展 Instrumentation 的获取策略。
   */
  public interface FallbackLoader {
    /**
     * 获取 loader 名称（用于诊断日志）
     *
     * @return 名称
     */
    String getName();

    /**
     * 尝试加载 Instrumentation
     *
     * @return Instrumentation 实例，如果无法获取则返回 null
     */
    @Nullable
    Instrumentation load();
  }
}
