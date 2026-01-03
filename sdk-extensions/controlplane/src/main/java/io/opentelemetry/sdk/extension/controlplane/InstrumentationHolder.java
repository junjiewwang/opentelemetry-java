/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Instrumentation 实例持有者
 *
 * <p>用于在 Agent 启动时保存 Instrumentation 实例，供后续组件（如 Arthas）使用。
 *
 * <p>支持两种获取方式：
 * <ol>
 *   <li>显式设置：在 Agent 的 premain 方法中调用 {@link #set(Instrumentation)}</li>
 *   <li>自动获取：尝试通过 ByteBuddy Agent 动态获取（需要 byte-buddy-agent 依赖）</li>
 * </ol>
 *
 * <p>示例：
 * <pre>{@code
 * // 在 premain 中设置（推荐）
 * public static void premain(String args, Instrumentation inst) {
 *     InstrumentationHolder.set(inst);
 *     // ...
 * }
 *
 * // 在其他地方获取
 * Instrumentation inst = InstrumentationHolder.get();
 * }</pre>
 */
public final class InstrumentationHolder {

  private static final Logger logger = Logger.getLogger(InstrumentationHolder.class.getName());

  private static final AtomicReference<Instrumentation> INSTANCE = new AtomicReference<>();
  private static final AtomicBoolean AUTO_OBTAIN_ATTEMPTED = new AtomicBoolean(false);

  private InstrumentationHolder() {}

  /**
   * 设置 Instrumentation 实例
   *
   * <p>应该在 Agent 的 premain 方法中尽早调用。只能设置一次，后续调用会被忽略。
   *
   * @param instrumentation Instrumentation 实例
   */
  public static void set(Instrumentation instrumentation) {
    if (instrumentation == null) {
      logger.log(Level.WARNING, "Attempted to set null Instrumentation");
      return;
    }

    if (INSTANCE.compareAndSet(null, instrumentation)) {
      logger.log(Level.INFO, "Instrumentation set successfully");
    } else {
      logger.log(Level.FINE, "Instrumentation already set, ignoring duplicate set call");
    }
  }

  /**
   * 获取 Instrumentation 实例
   *
   * <p>如果未通过 {@link #set(Instrumentation)} 显式设置，会尝试自动获取。
   *
   * @return Instrumentation 实例，如果无法获取则返回 null
   */
  @Nullable
  public static Instrumentation get() {
    Instrumentation inst = INSTANCE.get();
    if (inst != null) {
      return inst;
    }

    // 兜底：尝试从 javaagent bootstrap 的 InstrumentationHolder 读取。
    // 该类在 agent 启动早期就会被设置，并位于 BootstrapClassLoader 可见范围。
    inst = tryGetFromJavaagentBootstrap();
    if (inst != null) {
      set(inst);
      return inst;
    }

    // 尝试自动获取（只尝试一次）
    if (AUTO_OBTAIN_ATTEMPTED.compareAndSet(false, true)) {
      inst = tryObtainInstrumentation();
      if (inst != null) {
        set(inst);
        return inst;
      }
    }

    return INSTANCE.get();
  }

  @Nullable
  private static Instrumentation tryGetFromJavaagentBootstrap() {
    try {
      // 使用 BootstrapClassLoader（null）加载，避免被应用/agent classloader 隔离影响。
      Class<?> holder = Class.forName("io.opentelemetry.javaagent.bootstrap.InstrumentationHolder", false, null);
      Method getter = holder.getMethod("getInstrumentation");
      Object value = getter.invoke(null);
      if (value instanceof Instrumentation) {
        return (Instrumentation) value;
      }
    } catch (Throwable t) {
      // 静默失败：该兜底在非 javaagent 环境下必然失败。
      logger.log(Level.FINE, "javaagent bootstrap InstrumentationHolder not available: {0}", t.toString());
    }
    return null;
  }

  /**
   * 检查 Instrumentation 是否可用
   *
   * @return 是否可用
   */
  public static boolean isAvailable() {
    return get() != null;
  }

  /**
   * 尝试自动获取 Instrumentation
   *
   * <p>尝试以下方式：
   * <ol>
   *   <li>ByteBuddy Agent（如果可用）</li>
   *   <li>通过 ManagementFactory 获取 VirtualMachine（JDK 9+）</li>
   * </ol>
   *
   * @return Instrumentation 实例，如果无法获取则返回 null
   */
  @Nullable
  private static Instrumentation tryObtainInstrumentation() {
    // 方式 1: 尝试通过 ByteBuddy Agent 获取
    Instrumentation inst = tryByteBuddyAgent();
    if (inst != null) {
      logger.log(Level.INFO, "Obtained Instrumentation via ByteBuddy Agent");
      return inst;
    }

    // 方式 2: 尝试通过 VirtualMachine attach 获取（需要 JDK）
    inst = tryVirtualMachineAttach();
    if (inst != null) {
      logger.log(Level.INFO, "Obtained Instrumentation via VirtualMachine attach");
      return inst;
    }

    logger.log(Level.WARNING, 
        "Could not obtain Instrumentation automatically. " +
        "Please ensure InstrumentationHolder.set() is called in premain, " +
        "or byte-buddy-agent is available in classpath.");
    return null;
  }

  /**
   * 尝试通过 ByteBuddy Agent 获取 Instrumentation
   *
   * @return Instrumentation 实例，如果失败则返回 null
   */
  @Nullable
  private static Instrumentation tryByteBuddyAgent() {
    try {
      // 尝试加载 ByteBuddyAgent 类
      Class<?> byteBuddyAgentClass = Class.forName("net.bytebuddy.agent.ByteBuddyAgent");
      
      // 调用 install() 方法
      Method installMethod = byteBuddyAgentClass.getMethod("install");
      Object result = installMethod.invoke(null);
      
      if (result instanceof Instrumentation) {
        return (Instrumentation) result;
      }
    } catch (ClassNotFoundException e) {
      logger.log(Level.FINE, "ByteBuddy Agent not available in classpath");
    } catch (NoSuchMethodException | SecurityException e) {
      logger.log(Level.FINE, "ByteBuddy Agent install method not found: {0}", e.getMessage());
    } catch (ReflectiveOperationException e) {
      logger.log(Level.FINE, "Failed to obtain Instrumentation via ByteBuddy Agent: {0}", 
          e.getMessage());
    }
    return null;
  }

  /**
   * 尝试通过 VirtualMachine attach 获取 Instrumentation
   *
   * <p>这种方式需要 JDK（不仅仅是 JRE），因为需要 tools.jar 或 attach API。
   *
   * @return Instrumentation 实例，如果失败则返回 null
   */
  @Nullable
  private static Instrumentation tryVirtualMachineAttach() {
    // 这种方式比较复杂，且需要创建临时 agent jar
    // 暂时不实现，留作后续扩展
    logger.log(Level.FINE, "VirtualMachine attach not implemented");
    return null;
  }

  /**
   * 清除 Instrumentation 实例（仅用于测试）
   */
  static void clear() {
    INSTANCE.set(null);
    AUTO_OBTAIN_ATTEMPTED.set(false);
  }
}
