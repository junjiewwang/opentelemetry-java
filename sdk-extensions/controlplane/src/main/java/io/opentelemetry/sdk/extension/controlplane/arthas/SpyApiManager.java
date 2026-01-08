/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.io.File;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.jar.JarFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * SpyAPI 管理器
 *
 * <p>负责 SpyAPI 的统一管理，包括：
 * <ul>
 *   <li>加载 SpyAPI 到 Bootstrap ClassLoader</li>
 *   <li>诊断 SpyAPI 状态</li>
 *   <li>二次 attach 自愈（恢复 SpyImpl）</li>
 * </ul>
 *
 * <p>设计原则：
 * <ul>
 *   <li><b>单一职责</b>：只负责 SpyAPI 相关操作</li>
 *   <li><b>统一入口</b>：所有 SpyAPI 反射操作集中在此</li>
 *   <li><b>诊断友好</b>：提供完整的诊断信息</li>
 * </ul>
 *
 * <p>Arthas 4.0.3+ SpyAPI 结构说明：
 * <ul>
 *   <li>NopSpy 变成了内部类 SpyAPI$NopSpy，不再是独立类</li>
 *   <li>使用 NOPSPY 静态常量</li>
 *   <li>SpyImpl 通过 Enhancer 类的静态初始化块中的 SpyAPI.setSpy() 设置</li>
 *   <li>Enhancer 类只有在执行 trace/watch/stack 命令时才会被加载</li>
 * </ul>
 */
public final class SpyApiManager {

  private static final Logger logger = Logger.getLogger(SpyApiManager.class.getName());

  private static final String SPY_API_CLASS = "java.arthas.SpyAPI";

  /** 是否已加载到 Bootstrap ClassLoader */
  private volatile boolean loaded = false;

  public SpyApiManager() {}

  /**
   * 检查 SpyAPI 是否已加载到 Bootstrap ClassLoader
   *
   * @return 是否已加载
   */
  public boolean isLoaded() {
    if (loaded) {
      return true;
    }
    // 实际检查
    boolean actualLoaded = isSpyApiInBootstrap();
    if (actualLoaded) {
      loaded = true;
    }
    return actualLoaded;
  }

  /**
   * 加载 SpyAPI 到 Bootstrap ClassLoader
   *
   * <p>SpyAPI 必须在 Bootstrap ClassLoader 中加载，这样才能被所有类访问。
   * 使用 {@link Instrumentation#appendToBootstrapClassLoaderSearch(JarFile)} 实现。
   *
   * @param inst Instrumentation 实例
   * @param config Arthas 配置（用于查找 spy jar）
   * @return 是否成功加载
   */
  public boolean loadToBootstrap(@Nullable Instrumentation inst, ArthasConfig config) {
    // 检查是否已经加载
    if (isLoaded()) {
      logger.log(Level.FINE, "SpyAPI already loaded to Bootstrap ClassLoader");
      // 验证状态并记录诊断信息
      diagnose();
      return true;
    }

    // 检查 Instrumentation 是否可用
    if (inst == null) {
      logger.log(
          Level.WARNING,
          "Instrumentation not available, cannot load SpyAPI to Bootstrap ClassLoader");
      return false;
    }

    try {
      // 查找 arthas-spy.jar
      File spyJarFile = ArthasResourceExtractor.extractSpyJar(config);
      if (spyJarFile == null) {
        logger.log(Level.WARNING, "arthas-spy.jar not found");
        return false;
      }

      // 使用 Instrumentation 将 spy jar 添加到 Bootstrap ClassLoader
      logger.log(Level.INFO, "Loading SpyAPI from: {0}", spyJarFile.getAbsolutePath());
      inst.appendToBootstrapClassLoaderSearch(new JarFile(spyJarFile));

      // 验证加载成功
      if (isSpyApiInBootstrap()) {
        loaded = true;
        logger.log(Level.INFO, "SpyAPI loaded to Bootstrap ClassLoader successfully");
        // 验证状态并记录诊断信息
        diagnose();
        return true;
      } else {
        logger.log(Level.WARNING, "SpyAPI jar added but class not found");
        return false;
      }

    } catch (Exception e) {
      logger.log(Level.WARNING, "Failed to load SpyAPI: {0}", e.getMessage());
      return false;
    }
  }

  /**
   * 诊断 SpyAPI 状态
   *
   * <p>【职责边界】本方法<strong>只做诊断</strong>，用于记录 SpyAPI 的当前状态。
   * 二次 attach 的"自愈修复"由 {@link #ensureInstalled} 负责。
   *
   * @return 诊断快照
   */
  public SpyApiSnapshot diagnose() {
    SpyApiSnapshot snapshot = SpyApiSnapshot.read();

    if (!snapshot.isAvailable()) {
      logger.log(
          Level.WARNING,
          "[SpyAPI] SpyAPI class not found in Bootstrap ClassLoader: {0}. "
              + "arthas-spy.jar may not be loaded correctly.",
          snapshot.getDiagnosticMessage());
      return snapshot;
    }

    String currentClassName = snapshot.getSpyInstanceSimpleName();

    if ("NopSpy".equals(currentClassName)) {
      // 这是正常的初始状态
      logger.log(
          Level.INFO,
          "[SpyAPI] Current spyInstance is NopSpy (initial state). "
              + "SpyImpl will be set when Enhancer class is loaded.");

      // 验证 Arthas 4.0.3+ 结构
      verifyNopSpyState(snapshot);

    } else if ("SpyImpl".equals(currentClassName)) {
      // SpyImpl 已经设置，一切正常
      logger.log(Level.INFO, "[SpyAPI] spyInstance is already SpyImpl. Enhancement is working.");

    } else {
      // 其他类型，记录警告
      logger.log(
          Level.WARNING,
          "[SpyAPI] Unexpected spyInstance type: {0}. Enhancement may not work correctly.",
          currentClassName);
    }

    return snapshot;
  }

  /**
   * 二次 attach 自愈：确保 SpyAPI.spyInstance 已恢复为 SpyImpl
   *
   * <p>背景：Arthas destroy() 会把 SpyAPI 置回 NOPSPY，但 Enhancer 的静态块只执行一次，
   * 导致后续 attach 虽然 init/bind 成功，但增强回调链路仍是 NOP。
   *
   * <p>策略：
   * <ul>
   *   <li>幂等：若当前已是 SpyImpl，则不做处理</li>
   *   <li>优先复用 Arthas 自己创建的 Enhancer.spyImpl 静态字段</li>
   *   <li>失败降级：任何反射失败只记录日志，不阻塞启动</li>
   * </ul>
   *
   * @param arthasLoader Arthas ClassLoader（用于加载 Enhancer/SpyImpl）
   */
  public void ensureInstalled(ClassLoader arthasLoader) {
    try {
      SpyApiSnapshot snapshot = SpyApiSnapshot.read();
      if (!snapshot.isAvailable()) {
        logger.log(
            Level.WARNING,
            "[SpyAPI] SpyAPI class not found in Bootstrap ClassLoader: {0}",
            snapshot.getDiagnosticMessage());
        return;
      }

      String beforeType = snapshot.getSpyInstanceClassName();

      // 已经是 SpyImpl（或其子类）则直接返回
      if (snapshot.isSpyImplInstalled()) {
        logger.log(Level.FINE, "[SpyAPI] spyInstance already installed: {0}", beforeType);
        return;
      }

      // 优先从 Enhancer.spyImpl 复用 Arthas 内部实例
      Object spyImpl = tryGetSpyImplFromEnhancer(arthasLoader);

      // 拿不到则尝试反射创建 SpyImpl
      if (spyImpl == null) {
        spyImpl = tryCreateSpyImpl(arthasLoader);
      }

      if (spyImpl == null) {
        logger.log(
            Level.WARNING,
            "[SpyAPI] Cannot restore spyInstance because SpyImpl is not available. current={0}",
            beforeType);
        return;
      }

      // 调用 SpyAPI.setSpy(spyImpl)
      try {
        snapshot.invokeSetSpy(spyImpl);
      } catch (Exception e) {
        logger.log(
            Level.WARNING,
            "[SpyAPI] Failed to invoke SpyAPI.setSpy: {0}",
            e.getMessage());
        return;
      }

      // 再次确认 before/after
      SpyApiSnapshot after = SpyApiSnapshot.read();
      String afterType = after.isAvailable() ? after.getSpyInstanceClassName() : "unknown";
      logger.log(
          Level.INFO,
          "[SpyAPI] Restored spyInstance after attach. before={0}, after={1}",
          new Object[] {beforeType, afterType});

    } catch (RuntimeException e) {
      logger.log(
          Level.WARNING,
          "[SpyAPI] Unexpected error while restoring spyInstance: {0}",
          e.getMessage());
    }
  }

  // ===== 私有方法 =====

  /**
   * 检查 SpyAPI 是否已在 Bootstrap ClassLoader 中
   */
  private static boolean isSpyApiInBootstrap() {
    try {
      Class.forName(SPY_API_CLASS, false, null);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * 验证 NopSpy 状态（兼容 Arthas 4.0.3+）
   */
  private static void verifyNopSpyState(SpyApiSnapshot snapshot) {
    Class<?> spyApiClass = snapshot.getSpyApiClass();
    Field spyInstanceField = snapshot.getSpyInstanceField();
    Object currentValue = snapshot.getSpyInstance();

    if (spyApiClass == null || spyInstanceField == null) {
      return;
    }

    try {
      // 尝试获取 NOPSPY 常量
      Field nopspyField = spyApiClass.getDeclaredField("NOPSPY");
      nopspyField.setAccessible(true);
      Object nopspyValue = nopspyField.get(null);

      if (currentValue == nopspyValue) {
        logger.log(
            Level.FINE,
            "[SpyAPI] Verified: spyInstance == NOPSPY (Arthas 4.0.3+ detected).");
      } else {
        // 旧版本或异常情况：重置
        logger.log(
            Level.INFO, "[SpyAPI] spyInstance != NOPSPY, resetting to NOPSPY for compatibility.");
        spyInstanceField.set(null, nopspyValue);
      }
    } catch (NoSuchFieldException e) {
      // 没有 NOPSPY 字段，可能是旧版本
      logger.log(Level.FINE, "[SpyAPI] NOPSPY field not found, trying legacy NopSpy.INSTANCE");
      if (currentValue != null) {
        resetSpyApiLegacy(spyInstanceField, currentValue);
      }
    } catch (ReflectiveOperationException e) {
      logger.log(Level.WARNING, "[SpyAPI] Failed to verify NopSpy state: {0}", e.getMessage());
    }
  }

  /**
   * 旧版本 Arthas 的 SpyAPI 重置逻辑
   */
  private static void resetSpyApiLegacy(Field spyInstanceField, Object currentValue) {
    try {
      Class<?> nopSpyClass = Class.forName("java.arthas.NopSpy", true, null);
      Field instanceField = nopSpyClass.getDeclaredField("INSTANCE");
      instanceField.setAccessible(true);
      Object nopSpyInstance = instanceField.get(null);

      if (nopSpyInstance == null) {
        logger.log(
            Level.WARNING, "[SpyAPI] Legacy NopSpy.INSTANCE is null, cannot reset spyInstance");
        return;
      }

      if (currentValue == nopSpyInstance) {
        logger.log(
            Level.FINE, "[SpyAPI] Legacy: spyInstance is already NopSpy.INSTANCE, no reset needed");
        return;
      }

      spyInstanceField.set(null, nopSpyInstance);
      logger.log(Level.INFO, "[SpyAPI] Legacy: Reset spyInstance to NopSpy.INSTANCE.");

    } catch (ClassNotFoundException e) {
      logger.log(
          Level.INFO,
          "[SpyAPI] Neither NOPSPY nor java.arthas.NopSpy found. "
              + "Assuming Arthas 4.0.3+ with setSpy() that works without reset.");
    } catch (ReflectiveOperationException e) {
      logger.log(Level.WARNING, "[SpyAPI] Legacy reset failed: {0}.", e.getMessage());
    }
  }

  /**
   * 从 Enhancer.spyImpl 获取 SpyImpl 实例
   */
  @Nullable
  private static Object tryGetSpyImplFromEnhancer(ClassLoader arthasLoader) {
    String enhancerClassName = "com.taobao.arthas.core.advisor.Enhancer";

    try {
      Class<?> enhancerClass = arthasLoader.loadClass(enhancerClassName);
      Field spyImplField = enhancerClass.getDeclaredField("spyImpl");
      spyImplField.setAccessible(true);

      if (!Modifier.isStatic(spyImplField.getModifiers())) {
        logger.log(
            Level.WARNING,
            "[SpyAPI] Enhancer.spyImpl is not static, cannot use it for restoration");
        return null;
      }

      Object value = spyImplField.get(null);
      if (value == null) {
        logger.log(Level.WARNING, "[SpyAPI] Enhancer.spyImpl is null");
        return null;
      }

      logger.log(
          Level.FINE,
          "[SpyAPI] Loaded spyImpl from Enhancer: class={0}",
          value.getClass().getName());
      return value;

    } catch (ClassNotFoundException e) {
      logger.log(
          Level.WARNING,
          "[SpyAPI] Enhancer class not found in Arthas ClassLoader: {0}",
          e.getMessage());
      return null;
    } catch (NoSuchFieldException e) {
      logger.log(Level.WARNING, "[SpyAPI] Enhancer.spyImpl field not found: {0}", e.getMessage());
      return null;
    } catch (ReflectiveOperationException e) {
      logger.log(Level.WARNING, "[SpyAPI] Failed to read Enhancer.spyImpl: {0}", e.getMessage());
      return null;
    }
  }

  /**
   * 反射创建 SpyImpl 实例
   */
  @Nullable
  private static Object tryCreateSpyImpl(ClassLoader arthasLoader) {
    try {
      Class<?> spyImplClass = arthasLoader.loadClass("com.taobao.arthas.core.advisor.SpyImpl");
      java.lang.reflect.Constructor<?> ctor = spyImplClass.getDeclaredConstructor();
      ctor.setAccessible(true);
      Object value = ctor.newInstance();
      logger.log(
          Level.FINE,
          "[SpyAPI] Created SpyImpl via reflection: class={0}",
          value.getClass().getName());
      return value;
    } catch (ReflectiveOperationException | RuntimeException e) {
      logger.log(
          Level.FINE, "[SpyAPI] Failed to create SpyImpl via reflection: {0}", e.getMessage());
      return null;
    }
  }

  // ===== 内部类 =====

  /**
   * SpyAPI 反射访问快照
   *
   * <p>统一管理 SpyAPI 的反射访问，避免反射细节散落。
   */
  public static final class SpyApiSnapshot {

    @Nullable private final Class<?> spyApiClass;
    @Nullable private final Field spyInstanceField;
    @Nullable private final Object spyInstance;
    @Nullable private final Method setSpyMethod;
    @Nullable private final Class<?> setSpyParamType;
    @Nullable private final String diagnosticMessage;

    private SpyApiSnapshot(
        @Nullable Class<?> spyApiClass,
        @Nullable Field spyInstanceField,
        @Nullable Object spyInstance,
        @Nullable Method setSpyMethod,
        @Nullable Class<?> setSpyParamType,
        @Nullable String diagnosticMessage) {
      this.spyApiClass = spyApiClass;
      this.spyInstanceField = spyInstanceField;
      this.spyInstance = spyInstance;
      this.setSpyMethod = setSpyMethod;
      this.setSpyParamType = setSpyParamType;
      this.diagnosticMessage = diagnosticMessage;
    }

    /**
     * 读取当前 SpyAPI 状态快照
     *
     * @return 快照
     */
    public static SpyApiSnapshot read() {
      try {
        Class<?> spyApiClass = Class.forName(SPY_API_CLASS, true, null);
        Field spyInstanceField = spyApiClass.getDeclaredField("spyInstance");
        spyInstanceField.setAccessible(true);
        Object spyInstance = spyInstanceField.get(null);

        // 扫描 setSpy 方法（不写死参数类型）
        Method setSpyMethod = null;
        Class<?> setSpyParamType = null;
        for (Method m : spyApiClass.getMethods()) {
          if (!"setSpy".equals(m.getName())) {
            continue;
          }
          if (!Modifier.isStatic(m.getModifiers())) {
            continue;
          }
          Class<?>[] params = m.getParameterTypes();
          if (params.length != 1) {
            continue;
          }
          setSpyMethod = m;
          setSpyParamType = params[0];
          break;
        }

        return new SpyApiSnapshot(
            spyApiClass, spyInstanceField, spyInstance, setSpyMethod, setSpyParamType, null);
      } catch (ClassNotFoundException e) {
        return new SpyApiSnapshot(null, null, null, null, null, e.getMessage());
      } catch (NoSuchFieldException e) {
        return new SpyApiSnapshot(null, null, null, null, null, e.getMessage());
      } catch (ReflectiveOperationException e) {
        return new SpyApiSnapshot(null, null, null, null, null, e.getMessage());
      }
    }

    public boolean isAvailable() {
      return spyApiClass != null && spyInstanceField != null;
    }

    @Nullable
    public String getDiagnosticMessage() {
      return diagnosticMessage;
    }

    @Nullable
    public Class<?> getSpyApiClass() {
      return spyApiClass;
    }

    @Nullable
    public Field getSpyInstanceField() {
      return spyInstanceField;
    }

    @Nullable
    public Object getSpyInstance() {
      return spyInstance;
    }

    public String getSpyInstanceSimpleName() {
      if (spyInstance == null) {
        return "null";
      }
      return spyInstance.getClass().getSimpleName();
    }

    public String getSpyInstanceClassName() {
      if (spyInstance == null) {
        return "null";
      }
      return spyInstance.getClass().getName();
    }

    /**
     * 判断是否已安装 SpyImpl
     *
     * @return 是否是 SpyImpl
     */
    public boolean isSpyImplInstalled() {
      if (spyInstance == null) {
        return false;
      }
      return spyInstance.getClass().getName().endsWith("SpyImpl");
    }

    /**
     * 调用 SpyAPI.setSpy(spyImpl)
     *
     * @param spyImpl SpyImpl 实例
     * @throws ReflectiveOperationException 反射异常
     */
    public void invokeSetSpy(Object spyImpl) throws ReflectiveOperationException {
      if (spyApiClass == null) {
        throw new ClassNotFoundException(SPY_API_CLASS);
      }
      if (setSpyMethod == null || setSpyParamType == null) {
        throw new NoSuchMethodException(
            "public static setSpy(xxx) not found on " + spyApiClass.getName());
      }

      // 类型检查
      if (!setSpyParamType.isInstance(spyImpl)) {
        StringBuilder sb = new StringBuilder();
        sb.append("argument type mismatch. expected=")
            .append(setSpyParamType.getName())
            .append(" but got=")
            .append(spyImpl.getClass().getName());

        // 打印父类链，帮助定位类加载器隔离问题
        sb.append(", superTypes=");
        Class<?> c = spyImpl.getClass();
        boolean first = true;
        while (c != null) {
          if (!first) {
            sb.append(" -> ");
          }
          first = false;
          sb.append(c.getName()).append("@").append(c.getClassLoader());
          c = c.getSuperclass();
        }

        throw new IllegalArgumentException(sb.toString());
      }

      setSpyMethod.invoke(null, spyImpl);
    }
  }
}
