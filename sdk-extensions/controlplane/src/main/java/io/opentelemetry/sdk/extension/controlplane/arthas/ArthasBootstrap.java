/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import io.opentelemetry.sdk.extension.controlplane.core.InstrumentationProvider;
import io.opentelemetry.sdk.extension.controlplane.core.InstrumentationSnapshot;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 启动引导类
 *
 * <p>负责动态加载和启动 Arthas，使用反射机制调用 Arthas API，避免编译时强依赖。
 *
 * <p>支持两种部署模式：
 * <ul>
 *   <li>嵌入式模式：从 classpath 加载 Arthas jar（推荐用于打包发布）
 *   <li>外部加载模式：从指定路径加载 Arthas jar（用于开发测试）
 * </ul>
 */
public final class ArthasBootstrap {

  private static final Logger logger = Logger.getLogger(ArthasBootstrap.class.getName());

  // Arthas 类名常量
  private static final String ARTHAS_BOOTSTRAP_CLASS = "com.taobao.arthas.core.server.ArthasBootstrap";
  private static final String SPY_API_CLASS = "java.arthas.SpyAPI";

  // Arthas jar 资源路径（嵌入式模式）
  private static final String ARTHAS_CORE_JAR_RESOURCE = "/arthas/arthas-core.jar";
  private static final String ARTHAS_CLIENT_JAR_RESOURCE = "/arthas/arthas-client.jar";
  private static final String ARTHAS_SPY_JAR_RESOURCE = "/arthas/arthas-spy.jar";

  private final ArthasConfig config;
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean spyLoaded = new AtomicBoolean(false);
  private final AtomicReference<ClassLoader> arthasClassLoader = new AtomicReference<>();
  private final AtomicReference<Object> arthasBootstrapInstance = new AtomicReference<>();

  /** Instrumentation 提供者（用于获取 Instrumentation 及其能力信息） */
  private final InstrumentationProvider instrumentationProvider;

  /** 缓存的 Instrumentation 快照（用于诊断） */
  @Nullable private volatile InstrumentationSnapshot instrumentationSnapshot;

  // 回调接口（预留给未来扩展使用）
  @SuppressWarnings("UnusedVariable")
  @Nullable private OutputCallback outputCallback;

  /**
   * 创建 Arthas 启动引导器
   *
   * @param config Arthas 配置
   */
  public ArthasBootstrap(ArthasConfig config) {
    this(config, InstrumentationProvider.getInstance());
  }

  /**
   * 创建 Arthas 启动引导器（带 Instrumentation）
   *
   * @param config Arthas 配置
   * @param instrumentation Instrumentation 实例
   * @deprecated 推荐使用 {@link #ArthasBootstrap(ArthasConfig, InstrumentationProvider)}
   */
  @Deprecated
  public ArthasBootstrap(ArthasConfig config, @Nullable Instrumentation instrumentation) {
    this.config = config;
    this.instrumentationProvider = InstrumentationProvider.getInstance();
    // 如果显式传入了 Instrumentation，设置到 Provider 中
    if (instrumentation != null) {
      this.instrumentationProvider.setInstrumentation(instrumentation);
    }
  }

  /**
   * 创建 Arthas 启动引导器（推荐构造函数）
   *
   * <p>使用 InstrumentationProvider 获取 Instrumentation，解耦获取逻辑。
   *
   * @param config Arthas 配置
   * @param provider Instrumentation 提供者
   */
  public ArthasBootstrap(ArthasConfig config, InstrumentationProvider provider) {
    this.config = config;
    this.instrumentationProvider = provider;
  }

  /**
   * 设置 Instrumentation 实例
   *
   * <p>Instrumentation 用于：
   * <ul>
   *   <li>加载 SpyAPI 到 Bootstrap ClassLoader</li>
   *   <li>传递给 Arthas 进行字节码增强</li>
   * </ul>
   *
   * @param instrumentation Instrumentation 实例
   * @deprecated 推荐在构造时通过 InstrumentationProvider 设置
   */
  @Deprecated
  public void setInstrumentation(@Nullable Instrumentation instrumentation) {
    if (instrumentation != null) {
      this.instrumentationProvider.setInstrumentation(instrumentation);
      this.instrumentationSnapshot = null; // 清除缓存
    }
  }

  /**
   * 获取 Instrumentation 实例
   *
   * @return Instrumentation 实例，可能为 null
   */
  @Nullable
  public Instrumentation getInstrumentation() {
    return instrumentationProvider.getInstrumentation();
  }

  /**
   * 获取 Instrumentation 快照
   *
   * <p>包含 Instrumentation 实例、能力信息和诊断信息，用于问题排查。
   *
   * @return Instrumentation 快照
   */
  public InstrumentationSnapshot getInstrumentationSnapshot() {
    InstrumentationSnapshot snapshot = this.instrumentationSnapshot;
    if (snapshot == null) {
      snapshot = instrumentationProvider.getSnapshot();
      this.instrumentationSnapshot = snapshot;
    }
    return snapshot;
  }

  /**
   * 检查是否具备字节码增强能力
   *
   * <p>只有具备增强能力时，trace/watch/stack 等命令才能正常工作。
   *
   * @return 是否具备增强能力
   */
  public boolean hasEnhancementCapability() {
    return getInstrumentationSnapshot().hasEnhancementCapability();
  }

  /**
   * 设置输出回调
   *
   * @param callback 输出回调
   */
  public void setOutputCallback(OutputCallback callback) {
    this.outputCallback = callback;
  }

  /**
   * 初始化 Arthas（加载依赖但不启动）
   *
   * @return 是否初始化成功
   */
  public boolean initialize() {
    if (initialized.get()) {
      logger.log(Level.FINE, "Arthas already initialized");
      return true;
    }

    try {
      // 1. 首先加载 SpyAPI 到 Bootstrap ClassLoader
      if (!loadSpyApi()) {
        logger.log(Level.WARNING, "Failed to load SpyAPI, Arthas may not work properly");
        // 继续执行，因为在某些场景下可能不需要 SpyAPI
      }

      // 2. 创建 Arthas ClassLoader
      ClassLoader loader = createArthasClassLoader();
      if (loader == null) {
        logger.log(Level.WARNING, "Failed to create Arthas ClassLoader");
        return false;
      }

      arthasClassLoader.set(loader);
      initialized.set(true);

      logger.log(Level.INFO, "Arthas initialized successfully");
      return true;

    } catch (RuntimeException e) {
      logger.log(Level.SEVERE, "Failed to initialize Arthas", e);
      return false;
    }
  }

  /**
   * 加载 SpyAPI 到 Bootstrap ClassLoader
   *
   * <p>SpyAPI 必须在 Bootstrap ClassLoader 中加载，这样才能被所有类访问。
   * 使用 {@link Instrumentation#appendToBootstrapClassLoaderSearch(JarFile)} 实现。
   *
   * @return 是否成功加载
   */
  private boolean loadSpyApi() {
    // 检查是否已经加载
    if (spyLoaded.get()) {
      logger.log(Level.FINE, "SpyAPI already loaded");
      return true;
    }

    // 检查 SpyAPI 是否已在 Bootstrap ClassLoader 中
    if (isSpyApiLoaded()) {
      logger.log(Level.INFO, "SpyAPI already present in Bootstrap ClassLoader");
      spyLoaded.set(true);
      // 验证 SpyAPI 状态并记录诊断信息（兼容 Arthas 4.0.3+）
      resetSpyApiToNopSpyInstance();
      return true;
    }

    // 通过 Provider 获取 Instrumentation 快照
    InstrumentationSnapshot snapshot = getInstrumentationSnapshot();
    Instrumentation inst = snapshot.getInstrumentation();
    
    // 检查 Instrumentation 是否可用
    if (inst == null) {
      logger.log(Level.WARNING, 
          "Instrumentation not available, cannot load SpyAPI to Bootstrap ClassLoader. " +
          "Diagnostic: {0}", snapshot.getDiagnosticMessage());
      return false;
    }

    try {
      // 查找 arthas-spy.jar
      File spyJarFile = findSpyJar();
      if (spyJarFile == null) {
        logger.log(Level.WARNING, "arthas-spy.jar not found");
        return false;
      }

      // 使用 Instrumentation 将 spy jar 添加到 Bootstrap ClassLoader
      logger.log(Level.INFO, "Loading SpyAPI from: {0}", spyJarFile.getAbsolutePath());
      inst.appendToBootstrapClassLoaderSearch(new JarFile(spyJarFile));

      // 验证加载成功
      if (isSpyApiLoaded()) {
        spyLoaded.set(true);
        logger.log(Level.INFO, "SpyAPI loaded to Bootstrap ClassLoader successfully");
        
        // 验证 SpyAPI 状态并记录诊断信息
        // 
        // Arthas 4.0.3+ 说明：
        // - SpyImpl 通过 Enhancer 类的静态初始化块中的 SpyAPI.setSpy() 设置
        // - Enhancer 类只有在执行 trace/watch/stack 命令时才会被加载
        // - 因此此时 spyInstance 是 NopSpy 是正常的
        // - 这里只做状态验证和诊断日志记录
        resetSpyApiToNopSpyInstance();
        
        return true;
      } else {
        logger.log(Level.WARNING, "SpyAPI jar added but class not found");
        return false;
      }

    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to load SpyAPI: {0}", e.getMessage());
      return false;
    }
  }

  /**
   * 验证 SpyAPI 状态并记录诊断信息
   *
   * <p>【兼容 Arthas 4.0.3+】此方法用于验证 SpyAPI 的初始状态。
   *
   * <p>Arthas 4.0.3 的 SpyAPI 结构变化：
   * <ul>
   *   <li>NopSpy 变成了内部类 SpyAPI$NopSpy，不再是独立类 java.arthas.NopSpy</li>
   *   <li>使用 NOPSPY 静态常量，不再是 NopSpy.INSTANCE</li>
   *   <li>setSpy() 方法是无条件设置，没有幂等性检查</li>
   *   <li>spyInstance 在静态初始化时就等于 NOPSPY（同一个对象引用）</li>
   * </ul>
   *
   * <p>重要说明：
   * <ul>
   *   <li>Arthas 4.0.3 中，SpyAPI.setSpy(spyImpl) 是在 Enhancer 类的静态初始化块中调用的</li>
   *   <li>Enhancer 类只有在用户执行 trace/watch/stack 等命令时才会被加载</li>
   *   <li>因此，在执行这些命令之前，spyInstance 自然是 NopSpy，这是正常的</li>
   * </ul>
   */
  private static void resetSpyApiToNopSpyInstance() {
    try {
      // 1. 从 Bootstrap ClassLoader 加载 SpyAPI 类
      Class<?> spyApiClass = Class.forName(SPY_API_CLASS, true, null);
      
      // 2. 获取 SpyAPI.spyInstance 字段
      java.lang.reflect.Field spyInstanceField = spyApiClass.getDeclaredField("spyInstance");
      spyInstanceField.setAccessible(true);
      
      // 3. 检查当前值
      Object currentValue = spyInstanceField.get(null);
      String currentClassName = currentValue != null ? currentValue.getClass().getSimpleName() : "null";
      
      // 4. 记录当前状态（诊断用）
      if ("NopSpy".equals(currentClassName)) {
        // 这是正常的初始状态
        // Arthas 4.0.3 中，SpyImpl 会在 Enhancer 类加载时通过 setSpy() 设置
        // Enhancer 类只有在执行 trace/watch/stack 命令时才会被加载
        logger.log(Level.INFO, 
            "[SpyAPI] Current spyInstance is NopSpy (initial state). " +
            "SpyImpl will be set when Enhancer class is loaded (on first trace/watch/stack command).");
        
        // 5. 尝试获取 NOPSPY 常量，验证 Arthas 4.0.3+ 结构
        try {
          java.lang.reflect.Field nopspyField = spyApiClass.getDeclaredField("NOPSPY");
          nopspyField.setAccessible(true);
          Object nopspyValue = nopspyField.get(null);
          
          if (currentValue == nopspyValue) {
            logger.log(Level.FINE, 
                "[SpyAPI] Verified: spyInstance == NOPSPY (Arthas 4.0.3+ detected). " +
                "No reset needed, setSpy() will work correctly.");
          } else {
            // 旧版本 Arthas 或异常情况：spyInstance 不等于 NOPSPY
            // 尝试重置
            logger.log(Level.INFO, 
                "[SpyAPI] spyInstance != NOPSPY, resetting to NOPSPY for compatibility.");
            spyInstanceField.set(null, nopspyValue);
          }
        } catch (NoSuchFieldException e) {
          // 没有 NOPSPY 字段，可能是旧版本 Arthas
          logger.log(Level.FINE, 
              "[SpyAPI] NOPSPY field not found, trying legacy NopSpy.INSTANCE");
          resetSpyApiLegacy(spyInstanceField, currentValue);
        }
        
      } else if ("SpyImpl".equals(currentClassName)) {
        // SpyImpl 已经设置，说明 Enhancer 已加载，一切正常
        logger.log(Level.INFO, 
            "[SpyAPI] spyInstance is already SpyImpl. " +
            "Arthas enhancement is working correctly.");
      } else {
        // 其他类型，记录警告但不干预
        logger.log(Level.WARNING, 
            "[SpyAPI] Unexpected spyInstance type: {0}. " +
            "Arthas enhancement may not work correctly.",
            currentClassName);
      }
      
    } catch (ClassNotFoundException e) {
      // SpyAPI 类不存在
      logger.log(Level.WARNING, 
          "[SpyAPI] SpyAPI class not found in Bootstrap ClassLoader: {0}. " +
          "arthas-spy.jar may not be loaded correctly.",
          e.getMessage());
    } catch (NoSuchFieldException e) {
      // spyInstance 字段不存在，可能是 Arthas 版本变更
      logger.log(Level.WARNING, 
          "[SpyAPI] spyInstance field not found: {0}. " +
          "This may be due to Arthas version incompatibility.",
          e.getMessage());
    } catch (ReflectiveOperationException e) {
      // 其他反射异常
      logger.log(Level.WARNING, 
          "[SpyAPI] Failed to check/reset spyInstance: {0}.",
          e.getMessage());
    }
  }
  
  /**
   * 旧版本 Arthas 的 SpyAPI 重置逻辑
   *
   * <p>旧版本使用独立的 java.arthas.NopSpy 类和 NopSpy.INSTANCE 常量。
   */
  private static void resetSpyApiLegacy(
      java.lang.reflect.Field spyInstanceField,
      Object currentValue) {
    try {
      // 尝试加载旧版本的 NopSpy 类
      Class<?> nopSpyClass = Class.forName("java.arthas.NopSpy", true, null);
      java.lang.reflect.Field instanceField = nopSpyClass.getDeclaredField("INSTANCE");
      instanceField.setAccessible(true);
      Object nopSpyInstance = instanceField.get(null);
      
      if (nopSpyInstance == null) {
        logger.log(Level.WARNING, 
            "[SpyAPI] Legacy NopSpy.INSTANCE is null, cannot reset spyInstance");
        return;
      }
      
      if (currentValue == nopSpyInstance) {
        logger.log(Level.FINE, 
            "[SpyAPI] Legacy: spyInstance is already NopSpy.INSTANCE, no reset needed");
        return;
      }
      
      // 重置为 NopSpy.INSTANCE
      spyInstanceField.set(null, nopSpyInstance);
      logger.log(Level.INFO, 
          "[SpyAPI] Legacy: Reset spyInstance to NopSpy.INSTANCE.");
      
    } catch (ClassNotFoundException e) {
      // 既没有 NOPSPY 也没有 java.arthas.NopSpy，版本不兼容
      logger.log(Level.INFO, 
          "[SpyAPI] Neither NOPSPY nor java.arthas.NopSpy found. " +
          "Assuming Arthas 4.0.3+ with setSpy() that works without reset.");
    } catch (ReflectiveOperationException e) {
      logger.log(Level.WARNING, 
          "[SpyAPI] Legacy reset failed: {0}.", e.getMessage());
    }
  }

  /**
   * 检查 SpyAPI 是否已在 Bootstrap ClassLoader 中
   *
   * @return 是否已加载
   */
  private static boolean isSpyApiLoaded() {
    try {
      Class.forName(SPY_API_CLASS, false, null);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * 查找 arthas-spy.jar 文件
   *
   * @return jar 文件，未找到返回 null
   */
  @Nullable
  private File findSpyJar() {
    // 1. 从配置的外部路径查找
    String libPath = config.getLibPath();
    if (libPath != null && !libPath.isEmpty()) {
      File spyJar = new File(libPath, "arthas-spy.jar");
      if (spyJar.exists()) {
        return spyJar;
      }
    }

    // 2. 从 classpath 资源提取
    try {
      Path tempDir = Files.createTempDirectory("arthas-spy-");
      tempDir.toFile().deleteOnExit();

      Path spyJar = extractResource(ARTHAS_SPY_JAR_RESOURCE, tempDir, "arthas-spy.jar");
      if (spyJar != null) {
        return spyJar.toFile();
      }
    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to extract arthas-spy.jar: {0}", e.getMessage());
    }

    return null;
  }

  /**
   * 启动 Arthas 服务
   *
   * @return 启动结果
   */
  public StartResult start() {
    if (running.get()) {
      return StartResult.success("Arthas already running");
    }

    if (!initialized.get() && !initialize()) {
      return StartResult.failed("Failed to initialize Arthas");
    }

    ClassLoader loader = arthasClassLoader.get();
    if (loader == null) {
      return StartResult.failed("Arthas ClassLoader not available");
    }

    try {
      // 使用反射启动 Arthas
      Object bootstrap = startArthasViaReflection(loader);
      if (bootstrap != null) {
        arthasBootstrapInstance.set(bootstrap);
        running.set(true);
        logger.log(Level.INFO, "Arthas started successfully");
        return StartResult.success("Arthas started");
      } else {
        return StartResult.failed("Failed to create Arthas bootstrap instance");
      }

    } catch (RuntimeException e) {
      logger.log(Level.SEVERE, "Failed to start Arthas", e);
      return StartResult.failed("Failed to start Arthas: " + e.getMessage());
    }
  }

  /**
   * 停止 Arthas 服务
   *
   * @return 是否成功停止
   */
  public boolean stop() {
    if (!running.get()) {
      logger.log(Level.FINE, "Arthas not running");
      return true;
    }

    try {
      Object bootstrap = arthasBootstrapInstance.get();
      if (bootstrap != null) {
        stopArthasViaReflection(bootstrap);
        arthasBootstrapInstance.set(null);
      }

      running.set(false);
      logger.log(Level.INFO, "Arthas stopped successfully");
      return true;

    } catch (RuntimeException e) {
      logger.log(Level.SEVERE, "Error stopping Arthas", e);
      running.set(false);
      return false;
    }
  }

  /**
   * 检查 Arthas 是否正在运行
   *
   * <p>【关键修复】不仅检查我们本地的 running 标志，还要检查 Arthas 真实的运行状态。
   *
   * <p>背景：当用户在 Arthas Terminal 里执行 stop 命令时，Arthas 内部直接调用 destroy()，
   * 而不是通过我们的 stop() 方法。这导致我们的 running 标志没有被更新，但 Arthas 实际已停止。
   *
   * <p>通过检查 Arthas 实例的 isBind() 方法（如果存在），可以确认 Arthas 是否真正在运行。
   *
   * @return 运行状态
   */
  public boolean isRunning() {
    // 1. 首先检查本地标志
    if (!running.get()) {
      return false;
    }

    // 2. 检查 Arthas 实例是否仍然存在
    Object bootstrap = arthasBootstrapInstance.get();
    if (bootstrap == null) {
      return false;
    }

    // 3. 检查 Arthas 真实的运行状态（调用 isBind() 方法）
    // isBind() 在 Arthas 被 destroy() 后会返回 false
    return isArthasActuallyRunning(bootstrap);
  }

  /**
   * 检查 Arthas 实例是否真正在运行
   *
   * <p>通过反射调用 Arthas Bootstrap 的 isBind() 方法。
   * isBind() 在 shellServer.listen() 成功后返回 true，在 destroy() 后返回 false。
   *
   * @param bootstrap Arthas Bootstrap 实例
   * @return 是否真正在运行
   */
  private static boolean isArthasActuallyRunning(Object bootstrap) {
    try {
      // 尝试调用 isBind() 方法
      java.lang.reflect.Method isBindMethod = bootstrap.getClass().getMethod("isBind");
      Object result = isBindMethod.invoke(bootstrap);
      if (result instanceof Boolean) {
        return (Boolean) result;
      }
    } catch (NoSuchMethodException e) {
      // isBind() 方法不存在，可能是旧版本 Arthas
      logger.log(Level.FINE, "isBind() method not found, assuming running");
    } catch (ReflectiveOperationException e) {
      logger.log(Level.WARNING, "Failed to check Arthas isBind(): {0}", e.getMessage());
    }
    // 如果无法确定，保守返回 true
    return true;
  }

  /**
   * 检查 Arthas 是否已初始化
   *
   * @return 初始化状态
   */
  public boolean isInitialized() {
    return initialized.get();
  }

  /**
   * 获取 Arthas ClassLoader
   *
   * @return ClassLoader
   */
  @Nullable
  public ClassLoader getArthasClassLoader() {
    return arthasClassLoader.get();
  }

  /**
   * 获取 Arthas Bootstrap 实例
   *
   * @return Bootstrap 实例
   */
  @Nullable
  public Object getBootstrapInstance() {
    return arthasBootstrapInstance.get();
  }

  /** 销毁资源 */
  public void destroy() {
    stop();

    ClassLoader loader = arthasClassLoader.getAndSet(null);
    if (loader instanceof URLClassLoader) {
      try {
        ((URLClassLoader) loader).close();
      } catch (IOException e) {
        logger.log(Level.WARNING, "Error closing Arthas ClassLoader", e);
      }
    }

    initialized.set(false);
    logger.log(Level.INFO, "Arthas bootstrap destroyed");
  }

  // ===== 私有方法 =====

  /**
   * 创建 Arthas ClassLoader
   *
   * @return ClassLoader，如果创建失败返回 null
   */
  @Nullable
  private ClassLoader createArthasClassLoader() {
    // 1. 首先尝试从配置的外部路径加载
    String libPath = config.getLibPath();
    if (libPath != null && !libPath.isEmpty()) {
      ClassLoader loader = loadFromExternalPath(libPath);
      if (loader != null) {
        logger.log(Level.INFO, "Loaded Arthas from external path: {0}", libPath);
        return loader;
      }
    }

    // 2. 尝试从 classpath 资源加载
    ClassLoader loader = loadFromClasspathResources();
    if (loader != null) {
      logger.log(Level.INFO, "Loaded Arthas from classpath resources");
      return loader;
    }

    // 3. 如果都失败，返回当前 ClassLoader（假设 Arthas 已在 classpath 中）
    logger.log(Level.INFO, "Using current ClassLoader for Arthas (assuming Arthas is in classpath)");
    return getClass().getClassLoader();
  }

  /**
   * 从外部路径加载 Arthas jar
   *
   * @param libPath 库路径
   * @return ClassLoader
   */
  @Nullable
  private ClassLoader loadFromExternalPath(String libPath) {
    try {
      File libDir = new File(libPath);
      if (!libDir.exists() || !libDir.isDirectory()) {
        logger.log(Level.WARNING, "Arthas lib path does not exist or is not a directory: {0}", libPath);
        return null;
      }

      File[] jars = libDir.listFiles((dir, name) -> name.endsWith(".jar"));
      if (jars == null || jars.length == 0) {
        logger.log(Level.WARNING, "No jar files found in Arthas lib path: {0}", libPath);
        return null;
      }

      URL[] urls = new URL[jars.length];
      for (int i = 0; i < jars.length; i++) {
        urls[i] = jars[i].toURI().toURL();
        logger.log(Level.FINE, "Adding Arthas jar: {0}", jars[i].getAbsolutePath());
      }

      // 创建 URLClassLoader 用于动态加载外部 Arthas jar
      @SuppressWarnings("BanClassLoader")
      URLClassLoader loader = new URLClassLoader(urls, getClass().getClassLoader());
      return loader;

    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to load Arthas from external path", e);
      return null;
    }
  }

  /**
   * 从 classpath 资源加载 Arthas jar
   *
   * <p>同时提取 async-profiler native library 到 arthas-home 目录，
   * 使 Arthas profiler 命令能够正常工作。
   *
   * @return ClassLoader
   */
  @Nullable
  private ClassLoader loadFromClasspathResources() {
    try {
      // 创建临时目录存放解压的 jar
      Path tempDir = Files.createTempDirectory("arthas-");
      tempDir.toFile().deleteOnExit();

      // 尝试解压 arthas-core.jar
      Path coreJar = extractResource(ARTHAS_CORE_JAR_RESOURCE, tempDir, "arthas-core.jar");
      if (coreJar == null) {
        logger.log(Level.FINE, "Arthas core jar not found in classpath resources");
        return null;
      }

      // 尝试解压 arthas-client.jar（可选）
      Path clientJar = extractResource(ARTHAS_CLIENT_JAR_RESOURCE, tempDir, "arthas-client.jar");

      // 【关键】提取 async-profiler native library 到 arthas-home 目录
      // 使 Arthas profiler 命令能够找到 libasyncProfiler.so
      // 失败不阻塞 Arthas 启动，仅影响 profiler 命令
      extractAsyncProfilerLibrary(tempDir);

      // 构建 URL 数组
      int urlCount = clientJar != null ? 2 : 1;
      URL[] urls = new URL[urlCount];
      urls[0] = coreJar.toUri().toURL();
      if (clientJar != null) {
        urls[1] = clientJar.toUri().toURL();
      }

      // 创建 URLClassLoader 用于动态加载 classpath 中的 Arthas jar
      @SuppressWarnings("BanClassLoader")
      URLClassLoader loader = new URLClassLoader(urls, getClass().getClassLoader());
      return loader;

    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to load Arthas from classpath resources", e);
      return null;
    }
  }

  /**
   * 提取 async-profiler native library 到 arthas-home 目录
   *
   * <p>Arthas profiler 命令查找 async-profiler 库的路径是：
   * <code>{arthas-home}/async-profiler/libasyncProfiler-{platform}.{ext}</code>
   *
   * <p>失败不阻塞 Arthas 启动，仅记录警告日志。profiler 命令将不可用。
   *
   * @param arthasHome Arthas 运行时根目录
   */
  private static void extractAsyncProfilerLibrary(Path arthasHome) {
    try {
      // 使用独立的 AsyncProfilerResourceExtractor 提取库文件
      io.opentelemetry.sdk.extension.controlplane.profiler.AsyncProfilerResourceExtractor
          .ExtractionResult result =
              io.opentelemetry.sdk.extension.controlplane.profiler.AsyncProfilerResourceExtractor
                  .extractTo(arthasHome);

      if (result.isSuccess()) {
        if (result.isSkipped()) {
          logger.log(Level.FINE,
              "[ARTHAS] async-profiler library already exists: {0}",
              result.getLibraryPath());
        } else {
          logger.log(Level.INFO,
              "[ARTHAS] async-profiler library extracted: {0}",
              result.getLibraryPath());
        }
      } else {
        // 提取失败，记录警告但不阻塞
        logger.log(Level.WARNING,
            "[ARTHAS] Failed to extract async-profiler library: {0}. "
                + "Arthas profiler command will not work.",
            result.getMessage());
      }
    } catch (RuntimeException e) {
      // 捕获所有异常，防止影响 Arthas 启动
      logger.log(Level.WARNING,
          "[ARTHAS] Error extracting async-profiler library: {0}. "
              + "Arthas profiler command will not work.",
          e.getMessage());
    }
  }

  /**
   * 解压资源到临时目录
   *
   * @param resourcePath 资源路径
   * @param targetDir 目标目录
   * @param fileName 文件名
   * @return 解压后的文件路径，如果资源不存在返回 null
   */
  @Nullable
  private Path extractResource(String resourcePath, Path targetDir, String fileName) {
    try (InputStream is = getClass().getResourceAsStream(resourcePath)) {
      if (is == null) {
        return null;
      }

      Path targetFile = targetDir.resolve(fileName);
      Files.copy(is, targetFile, StandardCopyOption.REPLACE_EXISTING);
      targetFile.toFile().deleteOnExit();

      return targetFile;
    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to extract resource: {0}", resourcePath);
      return null;
    }
  }

  /**
   * 通过反射启动 Arthas
   *
   * <p>使用正确的 getInstance(Instrumentation, Map) 方法签名启动 Arthas。
   *
   * @param loader Arthas ClassLoader
   * @return Arthas Bootstrap 实例
   */
  @Nullable
  private Object startArthasViaReflection(ClassLoader loader) {
    // 通过 Provider 获取 Instrumentation 快照
    InstrumentationSnapshot snapshot = getInstrumentationSnapshot();
    Instrumentation inst = snapshot.getInstrumentation();
    
    // 检查 Instrumentation
    if (inst == null) {
      logger.log(Level.WARNING, 
          "Instrumentation not available, trying legacy startup method. Diagnostic: {0}",
          snapshot.getDiagnosticMessage());
      return startArthasViaReflectionLegacy(loader);
    }
    
    // 记录 Instrumentation 能力信息
    logger.log(Level.INFO, 
        "[Arthas] Instrumentation capability: {0}", snapshot.toSummary());
    
    // 如果不支持 retransform，发出警告（trace/watch/stack 将无法工作）
    if (!snapshot.supportsRetransform()) {
      logger.log(Level.WARNING, 
          "[Arthas] WARNING: Instrumentation does not support retransform! " +
          "trace/watch/stack commands will NOT work.");
    }

    try {
      // 构建配置 Map
      Map<String, String> configMap = buildArthasConfigMap();

      // 使用反射创建 Arthas Bootstrap
      Class<?> bootstrapClass = loader.loadClass(ARTHAS_BOOTSTRAP_CLASS);
      java.lang.reflect.Method getInstanceMethod;
      long t0 = System.nanoTime();
      getInstanceMethod = bootstrapClass.getMethod("getInstance", Instrumentation.class, Map.class);
      long t1 = System.nanoTime();

      logger.log(
          Level.INFO,
          "Arthas reflection prepared: thread={0}, loadClass+getMethodCostMs={1}",
          new Object[] {Thread.currentThread().getName(), (t1 - t0) / 1_000_000});

      // 关键点：invoke 可能阻塞/死锁，增加前后打点
      long invokeStart = System.nanoTime();
      logger.log(
          Level.INFO,
          "Arthas reflection invoking getInstance... thread={0}",
          Thread.currentThread().getName());
      Object bootstrap = getInstanceMethod.invoke(null, inst, configMap);
      long invokeEnd = System.nanoTime();

      logger.log(
          Level.INFO,
          "Arthas reflection invoked getInstance ok: thread={0}, invokeCostMs={1}",
          new Object[] {Thread.currentThread().getName(), (invokeEnd - invokeStart) / 1_000_000});

      logger.log(
          Level.INFO,
          "Arthas Bootstrap created with Instrumentation, config keys: {0}",
          configMap.keySet());
      return bootstrap;

    } catch (ClassNotFoundException e) {
      logger.log(Level.SEVERE, "Arthas classes not found in ClassLoader");
      return null;

    } catch (NoSuchMethodException e) {
      // 如果没有 getInstance(Instrumentation, Map) 方法，尝试旧版方法
      logger.log(Level.INFO, 
          "getInstance(Instrumentation, Map) not found, trying legacy method");
      return startArthasViaReflectionLegacy(loader);

    } catch (ReflectiveOperationException e) {
      logger.log(Level.SEVERE, "Failed to start Arthas via reflection", e);
      return null;
    }
  }

  /**
   * 使用旧版方法启动 Arthas（兼容旧版 Arthas）
   *
   * @param loader Arthas ClassLoader
   * @return Arthas Bootstrap 实例
   */
  @Nullable
  private Object startArthasViaReflectionLegacy(ClassLoader loader) {
    try {
      // 获取当前 JVM 的 PID
      long pid = getCurrentPid();

      // 构建配置字符串
      Map<String, String> configMap = buildArthasConfigMap();
      StringBuilder configStr = new StringBuilder();
      for (Map.Entry<String, String> entry : configMap.entrySet()) {
        if (configStr.length() > 0) {
          configStr.append(";");
        }
        configStr.append(entry.getKey()).append("=").append(entry.getValue());
      }

      // 加载 ArthasBootstrap 类
      Class<?> bootstrapClass = loader.loadClass(ARTHAS_BOOTSTRAP_CLASS);

      // 获取 getInstance(long, String) 方法
      Method getInstanceMethod = bootstrapClass.getMethod("getInstance", long.class, String.class);

      // 调用 getInstance
      Object bootstrap = getInstanceMethod.invoke(null, pid, configStr.toString());

      logger.log(Level.INFO, "Arthas Bootstrap created (legacy) for PID: {0}", pid);
      return bootstrap;

    } catch (ClassNotFoundException e) {
      logger.log(Level.SEVERE, "Arthas classes not found in ClassLoader");
      return null;

    } catch (ReflectiveOperationException e) {
      logger.log(Level.SEVERE, "Failed to start Arthas via legacy reflection", e);
      return null;
    }
  }

  /**
   * 通过反射停止 Arthas
   *
   * <p>【关键修复】除了调用 destroy() 外，还必须重置 Arthas 的静态单例字段。
   * 否则下次调用 getInstance() 会返回"已死"的旧实例（TunnelClient 已 shutdown），
   * 导致 tunnel 永远无法重新连接。
   *
   * @param bootstrap Arthas Bootstrap 实例
   */
  private static void stopArthasViaReflection(Object bootstrap) {
    try {
      // 1. 调用 destroy() 停止内部组件（包括 TunnelClient）
      Method destroyMethod = bootstrap.getClass().getMethod("destroy");
      destroyMethod.invoke(bootstrap);
      logger.log(Level.INFO, "Arthas destroyed via reflection");

      // 2.【关键】重置 Arthas 静态单例字段，使下次 getInstance() 能重新创建实例
      // Arthas 官方 ArthasBootstrap 使用静态变量 arthasBootstrap 保存单例
      resetArthasSingletonField(bootstrap.getClass());

    } catch (ReflectiveOperationException e) {
      logger.log(Level.WARNING, "Failed to stop Arthas via reflection", e);
    }
  }

  /**
   * 重置 Arthas 静态单例字段
   *
   * <p>Arthas 官方的 ArthasBootstrap 是单例模式，静态字段名可能因版本而异。
   * 常见的字段名包括：arthasBootstrap, INSTANCE, instance 等。
   *
   * @param bootstrapClass Arthas Bootstrap 类
   */
  private static void resetArthasSingletonField(Class<?> bootstrapClass) {
    // 按优先级尝试不同的字段名
    String[] possibleFieldNames = {"arthasBootstrap", "INSTANCE", "instance"};

    for (String fieldName : possibleFieldNames) {
      try {
        java.lang.reflect.Field instanceField = bootstrapClass.getDeclaredField(fieldName);
        instanceField.setAccessible(true);

        // 检查是否是静态字段
        if (java.lang.reflect.Modifier.isStatic(instanceField.getModifiers())) {
          Object oldValue = instanceField.get(null);
          instanceField.set(null, null);
          logger.log(
              Level.INFO,
              "Arthas static singleton field ''{0}'' reset (was: {1})",
              new Object[] {fieldName, oldValue != null ? "non-null" : "null"});
          return; // 成功重置，退出
        }
      } catch (NoSuchFieldException e) {
        // 该字段名不存在，尝试下一个
        logger.log(Level.FINE, "Field ''{0}'' not found in ArthasBootstrap", fieldName);
      } catch (ReflectiveOperationException e) {
        logger.log(
            Level.WARNING,
            "Failed to reset Arthas singleton field ''{0}'': {1}",
            new Object[] {fieldName, e.getMessage()});
      }
    }

    // 如果所有尝试都失败，输出警告
    logger.log(
        Level.WARNING,
        "Cannot find Arthas singleton field to reset. " +
        "Subsequent attach may fail due to stale singleton instance. " +
        "Tried fields: {0}",
        java.util.Arrays.toString(possibleFieldNames));
  }

  /**
   * 构建 Arthas 配置 Map
   *
   * <p>【模式2改造】由 Arthas 内部 TunnelClient 负责 tunnel 连接，
   * OTel 只负责配置传递和状态观测。
   *
   * <p>重要：配置键名必须与 Arthas Configure 类中的属性名一致（带 arthas. 前缀）：
   * <ul>
   *   <li>arthas.tunnelServer: Tunnel Server 地址（可在 URL 中携带 token）</li>
   *   <li>arthas.agentId: Agent 标识（支持重连复用）</li>
   *   <li>arthas.appName: 应用名称</li>
   * </ul>
   *
   * <p>注意：getInstance(Instrumentation, Map) 方法直接使用传入的 Map，不会自动添加 arthas. 前缀。
   * 而 getInstance(Instrumentation, String) 方法会自动添加前缀。因此我们必须手动添加前缀。
   *
   * @return 配置 Map
   */
  private Map<String, String> buildArthasConfigMap() {
    Map<String, String> configMap = new HashMap<>();

    // ===== 基础配置 =====
    // 禁用 Arthas 默认的 telnet 和 http 服务（我们使用 Tunnel）
    configMap.put("arthas.telnetPort", "-1");
    configMap.put("arthas.httpPort", "-1");

    // 设置 session 超时（Java 8 兼容）
    configMap.put(
        "arthas.sessionTimeout",
        String.valueOf(config.getSessionIdleTimeout().toMillis() / 1000));

    // ===== Tunnel 相关配置（模式2核心）=====
    // 【重要修复】配置键必须是 arthas.tunnelServer 而非 tunnel-server
    String tunnelServer = config.getTunnelEndpoint();
    if (tunnelServer != null && !tunnelServer.isEmpty()) {
      configMap.put("arthas.tunnelServer", tunnelServer);
      logger.log(Level.INFO, "Tunnel server configured: {0}", tunnelServer);
    }

    // Agent ID（支持重连复用）
    // 【重要修复】配置键必须是 arthas.agentId 而非 agent-id
    String agentId = getAgentIdForTunnel();
    if (agentId != null && !agentId.isEmpty()) {
      configMap.put("arthas.agentId", agentId);
      logger.log(Level.INFO, "Agent ID configured for tunnel: {0}", agentId);
    }

    // 应用名称（用于 tunnel-server 端识别）
    // 【重要修复】配置键必须是 arthas.appName 而非 app-name
    String appName = config.getAuthToken();
    if (appName != null && !appName.isEmpty()) {
      configMap.put("arthas.appName", appName);
      logger.log(Level.INFO, "App name configured for tunnel: {0}", appName);
    }

    logger.log(Level.INFO, "Arthas config map built: keys={0}", configMap.keySet());

    return configMap;
  }

  /**
   * 获取用于 Tunnel 的 Agent ID
   *
   * <p>复用 OTel AgentIdentityProvider 提供的稳定 ID，
   * 支持 tunnel 重连时复用同一个 agentId。
   *
   * @return Agent ID
   */
  @Nullable
  private static String getAgentIdForTunnel() {
    try {
      // 使用反射获取 AgentIdentityProvider（避免编译期硬依赖）
      Class<?> providerClass = Class.forName(
          "io.opentelemetry.sdk.extension.controlplane.identity.AgentIdentityProvider");
      java.lang.reflect.Method getMethod = providerClass.getMethod("get");
      Object identity = getMethod.invoke(null);
      if (identity != null) {
        java.lang.reflect.Method getAgentIdMethod = identity.getClass().getMethod("getAgentId");
        return (String) getAgentIdMethod.invoke(identity);
      }
    } catch (ReflectiveOperationException e) {
      logger.log(Level.FINE, "Failed to get agent ID from AgentIdentityProvider", e);
    }
    return null;
  }


  /**
   * 获取当前 JVM 的 PID
   *
   * @return PID
   */
  private static long getCurrentPid() {
    // Java 9+ 可以直接用 ProcessHandle.current().pid()
    // 为了兼容 Java 8，使用 ManagementFactory
    String jvmName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
    int atIndex = jvmName.indexOf('@');
    if (atIndex > 0) {
      try {
        return Long.parseLong(jvmName.substring(0, atIndex));
      } catch (NumberFormatException e) {
        // ignore
      }
    }
    return -1;
  }

  // ===== 内部类 =====

  /** 启动结果 */
  public static final class StartResult {
    private final boolean success;
    private final String message;

    private StartResult(boolean success, String message) {
      this.success = success;
      this.message = message;
    }

    public static StartResult success(String message) {
      return new StartResult(/* success= */ true, message);
    }

    public static StartResult failed(String message) {
      return new StartResult(/* success= */ false, message);
    }

    public boolean isSuccess() {
      return success;
    }

    public String getMessage() {
      return message;
    }
  }

  /** 输出回调接口 */
  public interface OutputCallback {
    /**
     * 处理输出数据
     *
     * @param sessionId 会话 ID
     * @param data 输出数据
     */
    void onOutput(String sessionId, byte[] data);
  }

}
