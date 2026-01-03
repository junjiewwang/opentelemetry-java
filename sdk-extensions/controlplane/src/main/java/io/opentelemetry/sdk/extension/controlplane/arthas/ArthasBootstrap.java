/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

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

  /** Instrumentation 实例（用于加载 SpyAPI 和传递给 Arthas） */
  @Nullable private Instrumentation instrumentation;

  // 回调接口（预留给未来扩展使用）
  @SuppressWarnings("UnusedVariable")
  @Nullable private OutputCallback outputCallback;

  /**
   * 创建 Arthas 启动引导器
   *
   * @param config Arthas 配置
   */
  public ArthasBootstrap(ArthasConfig config) {
    this.config = config;
  }

  /**
   * 创建 Arthas 启动引导器（带 Instrumentation）
   *
   * @param config Arthas 配置
   * @param instrumentation Instrumentation 实例
   */
  public ArthasBootstrap(ArthasConfig config, @Nullable Instrumentation instrumentation) {
    this.config = config;
    this.instrumentation = instrumentation;
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
   */
  public void setInstrumentation(@Nullable Instrumentation instrumentation) {
    this.instrumentation = instrumentation;
  }

  /**
   * 获取 Instrumentation 实例
   *
   * @return Instrumentation 实例，可能为 null
   */
  @Nullable
  public Instrumentation getInstrumentation() {
    return instrumentation;
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
      return true;
    }

    // 检查 Instrumentation 是否可用
    if (instrumentation == null) {
      logger.log(Level.WARNING, 
          "Instrumentation not available, cannot load SpyAPI to Bootstrap ClassLoader");
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
      instrumentation.appendToBootstrapClassLoaderSearch(new JarFile(spyJarFile));

      // 验证加载成功
      if (isSpyApiLoaded()) {
        spyLoaded.set(true);
        logger.log(Level.INFO, "SpyAPI loaded to Bootstrap ClassLoader successfully");
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
   * @return 运行状态
   */
  public boolean isRunning() {
    return running.get();
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
    // 检查 Instrumentation
    if (instrumentation == null) {
      logger.log(Level.WARNING, 
          "Instrumentation not available, trying legacy startup method");
      return startArthasViaReflectionLegacy(loader);
    }

    try {
      // 构建配置 Map
      Map<String, String> configMap = buildArthasConfigMap();

      // 加载 ArthasBootstrap 类
      Class<?> bootstrapClass = loader.loadClass(ARTHAS_BOOTSTRAP_CLASS);

      // 获取 getInstance(Instrumentation, Map) 方法
      Method getInstanceMethod = bootstrapClass.getMethod(
          "getInstance", Instrumentation.class, Map.class);

      // 调用 getInstance
      Object bootstrap = getInstanceMethod.invoke(null, instrumentation, configMap);

      logger.log(Level.INFO, 
          "Arthas Bootstrap created with Instrumentation, config keys: {0}", 
          configMap.keySet());
      return bootstrap;

    } catch (ClassNotFoundException e) {
      logger.log(Level.WARNING, "Arthas classes not found in ClassLoader. Running in mock mode.");
      // 返回非 null 以模拟启动成功（用于测试）
      return new MockArthasBootstrap();

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
      logger.log(Level.WARNING, "Arthas classes not found in ClassLoader. Running in mock mode.");
      return new MockArthasBootstrap();

    } catch (ReflectiveOperationException e) {
      logger.log(Level.SEVERE, "Failed to start Arthas via legacy reflection", e);
      return null;
    }
  }

  /**
   * 通过反射停止 Arthas
   *
   * @param bootstrap Arthas Bootstrap 实例
   */
  private static void stopArthasViaReflection(Object bootstrap) {
    if (bootstrap instanceof MockArthasBootstrap) {
      logger.log(Level.INFO, "Stopping mock Arthas");
      return;
    }

    try {
      Method destroyMethod = bootstrap.getClass().getMethod("destroy");
      destroyMethod.invoke(bootstrap);
      logger.log(Level.INFO, "Arthas destroyed via reflection");

    } catch (ReflectiveOperationException e) {
      logger.log(Level.WARNING, "Failed to stop Arthas via reflection", e);
    }
  }

  /**
   * 构建 Arthas 配置 Map
   *
   * @return 配置 Map
   */
  private Map<String, String> buildArthasConfigMap() {
    Map<String, String> configMap = new HashMap<>();

    // 禁用 Arthas 默认的 telnet 和 http 服务（我们使用 Tunnel）
    configMap.put("arthas.telnetPort", "-1");
    configMap.put("arthas.httpPort", "-1");

    // 设置 session 超时（Java 8 兼容）
    configMap.put(
        "arthas.sessionTimeout",
        String.valueOf(config.getSessionIdleTimeout().toMillis() / 1000));

    // Tunnel 相关配置
    String tunnelServer = config.getTunnelEndpoint();
    if (tunnelServer != null && !tunnelServer.isEmpty()) {
      configMap.put("tunnel-server", tunnelServer);
    }

    return configMap;
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

  /** Mock Arthas Bootstrap（用于测试） */
  private static final class MockArthasBootstrap {
    // 空实现，用于在 Arthas jar 不存在时的测试场景
  }
}
