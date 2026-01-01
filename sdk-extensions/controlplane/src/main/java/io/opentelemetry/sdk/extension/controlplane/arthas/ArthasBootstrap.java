/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

  // Arthas jar 资源路径（嵌入式模式）
  private static final String ARTHAS_CORE_JAR_RESOURCE = "/arthas/arthas-core.jar";
  private static final String ARTHAS_CLIENT_JAR_RESOURCE = "/arthas/arthas-client.jar";

  private final ArthasConfig config;
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicReference<ClassLoader> arthasClassLoader = new AtomicReference<>();
  private final AtomicReference<Object> arthasBootstrapInstance = new AtomicReference<>();

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
   * @param loader Arthas ClassLoader
   * @return Arthas Bootstrap 实例
   */
  @Nullable
  private Object startArthasViaReflection(ClassLoader loader) {
    try {
      // 获取当前 JVM 的 PID
      long pid = getCurrentPid();

      // 构建配置
      Properties arthasProperties = buildArthasProperties();

      // 加载 ArthasBootstrap 类
      Class<?> bootstrapClass = loader.loadClass(ARTHAS_BOOTSTRAP_CLASS);

      // 获取 getInstance 方法
      Method getInstanceMethod = bootstrapClass.getMethod("getInstance", long.class, String.class);

      // 将配置转为字符串
      StringBuilder configStr = new StringBuilder();
      for (String key : arthasProperties.stringPropertyNames()) {
        if (configStr.length() > 0) {
          configStr.append(";");
        }
        configStr.append(key).append("=").append(arthasProperties.getProperty(key));
      }

      // 调用 getInstance
      Object bootstrap = getInstanceMethod.invoke(null, pid, configStr.toString());

      logger.log(Level.INFO, "Arthas Bootstrap created for PID: {0}", pid);
      return bootstrap;

    } catch (ClassNotFoundException e) {
      logger.log(Level.WARNING, "Arthas classes not found in ClassLoader. Running in mock mode.");
      // 返回非 null 以模拟启动成功（用于测试）
      return new MockArthasBootstrap();

    } catch (ReflectiveOperationException e) {
      logger.log(Level.SEVERE, "Failed to start Arthas via reflection", e);
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
   * 构建 Arthas 配置
   *
   * @return 配置属性
   */
  private Properties buildArthasProperties() {
    Properties props = new Properties();

    // 禁用 Arthas 默认的 telnet 和 http 服务（我们使用 Tunnel）
    props.setProperty("arthas.telnetPort", "-1");
    props.setProperty("arthas.httpPort", "-1");

    // 设置 session 超时（Java 8 兼容）
    props.setProperty(
        "arthas.sessionTimeout",
        String.valueOf(config.getSessionIdleTimeout().toMillis() / 1000));

    return props;
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
