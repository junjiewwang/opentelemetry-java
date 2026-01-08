/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 资源提取器
 *
 * <p>负责从 classpath 或外部路径提取 Arthas 相关资源：
 * <ul>
 *   <li>arthas-spy.jar - SpyAPI 类所在的 jar</li>
 *   <li>arthas-core.jar - Arthas 核心 jar</li>
 *   <li>arthas-client.jar - Arthas 客户端 jar</li>
 *   <li>async-profiler native library - profiler 命令依赖</li>
 * </ul>
 *
 * <p>设计原则：
 * <ul>
 *   <li><b>单一职责</b>：只负责资源提取，不负责加载和使用</li>
 *   <li><b>静态工具类</b>：所有方法都是静态的，无状态</li>
 * </ul>
 */
public final class ArthasResourceExtractor {

  private static final Logger logger = Logger.getLogger(ArthasResourceExtractor.class.getName());

  // Arthas jar 资源路径（嵌入式模式）
  private static final String ARTHAS_CORE_JAR_RESOURCE = "/arthas/arthas-core.jar";
  private static final String ARTHAS_CLIENT_JAR_RESOURCE = "/arthas/arthas-client.jar";
  private static final String ARTHAS_SPY_JAR_RESOURCE = "/arthas/arthas-spy.jar";

  private ArthasResourceExtractor() {
    // 工具类，禁止实例化
  }

  /**
   * 查找 arthas-spy.jar 文件
   *
   * <p>查找顺序：
   * <ol>
   *   <li>从配置的外部路径查找</li>
   *   <li>从 classpath 资源提取</li>
   * </ol>
   *
   * @param config Arthas 配置
   * @return jar 文件，未找到返回 null
   */
  @Nullable
  public static File extractSpyJar(ArthasConfig config) {
    // 1. 从配置的外部路径查找
    String libPath = config.getLibPath();
    if (libPath != null && !libPath.isEmpty()) {
      File spyJar = new File(libPath, "arthas-spy.jar");
      if (spyJar.exists()) {
        logger.log(Level.FINE, "Found arthas-spy.jar from external path: {0}", spyJar);
        return spyJar;
      }
    }

    // 2. 从 classpath 资源提取
    try {
      Path tempDir = Files.createTempDirectory("arthas-spy-");
      tempDir.toFile().deleteOnExit();

      Path spyJar = extractResource(ARTHAS_SPY_JAR_RESOURCE, tempDir, "arthas-spy.jar");
      if (spyJar != null) {
        logger.log(Level.FINE, "Extracted arthas-spy.jar to: {0}", spyJar);
        return spyJar.toFile();
      }
    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to extract arthas-spy.jar: {0}", e.getMessage());
    }

    return null;
  }

  /**
   * 从 classpath 资源提取 Arthas core jars
   *
   * <p>同时提取 async-profiler native library 到 arthas-home 目录。
   *
   * @return URL 数组，包含 core jar 和 client jar（如果存在）；如果提取失败返回 null
   */
  @Nullable
  @SuppressWarnings("AvoidObjectArrays") // URLClassLoader 需要 URL[] 参数
  public static URL[] extractCoreJars() {
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

      logger.log(Level.INFO, "Extracted Arthas core jars to: {0}", tempDir);
      return urls;

    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to extract Arthas core jars: {0}", e.getMessage());
      return null;
    }
  }

  /**
   * 从外部路径加载 Arthas jars 的 URL 数组
   *
   * @param libPath 外部库路径
   * @return URL 数组，如果加载失败返回 null
   */
  @Nullable
  @SuppressWarnings("AvoidObjectArrays") // URLClassLoader 需要 URL[] 参数
  public static URL[] loadFromExternalPath(String libPath) {
    try {
      File libDir = new File(libPath);
      if (!libDir.exists() || !libDir.isDirectory()) {
        logger.log(
            Level.WARNING,
            "Arthas lib path does not exist or is not a directory: {0}",
            libPath);
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

      return urls;

    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to load Arthas from external path: {0}", e.getMessage());
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
  public static void extractAsyncProfilerLibrary(Path arthasHome) {
    try {
      // 使用独立的 AsyncProfilerResourceExtractor 提取库文件
      io.opentelemetry.sdk.extension.controlplane.profiler.AsyncProfilerResourceExtractor
              .ExtractionResult
          result =
              io.opentelemetry.sdk.extension.controlplane.profiler.AsyncProfilerResourceExtractor
                  .extractTo(arthasHome);

      if (result.isSuccess()) {
        if (result.isSkipped()) {
          logger.log(
              Level.FINE,
              "[ARTHAS] async-profiler library already exists: {0}",
              result.getLibraryPath());
        } else {
          logger.log(
              Level.INFO,
              "[ARTHAS] async-profiler library extracted: {0}",
              result.getLibraryPath());
        }
      } else {
        // 提取失败，记录警告但不阻塞
        logger.log(
            Level.WARNING,
            "[ARTHAS] Failed to extract async-profiler library: {0}. "
                + "Arthas profiler command will not work.",
            result.getMessage());
      }
    } catch (RuntimeException e) {
      // 捕获所有异常，防止影响 Arthas 启动
      logger.log(
          Level.WARNING,
          "[ARTHAS] Error extracting async-profiler library: {0}. "
              + "Arthas profiler command will not work.",
          e.getMessage());
    }
  }

  /**
   * 解压资源到目标目录
   *
   * @param resourcePath 资源路径
   * @param targetDir 目标目录
   * @param fileName 文件名
   * @return 解压后的文件路径，如果资源不存在返回 null
   */
  @Nullable
  public static Path extractResource(String resourcePath, Path targetDir, String fileName) {
    try (InputStream is = ArthasResourceExtractor.class.getResourceAsStream(resourcePath)) {
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
}
