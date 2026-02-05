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
 *   <li>Arthas JNI library - vmtool 命令依赖</li>
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

  /**
   * Arthas 专用 logback.xml 模板
   *
   * <p>【关键设计】：
   * <ul>
   *   <li>只有 RollingFileAppender，不含 ConsoleAppender，防止污染应用 stdout</li>
   *   <li>对齐 Arthas 官方 LogUtil：使用 ${ARTHAS_LOG_PATH} / ${ARTHAS_LOG_FILE} 变量
   *       （由 LogUtil 从 arthas.logging.file.path/name 转写到 LoggerContext property）</li>
   *   <li>root level 设置为 INFO，Netty 等底层组件降级为 WARN 减少噪音</li>
   * </ul>
   */
  private static final String ARTHAS_LOGBACK_XML_CONTENT =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<configuration>\n"
          + "  <!-- Arthas 专用日志配置 - 仅文件输出，不含 ConsoleAppender -->\n"
          + "  <property name=\"LOG_PATH\" value=\"${ARTHAS_LOG_PATH:-${java.io.tmpdir}}\"/>\n"
          + "  <property name=\"LOG_FILE\" value=\"${ARTHAS_LOG_FILE:-arthas.log}\"/>\n"
          + "\n"
          + "  <appender name=\"ARTHAS\" class=\"com.alibaba.arthas.deps.ch.qos.logback.core.rolling.RollingFileAppender\">\n"
          + "    <file>${LOG_PATH}/${LOG_FILE}</file>\n"
          + "    <rollingPolicy class=\"com.alibaba.arthas.deps.ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy\">\n"
          + "      <fileNamePattern>${LOG_PATH}/${LOG_FILE}.%d{yyyy-MM-dd}.%i</fileNamePattern>\n"
          + "      <maxFileSize>10MB</maxFileSize>\n"
          + "      <maxHistory>3</maxHistory>\n"
          + "      <totalSizeCap>50MB</totalSizeCap>\n"
          + "    </rollingPolicy>\n"
          + "    <encoder>\n"
          + "      <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>\n"
          + "    </encoder>\n"
          + "  </appender>\n"
          + "\n"
          + "  <!-- 底层组件降级为 WARN，减少日志噪音 -->\n"
          + "  <logger name=\"com.alibaba.arthas.deps.io.netty\" level=\"WARN\"/>\n"
          + "  <logger name=\"io.netty\" level=\"WARN\"/>\n"
          + "\n"
          + "  <root level=\"INFO\">\n"
          + "    <appender-ref ref=\"ARTHAS\"/>\n"
          + "  </root>\n"
          + "</configuration>\n";

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

      // 【关键】生成 Arthas 专用 logback.xml 到 arthas-home 目录
      // 让 Arthas 的 SLF4J 日志只写文件，不污染应用控制台
      generateArthasLogbackXml(tempDir);

      // 【关键】提取 async-profiler native library 到 arthas-home 目录
      // 使 Arthas profiler 命令能够找到 libasyncProfiler.so
      // 失败不阻塞 Arthas 启动，仅影响 profiler 命令
      extractAsyncProfilerLibrary(tempDir);

      // 【关键】提取 Arthas JNI library 到 arthas-home/lib 目录
      // 使 Arthas vmtool 命令能够找到 libArthasJniLibrary
      // 失败不阻塞 Arthas 启动，仅影响 vmtool 命令
      extractArthasJniLibrary(tempDir);

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
   * 提取 Arthas JNI library 到 arthas-home 的 lib 目录
   *
   * <p>Arthas vmtool 命令查找 JNI 库的路径是： <code>{arthas-home}/lib/libArthasJniLibrary-{platform}.{ext}</code>
   *
   * <p>失败不阻塞 Arthas 启动，仅记录警告日志。vmtool 命令将不可用。
   *
   * @param arthasHome Arthas 运行时根目录
   */
  public static void extractArthasJniLibrary(Path arthasHome) {
    try {
      // 使用 ArthasJniLibraryExtractor 提取库文件
      ArthasJniLibraryExtractor.ExtractionResult result =
          ArthasJniLibraryExtractor.extractTo(arthasHome);

      if (result.isSuccess()) {
        if (result.isSkipped()) {
          logger.log(
              Level.FINE,
              "[ARTHAS] JNI library already exists: {0}",
              result.getLibraryPath());
        } else {
          logger.log(
              Level.INFO, "[ARTHAS] JNI library extracted: {0}", result.getLibraryPath());
        }
      } else {
        // 提取失败，记录警告但不阻塞
        logger.log(
            Level.WARNING,
            "[ARTHAS] Failed to extract JNI library: {0}. "
                + "Arthas vmtool command will not work.",
            result.getMessage());
      }
    } catch (RuntimeException e) {
      // 捕获所有异常，防止影响 Arthas 启动
      logger.log(
          Level.WARNING,
          "[ARTHAS] Error extracting JNI library: {0}. " + "Arthas vmtool command will not work.",
          e.getMessage());
    }
  }

  /**
   * 生成 Arthas 专用 logback.xml 到 arthas-home 目录
   *
   * <p>Arthas 启动时会在 arthas-home 目录下查找 logback.xml。
   * 通过生成仅含 RollingFileAppender 的配置，确保 Arthas 的 SLF4J 日志
   * （如 TunnelClient、Netty 等）只写入文件，不污染应用控制台。
   *
   * <p>失败不阻塞 Arthas 启动，仅记录警告日志。
   *
   * @param arthasHome Arthas 运行时根目录
   */
  private static void generateArthasLogbackXml(Path arthasHome) {
    Path logbackXml = arthasHome.resolve("logback.xml");
    try {
      Files.write(
          logbackXml,
          ARTHAS_LOGBACK_XML_CONTENT.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      logbackXml.toFile().deleteOnExit();
      logger.log(Level.INFO, "[ARTHAS] Generated logback.xml: {0}", logbackXml);
    } catch (IOException e) {
      // 失败不阻塞 Arthas 启动，仅记录警告
      logger.log(
          Level.WARNING,
          "[ARTHAS] Failed to generate logback.xml: {0}. "
              + "Arthas SLF4J logs may leak to application console.",
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
