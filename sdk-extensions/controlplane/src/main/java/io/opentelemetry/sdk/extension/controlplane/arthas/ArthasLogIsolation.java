/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 日志隔离组件
 *
 * <p>负责统一管理 Arthas 的日志隔离，避免 Arthas 日志污染服务日志。
 *
 * <p>治理策略（两层防护）：
 * <ol>
 *   <li><b>主线</b>：通过 Arthas 官方配置（arthas.logging.*）让 Arthas logback 只写文件</li>
 *   <li><b>隔离+降噪</b>：反射重定向 AnsiLog.out 到独立文件，并提高 AnsiLog.LEVEL，避免污染服务 stdout</li>
 * </ol>
 *
 * <p>设计原则：
 * <ul>
 *   <li><b>单一职责</b>：只负责日志隔离，不涉及 Arthas 生命周期管理</li>
 *   <li><b>最小侵入</b>：优先使用 Arthas 官方配置，避免全局副作用</li>
 *   <li><b>可观测</b>：所有治理操作都有诊断日志</li>
 * </ul>
 */
public final class ArthasLogIsolation {

  private static final Logger logger = Logger.getLogger(ArthasLogIsolation.class.getName());

  /** AnsiLog 类名 */
  private static final String ANSI_LOG_CLASS = "com.taobao.arthas.common.AnsiLog";

  /** 配置 */
  private final ArthasConfig config;

  /**
   * AnsiLog.out(PrintStream) 是静态全局开关，需要全局重入保护，避免并发 start/stop 互相覆盖。
   */
  private static final Object ANSI_LOG_OUT_LOCK = new Object();

  /** AnsiLog.out 重定向引用计数（在 {@link #ANSI_LOG_OUT_LOCK} 下保护） */
  private static final java.util.concurrent.atomic.AtomicInteger ansiLogOutRefCount =
      new java.util.concurrent.atomic.AtomicInteger(0);

  /** 当前生效的 AnsiLog.out 重定向状态（在 {@link #ANSI_LOG_OUT_LOCK} 下保护） */
  private static final java.util.concurrent.atomic.AtomicReference<AnsiLogOutRedirection>
      ansiLogOutRedirection = new java.util.concurrent.atomic.AtomicReference<>();

  /**
   * 创建日志隔离组件
   *
   * @param config Arthas 配置
   */
  public ArthasLogIsolation(ArthasConfig config) {
    this.config = config;
  }

  // ===== 主线：Arthas logback 配置 =====

  /**
   * 将日志隔离配置添加到 Arthas 配置 Map
   *
   * <p>设置 arthas.logging.file.path 和 arthas.logging.file.name，
   * 让 Arthas 的 logback 只写文件，不输出到 console。
   *
   * @param configMap Arthas 配置 Map（会被修改）
   */
  public void applyLoggingConfig(Map<String, String> configMap) {
    // 1. 设置日志文件路径
    String logPath = config.getLogFilePath();
    if (logPath == null || logPath.isEmpty()) {
      // 使用临时目录
      logPath = getOrCreateTempLogDir();
    }
    if (logPath != null) {
      configMap.put("arthas.logging.file.path", logPath);
      logger.log(Level.INFO, "[ArthasLogIsolation] Log file path: {0}", logPath);
    }

    // 2. 设置日志文件名
    String logFileName = config.getLogFileName();
    if (logFileName != null && !logFileName.isEmpty()) {
      configMap.put("arthas.logging.file.name", logFileName);
      logger.log(Level.FINE, "[ArthasLogIsolation] Log file name: {0}", logFileName);
    }

    // 3. 【关键】不提供自定义 logback.xml，让 Arthas 使用默认配置
    // Arthas 默认的 logback.xml 已经是纯文件输出（RollingFileAppender）
    // 我们只需要确保 arthas.logging.file.path 正确设置即可
  }

  /**
   * 获取或创建临时日志目录
   *
   * <p>委托给 {@link ArthasTempDirectoryManager} 统一管理，支持跨 attach 周期复用。
   *
   * @return 临时目录路径，失败返回 null
   */
  @Nullable
  private static String getOrCreateTempLogDir() {
    try {
      Path dir = ArthasTempDirectoryManager.getInstance()
                     .getOrCreateDir(ArthasTempDirectoryManager.DirType.LOGS);
      return dir.toString();
    } catch (RuntimeException e) {
      logger.log(Level.WARNING,
          "[ArthasLogIsolation] Failed to create temp log dir: {0}", e.getMessage());
      return null;
    }
  }

  // ===== 隔离+降噪：AnsiLog.out 重定向 + AnsiLog.LEVEL 调整 =====

  /**
   * 开始隔离 AnsiLog 输出（重定向到独立文件）并提高其日志级别减少噪音。
   *
   * <p>注意：AnsiLog.out 是静态全局设置，必须与 {@link #endAnsiLogIsolation()} 配对使用。
   *
   * @param arthasLoader Arthas ClassLoader
   */
  public void beginAnsiLogIsolation(@Nullable ClassLoader arthasLoader) {
    if (arthasLoader == null) {
      logger.log(Level.FINE, "[ArthasLogIsolation] Arthas ClassLoader is null, skip AnsiLog isolation");
      return;
    }

    try {
      Class<?> ansiLogClass = arthasLoader.loadClass(ANSI_LOG_CLASS);

      synchronized (ANSI_LOG_OUT_LOCK) {
        int refCount = ansiLogOutRefCount.incrementAndGet();
        if (refCount > 1) {
          // 重入场景：只增加引用计数，避免覆盖已有重定向。
          logger.log(Level.FINE, "[ArthasLogIsolation] AnsiLog isolation re-entered, refCount={0}", refCount);
          return;
        }

        Path dir = resolveAnsiLogDir();
        Path filePath = dir.resolve("arthas-ansi-console.log");
        OutputStream outStream =
            Files.newOutputStream(
                filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
        PrintStream printStream =
            new PrintStream(
                new java.io.BufferedOutputStream(outStream),
                true,
                java.nio.charset.StandardCharsets.UTF_8.name());

        Method getOutMethod = ansiLogClass.getMethod("out");
        Method setOutMethod = ansiLogClass.getMethod("out", PrintStream.class);

        PrintStream previousOut = (PrintStream) getOutMethod.invoke(null);
        setOutMethod.invoke(null, printStream);

        ansiLogOutRedirection.set(
            new AnsiLogOutRedirection(setOutMethod, previousOut, printStream, outStream));

        logger.log(Level.INFO, "[ArthasLogIsolation] AnsiLog output redirected to: {0}", filePath);
      }

      // 在 begin 时顺带调整日志级别
      adjustAnsiLogLevelInternal(ansiLogClass);

    } catch (ClassNotFoundException e) {
      logger.log(Level.FINE, "[ArthasLogIsolation] AnsiLog class not found: {0}", e.getMessage());
    } catch (NoSuchMethodException e) {
      logger.log(Level.WARNING, "[ArthasLogIsolation] AnsiLog.out/level method not found: {0}", e.getMessage());
    } catch (ReflectiveOperationException | IOException e) {
      logger.log(Level.WARNING, "[ArthasLogIsolation] Failed to begin AnsiLog isolation: {0}", e.getMessage());
      // begin 失败时，避免引用计数卡死
      synchronized (ANSI_LOG_OUT_LOCK) {
        if (ansiLogOutRefCount.get() > 0) {
          ansiLogOutRefCount.decrementAndGet();
        }
      }
    }
  }

  /**
   * 结束 AnsiLog 输出隔离，恢复原始输出流。
   */
  public void endAnsiLogIsolation() {
    synchronized (ANSI_LOG_OUT_LOCK) {
      if (ansiLogOutRefCount.get() <= 0) {
        return;
      }

      int refCount = ansiLogOutRefCount.decrementAndGet();
      if (refCount > 0) {
        logger.log(Level.FINE, "[ArthasLogIsolation] AnsiLog isolation exit (still referenced), refCount={0}", refCount);
        return;
      }

      AnsiLogOutRedirection redirection = ansiLogOutRedirection.getAndSet(null);

      if (redirection == null) {
        return;
      }

      try {
        // 恢复原始 out
        redirection.setOutMethod.invoke(null, redirection.previousOut);

      } catch (ReflectiveOperationException e) {
        logger.log(Level.WARNING, "[ArthasLogIsolation] Failed to restore AnsiLog.out: {0}", e.getMessage());

      } finally {
        // 关闭我们创建的流
        try {
          redirection.printStream.flush();
        } catch (RuntimeException ignored) {
          // ignored
        }
        try {
          redirection.printStream.close();
        } catch (RuntimeException ignored) {
          // ignored
        }
        try {
          redirection.outStream.close();
        } catch (IOException ignored) {
          // ignored
        }

        logger.log(Level.INFO, "[ArthasLogIsolation] AnsiLog output restored");
      }
    }
  }

  private void adjustAnsiLogLevelInternal(Class<?> ansiLogClass) {
    String targetLevel = config.getLogLevel();
    if (targetLevel == null || targetLevel.isEmpty()) {
      targetLevel = "WARNING";
    }

    try {
      Method levelMethod = ansiLogClass.getMethod("level", java.util.logging.Level.class);
      java.util.logging.Level level = parseLogLevel(targetLevel);
      Object oldLevel = levelMethod.invoke(null, level);
      logger.log(
          Level.INFO,
          "[ArthasLogIsolation] AnsiLog level adjusted: {0} -> {1}",
          new Object[] {oldLevel, level});

    } catch (NoSuchMethodException e) {
      logger.log(Level.WARNING, "[ArthasLogIsolation] AnsiLog.level(Level) method not found: {0}", e.getMessage());
    } catch (ReflectiveOperationException e) {
      logger.log(Level.WARNING, "[ArthasLogIsolation] Failed to adjust AnsiLog level: {0}", e.getMessage());
    }
  }

  private Path resolveAnsiLogDir() {
    String logPath = config.getLogFilePath();
    if (logPath == null || logPath.isEmpty()) {
      logPath = getOrCreateTempLogDir();
    }

    if (logPath == null || logPath.isEmpty()) {
      return Paths.get(System.getProperty("java.io.tmpdir"));
    }

    return Paths.get(logPath);
  }

  private static final class AnsiLogOutRedirection {
    private final Method setOutMethod;
    private final PrintStream previousOut;
    private final PrintStream printStream;
    private final OutputStream outStream;

    private AnsiLogOutRedirection(
        Method setOutMethod,
        PrintStream previousOut,
        PrintStream printStream,
        OutputStream outStream) {
      this.setOutMethod = setOutMethod;
      this.previousOut = previousOut;
      this.printStream = printStream;
      this.outStream = outStream;
    }
  }

  /**
   * 解析日志级别字符串
   *
   * @param levelStr 级别字符串
   * @return Level 对象
   */
  private static java.util.logging.Level parseLogLevel(String levelStr) {
    switch (levelStr.toUpperCase(Locale.ROOT)) {
      case "FINEST":
      case "TRACE":
        return java.util.logging.Level.FINEST;
      case "FINER":
      case "DEBUG":
        return java.util.logging.Level.FINER;
      case "FINE":
        return java.util.logging.Level.FINE;
      case "CONFIG":
        return java.util.logging.Level.CONFIG;
      case "INFO":
        return java.util.logging.Level.INFO;
      case "WARNING":
      case "WARN":
        return java.util.logging.Level.WARNING;
      case "SEVERE":
      case "ERROR":
        return java.util.logging.Level.SEVERE;
      default:
        logger.log(Level.WARNING, 
            "[ArthasLogIsolation] Unknown log level: {0}, using WARNING", levelStr);
        return java.util.logging.Level.WARNING;
    }
  }

}
