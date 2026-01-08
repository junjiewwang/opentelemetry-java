/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Files;
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
 * <p>治理策略（三层防护）：
 * <ol>
 *   <li><b>主线</b>：通过 Arthas 官方配置（arthas.logging.*）让 Arthas logback 只写文件</li>
 *   <li><b>降噪</b>：反射提高 AnsiLog.LEVEL，减少 stdout 输出频率</li>
 *   <li><b>兜底</b>：在 start/stop 短窗口临时捕获 stdout/stderr</li>
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

  /** 临时目录（用于存放 Arthas 日志文件） */
  @Nullable private volatile Path tempLogDir;

  /** stdout/stderr 捕获状态 */
  private final StdoutCapture stdoutCapture = new StdoutCapture();

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
   * @return 临时目录路径，失败返回 null
   */
  @Nullable
  private String getOrCreateTempLogDir() {
    Path dir = this.tempLogDir;
    if (dir != null && Files.exists(dir)) {
      return dir.toString();
    }

    synchronized (this) {
      dir = this.tempLogDir;
      if (dir != null && Files.exists(dir)) {
        return dir.toString();
      }

      try {
        dir = Files.createTempDirectory("arthas-logs-");
        dir.toFile().deleteOnExit();
        this.tempLogDir = dir;
        logger.log(Level.INFO, "[ArthasLogIsolation] Created temp log dir: {0}", dir);
        return dir.toString();
      } catch (IOException e) {
        logger.log(Level.WARNING, 
            "[ArthasLogIsolation] Failed to create temp log dir: {0}", e.getMessage());
        return null;
      }
    }
  }

  // ===== 降噪：AnsiLog.LEVEL 反射调整 =====

  /**
   * 通过反射提高 AnsiLog.LEVEL，减少 stdout 输出
   *
   * <p>AnsiLog 直接使用 System.out.println 输出，无法通过 logback 配置控制。
   * 通过反射提高其 LEVEL，可以减少输出频率。
   *
   * @param arthasLoader Arthas ClassLoader
   */
  public void adjustAnsiLogLevel(@Nullable ClassLoader arthasLoader) {
    if (arthasLoader == null) {
      logger.log(Level.FINE, "[ArthasLogIsolation] Arthas ClassLoader is null, skip AnsiLog adjustment");
      return;
    }

    String targetLevel = config.getLogLevel();
    if (targetLevel == null || targetLevel.isEmpty()) {
      targetLevel = "WARNING"; // 默认降噪到 WARNING
    }

    try {
      // 加载 AnsiLog 类
      Class<?> ansiLogClass = arthasLoader.loadClass(ANSI_LOG_CLASS);

      // 获取 level(Level) 方法
      java.lang.reflect.Method levelMethod = 
          ansiLogClass.getMethod("level", java.util.logging.Level.class);

      // 转换级别字符串到 Level 对象
      java.util.logging.Level level = parseLogLevel(targetLevel);

      // 调用 level(Level) 设置级别
      Object oldLevel = levelMethod.invoke(null, level);

      logger.log(Level.INFO, 
          "[ArthasLogIsolation] AnsiLog level adjusted: {0} -> {1}", 
          new Object[]{oldLevel, level});

    } catch (ClassNotFoundException e) {
      // AnsiLog 类不存在（可能是不同版本的 Arthas）
      logger.log(Level.FINE, 
          "[ArthasLogIsolation] AnsiLog class not found: {0}", e.getMessage());
    } catch (NoSuchMethodException e) {
      // level(Level) 方法不存在
      logger.log(Level.WARNING, 
          "[ArthasLogIsolation] AnsiLog.level(Level) method not found: {0}", e.getMessage());
    } catch (ReflectiveOperationException e) {
      logger.log(Level.WARNING, 
          "[ArthasLogIsolation] Failed to adjust AnsiLog level: {0}", e.getMessage());
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

  // ===== 兜底：stdout/stderr 短窗口捕获 =====

  /**
   * 开始捕获 stdout/stderr
   *
   * <p>将 stdout/stderr 重定向到独立文件，用于捕获 Arthas 的 System.out 输出。
   * 必须与 {@link #endStdoutCapture()} 配对使用（try/finally）。
   *
   * <p>注意：这是短窗口操作，应仅在 Arthas start/stop 期间使用。
   */
  public void beginStdoutCapture() {
    if (!config.isStdoutCaptureEnabled()) {
      logger.log(Level.FINE, "[ArthasLogIsolation] Stdout capture disabled by config");
      return;
    }

    try {
      stdoutCapture.begin(getOrCreateTempLogDir());
    } catch (RuntimeException e) {
      // 捕获失败不应阻塞 Arthas 启动
      logger.log(Level.WARNING, 
          "[ArthasLogIsolation] Failed to begin stdout capture: {0}", e.getMessage());
    }
  }

  /**
   * 结束 stdout/stderr 捕获，恢复原始流
   */
  public void endStdoutCapture() {
    if (!config.isStdoutCaptureEnabled()) {
      return;
    }

    try {
      stdoutCapture.end();
    } catch (RuntimeException e) {
      logger.log(Level.WARNING, 
          "[ArthasLogIsolation] Failed to end stdout capture: {0}", e.getMessage());
    }
  }

  /**
   * 获取 stdout 捕获文件路径（用于诊断）
   *
   * @return 捕获文件路径，未捕获时返回 null
   */
  @Nullable
  public String getStdoutCaptureFilePath() {
    return stdoutCapture.getCaptureFilePath();
  }

  /**
   * stdout/stderr 捕获器
   *
   * <p>内部类，封装捕获逻辑，确保 try/finally 语义。
   */
  private static final class StdoutCapture {

    /** 原始 stdout */
    @Nullable private PrintStream originalOut;
    /** 原始 stderr */
    @Nullable private PrintStream originalErr;
    /** 捕获文件输出流 */
    @Nullable private FileOutputStream captureFileStream;
    /** 捕获文件路径 */
    @Nullable private String captureFilePath;
    /** 是否正在捕获 */
    private volatile boolean capturing = false;

    /**
     * 开始捕获
     *
     * @param logDir 日志目录
     */
    @SuppressWarnings("SystemOut") // 故意访问 System.out/err 进行捕获
    synchronized void begin(@Nullable String logDir) {
      if (capturing) {
        logger.log(Level.WARNING, "[StdoutCapture] Already capturing, skip");
        return;
      }

      try {
        // 保存原始流
        originalOut = System.out;
        originalErr = System.err;

        // 创建捕获文件
        File captureFile;
        if (logDir != null) {
          captureFile = new File(logDir, "arthas-console-" + System.currentTimeMillis() + ".log");
        } else {
          captureFile = File.createTempFile("arthas-console-", ".log");
        }
        captureFile.deleteOnExit();
        captureFilePath = captureFile.getAbsolutePath();

        // 创建输出流
        captureFileStream = new FileOutputStream(captureFile);

        // 创建 Tee 流（同时输出到原始流和捕获文件）
        // 注意：我们使用 Tee 模式而非完全替换，避免丢失业务输出
        PrintStream teeOut = new TeePrintStream(originalOut, captureFileStream, "[ARTHAS-OUT] ");
        PrintStream teeErr = new TeePrintStream(originalErr, captureFileStream, "[ARTHAS-ERR] ");

        // 替换系统流
        System.setOut(teeOut);
        System.setErr(teeErr);

        capturing = true;
        logger.log(Level.INFO, 
            "[StdoutCapture] Started capturing to: {0}", captureFilePath);

      } catch (IOException e) {
        // 失败时恢复
        restoreOriginalStreams();
        throw new IllegalStateException("Failed to begin stdout capture", e);
      }
    }

    /**
     * 结束捕获
     */
    synchronized void end() {
      if (!capturing) {
        return;
      }

      try {
        // 恢复原始流
        restoreOriginalStreams();

        // 关闭捕获文件
        if (captureFileStream != null) {
          try {
            captureFileStream.flush();
            captureFileStream.close();
          } catch (IOException e) {
            logger.log(Level.WARNING, 
                "[StdoutCapture] Failed to close capture file: {0}", e.getMessage());
          }
          captureFileStream = null;
        }

        logger.log(Level.INFO, 
            "[StdoutCapture] Ended capturing, file: {0}", captureFilePath);

      } finally {
        capturing = false;
      }
    }

    /**
     * 恢复原始流
     */
    private void restoreOriginalStreams() {
      if (originalOut != null) {
        System.setOut(originalOut);
        originalOut = null;
      }
      if (originalErr != null) {
        System.setErr(originalErr);
        originalErr = null;
      }
    }

    /**
     * 获取捕获文件路径
     */
    @Nullable
    String getCaptureFilePath() {
      return captureFilePath;
    }
  }

  /**
   * Tee PrintStream - 同时输出到两个流
   *
   * <p>用于在捕获 Arthas 输出的同时，保留原始 stdout/stderr 的输出。
   * 对于 Arthas 相关输出添加前缀标记，便于后续分析。
   */
  private static final class TeePrintStream extends PrintStream {

    private final PrintStream original;
    private final OutputStream capture;
    private final String prefix;

    TeePrintStream(PrintStream original, OutputStream capture, String prefix) {
      super(original, true);
      this.original = original;
      this.capture = capture;
      this.prefix = prefix;
    }

    @Override
    public void write(int b) {
      original.write(b);
      try {
        capture.write(b);
      } catch (IOException e) {
        // 忽略捕获写入错误
      }
    }

    @Override
    public void write(byte[] buf, int off, int len) {
      original.write(buf, off, len);
      try {
        capture.write(buf, off, len);
      } catch (IOException e) {
        // 忽略捕获写入错误
      }
    }

    @Override
    public void println(String x) {
      // 检查是否是 Arthas 相关输出
      if (isArthasOutput(x)) {
        // 只写入捕获文件，不输出到原始流
        try {
          capture.write((prefix + x + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
          capture.flush();
        } catch (IOException e) {
          // 忽略
        }
      } else {
        // 非 Arthas 输出，正常处理
        super.println(x);
      }
    }

    /**
     * 检查是否是 Arthas 相关输出
     *
     * <p>通过关键字匹配识别 Arthas 的 AnsiLog 输出。
     */
    private static boolean isArthasOutput(String line) {
      if (line == null) {
        return false;
      }
      // Arthas AnsiLog 输出特征
      return line.contains("[arthas-")
          || line.contains("arthas.core")
          || line.contains("arthas.tunnel")
          || line.contains("com.taobao.arthas")
          || line.contains("ArthasBanner")
          || line.contains("ArthasBootstrap")
          || line.contains("[TRACE]")
          || line.contains("[DEBUG]")
          || line.contains("[INFO]")
          || line.contains("[WARN]")
          || line.contains("[ERROR]")
          // Arthas Netty 日志
          || line.contains("arthas-TunnelClient")
          || line.contains("arthas-ForwardClient")
          || line.contains("arthas-NettyWebsocket");
    }

    @Override
    public void flush() {
      super.flush();
      try {
        capture.flush();
      } catch (IOException e) {
        // 忽略
      }
    }
  }
}
