/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import javax.annotation.Nullable;

/**
 * 动态增强模块的日志隔离工具
 *
 * <p>为动态增强模块提供独立的日志输出通道，避免增强日志污染业务日志。
 * 所有动态增强相关的日志（包括 Advice 内联代码中的异常、AdviceDispatcher 分发日志、
 * 各类型 Advice 的诊断日志）统一通过此类输出到独立的日志文件。
 *
 * <p><b>核心设计</b>：
 * <ul>
 *   <li>使用 JUL（{@link java.util.logging.Logger}）+ {@link FileHandler}，不引入额外依赖</li>
 *   <li>{@code setUseParentHandlers(false)} 阻断日志向 Root Logger 传播，确保不污染 stdout/stderr</li>
 *   <li>支持日志文件滚动（默认 10MB/文件，最多 3 个滚动文件）</li>
 *   <li>文件路径可通过系统属性 {@code otel.controlplane.instrument.log.file} 配置</li>
 * </ul>
 *
 * <p><b>Logger 命名规范</b>：所有子 Logger 以 {@code io.opentelemetry.dynamic-instrumentation}
 * 为前缀，自动继承 FileHandler 配置：
 * <ul>
 *   <li>{@code io.opentelemetry.dynamic-instrumentation} — 根 Logger（AdviceDispatcher 等通用日志）</li>
 *   <li>{@code io.opentelemetry.dynamic-instrumentation.advice-error} — Advice 内联代码异常</li>
 *   <li>{@code io.opentelemetry.dynamic-instrumentation.trace} — DynamicTraceAdvice 日志</li>
 *   <li>{@code io.opentelemetry.dynamic-instrumentation.metric} — DynamicMetricAdvice 日志</li>
 *   <li>{@code io.opentelemetry.dynamic-instrumentation.log} — DynamicLogAdvice 日志</li>
 * </ul>
 *
 * @see DynamicByteBuddyAdvice
 * @see AdviceDispatcher
 */
@SuppressWarnings("SystemOut") // System.err 是日志系统初始化失败时的合理兜底输出
public final class DynamicInstrumentLogger {

  /** 日志命名空间前缀 */
  static final String LOGGER_PREFIX = "io.opentelemetry.dynamic-instrumentation";

  /** 系统属性：日志文件路径 */
  static final String LOG_FILE_PROPERTY = "otel.controlplane.instrument.log.file";

  /** 系统属性：日志级别 */
  static final String LOG_LEVEL_PROPERTY = "otel.controlplane.instrument.log.level";

  /** 默认日志文件名 */
  private static final String DEFAULT_LOG_FILE = "dynamic-instrument.log";

  /** 默认日志文件大小上限（10MB） */
  private static final int DEFAULT_MAX_FILE_SIZE = 10 * 1024 * 1024;

  /** 默认滚动文件数 */
  private static final int DEFAULT_FILE_COUNT = 3;

  /** 根 Logger（所有子 Logger 继承此 Logger 的 FileHandler 配置） */
  @Nullable private static volatile Logger rootLogger;

  /** 初始化标志 */
  private static volatile boolean initialized = false;

  /** Advice 内联代码异常专用 Logger */
  @Nullable private static volatile Logger adviceErrorLogger;

  private DynamicInstrumentLogger() {}

  /**
   * 初始化日志系统
   *
   * <p>在 {@link DynamicInstrumentationIntegration#create()} 时调用。
   * 线程安全，重复调用无副作用。
   */
  public static synchronized void initialize() {
    if (initialized) {
      return;
    }

    try {
      rootLogger = Logger.getLogger(LOGGER_PREFIX);
      // 关键：阻断向父 Logger（Root）传播，避免污染业务日志
      rootLogger.setUseParentHandlers(false);

      // 配置日志级别
      Level logLevel = resolveLogLevel();
      rootLogger.setLevel(logLevel);

      // 配置 FileHandler
      FileHandler fileHandler = createFileHandler();
      if (fileHandler != null) {
        rootLogger.addHandler(fileHandler);
      } else {
        // FileHandler 创建失败时，回退使用父 Handler（至少有输出）
        rootLogger.setUseParentHandlers(true);
        rootLogger.log(Level.WARNING,
            "[DYNAMIC-INSTRUMENT-LOG] FileHandler creation failed, falling back to parent handlers");
      }

      // 初始化 Advice 异常专用 Logger（继承 rootLogger 的 Handler）
      adviceErrorLogger = Logger.getLogger(LOGGER_PREFIX + ".advice-error");

      initialized = true;

      rootLogger.log(Level.INFO,
          "[DYNAMIC-INSTRUMENT-LOG] Logger initialized: level={0}, file={1}",
          new Object[] {logLevel, resolveLogFilePath()});
    } catch (RuntimeException e) {
      // 日志初始化失败不应阻塞主流程，回退到 System.err
      System.err.println("[DYNAMIC-INSTRUMENT-LOG] Failed to initialize logger: " + e);
      e.printStackTrace(System.err);
    }
  }

  /**
   * 获取动态增强模块的根 Logger
   *
   * <p>用于 {@link AdviceDispatcher}、{@link TransformerManager} 等类的日志输出。
   *
   * @return 根 Logger，未初始化时返回基于命名空间的默认 Logger
   */
  public static Logger getLogger() {
    if (rootLogger != null) {
      return rootLogger;
    }
    // 未初始化时返回普通 Logger（不隔离），避免 NPE
    return Logger.getLogger(LOGGER_PREFIX);
  }

  /**
   * 获取指定子命名空间的 Logger
   *
   * <p>例如：{@code getLogger("trace")} 返回 {@code io.opentelemetry.dynamic-instrumentation.trace}
   *
   * @param subNamespace 子命名空间
   * @return 子 Logger
   */
  public static Logger getLogger(String subNamespace) {
    return Logger.getLogger(LOGGER_PREFIX + "." + subNamespace);
  }

  /**
   * 记录 Advice 内联代码中的异常
   *
   * <p>此方法设计为<b>极简且健壮</b>，因为它在 ByteBuddy Advice 内联代码的 catch 块中调用，
   * 必须确保自身不抛出任何异常。
   *
   * <p>调用场景：{@link DynamicByteBuddyAdvice} 内联代码中 catch 到 {@code suppress} 异常时。
   *
   * @param phase "onEnter" 或 "onExit"
   * @param ruleId 规则 ID
   * @param error 捕获到的异常
   */
  public static void logAdviceError(String phase, String ruleId, Throwable error) {
    try {
      Logger logger = adviceErrorLogger;
      if (logger == null) {
        // 未初始化时回退到 System.err
        System.err.println("[DYNAMIC-INSTRUMENT-ADVICE-ERROR] " + phase
            + " failed for rule: " + ruleId + ", error: " + error);
        error.printStackTrace(System.err);
        return;
      }
      logger.log(Level.SEVERE,
          "[ADVICE-ERROR] {0} failed for rule: {1}, error: {2}",
          new Object[] {phase, ruleId, error.toString()});
      logger.log(Level.SEVERE, "[ADVICE-ERROR] Stack trace:", error);
    } catch (Throwable ignored) {
      // 绝对不能让日志方法自身抛出异常
      // 最后的兜底：System.err
      try {
        System.err.println("[DYNAMIC-INSTRUMENT-ADVICE-ERROR-FALLBACK] " + phase
            + " failed for rule: " + ruleId + ", error: " + error);
      } catch (Throwable alsoIgnored) {
        // 无能为力
      }
    }
  }

  /**
   * 创建 FileHandler
   *
   * @return FileHandler 实例，失败返回 null
   */
  @Nullable
  private static FileHandler createFileHandler() {
    try {
      String logFilePath = resolveLogFilePath();

      // 确保日志目录存在
      Path logDir = Paths.get(logFilePath).getParent();
      if (logDir != null && !Files.exists(logDir)) {
        Files.createDirectories(logDir);
      }

      FileHandler fileHandler = new FileHandler(
          logFilePath,
          DEFAULT_MAX_FILE_SIZE,
          DEFAULT_FILE_COUNT,
          true  // append 模式
      );
      fileHandler.setFormatter(new SimpleFormatter());
      fileHandler.setLevel(Level.ALL);

      return fileHandler;
    } catch (IOException | SecurityException e) {
      System.err.println("[DYNAMIC-INSTRUMENT-LOG] Failed to create FileHandler: " + e);
      return null;
    }
  }

  /**
   * 解析日志文件路径
   *
   * <p>优先级：系统属性 > 默认路径（{@code ${java.io.tmpdir}/otel-agent/dynamic-instrument.log}）
   *
   * @return 日志文件绝对路径
   */
  private static String resolveLogFilePath() {
    String configPath = System.getProperty(LOG_FILE_PROPERTY);
    if (configPath != null && !configPath.isEmpty()) {
      return configPath;
    }

    // 默认路径：${java.io.tmpdir}/otel-agent/dynamic-instrument.log
    String tmpDir = System.getProperty("java.io.tmpdir", "/tmp");
    return Paths.get(tmpDir, "otel-agent", DEFAULT_LOG_FILE).toString();
  }

  /**
   * 解析日志级别
   *
   * @return 日志级别，默认 ALL
   */
  private static Level resolveLogLevel() {
    String levelStr = System.getProperty(LOG_LEVEL_PROPERTY);
    if (levelStr == null || levelStr.isEmpty()) {
      return Level.ALL;
    }

    try {
      return Level.parse(levelStr.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      return Level.ALL;
    }
  }

  /**
   * 关闭日志系统，释放 FileHandler 资源
   *
   * <p>在 {@link DynamicInstrumentationIntegration#close()} 时调用。
   */
  public static synchronized void shutdown() {
    if (!initialized || rootLogger == null) {
      return;
    }

    java.util.logging.Handler[] handlers = rootLogger.getHandlers();
    for (java.util.logging.Handler handler : handlers) {
      try {
        handler.close();
      } catch (RuntimeException ignored) {
        // ignored
      }
      rootLogger.removeHandler(handler);
    }

    initialized = false;
    rootLogger.log(Level.INFO, "[DYNAMIC-INSTRUMENT-LOG] Logger shutdown");
  }
}
