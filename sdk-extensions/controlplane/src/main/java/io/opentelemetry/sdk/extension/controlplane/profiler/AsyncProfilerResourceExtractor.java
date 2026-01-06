/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.profiler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * AsyncProfiler 资源提取器
 *
 * <p>负责从 classpath 提取 async-profiler native library 到指定目录。
 * 根据当前操作系统和 CPU 架构自动选择正确的库文件。
 *
 * <p>设计原则：
 * <ul>
 *   <li><b>SRP</b>：只负责平台检测和资源提取，不涉及 Arthas 或其他业务逻辑</li>
 *   <li><b>独立可复用</b>：可被 Arthas 和独立 AsyncProfiler 模块共同使用</li>
 *   <li><b>失败降级</b>：提取失败不阻塞调用方，仅影响 profiler 功能</li>
 *   <li><b>幂等性</b>：重复提取检查文件是否存在，避免重复 I/O</li>
 * </ul>
 *
 * <p>资源路径：<code>/async-profiler/libasyncProfiler-{platform}.{ext}</code>
 *
 * <p>支持的平台：
 * <ul>
 *   <li>Linux x86_64: libasyncProfiler-linux-x64.so</li>
 *   <li>Linux aarch64: libasyncProfiler-linux-arm64.so</li>
 *   <li>macOS: libasyncProfiler-mac.dylib</li>
 * </ul>
 *
 * <p>输出目录结构（对齐 Arthas profiler 命令的查找路径）：
 * <pre>
 *   {targetDir}/async-profiler/libasyncProfiler-{platform}.{ext}
 * </pre>
 */
public final class AsyncProfilerResourceExtractor {

  private static final Logger logger =
      Logger.getLogger(AsyncProfilerResourceExtractor.class.getName());

  /** 资源根路径 */
  private static final String RESOURCE_BASE_PATH = "/async-profiler/";

  /** 输出子目录名（对齐 Arthas profiler 命令的查找路径） */
  private static final String OUTPUT_SUBDIR = "async-profiler";

  /** 并发锁：防止多线程同时提取 */
  private static final Object EXTRACTION_LOCK = new Object();

  /** 已提取标记（进程内幂等） */
  private static final AtomicBoolean extracted = new AtomicBoolean(false);

  /** 已提取的路径（用于复用） */
  @Nullable private static volatile Path extractedLibraryPath;

  private AsyncProfilerResourceExtractor() {
    // 工具类，禁止实例化
  }

  /**
   * 提取 async-profiler native library 到指定目录
   *
   * <p>根据当前平台自动选择正确的库文件，提取到 targetDir/async-profiler/ 目录下。
   *
   * <p>幂等性：如果目标文件已存在且大小 > 0，则跳过提取。
   *
   * @param targetDir 目标目录（通常是 arthas-home 或独立 profiler home）
   * @return 提取结果
   */
  public static ExtractionResult extractTo(Path targetDir) {
    // 1. 检测当前平台
    PlatformInfo platform = detectPlatform();
    if (!platform.isSupported()) {
      String reason = String.format(Locale.ROOT,
          "Unsupported platform: os=%s, arch=%s", platform.getOs(), platform.getArch());
      logger.log(Level.WARNING, "[ASYNC-PROFILER] {0}", reason);
      return ExtractionResult.unsupported(reason);
    }

    // 2. 构建资源路径和输出路径
    String resourceName = platform.getLibraryFileName();
    String resourcePath = RESOURCE_BASE_PATH + resourceName;
    Path outputDir = targetDir.resolve(OUTPUT_SUBDIR);
    Path outputFile = outputDir.resolve(resourceName);

    // 3. 幂等性检查：文件已存在则跳过
    if (Files.exists(outputFile)) {
      try {
        long size = Files.size(outputFile);
        if (size > 0) {
          logger.log(Level.FINE,
              "[ASYNC-PROFILER] Library already exists, skipping extraction: {0} ({1} bytes)",
              new Object[] {outputFile, size});
          extractedLibraryPath = outputFile;
          extracted.set(true);
          return ExtractionResult.success(outputFile, /* skipped= */ true);
        }
      } catch (IOException e) {
        // 无法读取文件大小，继续尝试提取
        logger.log(Level.FINE, "[ASYNC-PROFILER] Cannot read existing file size, will re-extract");
      }
    }

    // 4. 并发控制：防止多线程同时写入
    synchronized (EXTRACTION_LOCK) {
      // 双重检查
      if (Files.exists(outputFile)) {
        try {
          if (Files.size(outputFile) > 0) {
            extractedLibraryPath = outputFile;
            extracted.set(true);
            return ExtractionResult.success(outputFile, /* skipped= */ true);
          }
        } catch (IOException e) {
          // ignore
        }
      }

      // 5. 执行提取
      return doExtract(resourcePath, outputDir, outputFile, platform);
    }
  }

  /**
   * 执行实际的提取操作
   */
  private static ExtractionResult doExtract(
      String resourcePath, Path outputDir, Path outputFile, PlatformInfo platform) {

    logger.log(Level.INFO,
        "[ASYNC-PROFILER] Extracting native library: resource={0}, target={1}",
        new Object[] {resourcePath, outputFile});

    try {
      // 创建输出目录
      Files.createDirectories(outputDir);

      // 从 classpath 读取资源
      try (InputStream is = AsyncProfilerResourceExtractor.class.getResourceAsStream(resourcePath)) {
        if (is == null) {
          String reason = "Resource not found in classpath: " + resourcePath;
          logger.log(Level.WARNING, "[ASYNC-PROFILER] {0}", reason);
          return ExtractionResult.notFound(reason);
        }

        // 使用原子写入：先写临时文件，再 move
        Path tempFile = outputDir.resolve(outputFile.getFileName() + ".tmp");
        try {
          Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);

          // 设置可执行权限（Linux/macOS）
          if (!platform.isWindows()) {
            try {
              tempFile.toFile().setExecutable(true, false);
              tempFile.toFile().setReadable(true, false);
            } catch (SecurityException e) {
              logger.log(Level.FINE,
                  "[ASYNC-PROFILER] Cannot set executable permission: {0}", e.getMessage());
              // 不致命，继续
            }
          }

          // 原子 move
          Files.move(tempFile, outputFile, StandardCopyOption.REPLACE_EXISTING);

          // 再次设置权限（move 后可能丢失）
          if (!platform.isWindows()) {
            try {
              outputFile.toFile().setExecutable(true, false);
              outputFile.toFile().setReadable(true, false);
            } catch (SecurityException e) {
              // ignore
            }
          }

          // 标记 deleteOnExit（可选，取决于是否需要清理）
          // outputFile.toFile().deleteOnExit();

          long size = Files.size(outputFile);
          logger.log(Level.INFO,
              "[ASYNC-PROFILER] Native library extracted successfully: {0} ({1} bytes)",
              new Object[] {outputFile, size});

          extractedLibraryPath = outputFile;
          extracted.set(true);
          return ExtractionResult.success(outputFile, /* skipped= */ false);

        } finally {
          // 清理临时文件（如果 move 失败）
          try {
            Files.deleteIfExists(tempFile);
          } catch (IOException e) {
            // ignore
          }
        }
      }

    } catch (IOException e) {
      String reason = "Failed to extract native library: " + e.getMessage();
      logger.log(Level.WARNING, "[ASYNC-PROFILER] {0}", reason);
      return ExtractionResult.failed(reason, e);
    }
  }

  /**
   * 检测当前平台信息
   *
   * @return 平台信息
   */
  public static PlatformInfo detectPlatform() {
    String osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    String osArch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);

    return new PlatformInfo(osName, osArch);
  }

  /**
   * 获取已提取的库文件路径
   *
   * @return 库文件路径，如果未提取返回 null
   */
  @Nullable
  public static Path getExtractedLibraryPath() {
    return extractedLibraryPath;
  }

  /**
   * 检查是否已成功提取
   *
   * @return 是否已提取
   */
  public static boolean isExtracted() {
    return extracted.get();
  }

  /**
   * 重置提取状态（用于测试）
   */
  static void reset() {
    synchronized (EXTRACTION_LOCK) {
      extracted.set(false);
      extractedLibraryPath = null;
    }
  }

  // ===== 内部类 =====

  /**
   * 平台信息
   */
  public static final class PlatformInfo {
    private final String os;
    private final String arch;
    private final boolean supported;
    @Nullable private final String libraryFileName;

    PlatformInfo(String osName, String osArch) {
      this.os = osName;
      this.arch = osArch;

      // 根据 OS 和 Arch 确定库文件名
      // 命名规则对齐 Arthas profiler 命令的查找逻辑
      if (osName.contains("linux")) {
        if (osArch.contains("aarch64") || osArch.contains("arm64")) {
          this.libraryFileName = "libasyncProfiler-linux-arm64.so";
          this.supported = true;
        } else if (osArch.contains("amd64") || osArch.contains("x86_64")) {
          this.libraryFileName = "libasyncProfiler-linux-x64.so";
          this.supported = true;
        } else {
          this.libraryFileName = null;
          this.supported = false;
        }
      } else if (osName.contains("mac") || osName.contains("darwin")) {
        // macOS：不区分 arm64/x64，async-profiler 使用 universal binary
        this.libraryFileName = "libasyncProfiler-mac.dylib";
        this.supported = true;
      } else {
        // Windows 或其他不支持的平台
        this.libraryFileName = null;
        this.supported = false;
      }
    }

    public String getOs() {
      return os;
    }

    public String getArch() {
      return arch;
    }

    public boolean isSupported() {
      return supported;
    }

    public boolean isWindows() {
      return os.contains("windows");
    }

    @Nullable
    public String getLibraryFileName() {
      return libraryFileName;
    }

    @Override
    public String toString() {
      return String.format(Locale.ROOT,
          "PlatformInfo{os='%s', arch='%s', supported=%s, library='%s'}",
          os, arch, supported, libraryFileName);
    }
  }

  /**
   * 提取结果
   */
  public static final class ExtractionResult {
    /** 结果状态 */
    public enum Status {
      /** 提取成功 */
      SUCCESS,
      /** 文件已存在（幂等跳过） */
      ALREADY_EXISTS,
      /** 平台不支持 */
      UNSUPPORTED,
      /** 资源未找到 */
      NOT_FOUND,
      /** 提取失败 */
      FAILED
    }

    private final Status status;
    @Nullable private final Path libraryPath;
    @Nullable private final String message;
    @Nullable private final Throwable cause;
    private final boolean skipped;

    private ExtractionResult(
        Status status,
        @Nullable Path libraryPath,
        @Nullable String message,
        @Nullable Throwable cause,
        boolean skipped) {
      this.status = status;
      this.libraryPath = libraryPath;
      this.message = message;
      this.cause = cause;
      this.skipped = skipped;
    }

    static ExtractionResult success(Path libraryPath, boolean skipped) {
      return new ExtractionResult(
          skipped ? Status.ALREADY_EXISTS : Status.SUCCESS,
          libraryPath,
          skipped ? "Library already exists" : "Extraction successful",
          null,
          skipped);
    }

    static ExtractionResult unsupported(String reason) {
      return new ExtractionResult(Status.UNSUPPORTED, null, reason, null, /* skipped= */ false);
    }

    static ExtractionResult notFound(String reason) {
      return new ExtractionResult(Status.NOT_FOUND, null, reason, null, /* skipped= */ false);
    }

    static ExtractionResult failed(String reason, @Nullable Throwable cause) {
      return new ExtractionResult(Status.FAILED, null, reason, cause, /* skipped= */ false);
    }

    public Status getStatus() {
      return status;
    }

    public boolean isSuccess() {
      return status == Status.SUCCESS || status == Status.ALREADY_EXISTS;
    }

    public boolean isSkipped() {
      return skipped;
    }

    @Nullable
    public Path getLibraryPath() {
      return libraryPath;
    }

    @Nullable
    public String getMessage() {
      return message;
    }

    @Nullable
    public Throwable getCause() {
      return cause;
    }

    @Override
    public String toString() {
      return String.format(Locale.ROOT,
          "ExtractionResult{status=%s, path=%s, message='%s', skipped=%s}",
          status, libraryPath, message, skipped);
    }
  }
}
