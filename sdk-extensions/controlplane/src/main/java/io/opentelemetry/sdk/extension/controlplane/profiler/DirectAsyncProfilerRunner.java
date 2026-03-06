/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.profiler;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import one.profiler.AsyncProfiler;

/**
 * 直接调用 AsyncProfiler Java API 的运行器
 *
 * <p>通过 {@code implementation} 依赖引入 {@code tools.profiler:async-profiler}，
 * 直接调用 {@link AsyncProfiler} Java API，具备编译期类型安全和 IDE 支持。
 *
 * <p>优势（对比旧版 ReflectiveAsyncProfilerRunner）：
 * <ul>
 *   <li>编译期类型安全：IDE 提示、重构安全</li>
 *   <li>代码简洁：无反射样板代码</li>
 *   <li>性能更优：消除反射开销</li>
 *   <li>可维护性：async-profiler API 升级时编译器直接检查兼容性</li>
 * </ul>
 *
 * <p>调用链路：
 * <pre>
 *   AsyncProfiler.getInstance(libPath)
 *     → execute("start,event={event},interval={interval}[,threads],jfr,file={outputPath}")
 *     → Thread.sleep(durationMs)
 *     → execute("stop")
 * </pre>
 *
 * <p>对于 collapsed 格式：
 * <pre>
 *   AsyncProfiler.getInstance(libPath)
 *     → execute("start,event={event},interval={interval}[,threads]")
 *     → Thread.sleep(durationMs)
 *     → execute("stop,collapsed,file={outputPath}")
 * </pre>
 */
public final class DirectAsyncProfilerRunner implements AsyncProfilerRunner {

  private static final Logger logger =
      Logger.getLogger(DirectAsyncProfilerRunner.class.getName());

  /** 缓存的可用性检查结果（避免每次重复检测） */
  @Nullable private volatile Boolean available;

  @Override
  public ProfilerResult profile(Path libPath, ProfileRequest request, Path outputPath)
      throws ProfilerException {

    logger.log(
        Level.INFO,
        "[ASYNC-PROFILER] Starting profiling: {0}, output={1}",
        new Object[] {request, outputPath});

    long startTime = System.currentTimeMillis();

    try {
      // 1. 获取 AsyncProfiler 实例（直接调用，无反射）
      AsyncProfiler profiler = AsyncProfiler.getInstance(libPath.toString());
      if (profiler == null) {
        throw new ProfilerException(
            "PROFILER_UNAVAILABLE", "AsyncProfiler.getInstance() returned null");
      }

      // 2. 构建并执行 start 命令
      String startCommand = buildStartCommand(request, outputPath);
      logger.log(Level.FINE, "[ASYNC-PROFILER] Executing start command: {0}", startCommand);
      executeCommand(profiler, startCommand);

      // 3. 等待采样时长
      logger.log(
          Level.INFO,
          "[ASYNC-PROFILER] Profiling in progress, waiting {0}ms...",
          request.getDurationMs());
      Thread.sleep(request.getDurationMs());

      // 4. 执行 stop 命令
      String stopCommand = buildStopCommand(request, outputPath);
      logger.log(Level.FINE, "[ASYNC-PROFILER] Executing stop command: {0}", stopCommand);
      executeCommand(profiler, stopCommand);

      // 5. 验证输出文件
      long durationMs = System.currentTimeMillis() - startTime;
      if (!Files.exists(outputPath)) {
        throw new ProfilerException(
            "OUTPUT_FILE_ERROR",
            "Profiler output file not found after stop: " + outputPath);
      }

      long fileSize = Files.size(outputPath);
      if (fileSize == 0) {
        throw new ProfilerException(
            "OUTPUT_FILE_ERROR", "Profiler output file is empty: " + outputPath);
      }

      ProfilerResult result =
          ProfilerResult.success(
              outputPath, fileSize, durationMs, request.getEventName(), request.getFormat());
      logger.log(Level.INFO, "[ASYNC-PROFILER] Profiling completed: {0}", result);
      return result;

    } catch (ProfilerException e) {
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ProfilerException(
          "PROFILER_INTERRUPTED", "Profiling was interrupted", e);
    } catch (Exception e) {
      throw new ProfilerException(
          "PROFILER_EXEC_FAILED", "Profiling execution failed: " + e.getMessage(), e);
    }
  }

  @Override
  public boolean isAvailable() {
    Boolean cached = available;
    if (cached != null) {
      return cached;
    }

    // 只需检查平台支持（API 已由 implementation 依赖保证在 classpath 中）
    AsyncProfilerResourceExtractor.PlatformInfo platform =
        AsyncProfilerResourceExtractor.detectPlatform();
    boolean supported = platform.isSupported();

    if (!supported) {
      logger.log(
          Level.FINE,
          "[ASYNC-PROFILER] Platform not supported: {0}",
          platform);
    }

    available = supported;
    return supported;
  }

  /**
   * 执行 AsyncProfiler 命令并检查结果
   *
   * @param profiler AsyncProfiler 实例
   * @param command 命令字符串
   * @return 命令执行结果
   * @throws ProfilerException 如果命令执行失败
   */
  private static String executeCommand(AsyncProfiler profiler, String command)
      throws ProfilerException {
    try {
      String result = profiler.execute(command);
      String resultStr = result != null ? result : "";

      // 检查是否返回了错误信息
      if (resultStr.startsWith("ERROR:") || resultStr.startsWith("Failed")) {
        throw new ProfilerException(
            "PROFILER_EXEC_FAILED", "Profiler command failed: " + resultStr);
      }

      return resultStr;
    } catch (ProfilerException e) {
      throw e;
    } catch (Exception e) {
      throw new ProfilerException(
          "PROFILER_EXEC_FAILED",
          "Failed to execute profiler command '" + command + "': " + e.getMessage(),
          e);
    }
  }

  /**
   * 构建 start 命令
   *
   * <p>行为取决于 {@link OutputFormat#isSpecifyFileOnStart()}：
   * <ul>
   *   <li>{@code true}（如 JFR）：start 时指定格式标记和文件路径</li>
   *   <li>{@code false}（如 collapsed）：start 时不指定文件</li>
   * </ul>
   */
  private static String buildStartCommand(ProfileRequest request, Path outputPath) {
    StringBuilder cmd = new StringBuilder("start");
    OutputFormat format = request.getOutputFormat();

    // 需要在 start 时指定格式标记（如 jfr）
    if (format.isSpecifyFileOnStart()) {
      cmd.append(",").append(format.getValue());
    }

    cmd.append(",event=").append(request.getEventName());
    cmd.append(",interval=").append(request.getInterval());

    if (request.isThreads()) {
      cmd.append(",threads");
    }

    // 需要在 start 时指定输出文件
    if (format.isSpecifyFileOnStart()) {
      cmd.append(",file=").append(outputPath.toAbsolutePath());
    }

    return cmd.toString();
  }

  /**
   * 构建 stop 命令
   *
   * <p>行为取决于 {@link OutputFormat#isSpecifyFileOnStart()}：
   * <ul>
   *   <li>{@code false}（如 collapsed）：stop 时指定格式标记和文件路径</li>
   *   <li>{@code true}（如 JFR）：stop 时仅发送 stop</li>
   * </ul>
   */
  private static String buildStopCommand(ProfileRequest request, Path outputPath) {
    StringBuilder cmd = new StringBuilder("stop");
    OutputFormat format = request.getOutputFormat();

    // 不在 start 时指定文件的格式，需要在 stop 时指定
    if (!format.isSpecifyFileOnStart()) {
      cmd.append(",").append(format.getValue());
      cmd.append(",file=").append(outputPath.toAbsolutePath());
    }

    return cmd.toString();
  }

  /**
   * 重置可用性缓存（用于测试）
   */
  void resetAvailabilityCache() {
    available = null;
  }
}
