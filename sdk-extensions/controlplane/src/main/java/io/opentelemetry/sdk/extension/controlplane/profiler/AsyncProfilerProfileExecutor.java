/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.profiler;

import io.opentelemetry.sdk.extension.controlplane.profiler.AsyncProfilerResourceExtractor.ExtractionResult;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionContext;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionResult;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutor;
import io.opentelemetry.sdk.extension.controlplane.task.status.TaskStatusEmitter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * AsyncProfiler 采样执行器
 *
 * <p>编排 profiling + upload + cleanup 的完整闭环（"终态"模式）：
 * <pre>
 *   1. 解析参数 → ProfileRequest
 *   2. 提取 native library → libPath
 *   3. 执行 profiling → 文件输出到指定路径
 *   4. 流式上传文件 → 服务端
 *   5. 上传成功 → 删除本地文件
 *   6. 返回 TaskExecutionResult.success()
 * </pre>
 *
 * <p>Executor 返回 SUCCESS 时，profiling + 上传都已完成（真终态）。
 * {@link io.opentelemetry.sdk.extension.controlplane.task.executor.TaskDispatcher}
 * 的超时控制自然覆盖整个流程。
 *
 * <p>并发控制：async-profiler 是进程级单例，同一时刻只能有一个采样在运行。
 * 内部使用 {@link AtomicBoolean} + CAS 实现快速失败的互斥。
 */
public final class AsyncProfilerProfileExecutor implements TaskExecutor {

  private static final Logger logger =
      Logger.getLogger(AsyncProfilerProfileExecutor.class.getName());

  /** 任务类型 */
  public static final String TASK_TYPE = "async-profiler";

  /** profiler 输出子目录名 */
  private static final String PROFILER_RESULTS_DIR = "profiler-results";

  /** 并发控制：同一时刻只允许一个 profiling */
  private final AtomicBoolean profiling = new AtomicBoolean(false);

  /** profiler 运行器 */
  private final AsyncProfilerRunner runner;

  /** 文件流式上传器 */
  private final FileStreamUploader uploader;

  /** 工作目录（用于存放 native lib 和 profiler 输出） */
  private final Path workDir;

  /** profiling 执行线程池（阻塞操作不应占用公共线程池） */
  private final ExecutorService profilingExecutor;

  /**
   * 创建 AsyncProfiler 采样执行器
   *
   * @param runner profiler 运行器
   * @param uploader 文件流式上传器
   * @param storageDir 存储目录路径
   */
  public AsyncProfilerProfileExecutor(
      AsyncProfilerRunner runner, FileStreamUploader uploader, String storageDir) {
    this.runner = runner;
    this.uploader = uploader;
    this.workDir = Paths.get(storageDir);
    this.profilingExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "otel-async-profiler");
              t.setDaemon(true);
              return t;
            });
  }

  @Override
  public String getTaskType() {
    return TASK_TYPE;
  }

  @Override
  public String getDescription() {
    return "AsyncProfiler one-shot profiling executor (file-first, stream upload)";
  }

  @Override
  public boolean isAvailable() {
    // 1. 平台支持检查
    if (!AsyncProfilerResourceExtractor.detectPlatform().isSupported()) {
      return false;
    }
    // 2. 并发检查：没有正在进行的采样
    return !profiling.get();
  }

  @Override
  public CompletableFuture<TaskExecutionResult> execute(TaskExecutionContext context) {
    // CAS 抢占：双重保险（isAvailable + CAS 覆盖竞态条件）
    if (!profiling.compareAndSet(false, true)) {
      return CompletableFuture.completedFuture(
          TaskExecutionResult.failed(
              "PROFILER_BUSY", "Another profiling session is already in progress"));
    }

    // 在独立线程中执行整个闭环（profiling 是阻塞操作）
    return CompletableFuture.supplyAsync(() -> doExecute(context), profilingExecutor)
        .whenComplete(
            (result, error) -> {
              // 无论成功/失败/异常，都释放锁
              profiling.set(false);
            })
        .exceptionally(
            error -> {
              String msg =
                  error.getMessage() != null ? error.getMessage() : error.getClass().getName();
              logger.log(
                  Level.SEVERE,
                  "[ASYNC-PROFILER] Unexpected error in profiling executor: {0}",
                  msg);
              return TaskExecutionResult.failed("PROFILER_EXEC_FAILED", msg);
            });
  }

  /**
   * 执行完整的 profiling → upload → cleanup 闭环
   *
   * <p>该方法在独立线程中同步执行，不需要额外的异步编排。
   */
  private TaskExecutionResult doExecute(TaskExecutionContext context) {
    String taskId = context.getTaskId();
    TaskStatusEmitter emitter = context.getStatusEmitter();

    // 1. 解析并校验参数
    ProfileRequest request = ProfileRequest.fromContext(context);
    String validationError = request.validate();
    if (validationError != null) {
      logger.log(
          Level.WARNING,
          "[ASYNC-PROFILER] Invalid parameters: taskId={0}, error={1}",
          new Object[] {taskId, validationError});
      return TaskExecutionResult.failed("INVALID_PARAMETERS", validationError);
    }

    logger.log(
        Level.INFO,
        "[ASYNC-PROFILER] Starting profiling task: taskId={0}, request={1}",
        new Object[] {taskId, request});

    // 2. 构建输出路径
    Path outputPath = buildOutputPath(taskId, request);

    try {
      // 确保输出目录存在
      Files.createDirectories(outputPath.getParent());
    } catch (IOException e) {
      return TaskExecutionResult.failed(
          "OUTPUT_FILE_ERROR",
          "Failed to create output directory: " + e.getMessage());
    }

    try {
      // 3. 提取 native library
      emitRunning(emitter, "Extracting profiler native library");
      ExtractionResult extractionResult = AsyncProfilerResourceExtractor.extractTo(workDir);
      if (!extractionResult.isSuccess()) {
        String errorCode =
            extractionResult.getStatus()
                    == AsyncProfilerResourceExtractor.ExtractionResult.Status.UNSUPPORTED
                ? "UNSUPPORTED_PLATFORM"
                : "RESOURCE_NOT_FOUND";
        return TaskExecutionResult.failed(
            errorCode,
            "Failed to extract profiler library: " + extractionResult.getMessage());
      }

      Path libPath = extractionResult.getLibraryPath();
      if (libPath == null) {
        return TaskExecutionResult.failed(
            "RESOURCE_NOT_FOUND", "Profiler library path is null after extraction");
      }

      // 4. 执行 profiling
      emitRunning(
          emitter,
          String.format(
              Locale.ROOT,
              "Profiling: event=%s, duration=%dms, interval=%d %s, format=%s",
              request.getEventName(),
              request.getDurationMs(),
              request.getInterval(),
              request.getEventType().getUnit(),
              request.getFormat()));

      ProfilerResult profilerResult = runner.profile(libPath, request, outputPath);
      if (!profilerResult.isSuccess()) {
        return TaskExecutionResult.failed(
            "PROFILER_EXEC_FAILED",
            "Profiling failed: " + profilerResult.getErrorMessage());
      }

      // 5. 流式上传结果
      emitRunning(
          emitter,
          String.format(
              Locale.ROOT,
              "Uploading result: size=%d bytes, format=%s",
              profilerResult.getFileSize(),
              request.getFormat()));

      // 构建结果数据模型（上传前 uploadResult 为 null）
      ProfilingResultData resultData = new ProfilingResultData(request, profilerResult, null);

      FileStreamUploader.UploadResult uploadResult =
          uploader
              .uploadFile(
                  taskId,
                  TASK_TYPE,
                  outputPath,
                  request.getContentType(),
                  resultData.toMetadata())
              .join(); // 同步等待（已在独立线程中）

      if (!uploadResult.isSuccess()) {
        // Phase A：上传失败，保留文件，返回失败
        logger.log(
            Level.WARNING,
            "[ASYNC-PROFILER] Upload failed: taskId={0}, reason={1}, file retained at {2}",
            new Object[] {taskId, uploadResult.getFailureReason(), outputPath});
        return TaskExecutionResult.failed(
            "UPLOAD_FAILED",
            "Upload failed: " + uploadResult.getFailureReason());
      }

      // 6. 上传成功，删除本地文件
      cleanupFile(outputPath, taskId);

      // 7. 构建成功结果（补充 uploadId）
      ProfilingResultData finalResultData =
          new ProfilingResultData(request, profilerResult, uploadResult);
      String resultJson = finalResultData.toJson();
      logger.log(
          Level.INFO,
          "[ASYNC-PROFILER] Profiling task completed successfully: taskId={0}",
          taskId);
      return TaskExecutionResult.success(resultJson);

    } catch (ProfilerException e) {
      logger.log(
          Level.WARNING,
          "[ASYNC-PROFILER] Profiler error: taskId={0}, code={1}, message={2}",
          new Object[] {taskId, e.getErrorCode(), e.getMessage()});
      cleanupFile(outputPath, taskId);
      String errorMsg =
          e.getMessage() != null ? e.getMessage() : e.getClass().getName();
      return TaskExecutionResult.failed(e.getErrorCode(), errorMsg);

    } catch (RuntimeException e) {
      String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
      logger.log(
          Level.SEVERE,
          "[ASYNC-PROFILER] Unexpected error: taskId={0}, error={1}",
          new Object[] {taskId, msg});
      cleanupFile(outputPath, taskId);
      return TaskExecutionResult.failed("PROFILER_EXEC_FAILED", msg);
    }
  }

  /**
   * 构建输出文件路径
   *
   * <p>格式：{workDir}/profiler-results/{taskId}.{format}
   */
  private Path buildOutputPath(String taskId, ProfileRequest request) {
    return workDir
        .resolve(PROFILER_RESULTS_DIR)
        .resolve(taskId + "." + request.getFileExtension());
  }

  /**
   * 安全发送 running 状态事件
   */
  private static void emitRunning(@Nullable TaskStatusEmitter emitter, String message) {
    if (emitter != null) {
      try {
        emitter.running(message);
      } catch (RuntimeException e) {
        // 状态上报失败不影响主流程
        String errMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
        logger.log(Level.FINE, "[ASYNC-PROFILER] Failed to emit running status: {0}", errMsg);
      }
    }
  }

  /**
   * 安全清理本地文件
   */
  private static void cleanupFile(Path filePath, String taskId) {
    try {
      if (Files.exists(filePath)) {
        Files.delete(filePath);
        logger.log(
            Level.FINE,
            "[ASYNC-PROFILER] Cleaned up output file: taskId={0}, path={1}",
            new Object[] {taskId, filePath});
      }
    } catch (IOException e) {
      logger.log(
          Level.WARNING,
          "[ASYNC-PROFILER] Failed to cleanup output file: taskId={0}, path={1}, error={2}",
          new Object[] {taskId, filePath, e.getMessage()});
    }
  }
}
