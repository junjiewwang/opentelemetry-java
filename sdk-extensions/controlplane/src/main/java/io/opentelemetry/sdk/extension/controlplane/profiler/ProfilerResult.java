/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.profiler;

import java.nio.file.Path;
import java.util.Locale;
import javax.annotation.Nullable;

/**
 * AsyncProfiler 采样结果
 *
 * <p>封装一次 profiling 完成后的结果信息，包含输出文件路径和元数据。
 */
public final class ProfilerResult {

  @Nullable private final Path outputPath;
  private final long fileSize;
  private final long durationMs;
  private final String event;
  private final String format;
  @Nullable private final String errorMessage;

  private ProfilerResult(Builder builder) {
    this.outputPath = builder.outputPath;
    this.fileSize = builder.fileSize;
    this.durationMs = builder.durationMs;
    this.event = builder.event;
    this.format = builder.format;
    this.errorMessage = builder.errorMessage;
  }

  /** 获取输出文件路径（采样失败时可能为 null） */
  @Nullable
  public Path getOutputPath() {
    return outputPath;
  }

  /** 获取输出文件大小（字节） */
  public long getFileSize() {
    return fileSize;
  }

  /** 获取实际采样时长（毫秒） */
  public long getDurationMs() {
    return durationMs;
  }

  /** 获取采样事件类型 */
  public String getEvent() {
    return event;
  }

  /** 获取输出格式 */
  public String getFormat() {
    return format;
  }

  /** 获取错误信息（如果采样失败） */
  @Nullable
  public String getErrorMessage() {
    return errorMessage;
  }

  /** 是否采样成功 */
  public boolean isSuccess() {
    return errorMessage == null;
  }

  @Override
  public String toString() {
    if (isSuccess()) {
      return String.format(
          Locale.ROOT,
          "ProfilerResult{success, path=%s, size=%d, duration=%dms, event=%s, format=%s}",
          outputPath, fileSize, durationMs, event, format);
    }
    return String.format(
        Locale.ROOT,
        "ProfilerResult{failed, error='%s', event=%s, format=%s}",
        errorMessage, event, format);
  }

  // ===== 工厂方法 =====

  /**
   * 创建成功结果
   */
  public static ProfilerResult success(
      Path outputPath, long fileSize, long durationMs, String event, String format) {
    return new Builder()
        .outputPath(outputPath)
        .fileSize(fileSize)
        .durationMs(durationMs)
        .event(event)
        .format(format)
        .build();
  }

  /**
   * 创建失败结果
   */
  public static ProfilerResult failed(String errorMessage, String event, String format) {
    return new Builder()
        .event(event)
        .format(format)
        .errorMessage(errorMessage)
        .build();
  }

  // ===== Builder =====

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    @Nullable private Path outputPath;
    private long fileSize;
    private long durationMs;
    private String event = "";
    private String format = "";
    @Nullable private String errorMessage;

    private Builder() {}

    public Builder outputPath(@Nullable Path outputPath) {
      this.outputPath = outputPath;
      return this;
    }

    public Builder fileSize(long fileSize) {
      this.fileSize = fileSize;
      return this;
    }

    public Builder durationMs(long durationMs) {
      this.durationMs = durationMs;
      return this;
    }

    public Builder event(String event) {
      this.event = event;
      return this;
    }

    public Builder format(String format) {
      this.format = format;
      return this;
    }

    public Builder errorMessage(@Nullable String errorMessage) {
      this.errorMessage = errorMessage;
      return this;
    }

    public ProfilerResult build() {
      return new ProfilerResult(this);
    }
  }
}
