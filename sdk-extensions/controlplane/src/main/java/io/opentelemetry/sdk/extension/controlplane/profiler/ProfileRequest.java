/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.profiler;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * AsyncProfiler 采样请求参数
 *
 * <p>封装一次性 profiling 的所有参数，包含参数校验逻辑。
 *
 * <p>支持的输出格式：
 * <ul>
 *   <li>{@code collapsed} — 折叠栈格式（文本，适合生成火焰图）
 *   <li>{@code jfr} — Java Flight Recorder 格式（二进制，信息更丰富）
 * </ul>
 */
public final class ProfileRequest {

  /** 支持的事件类型白名单 */
  private static final Set<String> ALLOWED_EVENTS =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList("cpu", "alloc", "lock", "wall")));

  /** 支持的输出格式白名单 */
  private static final Set<String> ALLOWED_FORMATS =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList("collapsed", "jfr")));

  /** 默认采样时长：30 秒 */
  private static final long DEFAULT_DURATION_MS = 30_000;

  /** 最大采样时长：120 秒 */
  private static final long MAX_DURATION_MS = 120_000;

  /** 最小采样时长：1 秒 */
  private static final long MIN_DURATION_MS = 1_000;

  /** 默认采样间隔：10ms（纳秒） */
  private static final long DEFAULT_INTERVAL_NS = 10_000_000L;

  private final long durationMs;
  private final String event;
  private final String format;
  private final long intervalNs;
  private final boolean threads;

  private ProfileRequest(Builder builder) {
    this.durationMs = builder.durationMs;
    this.event = builder.event;
    this.format = builder.format;
    this.intervalNs = builder.intervalNs;
    this.threads = builder.threads;
  }

  // ===== Getters =====

  /** 获取采样时长（毫秒） */
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

  /** 获取采样间隔（纳秒） */
  public long getIntervalNs() {
    return intervalNs;
  }

  /** 是否按线程分组 */
  public boolean isThreads() {
    return threads;
  }

  /**
   * 获取对应的 Content-Type
   *
   * @return MIME 类型
   */
  public String getContentType() {
    if ("jfr".equals(format)) {
      return "application/x-jfr";
    }
    return "text/plain; charset=utf-8";
  }

  /**
   * 获取输出文件扩展名
   *
   * @return 文件扩展名（不含点号）
   */
  public String getFileExtension() {
    return format;
  }

  /**
   * 校验请求参数
   *
   * @return 校验失败的原因，null 表示校验通过
   */
  @Nullable
  public String validate() {
    if (durationMs < MIN_DURATION_MS || durationMs > MAX_DURATION_MS) {
      return String.format(
          Locale.ROOT,
          "duration_ms must be between %d and %d, got %d",
          MIN_DURATION_MS, MAX_DURATION_MS, durationMs);
    }
    if (!ALLOWED_EVENTS.contains(event)) {
      return "Unsupported event type: " + event + ", allowed: " + ALLOWED_EVENTS;
    }
    if (!ALLOWED_FORMATS.contains(format)) {
      return "Unsupported format: " + format + ", allowed: " + ALLOWED_FORMATS;
    }
    if (intervalNs <= 0) {
      return "interval_ns must be positive, got " + intervalNs;
    }
    return null;
  }

  /**
   * 从任务参数构建请求
   *
   * @param context 任务执行上下文
   * @return ProfileRequest
   */
  public static ProfileRequest fromContext(
      io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionContext context) {
    return builder()
        .durationMs(context.getLongParameter("duration_ms", DEFAULT_DURATION_MS))
        .event(context.getStringParameter("event", "cpu"))
        .format(context.getStringParameter("format", "collapsed"))
        .intervalNs(context.getLongParameter("interval_ns", DEFAULT_INTERVAL_NS))
        .threads(context.getBooleanParameter("threads", false))
        .build();
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "ProfileRequest{duration=%dms, event='%s', format='%s', interval=%dns, threads=%s}",
        durationMs, event, format, intervalNs, threads);
  }

  // ===== Builder =====

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private long durationMs = DEFAULT_DURATION_MS;
    private String event = "cpu";
    private String format = "collapsed";
    private long intervalNs = DEFAULT_INTERVAL_NS;
    private boolean threads = false;

    private Builder() {}

    public Builder durationMs(long durationMs) {
      this.durationMs = durationMs;
      return this;
    }

    public Builder event(String event) {
      this.event = event != null ? event.toLowerCase(Locale.ROOT) : "cpu";
      return this;
    }

    public Builder format(String format) {
      this.format = format != null ? format.toLowerCase(Locale.ROOT) : "collapsed";
      return this;
    }

    public Builder intervalNs(long intervalNs) {
      this.intervalNs = intervalNs;
      return this;
    }

    public Builder threads(boolean threads) {
      this.threads = threads;
      return this;
    }

    public ProfileRequest build() {
      return new ProfileRequest(this);
    }
  }
}
