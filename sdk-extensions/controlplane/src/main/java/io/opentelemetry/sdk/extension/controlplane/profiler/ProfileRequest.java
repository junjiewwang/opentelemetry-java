/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.profiler;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.Arrays;
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
 *
 * <p>interval 参数的含义取决于事件类型，详见 {@link EventType}。
 */
public final class ProfileRequest {

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

  private final long durationMs;
  private final EventType eventType;
  private final String format;
  private final long interval;
  private final boolean threads;

  private ProfileRequest(Builder builder) {
    this.durationMs = builder.durationMs;
    this.eventType = builder.eventType;
    this.format = builder.format;
    this.interval = builder.interval;
    this.threads = builder.threads;
  }

  // ===== Getters =====

  /** 获取采样时长（毫秒） */
  public long getDurationMs() {
    return durationMs;
  }

  /** 获取采样事件类型枚举 */
  public EventType getEventType() {
    return eventType;
  }

  /**
   * 获取事件名称字符串（便利方法，等价于 {@code getEventType().getValue()}）
   *
   * @return 事件名称，如 "cpu"、"alloc" 等
   */
  public String getEventName() {
    return eventType.getValue();
  }

  /** 获取输出格式 */
  public String getFormat() {
    return format;
  }

  /**
   * 获取采样间隔/阈值
   *
   * <p>含义取决于事件类型：
   * <ul>
   *   <li>cpu/wall/lock — 时间间隔（纳秒）
   *   <li>alloc — 分配字节阈值（字节）
   * </ul>
   *
   * @see EventType#getDescription()
   */
  public long getInterval() {
    return interval;
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
    if (!ALLOWED_FORMATS.contains(format)) {
      return "Unsupported format: " + format + ", allowed: " + ALLOWED_FORMATS;
    }
    // 委托 EventType 校验 interval 范围
    return eventType.validateInterval(interval);
  }

  /**
   * 从任务参数构建请求
   *
   * @param context 任务执行上下文
   * @return ProfileRequest
   */
  public static ProfileRequest fromContext(
      io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionContext context) {
    // 解析事件类型
    String eventStr = context.getStringParameter("event", "cpu");
    EventType eventType = EventType.fromString(eventStr);
    if (eventType == null) {
      // 未知事件类型，使用 CPU 作为回退（validate() 会在后续校验中捕获问题）
      // 但此处直接抛出更清晰的错误
      throw new IllegalArgumentException(
          "Unsupported event type: " + eventStr + ", supported: " + EventType.supportedValues());
    }

    // interval 默认值由事件类型决定
    long interval = context.getLongParameter("interval", eventType.getDefaultInterval());

    return builder()
        .durationMs(context.getLongParameter("duration_ms", DEFAULT_DURATION_MS))
        .eventType(eventType)
        .format(context.getStringParameter("format", "collapsed"))
        .interval(interval)
        .threads(context.getBooleanParameter("threads", false))
        .build();
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "ProfileRequest{duration=%dms, event='%s', format='%s', interval=%d %s, threads=%s}",
        durationMs, eventType.getValue(), format, interval, eventType.getUnit(), threads);
  }

  // ===== Builder =====

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private long durationMs = DEFAULT_DURATION_MS;
    private EventType eventType = EventType.CPU;
    private String format = "collapsed";
    private long interval = EventType.CPU.getDefaultInterval();
    private boolean threads = false;

    private Builder() {}

    public Builder durationMs(long durationMs) {
      this.durationMs = durationMs;
      return this;
    }

    public Builder eventType(EventType eventType) {
      this.eventType = eventType;
      // 如果 interval 还是旧的默认值，则更新为新事件类型的默认值
      return this;
    }

    public Builder format(String format) {
      this.format = format != null ? format.toLowerCase(Locale.ROOT) : "collapsed";
      return this;
    }

    public Builder interval(long interval) {
      this.interval = interval;
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
