/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.profiler;

import java.util.Locale;
import javax.annotation.Nullable;

/**
 * AsyncProfiler 支持的事件类型枚举
 *
 * <p>不同事件类型的 interval 参数含义不同：
 * <ul>
 *   <li>{@link #CPU}, {@link #WALL} — interval 表示采样时间间隔（单位：纳秒）
 *   <li>{@link #ALLOC} — interval 表示内存分配字节阈值（单位：字节），每分配 N 字节记录一次堆栈
 *   <li>{@link #LOCK} — interval 表示锁等待时间阈值（单位：纳秒）
 * </ul>
 *
 * <p>每种事件封装了 interval 的默认值、合法范围和单位描述，
 * 避免调用方为不同事件类型手动处理参数语义差异。
 */
public enum EventType {

  /** CPU 采样：interval 为采样时间间隔（纳秒），默认 10ms */
  CPU("cpu", 10_000_000L, 1_000_000L, 1_000_000_000L, "ns",
      "sampling time interval in nanoseconds"),

  /** Wall-clock 采样：interval 为采样时间间隔（纳秒），默认 10ms */
  WALL("wall", 10_000_000L, 1_000_000L, 1_000_000_000L, "ns",
      "sampling time interval in nanoseconds"),

  /** 内存分配采样：interval 为分配字节阈值（字节），默认 512KB */
  ALLOC("alloc", 524_288L, 4_096L, 104_857_600L, "bytes",
      "allocation size threshold in bytes"),

  /** 锁竞争采样：interval 为锁等待时间阈值（纳秒），默认 10ms */
  LOCK("lock", 10_000_000L, 1_000L, 10_000_000_000L, "ns",
      "lock wait time threshold in nanoseconds");

  /** async-profiler 命令行中使用的事件名称 */
  private final String value;

  /** 默认 interval 值 */
  private final long defaultInterval;

  /** interval 最小值（含） */
  private final long minInterval;

  /** interval 最大值（含） */
  private final long maxInterval;

  /** interval 单位标签（用于错误提示） */
  private final String unit;

  /** interval 含义描述（用于错误提示） */
  private final String description;

  EventType(
      String value,
      long defaultInterval,
      long minInterval,
      long maxInterval,
      String unit,
      String description) {
    this.value = value;
    this.defaultInterval = defaultInterval;
    this.minInterval = minInterval;
    this.maxInterval = maxInterval;
    this.unit = unit;
    this.description = description;
  }

  /** 获取 async-profiler 命令行中使用的事件名称 */
  public String getValue() {
    return value;
  }

  /** 获取默认 interval 值 */
  public long getDefaultInterval() {
    return defaultInterval;
  }

  /** 获取 interval 最小值（含） */
  public long getMinInterval() {
    return minInterval;
  }

  /** 获取 interval 最大值（含） */
  public long getMaxInterval() {
    return maxInterval;
  }

  /** 获取 interval 单位标签 */
  public String getUnit() {
    return unit;
  }

  /** 获取 interval 含义描述 */
  public String getDescription() {
    return description;
  }

  /**
   * 校验 interval 值是否在合法范围内
   *
   * @param interval 待校验的 interval 值
   * @return 校验失败原因，null 表示校验通过
   */
  @Nullable
  public String validateInterval(long interval) {
    if (interval < minInterval || interval > maxInterval) {
      return String.format(
          Locale.ROOT,
          "interval for event '%s' must be between %d and %d %s (%s), got %d",
          value, minInterval, maxInterval, unit, description, interval);
    }
    return null;
  }

  /**
   * 根据事件名称解析枚举值
   *
   * @param eventName 事件名称（不区分大小写）
   * @return 对应的枚举值，未知事件返回 null
   */
  @Nullable
  public static EventType fromString(String eventName) {
    if (eventName == null) {
      return null;
    }
    String lower = eventName.toLowerCase(Locale.ROOT);
    for (EventType type : values()) {
      if (type.value.equals(lower)) {
        return type;
      }
    }
    return null;
  }

  /**
   * 获取所有支持的事件名称（用于错误提示）
   *
   * @return 逗号分隔的事件名称列表
   */
  public static String supportedValues() {
    StringBuilder sb = new StringBuilder("[");
    EventType[] types = values();
    for (int i = 0; i < types.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(types[i].value);
    }
    sb.append("]");
    return sb.toString();
  }
}
