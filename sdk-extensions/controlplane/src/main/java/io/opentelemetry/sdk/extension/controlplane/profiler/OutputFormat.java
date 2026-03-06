/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.profiler;

import java.util.Locale;
import javax.annotation.Nullable;

/**
 * AsyncProfiler 输出格式枚举
 *
 * <p>封装输出格式相关的所有属性，包括：
 * <ul>
 *   <li>格式名称（async-profiler 命令行参数值）</li>
 *   <li>MIME Content-Type</li>
 *   <li>命令行行为差异（JFR 在 start 时指定文件，collapsed 在 stop 时指定）</li>
 * </ul>
 *
 * <p>新增格式时只需在此处添加一行枚举常量，无需修改其他文件。
 *
 * @see EventType
 */
public enum OutputFormat {

  /**
   * 折叠栈格式（文本，适合生成火焰图）
   *
   * <p>命令行行为：start 时不指定文件，stop 时指定格式和文件路径。
   * <pre>
   *   start,event=cpu,interval=10000000
   *   stop,collapsed,file=/path/to/output.collapsed
   * </pre>
   */
  COLLAPSED("collapsed", "text/plain; charset=utf-8", /* specifyFileOnStart= */ false),

  /**
   * Java Flight Recorder 格式（二进制，信息更丰富）
   *
   * <p>命令行行为：start 时需要指定 jfr 标记和文件路径，stop 时只需 stop。
   * <pre>
   *   start,jfr,event=cpu,interval=10000000,file=/path/to/output.jfr
   *   stop
   * </pre>
   */
  JFR("jfr", "application/x-jfr", /* specifyFileOnStart= */ true);

  /** async-profiler 命令行中使用的格式名称 */
  private final String value;

  /** 对应的 MIME Content-Type */
  private final String contentType;

  /**
   * 是否在 start 命令时指定输出文件
   *
   * <p>{@code true}：start 时指定（如 JFR），stop 时不指定
   * <p>{@code false}：start 时不指定，stop 时指定（如 collapsed）
   */
  private final boolean specifyFileOnStart;

  OutputFormat(String value, String contentType, boolean specifyFileOnStart) {
    this.value = value;
    this.contentType = contentType;
    this.specifyFileOnStart = specifyFileOnStart;
  }

  /** 获取格式名称（用于 async-profiler 命令行参数和文件扩展名） */
  public String getValue() {
    return value;
  }

  /** 获取对应的 MIME Content-Type */
  public String getContentType() {
    return contentType;
  }

  /**
   * 是否在 start 命令时指定输出文件
   *
   * @return {@code true} 表示 start 时指定文件（stop 时不指定），
   *         {@code false} 表示 stop 时指定文件（start 时不指定）
   */
  public boolean isSpecifyFileOnStart() {
    return specifyFileOnStart;
  }

  /**
   * 获取输出文件扩展名（不含点号）
   *
   * <p>当前直接使用格式名称作为扩展名。
   *
   * @return 文件扩展名
   */
  public String getFileExtension() {
    return value;
  }

  /**
   * 根据格式名称解析枚举值
   *
   * @param formatName 格式名称（不区分大小写）
   * @return 对应的枚举值，未知格式返回 null
   */
  @Nullable
  public static OutputFormat fromString(String formatName) {
    if (formatName == null) {
      return null;
    }
    String lower = formatName.toLowerCase(Locale.ROOT);
    for (OutputFormat format : values()) {
      if (format.value.equals(lower)) {
        return format;
      }
    }
    return null;
  }

  /**
   * 获取所有支持的格式名称（用于错误提示）
   *
   * @return 逗号分隔的格式名称列表，如 "[collapsed, jfr]"
   */
  public static String supportedValues() {
    StringBuilder sb = new StringBuilder("[");
    OutputFormat[] formats = values();
    for (int i = 0; i < formats.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(formats[i].value);
    }
    sb.append("]");
    return sb.toString();
  }
}
