/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import java.util.Locale;
import javax.annotation.Nullable;

/**
 * 动态增强类型枚举
 *
 * <p>定义支持的增强类型，每种类型对应不同的 ByteBuddy Advice 实现和 OTel SDK 桥接方式。
 */
public enum InstrumentationType {

  /** 链路采集：在方法入口/出口创建 Span */
  TRACE("trace"),

  /** 指标采集：记录方法调用次数和耗时 */
  METRIC("metric"),

  /** 日志采集：在方法入口/出口记录日志 */
  LOG("log");

  private final String value;

  InstrumentationType(String value) {
    this.value = value;
  }

  /** 获取类型标识字符串 */
  public String getValue() {
    return value;
  }

  /**
   * 根据字符串解析枚举值
   *
   * @param typeName 类型名称（不区分大小写）
   * @return 对应的枚举值，未知类型返回 null
   */
  @Nullable
  public static InstrumentationType fromString(String typeName) {
    if (typeName == null) {
      return null;
    }
    String lower = typeName.toLowerCase(Locale.ROOT);
    for (InstrumentationType type : values()) {
      if (type.value.equals(lower)) {
        return type;
      }
    }
    return null;
  }

  /**
   * 获取所有支持的类型名称
   *
   * @return 逗号分隔的类型列表，如 "[trace, metric, log]"
   */
  public static String supportedValues() {
    StringBuilder sb = new StringBuilder("[");
    InstrumentationType[] types = values();
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
