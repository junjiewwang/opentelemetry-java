/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.status;

import java.util.Map;

/**
 * Agent 状态收集器接口
 *
 * <p>采用策略模式，支持灵活扩展不同类型的状态收集。 每个收集器负责收集特定领域的状态信息。
 */
public interface AgentStatusCollector {

  /**
   * 获取收集器名称
   *
   * @return 收集器唯一标识名称
   */
  String getName();

  /**
   * 收集状态数据
   *
   * @return 收集到的状态数据，key 为字段名，value 为字段值
   */
  Map<String, Object> collect();

  /**
   * 获取收集器优先级，数值越小优先级越高
   *
   * @return 优先级
   */
  default int getPriority() {
    return 100;
  }

  /**
   * 检查收集器是否可用
   *
   * @return 是否可用
   */
  default boolean isEnabled() {
    return true;
  }
}
