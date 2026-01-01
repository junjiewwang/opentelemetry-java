/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

/**
 * 长轮询类型枚举
 *
 * <p>定义支持的长轮询类型，用于区分不同的轮询请求。
 */
public enum LongPollType {
  /** 配置轮询 - 用于获取动态配置更新 */
  CONFIG("config"),

  /** 任务轮询 - 用于获取待执行的任务 */
  TASK("task"),

  /** 合并轮询 - 一次请求同时获取配置和任务（推荐） */
  COMBINED("combined");

  private final String name;

  LongPollType(String name) {
    this.name = name;
  }

  /**
   * 获取类型名称
   *
   * @return 类型名称
   */
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return name;
  }
}
