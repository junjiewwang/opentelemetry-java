/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.status;

import io.opentelemetry.sdk.extension.controlplane.identity.AgentIdentityProvider;
import java.util.HashMap;
import java.util.Map;

/**
 * 运行时长收集器
 *
 * <p>收集 Agent 的运行时长信息，包括：
 * <ul>
 *   <li>uptimeMs - 运行时长（毫秒）
 *   <li>timestamp - 当前时间戳
 *   <li>runningState - 运行状态
 * </ul>
 */
public final class UptimeCollector implements AgentStatusCollector {

  private static final String NAME = "uptime";

  /** 运行状态枚举 */
  public enum RunningState {
    STARTING,
    RUNNING,
    STOPPING,
    STOPPED
  }

  private volatile RunningState currentState = RunningState.RUNNING;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Map<String, Object> collect() {
    AgentIdentityProvider.AgentIdentity identity = AgentIdentityProvider.get();
    long startTimeMs = identity.getStartTimeUnixNano() / 1_000_000;
    long currentTimeMs = System.currentTimeMillis();
    long uptimeMs = currentTimeMs - startTimeMs;

    Map<String, Object> data = new HashMap<>();
    data.put("uptimeMs", uptimeMs);
    data.put("timestamp", currentTimeMs);
    data.put("runningState", currentState.name());
    return data;
  }

  @Override
  public int getPriority() {
    return 10;
  }

  /**
   * 设置当前运行状态
   *
   * @param state 运行状态
   */
  public void setRunningState(RunningState state) {
    this.currentState = state;
  }

  /**
   * 获取当前运行状态
   *
   * @return 运行状态
   */
  public RunningState getRunningState() {
    return currentState;
  }
}
