/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.status;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * 控制平面状态收集器
 *
 * <p>收集控制平面的连接状态信息，包括：
 * <ul>
 *   <li>connectionState - 连接状态
 *   <li>configVersion - 当前配置版本
 *   <li>lastConfigFetchTime - 最后配置拉取时间
 *   <li>configPollCount - 配置轮询次数
 *   <li>taskPollCount - 任务轮询次数
 * </ul>
 */
public final class ControlPlaneStateCollector implements AgentStatusCollector {

  private static final String NAME = "controlPlaneState";

  private final AtomicReference<String> connectionState;
  private final AtomicReference<String> configVersion;
  private final AtomicLong lastConfigFetchTime;
  private final AtomicLong configPollCount;
  private final AtomicLong taskPollCount;
  private final AtomicLong statusReportCount;

  public ControlPlaneStateCollector() {
    this.connectionState = new AtomicReference<>("DISCONNECTED");
    this.configVersion = new AtomicReference<>("");
    this.lastConfigFetchTime = new AtomicLong(0);
    this.configPollCount = new AtomicLong(0);
    this.taskPollCount = new AtomicLong(0);
    this.statusReportCount = new AtomicLong(0);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Map<String, Object> collect() {
    Map<String, Object> data = new HashMap<>();
    data.put("connectionState", connectionState.get());
    
    String version = configVersion.get();
    if (version != null && !version.isEmpty()) {
      data.put("configVersion", version);
    }
    
    long fetchTime = lastConfigFetchTime.get();
    if (fetchTime > 0) {
      data.put("lastConfigFetchTime", fetchTime);
    }
    
    data.put("configPollCount", configPollCount.get());
    data.put("taskPollCount", taskPollCount.get());
    data.put("statusReportCount", statusReportCount.get());
    
    return data;
  }

  @Override
  public int getPriority() {
    return 15;
  }

  // ============ 状态更新方法 ============

  public void setConnectionState(String state) {
    this.connectionState.set(state);
  }

  public void setConfigVersion(String version) {
    this.configVersion.set(version);
  }

  public void recordConfigFetch() {
    this.lastConfigFetchTime.set(System.currentTimeMillis());
    this.configPollCount.incrementAndGet();
  }

  public void recordTaskPoll() {
    this.taskPollCount.incrementAndGet();
  }

  public void recordStatusReport() {
    this.statusReportCount.incrementAndGet();
  }

  public void setConfigPollCount(long count) {
    this.configPollCount.set(count);
  }

  public void setTaskPollCount(long count) {
    this.taskPollCount.set(count);
  }

  public void setStatusReportCount(long count) {
    this.statusReportCount.set(count);
  }

  // ============ Getters ============

  public String getConnectionState() {
    String state = connectionState.get();
    return state != null ? state : "DISCONNECTED";
  }

  @Nullable
  public String getConfigVersion() {
    return configVersion.get();
  }

  public long getLastConfigFetchTime() {
    return lastConfigFetchTime.get();
  }

  public long getConfigPollCount() {
    return configPollCount.get();
  }

  public long getTaskPollCount() {
    return taskPollCount.get();
  }

  public long getStatusReportCount() {
    return statusReportCount.get();
  }
}
