/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.status;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * 控制平面状态持有器
 *
 * <p>持有控制平面的连接状态信息，用于统计和监控，包括：
 * <ul>
 *   <li>connectionState - 连接状态
 *   <li>configVersion - 当前配置版本
 *   <li>lastConfigFetchTime - 最后配置拉取时间
 *   <li>configPollCount - 配置轮询次数
 *   <li>taskPollCount - 任务轮询次数
 * </ul>
 */
public final class ControlPlaneStateCollector {

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
