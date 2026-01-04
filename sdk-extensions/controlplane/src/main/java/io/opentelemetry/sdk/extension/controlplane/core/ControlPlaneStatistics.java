/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core;

import io.opentelemetry.sdk.extension.controlplane.status.ControlPlaneStateCollector;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 控制平面统计信息管理器。
 *
 * <p>负责管理控制平面的统计计数和周期性日志输出，包括：
 *
 * <ul>
 *   <li>配置轮询计数
 *   <li>任务轮询计数
 *   <li>状态上报计数
 *   <li>周期性状态日志
 * </ul>
 */
public final class ControlPlaneStatistics {

  private static final Logger logger = Logger.getLogger(ControlPlaneStatistics.class.getName());

  /** 默认状态日志输出间隔（毫秒） */
  private static final long DEFAULT_STATUS_LOG_INTERVAL_MS = 60_000;

  private final ControlPlaneStateCollector stateCollector;
  private final ConnectionStateManager connectionStateManager;
  private final HealthCheckCoordinator healthCheckCoordinator;
  private final String configUrl;

  private final AtomicLong configPollCount = new AtomicLong(0);
  private final AtomicLong taskPollCount = new AtomicLong(0);
  private final AtomicLong statusReportCount = new AtomicLong(0);
  private final AtomicLong lastStatusLogTime = new AtomicLong(0);
  private final long statusLogIntervalMs;

  /**
   * 创建统计信息管理器
   *
   * @param stateCollector 状态收集器
   * @param connectionStateManager 连接状态管理器
   * @param healthCheckCoordinator 健康检查协调器
   * @param configUrl 配置 URL
   */
  public ControlPlaneStatistics(
      ControlPlaneStateCollector stateCollector,
      ConnectionStateManager connectionStateManager,
      HealthCheckCoordinator healthCheckCoordinator,
      String configUrl) {
    this(stateCollector, connectionStateManager, healthCheckCoordinator, configUrl,
        DEFAULT_STATUS_LOG_INTERVAL_MS);
  }

  /**
   * 创建统计信息管理器（自定义日志间隔）
   *
   * @param stateCollector 状态收集器
   * @param connectionStateManager 连接状态管理器
   * @param healthCheckCoordinator 健康检查协调器
   * @param configUrl 配置 URL
   * @param statusLogIntervalMs 状态日志间隔（毫秒）
   */
  public ControlPlaneStatistics(
      ControlPlaneStateCollector stateCollector,
      ConnectionStateManager connectionStateManager,
      HealthCheckCoordinator healthCheckCoordinator,
      String configUrl,
      long statusLogIntervalMs) {
    this.stateCollector = stateCollector;
    this.connectionStateManager = connectionStateManager;
    this.healthCheckCoordinator = healthCheckCoordinator;
    this.configUrl = configUrl;
    this.statusLogIntervalMs = statusLogIntervalMs;
  }

  /**
   * 记录配置轮询
   *
   * @return 当前计数
   */
  public long recordConfigPoll() {
    long count = configPollCount.incrementAndGet();
    stateCollector.setConfigPollCount(count);
    return count;
  }

  /**
   * 记录任务轮询
   *
   * @return 当前计数
   */
  public long recordTaskPoll() {
    long count = taskPollCount.incrementAndGet();
    stateCollector.setTaskPollCount(count);
    return count;
  }

  /**
   * 记录状态上报
   *
   * @return 当前计数
   */
  public long recordStatusReport() {
    long count = statusReportCount.incrementAndGet();
    stateCollector.setStatusReportCount(count);
    return count;
  }

  /**
   * 记录配置获取成功
   */
  public void recordConfigFetchSuccess() {
    stateCollector.recordConfigFetch();
    stateCollector.setConnectionState(ConnectionStateManager.ConnectionState.CONNECTED.name());
  }

  /**
   * 更新连接状态到收集器
   *
   * @param state 连接状态
   */
  public void updateConnectionState(ConnectionStateManager.ConnectionState state) {
    stateCollector.setConnectionState(state.name());
  }

  /**
   * 获取配置轮询计数
   *
   * @return 计数
   */
  public long getConfigPollCount() {
    return configPollCount.get();
  }

  /**
   * 获取任务轮询计数
   *
   * @return 计数
   */
  public long getTaskPollCount() {
    return taskPollCount.get();
  }

  /**
   * 获取状态上报计数
   *
   * @return 计数
   */
  public long getStatusReportCount() {
    return statusReportCount.get();
  }

  /**
   * 周期性输出状态日志（如果满足时间间隔条件）
   */
  public void logPeriodicStatusIfNeeded() {
    long now = System.currentTimeMillis();
    long lastLog = lastStatusLogTime.get();
    if (now - lastLog >= statusLogIntervalMs && lastStatusLogTime.compareAndSet(lastLog, now)) {
      logStatus();
    }
  }

  /**
   * 强制输出状态日志
   */
  public void logStatus() {
    String otlpHealthInfo = healthCheckCoordinator.buildOtlpHealthInfo();

    logger.log(
        Level.INFO,
        "Control plane status - state: {0}, configPolls: {1}, taskPolls: {2}, statusReports: {3}, otlp: [{4}], configUrl: {5}",
        new Object[] {
          connectionStateManager.getState(),
          configPollCount.get(),
          taskPollCount.get(),
          statusReportCount.get(),
          otlpHealthInfo,
          configUrl
        });
  }
}
