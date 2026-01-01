/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.tasks;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient;
import io.opentelemetry.sdk.extension.controlplane.core.ConnectionStateManager;
import io.opentelemetry.sdk.extension.controlplane.core.ControlPlaneStatistics;
import io.opentelemetry.sdk.extension.controlplane.core.HealthCheckCoordinator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 配置轮询任务。
 *
 * <p>负责定期从控制平面服务器拉取配置。
 */
public final class ConfigPollTask implements Runnable {

  private static final Logger logger = Logger.getLogger(ConfigPollTask.class.getName());

  private final ControlPlaneClient client;
  private final ConnectionStateManager connectionStateManager;
  private final HealthCheckCoordinator healthCheckCoordinator;
  private final ControlPlaneStatistics statistics;

  /**
   * 创建配置轮询任务
   *
   * @param client 控制平面客户端
   * @param connectionStateManager 连接状态管理器
   * @param healthCheckCoordinator 健康检查协调器
   * @param statistics 统计管理器
   */
  public ConfigPollTask(
      ControlPlaneClient client,
      ConnectionStateManager connectionStateManager,
      HealthCheckCoordinator healthCheckCoordinator,
      ControlPlaneStatistics statistics) {
    this.client = client;
    this.connectionStateManager = connectionStateManager;
    this.healthCheckCoordinator = healthCheckCoordinator;
    this.statistics = statistics;
  }

  @Override
  public void run() {
    long count = statistics.recordConfigPoll();

    if (!healthCheckCoordinator.shouldConnect()) {
      statistics.logPeriodicStatusIfNeeded();
      return;
    }

    try {
      logger.log(Level.FINE, "Polling config (count: {0})...", count);

      boolean success = client.fetchConfig();

      if (success) {
        // 请求成功，更新连接状态为 CONNECTED
        ConnectionStateManager.ConnectionState previousState = connectionStateManager.markConnected();
        statistics.recordConfigFetchSuccess();
        if (previousState != ConnectionStateManager.ConnectionState.CONNECTED) {
          logger.log(
              Level.INFO,
              "Control plane connected, state changed: {0} -> CONNECTED",
              previousState);
        }
      } else {
        // 请求失败（如 404、500 等），说明服务端不可用
        ConnectionStateManager.ConnectionState previousState = connectionStateManager.getState();
        if (previousState != ConnectionStateManager.ConnectionState.SERVER_UNAVAILABLE) {
          connectionStateManager.markServerUnavailable();
          statistics.updateConnectionState(ConnectionStateManager.ConnectionState.SERVER_UNAVAILABLE);
          logger.log(
              Level.WARNING,
              "Control plane server unavailable (API endpoint may not exist), state changed: {0} -> SERVER_UNAVAILABLE",
              previousState);
        }
      }
      statistics.logPeriodicStatusIfNeeded();
    } catch (RuntimeException e) {
      ConnectionStateManager.ConnectionState previousState =
          connectionStateManager.markDisconnected(e.getMessage());
      logger.log(
          Level.WARNING,
          "Failed to poll config (count: {0}, state: {1} -> DISCONNECTED): {2}",
          new Object[] {count, previousState, e.getMessage()});
    }
  }
}
