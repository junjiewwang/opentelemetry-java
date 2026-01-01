/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.ConfigRequest;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.ConfigResponse;
import io.opentelemetry.sdk.extension.controlplane.core.ControlPlaneStatistics;
import io.opentelemetry.sdk.extension.controlplane.task.TaskExecutionLogger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 配置长轮询处理器
 *
 * <p>负责从控制平面获取配置更新，支持增量更新（通过 ETag 和版本号）。
 */
public final class ConfigLongPollHandler implements LongPollHandler<ConfigResponse> {

  private static final Logger logger = Logger.getLogger(ConfigLongPollHandler.class.getName());

  private final ControlPlaneClient client;
  private final ControlPlaneStatistics statistics;
  private final LongPollConfig config;
  private final String agentId;
  private final AtomicBoolean running;
  private final TaskExecutionLogger taskLogger;

  // 配置状态（用于增量更新）
  private volatile String currentConfigEtag = "";
  private volatile String currentConfigVersion = "";

  // 当前任务 ID（用于日志）
  private volatile String currentTaskId = "";

  /**
   * 创建配置长轮询处理器
   *
   * @param client 控制平面客户端
   * @param statistics 统计管理器
   * @param config 长轮询配置
   * @param agentId Agent ID
   * @param running 运行状态标志
   */
  public ConfigLongPollHandler(
      ControlPlaneClient client,
      ControlPlaneStatistics statistics,
      LongPollConfig config,
      String agentId,
      AtomicBoolean running) {
    this.client = client;
    this.statistics = statistics;
    this.config = config;
    this.agentId = agentId;
    this.running = running;
    this.taskLogger = TaskExecutionLogger.getInstance();
  }

  @Override
  public LongPollType getType() {
    return LongPollType.CONFIG;
  }

  @Override
  public Map<String, Object> buildRequestParams() {
    Map<String, Object> params = new HashMap<>();
    params.put("agentId", agentId);
    params.put("currentConfigVersion", currentConfigVersion);
    params.put("currentEtag", currentConfigEtag);
    params.put("timeoutMillis", config.getTimeoutMillis());
    return params;
  }

  @Override
  public HandlerResult handleResponse(ConfigResponse response) {
    if (!response.isSuccess()) {
      taskLogger.logTaskProgress(
          currentTaskId,
          "config_error",
          "Config response error: " + response.getErrorMessage());
      return HandlerResult.noChange();
    }

    statistics.recordConfigFetchSuccess();

    if (response.hasChanges()) {
      taskLogger.logTaskProgress(
          currentTaskId,
          "config_changed",
          "Config version: " + response.getConfigVersion() + ", etag: " + response.getEtag());

      // 更新本地状态
      this.currentConfigVersion = response.getConfigVersion();
      this.currentConfigEtag = response.getEtag();

      logger.log(
          Level.INFO,
          "Config updated, version: {0}, etag: {1}",
          new Object[] {currentConfigVersion, currentConfigEtag});

      return HandlerResult.changed(
          "version=" + currentConfigVersion + ", etag=" + currentConfigEtag);
    } else {
      taskLogger.logTaskProgress(currentTaskId, "config_unchanged", "No config changes");
      return HandlerResult.noChange();
    }
  }

  @Override
  public void handleError(Throwable error) {
    logger.log(Level.WARNING, "Config poll failed: {0}", error.getMessage());
    taskLogger.logTaskProgress(
        currentTaskId, "config_error", "Config poll error: " + error.getMessage());
  }

  @Override
  public boolean shouldContinue() {
    return running.get();
  }

  /**
   * 发起配置轮询请求
   *
   * @return 配置响应 Future
   */
  @Override
  public CompletableFuture<ConfigResponse> poll() {
    statistics.recordConfigPoll();
    return client.getConfig(createConfigRequest());
  }

  /**
   * 设置当前任务 ID（用于日志追踪）
   *
   * @param taskId 任务 ID
   */
  @Override
  public void setCurrentTaskId(String taskId) {
    this.currentTaskId = taskId;
  }

  /** 创建配置请求 */
  private ConfigRequest createConfigRequest() {
    return new ConfigRequest() {
      @Override
      public String getAgentId() {
        return agentId;
      }

      @Override
      public String getCurrentConfigVersion() {
        return currentConfigVersion;
      }

      @Override
      public String getCurrentEtag() {
        return currentConfigEtag;
      }

      @Override
      public long getLongPollTimeoutMillis() {
        return config.getTimeoutMillis();
      }
    };
  }

  // ===== Getters =====

  /**
   * 获取当前配置版本
   *
   * @return 配置版本
   */
  public String getCurrentConfigVersion() {
    return currentConfigVersion;
  }

  /**
   * 获取当前配置 ETag
   *
   * @return ETag
   */
  public String getCurrentConfigEtag() {
    return currentConfigEtag;
  }
}
