/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.ConfigRequest;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.ConfigResponse;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.PollResult;
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
 *
 * <p>支持两种模式：
 * <ul>
 *   <li>独立模式：通过 poll() 方法直接调用 /v1/control/poll/config
 *   <li>统一模式：通过 processUnifiedResult() 处理 /v1/control/poll 响应中的 CONFIG 部分
 * </ul>
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
      logger.log(
          Level.WARNING,
          "[CONFIG-ERROR] Config response error from server: {0}",
          response.getErrorMessage());
      taskLogger.logTaskProgress(
          currentTaskId,
          "config_error",
          "Config response error: " + response.getErrorMessage());
      return HandlerResult.noChange();
    }

    statistics.recordConfigFetchSuccess();

    if (response.hasChanges()) {
      // 配置有更新
      logger.log(
          Level.INFO,
          "[CONFIG-RECEIVED] Config updated from server (independent poll), version={0}, etag={1}",
          new Object[] {response.getConfigVersion(), response.getEtag()});
      
      taskLogger.logTaskProgress(
          currentTaskId,
          "config_changed",
          "Config version: " + response.getConfigVersion() + ", etag: " + response.getEtag());

      // 更新本地状态
      this.currentConfigVersion = response.getConfigVersion();
      this.currentConfigEtag = response.getEtag();

      return HandlerResult.changed(
          "version=" + currentConfigVersion + ", etag=" + currentConfigEtag);
    } else {
      // 配置无更新，输出 INFO 级别日志便于确认轮询正常工作
      logger.log(
          Level.INFO,
          "[CONFIG-POLL] No config changes from server (independent poll)");
      taskLogger.logTaskProgress(currentTaskId, "config_unchanged", "No config changes");
      return HandlerResult.noChange();
    }
  }

  /**
   * 处理统一轮询响应中的配置结果
   *
   * <p>这是推荐的方式，用于处理 /v1/control/poll 统一端点返回的 CONFIG 部分
   *
   * @param result 轮询结果
   * @return 是否成功处理
   */
  public boolean processUnifiedResult(PollResult result) {
    if (result == null) {
      logger.log(Level.FINE, "[CONFIG-POLL] No config result in unified response");
      return false;
    }

    if (result.hasChanges()) {
      String newVersion = result.getConfigVersion();
      String newEtag = result.getConfigEtag();

      // 配置有更新
      logger.log(
          Level.INFO,
          "[CONFIG-RECEIVED] Config updated via unified poll, version={0}, etag={1}",
          new Object[] {newVersion, newEtag});

      taskLogger.logTaskProgress(
          currentTaskId,
          "config_changed",
          "Config version: " + newVersion + ", etag: " + newEtag);

      // 更新本地状态
      if (newVersion != null) {
        this.currentConfigVersion = newVersion;
      }
      if (newEtag != null) {
        this.currentConfigEtag = newEtag;
      }

      // TODO: 处理配置数据（result.getConfigData()）
      
      return true;
    } else {
      // 配置无更新，输出 INFO 级别日志便于确认轮询正常工作
      logger.log(
          Level.INFO,
          "[CONFIG-POLL] No config changes via unified poll");
      taskLogger.logTaskProgress(currentTaskId, "config_unchanged", "No config changes");
      return true;
    }
  }

  @Override
  public void handleError(Throwable error) {
    logger.log(
        Level.WARNING,
        "[CONFIG-ERROR] Config poll failed: {0}",
        error.getMessage());
    taskLogger.logTaskProgress(
        currentTaskId, "config_error", "Config poll error: " + error.getMessage());
  }

  @Override
  public boolean shouldContinue() {
    return running.get();
  }

  /**
   * 发起配置轮询请求（独立模式）
   *
   * <p>直接调用 /v1/control/poll/config 端点
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
