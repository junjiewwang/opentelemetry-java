/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.response;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.PollResult;
import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskInfo;
import java.util.List;
import javax.annotation.Nullable;

/**
 * 默认轮询结果实现
 *
 * <p>供 HTTP 和 gRPC 客户端共享使用
 */
public final class DefaultPollResult implements PollResult {
  private final String type;
  private final boolean hasChanges;
  @Nullable private final byte[] configData;
  @Nullable private final String configVersion;
  @Nullable private final String configEtag;
  @Nullable private final List<TaskInfo> tasks;

  /**
   * 创建轮询结果
   *
   * @param type 类型（CONFIG 或 TASK）
   * @param hasChanges 是否有变更
   * @param configData 配置数据（仅 CONFIG 类型）
   * @param configVersion 配置版本（仅 CONFIG 类型）
   * @param configEtag 配置 ETag（仅 CONFIG 类型）
   * @param tasks 任务列表（仅 TASK 类型）
   */
  public DefaultPollResult(
      String type,
      boolean hasChanges,
      @Nullable byte[] configData,
      @Nullable String configVersion,
      @Nullable String configEtag,
      @Nullable List<TaskInfo> tasks) {
    this.type = type != null ? type : "UNKNOWN";
    this.hasChanges = hasChanges;
    this.configData = configData;
    this.configVersion = configVersion;
    this.configEtag = configEtag;
    this.tasks = tasks;
  }

  /**
   * 创建 CONFIG 类型的结果
   */
  public static DefaultPollResult config(
      boolean hasChanges,
      @Nullable byte[] configData,
      @Nullable String configVersion,
      @Nullable String configEtag) {
    return new DefaultPollResult("CONFIG", hasChanges, configData, configVersion, configEtag, null);
  }

  /**
   * 创建 TASK 类型的结果
   */
  public static DefaultPollResult task(boolean hasChanges, @Nullable List<TaskInfo> tasks) {
    return new DefaultPollResult("TASK", hasChanges, null, null, null, tasks);
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public boolean hasChanges() {
    return hasChanges;
  }

  @Override
  @Nullable
  public byte[] getConfigData() {
    return configData;
  }

  @Override
  @Nullable
  public String getConfigVersion() {
    return configVersion;
  }

  @Override
  @Nullable
  public String getConfigEtag() {
    return configEtag;
  }

  @Override
  @Nullable
  public List<TaskInfo> getTasks() {
    return tasks;
  }
}
