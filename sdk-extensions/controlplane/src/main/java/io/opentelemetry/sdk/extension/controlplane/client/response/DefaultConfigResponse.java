/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client.response;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.ConfigResponse;

/**
 * 默认配置响应实现
 *
 * <p>供 HTTP 和 gRPC 客户端共享使用
 */
public final class DefaultConfigResponse implements ConfigResponse {
  private final boolean success;
  private final boolean hasChanges;
  private final String configVersion;
  private final String etag;
  private final byte[] configData;
  private final String errorMessage;
  private final long suggestedPollIntervalMillis;

  /**
   * 创建配置响应
   */
  public DefaultConfigResponse(
      boolean success,
      boolean hasChanges,
      String configVersion,
      String etag,
      byte[] configData,
      String errorMessage,
      long suggestedPollIntervalMillis) {
    this.success = success;
    this.hasChanges = hasChanges;
    this.configVersion = configVersion != null ? configVersion : "";
    this.etag = etag != null ? etag : "";
    this.configData = configData != null ? configData : new byte[0];
    this.errorMessage = errorMessage != null ? errorMessage : "";
    this.suggestedPollIntervalMillis = suggestedPollIntervalMillis;
  }

  /**
   * 创建错误响应
   *
   * @param errorMessage 错误信息
   * @return 错误响应实例
   */
  public static ConfigResponse error(String errorMessage) {
    return new DefaultConfigResponse(
        /* success= */ false, /* hasChanges= */ false, "", "", new byte[0], errorMessage, 30000);
  }

  @Override
  public boolean isSuccess() {
    return success;
  }

  @Override
  public boolean hasChanges() {
    return hasChanges;
  }

  @Override
  public String getConfigVersion() {
    return configVersion;
  }

  @Override
  public String getEtag() {
    return etag;
  }

  @Override
  public byte[] getConfigData() {
    return configData;
  }

  @Override
  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public long getSuggestedPollIntervalMillis() {
    return suggestedPollIntervalMillis;
  }
}
