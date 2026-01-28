/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.dynamic;

import io.opentelemetry.sdk.extension.controlplane.proto.v1.ConfigProtos.AgentConfig;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.ConfigProtos.BatchConfig;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.ConfigProtos.SamplerConfig;
import java.util.Map;

/**
 * Protobuf AgentConfig 到 AgentConfigData 的适配器
 *
 * <p>将 Protobuf 生成的 AgentConfig 消息适配为 {@link DynamicConfigManager.AgentConfigData} 接口，
 * 以便 {@link DynamicConfigManager} 可以统一处理配置数据，无需关心数据来源是 Protobuf 还是其他格式。
 *
 * <p>这是适配器模式（Adapter Pattern）的应用，实现了接口隔离和依赖倒置原则。
 */
public final class ProtobufAgentConfigAdapter implements DynamicConfigManager.AgentConfigData {

  private final AgentConfig protoConfig;
  private final String configVersion;

  /**
   * 创建适配器实例
   *
   * @param protoConfig Protobuf AgentConfig 消息
   * @param configVersion 配置版本字符串
   */
  public ProtobufAgentConfigAdapter(AgentConfig protoConfig, String configVersion) {
    this.protoConfig = protoConfig;
    this.configVersion = configVersion != null ? configVersion : "";
  }

  @Override
  public String getConfigVersion() {
    // 优先使用传入的版本，其次使用 proto 内嵌的版本
    if (!configVersion.isEmpty()) {
      return configVersion;
    }
    if (protoConfig.hasVersion()) {
      return protoConfig.getVersion().getVersion();
    }
    return "";
  }

  @Override
  public boolean hasSamplerConfig() {
    return protoConfig.hasSampler();
  }

  @Override
  public DynamicConfigManager.SamplerConfigData getSamplerConfig() {
    return new ProtobufSamplerConfigAdapter(protoConfig.getSampler());
  }

  @Override
  public boolean hasBatchConfig() {
    return protoConfig.hasBatch();
  }

  @Override
  public DynamicConfigManager.BatchConfigData getBatchConfig() {
    return new ProtobufBatchConfigAdapter(protoConfig.getBatch());
  }

  @Override
  public boolean hasDynamicResourceAttributes() {
    return protoConfig.getDynamicResourceAttributesCount() > 0;
  }

  @Override
  public Map<String, String> getDynamicResourceAttributes() {
    return protoConfig.getDynamicResourceAttributesMap();
  }

  @Override
  public boolean hasExtensionConfig() {
    return !protoConfig.getExtensionConfigJson().isEmpty();
  }

  @Override
  public String getExtensionConfigJson() {
    return protoConfig.getExtensionConfigJson();
  }

  @Override
  public Map<String, String> getServerMetadata() {
    return protoConfig.getServerMetadataMap();
  }

  // ===== 内部适配器类 =====

  /** Protobuf SamplerConfig 适配器 */
  private static final class ProtobufSamplerConfigAdapter
      implements DynamicConfigManager.SamplerConfigData {

    private final SamplerConfig protoSampler;

    ProtobufSamplerConfigAdapter(SamplerConfig protoSampler) {
      this.protoSampler = protoSampler;
    }

    @Override
    public SamplerType getType() {
      switch (protoSampler.getType()) {
        case SAMPLER_TYPE_ALWAYS_ON:
          return SamplerType.ALWAYS_ON;
        case SAMPLER_TYPE_ALWAYS_OFF:
          return SamplerType.ALWAYS_OFF;
        case SAMPLER_TYPE_TRACE_ID_RATIO:
          return SamplerType.TRACE_ID_RATIO;
        case SAMPLER_TYPE_PARENT_BASED:
          return SamplerType.PARENT_BASED;
        case SAMPLER_TYPE_RULE_BASED:
          return SamplerType.RULE_BASED;
        default:
          return SamplerType.ALWAYS_ON; // 默认值
      }
    }

    @Override
    public double getRatio() {
      return protoSampler.getRatio();
    }
  }

  /** Protobuf BatchConfig 适配器 */
  private static final class ProtobufBatchConfigAdapter
      implements DynamicConfigManager.BatchConfigData {

    private final BatchConfig protoBatch;

    ProtobufBatchConfigAdapter(BatchConfig protoBatch) {
      this.protoBatch = protoBatch;
    }

    @Override
    public int getMaxExportBatchSize() {
      return protoBatch.getMaxExportBatchSize();
    }

    @Override
    public int getMaxQueueSize() {
      return protoBatch.getMaxQueueSize();
    }

    @Override
    public long getScheduleDelayMillis() {
      return protoBatch.getScheduleDelayMillis();
    }

    @Override
    public long getExportTimeoutMillis() {
      return protoBatch.getExportTimeoutMillis();
    }
  }
}
