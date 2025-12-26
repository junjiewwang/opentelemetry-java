/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.dynamic;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 动态采样器
 *
 * <p>支持运行时动态更新采样策略，无需重启应用。
 */
public final class DynamicSampler implements Sampler, DynamicConfigManager.HotUpdatableComponent {

  private static final Logger logger = Logger.getLogger(DynamicSampler.class.getName());
  private static final Sampler DEFAULT_SAMPLER = Sampler.alwaysOn();

  private final AtomicReference<Sampler> delegate;
  private final String description;

  /**
   * 创建动态采样器
   *
   * @param initialSampler 初始采样器
   */
  public DynamicSampler(Sampler initialSampler) {
    this.delegate = new AtomicReference<>(initialSampler);
    this.description = "DynamicSampler{" + initialSampler.getDescription() + "}";
  }

  /**
   * 创建默认的动态采样器 (始终采样)
   *
   * @return 动态采样器
   */
  public static DynamicSampler create() {
    return new DynamicSampler(Sampler.alwaysOn());
  }

  /**
   * 创建指定采样率的动态采样器
   *
   * @param ratio 采样率 (0.0 ~ 1.0)
   * @return 动态采样器
   */
  public static DynamicSampler create(double ratio) {
    return new DynamicSampler(Sampler.traceIdRatioBased(ratio));
  }

  /**
   * 更新采样器
   *
   * @param newSampler 新的采样器
   */
  public void update(Sampler newSampler) {
    Sampler oldSampler = delegate.getAndSet(newSampler);
    logger.log(
        Level.INFO,
        "Sampler updated: {0} -> {1}",
        new Object[] {oldSampler.getDescription(), newSampler.getDescription()});
  }

  /**
   * 实现 HotUpdatableComponent 接口的 update 方法
   *
   * @param config 配置对象，支持 Sampler 或 SamplerConfigData
   */
  @Override
  public void update(Object config) {
    if (config instanceof Sampler) {
      update((Sampler) config);
    } else if (config instanceof DynamicConfigManager.SamplerConfigData) {
      DynamicConfigManager.SamplerConfigData samplerConfig =
          (DynamicConfigManager.SamplerConfigData) config;
      switch (samplerConfig.getType()) {
        case ALWAYS_ON:
          updateRatio(1.0);
          break;
        case ALWAYS_OFF:
          updateRatio(0.0);
          break;
        case TRACE_ID_RATIO:
          updateRatio(samplerConfig.getRatio());
          break;
        case PARENT_BASED:
          updateParentBased(samplerConfig.getRatio());
          break;
        default:
          logger.log(Level.WARNING, "Unknown sampler type: {0}", samplerConfig.getType());
      }
    } else {
      logger.log(
          Level.WARNING,
          "Unsupported config type for DynamicSampler: {0}",
          config != null ? config.getClass().getName() : "null");
    }
  }

  /**
   * 更新为指定采样率
   *
   * @param ratio 采样率 (0.0 ~ 1.0)
   */
  public void updateRatio(double ratio) {
    if (ratio < 0.0 || ratio > 1.0) {
      logger.log(Level.WARNING, "Invalid sampling ratio: {0}, must be between 0.0 and 1.0", ratio);
      return;
    }

    Sampler newSampler;
    if (ratio <= 0.0) {
      newSampler = Sampler.alwaysOff();
    } else if (ratio >= 1.0) {
      newSampler = Sampler.alwaysOn();
    } else {
      newSampler = Sampler.traceIdRatioBased(ratio);
    }

    update(newSampler);
  }

  /**
   * 更新为 ParentBased 采样器
   *
   * @param ratio 根采样率
   */
  public void updateParentBased(double ratio) {
    Sampler rootSampler;
    if (ratio <= 0.0) {
      rootSampler = Sampler.alwaysOff();
    } else if (ratio >= 1.0) {
      rootSampler = Sampler.alwaysOn();
    } else {
      rootSampler = Sampler.traceIdRatioBased(ratio);
    }

    update(Sampler.parentBased(rootSampler));
  }

  /**
   * 获取当前采样器
   *
   * @return 当前采样器
   */
  public Sampler getCurrentSampler() {
    Sampler sampler = delegate.get();
    return sampler != null ? sampler : DEFAULT_SAMPLER;
  }

  @Override
  public SamplingResult shouldSample(
      Context parentContext,
      String traceId,
      String name,
      SpanKind spanKind,
      Attributes attributes,
      List<LinkData> parentLinks) {
    return getCurrentSampler()
        .shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
  }

  @Override
  public String getDescription() {
    return description + "[current=" + getCurrentSampler().getDescription() + "]";
  }
}
