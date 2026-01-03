/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.spi;

import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.extension.controlplane.ControlPlaneManager;
import io.opentelemetry.sdk.extension.controlplane.InstrumentationHolder;
import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import io.opentelemetry.sdk.extension.controlplane.dynamic.DynamicConfigManager;
import io.opentelemetry.sdk.extension.controlplane.dynamic.DynamicSampler;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpHealthMonitor;
import io.opentelemetry.sdk.extension.controlplane.identity.AgentIdentityProvider;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 控制平面自动配置提供者
 *
 * <p>通过 SPI 机制自动集成到 OpenTelemetry SDK 自动配置中。
 */
public final class ControlPlaneAutoConfigurationProvider
    implements AutoConfigurationCustomizerProvider {

  private static final Logger logger =
      Logger.getLogger(ControlPlaneAutoConfigurationProvider.class.getName());

  @Nullable private static volatile ControlPlaneManager controlPlaneManager;
  @Nullable private static volatile DynamicSampler dynamicSampler;
  @Nullable private static volatile OtlpHealthMonitor healthMonitor;

  @Override
  public void customize(AutoConfigurationCustomizer autoConfiguration) {
    logger.log(Level.INFO, "ControlPlaneAutoConfigurationProvider.customize() called");

    // 添加采样器自定义
    autoConfiguration.addSamplerCustomizer(
        (sampler, config) -> {
          if (!isEnabled(config)) {
            return sampler;
          }

          // 创建动态采样器包装原始采样器
          dynamicSampler = new DynamicSampler(sampler);
          logger.log(Level.INFO, "Wrapped sampler with DynamicSampler for control plane");
          return dynamicSampler;
        });

    // 添加 SpanExporter 自定义 (用于监控 OTLP 健康状态)
    autoConfiguration.addSpanExporterCustomizer(
        (exporter, config) -> {
          if (!isEnabled(config)) {
            return exporter;
          }

          // 包装 exporter 以监控导出状态
          return wrapExporterWithHealthMonitor(exporter, config);
        });

    // 添加 TracerProvider 自定义
    autoConfiguration.addTracerProviderCustomizer(
        (builder, config) -> {
          if (!isEnabled(config)) {
            return builder;
          }

          // 初始化并启动控制平面管理器
          initializeControlPlane(config);
          return builder;
        });

    // 添加资源自定义
    autoConfiguration.addResourceCustomizer(
        (resource, config) -> {
          if (!isEnabled(config)) {
            return resource;
          }

          // 初始化 Agent 身份
          String serviceName = config.getString("otel.service.name");
          String serviceNamespace = config.getString("otel.service.namespace");
          AgentIdentityProvider.initialize(serviceName, serviceNamespace);

          logger.log(
              Level.INFO,
              "Control plane initialized with agentId: {0}",
              AgentIdentityProvider.get().getAgentId());

          return resource;
        });
  }

  @Override
  public int order() {
    // 确保在其他自定义之后执行
    return Integer.MAX_VALUE - 100;
  }

  private static boolean isEnabled(ConfigProperties config) {
    return config.getBoolean("otel.agent.control.enabled", true);
  }

  private static SpanExporter wrapExporterWithHealthMonitor(
      SpanExporter exporter, ConfigProperties config) {

    logger.log(
        Level.INFO,
        "Wrapping SpanExporter with HealthMonitoringSpanExporter: {0}",
        exporter.getClass().getName());

    // 创建健康监控器 (如果尚未创建)
    if (healthMonitor == null) {
      synchronized (ControlPlaneAutoConfigurationProvider.class) {
        if (healthMonitor == null) {
          ControlPlaneConfig controlConfig = ControlPlaneConfig.create(config);
          healthMonitor =
              new OtlpHealthMonitor(
                  controlConfig.getHealthWindowSize(),
                  controlConfig.getHealthyThreshold(),
                  controlConfig.getUnhealthyThreshold());
          logger.log(
              Level.INFO,
              "Created OtlpHealthMonitor with windowSize={0}, healthyThreshold={1}, unhealthyThreshold={2}",
              new Object[] {
                controlConfig.getHealthWindowSize(),
                controlConfig.getHealthyThreshold(),
                controlConfig.getUnhealthyThreshold()
              });
        }
      }
    }

    return new HealthMonitoringSpanExporter(exporter, healthMonitor);
  }

  private static void initializeControlPlane(ConfigProperties config) {
    if (controlPlaneManager != null) {
      return;
    }

    synchronized (ControlPlaneAutoConfigurationProvider.class) {
      if (controlPlaneManager != null) {
        return;
      }

      ControlPlaneConfig controlConfig = ControlPlaneConfig.create(config);

      // 确保健康监控器已创建
      if (healthMonitor == null) {
        healthMonitor =
            new OtlpHealthMonitor(
                controlConfig.getHealthWindowSize(),
                controlConfig.getHealthyThreshold(),
                controlConfig.getUnhealthyThreshold());
      }

      // 确保动态采样器已创建
      if (dynamicSampler == null) {
        dynamicSampler = DynamicSampler.create();
      }

      // 创建配置管理器
      DynamicConfigManager configManager = new DynamicConfigManager();

      // 创建并启动控制平面管理器
      ControlPlaneManager.Builder managerBuilder =
          ControlPlaneManager.builder()
              .setConfig(controlConfig)
              .setHealthMonitor(healthMonitor)
              .setConfigManager(configManager)
              .setDynamicSampler(dynamicSampler);

      // 设置 Instrumentation（如果可用）
      if (InstrumentationHolder.isAvailable()) {
        managerBuilder.setInstrumentation(InstrumentationHolder.get());
        logger.log(Level.INFO, "Instrumentation set for control plane manager");
      } else {
        logger.log(Level.WARNING, 
            "Instrumentation not available, Arthas may not work properly. " +
            "Make sure InstrumentationHolder.set() is called in premain.");
      }

      // 根据配置启用 Arthas
      if (controlConfig.isArthasEnabled()) {
        managerBuilder.enableArthas();
        logger.log(Level.INFO, "Arthas integration enabled via configuration");
      }

      controlPlaneManager = managerBuilder.build();

      controlPlaneManager.start();

      // 注册关闭钩子
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      if (controlPlaneManager != null) {
                        controlPlaneManager.close();
                      }
                    } catch (Exception e) {
                      logger.log(Level.WARNING, "Failed to close control plane manager", e);
                    }
                  },
                  "otel-controlplane-shutdown"));

      logger.log(Level.INFO, "Control plane manager started");
    }
  }

  /**
   * 获取控制平面管理器实例
   *
   * @return 控制平面管理器，如果未初始化则返回 null
   */
  @Nullable
  public static ControlPlaneManager getControlPlaneManager() {
    return controlPlaneManager;
  }

  /**
   * 获取动态采样器实例
   *
   * @return 动态采样器，如果未初始化则返回 null
   */
  @Nullable
  public static DynamicSampler getDynamicSampler() {
    return dynamicSampler;
  }

  /**
   * 获取健康监控器实例
   *
   * @return 健康监控器，如果未初始化则返回 null
   */
  @Nullable
  public static OtlpHealthMonitor getHealthMonitor() {
    return healthMonitor;
  }
}
