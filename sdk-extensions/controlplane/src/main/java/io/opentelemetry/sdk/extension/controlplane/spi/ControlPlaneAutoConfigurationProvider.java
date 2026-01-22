/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.spi;

import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.extension.controlplane.ControlPlaneManager;
import io.opentelemetry.sdk.extension.controlplane.core.InstrumentationProvider;
import io.opentelemetry.sdk.extension.controlplane.config.ControlPlaneConfig;
import io.opentelemetry.sdk.extension.controlplane.dynamic.DynamicConfigManager;
import io.opentelemetry.sdk.extension.controlplane.dynamic.DynamicSampler;
import io.opentelemetry.sdk.extension.controlplane.health.OtlpExportMetrics;
import io.opentelemetry.sdk.extension.controlplane.identity.AgentIdentityProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 控制平面自动配置提供者
 *
 * <p>通过 SPI 机制自动集成到 OpenTelemetry SDK 自动配置中。
 *
 * <p>支持多信号源指标收集：
 * <ul>
 *   <li>SpanExporter - 收集 Span 导出指标</li>
 *   <li>MetricExporter - 收集 Metric 导出指标（更稳定，权重更高）</li>
 * </ul>
 */
public final class ControlPlaneAutoConfigurationProvider
    implements AutoConfigurationCustomizerProvider {

  private static final Logger logger =
      Logger.getLogger(ControlPlaneAutoConfigurationProvider.class.getName());

  @Nullable private static volatile ControlPlaneManager controlPlaneManager;
  @Nullable private static volatile DynamicSampler dynamicSampler;
  @Nullable private static volatile OtlpExportMetrics exportMetrics;

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

    // 添加 SpanExporter 自定义 (用于监控 OTLP Span 导出健康状态)
    autoConfiguration.addSpanExporterCustomizer(
        (exporter, config) -> {
          if (!isEnabled(config)) {
            return exporter;
          }

          // 包装 exporter 以收集导出指标
          return wrapSpanExporterWithExportMetrics(exporter, config);
        });

    // 添加 MetricExporter 自定义 (用于监控 OTLP Metric 导出健康状态)
    autoConfiguration.addMetricExporterCustomizer(
        (exporter, config) -> {
          if (!isEnabled(config)) {
            return exporter;
          }

          // 包装 exporter 以收集导出指标
          return wrapMetricExporterWithExportMetrics(exporter, config);
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

  /**
   * 确保导出指标收集器已创建
   */
  private static OtlpExportMetrics ensureExportMetrics(ConfigProperties config) {
    if (exportMetrics == null) {
      synchronized (ControlPlaneAutoConfigurationProvider.class) {
        if (exportMetrics == null) {
          ControlPlaneConfig controlConfig = ControlPlaneConfig.create(config);
          exportMetrics = OtlpExportMetrics.builder()
              .windowMillis(controlConfig.getHealthWindowMillis())
              .minSamples(controlConfig.getHealthMinSamples())
              .build();
          logger.log(
              Level.INFO,
              "Created OtlpExportMetrics with windowMillis={0}, minSamples={1}",
              new Object[] {
                controlConfig.getHealthWindowMillis(),
                controlConfig.getHealthMinSamples()
              });
        }
      }
    }
    return exportMetrics;
  }

  /**
   * 包装 SpanExporter 以收集导出指标
   */
  private static SpanExporter wrapSpanExporterWithExportMetrics(
      SpanExporter exporter, ConfigProperties config) {

    logger.log(
        Level.INFO,
        "Wrapping SpanExporter with HealthMonitoringSpanExporter: {0}",
        exporter.getClass().getName());

    OtlpExportMetrics metrics = ensureExportMetrics(config);
    return new HealthMonitoringSpanExporter(exporter, metrics);
  }

  /**
   * 包装 MetricExporter 以收集导出指标
   */
  private static MetricExporter wrapMetricExporterWithExportMetrics(
      MetricExporter exporter, ConfigProperties config) {

    logger.log(
        Level.INFO,
        "Wrapping MetricExporter with HealthMonitoringMetricExporter: {0}",
        exporter.getClass().getName());

    OtlpExportMetrics metrics = ensureExportMetrics(config);
    return new HealthMonitoringMetricExporter(exporter, metrics);
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

      // 确保导出指标收集器已创建
      OtlpExportMetrics metrics = ensureExportMetrics(config);

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
              .setExportMetrics(metrics)
              .setConfigManager(configManager)
              .setDynamicSampler(dynamicSampler);

      // 设置 Instrumentation（如果可用）
      InstrumentationProvider provider = InstrumentationProvider.getInstance();
      if (provider.isAvailable()) {
        managerBuilder.setInstrumentation(provider.getInstrumentation());
        logger.log(Level.INFO, "Instrumentation set for control plane manager");
      } else {
        logger.log(
            Level.WARNING,
            "Instrumentation not available. "
                + "Make sure InstrumentationProvider.setInstrumentation() is called in premain.");
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
                    } catch (RuntimeException e) {
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
   * 获取导出指标收集器实例
   *
   * @return 导出指标收集器，如果未初始化则返回 null
   */
  @Nullable
  public static OtlpExportMetrics getExportMetrics() {
    return exportMetrics;
  }
}
