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
import io.opentelemetry.sdk.extension.controlplane.instrument.DynamicInstrumentationIntegration;
import io.opentelemetry.sdk.extension.controlplane.dynamic.DynamicConfigManager;
import io.opentelemetry.sdk.extension.controlplane.dynamic.DynamicSampler;
import io.opentelemetry.sdk.extension.controlplane.identity.AgentIdentityProvider;
import io.opentelemetry.sdk.extension.controlplane.peerservice.CallerServiceBaggageSpanProcessor;
import io.opentelemetry.sdk.extension.controlplane.peerservice.PeerServiceResolverConfig;
import io.opentelemetry.sdk.extension.controlplane.peerservice.PeerServiceSpanProcessor;
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

    // 添加 TracerProvider 自定义
    autoConfiguration.addTracerProviderCustomizer(
        (builder, config) -> {
          if (!isEnabled(config)) {
            return builder;
          }

          // 注册 peer.service 自动填充处理器
          PeerServiceResolverConfig peerServiceConfig = PeerServiceResolverConfig.create(config);
          if (peerServiceConfig.isEnabled()) {
            String serviceName = AgentIdentityProvider.getServiceName();
            builder.addSpanProcessor(
                new CallerServiceBaggageSpanProcessor(
                    serviceName, peerServiceConfig.getBaggageKey()));
            builder.addSpanProcessor(new PeerServiceSpanProcessor(peerServiceConfig));
            logger.log(Level.INFO, "Registered peer.service processors");
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

  private static void initializeControlPlane(ConfigProperties config) {
    if (controlPlaneManager != null) {
      return;
    }

    synchronized (ControlPlaneAutoConfigurationProvider.class) {
      if (controlPlaneManager != null) {
        return;
      }

      ControlPlaneConfig controlConfig = ControlPlaneConfig.create(config);

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

      // 注册动态增强集成（提供 dynamic_instrument / dynamic_uninstrument 任务执行器）
      managerBuilder.addComponent(DynamicInstrumentationIntegration.create());
      logger.log(Level.INFO, "DynamicInstrumentationIntegration registered");

      try {
        controlPlaneManager = managerBuilder.build();
        controlPlaneManager.start();
      } catch (RuntimeException e) {
        // AutoConfiguredOpenTelemetrySdkBuilder 会吞掉堆栈（只打印一行 INFO），这里补充根因日志
        logger.log(Level.SEVERE, "Failed to initialize/start control plane manager", e);
        throw e;
      }

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
}
