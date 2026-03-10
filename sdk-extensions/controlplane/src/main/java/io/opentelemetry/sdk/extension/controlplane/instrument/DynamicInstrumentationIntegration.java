/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import io.opentelemetry.sdk.extension.controlplane.core.ControlPlaneComponent;
import io.opentelemetry.sdk.extension.controlplane.core.InstrumentationProvider;
import io.opentelemetry.sdk.extension.controlplane.core.TaskExecutorProvider;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutor;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 动态增强集成入口
 *
 * <p>作为 {@link ControlPlaneComponent} 和 {@link TaskExecutorProvider} 的实现，
 * 将动态增强模块集成到控制平面管理器（{@code ControlPlaneManager}）。
 *
 * <p>遵循与 {@code AsyncProfilerIntegration} 相同的设计模式：
 * <ul>
 *   <li>{@link ControlPlaneComponent}：统一生命周期管理（start / stop / close）</li>
 *   <li>{@link TaskExecutorProvider}：自动注册 {@code dynamic_instrument} 和
 *       {@code dynamic_uninstrument} 任务执行器到 TaskDispatcher</li>
 * </ul>
 *
 * <p>使用方式：
 * <pre>{@code
 * ControlPlaneManager.builder()
 *     .setConfig(config)
 *     .addComponent(DynamicInstrumentationIntegration.create())
 *     .build();
 * }</pre>
 *
 * @see TransformerManager
 * @see DynamicInstrumentExecutor
 * @see DynamicUninstrumentExecutor
 */
public final class DynamicInstrumentationIntegration
    implements ControlPlaneComponent, TaskExecutorProvider {

  private static final Logger logger =
      DynamicInstrumentLogger.getLogger("integration");

  private final TransformerManager transformerManager;
  private final EnhancementStateRegistry stateRegistry;
  private final DynamicInstrumentExecutor instrumentExecutor;
  private final DynamicUninstrumentExecutor uninstrumentExecutor;
  private final AtomicBoolean started = new AtomicBoolean(false);

  private DynamicInstrumentationIntegration(
      TransformerManager transformerManager,
      EnhancementStateRegistry stateRegistry,
      DynamicInstrumentExecutor instrumentExecutor,
      DynamicUninstrumentExecutor uninstrumentExecutor) {
    this.transformerManager = transformerManager;
    this.stateRegistry = stateRegistry;
    this.instrumentExecutor = instrumentExecutor;
    this.uninstrumentExecutor = uninstrumentExecutor;
  }

  /**
   * 创建动态增强集成
   *
   * <p>使用默认的 {@link InstrumentationProvider} 单例获取 {@link java.lang.instrument.Instrumentation}。
   * 如果 Instrumentation 不可用或不支持 retransform，增强任务会在执行时返回错误（优雅降级）。
   *
   * @return 集成实例
   */
  public static DynamicInstrumentationIntegration create() {
    // 先初始化独立日志通道，确保后续所有组件的日志输出到独立文件
    DynamicInstrumentLogger.initialize();

    InstrumentationProvider provider = InstrumentationProvider.getInstance();
    EnhancementStateRegistry registry = new EnhancementStateRegistry();
    TransformerManager manager = new TransformerManager(provider, registry);
    DynamicInstrumentExecutor instrumentExecutor = new DynamicInstrumentExecutor(manager);
    DynamicUninstrumentExecutor uninstrumentExecutor = new DynamicUninstrumentExecutor(manager);

    logger.log(Level.INFO,
        "[DYNAMIC-INSTRUMENT] Integration created: instrumentationAvailable={0}, "
            + "enhancementCapability={1}",
        new Object[] {provider.isAvailable(), provider.hasEnhancementCapability()});

    return new DynamicInstrumentationIntegration(
        manager, registry, instrumentExecutor, uninstrumentExecutor);
  }

  // ===== ControlPlaneComponent 接口 =====

  @Override
  public void start(ScheduledExecutorService scheduler) {
    if (started.compareAndSet(false, true)) {
      logger.log(Level.INFO, "[DYNAMIC-INSTRUMENT] Integration started");
    }
  }

  @Override
  public void stop() {
    if (started.compareAndSet(true, false)) {
      // 停止时还原所有增强
      int activeCount = transformerManager.getActiveCount();
      if (activeCount > 0) {
        logger.log(Level.INFO,
            "[DYNAMIC-INSTRUMENT] Reverting {0} active enhancement(s) on stop", activeCount);
        transformerManager.revertAll();
      }
      logger.log(Level.INFO, "[DYNAMIC-INSTRUMENT] Integration stopped");
    }
  }

  @Override
  public void close() {
    stop();
    stateRegistry.clear();
    DynamicInstrumentLogger.shutdown();
    logger.log(Level.INFO, "[DYNAMIC-INSTRUMENT] Integration closed");
  }

  @Override
  public String getComponentName() {
    return "DynamicInstrumentationIntegration";
  }

  @Override
  public boolean isStarted() {
    return started.get();
  }

  // ===== TaskExecutorProvider 接口 =====

  @Override
  public List<TaskExecutor> getTaskExecutors() {
    return Arrays.asList(instrumentExecutor, uninstrumentExecutor);
  }

  // ===== 额外的管理 API =====

  /**
   * 获取 TransformerManager
   *
   * <p>用于高级场景，如编程式增强（不通过任务下发）。
   *
   * @return TransformerManager
   */
  public TransformerManager getTransformerManager() {
    return transformerManager;
  }

  /**
   * 获取增强状态注册表
   *
   * @return 增强状态注册表
   */
  public EnhancementStateRegistry getStateRegistry() {
    return stateRegistry;
  }
}
