/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.profiler;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneService;
import io.opentelemetry.sdk.extension.controlplane.core.ControlPlaneComponent;
import io.opentelemetry.sdk.extension.controlplane.core.TaskExecutorProvider;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutor;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * AsyncProfiler 集成入口
 *
 * <p>作为 {@link ControlPlaneComponent} 和 {@link TaskExecutorProvider} 的实现，
 * 遵循现有的组件注册和生命周期管理模式（参照 {@code ArthasIntegration}）。
 *
 * <p>职责：
 * <ul>
 *   <li>创建和管理 {@link AsyncProfilerProfileExecutor}</li>
 *   <li>通过 {@link TaskExecutorProvider#getTaskExecutors()} 自动注册到 TaskDispatcher</li>
 *   <li>可选依赖、优雅降级：如果平台不支持或 native lib 不存在，不影响其他功能</li>
 * </ul>
 *
 * <p>使用方式：
 * <pre>{@code
 * ControlPlaneManager.builder()
 *     .setConfig(config)
 *     .setAsyncProfilerIntegration(
 *         AsyncProfilerIntegration.create(service, config.getStorageDir()))
 *     .build();
 * }</pre>
 */
public final class AsyncProfilerIntegration implements ControlPlaneComponent, TaskExecutorProvider {

  private static final Logger logger =
      Logger.getLogger(AsyncProfilerIntegration.class.getName());

  private final AsyncProfilerProfileExecutor executor;
  private final AtomicBoolean started = new AtomicBoolean(false);

  private AsyncProfilerIntegration(AsyncProfilerProfileExecutor executor) {
    this.executor = executor;
  }

  /**
   * 创建 AsyncProfiler 集成
   *
   * <p>如果平台不支持（Windows、不支持的架构），仍然可以创建实例，
   * 但 executor 的 {@code isAvailable()} 会返回 false，任务会被拒绝。
   *
   * @param service 控制平面服务（用于文件上传）
   * @param storageDir 存储目录路径
   * @return 集成实例
   */
  public static AsyncProfilerIntegration create(ControlPlaneService service, String storageDir) {
    AsyncProfilerRunner runner = new DirectAsyncProfilerRunner();
    FileStreamUploader uploader = new FileStreamUploader(service);
    AsyncProfilerProfileExecutor executor =
        new AsyncProfilerProfileExecutor(runner, uploader, storageDir);

    logger.log(
        Level.INFO,
        "[ASYNC-PROFILER] Integration created: platform={0}, available={1}",
        new Object[] {AsyncProfilerResourceExtractor.detectPlatform(), executor.isAvailable()});

    return new AsyncProfilerIntegration(executor);
  }

  // ===== ControlPlaneComponent 接口 =====

  @Override
  public void start(ScheduledExecutorService scheduler) {
    if (started.compareAndSet(false, true)) {
      logger.log(Level.INFO, "[ASYNC-PROFILER] Integration started");
    }
  }

  @Override
  public void stop() {
    if (started.compareAndSet(true, false)) {
      logger.log(Level.INFO, "[ASYNC-PROFILER] Integration stopped");
    }
  }

  @Override
  public void close() {
    stop();
    logger.log(Level.INFO, "[ASYNC-PROFILER] Integration closed");
  }

  @Override
  public String getComponentName() {
    return "AsyncProfilerIntegration";
  }

  @Override
  public boolean isStarted() {
    return started.get();
  }

  // ===== TaskExecutorProvider 接口 =====

  @Override
  public List<TaskExecutor> getTaskExecutors() {
    return Collections.singletonList(executor);
  }
}
