/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core;

import java.io.Closeable;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 控制平面组件接口
 *
 * <p>定义控制平面子组件的统一生命周期管理接口。所有受控组件都应实现此接口，
 * 以便 {@link io.opentelemetry.sdk.extension.controlplane.ControlPlaneManager} 统一管理。
 *
 * <p>生命周期顺序：
 * <ol>
 *   <li>构造：组件被创建</li>
 *   <li>{@link #start(ScheduledExecutorService)}：组件启动，开始工作</li>
 *   <li>{@link #stop()}：组件停止，释放运行时资源</li>
 *   <li>{@link #close()}：组件关闭，释放所有资源</li>
 * </ol>
 *
 * <p>实现要求：
 * <ul>
 *   <li>所有方法必须是线程安全的</li>
 *   <li>多次调用 {@link #stop()} 和 {@link #close()} 应是幂等的</li>
 *   <li>在 {@link #close()} 后不应再使用组件</li>
 * </ul>
 */
public interface ControlPlaneComponent extends Closeable {

  /**
   * 启动组件
   *
   * <p>组件应在此方法中启动其运行时逻辑，如启动后台线程、注册监听器等。
   *
   * @param scheduler 共享的调度器，用于执行定时任务
   */
  void start(ScheduledExecutorService scheduler);

  /**
   * 停止组件
   *
   * <p>组件应在此方法中停止其运行时逻辑，如停止后台线程、注销监听器等。
   * 停止后组件可以被重新启动（取决于具体实现）。
   */
  void stop();

  /**
   * 获取组件名称
   *
   * <p>用于日志和调试
   *
   * @return 组件名称
   */
  default String getComponentName() {
    return getClass().getSimpleName();
  }

  /**
   * 检查组件是否已启动
   *
   * @return 是否已启动
   */
  default boolean isStarted() {
    return false;
  }
}
