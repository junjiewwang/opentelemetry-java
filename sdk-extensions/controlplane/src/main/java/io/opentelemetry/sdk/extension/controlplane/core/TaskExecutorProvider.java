/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core;

import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutor;
import java.util.List;

/**
 * 任务执行器提供者接口
 *
 * <p>组件实现此接口以声明其能提供的任务执行器。这遵循开闭原则（OCP）：
 * {@link io.opentelemetry.sdk.extension.controlplane.ControlPlaneManager} 无需知道具体的执行器类型，
 * 只需遍历所有实现了此接口的组件，自动发现和注册执行器。
 *
 * <p>使用示例：
 * <pre>{@code
 * public class ArthasIntegration implements TaskExecutorProvider {
 *     @Override
 *     public List<TaskExecutor> getTaskExecutors() {
 *         return Arrays.asList(
 *             new ArthasAttachExecutor(this),
 *             new ArthasDetachExecutor(this)
 *         );
 *     }
 * }
 * }</pre>
 *
 * <p>扩展新任务类型时：
 * <ol>
 *   <li>创建新的 {@link TaskExecutor} 实现</li>
 *   <li>在对应组件的 {@link #getTaskExecutors()} 中返回该执行器</li>
 *   <li>无需修改 ControlPlaneManager 代码</li>
 * </ol>
 */
public interface TaskExecutorProvider {

  /**
   * 获取该组件提供的任务执行器列表
   *
   * <p>返回的执行器将被自动注册到 {@link io.opentelemetry.sdk.extension.controlplane.task.executor.TaskDispatcher}。
   *
   * <p>实现要求：
   * <ul>
   *   <li>每次调用应返回相同的执行器实例（或等价的新实例）</li>
   *   <li>如果组件未就绪或不可用，可返回空列表</li>
   *   <li>不应返回 null</li>
   * </ul>
   *
   * @return 任务执行器列表，不可为 null
   */
  List<TaskExecutor> getTaskExecutors();
}
