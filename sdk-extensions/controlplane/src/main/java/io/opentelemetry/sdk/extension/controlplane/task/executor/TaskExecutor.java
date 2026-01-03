/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.executor;

import java.util.concurrent.CompletableFuture;

/**
 * 任务执行器接口
 *
 * <p>采用策略模式，支持不同类型的任务执行。每种任务类型对应一个执行器实现。
 *
 * <p>实现类必须：
 * <ul>
 *   <li>声明支持的任务类型（{@link #getTaskType()}）
 *   <li>实现异步执行逻辑（{@link #execute(TaskExecutionContext)}）
 *   <li>确保线程安全
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * public class ArthasAttachExecutor implements TaskExecutor {
 *     @Override
 *     public String getTaskType() {
 *         return "arthas_attach";
 *     }
 *
 *     @Override
 *     public CompletableFuture<TaskExecutionResult> execute(TaskExecutionContext context) {
 *         // 执行 Arthas attach 逻辑
 *     }
 * }
 * }</pre>
 */
public interface TaskExecutor {

  /**
   * 获取执行器支持的任务类型
   *
   * <p>任务类型应与服务端下发的 task_type 字段匹配
   *
   * @return 任务类型标识符（如 "arthas_attach"、"config_update"）
   */
  String getTaskType();

  /**
   * 异步执行任务
   *
   * <p>执行器应该：
   * <ul>
   *   <li>快速返回 CompletableFuture，不阻塞调用线程
   *   <li>在内部处理超时（使用 context 中的 timeoutMillis）
   *   <li>捕获所有异常并转换为 TaskExecutionResult.failed()
   *   <li>支持取消操作（通过 CompletableFuture.cancel()）
   * </ul>
   *
   * @param context 任务执行上下文，包含任务信息和依赖组件
   * @return 异步执行结果
   */
  CompletableFuture<TaskExecutionResult> execute(TaskExecutionContext context);

  /**
   * 检查执行器是否可用
   *
   * <p>用于快速检查执行器是否可以执行任务，例如：
   * <ul>
   *   <li>必要的依赖是否已初始化
   *   <li>资源是否可用
   *   <li>前置条件是否满足
   * </ul>
   *
   * @return 是否可用
   */
  default boolean isAvailable() {
    return true;
  }

  /**
   * 获取执行器优先级
   *
   * <p>当多个执行器可以处理同一任务类型时，优先级高的执行器先执行。
   * 默认优先级为 0，数值越大优先级越高。
   *
   * @return 优先级
   */
  default int getPriority() {
    return 0;
  }

  /**
   * 获取执行器描述
   *
   * <p>用于日志和调试
   *
   * @return 执行器描述
   */
  default String getDescription() {
    return getTaskType() + " executor";
  }
}
