/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

/**
 * 长轮询处理器接口
 *
 * <p>定义长轮询请求和响应的处理逻辑，各处理器实现具体的业务处理。 遵循开闭原则，新增轮询类型只需实现此接口并注册到协调器即可。
 *
 * @param <R> 响应类型
 */
public interface LongPollHandler<R> {

  /**
   * 获取处理器类型
   *
   * @return 长轮询类型
   */
  LongPollType getType();

  /**
   * 构建请求参数
   *
   * <p>返回值将被合并到长轮询请求中
   *
   * @return 请求参数映射
   */
  Map<String, Object> buildRequestParams();

  /**
   * 发起轮询请求
   *
   * <p>返回异步 Future，由 LongPollSelector 管理等待和处理
   *
   * @return 响应 Future
   */
  CompletableFuture<R> poll();

  /**
   * 处理响应
   *
   * @param response 响应数据
   * @return 处理结果
   */
  HandlerResult handleResponse(R response);

  /**
   * 处理错误
   *
   * @param error 错误信息
   */
  void handleError(Throwable error);

  /**
   * 是否应该继续轮询
   *
   * <p>例如：当处于 shutting down 状态时返回 false
   *
   * @return 是否继续轮询
   */
  boolean shouldContinue();

  /**
   * 设置当前任务 ID（用于日志追踪）
   *
   * @param taskId 任务 ID
   */
  void setCurrentTaskId(String taskId);

  /** 处理结果 */
  final class HandlerResult {
    private final boolean hasChanges;
    @Nullable private final String message;

    private HandlerResult(boolean hasChanges, @Nullable String message) {
      this.hasChanges = hasChanges;
      this.message = message;
    }

    /**
     * 创建无变更结果
     *
     * @return 无变更结果
     */
    public static HandlerResult noChange() {
      return new HandlerResult(/* hasChanges= */ false, null);
    }

    /**
     * 创建有变更结果
     *
     * @return 有变更结果
     */
    public static HandlerResult changed() {
      return new HandlerResult(/* hasChanges= */ true, null);
    }

    /**
     * 创建有变更结果（带消息）
     *
     * @param message 变更消息
     * @return 有变更结果
     */
    public static HandlerResult changed(String message) {
      return new HandlerResult(/* hasChanges= */ true, message);
    }

    /**
     * 是否有变更
     *
     * @return 是否有变更
     */
    public boolean hasChanges() {
      return hasChanges;
    }

    /**
     * 获取消息
     *
     * @return 消息，可能为 null
     */
    @Nullable
    public String getMessage() {
      return message;
    }
  }
}
