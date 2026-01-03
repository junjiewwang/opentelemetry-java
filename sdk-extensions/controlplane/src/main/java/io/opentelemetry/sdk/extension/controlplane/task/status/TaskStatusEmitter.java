package io.opentelemetry.sdk.extension.controlplane.task.status;

import javax.annotation.Nullable;

/**
 * 任务状态事件发射器。
 *
 * <p>执行器通过该接口在关键节点立即发射事件，由统一的 {@link TaskStatusEventManager} 负责
 * 去重/节流/聚合并最终触发上报。
 */
public interface TaskStatusEmitter {

  /**
   * 上报 RUNNING（含可选 message）。
   */
  void running(String message);

  /**
   * 上报 SUCCESS。
   */
  void success(@Nullable String resultJson);

  /**
   * 上报 FAILED。
   */
  void failed(String errorCode, String errorMessage);
}
