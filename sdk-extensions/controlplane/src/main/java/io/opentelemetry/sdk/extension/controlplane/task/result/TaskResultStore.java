/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task.result;

import java.util.List;
import javax.annotation.Nullable;

/**
 * 任务结果存储接口
 *
 * <p>负责结果文件的持久化与待处理结果的管理：
 * <ul>
 *   <li>结果文件落盘
 *   <li>结果文件删除
 *   <li>失败结果标记（用于后续处理或人工干预）
 *   <li>待上传列表维护（支持故障恢复、补偿上传）
 * </ul>
 *
 * <p>实现类可以选择不同的存储方式：
 * <ul>
 *   <li>文件系统存储（默认）
 *   <li>内存存储（用于测试）
 * </ul>
 */
public interface TaskResultStore {

  /**
   * 保存结果数据
   *
   * <p>将结果数据持久化到存储，并记录描述符信息
   *
   * @param descriptor 结果描述符
   * @param data 结果数据
   * @return 更新后的描述符（包含实际存储路径等信息）
   */
  TaskResultDescriptor save(TaskResultDescriptor descriptor, byte[] data);

  /**
   * 删除结果数据
   *
   * <p>在上传成功后调用，清理本地存储
   *
   * @param descriptor 结果描述符
   * @return 是否删除成功
   */
  boolean delete(TaskResultDescriptor descriptor);

  /**
   * 标记结果为失败
   *
   * <p>在重试耗尽后调用，记录失败原因供后续处理
   *
   * @param descriptor 结果描述符
   * @param reason 失败原因
   * @return 更新后的描述符
   */
  TaskResultDescriptor markFailed(TaskResultDescriptor descriptor, String reason);

  /**
   * 标记结果为已放弃
   *
   * <p>在超过最大重试次数后调用
   *
   * @param descriptor 结果描述符
   * @param reason 放弃原因
   * @return 更新后的描述符
   */
  TaskResultDescriptor markAbandoned(TaskResultDescriptor descriptor, String reason);

  /**
   * 更新描述符状态
   *
   * @param descriptor 新的描述符状态
   * @return 是否更新成功
   */
  boolean updateDescriptor(TaskResultDescriptor descriptor);

  /**
   * 获取待上传的结果列表
   *
   * <p>返回所有状态为 PENDING 或 FAILED（可重试）的结果
   *
   * @return 待上传的结果描述符列表
   */
  List<TaskResultDescriptor> listPending();

  /**
   * 获取指定任务的结果描述符
   *
   * @param taskId 任务 ID
   * @return 结果描述符，如果不存在则返回 null
   */
  @Nullable
  TaskResultDescriptor get(String taskId);

  /**
   * 读取结果数据
   *
   * @param descriptor 结果描述符
   * @return 结果数据，如果不存在则返回 null
   */
  @Nullable
  byte[] readData(TaskResultDescriptor descriptor);

  /**
   * 获取存储中的结果数量
   *
   * @return 结果数量
   */
  int size();

  /**
   * 清理过期的失败结果
   *
   * <p>删除超过指定时间的已放弃结果
   *
   * @param maxAgeMillis 最大保留时间（毫秒）
   * @return 清理的结果数量
   */
  int cleanupExpired(long maxAgeMillis);
}
