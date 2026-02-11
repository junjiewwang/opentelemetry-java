/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.profiler;

import java.nio.file.Path;

/**
 * AsyncProfiler 运行器接口
 *
 * <p>隔离 async-profiler 的调用细节，支持策略模式切换不同的实现：
 * <ul>
 *   <li>{@link DirectAsyncProfilerRunner} — 直接调用 AsyncProfiler Java API（推荐）
 *   <li>未来可扩展 ProcessAsyncProfilerRunner — 通过外部进程调用
 * </ul>
 *
 * <p>实现类必须保证线程安全。
 */
public interface AsyncProfilerRunner {

  /**
   * 执行一次性 profiling，结果写入指定路径
   *
   * <p>该方法是阻塞的，直到采样完成才返回。调用方应在异步线程中调用。
   *
   * @param libPath native library 路径
   * @param request profiling 请求参数
   * @param outputPath 输出文件路径（由调用方指定）
   * @return profiling 结果信息
   * @throws ProfilerException 如果采样执行失败
   */
  ProfilerResult profile(Path libPath, ProfileRequest request, Path outputPath)
      throws ProfilerException;

  /**
   * 检查 profiler 是否可用
   *
   * <p>用于快速检查：
   * <ul>
   *   <li>平台是否支持（Linux/macOS）
   *   <li>反射 API 是否可加载
   *   <li>当前是否有其他采样在进行
   * </ul>
   *
   * @return 是否可用
   */
  boolean isAvailable();
}
