/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * AsyncProfiler 相关功能模块
 *
 * <p>提供 async-profiler native library 的资源提取、生命周期管理等功能。
 *
 * <p>设计原则：
 * <ul>
 *   <li><b>独立模块</b>：与 Arthas 模块解耦，但可被 Arthas profiler 命令复用</li>
 *   <li><b>资源独立</b>：native library 资源位于 /async-profiler/ 目录</li>
 *   <li><b>按需加载</b>：只在需要时提取对应平台的库文件</li>
 * </ul>
 *
 * <p>核心组件：
 * <ul>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.profiler.AsyncProfilerResourceExtractor}：
 *       负责平台检测和 native library 提取</li>
 * </ul>
 */
@ParametersAreNonnullByDefault
package io.opentelemetry.sdk.extension.controlplane.profiler;

import javax.annotation.ParametersAreNonnullByDefault;
