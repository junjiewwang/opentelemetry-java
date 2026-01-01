/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * 长轮询模块
 *
 * <p>提供统一的长轮询机制，用于配置和任务的实时获取：
 *
 * <ul>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.core.longpoll.LongPollCoordinator} -
 *       长轮询协调器，管理轮询生命周期
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.core.longpoll.LongPollHandler} -
 *       处理器接口，定义请求构建和响应处理
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.core.longpoll.LongPollConfig} - 长轮询配置
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.core.longpoll.LongPollType} - 轮询类型枚举
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.core.longpoll.ExponentialBackoff} -
 *       指数退避计算器
 * </ul>
 *
 * <p>长轮询相比短轮询的优势：
 *
 * <ul>
 *   <li>减少 95%+ 的无效请求
 *   <li>配置/任务变更时立即响应（延迟 小于 100ms）
 *   <li>降低服务端负载
 * </ul>
 */
@ParametersAreNonnullByDefault
package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

import javax.annotation.ParametersAreNonnullByDefault;
