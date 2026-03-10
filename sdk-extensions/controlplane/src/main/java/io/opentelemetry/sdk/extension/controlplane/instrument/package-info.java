/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * 动态类增强与还原模块
 *
 * <p>提供类似 Datadog Dynamic Instrumentation 的能力，支持在运行时动态插入和移除字节码增强，
 * 包括链路采集（TRACE）、指标采集（METRIC）和日志采集（LOG）。
 *
 * <p>核心组件：
 * <ul>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.instrument.DynamicInstrumentationIntegration}
 *       — 集成入口，实现 ControlPlaneComponent + TaskExecutorProvider</li>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.instrument.TransformerManager}
 *       — Transformer 生命周期管理器，负责增强的应用和还原</li>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.instrument.InstrumentationRule}
 *       — 增强规则模型，定义"对哪个类的哪个方法、插入什么逻辑"</li>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.instrument.EnhancementStateRegistry}
 *       — 增强状态注册表，追踪所有增强的当前状态</li>
 * </ul>
 *
 * <p>技术栈：
 * <ul>
 *   <li>{@code java.lang.instrument.Instrumentation} — JVM retransformClasses 机制</li>
 *   <li>{@code net.bytebuddy} — ByteBuddy AgentBuilder + Advice 字节码织入</li>
 *   <li>{@code io.opentelemetry.api} — OTel Tracer / Meter / Logger 桥接</li>
 * </ul>
 */
package io.opentelemetry.sdk.extension.controlplane.instrument;
