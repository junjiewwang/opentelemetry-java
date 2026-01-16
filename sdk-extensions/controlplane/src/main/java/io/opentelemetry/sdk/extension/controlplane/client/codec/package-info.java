/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * 控制平面编解码层
 *
 * <p>提供 Protobuf 消息的序列化和反序列化功能。
 *
 * <p>主要组件：
 * <ul>
 *   <li>{@link io.opentelemetry.sdk.extension.controlplane.client.codec.ProtobufCodec} - Protobuf 编解码器
 * </ul>
 */
@ParametersAreNonnullByDefault
package io.opentelemetry.sdk.extension.controlplane.client.codec;

import javax.annotation.ParametersAreNonnullByDefault;
