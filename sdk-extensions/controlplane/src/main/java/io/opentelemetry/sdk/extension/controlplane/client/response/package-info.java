/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * 控制平面客户端响应 DTO 实现类
 *
 * <p>此包包含 {@link io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient}
 * 接口中定义的各种响应接口的默认实现。
 *
 * <p>这些实现类被 HTTP 和 gRPC 客户端共享使用，避免代码重复。
 *
 * @see io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient
 */
@ParametersAreNonnullByDefault
package io.opentelemetry.sdk.extension.controlplane.client.response;

import javax.annotation.ParametersAreNonnullByDefault;
