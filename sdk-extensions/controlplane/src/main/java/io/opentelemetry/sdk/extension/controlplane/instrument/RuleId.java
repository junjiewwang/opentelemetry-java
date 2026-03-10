/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * ByteBuddy 自定义绑定注解：规则 ID
 *
 * <p>用于在 Advice 方法参数中注入 {@code ruleId} 值。
 * 通过 {@code Advice.withCustomMapping().bind(RuleId.class, ruleId)} 绑定。
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
@interface RuleId {
}
