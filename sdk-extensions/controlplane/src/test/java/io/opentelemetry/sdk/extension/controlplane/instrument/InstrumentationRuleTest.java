/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class InstrumentationRuleTest {

  private static InstrumentationRule baseRule() {
    return InstrumentationRule.builder()
        .ruleId("rule-001")
        .className("com.example.MyService")
        .methodName("handleRequest")
        .type(InstrumentationType.TRACE)
        .build();
  }

  private static InstrumentationRule withParameterTypes(String... types) {
    return InstrumentationRule.builder()
        .ruleId("rule-001")
        .className("com.example.MyService")
        .methodName("handleRequest")
        .parameterTypes(Arrays.asList(types))
        .type(InstrumentationType.TRACE)
        .build();
  }

  @Test
  void semanticallyEqualWhenIdentical() {
    assertThat(baseRule().isSemanticallyEqual(baseRule())).isTrue();
  }

  @Test
  void notSemanticallyEqualWhenNull() {
    assertThat(baseRule().isSemanticallyEqual(null)).isFalse();
  }

  @Test
  void notSemanticallyEqualWhenClassNameDiffers() {
    InstrumentationRule other =
        InstrumentationRule.builder()
            .ruleId("rule-002")
            .className("com.example.OtherService")
            .methodName("handleRequest")
            .type(InstrumentationType.TRACE)
            .build();
    assertThat(baseRule().isSemanticallyEqual(other)).isFalse();
  }

  @Test
  void notSemanticallyEqualWhenMethodNameDiffers() {
    InstrumentationRule other =
        InstrumentationRule.builder()
            .ruleId("rule-002")
            .className("com.example.MyService")
            .methodName("handleLogin")
            .type(InstrumentationType.TRACE)
            .build();
    assertThat(baseRule().isSemanticallyEqual(other)).isFalse();
  }

  @Test
  void notSemanticallyEqualWhenTypeDiffers() {
    InstrumentationRule other =
        InstrumentationRule.builder()
            .ruleId("rule-002")
            .className("com.example.MyService")
            .methodName("handleRequest")
            .type(InstrumentationType.METRIC)
            .build();
    assertThat(baseRule().isSemanticallyEqual(other)).isFalse();
  }

  @Test
  void notSemanticallyEqualWhenParameterTypesDiffer() {
    InstrumentationRule noParams = baseRule(); // parameterTypes == null
    InstrumentationRule oneParam = withParameterTypes("String");
    InstrumentationRule twoParams = withParameterTypes("String", "int");
    InstrumentationRule emptyParams = withParameterTypes(); // empty list == no-arg method

    assertThat(noParams.isSemanticallyEqual(oneParam)).isFalse();
    assertThat(oneParam.isSemanticallyEqual(twoParams)).isFalse();
    // null (match all overloads) vs empty list (match no-arg) 语义不同
    assertThat(noParams.isSemanticallyEqual(emptyParams)).isFalse();
    assertThat(oneParam.isSemanticallyEqual(withParameterTypes("String"))).isTrue();
  }

  @Test
  void notSemanticallyEqualWhenConfigDiffers() {
    Map<String, String> captureArgs = new HashMap<>();
    captureArgs.put("capture_args", "*");
    InstrumentationRule withConfig =
        InstrumentationRule.builder()
            .ruleId("rule-002")
            .className("com.example.MyService")
            .methodName("handleRequest")
            .type(InstrumentationType.TRACE)
            .config(captureArgs)
            .build();
    assertThat(baseRule().isSemanticallyEqual(withConfig)).isFalse();
    assertThat(withConfig.isSemanticallyEqual(withConfig)).isTrue();
  }

  @Test
  void notSemanticallyEqualWhenSpanNameDiffers() {
    InstrumentationRule withSpanName =
        InstrumentationRule.builder()
            .ruleId("rule-002")
            .className("com.example.MyService")
            .methodName("handleRequest")
            .type(InstrumentationType.TRACE)
            .spanName("custom.span")
            .build();
    assertThat(baseRule().isSemanticallyEqual(withSpanName)).isFalse();
  }

  @Test
  void ruleIdDoesNotAffectSemanticEquality() {
    InstrumentationRule other =
        InstrumentationRule.builder()
            .ruleId("rule-different-id")
            .className("com.example.MyService")
            .methodName("handleRequest")
            .type(InstrumentationType.TRACE)
            .build();
    assertThat(baseRule().isSemanticallyEqual(other)).isTrue();
  }
}
