/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.extension.controlplane.instrument.TransformerManager.EnhancementResult;
import org.junit.jupiter.api.Test;

class TransformerManagerTest {

  private static InstrumentationRule rule(String ruleId, String className, String methodName,
      InstrumentationType type) {
    return InstrumentationRule.builder()
        .ruleId(ruleId)
        .className(className)
        .methodName(methodName)
        .type(type)
        .build();
  }

  @Test
  void resolveAlreadyAppliedReturnsNullWhenNoExistingRule() {
    InstrumentationRule newRule =
        rule("rule-001", "com.example.MyService", "handleRequest", InstrumentationType.TRACE);

    assertThat(TransformerManager.resolveAlreadyApplied("rule-001", null, newRule)).isNull();
  }

  @Test
  void resolveAlreadyAppliedReturnsIdempotentSuccessWhenSemanticallyEqual() {
    InstrumentationRule existing =
        rule("rule-001", "com.example.MyService", "handleRequest", InstrumentationType.TRACE);
    InstrumentationRule newRule =
        rule("rule-001", "com.example.MyService", "handleRequest", InstrumentationType.TRACE);

    EnhancementResult result =
        TransformerManager.resolveAlreadyApplied("rule-001", existing, newRule);

    assertThat(result).isNotNull();
    assertThat(result.isSuccess()).isTrue();
    assertThat(result.isIdempotent()).isTrue();
    assertThat(result.getErrorCode()).isNull();
  }

  @Test
  void resolveAlreadyAppliedReturnsConflictWhenContentDiffers() {
    InstrumentationRule existing =
        rule("rule-001", "com.example.MyService", "handleRequest", InstrumentationType.TRACE);
    InstrumentationRule differentTarget =
        rule("rule-001", "com.example.MyService", "handleLogin", InstrumentationType.TRACE);

    EnhancementResult result =
        TransformerManager.resolveAlreadyApplied("rule-001", existing, differentTarget);

    assertThat(result).isNotNull();
    assertThat(result.isSuccess()).isFalse();
    assertThat(result.isIdempotent()).isFalse();
    assertThat(result.getErrorCode()).isEqualTo("RULE_ID_CONFLICT");
    assertThat(result.getErrorMessage()).contains("rule-001").contains("handleRequest");
  }

  @Test
  void resolveAlreadyAppliedDetectsTypeConflict() {
    InstrumentationRule existing =
        rule("rule-001", "com.example.MyService", "handleRequest", InstrumentationType.TRACE);
    InstrumentationRule differentType =
        rule("rule-001", "com.example.MyService", "handleRequest", InstrumentationType.METRIC);

    EnhancementResult result =
        TransformerManager.resolveAlreadyApplied("rule-001", existing, differentType);

    assertThat(result.isSuccess()).isFalse();
    assertThat(result.getErrorCode()).isEqualTo("RULE_ID_CONFLICT");
  }

  @Test
  void idempotentSuccessResultContract() {
    EnhancementResult result = EnhancementResult.idempotentSuccess("rule-001");

    assertThat(result.getRuleId()).isEqualTo("rule-001");
    assertThat(result.isSuccess()).isTrue();
    assertThat(result.isIdempotent()).isTrue();
    assertThat(result.hasWarnings()).isFalse();
  }

  @Test
  void normalSuccessIsNotIdempotent() {
    EnhancementResult result = EnhancementResult.success("rule-001");

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.isIdempotent()).isFalse();
  }
}
