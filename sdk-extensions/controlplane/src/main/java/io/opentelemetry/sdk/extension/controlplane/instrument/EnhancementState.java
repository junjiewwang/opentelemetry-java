/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import java.util.Locale;
import javax.annotation.Nullable;

/**
 * 增强状态
 *
 * <p>记录一条增强规则的当前运行时状态，包括：
 * <ul>
 *   <li>关联的规则信息</li>
 *   <li>当前状态（PENDING / ACTIVE / REVERTING / REVERTED / FAILED）</li>
 *   <li>时间戳</li>
 *   <li>错误信息（失败时）</li>
 *   <li>已增强的类名（用于还原时触发 retransformClasses）</li>
 * </ul>
 *
 * <p>可变对象（状态会随生命周期变化），通过 {@link EnhancementStateRegistry} 管理。
 */
public final class EnhancementState {

  /** 增强状态枚举 */
  public enum Status {
    /** 等待应用 */
    PENDING,
    /** 已生效 */
    ACTIVE,
    /** 正在还原 */
    REVERTING,
    /** 已还原 */
    REVERTED,
    /** 应用/还原失败 */
    FAILED
  }

  private final InstrumentationRule rule;
  private volatile Status status;
  private volatile long createdAtMillis;
  private volatile long activatedAtMillis;
  private volatile long revertedAtMillis;
  @Nullable private volatile String errorMessage;
  @Nullable private volatile String enhancedClassName;

  /**
   * 创建增强状态（初始为 PENDING）
   *
   * @param rule 增强规则
   */
  public EnhancementState(InstrumentationRule rule) {
    this.rule = rule;
    this.status = Status.PENDING;
    this.createdAtMillis = System.currentTimeMillis();
  }

  /** 获取关联的增强规则 */
  public InstrumentationRule getRule() {
    return rule;
  }

  /** 获取规则 ID */
  public String getRuleId() {
    return rule.getRuleId();
  }

  /** 获取当前状态 */
  public Status getStatus() {
    return status;
  }

  /** 获取创建时间 */
  public long getCreatedAtMillis() {
    return createdAtMillis;
  }

  /** 获取激活时间 */
  public long getActivatedAtMillis() {
    return activatedAtMillis;
  }

  /** 获取还原时间 */
  public long getRevertedAtMillis() {
    return revertedAtMillis;
  }

  /** 获取错误信息 */
  @Nullable
  public String getErrorMessage() {
    return errorMessage;
  }

  /** 获取已增强的类名 */
  @Nullable
  public String getEnhancedClassName() {
    return enhancedClassName;
  }

  /** 标记为已激活 */
  public void markActive(String enhancedClassName) {
    this.status = Status.ACTIVE;
    this.activatedAtMillis = System.currentTimeMillis();
    this.enhancedClassName = enhancedClassName;
    this.errorMessage = null;
  }

  /** 标记为正在还原 */
  public void markReverting() {
    this.status = Status.REVERTING;
  }

  /** 标记为已还原 */
  public void markReverted() {
    this.status = Status.REVERTED;
    this.revertedAtMillis = System.currentTimeMillis();
  }

  /** 标记为失败 */
  public void markFailed(String errorMessage) {
    this.status = Status.FAILED;
    this.errorMessage = errorMessage;
  }

  /** 是否处于活跃状态 */
  public boolean isActive() {
    return status == Status.ACTIVE;
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "EnhancementState{ruleId='%s', status=%s, target='%s.%s', enhanced='%s'}",
        rule.getRuleId(), status, rule.getClassName(), rule.getMethodName(), enhancedClassName);
  }
}
