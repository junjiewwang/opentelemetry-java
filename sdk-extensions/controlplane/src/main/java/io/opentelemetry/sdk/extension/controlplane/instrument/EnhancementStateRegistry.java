/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 增强状态注册表
 *
 * <p>追踪所有动态增强规则的当前状态。线程安全，支持并发访问。
 *
 * <p>职责：
 * <ul>
 *   <li>注册新的增强状态</li>
 *   <li>查询/更新增强状态</li>
 *   <li>获取所有活跃的增强列表</li>
 *   <li>生成状态统计摘要</li>
 * </ul>
 */
public final class EnhancementStateRegistry {

  private static final Logger logger =
      DynamicInstrumentLogger.getLogger("state-registry");

  /** ruleId -> EnhancementState */
  private final ConcurrentHashMap<String, EnhancementState> states = new ConcurrentHashMap<>();

  /**
   * 注册新的增强状态
   *
   * @param rule 增强规则
   * @return 增强状态
   */
  public EnhancementState register(InstrumentationRule rule) {
    EnhancementState state = new EnhancementState(rule);
    EnhancementState existing = states.putIfAbsent(rule.getRuleId(), state);
    if (existing != null) {
      logger.log(Level.WARNING,
          "[ENHANCEMENT-REGISTRY] Rule already registered: {0}, status={1}",
          new Object[] {rule.getRuleId(), existing.getStatus()});
      return existing;
    }
    logger.log(Level.INFO,
        "[ENHANCEMENT-REGISTRY] Registered: {0}", rule.getRuleId());
    return state;
  }

  /**
   * 获取增强状态
   *
   * @param ruleId 规则 ID
   * @return 增强状态，不存在则返回 null
   */
  @Nullable
  public EnhancementState get(String ruleId) {
    return states.get(ruleId);
  }

  /**
   * 移除增强状态
   *
   * @param ruleId 规则 ID
   * @return 被移除的增强状态，不存在则返回 null
   */
  @Nullable
  public EnhancementState remove(String ruleId) {
    EnhancementState removed = states.remove(ruleId);
    if (removed != null) {
      logger.log(Level.INFO,
          "[ENHANCEMENT-REGISTRY] Removed: {0}", ruleId);
    }
    return removed;
  }

  /**
   * 检查规则是否已注册
   *
   * @param ruleId 规则 ID
   * @return 是否已注册
   */
  public boolean contains(String ruleId) {
    return states.containsKey(ruleId);
  }

  /**
   * 获取所有活跃的增强状态
   *
   * @return 活跃的增强状态列表
   */
  public List<EnhancementState> getActiveStates() {
    List<EnhancementState> active = new ArrayList<>();
    for (EnhancementState state : states.values()) {
      if (state.isActive()) {
        active.add(state);
      }
    }
    return active;
  }

  /**
   * 获取所有增强状态
   *
   * @return 所有增强状态列表
   */
  public List<EnhancementState> getAllStates() {
    return new ArrayList<>(states.values());
  }

  /**
   * 获取当前注册的规则数量
   *
   * @return 规则数量
   */
  public int size() {
    return states.size();
  }

  /**
   * 获取活跃的增强数量
   *
   * @return 活跃增强数量
   */
  public int getActiveCount() {
    int count = 0;
    for (EnhancementState state : states.values()) {
      if (state.isActive()) {
        count++;
      }
    }
    return count;
  }

  /**
   * 清除所有状态（用于关闭时清理）
   */
  public void clear() {
    int size = states.size();
    states.clear();
    logger.log(Level.INFO,
        "[ENHANCEMENT-REGISTRY] Cleared all {0} state(s)", size);
  }

  /**
   * 生成状态摘要
   *
   * @return 状态摘要字符串
   */
  public String toSummary() {
    int total = states.size();
    int active = 0;
    int failed = 0;
    int reverted = 0;
    for (EnhancementState state : states.values()) {
      switch (state.getStatus()) {
        case ACTIVE:
          active++;
          break;
        case FAILED:
          failed++;
          break;
        case REVERTED:
          reverted++;
          break;
        default:
          break;
      }
    }
    return String.format(
        Locale.ROOT,
        "EnhancementStateRegistry{total=%d, active=%d, failed=%d, reverted=%d}",
        total, active, failed, reverted);
  }
}
