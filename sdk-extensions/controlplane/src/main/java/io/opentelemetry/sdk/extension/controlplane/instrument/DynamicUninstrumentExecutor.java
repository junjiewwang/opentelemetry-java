/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionContext;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionResult;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutor;
import io.opentelemetry.sdk.extension.controlplane.util.JsonUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 动态还原任务执行器
 *
 * <p>处理 {@code dynamic_uninstrument} 类型的任务，从控制平面下发的还原指令。
 *
 * <p>支持两种还原方式：
 *
 * <p>方式 1：按 rule_id 还原（精确）
 * <pre>{@code
 * {
 *   "rule_id": "rule-001"
 * }
 * }</pre>
 *
 * <p>方式 2：按目标方法还原（简化，自动推导 rule_id）
 * <pre>{@code
 * {
 *   "class_name": "com.example.MyService",
 *   "method_name": "handleRequest",
 *   "type": "trace"              // 可选，默认 trace
 * }
 * }</pre>
 *
 * <p>当两种方式都传时，优先使用 {@code rule_id}。
 * 方式 2 会查找该目标方法下所有匹配的增强规则并逐一还原。
 */
public final class DynamicUninstrumentExecutor implements TaskExecutor {

  private static final Logger logger =
      DynamicInstrumentLogger.getLogger("uninstrument-executor");

  /** 任务类型常量 */
  public static final String TASK_TYPE = "dynamic_uninstrument";

  private final TransformerManager transformerManager;

  DynamicUninstrumentExecutor(TransformerManager transformerManager) {
    this.transformerManager = transformerManager;
  }

  @Override
  public String getTaskType() {
    return TASK_TYPE;
  }

  @Override
  public CompletableFuture<TaskExecutionResult> execute(TaskExecutionContext context) {
    return CompletableFuture.supplyAsync(() -> {
      long startTime = System.currentTimeMillis();
      try {
        // 解析要还原的 ruleId 列表
        List<String> ruleIds = resolveRuleIds(context);
        if (ruleIds.isEmpty()) {
          return TaskExecutionResult.failed("INVALID_PARAMETERS",
              "Either 'rule_id' or 'class_name'+'method_name' is required",
              System.currentTimeMillis() - startTime);
        }

        logger.log(Level.INFO,
            "[DYNAMIC-UNINSTRUMENT] Executing revert task: ruleIds={0}", ruleIds);

        // 逐一还原（多条规则时，如按目标方法匹配到多个重载）
        List<String> successIds = new ArrayList<>();
        TransformerManager.EnhancementResult lastFailure = null;

        for (String ruleId : ruleIds) {
          TransformerManager.EnhancementResult result = transformerManager.revertRule(ruleId);
          if (result.isSuccess()) {
            successIds.add(ruleId);
            logger.log(Level.INFO,
                "[DYNAMIC-UNINSTRUMENT] Revert successful: {0}", ruleId);
          } else {
            lastFailure = result;
            logger.log(Level.WARNING,
                "[DYNAMIC-UNINSTRUMENT] Revert failed: {0}", result);
          }
        }

        // 构建返回结果
        long executionTime = System.currentTimeMillis() - startTime;
        if (!successIds.isEmpty() && lastFailure == null) {
          // 全部成功
          return TaskExecutionResult.success(
              JsonUtils.toJsonObject(
                  "rule_ids", successIds,
                  "status", "reverted"),
              executionTime);
        } else if (!successIds.isEmpty()) {
          // 部分成功
          return TaskExecutionResult.success(
              JsonUtils.objectBuilder()
                  .put("rule_ids", successIds)
                  .put("status", "partially_reverted")
                  .putIfNotNull("last_error",
                      lastFailure != null ? lastFailure.getErrorMessage() : null)
                  .build(),
              executionTime);
        } else if (lastFailure != null) {
          // 全部失败
          return TaskExecutionResult.failed(
              lastFailure.getErrorCode() != null ? lastFailure.getErrorCode() : "REVERT_FAILED",
              lastFailure.getErrorMessage() != null ? lastFailure.getErrorMessage() : "Unknown error",
              executionTime);
        } else {
          return TaskExecutionResult.failed("RULE_NOT_FOUND",
              "No matching rules found for the specified target",
              executionTime);
        }

      } catch (RuntimeException e) {
        logger.log(Level.SEVERE,
            "[DYNAMIC-UNINSTRUMENT] Unexpected error: " + e.getMessage(), e);
        return TaskExecutionResult.fromException("REVERT_ERROR", e);
      }
    });
  }

  /**
   * 解析要还原的 ruleId 列表
   *
   * <p>优先使用显式传入的 {@code rule_id}；如果未传，则尝试通过
   * {@code class_name + method_name + type} 在 TransformerManager 中查找。
   *
   * @param context 任务执行上下文
   * @return ruleId 列表，可能为空
   */
  private List<String> resolveRuleIds(TaskExecutionContext context) {
    // 方式 1：显式指定 rule_id
    String ruleId = context.getStringParameter("rule_id", "");
    if (!ruleId.isEmpty()) {
      return Collections.singletonList(ruleId);
    }

    // 方式 2：按 class_name + method_name [+ type] 查找
    String className = context.getStringParameter("class_name", "");
    String methodName = context.getStringParameter("method_name", "");
    if (className.isEmpty() || methodName.isEmpty()) {
      return Collections.emptyList();
    }

    String typeStr = context.getStringParameter("type", "");
    if (typeStr.isEmpty()) {
      // 未指定 type，查找所有类型
      List<String> allRuleIds = new ArrayList<>();
      for (InstrumentationType type : InstrumentationType.values()) {
        allRuleIds.addAll(transformerManager.findRuleIdsByTarget(className, methodName, type));
      }
      if (!allRuleIds.isEmpty()) {
        logger.log(Level.INFO,
            "[DYNAMIC-UNINSTRUMENT] Resolved ruleIds by target {0}#{1} (all types): {2}",
            new Object[] {className, methodName, allRuleIds});
      }
      return Collections.unmodifiableList(allRuleIds);
    }

    InstrumentationType type = InstrumentationType.fromString(typeStr);
    if (type == null) {
      logger.log(Level.WARNING,
          "[DYNAMIC-UNINSTRUMENT] Unsupported type: {0}", typeStr);
      return Collections.emptyList();
    }

    List<String> ruleIds = transformerManager.findRuleIdsByTarget(className, methodName, type);
    if (!ruleIds.isEmpty()) {
      logger.log(Level.INFO,
          "[DYNAMIC-UNINSTRUMENT] Resolved ruleIds by target {0}#{1}#{2}: {3}",
          new Object[] {className, methodName, typeStr, ruleIds});
    }
    return ruleIds; // findRuleIdsByTarget 已返回不可变列表
  }

  @Override
  public String getDescription() {
    return "Dynamic class enhancement revert executor";
  }
}
