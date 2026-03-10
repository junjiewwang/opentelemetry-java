/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionContext;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionResult;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutor;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 动态还原任务执行器
 *
 * <p>处理 {@code dynamic_uninstrument} 类型的任务，从控制平面下发的还原指令。
 *
 * <p>任务参数示例：
 * <pre>{@code
 * {
 *   "rule_id": "rule-001"   // 要还原的规则 ID
 * }
 * }</pre>
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
        // 获取要还原的规则 ID
        String ruleId = context.getStringParameter("rule_id", "");
        if (ruleId.isEmpty()) {
          return TaskExecutionResult.failed("INVALID_PARAMETERS",
              "rule_id is required",
              System.currentTimeMillis() - startTime);
        }

        logger.log(Level.INFO,
            "[DYNAMIC-UNINSTRUMENT] Executing revert task: ruleId={0}", ruleId);

        // 执行还原
        TransformerManager.EnhancementResult result = transformerManager.revertRule(ruleId);

        // 构建返回结果
        long executionTime = System.currentTimeMillis() - startTime;
        if (result.isSuccess()) {
          logger.log(Level.INFO,
              "[DYNAMIC-UNINSTRUMENT] Revert successful: {0}", ruleId);
          return TaskExecutionResult.success(
              "{\"rule_id\":\"" + ruleId + "\",\"status\":\"reverted\"}",
              executionTime);
        } else {
          logger.log(Level.WARNING,
              "[DYNAMIC-UNINSTRUMENT] Revert failed: {0}", result);
          return TaskExecutionResult.failed(
              result.getErrorCode() != null ? result.getErrorCode() : "REVERT_FAILED",
              result.getErrorMessage() != null ? result.getErrorMessage() : "Unknown error",
              executionTime);
        }

      } catch (RuntimeException e) {
        logger.log(Level.SEVERE,
            "[DYNAMIC-UNINSTRUMENT] Unexpected error: " + e.getMessage(), e);
        return TaskExecutionResult.fromException("REVERT_ERROR", e);
      }
    });
  }

  @Override
  public String getDescription() {
    return "Dynamic class enhancement revert executor";
  }
}
