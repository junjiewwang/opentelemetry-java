/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionContext;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionResult;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 动态增强任务执行器
 *
 * <p>处理 {@code dynamic_instrument} 类型的任务，从控制平面下发的增强规则。
 *
 * <p>任务参数示例：
 * <pre>{@code
 * {
 *   "rule_id": "rule-001",
 *   "class_name": "com.example.MyService",
 *   "method_name": "handleRequest",
 *   "method_descriptor": "(Ljava/lang/String;)V",  // 可选
 *   "type": "trace",                                 // trace | metric | log
 *   "span_name": "MyService.handleRequest",          // 可选
 *   "config.key": "value"                             // 额外配置
 * }
 * }</pre>
 */
public final class DynamicInstrumentExecutor implements TaskExecutor {

  private static final Logger logger =
      DynamicInstrumentLogger.getLogger("executor");

  /** 任务类型常量 */
  public static final String TASK_TYPE = "dynamic_instrument";

  private final TransformerManager transformerManager;

  DynamicInstrumentExecutor(TransformerManager transformerManager) {
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
        logger.log(Level.INFO,
            "[DYNAMIC-INSTRUMENT] Executing enhancement task: {0}",
            context.getTaskId());

        // 1. 解析增强规则
        InstrumentationRule rule = parseRuleFromContext(context);
        logger.log(Level.INFO,
            "[DYNAMIC-INSTRUMENT] Parsed rule: {0}", rule);

        // 2. 应用增强
        TransformerManager.EnhancementResult result = transformerManager.applyRule(rule);

        // 3. 构建返回结果
        long executionTime = System.currentTimeMillis() - startTime;
        if (result.isSuccess()) {
          logger.log(Level.INFO,
              "[DYNAMIC-INSTRUMENT] Enhancement applied successfully: {0}", result);
          return TaskExecutionResult.success(
              "{\"rule_id\":\"" + rule.getRuleId()
                  + "\",\"class_name\":\"" + rule.getClassName()
                  + "\",\"method_name\":\"" + rule.getMethodName()
                  + "\",\"type\":\"" + rule.getType().getValue()
                  + "\",\"status\":\"active\"}",
              executionTime);
        } else {
          logger.log(Level.WARNING,
              "[DYNAMIC-INSTRUMENT] Enhancement failed: {0}", result);
          return TaskExecutionResult.failed(
              result.getErrorCode() != null ? result.getErrorCode() : "ENHANCEMENT_FAILED",
              result.getErrorMessage() != null ? result.getErrorMessage() : "Unknown error",
              executionTime);
        }

      } catch (IllegalArgumentException e) {
        String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
        logger.log(Level.WARNING,
            "[DYNAMIC-INSTRUMENT] Invalid parameters: " + msg, e);
        return TaskExecutionResult.failed("INVALID_PARAMETERS", msg,
            System.currentTimeMillis() - startTime);
      } catch (RuntimeException e) {
        logger.log(Level.SEVERE,
            "[DYNAMIC-INSTRUMENT] Unexpected error: " + e.getMessage(), e);
        return TaskExecutionResult.fromException("ENHANCEMENT_ERROR", e);
      } catch (Throwable t) {
        // 兜底捕获 Error（如 NoClassDefFoundError），防止异常逃逸到 CompletableFuture
        logger.log(Level.SEVERE,
            "[DYNAMIC-INSTRUMENT] Fatal error: " + t.getClass().getName()
                + ": " + t.getMessage(), t);
        return TaskExecutionResult.failed("FATAL_ERROR",
            t.getClass().getName() + ": " + t.getMessage(),
            System.currentTimeMillis() - startTime);
      }
    });
  }

  @Override
  public String getDescription() {
    return "Dynamic class enhancement executor (trace/metric/log)";
  }

  /**
   * 从 TaskExecutionContext 解析增强规则
   *
   * <p>此方法从 {@link InstrumentationRule} 中剥离到此处，
   * 使 InstrumentationRule 成为纯数据模型类，不依赖 TaskExecutionContext，
   * 可安全注入到 Bootstrap ClassLoader。
   *
   * @param context 任务执行上下文
   * @return 增强规则
   * @throws IllegalArgumentException 如果参数不合法
   */
  private static InstrumentationRule parseRuleFromContext(TaskExecutionContext context) {
    // 解析增强类型
    String typeStr = context.getStringParameter("type", "trace");
    InstrumentationType type = InstrumentationType.fromString(typeStr);
    if (type == null) {
      throw new IllegalArgumentException(
          "Unsupported instrumentation type: " + typeStr
              + ", supported: " + InstrumentationType.supportedValues());
    }

    // 解析额外配置
    Map<String, String> config = new HashMap<>();
    Map<String, Object> params = context.getParameters();
    for (Map.Entry<String, Object> entry : params.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith("config.") && entry.getValue() != null) {
        config.put(key.substring("config.".length()), String.valueOf(entry.getValue()));
      }
    }

    return InstrumentationRule.builder()
        .ruleId(context.getStringParameter("rule_id", context.getTaskId()))
        .className(context.getStringParameter("class_name", ""))
        .methodName(context.getStringParameter("method_name", ""))
        .methodDescriptor(context.getStringParameter("method_descriptor", ""))
        .type(type)
        .spanName(context.getStringParameter("span_name", ""))
        .config(config)
        .build();
  }
}
