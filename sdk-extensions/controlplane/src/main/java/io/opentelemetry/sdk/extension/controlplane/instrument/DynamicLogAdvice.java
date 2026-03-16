/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * LOG 类型的动态增强 Advice 桥接
 *
 * <p>在方法入口和出口记录日志，包含方法名、参数概要和执行耗时。
 *
 * <p>使用 {@link java.util.logging.Logger} 输出日志（JUL），
 * 在 OTel Java Agent 环境中，JUL 日志会自动桥接到 OTel LoggerProvider。
 *
 * <p>日志格式：
 * <ul>
 *   <li>入口：{@code [DYNAMIC-LOG] ENTER ClassName.methodName [ruleId]}</li>
 *   <li>出口：{@code [DYNAMIC-LOG] EXIT ClassName.methodName [ruleId] duration=Xms}</li>
 *   <li>异常：{@code [DYNAMIC-LOG] ERROR ClassName.methodName [ruleId] exception=...}</li>
 * </ul>
 */
final class DynamicLogAdvice {

  private static final Logger logger =
      DynamicInstrumentLogger.getLogger("log");

  /** ruleId -> InstrumentationRule */
  private static final ConcurrentHashMap<String, InstrumentationRule> RULE_REGISTRY =
      new ConcurrentHashMap<>();

  /** 预解析的 CaptureConfig 缓存：ruleId -> CaptureConfig（在增强阶段预解析，运行时零开销） */
  private static final ConcurrentHashMap<String, CaptureConfig> CAPTURE_CONFIG_CACHE =
      new ConcurrentHashMap<>();

  private DynamicLogAdvice() {}

  /**
   * 注册规则
   *
   * @param rule 增强规则
   */
  static void registerRule(InstrumentationRule rule) {
    RULE_REGISTRY.put(rule.getRuleId(), rule);
  }

  /**
   * 注册规则及其预解析的 CaptureConfig
   *
   * <p>统一注册入口。在增强阶段（非热路径）通过反射获取目标 Method 后预解析
   * CaptureConfig，运行时直接从缓存中取用，避免每次方法调用都重新 parse。
   * 无采集配置时 captureConfig 传 null 即可，此时行为等同于 {@link #registerRule(InstrumentationRule)}。
   *
   * @param rule 增强规则
   * @param captureConfig 预解析的采集配置（可以为 null）
   */
  static void registerRule(InstrumentationRule rule, @Nullable CaptureConfig captureConfig) {
    RULE_REGISTRY.put(rule.getRuleId(), rule);
    if (captureConfig != null && captureConfig.hasCaptureConfig()) {
      CAPTURE_CONFIG_CACHE.put(rule.getRuleId(), captureConfig);
    }
  }

  /**
   * 注销规则
   *
   * @param ruleId 规则 ID
   */
  static void unregisterRule(String ruleId) {
    RULE_REGISTRY.remove(ruleId);
    CAPTURE_CONFIG_CACHE.remove(ruleId);
  }

  /**
   * 方法入口回调：记录入口日志和起始时间
   *
   * @param ruleId 规则 ID
   * @return 起始时间（纳秒），失败返回 null
   */
  @Nullable
  static Long onMethodEnter(String ruleId) {
    try {
      InstrumentationRule rule = RULE_REGISTRY.get(ruleId);
      if (rule == null) {
        return null;
      }

      logger.log(Level.INFO,
          "[DYNAMIC-LOG] ENTER {0}.{1} [ruleId={2}]",
          new Object[] {rule.getClassName(), rule.getMethodName(), ruleId});

      return System.nanoTime();
    } catch (RuntimeException e) {
      // 日志增强不应影响业务方法
      return null;
    }
  }

  /**
   * 方法入口回调（带参数采集）：记录入口日志并附带采集的参数信息
   *
   * <p>当规则配置了 {@code capture_args} 时，由 {@link AdviceDispatcher#onEnterWithCapture} 调用。
   * 参数信息追加到日志消息体中。
   *
   * <p>返回上下文改为 {@code Object[]}：{@code [startTimeNanos, ruleId]}，
   * 让 {@link #onMethodExitWithCapture} 能够通过 ruleId 查找 CaptureConfig。
   *
   * @param ruleId 规则 ID
   * @param args 方法所有参数（来自 @AllArguments）
   * @return 入口上下文 Object[]：[startTimeNanos, ruleId]，失败返回 null
   */
  @SuppressWarnings("AvoidObjectArrays") // 必须使用 Object[]，来自 ByteBuddy @AllArguments
  @Nullable
  static Object[] onMethodEnterWithCapture(String ruleId, @Nullable Object[] args) {
    try {
      InstrumentationRule rule = RULE_REGISTRY.get(ruleId);
      if (rule == null) {
        return null;
      }

      // 构建参数日志字符串
      String argsStr = buildArgsCaptureString(ruleId, args);

      logger.log(Level.INFO,
          "[DYNAMIC-LOG] ENTER {0}.{1} [ruleId={2}]{3}",
          new Object[] {rule.getClassName(), rule.getMethodName(), ruleId, argsStr});

      return new Object[] {Long.valueOf(System.nanoTime()), ruleId};
    } catch (RuntimeException e) {
      // 日志增强不应影响业务方法
      return null;
    }
  }

  /**
   * 方法出口回调：记录出口日志
   *
   * @param ruleId 规则 ID
   * @param startTimeNanos 起始时间（纳秒，可能为 null）
   * @param thrown 方法抛出的异常（可能为 null）
   */
  static void onMethodExit(
      String ruleId, @Nullable Long startTimeNanos, @Nullable Throwable thrown) {
    try {
      InstrumentationRule rule = RULE_REGISTRY.get(ruleId);
      if (rule == null) {
        return;
      }

      long durationMs = 0;
      if (startTimeNanos != null) {
        durationMs = (System.nanoTime() - startTimeNanos) / 1_000_000;
      }

      if (thrown != null) {
        logger.log(Level.WARNING,
            "[DYNAMIC-LOG] ERROR {0}.{1} [ruleId={2}] duration={3}ms exception={4}",
            new Object[] {
              rule.getClassName(), rule.getMethodName(), ruleId,
              Long.valueOf(durationMs), thrown.getClass().getName() + ": " + thrown.getMessage()
            });
      } else {
        logger.log(Level.INFO,
            "[DYNAMIC-LOG] EXIT {0}.{1} [ruleId={2}] duration={3}ms",
            new Object[] {
              rule.getClassName(), rule.getMethodName(), ruleId, Long.valueOf(durationMs)
            });
      }
    } catch (RuntimeException e) {
      // 日志增强不应影响业务方法
    }
  }

  /**
   * 方法出口回调（带返回值采集）：记录出口日志并附带采集的返回值信息
   *
   * <p>当规则配置了 {@code capture_return} 时，由 {@link AdviceDispatcher#onExitWithCapture} 调用。
   * 返回值信息追加到日志消息体中。
   *
   * @param enterContext 方法入口返回的上下文（Object[] 或 null）
   * @param thrown 方法抛出的异常（可能为 null）
   * @param returnValue 方法返回值（可能为 null）
   */
  static void onMethodExitWithCapture(
      @Nullable Object[] enterContext, @Nullable Throwable thrown,
      @Nullable Object returnValue) {
    if (enterContext == null || enterContext.length < 2) {
      return;
    }
    try {
      Long startTimeNanos = (Long) enterContext[0];
      String ruleId = (String) enterContext[1];

      InstrumentationRule rule = RULE_REGISTRY.get(ruleId);
      if (rule == null) {
        return;
      }

      long durationMs = 0;
      if (startTimeNanos != null) {
        durationMs = (System.nanoTime() - startTimeNanos) / 1_000_000;
      }

      // 构建返回值日志字符串
      String returnStr = buildReturnCaptureString(ruleId, returnValue);

      if (thrown != null) {
        logger.log(Level.WARNING,
            "[DYNAMIC-LOG] ERROR {0}.{1} [ruleId={2}] duration={3}ms{4} exception={5}",
            new Object[] {
              rule.getClassName(), rule.getMethodName(), ruleId,
              Long.valueOf(durationMs), returnStr,
              thrown.getClass().getName() + ": " + thrown.getMessage()
            });
      } else {
        logger.log(Level.INFO,
            "[DYNAMIC-LOG] EXIT {0}.{1} [ruleId={2}] duration={3}ms{4}",
            new Object[] {
              rule.getClassName(), rule.getMethodName(), ruleId,
              Long.valueOf(durationMs), returnStr
            });
      }
    } catch (RuntimeException e) {
      // 日志增强不应影响业务方法
    }
  }

  // ======================== 私有工具方法 ========================

  /**
   * 构建参数采集日志字符串
   *
   * <p>按 {@link CaptureConfig} 中预解析的索引和名称，从方法参数中安全提取值并格式化。
   * 无采集配置时返回空字符串。
   *
   * @param ruleId 规则 ID
   * @param args 方法参数数组（可能为 null）
   * @return 格式化的参数字符串，如 {@code " args={userId=42, name=John}"}，或空字符串
   */
  private static String buildArgsCaptureString(String ruleId, @Nullable Object[] args) {
    if (args == null) {
      return "";
    }
    CaptureConfig captureConfig = CAPTURE_CONFIG_CACHE.get(ruleId);
    if (captureConfig == null) {
      return "";
    }
    int[] argIndices = captureConfig.getArgIndices();
    if (argIndices.length == 0) {
      return "";
    }
    StringBuilder sb = new StringBuilder(" args={");
    for (int i = 0; i < argIndices.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      int idx = argIndices[i];
      String keyName = captureConfig.getArgKeyName(i);
      String value = idx < args.length
          ? CaptureProcessor.safeToString(args[idx], captureConfig.getMaxLength())
          : "N/A";
      sb.append(keyName).append("=").append(value);
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * 构建返回值采集日志字符串
   *
   * <p>按 {@link CaptureConfig} 配置，安全提取返回值并格式化。
   * 支持 {@code "*"} 模式（整体 toString）和指定字段模式。
   * 无采集配置时返回空字符串。
   *
   * @param ruleId 规则 ID
   * @param returnValue 方法返回值（可能为 null）
   * @return 格式化的返回值字符串，如 {@code " return=UserInfo{...}"} 或
   *         {@code " return={status=OK, code=200}"}，或空字符串
   */
  private static String buildReturnCaptureString(String ruleId, @Nullable Object returnValue) {
    CaptureConfig captureConfig = CAPTURE_CONFIG_CACHE.get(ruleId);
    if (captureConfig == null || !captureConfig.isCaptureReturn() || returnValue == null) {
      return "";
    }
    String[] returnFields = captureConfig.getReturnFields();
    if (returnFields.length == 0) {
      // "*" 模式：采集 toString()
      return " return="
          + CaptureProcessor.safeToString(returnValue, captureConfig.getMaxLength());
    } else {
      // 指定字段模式：仅提取指定字段
      StringBuilder sb = new StringBuilder(" return={");
      for (int i = 0; i < returnFields.length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        Object fieldValue = CaptureProcessor.extractField(returnValue, returnFields[i]);
        sb.append(returnFields[i]).append("=")
            .append(CaptureProcessor.safeToString(fieldValue, captureConfig.getMaxLength()));
      }
      sb.append("}");
      return sb.toString();
    }
  }
}
