/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 参数/返回值采集配置
 *
 * <p>从 {@link InstrumentationRule#getConfig()} 中解析采集规则，支持三种参数指定方式：
 * <ul>
 *   <li><b>按索引</b>：{@code "config.capture_args": "0,2"} → 采集第0和第2个参数</li>
 *   <li><b>按参数名</b>：{@code "config.capture_args": "userId,requestType"} → 按参数名采集</li>
 *   <li><b>全部参数</b>：{@code "config.capture_args": "*"} → 采集所有参数</li>
 * </ul>
 *
 * <p>返回值采集通过 {@code config.capture_return} 统一配置：
 * <ul>
 *   <li>{@code "*"} — 采集返回值 toString()</li>
 *   <li>{@code "id,name"} — 仅提取指定字段（不采集 toString()）</li>
 *   <li>不传或 {@code "false"} — 不采集返回值</li>
 * </ul>
 *
 * <p>参数名在增强阶段（非热路径）预解析为索引，运行时只用索引取值，零开销。
 *
 * <p>不可变对象，线程安全。
 *
 * @see CaptureProcessor
 */
final class CaptureConfig {

  private static final Logger logger =
      DynamicInstrumentLogger.getLogger("capture-config");

  /** 配置键前缀 */
  static final String KEY_CAPTURE_ARGS = "capture_args";
  static final String KEY_CAPTURE_RETURN = "capture_return";
  static final String KEY_CAPTURE_MAX_LENGTH = "capture_max_length";

  /** 全部参数通配符 */
  private static final String WILDCARD_ALL = "*";

  /** 默认值序列化最大长度 */
  private static final int DEFAULT_MAX_LENGTH = 256;

  /** 无采集配置的单例 */
  static final CaptureConfig NONE = new CaptureConfig(
      new int[0], new String[0], /* captureReturn= */ false, new String[0], DEFAULT_MAX_LENGTH);

  /** 要采集的参数索引数组（预解析后） */
  private final int[] argIndices;

  /** 参数 Attribute key 名称数组（与 argIndices 一一对应），null 元素表示使用索引作为 key */
  private final String[] argNames;

  /** 是否采集返回值 */
  private final boolean captureReturn;

  /** 返回值要提取的字段名列表 */
  private final String[] returnFields;

  /** 值序列化最大长度 */
  private final int maxLength;

  private CaptureConfig(
      int[] argIndices, String[] argNames,
      boolean captureReturn, String[] returnFields, int maxLength) {
    this.argIndices = argIndices;
    this.argNames = argNames;
    this.captureReturn = captureReturn;
    this.returnFields = returnFields;
    this.maxLength = maxLength;
  }

  /**
   * 是否配置了任何采集（参数或返回值）
   *
   * @return 如果需要采集参数或返回值返回 true
   */
  boolean hasCaptureConfig() {
    return argIndices.length > 0 || captureReturn;
  }

  /** 获取要采集的参数索引数组 */
  int[] getArgIndices() {
    return argIndices;
  }

  /** 获取参数 Attribute key 名称数组 */
  String[] getArgNames() {
    return argNames;
  }

  /** 是否采集返回值 */
  boolean isCaptureReturn() {
    return captureReturn;
  }

  /** 获取返回值要提取的字段名列表 */
  String[] getReturnFields() {
    return returnFields;
  }

  /** 获取值序列化最大长度 */
  int getMaxLength() {
    return maxLength;
  }

  /**
   * 获取指定位置参数的 Attribute key 名称
   *
   * <p>如果参数名可用则返回参数名（如 "userId"），否则返回索引字符串（如 "0"）。
   *
   * @param position argIndices/argNames 数组中的位置
   * @return Attribute key 中使用的名称
   */
  String getArgKeyName(int position) {
    if (position >= 0 && position < argNames.length && argNames[position] != null) {
      return argNames[position];
    }
    if (position >= 0 && position < argIndices.length) {
      return String.valueOf(argIndices[position]);
    }
    return String.valueOf(position);
  }

  /**
   * 从 {@link InstrumentationRule#getConfig()} 中解析采集配置（不含参数名解析）
   *
   * <p>此方法适用于不需要参数名解析的场景（如仅按索引采集或仅采集返回值）。
   * 如果需要按参数名采集，请使用 {@link #resolve(Map, Method)} 方法。
   *
   * @param config 规则配置 Map
   * @return 采集配置，无采集时返回 {@link #NONE}
   */
  static CaptureConfig parse(Map<String, String> config) {
    if (config == null || config.isEmpty()) {
      return NONE;
    }
    return resolve(config, null);
  }

  /**
   * 从预解析的字段直接创建 CaptureConfig 实例（静态工厂方法）
   *
   * <p>用于跨 ClassLoader 重建场景：{@link TransformerManager} 在增强阶段预解析后，
   * 需要通过反射在 Bootstrap CL 中重建 CaptureConfig。此方法避免了重新解析配置字符串
   * 和反射获取 Method 的开销。
   *
   * @param argIndices 参数索引数组（已预解析）
   * @param argNames 参数名数组（已预解析，与 argIndices 一一对应）
   * @param captureReturn 是否采集返回值
   * @param returnFields 返回值字段名数组
   * @param maxLength 值序列化最大长度
   * @return CaptureConfig 实例
   */
  static CaptureConfig createResolved(
      int[] argIndices, String[] argNames,
      boolean captureReturn, String[] returnFields, int maxLength) {
    if ((argIndices == null || argIndices.length == 0) && !captureReturn) {
      return NONE;
    }
    return new CaptureConfig(
        argIndices != null ? argIndices : new int[0],
        argNames != null ? argNames : new String[0],
        captureReturn,
        returnFields != null ? returnFields : new String[0],
        maxLength > 0 ? maxLength : DEFAULT_MAX_LENGTH);
  }

  /**
   * 从配置中解析采集配置，并使用目标方法的反射信息解析参数名
   *
   * <p>这是完整的解析方法，在增强阶段（非热路径）调用。当用户使用参数名方式
   * 指定采集（如 {@code "userId,requestType"}）时，通过反射获取目标方法的参数名列表，
   * 建立「参数名 → 索引」映射。
   *
   * <p>参数名获取依赖编译选项（{@code -parameters} 或 {@code -g}），
   * 如果参数名不可用，将抛出 {@link IllegalArgumentException}。
   *
   * @param config 规则配置 Map
   * @param targetMethod 目标方法（可能为 null，此时按参数名采集将失败）
   * @return 采集配置
   * @throws IllegalArgumentException 如果参数名指定无效
   */
  static CaptureConfig resolve(Map<String, String> config, @Nullable Method targetMethod) {
    if (config == null || config.isEmpty()) {
      return NONE;
    }

    // 解析 capture_args
    String captureArgsStr = config.get(KEY_CAPTURE_ARGS);
    int[] argIndices = new int[0];
    String[] argNames = new String[0];

    if (captureArgsStr != null && !captureArgsStr.trim().isEmpty()) {
      CaptureArgsResult result = parseCaptureArgs(captureArgsStr.trim(), targetMethod);
      argIndices = result.indices;
      argNames = result.names;
    }

    // 解析 capture_return（统一语义：值即"采集什么"）
    // 支持：null/空/"false" → 不采集；"*" → 采集 toString()；"id,name" → 仅提取指定字段
    String captureReturnStr = config.getOrDefault(KEY_CAPTURE_RETURN, "").trim();
    boolean captureReturn = false;
    String[] returnFields = new String[0];

    if (!captureReturnStr.isEmpty() && !"false".equalsIgnoreCase(captureReturnStr)) {
      captureReturn = true;
      // "*" 仅采集 toString()，不提取字段
      if (!WILDCARD_ALL.equals(captureReturnStr)) {
        // 值本身就是要提取的字段列表（如 "id,name"），仅提取指定字段，不采集 toString()
        String[] parts = captureReturnStr.split(",");
        List<String> fields = new ArrayList<>();
        for (String part : parts) {
          String trimmed = part.trim();
          if (!trimmed.isEmpty()) {
            fields.add(trimmed);
          }
        }
        returnFields = fields.toArray(new String[0]);
      }
    }

    // 解析 capture_max_length
    int maxLength = DEFAULT_MAX_LENGTH;
    String maxLengthStr = config.get(KEY_CAPTURE_MAX_LENGTH);
    if (maxLengthStr != null && !maxLengthStr.trim().isEmpty()) {
      try {
        maxLength = Integer.parseInt(maxLengthStr.trim());
        if (maxLength <= 0) {
          maxLength = DEFAULT_MAX_LENGTH;
        }
      } catch (NumberFormatException e) {
        logger.log(Level.WARNING,
            "[CAPTURE-CONFIG] Invalid capture_max_length value: {0}, using default: {1}",
            new Object[] {maxLengthStr, DEFAULT_MAX_LENGTH});
      }
    }

    // 判断是否有采集配置
    if (argIndices.length == 0 && !captureReturn) {
      return NONE;
    }

    return new CaptureConfig(argIndices, argNames, captureReturn, returnFields, maxLength);
  }

  /**
   * 解析 capture_args 配置项
   *
   * <p>支持三种格式：
   * <ul>
   *   <li>{@code "*"} — 全部参数</li>
   *   <li>纯数字（{@code "0,2"}） — 按索引</li>
   *   <li>参数名（{@code "userId,requestType"}） — 按参数名（需要反射）</li>
   *   <li>混合（{@code "0,requestType"}） — 同时支持</li>
   * </ul>
   */
  private static CaptureArgsResult parseCaptureArgs(
      String captureArgsStr, @Nullable Method targetMethod) {

    // 构建参数名 → 索引映射（如果目标方法可用）
    Map<String, Integer> nameToIndex = new HashMap<>();
    Parameter[] params = null;
    if (targetMethod != null) {
      params = targetMethod.getParameters();
      for (int i = 0; i < params.length; i++) {
        if (params[i].isNamePresent()) {
          nameToIndex.put(params[i].getName(), i);
        }
      }
    }

    // 全部参数通配符
    if (WILDCARD_ALL.equals(captureArgsStr)) {
      if (params == null) {
        logger.log(Level.WARNING,
            "[CAPTURE-CONFIG] Wildcard '*' used but target method is unknown, "
                + "capturing nothing");
        return new CaptureArgsResult(new int[0], new String[0]);
      }
      int[] indices = new int[params.length];
      String[] names = new String[params.length];
      for (int i = 0; i < params.length; i++) {
        indices[i] = i;
        names[i] = params[i].isNamePresent() ? params[i].getName() : null;
      }
      return new CaptureArgsResult(indices, names);
    }

    // 逐项解析
    String[] parts = captureArgsStr.split(",");
    List<Integer> indicesList = new ArrayList<>();
    List<String> namesList = new ArrayList<>();

    for (String part : parts) {
      String trimmed = part.trim();
      if (trimmed.isEmpty()) {
        continue;
      }

      if (isNumeric(trimmed)) {
        // 按索引
        int idx = Integer.parseInt(trimmed);
        indicesList.add(idx);
        namesList.add(null); // Attribute key 使用索引
      } else {
        // 按参数名
        Integer idx = nameToIndex.get(trimmed);
        if (idx != null) {
          indicesList.add(idx);
          namesList.add(trimmed); // Attribute key 使用参数名
        } else {
          // 参数名找不到
          String availableNames = nameToIndex.isEmpty()
              ? "(parameter names not available, compile with -parameters)"
              : nameToIndex.keySet().toString();
          logger.log(Level.WARNING,
              "[CAPTURE-CONFIG] Parameter name ''{0}'' not found. "
                  + "Available: {1}. Try using index (0,1,2...) instead.",
              new Object[] {trimmed, availableNames});
          // 跳过该参数，不中断整个采集
        }
      }
    }

    int[] indices = new int[indicesList.size()];
    String[] names = new String[namesList.size()];
    for (int i = 0; i < indicesList.size(); i++) {
      indices[i] = indicesList.get(i);
      names[i] = namesList.get(i);
    }

    return new CaptureArgsResult(indices, names);
  }

  /**
   * 判断字符串是否为纯数字
   */
  private static boolean isNumeric(String str) {
    for (int i = 0; i < str.length(); i++) {
      if (!Character.isDigit(str.charAt(i))) {
        return false;
      }
    }
    return !str.isEmpty();
  }

  /** 参数采集解析结果 */
  private static final class CaptureArgsResult {
    final int[] indices;
    final String[] names;

    CaptureArgsResult(int[] indices, String[] names) {
      this.indices = indices;
      this.names = names;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CaptureConfig{");
    if (argIndices.length > 0) {
      sb.append("args=[");
      for (int i = 0; i < argIndices.length; i++) {
        if (i > 0) {
          sb.append(",");
        }
        String name = getArgKeyName(i);
        sb.append(name);
        if (argNames[i] != null) {
          sb.append("(idx=").append(argIndices[i]).append(")");
        }
      }
      sb.append("]");
    }
    if (captureReturn) {
      if (argIndices.length > 0) {
        sb.append(", ");
      }
      sb.append("return=true");
      if (returnFields.length > 0) {
        sb.append(", returnFields=[");
        for (int i = 0; i < returnFields.length; i++) {
          if (i > 0) {
            sb.append(",");
          }
          sb.append(returnFields[i]);
        }
        sb.append("]");
      }
    }
    sb.append(", maxLength=").append(maxLength).append("}");
    return sb.toString();
  }
}
