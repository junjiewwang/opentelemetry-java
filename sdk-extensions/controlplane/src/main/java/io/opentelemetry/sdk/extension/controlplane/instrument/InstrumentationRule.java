/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

// 注意：fromContext() 方法已移至 DynamicInstrumentExecutor，
// 使本类成为纯数据模型，不依赖 TaskExecutionContext，可安全注入 Bootstrap CL。

/**
 * 动态增强规则模型
 *
 * <p>定义一条增强规则的完整信息："对哪个类的哪个方法、插入什么类型的增强逻辑"。
 *
 * <p>由控制平面下发，TaskExecutor 解析后传递给 TransformerManager 执行。
 *
 * <p>不可变对象，使用 Builder 模式创建。
 */
public final class InstrumentationRule {

  private final String ruleId;
  private final String className;
  private final String methodName;
  @Nullable private final String methodDescriptor;
  @Nullable private final List<String> parameterTypes;
  private final InstrumentationType type;
  private final Map<String, String> config;
  @Nullable private final String spanName;

  private InstrumentationRule(Builder builder) {
    this.ruleId = Objects.requireNonNull(builder.ruleId, "ruleId is required");
    this.className = Objects.requireNonNull(builder.className, "className is required");
    this.methodName = Objects.requireNonNull(builder.methodName, "methodName is required");
    this.methodDescriptor = builder.methodDescriptor;
    this.parameterTypes = builder.parameterTypes != null
        ? Collections.unmodifiableList(builder.parameterTypes) : null;
    this.type = Objects.requireNonNull(builder.type, "type is required");
    this.config = Collections.unmodifiableMap(new HashMap<>(builder.config));
    this.spanName = builder.spanName;
  }

  /** 获取规则 ID（全局唯一） */
  public String getRuleId() {
    return ruleId;
  }

  /** 获取目标类的全限定名 */
  public String getClassName() {
    return className;
  }

  /** 获取目标方法名 */
  public String getMethodName() {
    return methodName;
  }

  /** 获取方法描述符（可选，用于精确匹配重载方法，JVM 格式） */
  @Nullable
  public String getMethodDescriptor() {
    return methodDescriptor;
  }

  /**
   * 获取参数类型列表（可选，用于匹配重载方法，Java 风格）
   *
   * <p>支持简单类名尾部匹配（如 "String" 匹配 "java.lang.String"）
   * 和全限定名精确匹配。空列表（size=0）表示匹配无参方法。
   * null 表示不指定（匹配所有同名方法）。
   *
   * <p>优先级：{@code methodDescriptor} > {@code parameterTypes} > 全部匹配。
   */
  @Nullable
  public List<String> getParameterTypes() {
    return parameterTypes;
  }

  /** 获取增强类型 */
  public InstrumentationType getType() {
    return type;
  }

  /** 获取额外配置 */
  public Map<String, String> getConfig() {
    return config;
  }

  /**
   * 获取自定义 Span 名称（仅 TRACE 类型有效）
   *
   * <p>如果未指定，默认使用 "ClassName.methodName" 格式。
   */
  @Nullable
  public String getSpanName() {
    return spanName;
  }

  /**
   * 获取 Span 名称，如果未自定义则使用默认格式
   *
   * @return Span 名称
   */
  public String getEffectiveSpanName() {
    if (spanName != null && !spanName.isEmpty()) {
      return spanName;
    }
    // 使用简短类名
    String simpleClassName = className;
    int lastDot = className.lastIndexOf('.');
    if (lastDot >= 0) {
      simpleClassName = className.substring(lastDot + 1);
    }
    return simpleClassName + "." + methodName;
  }

  /**
   * 根据规则的目标信息自动生成确定性的 ruleId
   *
   * <p>生成规则：{@code <SimpleClassName>.<methodName>[(<parameterTypes>)]_<type>}
   *
   * <p>示例：
   * <ul>
   *   <li>{@code UserService.handleRequest_trace} — 不指定参数类型（所有重载）</li>
   *   <li>{@code UserService.count()_trace} — 空参数列表（无参方法）</li>
   *   <li>{@code UserService.count(String,int)_metric} — 精确参数类型</li>
   * </ul>
   *
   * @param className 全限定类名
   * @param methodName 方法名
   * @param type 增强类型
   * @param parameterTypes 参数类型列表（null 表示不限制）
   * @return 自动生成的 ruleId
   */
  public static String generateRuleId(
      String className, String methodName, InstrumentationType type,
      @Nullable List<String> parameterTypes) {
    // 取简单类名
    String simpleClassName = className;
    int lastDot = className.lastIndexOf('.');
    if (lastDot >= 0) {
      simpleClassName = className.substring(lastDot + 1);
    }

    StringBuilder sb = new StringBuilder();
    sb.append(simpleClassName).append('.').append(methodName);

    // 如果指定了 parameterTypes，拼接参数签名
    if (parameterTypes != null) {
      sb.append('(').append(String.join(",", parameterTypes)).append(')');
    }

    sb.append('_').append(type.getValue());
    return sb.toString();
  }

  /**
   * 校验规则参数
   *
   * @return 校验失败的原因，null 表示校验通过
   */
  @Nullable
  public String validate() {
    if (ruleId == null || ruleId.isEmpty()) {
      return "ruleId is required";
    }
    if (className == null || className.isEmpty()) {
      return "className is required";
    }
    if (methodName == null || methodName.isEmpty()) {
      return "methodName is required";
    }
    if (type == null) {
      return "type is required";
    }
    return null;
  }

  @Override
  public String toString() {
    String paramInfo = "";
    if (methodDescriptor != null) {
      paramInfo = "(" + methodDescriptor + ")";
    } else if (parameterTypes != null) {
      paramInfo = "(" + String.join(",", parameterTypes) + ")";
    }
    return String.format(
        Locale.ROOT,
        "InstrumentationRule{ruleId='%s', type=%s, target='%s.%s'%s}",
        ruleId, type.getValue(), className, methodName, paramInfo);
  }

  // ===== Builder =====

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    @Nullable private String ruleId;
    @Nullable private String className;
    @Nullable private String methodName;
    @Nullable private String methodDescriptor;
    @Nullable private List<String> parameterTypes;
    @Nullable private InstrumentationType type;
    private Map<String, String> config = new HashMap<>();
    @Nullable private String spanName;

    // 使用 package-private 而非 private，避免编译器生成 synthetic $1 访问桥接类，
    // 该类不在 Bootstrap CL 注入列表中会导致 NoClassDefFoundError
    Builder() {}

    public Builder ruleId(String ruleId) {
      this.ruleId = ruleId;
      return this;
    }

    public Builder className(String className) {
      this.className = className;
      return this;
    }

    public Builder methodName(String methodName) {
      this.methodName = methodName;
      return this;
    }

    public Builder methodDescriptor(String methodDescriptor) {
      this.methodDescriptor = methodDescriptor != null && !methodDescriptor.isEmpty()
          ? methodDescriptor : null;
      return this;
    }

    /**
     * 设置参数类型列表（Java 风格，用户友好）
     *
     * <p>支持简单类名（如 "String"）和全限定名（如 "java.lang.String"）。
     * 空列表表示无参方法，null 表示不指定。
     *
     * @param parameterTypes 参数类型列表，null 表示不指定
     */
    public Builder parameterTypes(@Nullable List<String> parameterTypes) {
      this.parameterTypes = parameterTypes;
      return this;
    }

    public Builder type(InstrumentationType type) {
      this.type = type;
      return this;
    }

    public Builder config(Map<String, String> config) {
      this.config = config != null ? new HashMap<>(config) : new HashMap<>();
      return this;
    }

    public Builder spanName(String spanName) {
      this.spanName = spanName != null && !spanName.isEmpty() ? spanName : null;
      return this;
    }

    public InstrumentationRule build() {
      return new InstrumentationRule(this);
    }
  }
}
