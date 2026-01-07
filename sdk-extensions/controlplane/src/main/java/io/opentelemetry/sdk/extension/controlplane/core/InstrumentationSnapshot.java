/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core;

import java.lang.instrument.Instrumentation;
import javax.annotation.Nullable;

/**
 * Instrumentation 快照
 *
 * <p>封装 Instrumentation 实例及其能力信息和诊断信息。
 * 这是一个不可变的值对象，用于在组件间传递 Instrumentation 的完整状态。
 *
 * <p>设计目的：
 * <ul>
 *   <li>将 Instrumentation 的获取逻辑与使用逻辑解耦</li>
 *   <li>提供统一的能力检查接口</li>
 *   <li>携带诊断信息，便于问题排查</li>
 * </ul>
 */
public final class InstrumentationSnapshot {

  /** Instrumentation 来源 */
  public enum Source {
    /** 通过 Agent premain 直接设置 */
    AGENT_PREMAIN,
    /** 从 javaagent-bootstrap 模块的 InstrumentationHolder 获取 */
    JAVAAGENT_BOOTSTRAP,
    /** 通过 ByteBuddy Agent 动态获取 */
    BYTEBUDDY_AGENT,
    /** 通过 VirtualMachine attach 获取 */
    VM_ATTACH,
    /** 未知来源（兜底） */
    UNKNOWN,
    /** 未获取到 */
    NOT_AVAILABLE
  }

  @Nullable private final Instrumentation instrumentation;
  private final Source source;
  private final boolean supportsRetransform;
  private final boolean supportsRedefine;
  private final boolean supportsNativeMethodPrefix;
  private final String diagnosticMessage;

  private InstrumentationSnapshot(Builder builder) {
    this.instrumentation = builder.instrumentation;
    this.source = builder.source;
    this.supportsRetransform = builder.supportsRetransform;
    this.supportsRedefine = builder.supportsRedefine;
    this.supportsNativeMethodPrefix = builder.supportsNativeMethodPrefix;
    this.diagnosticMessage = builder.diagnosticMessage;
  }

  /**
   * 创建一个表示"不可用"的快照
   *
   * @param diagnosticMessage 诊断信息，说明为何不可用
   * @return 不可用的快照
   */
  public static InstrumentationSnapshot notAvailable(String diagnosticMessage) {
    return new Builder()
        .instrumentation(null)
        .source(Source.NOT_AVAILABLE)
        .diagnosticMessage(diagnosticMessage)
        .build();
  }

  /**
   * 从 Instrumentation 实例创建快照
   *
   * @param instrumentation Instrumentation 实例
   * @param source 来源
   * @return 快照
   */
  public static InstrumentationSnapshot of(Instrumentation instrumentation, Source source) {
    return new Builder()
        .instrumentation(instrumentation)
        .source(source)
        .supportsRetransform(instrumentation.isRetransformClassesSupported())
        .supportsRedefine(instrumentation.isRedefineClassesSupported())
        .supportsNativeMethodPrefix(instrumentation.isNativeMethodPrefixSupported())
        .diagnosticMessage("Instrumentation available from " + source)
        .build();
  }

  /**
   * 创建构建器
   *
   * @return 构建器
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * 获取 Instrumentation 实例
   *
   * @return Instrumentation 实例，可能为 null
   */
  @Nullable
  public Instrumentation getInstrumentation() {
    return instrumentation;
  }

  /**
   * 获取来源
   *
   * @return 来源
   */
  public Source getSource() {
    return source;
  }

  /**
   * 是否可用
   *
   * @return 是否可用
   */
  public boolean isAvailable() {
    return instrumentation != null;
  }

  /**
   * 是否支持 retransform
   *
   * <p>retransform 是 Arthas trace/watch/stack 等命令的核心能力依赖。
   *
   * @return 是否支持
   */
  public boolean supportsRetransform() {
    return supportsRetransform;
  }

  /**
   * 是否支持 redefine
   *
   * @return 是否支持
   */
  public boolean supportsRedefine() {
    return supportsRedefine;
  }

  /**
   * 是否支持 native method prefix
   *
   * @return 是否支持
   */
  public boolean supportsNativeMethodPrefix() {
    return supportsNativeMethodPrefix;
  }

  /**
   * 获取诊断信息
   *
   * <p>包含 Instrumentation 获取过程的详细信息，用于问题排查。
   *
   * @return 诊断信息
   */
  public String getDiagnosticMessage() {
    return diagnosticMessage;
  }

  /**
   * 是否具备字节码增强能力
   *
   * <p>同时满足以下条件才具备增强能力：
   * <ul>
   *   <li>Instrumentation 可用</li>
   *   <li>支持 retransform</li>
   * </ul>
   *
   * @return 是否具备增强能力
   */
  public boolean hasEnhancementCapability() {
    return isAvailable() && supportsRetransform;
  }

  /**
   * 生成简短的状态摘要
   *
   * @return 状态摘要
   */
  public String toSummary() {
    if (!isAvailable()) {
      return "Instrumentation NOT_AVAILABLE: " + diagnosticMessage;
    }
    return String.format(
        "Instrumentation available (source=%s, retransform=%s, redefine=%s)",
        source, supportsRetransform, supportsRedefine);
  }

  @Override
  public String toString() {
    return "InstrumentationSnapshot{"
        + "available=" + isAvailable()
        + ", source=" + source
        + ", supportsRetransform=" + supportsRetransform
        + ", supportsRedefine=" + supportsRedefine
        + ", diagnostic='" + diagnosticMessage + '\''
        + '}';
  }

  /** 构建器 */
  public static final class Builder {
    @Nullable private Instrumentation instrumentation;
    private Source source = Source.NOT_AVAILABLE;
    private boolean supportsRetransform = false;
    private boolean supportsRedefine = false;
    private boolean supportsNativeMethodPrefix = false;
    private String diagnosticMessage = "";

    private Builder() {}

    public Builder instrumentation(@Nullable Instrumentation instrumentation) {
      this.instrumentation = instrumentation;
      return this;
    }

    public Builder source(Source source) {
      this.source = source;
      return this;
    }

    public Builder supportsRetransform(boolean supportsRetransform) {
      this.supportsRetransform = supportsRetransform;
      return this;
    }

    public Builder supportsRedefine(boolean supportsRedefine) {
      this.supportsRedefine = supportsRedefine;
      return this;
    }

    public Builder supportsNativeMethodPrefix(boolean supportsNativeMethodPrefix) {
      this.supportsNativeMethodPrefix = supportsNativeMethodPrefix;
      return this;
    }

    public Builder diagnosticMessage(String diagnosticMessage) {
      this.diagnosticMessage = diagnosticMessage;
      return this;
    }

    public InstrumentationSnapshot build() {
      return new InstrumentationSnapshot(this);
    }
  }
}
