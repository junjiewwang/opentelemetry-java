/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane;

import io.opentelemetry.sdk.extension.controlplane.core.InstrumentationProvider;
import java.lang.instrument.Instrumentation;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Instrumentation 实例持有者
 *
 * <p>【兼容层】历史上本类负责保存/获取 Instrumentation。现在平台已引入 {@link InstrumentationProvider} 作为
 * <b>唯一的策略入口</b>（包含多来源解析、能力判断与诊断信息）。
 *
 * <p>本类保留仅用于兼容旧调用方：
 * <ul>
 *   <li>{@link #set(Instrumentation)}：将 Instrumentation 注入到 {@link InstrumentationProvider}</li>
 *   <li>{@link #get()} / {@link #isAvailable()}：从 {@link InstrumentationProvider} 读取</li>
 * </ul>
 *
 * <p>重要：本类不再承载任何“自动获取/兜底策略”（例如 ByteBuddy install、反射读取 bootstrap holder）。
 * 这些策略统一由 {@link InstrumentationProvider} 管理，避免双中心与重复逻辑。
 */
public final class InstrumentationHolder {

  private static final Logger logger = Logger.getLogger(InstrumentationHolder.class.getName());

  private InstrumentationHolder() {}

  /**
   * 设置 Instrumentation 实例
   *
   * <p>应该在 Agent 的 premain 方法中尽早调用。
   *
   * @param instrumentation Instrumentation 实例
   */
  public static void set(@Nullable Instrumentation instrumentation) {
    if (instrumentation == null) {
      logger.log(Level.WARNING, "Attempted to set null Instrumentation");
      return;
    }

    InstrumentationProvider.getInstance().setInstrumentation(instrumentation);
  }

  /**
   * 获取 Instrumentation 实例
   *
   * <p>兼容方法，实际由 {@link InstrumentationProvider} 负责解析与诊断。
   *
   * @return Instrumentation 实例，如果无法获取则返回 null
   */
  @Nullable
  public static Instrumentation get() {
    return InstrumentationProvider.getInstance().getInstrumentation();
  }

  /**
   * 检查 Instrumentation 是否可用
   *
   * @return 是否可用
   */
  public static boolean isAvailable() {
    return InstrumentationProvider.getInstance().isAvailable();
  }

  /**
   * 获取 InstrumentationProvider 实例
   *
   * @return InstrumentationProvider 实例
   */
  public static InstrumentationProvider getProvider() {
    return InstrumentationProvider.getInstance();
  }

  /**
   * 清除 Instrumentation 实例（仅用于测试）
   */
  static void clear() {
    InstrumentationProvider.getInstance().setInstrumentation(null);
    InstrumentationProvider.getInstance().invalidateCache();
  }
}
