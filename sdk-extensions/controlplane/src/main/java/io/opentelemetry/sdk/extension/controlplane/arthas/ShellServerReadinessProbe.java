/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * ShellServer 本地交互服务就绪探测。
 *
 * <p>Arthas 的 {@code ShellServerImpl.closed} 初值为 {@code true}，只有全部 TermServer
 * listen 完成后才被置为 {@code false}（{@code TermServerListenHandler} 在计数归零且无失败时
 * 调用 {@code setClosed(false)}）。因此 {@code !closed} 精确对应「本地交互服务可输出」。
 *
 * <p>注意 {@code isBind()} 不能作为就绪信号：{@code isBindRef} 在 {@code bind()} 开头即置
 * {@code true}，远早于 {@code shellServer.listen()} 完成。
 *
 * <p>设计约束：
 * <ul>
 *   <li>反射失败、字段缺失、实例为 null 一律保守返回 false，绝不把「读不到」当成 ready</li>
 *   <li>独立缓存反射句柄与日志去重标记，不与 TunnelClient 反射共用</li>
 *   <li>只有字段解析成功才算初始化完成；ShellServer 尚未创建时下次轮询继续重试</li>
 *   <li>不依赖 {@code ArthasBootstrap.isRunning()}（其基于 isBind，同样过早为 true）</li>
 * </ul>
 */
public interface ShellServerReadinessProbe {

  /** 总开关，默认开启；关闭后 {@link #isListening()} 恒 true，等价于回滚到旧的三维判定。 */
  String PROP_ENABLED = "otel.controlplane.arthas.shellserver.probe.enabled";

  /** 探测 ShellServer 是否已完成 listen。 */
  boolean isListening();

  /** 基于 Arthas 实例反射 {@code getShellServer()} + 私有状态字段的实现。 */
  final class Reflective implements ShellServerReadinessProbe {


    private static final Logger logger =
        Logger.getLogger(ShellServerReadinessProbe.Reflective.class.getName());

    /** 字段名候选：{@code closed} 为真值需取反；{@code listening} 为真值即就绪。 */
    private static final String[] FIELD_CANDIDATES = {"closed", "listening"};

    private final ArthasBootstrap arthasBootstrap;
    private final ArthasLifecycleManager.StartupLogCollector startupLogCollector;

    @Nullable private Method getShellServerMethod;
    @Nullable private Field stateField;
    private boolean negateField;
    private boolean failureLogged;

    Reflective(ArthasBootstrap arthasBootstrap,
        ArthasLifecycleManager.StartupLogCollector startupLogCollector) {
      this.arthasBootstrap = arthasBootstrap;
      this.startupLogCollector = startupLogCollector;
    }

    @Override
    public boolean isListening() {
      if (!Boolean.parseBoolean(System.getProperty(PROP_ENABLED, "true"))) {
        // 显式关闭探测时返回 true，等价于「不追加这一层条件」，用于回滚
        return true;
      }

      Object bootstrapInstance = arthasBootstrap.getBootstrapInstance();
      if (bootstrapInstance == null) {
        return false;
      }

      try {
        if (stateField == null) {
          initReflection(bootstrapInstance);
        }
        Field field = stateField;
        Method getter = getShellServerMethod;
        if (field == null || getter == null) {
          return false;
        }
        Object shellServer = getter.invoke(bootstrapInstance);
        if (shellServer == null) {
          return false;
        }
        boolean value = field.getBoolean(shellServer);
        // closed 字段为 true 表示未就绪，需取反；listening 字段为 true 即就绪
        return negateField != value;
      } catch (ReflectiveOperationException | RuntimeException e) {
        logFailureOnce("Failed to probe ShellServer readiness: " + e);
        logger.log(Level.FINE, "[ShellServerProbe] Probe failed: {0}", e.getMessage());
        return false;
      }
    }

    /**
     * 懒加载反射句柄。
     *
     * <p>只有 {@code stateField} 成功赋值才算完成；否则下次轮询重试，避免 ShellServer
     * 尚未创建时永久卡在 false。
     */
    private void initReflection(Object bootstrapInstance) {
      try {
        if (getShellServerMethod == null) {
          getShellServerMethod = bootstrapInstance.getClass().getMethod("getShellServer");
        }
        Object shellServer = getShellServerMethod.invoke(bootstrapInstance);
        if (shellServer == null) {
          return;
        }

        Class<?> serverClass = shellServer.getClass();
        for (String fieldName : FIELD_CANDIDATES) {
          Field field;
          try {
            field = serverClass.getDeclaredField(fieldName);
          } catch (NoSuchFieldException e) {
            continue;
          }
          if (field.getType() != boolean.class) {
            continue;
          }
          field.setAccessible(true);
          stateField = field;
          negateField = "closed".equals(fieldName);
          startupLogCollector.addLog("INFO",
              "ShellServer readiness reflection initialized: field=" + fieldName);
          return;
        }
        logFailureOnce("ShellServer readiness field not found (tried closed, listening)");
      } catch (ReflectiveOperationException | RuntimeException e) {
        logFailureOnce("Failed to initialize ShellServer readiness reflection: " + e);
      }
    }

    private void logFailureOnce(String message) {
      if (failureLogged) {
        return;
      }
      failureLogged = true;
      startupLogCollector.addLog("WARN", message);
      logger.log(Level.WARNING, "[ShellServerProbe] {0}", message);
    }
  }
}
