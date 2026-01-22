/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.client;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 控制平面统一日志门面
 *
 * <p>统一日志前缀、级别、格式，支持结构化日志和节流/采样。
 *
 * <p>设计原则：
 * <ul>
 *   <li><b>固定事件集合</b>：每种操作有明确的事件名（如 POLL_REQUEST、POLL_RESPONSE）
 *   <li><b>固定字段</b>：agentId、operation、timeoutMs、durationMs、httpCode、hasChanges、taskCount、errorCode
 *   <li><b>节流/采样</b>：对高频事件（如正常 POLL_RESPONSE）进行采样，避免刷屏
 *   <li><b>不打印大 payload</b>：默认不输出请求/响应体，除非 FINEST 级别
 * </ul>
 *
 * <p>日志事件命名规范：
 * <ul>
 *   <li>CONTROLPLANE_POLL_REQUEST - 发送轮询请求
 *   <li>CONTROLPLANE_POLL_RESPONSE - 收到轮询响应
 *   <li>CONTROLPLANE_POLL_FAST_RETURN - 服务端过快返回警告
 *   <li>CONTROLPLANE_POLL_ERROR - 轮询错误
 *   <li>CONTROLPLANE_TASK_RECEIVED - 收到任务
 *   <li>CONTROLPLANE_TASK_RESULT_REPORT - 上报任务结果
 *   <li>CONTROLPLANE_CONFIG_RECEIVED - 收到配置
 *   <li>CONTROLPLANE_PARSE_ERROR - 解析错误
 *   <li>CONTROLPLANE_HEALTH_WARN - 健康检查警告
 * </ul>
 */
public final class ControlPlaneLogger {

  private static final Logger logger = Logger.getLogger("io.opentelemetry.controlplane");
  private static final String PREFIX = "[CONTROL-PLANE]";

  /** 服务端响应过快的阈值（毫秒） */
  private static final long QUICK_RESPONSE_THRESHOLD_MS = 5_000;

  /** 期望的最小超时时间（毫秒），低于此值不输出过快警告 */
  private static final long MIN_EXPECTED_TIMEOUT_MS = 30_000;

  /** 正常 POLL_RESPONSE 的采样间隔（毫秒） */
  private static final long POLL_RESPONSE_SAMPLE_INTERVAL_MS = 60_000;

  /** 上次输出 POLL_RESPONSE 日志的时间 */
  private final AtomicLong lastPollResponseLogTime = new AtomicLong(0);

  private final boolean debugEnabled;

  /**
   * 创建日志门面
   *
   * @param debugEnabled 是否启用调试日志
   */
  public ControlPlaneLogger(boolean debugEnabled) {
    this.debugEnabled = debugEnabled;
  }

  // ===== Poll 相关日志 =====

  /**
   * 记录轮询请求
   *
   * @param agentId Agent ID
   * @param timeoutMs 超时时间（毫秒）
   * @param configVersion 当前配置版本
   */
  public void logPollRequest(String agentId, long timeoutMs, @Nullable String configVersion) {
    if (debugEnabled && logger.isLoggable(Level.INFO)) {
      logger.log(
          Level.INFO,
          "{0} [POLL_REQUEST] agentId={1}, timeoutMs={2}, configVersion={3}",
          new Object[] {PREFIX, agentId, timeoutMs, configVersion != null ? configVersion : ""});
    }
  }

  /**
   * 记录轮询响应
   *
   * @param durationMs 实际耗时（毫秒）
   * @param expectedTimeoutMs 期望超时时间（毫秒）
   * @param hasChanges 是否有变更
   * @param taskCount 任务数量
   * @param success 是否成功
   */
  public void logPollResponse(
      long durationMs, long expectedTimeoutMs, boolean hasChanges, int taskCount, boolean success) {

    // 检查是否服务端响应过快
    if (durationMs < QUICK_RESPONSE_THRESHOLD_MS && expectedTimeoutMs >= MIN_EXPECTED_TIMEOUT_MS) {
      logger.log(
          Level.WARNING,
          "{0} [POLL_FAST_RETURN] Server responded too quickly! durationMs={1}, expectedTimeoutMs={2}. "
              + "This may indicate server is not honoring timeout_millis parameter.",
          new Object[] {PREFIX, durationMs, expectedTimeoutMs});
    }

    // 对正常响应进行采样，避免刷屏
    Level level = Level.FINE;
    if (hasChanges || !success) {
      // 有变更或失败时，使用 INFO 级别
      level = Level.INFO;
    } else {
      // 正常无变更响应，采样输出
      long now = System.currentTimeMillis();
      long lastLogTime = lastPollResponseLogTime.get();
      if (now - lastLogTime < POLL_RESPONSE_SAMPLE_INTERVAL_MS) {
        // 在采样间隔内，跳过日志
        return;
      }
      lastPollResponseLogTime.compareAndSet(lastLogTime, now);
    }

    if (logger.isLoggable(level)) {
      logger.log(
          level,
          "{0} [POLL_RESPONSE] durationMs={1}, expectedTimeoutMs={2}, hasChanges={3}, taskCount={4}, success={5}",
          new Object[] {PREFIX, durationMs, expectedTimeoutMs, hasChanges, taskCount, success});
    }
  }

  /**
   * 记录轮询错误
   *
   * @param durationMs 实际耗时（毫秒）
   * @param error 错误信息
   * @param httpCode HTTP 状态码（如果适用）
   */
  public void logPollError(long durationMs, String error, int httpCode) {
    logger.log(
        Level.WARNING,
        "{0} [POLL_ERROR] durationMs={1}, httpCode={2}, error={3}",
        new Object[] {PREFIX, durationMs, httpCode, error});
  }

  // ===== Config 相关日志 =====

  /**
   * 记录收到配置
   *
   * @param version 配置版本
   * @param etag 配置 ETag
   * @param hasChanges 是否有变更
   */
  public void logConfigReceived(String version, String etag, boolean hasChanges) {
    Level level = hasChanges ? Level.INFO : Level.FINE;
    if (logger.isLoggable(level)) {
      logger.log(
          level,
          "{0} [CONFIG_RECEIVED] version={1}, etag={2}, hasChanges={3}",
          new Object[] {PREFIX, version, etag, hasChanges});
    }
  }

  // ===== Task 相关日志 =====

  /**
   * 记录收到任务
   *
   * @param taskId 任务 ID
   * @param taskType 任务类型
   * @param priority 优先级
   */
  public void logTaskReceived(String taskId, String taskType, int priority) {
    logger.log(
        Level.INFO,
        "{0} [TASK_RECEIVED] taskId={1}, type={2}, priority={3}",
        new Object[] {PREFIX, taskId, taskType, priority});
  }

  /**
   * 记录任务结果上报
   *
   * @param taskId 任务 ID
   * @param status 任务状态
   * @param errorCode 错误码（如果适用）
   */
  public void logTaskResultReport(String taskId, String status, @Nullable String errorCode) {
    Level level = "SUCCESS".equals(status) ? Level.FINE : Level.INFO;
    if (logger.isLoggable(level)) {
      logger.log(
          level,
          "{0} [TASK_RESULT_REPORT] taskId={1}, status={2}, errorCode={3}",
          new Object[] {PREFIX, taskId, status, errorCode != null ? errorCode : ""});
    }
  }

  // ===== 健康检查日志 =====

  /**
   * 记录健康检查警告
   *
   * @param healthState 健康状态
   * @deprecated 已废弃，使用 {@link #logExportMetricsWarning(double)}
   */
  @Deprecated
  public void logHealthWarning(String healthState) {
    logger.log(
        Level.FINE,
        "{0} [HEALTH_WARN] OTLP not healthy, state={1}",
        new Object[] {PREFIX, healthState});
  }

  /**
   * 记录导出指标警告
   *
   * @param successRate 成功率
   */
  public void logExportMetricsWarning(double successRate) {
    logger.log(
        Level.FINE,
        "{0} [EXPORT_METRICS_WARN] OTLP export success rate is low, rate={1}",
        new Object[] {PREFIX, String.format(java.util.Locale.ROOT, "%.1f%%", successRate * 100)});
  }

  // ===== 解析错误日志 =====

  /**
   * 记录解析错误
   *
   * @param messageType 消息类型
   * @param error 错误信息
   */
  public void logParseError(String messageType, String error) {
    logger.log(
        Level.WARNING,
        "{0} [PARSE_ERROR] messageType={1}, error={2}",
        new Object[] {PREFIX, messageType, error});
  }

  /**
   * 记录解析错误（带异常）
   *
   * @param messageType 消息类型
   * @param e 异常
   */
  public void logParseError(String messageType, Exception e) {
    logger.log(
        Level.WARNING,
        "{0} [PARSE_ERROR] messageType={1}, error={2}",
        new Object[] {PREFIX, messageType, e.getMessage()});
  }

  // ===== 传输层日志 =====

  /**
   * 记录传输请求
   *
   * @param operation 操作类型
   * @param url 请求 URL
   */
  public void logTransportRequest(String operation, String url) {
    if (debugEnabled && logger.isLoggable(Level.FINE)) {
      logger.log(
          Level.FINE,
          "{0} [TRANSPORT_REQUEST] operation={1}, url={2}",
          new Object[] {PREFIX, operation, url});
    }
  }

  /**
   * 记录传输错误
   *
   * @param operation 操作类型
   * @param error 错误信息
   * @param httpCode HTTP 状态码（如果适用）
   */
  public void logTransportError(String operation, String error, int httpCode) {
    logger.log(
        Level.WARNING,
        "{0} [TRANSPORT_ERROR] operation={1}, httpCode={2}, error={3}",
        new Object[] {PREFIX, operation, httpCode, error});
  }

  // ===== 生命周期日志 =====

  /**
   * 记录服务初始化
   *
   * @param transportType 传输类型
   * @param baseUrl 基础 URL
   * @param hasAuth 是否有鉴权
   */
  public void logServiceInitialized(String transportType, String baseUrl, boolean hasAuth) {
    logger.log(
        Level.INFO,
        "{0} [SERVICE_INITIALIZED] transportType={1}, baseUrl={2}, hasAuth={3}",
        new Object[] {PREFIX, transportType, baseUrl, hasAuth});
  }

  /**
   * 记录服务关闭
   */
  public void logServiceClosed() {
    logger.log(Level.INFO, "{0} [SERVICE_CLOSED]", PREFIX);
  }
}
