/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.extension.controlplane.task.TaskExecutionLogger.TaskContext;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * TaskExecutionLogger 单元测试
 */
class TaskExecutionLoggerTest {

  private TaskExecutionLogger taskLogger;
  private TestLogHandler logHandler;
  private Logger logger;

  @BeforeEach
  void setUp() {
    taskLogger = TaskExecutionLogger.getInstance();
    taskLogger.setEnabled(true);
    taskLogger.setLogCommandDetails(true);

    // 添加测试日志处理器
    logger = Logger.getLogger(TaskExecutionLogger.class.getName());
    logHandler = new TestLogHandler();
    logger.addHandler(logHandler);
    logger.setLevel(Level.ALL);
  }

  @AfterEach
  void tearDown() {
    logger.removeHandler(logHandler);
    // 清理所有活跃任务
    taskLogger.clearContext("test-task-1");
    taskLogger.clearContext("test-task-2");
  }

  @Test
  void logTaskReceived_shouldLogInfoLevel() {
    // When
    TaskContext context = taskLogger.logTaskReceived(
        "test-task-1",
        TaskExecutionLogger.TASK_TYPE_ARTHAS_SESSION,
        TaskExecutionLogger.SOURCE_TUNNEL_SERVER);

    // Then
    assertThat(context).isNotNull();
    assertThat(context.getTaskId()).isEqualTo("test-task-1");
    assertThat(context.getTaskType()).isEqualTo(TaskExecutionLogger.TASK_TYPE_ARTHAS_SESSION);
    assertThat(context.getSource()).isEqualTo(TaskExecutionLogger.SOURCE_TUNNEL_SERVER);
    assertThat(context.getReceivedAt()).isNotNull();

    // 验证日志
    assertThat(logHandler.getLastRecord()).isNotNull();
    assertThat(logHandler.getLastRecord().getLevel()).isEqualTo(Level.INFO);
    assertThat(logHandler.getFormattedMessage()).contains("[TASK-RECEIVED]");
  }

  @Test
  void logTaskReceived_withDetails_shouldLogFineLevel() {
    // Given
    logger.setLevel(Level.FINE);
    Map<String, Object> details = TaskExecutionLogger.details()
        .put("userId", "admin")
        .put("cols", 120)
        .put("rows", 40)
        .build();

    // When
    taskLogger.logTaskReceived(
        "test-task-1",
        TaskExecutionLogger.TASK_TYPE_ARTHAS_SESSION,
        TaskExecutionLogger.SOURCE_TUNNEL_SERVER,
        details);

    // Then - 应该有两条日志（INFO + FINE）
    assertThat(logHandler.getRecordCount()).isGreaterThanOrEqualTo(1);
  }

  @Test
  void logTaskStarted_shouldRecordQueueTime() throws InterruptedException {
    // Given
    taskLogger.logTaskReceived(
        "test-task-1",
        TaskExecutionLogger.TASK_TYPE_ARTHAS_COMMAND,
        TaskExecutionLogger.SOURCE_TERMINAL_INPUT);

    // 模拟排队时间
    Thread.sleep(50);

    // When
    taskLogger.logTaskStarted("test-task-1", "test-executor");

    // Then
    TaskContext context = taskLogger.getContext("test-task-1");
    assertThat(context).isNotNull();
    assertThat(context.getQueueTimeMs()).isGreaterThanOrEqualTo(50);
    assertThat(context.getStartedAt()).isNotNull();
  }

  @Test
  void logTaskCompleted_shouldRecordExecutionTime() throws InterruptedException {
    // Given
    taskLogger.logTaskReceived(
        "test-task-1",
        TaskExecutionLogger.TASK_TYPE_CONFIG_POLL,
        TaskExecutionLogger.SOURCE_SCHEDULER);
    taskLogger.logTaskStarted("test-task-1", "scheduler-thread");

    // 模拟执行时间
    Thread.sleep(50);

    // When
    taskLogger.logTaskCompleted("test-task-1", "success", 1024);

    // Then - 任务上下文应该被清理
    assertThat(taskLogger.getContext("test-task-1")).isNull();

    // 验证日志 - 检查最后一条日志包含 COMPLETED（INFO 或 FINE 级别）
    String formattedMsg = logHandler.getFormattedMessage();
    assertThat(formattedMsg).contains("[TASK-COMPLETED]");
  }

  @Test
  void logTaskFailed_shouldLogSevereLevel() {
    // Given
    taskLogger.logTaskReceived(
        "test-task-1",
        TaskExecutionLogger.TASK_TYPE_TASK_POLL,
        TaskExecutionLogger.SOURCE_SCHEDULER);
    taskLogger.logTaskStarted("test-task-1");

    // When
    taskLogger.logTaskFailed(
        "test-task-1",
        TaskExecutionLogger.ERROR_NETWORK,
        "Connection refused");

    // Then
    assertThat(taskLogger.getContext("test-task-1")).isNull();
    assertThat(logHandler.getLastRecord().getLevel()).isEqualTo(Level.SEVERE);
    assertThat(logHandler.getFormattedMessage()).contains("[TASK-FAILED]");
    assertThat(logHandler.getFormattedMessage()).contains("NETWORK_ERROR");
  }

  @Test
  void logTaskFailed_withNullMessage_shouldUseDefaultMessage() {
    // Given
    taskLogger.logTaskReceived(
        "test-task-1",
        TaskExecutionLogger.TASK_TYPE_CLEANUP,
        TaskExecutionLogger.SOURCE_SCHEDULER);

    // When
    taskLogger.logTaskFailed("test-task-1", TaskExecutionLogger.ERROR_INTERNAL, null);

    // Then
    assertThat(logHandler.getFormattedMessage()).contains("Unknown error");
  }

  @Test
  void logTaskTimeout_shouldLogWarningLevel() {
    // Given
    taskLogger.logTaskReceived(
        "test-task-1",
        TaskExecutionLogger.TASK_TYPE_ARTHAS_COMMAND,
        TaskExecutionLogger.SOURCE_TERMINAL_INPUT);

    // When
    taskLogger.logTaskTimeout("test-task-1", Duration.ofSeconds(30));

    // Then
    assertThat(logHandler.getLastRecord().getLevel()).isEqualTo(Level.WARNING);
    assertThat(logHandler.getFormattedMessage()).contains("[TASK-TIMEOUT]");
    assertThat(logHandler.getFormattedMessage()).contains("30");
  }

  @Test
  void logTaskCancelled_shouldLogInfoLevel() {
    // Given
    taskLogger.logTaskReceived(
        "test-task-1",
        TaskExecutionLogger.TASK_TYPE_ARTHAS_SESSION,
        TaskExecutionLogger.SOURCE_TUNNEL_SERVER);

    // When
    taskLogger.logTaskCancelled("test-task-1", "user_request");

    // Then
    assertThat(logHandler.getLastRecord().getLevel()).isEqualTo(Level.INFO);
    assertThat(logHandler.getFormattedMessage()).contains("[TASK-CANCELLED]");
    assertThat(logHandler.getFormattedMessage()).contains("user_request");
  }

  @Test
  void logTaskProgress_shouldLogFineLevel() {
    // Given
    logger.setLevel(Level.FINE);
    taskLogger.logTaskReceived(
        "test-task-1",
        TaskExecutionLogger.TASK_TYPE_ARTHAS_COMMAND,
        TaskExecutionLogger.SOURCE_TERMINAL_INPUT);
    taskLogger.logTaskStarted("test-task-1");

    // When
    taskLogger.logTaskProgress("test-task-1", "executing", "Running Arthas command");

    // Then
    assertThat(logHandler.getLastRecord().getLevel()).isEqualTo(Level.FINE);
    assertThat(logHandler.getFormattedMessage()).contains("[TASK-PROGRESS]");
    assertThat(logHandler.getFormattedMessage()).contains("executing");
  }

  @Test
  void logTaskWarning_shouldLogWarningLevel() {
    // When
    taskLogger.logTaskWarning("test-task-1", "SLOW_EXECUTION", "Execution taking longer than expected");

    // Then
    assertThat(logHandler.getLastRecord().getLevel()).isEqualTo(Level.WARNING);
    assertThat(logHandler.getFormattedMessage()).contains("SLOW_EXECUTION");
  }

  @Test
  void taskContext_isTimedOut_shouldReturnCorrectValue() throws InterruptedException {
    // Given
    TaskContext context = taskLogger.logTaskReceived(
        "test-task-1",
        TaskExecutionLogger.TASK_TYPE_ARTHAS_COMMAND,
        TaskExecutionLogger.SOURCE_TERMINAL_INPUT);
    taskLogger.logTaskConfig("test-task-1", "HIGH", Duration.ofMillis(50), null);

    // When - 等待超时
    Thread.sleep(100);

    // Then
    assertThat(context.isTimedOut()).isTrue();
  }

  @Test
  void detailsBuilder_shouldBuildCorrectMap() {
    // When
    Map<String, Object> details = TaskExecutionLogger.details()
        .put("stringKey", "stringValue")
        .put("longKey", 12345L)
        .put("intKey", 42)
        .put("boolKey", true)
        .putObject("objectKey", new Object())
        .put("nullKey", null) // 应该被忽略
        .build();

    // Then
    assertThat(details).hasSize(5);
    assertThat(details.get("stringKey")).isEqualTo("stringValue");
    assertThat(details.get("longKey")).isEqualTo(12345L);
    assertThat(details.get("intKey")).isEqualTo(42);
    assertThat(details.get("boolKey")).isEqualTo(true);
    assertThat(details.get("objectKey")).isNotNull();
    assertThat(details.containsKey("nullKey")).isFalse();
  }

  @Test
  void getActiveTaskCount_shouldReturnCorrectCount() {
    // Given
    taskLogger.logTaskReceived("task-1", TaskExecutionLogger.TASK_TYPE_CONFIG_POLL, TaskExecutionLogger.SOURCE_SCHEDULER);
    taskLogger.logTaskReceived("task-2", TaskExecutionLogger.TASK_TYPE_TASK_POLL, TaskExecutionLogger.SOURCE_SCHEDULER);

    // When
    int count = taskLogger.getActiveTaskCount();

    // Then
    assertThat(count).isGreaterThanOrEqualTo(2);

    // Cleanup
    taskLogger.clearContext("task-1");
    taskLogger.clearContext("task-2");
  }

  @Test
  void disabledLogger_shouldNotLog() {
    // Given
    taskLogger.setEnabled(false);
    int initialCount = logHandler.getRecordCount();

    // When
    taskLogger.logTaskReceived(
        "test-task-2",
        TaskExecutionLogger.TASK_TYPE_CONFIG_POLL,
        TaskExecutionLogger.SOURCE_SCHEDULER);
    taskLogger.logTaskStarted("test-task-2");
    taskLogger.logTaskCompleted("test-task-2");

    // Then - 不应该有新的日志记录
    assertThat(logHandler.getRecordCount()).isEqualTo(initialCount);

    // 重新启用
    taskLogger.setEnabled(true);
  }

  /** 测试日志处理器 */
  private static class TestLogHandler extends Handler {
    private LogRecord lastRecord;
    private int recordCount = 0;

    @Override
    public void publish(LogRecord record) {
      this.lastRecord = record;
      this.recordCount++;
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}

    LogRecord getLastRecord() {
      return lastRecord;
    }

    int getRecordCount() {
      return recordCount;
    }

    /**
     * 获取格式化后的日志消息
     */
    String getFormattedMessage() {
      if (lastRecord == null) {
        return null;
      }
      String message = lastRecord.getMessage();
      Object[] params = lastRecord.getParameters();
      if (params != null && params.length > 0) {
        return MessageFormat.format(message, params);
      }
      return message;
    }
  }
}
