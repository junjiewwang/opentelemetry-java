package io.opentelemetry.sdk.extension.controlplane.task.status;

import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionResult;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 统一任务状态事件管理器。
 *
 * <p>目标：
 * - **事件驱动**：执行器发射状态事件即可，避免 sleep/polling。
 * - **统一管理**：集中做去重/节流/广播，避免各执行器自己实现一套。
 *
 * <p><b>Phase 5 重构</b>：使用 {@link TaskExecutionResult.Status} 替代旧的
 * {@code ControlPlaneClient.TaskStatus}。
 *
 * <p><b>设计约束（方案A）</b>：
 * <ul>
 *   <li>终态（SUCCESS/FAILED/TIMEOUT/CANCELLED）只由 {@code TaskDispatcher} 统一上报</li>
 *   <li>{@code TaskStatusEmitter} 只负责 RUNNING 事件的实时广播</li>
 *   <li>执行器调用 {@code emitter.success()/failed()} 只记录状态，不触发上报</li>
 * </ul>
 * 这样可以确保终态的时间信息（started_at_millis, completed_at_millis, execution_time_millis）
 * 由调度层统一计算，避免执行器上报不准确的时间导致 started == completed 且 execution_time == 0 的问题。
 */
public final class TaskStatusEventManager {

  private static final Logger logger = Logger.getLogger(TaskStatusEventManager.class.getName());

  /** RUNNING 事件最小上报间隔（防止 spam），默认 1s */
  private static final long MIN_RUNNING_EVENT_INTERVAL_MILLIS = 1000;

  private final CopyOnWriteArrayList<TaskStatusEventListener> listeners = new CopyOnWriteArrayList<>();

  /** taskId -> last RUNNING report timestamp */
  private final Map<String, AtomicLong> lastRunningReportAt = new ConcurrentHashMap<>();

  /** taskId -> terminal status set */
  private final Map<String, TaskExecutionResult.Status> terminalStatus = new ConcurrentHashMap<>();

  public interface TaskStatusEventListener {
    void onEvent(TaskStatusEvent event);
  }

  public void addListener(TaskStatusEventListener listener) {
    listeners.add(listener);
  }

  public void removeListener(TaskStatusEventListener listener) {
    listeners.remove(listener);
  }

  public TaskStatusEmitter createEmitter(String taskId, String agentId) {
    return new TaskStatusEmitter() {
      @Override
      public void running(String message) {
        emitRunning(taskId, agentId, message);
      }

      @Override
      public void success(@Nullable String resultJson) {
        emitTerminal(TaskStatusEvent.success(taskId, agentId, resultJson));
      }

      @Override
      public void failed(String errorCode, String errorMessage) {
        emitTerminal(TaskStatusEvent.failed(taskId, agentId, errorCode, errorMessage));
      }
    };
  }

  public void closeEmitter(String taskId) {
    lastRunningReportAt.remove(taskId);
  }

  private void emitRunning(String taskId, String agentId, String message) {
    // 如果已经进入终态，不再发 RUNNING
    TaskExecutionResult.Status t = terminalStatus.get(taskId);
    if (t == TaskExecutionResult.Status.SUCCESS 
        || t == TaskExecutionResult.Status.FAILED 
        || t == TaskExecutionResult.Status.TIMEOUT 
        || t == TaskExecutionResult.Status.CANCELLED) {
      return;
    }

    long now = System.currentTimeMillis();
    AtomicLong last = lastRunningReportAt.computeIfAbsent(taskId, k -> new AtomicLong(0));
    long prev = last.get();
    if (now - prev < MIN_RUNNING_EVENT_INTERVAL_MILLIS) {
      return;
    }
    last.set(now);

    emit(TaskStatusEvent.running(taskId, agentId, message));
  }

  private void emitTerminal(TaskStatusEvent event) {
    terminalStatus.put(event.getTaskId(), event.getStatus());
    // 【方案A】终态事件只记录状态，不广播给监听器
    // 终态由 TaskDispatcher 在任务完成时统一上报，确保时间信息准确
    // 这样可以避免 TaskStatusEvent.toExecutionResult() 产生的 started == completed 且 execution_time == 0 问题
    logger.log(
        Level.FINE,
        "[STATUS-EVENT] Terminal status recorded (not broadcast): taskId={0}, status={1}",
        new Object[] {event.getTaskId(), event.getStatus()});
  }

  private void emit(TaskStatusEvent event) {
    for (TaskStatusEventListener l : listeners) {
      try {
        l.onEvent(event);
      } catch (RuntimeException e) {
        logger.log(Level.FINE, "TaskStatusEvent listener failed: {0}", e.getMessage());
      }
    }
  }
}
