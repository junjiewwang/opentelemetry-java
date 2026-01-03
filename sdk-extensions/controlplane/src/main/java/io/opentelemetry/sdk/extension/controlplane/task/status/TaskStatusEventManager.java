package io.opentelemetry.sdk.extension.controlplane.task.status;

import io.opentelemetry.sdk.extension.controlplane.client.ControlPlaneClient.TaskStatus;
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
 */
public final class TaskStatusEventManager {

  private static final Logger logger = Logger.getLogger(TaskStatusEventManager.class.getName());

  /** RUNNING 事件最小上报间隔（防止 spam），默认 1s */
  private static final long MIN_RUNNING_EVENT_INTERVAL_MILLIS = 1000;

  private final CopyOnWriteArrayList<TaskStatusEventListener> listeners = new CopyOnWriteArrayList<>();

  /** taskId -> last RUNNING report timestamp */
  private final Map<String, AtomicLong> lastRunningReportAt = new ConcurrentHashMap<>();

  /** taskId -> terminal status set */
  private final Map<String, TaskStatus> terminalStatus = new ConcurrentHashMap<>();

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
    TaskStatus t = terminalStatus.get(taskId);
    if (t == TaskStatus.SUCCESS || t == TaskStatus.FAILED || t == TaskStatus.TIMEOUT || t == TaskStatus.CANCELLED) {
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
    emit(event);
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
