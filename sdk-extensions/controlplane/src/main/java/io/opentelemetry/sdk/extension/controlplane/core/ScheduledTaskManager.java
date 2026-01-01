/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core;

import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 调度任务管理器。
 *
 * <p>负责统一管理控制平面的定时任务，包括：
 *
 * <ul>
 *   <li>任务注册和启动
 *   <li>任务取消和停止
 *   <li>调度器生命周期管理
 * </ul>
 */
public final class ScheduledTaskManager implements Closeable {

  private static final Logger logger = Logger.getLogger(ScheduledTaskManager.class.getName());

  /** 任务配置 */
  public static final class TaskConfig {
    private final String name;
    private final Runnable task;
    private final Duration initialDelay;
    private final Duration interval;

    private TaskConfig(String name, Runnable task, Duration initialDelay, Duration interval) {
      this.name = name;
      this.task = task;
      this.initialDelay = initialDelay;
      this.interval = interval;
    }

    /**
     * 创建任务配置
     *
     * @param name 任务名称
     * @param task 任务执行体
     * @param initialDelay 初始延迟
     * @param interval 执行间隔
     * @return 任务配置
     */
    public static TaskConfig create(
        String name, Runnable task, Duration initialDelay, Duration interval) {
      return new TaskConfig(name, task, initialDelay, interval);
    }

    /**
     * 创建任务配置（无初始延迟）
     *
     * @param name 任务名称
     * @param task 任务执行体
     * @param interval 执行间隔
     * @return 任务配置
     */
    @SuppressWarnings("InconsistentOverloads")
    public static TaskConfig create(String name, Runnable task, Duration interval) {
      return new TaskConfig(name, task, Duration.ZERO, interval);
    }

    public String getName() {
      return name;
    }

    public Runnable getTask() {
      return task;
    }

    public Duration getInitialDelay() {
      return initialDelay;
    }

    public Duration getInterval() {
      return interval;
    }
  }

  private final ScheduledExecutorService scheduler;
  private final Map<String, ScheduledFuture<?>> scheduledTasks;
  private volatile boolean closed;

  /**
   * 创建调度任务管理器
   *
   * @param threadPoolSize 线程池大小
   * @param threadNamePrefix 线程名前缀
   */
  public ScheduledTaskManager(int threadPoolSize, String threadNamePrefix) {
    this.scheduler =
        Executors.newScheduledThreadPool(
            threadPoolSize,
            r -> {
              Thread t = new Thread(r, threadNamePrefix);
              t.setDaemon(true);
              return t;
            });
    this.scheduledTasks = new ConcurrentHashMap<>();
    this.closed = false;
  }

  /**
   * 创建默认配置的调度任务管理器
   *
   * @return 调度任务管理器
   */
  public static ScheduledTaskManager createDefault() {
    return new ScheduledTaskManager(4, "otel-controlplane");
  }

  /**
   * 获取调度器
   *
   * @return 调度器
   */
  public ScheduledExecutorService getScheduler() {
    return scheduler;
  }

  /**
   * 调度任务
   *
   * @param config 任务配置
   * @return 是否调度成功
   */
  public boolean scheduleTask(TaskConfig config) {
    if (closed) {
      logger.log(Level.WARNING, "Cannot schedule task {0}, manager is closed", config.getName());
      return false;
    }

    // 如果任务已存在，先取消
    cancelTask(config.getName());

    logger.log(
        Level.INFO,
        "Scheduling task: {0}, initialDelay: {1}ms, interval: {2}ms",
        new Object[] {
          config.getName(),
          config.getInitialDelay().toMillis(),
          config.getInterval().toMillis()
        });

    ScheduledFuture<?> future =
        scheduler.scheduleWithFixedDelay(
            wrapTask(config.getName(), config.getTask()),
            config.getInitialDelay().toMillis(),
            config.getInterval().toMillis(),
            TimeUnit.MILLISECONDS);

    scheduledTasks.put(config.getName(), future);
    return true;
  }

  /**
   * 取消任务
   *
   * @param taskName 任务名称
   * @return 是否取消成功
   */
  public boolean cancelTask(String taskName) {
    ScheduledFuture<?> future = scheduledTasks.remove(taskName);
    if (future != null) {
      boolean cancelled = future.cancel(false);
      logger.log(Level.INFO, "Cancelled task: {0}, result: {1}", new Object[] {taskName, cancelled});
      return cancelled;
    }
    return false;
  }

  /**
   * 取消所有任务
   */
  public void cancelAllTasks() {
    for (String taskName : scheduledTasks.keySet()) {
      cancelTask(taskName);
    }
  }

  /**
   * 检查任务是否正在运行
   *
   * @param taskName 任务名称
   * @return 是否正在运行
   */
  public boolean isTaskRunning(String taskName) {
    ScheduledFuture<?> future = scheduledTasks.get(taskName);
    return future != null && !future.isCancelled() && !future.isDone();
  }

  /**
   * 获取正在运行的任务数量
   *
   * @return 任务数量
   */
  public int getRunningTaskCount() {
    return (int) scheduledTasks.values().stream()
        .filter(f -> !f.isCancelled() && !f.isDone())
        .count();
  }

  /**
   * 执行一次性任务
   *
   * @param taskName 任务名称
   * @param task 任务
   * @param delay 延迟时间
   * @return 调度的 Future
   */
  @Nullable
  public ScheduledFuture<?> scheduleOnce(String taskName, Runnable task, Duration delay) {
    if (closed) {
      logger.log(Level.WARNING, "Cannot schedule one-time task {0}, manager is closed", taskName);
      return null;
    }

    return scheduler.schedule(
        wrapTask(taskName, task),
        delay.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  /**
   * 立即执行任务
   *
   * @param taskName 任务名称
   * @param task 任务
   */
  public void executeNow(String taskName, Runnable task) {
    if (closed) {
      logger.log(Level.WARNING, "Cannot execute task {0}, manager is closed", taskName);
      return;
    }

    scheduler.execute(wrapTask(taskName, task));
  }

  /** 包装任务，添加异常处理 */
  private static Runnable wrapTask(String taskName, Runnable task) {
    return () -> {
      try {
        task.run();
      } catch (RuntimeException e) {
        logger.log(
            Level.WARNING,
            "Task {0} execution failed: {1}",
            new Object[] {taskName, e.getMessage()});
      }
    };
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;

    logger.log(Level.INFO, "Closing scheduled task manager...");

    // 取消所有任务
    cancelAllTasks();

    // 关闭调度器
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      scheduler.shutdownNow();
    }

    logger.log(Level.INFO, "Scheduled task manager closed");
  }
}
