/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.opentelemetry.sdk.extension.controlplane.core.ScheduledTaskManager.TaskConfig;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ScheduledTaskManagerTest {

  private ScheduledTaskManager manager;

  @BeforeEach
  void setUp() {
    manager = new ScheduledTaskManager(2, "test-thread");
  }

  @AfterEach
  void tearDown() {
    manager.close();
  }

  @Test
  void scheduleTaskExecutesPeriodically() {
    AtomicInteger counter = new AtomicInteger(0);
    
    manager.scheduleTask(
        TaskConfig.create("test-task", counter::incrementAndGet, Duration.ofMillis(50)));

    await().atMost(Duration.ofMillis(300)).until(() -> counter.get() >= 3);
    assertThat(counter.get()).isGreaterThanOrEqualTo(3);
  }

  @Test
  void scheduleTaskWithInitialDelay() {
    AtomicInteger counter = new AtomicInteger(0);
    long startTime = System.currentTimeMillis();
    
    manager.scheduleTask(
        TaskConfig.create(
            "delayed-task",
            counter::incrementAndGet,
            Duration.ofMillis(100),
            Duration.ofMillis(50)));

    await().atMost(Duration.ofMillis(300)).until(() -> counter.get() >= 1);
    long elapsed = System.currentTimeMillis() - startTime;
    assertThat(elapsed).isGreaterThanOrEqualTo(100);
  }

  @Test
  void cancelTaskStopsExecution() {
    AtomicInteger counter = new AtomicInteger(0);
    
    manager.scheduleTask(
        TaskConfig.create("cancel-task", counter::incrementAndGet, Duration.ofMillis(50)));

    await().atMost(Duration.ofMillis(150)).until(() -> counter.get() >= 1);
    
    boolean cancelled = manager.cancelTask("cancel-task");
    assertThat(cancelled).isTrue();
    
    int countAfterCancel = counter.get();
    // 等待一段时间确保不再执行
    try {
      Thread.sleep(150);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertThat(counter.get()).isEqualTo(countAfterCancel);
  }

  @Test
  void cancelNonExistentTaskReturnsFalse() {
    boolean cancelled = manager.cancelTask("non-existent");
    assertThat(cancelled).isFalse();
  }

  @Test
  void cancelAllTasksStopsAllExecutions() {
    AtomicInteger counter1 = new AtomicInteger(0);
    AtomicInteger counter2 = new AtomicInteger(0);
    
    manager.scheduleTask(
        TaskConfig.create("task1", counter1::incrementAndGet, Duration.ofMillis(50)));
    manager.scheduleTask(
        TaskConfig.create("task2", counter2::incrementAndGet, Duration.ofMillis(50)));

    await().atMost(Duration.ofMillis(150)).until(() -> counter1.get() >= 1 && counter2.get() >= 1);
    
    manager.cancelAllTasks();
    
    int count1AfterCancel = counter1.get();
    int count2AfterCancel = counter2.get();
    try {
      Thread.sleep(150);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertThat(counter1.get()).isEqualTo(count1AfterCancel);
    assertThat(counter2.get()).isEqualTo(count2AfterCancel);
  }

  @Test
  void isTaskRunningReturnsCorrectStatus() {
    manager.scheduleTask(
        TaskConfig.create("running-task", () -> {}, Duration.ofMillis(100)));
    
    assertThat(manager.isTaskRunning("running-task")).isTrue();
    assertThat(manager.isTaskRunning("non-existent")).isFalse();
    
    manager.cancelTask("running-task");
    assertThat(manager.isTaskRunning("running-task")).isFalse();
  }

  @Test
  void getRunningTaskCountReturnsCorrectCount() {
    assertThat(manager.getRunningTaskCount()).isZero();
    
    manager.scheduleTask(TaskConfig.create("task1", () -> {}, Duration.ofMillis(100)));
    assertThat(manager.getRunningTaskCount()).isEqualTo(1);
    
    manager.scheduleTask(TaskConfig.create("task2", () -> {}, Duration.ofMillis(100)));
    assertThat(manager.getRunningTaskCount()).isEqualTo(2);
    
    manager.cancelTask("task1");
    assertThat(manager.getRunningTaskCount()).isEqualTo(1);
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  void scheduleOnceExecutesOnlyOnce() {
    AtomicInteger counter = new AtomicInteger(0);
    
    manager.scheduleOnce("once-task", counter::incrementAndGet, Duration.ofMillis(50));

    await().atMost(Duration.ofMillis(200)).until(() -> counter.get() >= 1);
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertThat(counter.get()).isEqualTo(1);
  }

  @Test
  void executeNowExecutesImmediately() {
    AtomicInteger counter = new AtomicInteger(0);
    
    manager.executeNow("immediate-task", counter::incrementAndGet);

    await().atMost(Duration.ofMillis(500)).until(() -> counter.get() >= 1);
  }

  @Test
  void closedManagerRejectsNewTasks() {
    manager.close();
    
    boolean scheduled = manager.scheduleTask(
        TaskConfig.create("rejected-task", () -> {}, Duration.ofMillis(50)));
    assertThat(scheduled).isFalse();
  }

  @Test
  void createDefaultReturnsValidManager() {
    ScheduledTaskManager defaultManager = ScheduledTaskManager.createDefault();
    assertThat(defaultManager).isNotNull();
    assertThat(defaultManager.getScheduler()).isNotNull();
    defaultManager.close();
  }

  @Test
  void taskExceptionDoesNotStopScheduling() {
    AtomicInteger counter = new AtomicInteger(0);
    
    manager.scheduleTask(
        TaskConfig.create(
            "exception-task",
            () -> {
              counter.incrementAndGet();
              throw new RuntimeException("Test exception");
            },
            Duration.ofMillis(50)));

    await().atMost(Duration.ofMillis(300)).until(() -> counter.get() >= 3);
    assertThat(counter.get()).isGreaterThanOrEqualTo(3);
  }
}
