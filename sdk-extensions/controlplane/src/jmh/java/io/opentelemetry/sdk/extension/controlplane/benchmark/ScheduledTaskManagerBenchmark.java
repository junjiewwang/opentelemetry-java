/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.benchmark;

import io.opentelemetry.sdk.extension.controlplane.core.ScheduledTaskManager;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * ScheduledTaskManager 基准测试
 *
 * <p>量化定时任务调度的开销，包括：
 *
 * <ul>
 *   <li>任务注册开销
 *   <li>任务取消开销
 *   <li>任务执行调度延迟
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
public class ScheduledTaskManagerBenchmark {

  private ScheduledTaskManager taskManager;
  private final AtomicInteger taskCounter = new AtomicInteger(0);

  @Setup(Level.Trial)
  public void setup() {
    taskManager = new ScheduledTaskManager(2, "benchmark");
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    taskManager.close();
  }

  @Benchmark
  public void scheduleAndCancelTask(Blackhole bh) {
    String taskName = "bench-task-" + taskCounter.incrementAndGet();
    ScheduledFuture<?> future = taskManager.scheduleOnce(taskName, () -> {}, Duration.ofHours(1));
    bh.consume(future);
    taskManager.cancelTask(taskName);
  }

  @Benchmark
  public void scheduleOnceTask(Blackhole bh) {
    String taskName = "bench-once-" + taskCounter.incrementAndGet();
    ScheduledFuture<?> future = taskManager.scheduleOnce(taskName, () -> {}, Duration.ofHours(1));
    bh.consume(future);
  }

  /**
   * 测量实际调度执行延迟（单位：微秒级）
   *
   * <p>注意：此基准使用 SingleShot 模式，因为它测量的是异步任务完成的延迟
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void scheduleAndExecuteTask(Blackhole bh) throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    String taskName = "bench-exec-" + taskCounter.incrementAndGet();
    ScheduledFuture<?> future =
        taskManager.scheduleOnce(taskName, latch::countDown, Duration.ZERO);
    bh.consume(future);
    latch.await(1, TimeUnit.SECONDS);
  }
}
