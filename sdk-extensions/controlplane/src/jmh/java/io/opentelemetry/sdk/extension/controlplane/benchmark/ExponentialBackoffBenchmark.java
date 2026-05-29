/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.benchmark;

import io.opentelemetry.sdk.extension.controlplane.core.longpoll.ExponentialBackoff;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * ExponentialBackoff 基准测试
 *
 * <p>量化退避计算的 CPU 开销。在长轮询失败重试路径中，此计算会被高频调用。
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
public class ExponentialBackoffBenchmark {

  private ExponentialBackoff backoff;

  @Setup
  public void setup() {
    backoff = new ExponentialBackoff(1000, 30000, 2.0);
  }

  @Benchmark
  public long nextBackoff() {
    long result = backoff.nextBackoff();
    backoff.reset();
    return result;
  }

  @Benchmark
  public long getCurrentInterval() {
    return backoff.getCurrentInterval();
  }

  @Benchmark
  public long nextBackoffWithoutReset() {
    return backoff.nextBackoff();
  }

  /** 模拟连续5次失败后重置的场景 */
  @Benchmark
  public long consecutiveFailuresThenReset() {
    long result = 0;
    for (int i = 0; i < 5; i++) {
      result = backoff.nextBackoff();
    }
    backoff.reset();
    return result;
  }
}
