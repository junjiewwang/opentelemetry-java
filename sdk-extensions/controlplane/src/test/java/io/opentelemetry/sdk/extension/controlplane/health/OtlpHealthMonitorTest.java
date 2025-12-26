/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.health;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OtlpHealthMonitorTest {

  private OtlpHealthMonitor monitor;

  @BeforeEach
  void setUp() {
    // 窗口大小 10，健康阈值 0.8，不健康阈值 0.3
    monitor = new OtlpHealthMonitor(10, 0.8, 0.3);
  }

  @Test
  void initialStateIsUnknown() {
    assertThat(monitor.getState()).isEqualTo(OtlpHealthMonitor.HealthState.UNKNOWN);
    assertThat(monitor.isHealthy()).isTrue(); // UNKNOWN 被视为健康
  }

  @Test
  void becomesHealthyAfterEnoughSuccesses() {
    // 记录 6 次成功 (超过窗口大小的一半)
    for (int i = 0; i < 6; i++) {
      monitor.recordSuccess();
    }

    assertThat(monitor.getState()).isEqualTo(OtlpHealthMonitor.HealthState.HEALTHY);
    assertThat(monitor.isHealthy()).isTrue();
    assertThat(monitor.getSuccessRate()).isEqualTo(1.0);
  }

  @Test
  void becomesUnhealthyAfterEnoughFailures() {
    // 记录 6 次失败 (超过窗口大小的一半)
    for (int i = 0; i < 6; i++) {
      monitor.recordFailure("test error");
    }

    assertThat(monitor.getState()).isEqualTo(OtlpHealthMonitor.HealthState.UNHEALTHY);
    assertThat(monitor.isHealthy()).isFalse();
    assertThat(monitor.getSuccessRate()).isEqualTo(0.0);
  }

  @Test
  void becomesDegradedWithMixedResults() {
    // 记录 4 次成功和 4 次失败 (成功率 0.5，在阈值之间)
    for (int i = 0; i < 4; i++) {
      monitor.recordSuccess();
    }
    for (int i = 0; i < 4; i++) {
      monitor.recordFailure("test error");
    }

    assertThat(monitor.getState()).isEqualTo(OtlpHealthMonitor.HealthState.DEGRADED);
    assertThat(monitor.getSuccessRate()).isEqualTo(0.5);
  }

  @Test
  void countsAreUpdatedCorrectly() {
    monitor.recordSuccess();
    monitor.recordSuccess();
    monitor.recordFailure("error");

    assertThat(monitor.getSuccessCount()).isEqualTo(2);
    assertThat(monitor.getFailureCount()).isEqualTo(1);
  }

  @Test
  void listenerIsNotifiedOnStateChange() {
    AtomicReference<OtlpHealthMonitor.HealthState> previousState = new AtomicReference<>();
    AtomicReference<OtlpHealthMonitor.HealthState> newState = new AtomicReference<>();
    AtomicInteger callCount = new AtomicInteger(0);

    monitor.addListener(
        (prev, next) -> {
          previousState.set(prev);
          newState.set(next);
          callCount.incrementAndGet();
        });

    // 触发状态变更
    for (int i = 0; i < 6; i++) {
      monitor.recordSuccess();
    }

    assertThat(callCount.get()).isEqualTo(1);
    assertThat(previousState.get()).isEqualTo(OtlpHealthMonitor.HealthState.UNKNOWN);
    assertThat(newState.get()).isEqualTo(OtlpHealthMonitor.HealthState.HEALTHY);
  }

  @Test
  void slidingWindowEvictsOldEntries() {
    // 先记录 10 次成功 (填满窗口)
    for (int i = 0; i < 10; i++) {
      monitor.recordSuccess();
    }
    assertThat(monitor.getSuccessRate()).isEqualTo(1.0);

    // 再记录 10 次失败 (替换所有成功)
    for (int i = 0; i < 10; i++) {
      monitor.recordFailure("error");
    }
    assertThat(monitor.getSuccessRate()).isEqualTo(0.0);
    assertThat(monitor.getState()).isEqualTo(OtlpHealthMonitor.HealthState.UNHEALTHY);
  }

  @Test
  void lastTimestampsAreUpdated() {
    assertThat(monitor.getLastSuccessTimeNano()).isEqualTo(0);
    assertThat(monitor.getLastFailureTimeNano()).isEqualTo(0);

    monitor.recordSuccess();
    assertThat(monitor.getLastSuccessTimeNano()).isGreaterThan(0);

    monitor.recordFailure("error");
    assertThat(monitor.getLastFailureTimeNano()).isGreaterThan(0);
  }

  @Test
  void removeListenerStopsNotifications() {
    AtomicInteger callCount = new AtomicInteger(0);
    OtlpHealthMonitor.HealthStateListener listener = (prev, next) -> callCount.incrementAndGet();

    monitor.addListener(listener);

    // 触发状态变更
    for (int i = 0; i < 6; i++) {
      monitor.recordSuccess();
    }
    assertThat(callCount.get()).isEqualTo(1);

    // 移除监听器
    monitor.removeListener(listener);

    // 触发另一次状态变更
    for (int i = 0; i < 10; i++) {
      monitor.recordFailure("error");
    }

    // 应该不再收到通知
    assertThat(callCount.get()).isEqualTo(1);
  }
}
