/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class OtlpExportMetricsTest {

  @Nested
  @DisplayName("Basic Tests")
  class BasicTests {

    private OtlpExportMetrics metrics;

    @BeforeEach
    void setUp() {
      metrics = new OtlpExportMetrics();
    }

    @Test
    void initialStateIsEmpty() {
      assertThat(metrics.getSuccessRate()).isEqualTo(1.0); // 乐观默认
      assertThat(metrics.getSuccessCount()).isZero();
      assertThat(metrics.getFailureCount()).isZero();
      assertThat(metrics.getActiveSignalCount()).isZero();
    }

    @Test
    void recordSuccessUpdatesStats() {
      metrics.recordSuccess();

      assertThat(metrics.getSuccessCount()).isEqualTo(1);
      assertThat(metrics.getFailureCount()).isZero();
      assertThat(metrics.getLastSuccessTimeNano()).isGreaterThan(0);
    }

    @Test
    void recordFailureUpdatesStats() {
      metrics.recordFailure("test error");

      assertThat(metrics.getSuccessCount()).isZero();
      assertThat(metrics.getFailureCount()).isEqualTo(1);
      assertThat(metrics.getLastFailureTimeNano()).isGreaterThan(0);
    }

    @Test
    void multipleRecordsUpdateStats() {
      // 记录多次成功和失败
      for (int i = 0; i < 8; i++) {
        metrics.recordSuccess();
      }
      for (int i = 0; i < 2; i++) {
        metrics.recordFailure("error");
      }

      assertThat(metrics.getSuccessCount()).isEqualTo(8);
      assertThat(metrics.getFailureCount()).isEqualTo(2);
    }
  }

  @Nested
  @DisplayName("Signal Type Tests")
  class SignalTypeTests {

    private OtlpExportMetrics metrics;

    @BeforeEach
    void setUp() {
      metrics = new OtlpExportMetrics();
    }

    @Test
    void recordSuccessBySignalType() {
      metrics.recordSuccess(SignalType.SPAN);
      metrics.recordSuccess(SignalType.METRIC);

      assertThat(metrics.getSuccessCount(SignalType.SPAN)).isEqualTo(1);
      assertThat(metrics.getSuccessCount(SignalType.METRIC)).isEqualTo(1);
    }

    @Test
    void recordFailureBySignalType() {
      metrics.recordFailure("span error", SignalType.SPAN);
      metrics.recordFailure("metric error", SignalType.METRIC);

      assertThat(metrics.getFailureCount(SignalType.SPAN)).isEqualTo(1);
      assertThat(metrics.getFailureCount(SignalType.METRIC)).isEqualTo(1);
    }

    @Test
    void getSuccessRateBySignalType() {
      // SPAN: 8 成功, 2 失败 = 80%
      for (int i = 0; i < 8; i++) {
        metrics.recordSuccess(SignalType.SPAN);
      }
      for (int i = 0; i < 2; i++) {
        metrics.recordFailure("error", SignalType.SPAN);
      }

      // METRIC: 9 成功, 1 失败 = 90%
      for (int i = 0; i < 9; i++) {
        metrics.recordSuccess(SignalType.METRIC);
      }
      metrics.recordFailure("error", SignalType.METRIC);

      assertThat(metrics.getSuccessRate(SignalType.SPAN)).isCloseTo(0.8, org.assertj.core.data.Offset.offset(0.01));
      assertThat(metrics.getSuccessRate(SignalType.METRIC)).isCloseTo(0.9, org.assertj.core.data.Offset.offset(0.01));
    }
  }

  @Nested
  @DisplayName("Composite Rate Tests")
  class CompositeRateTests {

    private OtlpExportMetrics metrics;

    @BeforeEach
    void setUp() {
      // 使用较短的时间窗口和较低的最小样本数，以便测试
      metrics = OtlpExportMetrics.builder()
          .windowMillis(60_000)
          .minSamples(2)
          .build();
    }

    @Test
    void compositeRateWithBothSignals() {
      // SPAN: 成功率 80%，权重 40%
      for (int i = 0; i < 4; i++) {
        metrics.recordSuccess(SignalType.SPAN);
      }
      metrics.recordFailure("error", SignalType.SPAN);

      // METRIC: 成功率 100%，权重 60%
      for (int i = 0; i < 5; i++) {
        metrics.recordSuccess(SignalType.METRIC);
      }

      // 加权平均: 0.8 * 0.4 + 1.0 * 0.6 = 0.92
      double compositeRate = metrics.getSuccessRate();
      assertThat(compositeRate).isCloseTo(0.92, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test
    void compositeRateWithOnlySpan() {
      for (int i = 0; i < 8; i++) {
        metrics.recordSuccess(SignalType.SPAN);
      }
      for (int i = 0; i < 2; i++) {
        metrics.recordFailure("error", SignalType.SPAN);
      }

      // 只有 SPAN 信号，成功率 80%
      double compositeRate = metrics.getSuccessRate();
      assertThat(compositeRate).isCloseTo(0.8, org.assertj.core.data.Offset.offset(0.01));
    }
  }

  @Nested
  @DisplayName("Snapshot Tests")
  class SnapshotTests {

    private OtlpExportMetrics metrics;

    @BeforeEach
    void setUp() {
      metrics = OtlpExportMetrics.builder()
          .windowMillis(60_000)
          .minSamples(2)
          .build();
    }

    @Test
    void createSnapshotContainsAllData() {
      for (int i = 0; i < 5; i++) {
        metrics.recordSuccess(SignalType.SPAN);
        metrics.recordSuccess(SignalType.METRIC);
      }

      OtlpExportMetrics.ExportMetricsSnapshot snapshot = metrics.createSnapshot();

      assertThat(snapshot.getCompositeSuccessRate()).isEqualTo(1.0);
      assertThat(snapshot.getActiveSignalCount()).isEqualTo(2);
      assertThat(snapshot.getSignalSnapshots()).hasSize(2);
      assertThat(snapshot.getSignalSnapshots()).containsKeys(SignalType.SPAN, SignalType.METRIC);
    }

    @Test
    void snapshotToStringContainsInfo() {
      metrics.recordSuccess(SignalType.SPAN);
      metrics.recordSuccess(SignalType.METRIC);

      OtlpExportMetrics.ExportMetricsSnapshot snapshot = metrics.createSnapshot();
      String str = snapshot.toString();

      assertThat(str).contains("ExportMetricsSnapshot");
      assertThat(str).contains("compositeRate=");
    }
  }

  @Nested
  @DisplayName("Configuration Tests")
  class ConfigurationTests {

    @Test
    void builderSetsWindowMillis() {
      OtlpExportMetrics metrics = OtlpExportMetrics.builder()
          .windowMillis(120_000)
          .build();

      assertThat(metrics.getWindowMillis()).isEqualTo(120_000);
    }

    @Test
    void builderSetsMinSamples() {
      OtlpExportMetrics metrics = OtlpExportMetrics.builder()
          .minSamples(10)
          .build();

      assertThat(metrics.getMinSamples()).isEqualTo(10);
    }

    @Test
    void invalidWindowMillisThrowsException() {
      assertThatThrownBy(() -> new OtlpExportMetrics(0, 5))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("windowMillis must be positive");
    }

    @Test
    void negativeWindowMillisThrowsException() {
      assertThatThrownBy(() -> new OtlpExportMetrics(-1, 5))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("windowMillis must be positive");
    }
  }

  @Nested
  @DisplayName("Utility Tests")
  class UtilityTests {

    @Test
    void nanoTimeToUnixMsConvertsCorrectly() {
      long now = System.nanoTime();
      long unixMs = OtlpExportMetrics.nanoTimeToUnixMs(now);
      long currentMs = System.currentTimeMillis();

      // 应该在当前时间附近
      assertThat(unixMs).isCloseTo(currentMs, org.assertj.core.data.Offset.offset(1000L));
    }

    @Test
    void nanoTimeToUnixMsReturnsZeroForZero() {
      assertThat(OtlpExportMetrics.nanoTimeToUnixMs(0)).isZero();
    }

    @Test
    void toStringContainsInfo() {
      OtlpExportMetrics metrics = new OtlpExportMetrics();
      String str = metrics.toString();

      assertThat(str).contains("OtlpExportMetrics");
      assertThat(str).contains("compositeRate=");
      assertThat(str).contains("window=");
    }
  }

  @Nested
  @DisplayName("Tracker Tests")
  class TrackerTests {

    private OtlpExportMetrics metrics;

    @BeforeEach
    void setUp() {
      metrics = new OtlpExportMetrics();
    }

    @Test
    void getTrackerReturnsTrackerForValidType() {
      assertThat(metrics.getTracker(SignalType.SPAN)).isNotNull();
      assertThat(metrics.getTracker(SignalType.METRIC)).isNotNull();
    }

    @Test
    void getSampleCountReturnsCorrectValue() {
      metrics.recordSuccess(SignalType.SPAN);
      metrics.recordSuccess(SignalType.SPAN);
      metrics.recordFailure("error", SignalType.SPAN);

      assertThat(metrics.getSampleCount(SignalType.SPAN)).isEqualTo(3);
      assertThat(metrics.getSampleCount(SignalType.METRIC)).isZero();
    }
  }
}
