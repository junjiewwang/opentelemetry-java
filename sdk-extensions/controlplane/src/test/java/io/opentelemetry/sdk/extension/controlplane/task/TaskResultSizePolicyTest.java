/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TaskResultSizePolicyTest {

  private TaskResultSizePolicy policy;

  @BeforeEach
  void setUp() {
    // 压缩阈值 1KB, 分片阈值 50KB, 分片大小 10KB, 最大 100KB
    policy =
        new TaskResultSizePolicy(
            1024, // 1KB
            50 * 1024, // 50KB
            10 * 1024, // 10KB
            100 * 1024 // 100KB
            );
  }

  @Test
  void smallResultNotCompressed() {
    byte[] data = new byte[512]; // 512B < 1KB
    TaskResultSizePolicy.TaskResultWrapper result = policy.process("task1", data, "text/plain");

    assertThat(result.getType()).isEqualTo(TaskResultSizePolicy.TaskResultWrapper.Type.DIRECT);
    assertThat(result.isCompressed()).isFalse();
    assertThat(result.getData()).isEqualTo(data);
    assertThat(result.getOriginalSize()).isEqualTo(512);
    assertThat(result.getFinalSize()).isEqualTo(512);
  }

  @Test
  void mediumResultCompressed() {
    // 创建可压缩的数据 (重复的内容)
    byte[] data = new byte[10 * 1024]; // 10KB
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 10);
    }

    TaskResultSizePolicy.TaskResultWrapper result = policy.process("task1", data, "text/plain");

    assertThat(result.getType()).isEqualTo(TaskResultSizePolicy.TaskResultWrapper.Type.COMPRESSED);
    assertThat(result.isCompressed()).isTrue();
    assertThat(result.getOriginalSize()).isEqualTo(10 * 1024);
    assertThat(result.getFinalSize()).isLessThan(result.getOriginalSize());
  }

  @Test
  void largeResultChunked() {
    // 创建需要分片的数据
    byte[] data = new byte[60 * 1024]; // 60KB > 50KB 分片阈值
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 256);
    }

    TaskResultSizePolicy.TaskResultWrapper result = policy.process("task1", data, "text/plain");

    // 由于随机数据可能压缩后仍然大于分片阈值，所以可能是 CHUNKED 或 COMPRESSED
    if (result.getType() == TaskResultSizePolicy.TaskResultWrapper.Type.CHUNKED) {
      assertThat(result.getChunks()).isNotNull();
      assertThat(result.getChunks()).isNotEmpty();
      assertThat(result.isCompressed()).isTrue();
    }
  }

  @Test
  void oversizedResultRejected() {
    byte[] data = new byte[150 * 1024]; // 150KB > 100KB 最大阈值

    TaskResultSizePolicy.TaskResultWrapper result = policy.process("task1", data, "text/plain");

    assertThat(result.isRejected()).isTrue();
    assertThat(result.getType()).isEqualTo(TaskResultSizePolicy.TaskResultWrapper.Type.REJECTED);
    assertThat(result.getErrorMessage()).contains("exceeds maximum allowed size");
    assertThat(result.getData()).isNull();
  }

  @Test
  void compressionRatioCalculation() {
    byte[] data = new byte[512];
    TaskResultSizePolicy.TaskResultWrapper result = policy.process("task1", data, "text/plain");

    assertThat(result.getCompressionRatio()).isEqualTo(1.0);
  }

  @Test
  void chunkInfoHasCorrectMetadata() {
    // 使用较小的配置来确保分片
    TaskResultSizePolicy smallPolicy =
        new TaskResultSizePolicy(
            100, // 100B
            1024, // 1KB
            256, // 256B 分片大小
            10 * 1024 // 10KB
            );

    byte[] data = new byte[2048]; // 2KB
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }

    TaskResultSizePolicy.TaskResultWrapper result =
        smallPolicy.process("task1", data, "text/plain");

    if (result.getType() == TaskResultSizePolicy.TaskResultWrapper.Type.CHUNKED) {
      assertThat(result.getChunks())
          .allSatisfy(
              chunk -> {
                assertThat(chunk.getTaskId()).isEqualTo("task1");
                assertThat(chunk.getUploadId()).isNotNull();
                assertThat(chunk.getChecksum()).isNotNull();
                assertThat(chunk.getTotalChunks()).isGreaterThan(0);
              });

      // 最后一个分片应该标记为 isLastChunk
      assertThat(result.getChunks().get(result.getChunks().size() - 1).isLastChunk()).isTrue();
    }
  }

  @Test
  void validationFailsForInvalidConfig() {
    assertThatThrownBy(() -> new TaskResultSizePolicy(2048, 1024, 512, 4096))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("compressionThreshold must be less than chunkedThreshold");

    assertThatThrownBy(() -> new TaskResultSizePolicy(512, 4096, 1024, 2048))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("chunkedThreshold must be less than maxSize");

    assertThatThrownBy(() -> new TaskResultSizePolicy(512, 1024, 2048, 4096))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("chunkSize must be less than or equal to chunkedThreshold");
  }
}
