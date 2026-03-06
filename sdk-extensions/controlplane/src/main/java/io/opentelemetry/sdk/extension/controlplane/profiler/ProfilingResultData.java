/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.profiler;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.opentelemetry.sdk.extension.controlplane.util.JsonUtils;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Profiling 结果数据模型
 *
 * <p>封装一次 profiling 的完整结果数据，同时服务于两种输出场景：
 * <ul>
 *   <li>{@link #toJson()} — 序列化为 JSON 字符串，用于 {@code TaskExecutionResult.success()}</li>
 *   <li>{@link #toMetadata()} — 转为 {@code Map<String, String>}，用于文件上传元数据</li>
 * </ul>
 *
 * <p>通过统一的 Model 对象消除手动拼 JSON 和散落的魔法字符串，
 * 保证两种输出场景的字段名一致性，并由 Jackson {@code @JsonProperty} 注解提供类型安全的序列化。
 */
@SuppressWarnings("UnusedVariable") // 所有字段通过 Jackson 反射读取（@JsonProperty 序列化 + convertValue 转 Map）
final class ProfilingResultData {

  @JsonProperty("event")
  private final String event;

  @JsonProperty("format")
  private final String format;

  @JsonProperty("duration_ms")
  private final long durationMs;

  @JsonProperty("file_size")
  private final long fileSize;

  @JsonProperty("interval")
  private final long interval;

  @JsonProperty("interval_unit")
  private final String intervalUnit;

  @JsonProperty("threads")
  private final boolean threads;

  @JsonProperty("actual_duration_ms")
  private final long actualDurationMs;

  @JsonProperty("upload_id")
  private final String uploadId;

  /**
   * 从采样请求、采样结果和上传结果构建完整的结果数据
   *
   * @param request 采样请求参数
   * @param profilerResult 采样执行结果
   * @param uploadResult 文件上传结果（上传前构建 metadata 时传 null）
   */
  ProfilingResultData(
      ProfileRequest request,
      ProfilerResult profilerResult,
      @Nullable FileStreamUploader.UploadResult uploadResult) {
    this.event = request.getEventName();
    this.format = request.getFormat();
    this.durationMs = request.getDurationMs();
    this.fileSize = profilerResult.getFileSize();
    this.interval = request.getInterval();
    this.intervalUnit = request.getEventType().getUnit();
    this.threads = request.isThreads();
    this.actualDurationMs = profilerResult.getDurationMs();
    this.uploadId =
        uploadResult != null && uploadResult.getUploadId() != null
            ? uploadResult.getUploadId()
            : "";
  }

  /**
   * 序列化为 JSON 字符串
   *
   * <p>用于 {@code TaskExecutionResult.success(resultJson)}，
   * 由 Jackson 自动处理特殊字符转义，消除手动拼接的注入风险。
   *
   * @return JSON 格式字符串
   */
  String toJson() {
    return JsonUtils.toJsonString(this);
  }

  /**
   * 转为上传元数据 Map
   *
   * <p>利用 {@link JsonUtils#toStringMap(Object, String...)} 将对象按 {@code @JsonProperty}
   * 注解自动转为 {@code Map<String, String>}，与 {@link #toJson()} 共享同一份字段定义，
   * 消除手动维护魔法字符串的风险。
   *
   * <p>排除 {@code upload_id} 字段，因为上传时 uploadId 尚未生成。
   * 返回不可变 Map，防止调用方意外修改。
   *
   * @return 不可变的元数据 Map
   */
  Map<String, String> toMetadata() {
    return JsonUtils.toStringMap(this, "upload_id");
  }
}
