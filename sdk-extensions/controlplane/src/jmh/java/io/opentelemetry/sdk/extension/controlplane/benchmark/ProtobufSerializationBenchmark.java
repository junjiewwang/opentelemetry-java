/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.benchmark;

import io.opentelemetry.sdk.extension.controlplane.client.codec.ProtobufCodec;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.CommonProtos.AgentIdentity;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.CommonProtos.ConfigVersion;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.CommonProtos.ResponseStatus;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.ConfigProtos.AgentConfig;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.ConfigProtos.BatchConfig;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.ConfigProtos.ConfigRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.ConfigProtos.ConfigResponse;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.ConfigProtos.SamplerConfig;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.PollProtos.UnifiedPollRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.PollProtos.UnifiedPollResponse;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.StatusProtos.StatusRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.StatusProtos.StatusResponse;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.TaskRequest;
import io.opentelemetry.sdk.extension.controlplane.proto.v1.TaskProtos.TaskResponse;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * ControlPlane Protobuf 序列化/反序列化基准测试
 *
 * <p>量化心跳、配置拉取、统一轮询等核心操作的序列化开销，包括：
 *
 * <ul>
 *   <li>StatusRequest 序列化（心跳请求构建）
 *   <li>StatusResponse 反序列化（心跳响应解析）
 *   <li>ConfigResponse 反序列化（配置响应解析，含完整 AgentConfig）
 *   <li>UnifiedPollRequest 序列化（统一轮询请求构建）
 *   <li>UnifiedPollResponse 反序列化（统一轮询响应解析）
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
public class ProtobufSerializationBenchmark {

  private StatusRequest heartbeatRequest;
  private byte[] heartbeatRequestBytes;
  private byte[] heartbeatResponseBytes;
  private byte[] configResponseBytes;
  private UnifiedPollRequest unifiedPollRequest;
  private byte[] unifiedPollRequestBytes;
  private byte[] unifiedPollResponseBytes;

  @Setup
  public void setup() {
    // 构建心跳请求
    heartbeatRequest =
        StatusRequest.newBuilder()
            .setAgentIdentity(
                AgentIdentity.newBuilder()
                    .setAgentId("agent-benchmark-001")
                    .setServiceName("benchmark-service")
                    .setHostName("benchmark-host")
                    .setProcessId("12345")
                    .build())
            .setAgentId("agent-benchmark-001")
            .setTimestampMillis(System.currentTimeMillis())
            .build();
    heartbeatRequestBytes = heartbeatRequest.toByteArray();

    // 构建心跳响应
    StatusResponse heartbeatResponse =
        StatusResponse.newBuilder()
            .setStatus(
                ResponseStatus.newBuilder()
                    .setCode(ResponseStatus.Code.CODE_OK)
                    .setMessage("success")
                    .build())
            .setServerTimeMillis(System.currentTimeMillis())
            .build();
    heartbeatResponseBytes = heartbeatResponse.toByteArray();

    // 构建配置响应（含完整 AgentConfig）
    ConfigResponse configResponse =
        ConfigResponse.newBuilder()
            .setStatus(
                ResponseStatus.newBuilder()
                    .setCode(ResponseStatus.Code.CODE_OK)
                    .build())
            .setHasChanges(true)
            .setSuccess(true)
            .setConfigVersion("v1.2.3")
            .setEtag("etag-abc123")
            .setConfig(
                AgentConfig.newBuilder()
                    .setVersion(
                        ConfigVersion.newBuilder()
                            .setVersion("v1.2.3")
                            .setEtag("etag-abc123")
                            .build())
                    .setSampler(
                        SamplerConfig.newBuilder()
                            .setType(SamplerConfig.SamplerType.SAMPLER_TYPE_TRACE_ID_RATIO)
                            .setRatio(0.1)
                            .build())
                    .setBatch(
                        BatchConfig.newBuilder()
                            .setMaxExportBatchSize(512)
                            .setMaxQueueSize(2048)
                            .setScheduleDelayMillis(5000)
                            .setExportTimeoutMillis(30000)
                            .build())
                    .putDynamicResourceAttributes("deployment.environment", "production")
                    .putDynamicResourceAttributes("service.version", "2.1.0")
                    .putServerMetadata("collector.endpoint", "http://collector:4318")
                    .build())
            .build();
    configResponseBytes = configResponse.toByteArray();

    // 构建统一轮询请求
    unifiedPollRequest =
        UnifiedPollRequest.newBuilder()
            .setAgentId("agent-benchmark-001")
            .setTimeoutMillis(30000)
            .setConfigRequest(
                ConfigRequest.newBuilder()
                    .setAgentId("agent-benchmark-001")
                    .setServiceName("benchmark-service")
                    .setCurrentConfigVersion("v1.2.2")
                    .setCurrentEtag("etag-old")
                    .setLongPollTimeoutMillis(30000)
                    .build())
            .setTaskRequest(
                TaskRequest.newBuilder()
                    .setAgentId("agent-benchmark-001")
                    .build())
            .build();
    unifiedPollRequestBytes = unifiedPollRequest.toByteArray();

    // 构建统一轮询响应
    UnifiedPollResponse unifiedPollResponse =
        UnifiedPollResponse.newBuilder()
            .setStatus(
                ResponseStatus.newBuilder()
                    .setCode(ResponseStatus.Code.CODE_OK)
                    .build())
            .setHasAnyChanges(true)
            .setConfigResponse(configResponse)
            .setTaskResponse(
                TaskResponse.newBuilder()
                    .setStatus(
                        ResponseStatus.newBuilder()
                            .setCode(ResponseStatus.Code.CODE_OK)
                            .build())
                    .build())
            .setSuggestedPollIntervalMillis(0)
            .build();
    unifiedPollResponseBytes = unifiedPollResponse.toByteArray();
  }

  // ============ 心跳序列化 ============

  @Benchmark
  public byte[] heartbeatRequestSerialize() {
    return ProtobufCodec.encode(heartbeatRequest);
  }

  @Benchmark
  public void heartbeatRequestDeserialize(Blackhole bh) throws Exception {
    bh.consume(ProtobufCodec.decode(heartbeatRequestBytes, StatusRequest.parser()));
  }

  @Benchmark
  public void heartbeatResponseDeserialize(Blackhole bh) throws Exception {
    bh.consume(ProtobufCodec.decode(heartbeatResponseBytes, StatusResponse.parser()));
  }

  // ============ 配置响应反序列化 ============

  @Benchmark
  public void configResponseDeserialize(Blackhole bh) throws Exception {
    bh.consume(ProtobufCodec.decode(configResponseBytes, ConfigResponse.parser()));
  }

  // ============ 统一轮询 ============

  @Benchmark
  public byte[] unifiedPollRequestSerialize() {
    return ProtobufCodec.encode(unifiedPollRequest);
  }

  @Benchmark
  public void unifiedPollRequestDeserialize(Blackhole bh) throws Exception {
    bh.consume(ProtobufCodec.decode(unifiedPollRequestBytes, UnifiedPollRequest.parser()));
  }

  @Benchmark
  public void unifiedPollResponseDeserialize(Blackhole bh) throws Exception {
    bh.consume(ProtobufCodec.decode(unifiedPollResponseBytes, UnifiedPollResponse.parser()));
  }

  // ============ 完整心跳构建+序列化（模拟实际热路径） ============

  @Benchmark
  public byte[] heartbeatFullBuildAndSerialize() {
    StatusRequest request =
        StatusRequest.newBuilder()
            .setAgentId("agent-benchmark-001")
            .setTimestampMillis(System.currentTimeMillis())
            .build();
    return request.toByteArray();
  }
}
