/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.status;

import io.opentelemetry.sdk.extension.controlplane.identity.AgentIdentityProvider;
import java.util.HashMap;
import java.util.Map;

/**
 * 身份信息收集器
 *
 * <p>收集 Agent 的身份标识信息，包括：
 * <ul>
 *   <li>agentId - Agent 唯一标识
 *   <li>hostname - 主机名
 *   <li>processId - 进程 ID
 *   <li>serviceName - 服务名
 *   <li>serviceNamespace - 服务命名空间
 *   <li>sdkVersion - SDK 版本
 *   <li>startTimeUnixNano - 启动时间
 * </ul>
 */
public final class IdentityCollector implements AgentStatusCollector {

  private static final String NAME = "identity";

  private final AgentIdentityProvider.AgentIdentity identity;

  public IdentityCollector() {
    this.identity = AgentIdentityProvider.get();
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Map<String, Object> collect() {
    Map<String, Object> data = new HashMap<>();
    data.put("agentId", identity.getAgentId());
    data.put("hostname", identity.getHostName());
    
    // IP 地址
    String ip = identity.getIp();
    if (ip != null && !ip.isEmpty()) {
      data.put("ip", ip);
    }
    
    data.put("processId", identity.getProcessId());
    data.put("serviceName", identity.getServiceName());
    data.put("serviceNamespace", identity.getServiceNamespace());
    data.put("sdkVersion", identity.getSdkVersion());
    data.put("startupTimestamp", identity.getStartTimeUnixNano() / 1_000_000); // 转为毫秒
    
    // 用户自定义标签
    Map<String, String> labels = identity.getLabels();
    if (labels != null && !labels.isEmpty()) {
      data.put("labels", labels);
    }
    
    return data;
  }

  @Override
  public int getPriority() {
    return 0; // 最高优先级
  }
}
