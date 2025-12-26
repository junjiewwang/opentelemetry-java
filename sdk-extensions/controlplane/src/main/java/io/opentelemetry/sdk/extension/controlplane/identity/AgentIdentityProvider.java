/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.identity;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Agent 身份标识提供者
 *
 * <p>生成唯一的 Agent 标识: ${hostname}-${pid}-${startTimeMillis}
 */
public final class AgentIdentityProvider {

  private static final Logger logger = Logger.getLogger(AgentIdentityProvider.class.getName());

  @Nullable private static volatile AgentIdentity instance;
  private static final Object lock = new Object();

  private AgentIdentityProvider() {}

  /**
   * 获取 Agent 身份标识单例
   *
   * @return Agent 身份标识
   */
  public static AgentIdentity get() {
    if (instance == null) {
      synchronized (lock) {
        if (instance == null) {
          instance = createIdentity(null, null);
        }
      }
    }
    return instance;
  }

  /**
   * 初始化 Agent 身份标识 (带服务信息)
   *
   * @param serviceName 服务名称
   * @param serviceNamespace 服务命名空间
   * @return Agent 身份标识
   */
  public static AgentIdentity initialize(
      @Nullable String serviceName, @Nullable String serviceNamespace) {
    synchronized (lock) {
      if (instance == null) {
        instance = createIdentity(serviceName, serviceNamespace);
      }
      return instance;
    }
  }

  private static AgentIdentity createIdentity(
      @Nullable String serviceName, @Nullable String serviceNamespace) {
    String hostname = getHostname();
    long pid = getProcessId();
    long startTime = getStartTime();

    String agentId = String.format(Locale.ROOT, "%s-%d-%d", hostname, pid, startTime);

    return AgentIdentity.builder()
        .setAgentId(agentId)
        .setHostName(hostname)
        .setProcessId(String.valueOf(pid))
        .setStartTimeUnixNano(startTime * 1_000_000) // 转换为纳秒
        .setSdkVersion(getSdkVersion())
        .setServiceName(serviceName != null ? serviceName : getServiceName())
        .setServiceNamespace(serviceNamespace != null ? serviceNamespace : getServiceNamespace())
        .build();
  }

  private static String getHostname() {
    // 优先从环境变量获取
    String hostname = System.getenv("HOSTNAME");
    if (hostname != null && !hostname.isEmpty()) {
      return hostname;
    }

    // 尝试从 COMPUTERNAME 获取 (Windows)
    hostname = System.getenv("COMPUTERNAME");
    if (hostname != null && !hostname.isEmpty()) {
      return hostname;
    }

    // 尝试获取系统 hostname
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      logger.log(Level.WARNING, "Failed to get hostname", e);
      return "unknown-host";
    }
  }

  private static long getProcessId() {
    // 首先尝试 Java 9+ 的 ProcessHandle API
    try {
      Class<?> processHandleClass = Class.forName("java.lang.ProcessHandle");
      Method currentMethod = processHandleClass.getMethod("current");
      Object currentProcess = currentMethod.invoke(null);
      Method pidMethod = processHandleClass.getMethod("pid");
      return (Long) pidMethod.invoke(currentProcess);
    } catch (Exception e) {
      // 如果 ProcessHandle 不可用，回退到 RuntimeMXBean
      logger.log(Level.FINE, "ProcessHandle not available, falling back to RuntimeMXBean", e);
    }

    // 回退方案：使用 RuntimeMXBean
    // Use reflection to call ManagementFactory for Android compatibility (animalsniffer).
    try {
      Class<?> managementFactoryClass = Class.forName("java.lang.management.ManagementFactory");
      Class<?> runtimeMxBeanClass = Class.forName("java.lang.management.RuntimeMXBean");
      Method getRuntimeMxBeanMethod = managementFactoryClass.getMethod("getRuntimeMXBean");
      Object runtimeMxBean = getRuntimeMxBeanMethod.invoke(null);
      // 通过接口类获取方法，避免访问内部实现类 sun.management.RuntimeImpl
      Method getNameMethod = runtimeMxBeanClass.getMethod("getName");
      String runtimeName = (String) getNameMethod.invoke(runtimeMxBean);

      int atIndex = runtimeName.indexOf('@');
      if (atIndex > 0) {
        return Long.parseLong(runtimeName.substring(0, atIndex));
      }
      return Long.parseLong(runtimeName);
    } catch (NumberFormatException e) {
      logger.log(Level.WARNING, "Failed to parse process ID", e);
      return -1;
    } catch (Exception e) {
      logger.log(Level.WARNING, "Failed to get process ID via reflection", e);
      return -1;
    }
  }

  private static long getStartTime() {
    // Use reflection to call ManagementFactory for Android compatibility (animalsniffer).
    try {
      Class<?> managementFactoryClass = Class.forName("java.lang.management.ManagementFactory");
      Class<?> runtimeMxBeanClass = Class.forName("java.lang.management.RuntimeMXBean");
      Method getRuntimeMxBeanMethod = managementFactoryClass.getMethod("getRuntimeMXBean");
      Object runtimeMxBean = getRuntimeMxBeanMethod.invoke(null);
      // 通过接口类获取方法，避免访问内部实现类 sun.management.RuntimeImpl
      Method getStartTimeMethod = runtimeMxBeanClass.getMethod("getStartTime");
      return (Long) getStartTimeMethod.invoke(runtimeMxBean);
    } catch (Exception e) {
      logger.log(Level.WARNING, "Failed to get start time via reflection", e);
      return System.currentTimeMillis();
    }
  }

  private static String getServiceName() {
    String serviceName = System.getProperty("otel.service.name");
    if (serviceName != null && !serviceName.isEmpty()) {
      return serviceName;
    }

    serviceName = System.getenv("OTEL_SERVICE_NAME");
    if (serviceName != null && !serviceName.isEmpty()) {
      return serviceName;
    }

    return "unknown-service";
  }

  private static String getServiceNamespace() {
    String namespace = System.getProperty("otel.service.namespace");
    if (namespace != null && !namespace.isEmpty()) {
      return namespace;
    }

    namespace = System.getenv("OTEL_SERVICE_NAMESPACE");
    if (namespace != null && !namespace.isEmpty()) {
      return namespace;
    }

    return "";
  }

  private static String getSdkVersion() {
    // 尝试从 manifest 或 properties 读取版本
    Package pkg = AgentIdentityProvider.class.getPackage();
    if (pkg != null && pkg.getImplementationVersion() != null) {
      return pkg.getImplementationVersion();
    }
    return "1.0.0";
  }

  /** Agent 身份标识 */
  public static final class AgentIdentity {
    private final String agentId;
    private final String hostName;
    private final String processId;
    private final String sdkVersion;
    private final String serviceName;
    private final String serviceNamespace;
    private final long startTimeUnixNano;
    private final Map<String, String> attributes;

    private AgentIdentity(Builder builder) {
      this.agentId = builder.agentId;
      this.hostName = builder.hostName;
      this.processId = builder.processId;
      this.sdkVersion = builder.sdkVersion;
      this.serviceName = builder.serviceName;
      this.serviceNamespace = builder.serviceNamespace;
      this.startTimeUnixNano = builder.startTimeUnixNano;
      this.attributes = Collections.unmodifiableMap(new HashMap<>(builder.attributes));
    }

    public static Builder builder() {
      return new Builder();
    }

    public String getAgentId() {
      return agentId;
    }

    public String getHostName() {
      return hostName;
    }

    public String getProcessId() {
      return processId;
    }

    public String getSdkVersion() {
      return sdkVersion;
    }

    public String getServiceName() {
      return serviceName;
    }

    public String getServiceNamespace() {
      return serviceNamespace;
    }

    public long getStartTimeUnixNano() {
      return startTimeUnixNano;
    }

    public Map<String, String> getAttributes() {
      return attributes;
    }

    @Override
    public String toString() {
      return "AgentIdentity{"
          + "agentId='"
          + agentId
          + '\''
          + ", hostName='"
          + hostName
          + '\''
          + ", processId='"
          + processId
          + '\''
          + ", serviceName='"
          + serviceName
          + '\''
          + '}';
    }

    /** Builder for AgentIdentity */
    public static final class Builder {
      private String agentId = "";
      private String hostName = "";
      private String processId = "";
      private String sdkVersion = "";
      private String serviceName = "";
      private String serviceNamespace = "";
      private long startTimeUnixNano = 0;
      private Map<String, String> attributes = new HashMap<>();

      private Builder() {}

      public Builder setAgentId(String agentId) {
        this.agentId = Objects.requireNonNull(agentId, "agentId");
        return this;
      }

      public Builder setHostName(String hostName) {
        this.hostName = Objects.requireNonNull(hostName, "hostName");
        return this;
      }

      public Builder setProcessId(String processId) {
        this.processId = Objects.requireNonNull(processId, "processId");
        return this;
      }

      public Builder setSdkVersion(String sdkVersion) {
        this.sdkVersion = Objects.requireNonNull(sdkVersion, "sdkVersion");
        return this;
      }

      public Builder setServiceName(String serviceName) {
        this.serviceName = Objects.requireNonNull(serviceName, "serviceName");
        return this;
      }

      public Builder setServiceNamespace(String serviceNamespace) {
        this.serviceNamespace = Objects.requireNonNull(serviceNamespace, "serviceNamespace");
        return this;
      }

      public Builder setStartTimeUnixNano(long startTimeUnixNano) {
        this.startTimeUnixNano = startTimeUnixNano;
        return this;
      }

      public Builder putAttribute(String key, String value) {
        this.attributes.put(
            Objects.requireNonNull(key, "key"), Objects.requireNonNull(value, "value"));
        return this;
      }

      public Builder setAttributes(Map<String, String> attributes) {
        this.attributes = new HashMap<>(Objects.requireNonNull(attributes, "attributes"));
        return this;
      }

      public AgentIdentity build() {
        return new AgentIdentity(this);
      }
    }
  }
}
