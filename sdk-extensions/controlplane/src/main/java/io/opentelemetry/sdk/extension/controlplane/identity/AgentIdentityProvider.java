/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.identity;

import io.opentelemetry.sdk.extension.controlplane.proto.v1.CommonProtos;
import java.lang.reflect.Method;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
   */
  public static void initialize(
      @Nullable String serviceName, @Nullable String serviceNamespace) {
    synchronized (lock) {
      if (instance == null) {
        instance = createIdentity(serviceName, serviceNamespace);
      }
    }
  }

  // ===== 便捷静态方法（简化调用） =====

  /**
   * 获取 Agent ID
   *
   * <p>便捷方法，等价于 {@code get().getAgentId()}
   *
   * @return Agent ID
   */
  public static String getAgentId() {
    return get().getAgentId();
  }

  /**
   * 获取服务名称
   *
   * <p>便捷方法，等价于 {@code get().getServiceName()}
   *
   * @return 服务名称
   */
  public static String getServiceName() {
    return get().getServiceName();
  }

  /**
   * 获取服务命名空间
   *
   * <p>便捷方法，等价于 {@code get().getServiceNamespace()}
   *
   * @return 服务命名空间
   */
  public static String getServiceNamespace() {
    return get().getServiceNamespace();
  }

  /**
   * 获取主机名
   *
   * <p>便捷方法，等价于 {@code get().getHostName()}
   *
   * @return 主机名
   */
  public static String getHostName() {
    return get().getHostName();
  }

  /**
   * 获取 IP 地址
   *
   * <p>便捷方法，等价于 {@code get().getIp()}
   *
   * @return IP 地址
   */
  public static String getIp() {
    return get().getIp();
  }

  /**
   * 获取进程 ID
   *
   * <p>便捷方法，等价于 {@code get().getProcessId()}
   *
   * @return 进程 ID
   */
  public static String getProcessId() {
    return get().getProcessId();
  }

  private static AgentIdentity createIdentity(
      @Nullable String serviceName, @Nullable String serviceNamespace) {
    String hostname = detectHostname();
    long pid = detectProcessId();
    long startTime = detectStartTime();

    String agentId = String.format(Locale.ROOT, "%s-%d-%d", hostname, pid, startTime);

    return AgentIdentity.builder()
        .setAgentId(agentId)
        .setHostName(hostname)
        .setIp(detectIpAddress())
        .setProcessId(String.valueOf(pid))
        .setStartTimeMillis(startTime) // 毫秒时间戳
        .setSdkVersion(detectSdkVersion())
        .setServiceName(serviceName != null ? serviceName : detectServiceName())
        .setServiceNamespace(serviceNamespace != null ? serviceNamespace : detectServiceNamespace())
        .setLabels(detectLabels())
        .build();
  }

  private static String detectHostname() {
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

  private static long detectProcessId() {
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

  private static long detectStartTime() {
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

  private static String detectServiceName() {
    String serviceName = System.getProperty("otel.service.name");
    if (serviceName != null && !serviceName.isEmpty()) {
      return serviceName;
    }

    serviceName = System.getenv("OTEL_SERVICE_NAME");
    if (serviceName != null && !serviceName.isEmpty()) {
      return serviceName;
    }

    return "";
  }

  private static String detectServiceNamespace() {
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

  /**
   * 获取 IP 地址
   *
   * <p>优先级：
   * <ol>
   *   <li>环境变量 POD_IP（K8s Downward API 注入）</li>
   *   <li>环境变量 HOST_IP（自定义配置）</li>
   *   <li>遍历网卡获取非回环 IPv4 地址</li>
   *   <li>兜底返回 unknown</li>
   * </ol>
   *
   * @return IP 地址，如果无法获取则返回 unknown
   */
  private static String detectIpAddress() {
    // 1. 从环境变量 POD_IP 获取（K8s Downward API 注入）
    String ip = System.getenv("POD_IP");
    if (ip != null && !ip.isEmpty()) {
      return ip;
    }

    // 2. 从环境变量 HOST_IP 获取（自定义配置）
    ip = System.getenv("HOST_IP");
    if (ip != null && !ip.isEmpty()) {
      return ip;
    }

    // 3. 尝试从网络接口获取
    try {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while (interfaces.hasMoreElements()) {
        NetworkInterface ni = interfaces.nextElement();
        if (ni.isLoopback() || !ni.isUp()) {
          continue;
        }
        Enumeration<InetAddress> addresses = ni.getInetAddresses();
        while (addresses.hasMoreElements()) {
          InetAddress addr = addresses.nextElement();
          if (addr instanceof Inet4Address && !addr.isLoopbackAddress()) {
            return addr.getHostAddress();
          }
        }
      }
    } catch (SocketException e) {
      logger.log(Level.WARNING, "Failed to get IP addresses from network interfaces", e);
    }

    return "unknown";
  }

  /**
   * 获取用户自定义标签
   *
   * <p>标签来源（按优先级）：
   * <ol>
   *   <li>系统属性 otel.agent.labels (如 -Dotel.agent.labels=env=prod,region=cn-east)
   *   <li>环境变量 OTEL_AGENT_LABELS
   *   <li>自动检测的 Kubernetes 相关标签
   * </ol>
   *
   * @return 标签 Map，如果没有配置则返回空 Map
   */
  private static Map<String, String> detectLabels() {
    Map<String, String> labels = new LinkedHashMap<>();

    // 1. 从系统属性获取 (如 -Dotel.agent.labels=env=prod,region=cn-east)
    String labelsStr = System.getProperty("otel.agent.labels");
    if (labelsStr == null || labelsStr.isEmpty()) {
      // 2. 从环境变量获取
      labelsStr = System.getenv("OTEL_AGENT_LABELS");
    }

    if (labelsStr != null && !labelsStr.isEmpty()) {
      for (String pair : labelsStr.split(",")) {
        String[] kv = pair.split("=", 2);
        if (kv.length == 2) {
          String key = kv[0].trim();
          String value = kv[1].trim();
          if (!key.isEmpty()) {
            labels.put(key, value);
          }
        }
      }
    }

    // 3. 添加自动检测的 Kubernetes 标签
    String podName = System.getenv("POD_NAME");
    if (podName != null && !podName.isEmpty() && !labels.containsKey("k8s.pod.name")) {
      labels.put("k8s.pod.name", podName);
    }
    String podNamespace = System.getenv("POD_NAMESPACE");
    if (podNamespace != null && !podNamespace.isEmpty() && !labels.containsKey("k8s.namespace")) {
      labels.put("k8s.namespace", podNamespace);
    }
    String nodeName = System.getenv("NODE_NAME");
    if (nodeName != null && !nodeName.isEmpty() && !labels.containsKey("k8s.node.name")) {
      labels.put("k8s.node.name", nodeName);
    }

    return labels;
  }

  private static String detectSdkVersion() {
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
    private final String ip;
    private final String processId;
    private final String sdkVersion;
    private final String serviceName;
    private final String serviceNamespace;
    private final long startTimeMillis;
    private final Map<String, String> labels;
    private final Map<String, String> attributes;

    private AgentIdentity(Builder builder) {
      this.agentId = builder.agentId;
      this.hostName = builder.hostName;
      this.ip = builder.ip;
      this.processId = builder.processId;
      this.sdkVersion = builder.sdkVersion;
      this.serviceName = builder.serviceName;
      this.serviceNamespace = builder.serviceNamespace;
      this.startTimeMillis = builder.startTimeMillis;
      this.labels = Collections.unmodifiableMap(new LinkedHashMap<>(builder.labels));
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

    public String getIp() {
      return ip;
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

    public long getStartTimeMillis() {
      return startTimeMillis;
    }

    public Map<String, String> getLabels() {
      return labels;
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
          + ", ip='"
          + ip
          + '\''
          + ", processId='"
          + processId
          + '\''
          + ", serviceName='"
          + serviceName
          + '\''
          + ", labels="
          + labels
          + '}';
    }

    /**
     * 转换为 Protobuf AgentIdentity 对象
     *
     * <p>字段映射：
     * <ul>
     *   <li>startTimeMillis 直接映射到 start_time_millis</li>
     *   <li>labels 合并到 attributes 中（加 "label." 前缀）</li>
     * </ul>
     *
     * @return Protobuf AgentIdentity 对象
     */
    public CommonProtos.AgentIdentity toProto() {
      // 合并 labels 和 attributes 到 proto 的 attributes 字段
      Map<String, String> mergedAttributes = new HashMap<>(attributes);
      // labels 也作为 attributes 的一部分（加上 label. 前缀以区分）
      for (Map.Entry<String, String> entry : labels.entrySet()) {
        mergedAttributes.put("label." + entry.getKey(), entry.getValue());
      }

      return CommonProtos.AgentIdentity.newBuilder()
          .setAgentId(agentId)
          .setHostName(hostName)
          .setIp(ip)
          .setProcessId(processId)
          .setSdkVersion(sdkVersion)
          .setServiceName(serviceName)
          .setServiceNamespace(serviceNamespace)
          .setStartTimeMillis(startTimeMillis)
          .putAllAttributes(mergedAttributes)
          .build();
    }

    /**
     * 从 Protobuf AgentIdentity 对象转换
     *
     * <p>字段映射：
     * <ul>
     *   <li>start_time_millis 直接映射到 startTimeMillis</li>
     *   <li>attributes 中 "label." 前缀的提取到 labels</li>
     * </ul>
     *
     * @param proto Protobuf AgentIdentity 对象
     * @return Java AgentIdentity 对象
     */
    public static AgentIdentity fromProto(CommonProtos.AgentIdentity proto) {
      Map<String, String> labels = new LinkedHashMap<>();
      Map<String, String> attributes = new HashMap<>();

      // 分离 labels 和 attributes
      for (Map.Entry<String, String> entry : proto.getAttributesMap().entrySet()) {
        if (entry.getKey().startsWith("label.")) {
          labels.put(entry.getKey().substring(6), entry.getValue()); // 移除 "label." 前缀
        } else {
          attributes.put(entry.getKey(), entry.getValue());
        }
      }

      return AgentIdentity.builder()
          .setAgentId(proto.getAgentId())
          .setHostName(proto.getHostName())
          .setIp(proto.getIp())
          .setProcessId(proto.getProcessId())
          .setSdkVersion(proto.getSdkVersion())
          .setServiceName(proto.getServiceName())
          .setServiceNamespace(proto.getServiceNamespace())
          .setStartTimeMillis(proto.getStartTimeMillis())
          .setLabels(labels)
          .setAttributes(attributes)
          .build();
    }

    /** Builder for AgentIdentity */
    public static final class Builder {
      private String agentId = "";
      private String hostName = "";
      private String ip = "";
      private String processId = "";
      private String sdkVersion = "";
      private String serviceName = "";
      private String serviceNamespace = "";
      private long startTimeMillis = 0;
      private Map<String, String> labels = new LinkedHashMap<>();
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

      public Builder setIp(String ip) {
        this.ip = Objects.requireNonNull(ip, "ip");
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

      public Builder setStartTimeMillis(long startTimeMillis) {
        this.startTimeMillis = startTimeMillis;
        return this;
      }

      public Builder setLabels(Map<String, String> labels) {
        this.labels = new LinkedHashMap<>(Objects.requireNonNull(labels, "labels"));
        return this;
      }

      public Builder putLabel(String key, String value) {
        this.labels.put(
            Objects.requireNonNull(key, "key"), Objects.requireNonNull(value, "value"));
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
