/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.status;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.Map;

/**
 * 系统资源收集器
 *
 * <p>收集 JVM 和系统资源信息，包括：
 * <ul>
 *   <li>heapMemoryUsed - 堆内存使用量
 *   <li>heapMemoryMax - 堆内存最大值
 *   <li>cpuUsage - CPU 使用率（如可用）
 * </ul>
 */
public final class SystemResourceCollector implements AgentStatusCollector {

  private static final String NAME = "systemResource";

  private final boolean enabled;

  public SystemResourceCollector() {
    this(true);
  }

  public SystemResourceCollector(boolean enabled) {
    this.enabled = enabled;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Map<String, Object> collect() {
    Map<String, Object> data = new HashMap<>();

    // 收集堆内存信息
    MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage heapUsage = memoryMxBean.getHeapMemoryUsage();
    data.put("heapMemoryUsed", heapUsage.getUsed());
    data.put("heapMemoryMax", heapUsage.getMax());
    data.put("heapMemoryCommitted", heapUsage.getCommitted());

    // 收集非堆内存信息
    MemoryUsage nonHeapUsage = memoryMxBean.getNonHeapMemoryUsage();
    data.put("nonHeapMemoryUsed", nonHeapUsage.getUsed());

    // 收集线程信息
    java.lang.management.ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
    data.put("threadCount", threadMxBean.getThreadCount());
    data.put("daemonThreadCount", threadMxBean.getDaemonThreadCount());

    // 收集 GC 信息
    long gcCount = 0;
    long gcTimeMillis = 0;
    for (java.lang.management.GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      long count = gcBean.getCollectionCount();
      long time = gcBean.getCollectionTime();
      if (count >= 0) {
        gcCount += count;
      }
      if (time >= 0) {
        gcTimeMillis += time;
      }
    }
    data.put("gcCount", gcCount);
    data.put("gcTimeMillis", gcTimeMillis);

    // 尝试收集 CPU 使用率
    double cpuUsage = getCpuUsage();
    if (cpuUsage >= 0) {
      data.put("cpuUsage", cpuUsage);
    }

    // 收集系统负载
    OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
    double loadAverage = osMxBean.getSystemLoadAverage();
    if (loadAverage >= 0) {
      data.put("systemLoadAverage", loadAverage);
    }

    return data;
  }

  @Override
  public int getPriority() {
    return 30;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * 获取 CPU 使用率
   *
   * @return CPU 使用率 (0.0 ~ 1.0)，如果不可用返回 -1
   */
  private static double getCpuUsage() {
    OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();

    // 尝试使用 com.sun.management.OperatingSystemMXBean 获取精确 CPU 使用率
    if (osMxBean instanceof com.sun.management.OperatingSystemMXBean) {
      com.sun.management.OperatingSystemMXBean sunOsMxBean =
          (com.sun.management.OperatingSystemMXBean) osMxBean;
      double cpuLoad = sunOsMxBean.getProcessCpuLoad();
      if (cpuLoad >= 0) {
        return cpuLoad;
      }
    }

    // 回退到系统负载平均值（不太精确）
    double loadAverage = osMxBean.getSystemLoadAverage();
    if (loadAverage >= 0) {
      int processors = osMxBean.getAvailableProcessors();
      // 粗略估算：负载 / 处理器数
      return Math.min(1.0, loadAverage / processors);
    }

    return -1;
  }
}
