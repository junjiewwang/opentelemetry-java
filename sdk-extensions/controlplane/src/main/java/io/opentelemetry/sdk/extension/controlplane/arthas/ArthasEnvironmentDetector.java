/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 环境检测器
 *
 * <p>检测当前运行环境，确定可用功能和所需原生库。
 */
public final class ArthasEnvironmentDetector {

  private static final Logger logger = Logger.getLogger(ArthasEnvironmentDetector.class.getName());

  /** 操作系统类型 */
  public enum OsType {
    LINUX,
    MACOS,
    WINDOWS,
    UNKNOWN
  }

  /** CPU 架构 */
  public enum CpuArch {
    X86_64,
    AARCH64,
    X86,
    UNKNOWN
  }

  /** C 库类型（Linux 专用） */
  public enum LibcType {
    GLIBC,
    MUSL,
    NA // 非 Linux 系统
  }

  private ArthasEnvironmentDetector() {}

  /**
   * 检测当前环境
   *
   * @return 环境信息
   */
  public static Environment detect() {
    OsType osType = detectOsType();
    CpuArch cpuArch = detectCpuArch();
    LibcType libcType = detectLibcType(osType);
    boolean jdkAvailable = checkJdkAvailable();
    boolean perfEventsAvailable = checkPerfEvents(osType);
    boolean preserveFramePointer = checkPreserveFramePointer();
    Set<String> availableProfilerEvents = detectProfilerEvents(osType, perfEventsAvailable);

    Environment env =
        new Environment(
            osType,
            cpuArch,
            libcType,
            jdkAvailable,
            perfEventsAvailable,
            preserveFramePointer,
            availableProfilerEvents);

    logger.log(Level.INFO, "Detected environment: {0}", env);

    return env;
  }

  /** 检测操作系统类型 */
  private static OsType detectOsType() {
    String osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (osName.contains("linux")) {
      return OsType.LINUX;
    } else if (osName.contains("mac") || osName.contains("darwin")) {
      return OsType.MACOS;
    } else if (osName.contains("windows")) {
      return OsType.WINDOWS;
    }
    return OsType.UNKNOWN;
  }

  /** 检测 CPU 架构 */
  private static CpuArch detectCpuArch() {
    String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
    if (arch.contains("amd64") || arch.contains("x86_64")) {
      return CpuArch.X86_64;
    } else if (arch.contains("aarch64") || arch.contains("arm64")) {
      return CpuArch.AARCH64;
    } else if (arch.contains("x86") || arch.contains("i386") || arch.contains("i686")) {
      return CpuArch.X86;
    }
    return CpuArch.UNKNOWN;
  }

  /** 检测 C 库类型（区分 glibc 和 musl） */
  private static LibcType detectLibcType(OsType osType) {
    if (osType != OsType.LINUX) {
      return LibcType.NA;
    }

    // 方法 1: 检查 /lib/ld-musl-* 是否存在
    File libDir = new File("/lib");
    if (libDir.exists() && libDir.isDirectory()) {
      String[] muslFiles = libDir.list((dir, name) -> name.startsWith("ld-musl-"));
      if (muslFiles != null && muslFiles.length > 0) {
        return LibcType.MUSL;
      }
    }

    // 方法 2: 检查 /lib/x86_64-linux-musl/
    if (new File("/lib/x86_64-linux-musl").exists()
        || new File("/lib/aarch64-linux-musl").exists()) {
      return LibcType.MUSL;
    }

    // 方法 3: 执行 ldd --version 检查
    try {
      ProcessBuilder pb = new ProcessBuilder("ldd", "--version");
      pb.redirectErrorStream(true);
      Process p = pb.start();
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.toLowerCase(Locale.ROOT).contains("musl")) {
            return LibcType.MUSL;
          }
        }
      }
      p.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.log(Level.FINE, "Interrupted while detecting libc type: {0}", e.getMessage());
    } catch (IOException e) {
      logger.log(Level.FINE, "Failed to detect libc type via ldd: {0}", e.getMessage());
    }

    return LibcType.GLIBC;
  }

  /** 检查是否有 JDK（而非仅 JRE） */
  private static boolean checkJdkAvailable() {
    // JDK 9+ 使用模块系统
    try {
      Class.forName("com.sun.tools.attach.VirtualMachine");
      return true;
    } catch (ClassNotFoundException e) {
      // 继续检查
    }

    // JDK 8 检查 tools.jar
    String javaHome = System.getProperty("java.home");
    if (javaHome != null) {
      // java.home 通常指向 jre 目录，需要检查上级目录的 lib/tools.jar
      Path toolsJar = Paths.get(javaHome, "..", "lib", "tools.jar");
      if (Files.exists(toolsJar)) {
        return true;
      }

      // 或者 java.home 直接指向 jdk 目录
      toolsJar = Paths.get(javaHome, "lib", "tools.jar");
      if (Files.exists(toolsJar)) {
        return true;
      }
    }

    // 检查 jdk.attach 模块是否可用（JDK 9+）
    // 注意：ModuleLayer 是 Java 9+ API，这里使用反射来兼容 Java 8
    try {
      Class<?> moduleLayerClass = Class.forName("java.lang.ModuleLayer");
      Object bootLayer = moduleLayerClass.getMethod("boot").invoke(null);
      moduleLayerClass.getMethod("findModule", String.class).invoke(bootLayer, "jdk.attach");
      return true;
    } catch (Exception e) {
      // ignore - Java 8 or module not found
    }

    return false;
  }

  /** 检查 perf_events 是否可用 */
  private static boolean checkPerfEvents(OsType osType) {
    if (osType != OsType.LINUX) {
      return false;
    }

    // 检查 perf_event_paranoid 设置
    try {
      Path paranoidPath = Paths.get("/proc/sys/kernel/perf_event_paranoid");
      if (Files.exists(paranoidPath)) {
        // 使用 Java 8 兼容的方式读取文件
        String content = new String(Files.readAllBytes(paranoidPath), StandardCharsets.UTF_8).trim();
        int level = Integer.parseInt(content);
        // level <= 1 允许用户态 profiling
        // level <= 0 允许内核态 profiling
        // level == -1 无限制
        return level <= 1;
      }
    } catch (IOException | NumberFormatException e) {
      logger.log(Level.FINE, "Failed to check perf_event_paranoid: {0}", e.getMessage());
    }

    return false;
  }

  /** 检查 PreserveFramePointer 是否启用 */
  private static boolean checkPreserveFramePointer() {
    // 检查 JVM 参数
    for (String arg : java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      if (arg.contains("PreserveFramePointer")) {
        return arg.contains("+PreserveFramePointer");
      }
    }
    return false;
  }

  /** 检测可用的 profiler 事件 */
  private static Set<String> detectProfilerEvents(OsType osType, boolean perfEventsAvailable) {
    Set<String> events = new HashSet<>();

    // 基础事件总是可用
    events.add("wall");   // Wall-clock time
    events.add("itimer"); // Interval timer

    // CPU profiling
    if (perfEventsAvailable || osType == OsType.MACOS) {
      events.add("cpu");
    }

    // 内存分配（总是可用）
    events.add("alloc");

    // Lock contention（总是可用）
    events.add("lock");

    return Collections.unmodifiableSet(events);
  }

  /**
   * 获取平台路径（用于选择原生库）
   *
   * @param env 环境信息
   * @return 平台路径，如果不支持返回 null
   */
  @Nullable
  public static String getPlatformPath(Environment env) {
    switch (env.getOsType()) {
      case LINUX:
        String arch = env.getCpuArch() == CpuArch.X86_64 ? "x64" : "aarch64";
        String libc = env.getLibcType() == LibcType.MUSL ? "-musl" : "";
        return "linux" + libc + "-" + arch;
      case MACOS:
        return "macos";
      case WINDOWS:
        return "windows-" + (env.getCpuArch() == CpuArch.X86_64 ? "x64" : "x86");
      default:
        return null;
    }
  }

  /** 环境信息 */
  public static final class Environment {
    private final OsType osType;
    private final CpuArch cpuArch;
    private final LibcType libcType;
    private final boolean jdkAvailable;
    private final boolean perfEventsAvailable;
    private final boolean preserveFramePointer;
    private final Set<String> availableProfilerEvents;

    Environment(
        OsType osType,
        CpuArch cpuArch,
        LibcType libcType,
        boolean jdkAvailable,
        boolean perfEventsAvailable,
        boolean preserveFramePointer,
        Set<String> availableProfilerEvents) {
      this.osType = osType;
      this.cpuArch = cpuArch;
      this.libcType = libcType;
      this.jdkAvailable = jdkAvailable;
      this.perfEventsAvailable = perfEventsAvailable;
      this.preserveFramePointer = preserveFramePointer;
      this.availableProfilerEvents = availableProfilerEvents;
    }

    public OsType getOsType() {
      return osType;
    }

    public CpuArch getCpuArch() {
      return cpuArch;
    }

    public LibcType getLibcType() {
      return libcType;
    }

    public boolean isJdkAvailable() {
      return jdkAvailable;
    }

    public boolean isPerfEventsAvailable() {
      return perfEventsAvailable;
    }

    public boolean isPreserveFramePointer() {
      return preserveFramePointer;
    }

    public Set<String> getAvailableProfilerEvents() {
      return availableProfilerEvents;
    }

    /** 是否支持 Arthas */
    public boolean isArthasSupported() {
      return jdkAvailable && osType != OsType.UNKNOWN && cpuArch != CpuArch.UNKNOWN;
    }

    /** 获取不支持的原因 */
    @Nullable
    public String getUnsupportedReason() {
      if (!jdkAvailable) {
        return "JDK required but only JRE detected. Please use JDK-based image.";
      }
      if (osType == OsType.UNKNOWN) {
        return "Unsupported operating system: " + System.getProperty("os.name");
      }
      if (cpuArch == CpuArch.UNKNOWN) {
        return "Unsupported CPU architecture: " + System.getProperty("os.arch");
      }
      return null;
    }

    @Override
    public String toString() {
      return "Environment{"
          + "os="
          + osType
          + ", arch="
          + cpuArch
          + ", libc="
          + libcType
          + ", jdk="
          + jdkAvailable
          + ", perfEvents="
          + perfEventsAvailable
          + ", framePointer="
          + preserveFramePointer
          + ", profilerEvents="
          + availableProfilerEvents
          + '}';
    }
  }
}
