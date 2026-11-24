/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import io.opentelemetry.sdk.extension.controlplane.identity.AgentIdentityProvider;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Arthas 临时目录统一管理者（JVM 级 Singleton）
 *
 * <p>统一管理 Arthas 运行时所需的所有临时目录，支持跨 attach 周期复用，避免每次
 * attach/detach 周期在 /tmp 下堆积新的临时目录。
 *
 * <p>管理的目录分两类，生命周期严格隔离：
 * <ul>
 *   <li><b>共享（不可变）</b>：{@link DirType#ARTHAS_HOME} 由
 *       {@link ArthasResourceBundle} 以内容寻址方式发布到固定根，跨 JVM 只读复用，
 *       不参与任何 JVM 退出清理（I3）。可用
 *       {@code otel.controlplane.arthas.bundle.enabled=false} 回滚</li>
 *   <li><b>进程私有</b>：{@link DirType#SPY_JAR}、{@link DirType#LOGS} 仍每进程独立，
 *       由 {@link #cleanupAll()} 与残留扫描管理</li>
 * </ul>
 *
 * <p>设计原则：
 * <ul>
 *   <li><b>SRP</b>：只管理临时目录生命周期</li>
 *   <li><b>高内聚</b>：所有目录操作（命名、创建、缓存、复用、校验、清理）集中于此</li>
 *   <li><b>低耦合</b>：不依赖任何 Arthas 组件，通过 {@link #getInstance()} 暴露</li>
 *   <li><b>健壮</b>：ConcurrentHashMap 线程安全 + 文件有效性校验 + shutdown hook 兜底</li>
 *   <li><b>可辨识</b>：目录名含时间戳与 PID，并写入 {@code .arthas-dir.meta} 标记文件</li>
 *   <li><b>可扩展</b>：新增资源类型只需添加 {@link DirType} 枚举值</li>
 * </ul>
 *
 * <p>目录命名格式：{@code <prefix><yyyyMMdd-HHmmss>-p<pid>-<jdkRandom>}，
 * 例如 {@code arthas-logs-20261124-164530-p12345-6921006666689707971}。
 * 后缀随机数由 {@link Files#createTempDirectory(String, java.nio.file.attribute.FileAttribute...)}
 * 追加，用于保证同一秒内的唯一性。
 *
 * <p>残留目录清理默认开启：首次创建目录时按类型分组，每类只保留最近 2 个，其余删除。
 * 可通过 {@code otel.controlplane.arthas.tmp.cleanup.enabled=false} 关闭，
 * 通过 {@code otel.controlplane.arthas.tmp.cleanup.max.keep.per.type} 调整保留数量。
 *
 * <p>使用示例：
 * <pre>{@code
 * ArthasTempDirectoryManager manager = ArthasTempDirectoryManager.getInstance();
 *
 * // 获取或创建 arthas-home 目录（首次创建，后续复用）
 * Path arthasHome = manager.getOrCreateDir(DirType.ARTHAS_HOME);
 *
 * // JVM 关闭前主动清理（已由 shutdown hook 自动调用，通常无需手动调用）
 * manager.cleanupAll();
 * }</pre>
 */
public final class ArthasTempDirectoryManager {

  private static final Logger logger =
      Logger.getLogger(ArthasTempDirectoryManager.class.getName());

  private static final ArthasTempDirectoryManager INSTANCE =
      new ArthasTempDirectoryManager();

  /** 元数据标记文件名 */
  private static final String META_FILE_NAME = ".arthas-dir.meta";

  /** 元数据 owner 字段值（用于识别本组件创建的目录） */
  private static final String META_OWNER = "otel-controlplane";

  /** PID 合法字符（用于降级与文件名安全） */
  private static final Pattern SAFE_PID = Pattern.compile("[0-9A-Za-z_-]{1,32}");

  /**
   * 本组件管理的目录名模式：兼容新旧两种格式。
   *
   * <ul>
   *   <li>旧格式：{@code arthas-1383043882854248061}（前缀 + 纯随机数字）</li>
   *   <li>新格式：{@code arthas-logs-20261124-164530-p12345-6921006666689707971}</li>
   * </ul>
   */
  private static final Pattern MANAGED_DIR_NAME = Pattern.compile(
      "^(?:arthas|arthas-logs|arthas-spy-jar)-"
          + "(?:\\d{8}-\\d{6}-p[0-9A-Za-z_-]{1,32}-)?"
          + "\\d+$");

  /** 判断目录名是否为本组件管理（兼容新旧格式）；包级私有便于单测。 */
  static boolean isManagedDirName(String name) {
    return name != null && MANAGED_DIR_NAME.matcher(name).matches();
  }

  /** 残留清理总开关（默认开启；设为 false 可完全关闭，用于回滚） */
  private static final String PROP_CLEANUP_ENABLED =
      "otel.controlplane.arthas.tmp.cleanup.enabled";

  /** 每类目录最多保留数量（默认 2） */
  private static final String PROP_MAX_KEEP_PER_TYPE =
      "otel.controlplane.arthas.tmp.cleanup.max.keep.per.type";

  private static final int DEFAULT_MAX_KEEP_PER_TYPE = 2;

  /**
   * 近期活跃保护窗口（毫秒）。
   *
   * <p>默认开启清理后，必须避免误删其他 JVM 进程正在使用的目录：目录内最近一次文件修改
   * 在此窗口内的，一律视为活跃并跳过，无论数量是否超限。
   */
  private static final long ACTIVE_PROTECTION_MILLIS = 10L * 60 * 1000;

  /** 已缓存的目录路径（线程安全，跨 attach 周期持久） */
  private final Map<DirType, Path> dirCache = new ConcurrentHashMap<>();

  /**
   * 共享内容寻址 bundle 目录。
   *
   * <p>刻意不放入 {@link #dirCache}：{@link #cleanupAll()} 只清理本进程私有目录，
   * 共享 bundle 由所有 JVM 只读复用，任何单个 JVM 退出都不能删除它（I3）。
   */
  @Nullable private volatile Path sharedBundle;

  /** 残留扫描只在首次创建目录时执行一次 */
  private volatile boolean staleDirsScanned = false;

  private ArthasTempDirectoryManager() {
    registerShutdownHook();
  }

  /**
   * 注册 JVM 关闭钩子，主动递归清理本进程创建的临时目录。
   *
   * <p>{@code deleteOnExit()} 只能删空目录，而 Arthas 目录内会写入 JAR、日志与子目录，
   * 必须递归删除。钩子失败只记录日志，不影响 JVM 退出。
   */
  private void registerShutdownHook() {
    try {
      Thread hook = new Thread(this::cleanupAllQuietly, "otel-arthas-tmp-cleanup");
      hook.setDaemon(true);
      Runtime.getRuntime().addShutdownHook(hook);
      logger.log(Level.FINE, "[ArthasTempDir] Shutdown hook registered");
    } catch (IllegalStateException | SecurityException e) {
      // JVM 已在关闭中，或安全管理器禁止注册：不影响主流程
      logger.log(Level.FINE, "[ArthasTempDir] Failed to register shutdown hook: {0}",
          e.getMessage());
    }
  }

  private void cleanupAllQuietly() {
    try {
      cleanupAll();
    } catch (RuntimeException e) {
      logger.log(Level.FINE, "[ArthasTempDir] Shutdown cleanup failed: {0}", e.getMessage());
    }
  }

  /**
   * 获取全局单例
   *
   * @return 全局实例
   */
  public static ArthasTempDirectoryManager getInstance() {
    return INSTANCE;
  }

  /**
   * 获取或创建指定类型的临时目录（幂等）
   *
   * <p>策略：
   * <ol>
   *   <li>{@code ARTHAS_HOME} → 优先返回共享内容寻址 bundle；失败降级到私有目录</li>
   *   <li>缓存命中且目录有效 → 直接复用</li>
   *   <li>缓存未命中或目录失效 → 创建带时间戳/PID 的新目录、写入 meta 并缓存</li>
   * </ol>
   *
   * <p>首次调用时执行一次历史残留扫描（含共享 bundle 与 staging 回收）。
   *
   * @param type 目录类型
   * @return 目录路径，创建失败抛出 RuntimeException
   */
  public Path getOrCreateDir(DirType type) {
    scanStaleDirsOnce();

    // 0. ARTHAS_HOME 优先复用跨进程共享的内容寻址 bundle（不可变）
    //    resolve() 失败时静默降级到下面的每进程私有目录
    if (type == DirType.ARTHAS_HOME) {
      Path cachedBundle = sharedBundle;
      if (cachedBundle != null && isDirValid(cachedBundle, type)) {
        return cachedBundle;
      }
      Path bundle = ArthasResourceBundle.resolve();
      if (bundle != null) {
        sharedBundle = bundle;
        return bundle;
      }
    }

    // 1. 尝试从缓存获取
    Path cached = dirCache.get(type);
    if (cached != null && isDirValid(cached, type)) {
      logger.log(Level.FINE, "[ArthasTempDir] Reusing cached dir for {0}: {1}",
          new Object[] {type, cached});
      return cached;
    }

    // 2. 缓存中的目录已失效，清理
    if (cached != null) {
      logger.log(Level.INFO, "[ArthasTempDir] Cached dir for {0} is invalid, will create new: {1}",
          new Object[] {type, cached});
      dirCache.remove(type);
    }

    // 3. 创建新目录
    try {
      String pid = resolvePid();
      Path newDir = Files.createTempDirectory(buildDirPrefix(type, pid));
      // 保留兜底：目录为空时 deleteOnExit 仍生效；非空目录由 shutdown hook 递归删除
      newDir.toFile().deleteOnExit();
      writeMetaFile(newDir, type, pid);
      dirCache.put(type, newDir);
      logger.log(Level.INFO, "[ArthasTempDir] Created new dir for {0}: {1}",
          new Object[] {type, newDir});
      return newDir;
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to create temp directory for " + type + ": " + e.getMessage(), e);
    }
  }

  /**
   * 构建可辨识的目录前缀：{@code <prefix><yyyyMMdd-HHmmss>-p<pid>-}。
   *
   * <p>PID 复用 {@link AgentIdentityProvider#getProcessId()}（已处理 Java 8 兼容），
   * 获取失败降级为 {@code unknown}，不阻塞目录创建。
   */
  private static String buildDirPrefix(DirType type, String pid) {
    String timestamp = String.format(Locale.ROOT, "%1$tY%1$tm%1$td-%1$tH%1$tM%1$tS",
        System.currentTimeMillis());
    return type.getPrefix() + timestamp + "-p" + pid + "-";
  }

  private static String resolvePid() {
    try {
      String pid = AgentIdentityProvider.getProcessId();
      if (pid != null && SAFE_PID.matcher(pid).matches()) {
        return pid;
      }
    } catch (RuntimeException e) {
      logger.log(Level.FINE, "[ArthasTempDir] Failed to resolve PID, using fallback: {0}",
          e.getMessage());
    }
    return "unknown";
  }

  /** 写入元数据标记文件，便于事后离线对账；失败只记录日志，不阻塞目录创建。 */
  private static void writeMetaFile(Path dir, DirType type, String pid) {
    String content = "owner=" + META_OWNER + "\n"
        + "dir.type=" + type.name() + "\n"
        + "pid=" + pid + "\n"
        + "created.at=" + Instant.now() + "\n";
    try {
      Files.write(dir.resolve(META_FILE_NAME), content.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      logger.log(Level.WARNING, "[ArthasTempDir] Failed to write meta file in {0}: {1}",
          new Object[] {dir, e.getMessage()});
    }
  }

  /**
   * 校验目录中的核心文件是否完整有效
   *
   * @param dir 目录路径
   * @param type 目录类型（用于确定校验策略）
   * @return true 表示目录有效可复用
   */
  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  boolean isDirValid(Path dir, DirType type) {
    if (!Files.isDirectory(dir)) {
      return false;
    }

    switch (type) {
      case ARTHAS_HOME:
        // 核心 JAR 必须存在
        Path coreJar = dir.resolve("arthas-core.jar");
        return Files.isRegularFile(coreJar) && isFileNonEmpty(coreJar);

      case SPY_JAR:
        // Spy JAR 必须存在
        Path spyJar = dir.resolve("arthas-spy.jar");
        return Files.isRegularFile(spyJar) && isFileNonEmpty(spyJar);

      case LOGS:
        // 日志目录只要存在即可（可能为空）
        return true;
    }
    throw new AssertionError("unreachable, type=" + type);
  }

  /**
   * 主动清理所有临时目录（幂等、线程安全）
   *
   * <p>由 shutdown hook 自动调用；{@link java.io.File#deleteOnExit()} 仅作为空目录兜底。
   * 调用后 {@link #getOrCreateDir(DirType)} 会在下次访问时创建新目录。
   */
  public synchronized void cleanupAll() {
    // 共享 bundle（sharedBundle）刻意不在此清理：跨进程只读复用，由 I3 保证
    for (Map.Entry<DirType, Path> entry : dirCache.entrySet()) {
      Path dir = entry.getValue();
      try {
        deleteRecursively(dir);
        logger.log(Level.FINE, "[ArthasTempDir] Cleaned up dir for {0}: {1}",
            new Object[] {entry.getKey(), dir});
      } catch (IOException e) {
        logger.log(Level.WARNING, "[ArthasTempDir] Failed to clean up dir for {0}: {1}: {2}",
            new Object[] {entry.getKey(), dir, e.getMessage()});
      }
    }
    dirCache.clear();
  }

  /**
   * 首次创建目录时执行一次残留清理（默认开启）。
   *
   * <p>策略：按目录类型分组，每组按「目录内最新文件 mtime」倒序，保留最近
   * {@code maxKeepPerType} 个（默认 2），其余判定为残留并删除。
   *
   * <p>安全约束：
   * <ul>
   *   <li>只扫描 {@code java.io.tmpdir} 的一级子目录，且目录名必须匹配本组件命名格式</li>
   *   <li>跳过本进程缓存中正在使用的目录</li>
   *   <li>近期活跃保护窗口内的目录一律不删，避免误删其他 JVM 进程正在写日志的目录</li>
   *   <li>单个目录失败只记录日志，不中断整体流程</li>
   * </ul>
   */
  private void scanStaleDirsOnce() {
    if (staleDirsScanned) {
      return;
    }
    staleDirsScanned = true;

    boolean enabled = Boolean.parseBoolean(System.getProperty(PROP_CLEANUP_ENABLED, "true"));
    if (!enabled) {
      logger.log(Level.FINE, "[ArthasTempDir] Stale dir cleanup disabled");
      return;
    }
    int maxKeepPerType = resolveMaxKeepPerType();
    // 共享 bundle 目录在固定根的二级路径，原有 tmpdir 一级扫描覆盖不到，单独回收
    ArthasResourceBundle.cleanup(maxKeepPerType, System.currentTimeMillis());

    Path tmpDir;
    try {
      tmpDir = Paths.get(System.getProperty("java.io.tmpdir", "/tmp"));
    } catch (RuntimeException e) {
      logger.log(Level.FINE, "[ArthasTempDir] Cannot resolve tmpdir, skip stale scan: {0}",
          e.getMessage());
      return;
    }

    Set<Path> activeDirs = new HashSet<>();
    for (Path active : dirCache.values()) {
      activeDirs.add(active.toAbsolutePath().normalize());
    }
    List<Path> candidates;
    try (Stream<Path> entries = Files.list(tmpDir)) {
      candidates = entries
          .filter(Files::isDirectory)
          .filter(p -> isManagedDirName(p.getFileName().toString()))
          .filter(p -> !activeDirs.contains(p.toAbsolutePath().normalize()))
          .collect(Collectors.toList());
    } catch (IOException | SecurityException e) {
      logger.log(Level.FINE, "[ArthasTempDir] Failed to scan tmpdir: {0}", e.getMessage());
      return;
    }

    Map<DirType, List<Path>> grouped = groupByType(candidates);
    long nowMillis = System.currentTimeMillis();
    for (Map.Entry<DirType, List<Path>> entry : grouped.entrySet()) {
      // 每组只计算一次 mtime，避免排序比较器重复做文件 IO
      Map<Path, Long> lastModified = new HashMap<>();
      for (Path dir : entry.getValue()) {
        lastModified.put(dir, latestModifiedMillis(dir));
      }
      List<Path> toRemove = selectDirsToRemove(
          entry.getValue(), lastModified, maxKeepPerType,
          ACTIVE_PROTECTION_MILLIS, nowMillis);
      for (Path dir : toRemove) {
        try {
          deleteRecursively(dir);
          logger.log(Level.INFO, "[ArthasTempDir] Removed stale dir for {0}: {1}",
              new Object[] {entry.getKey(), dir});
        } catch (IOException | SecurityException e) {
          logger.log(Level.WARNING, "[ArthasTempDir] Failed to remove stale dir {0}: {1}",
              new Object[] {dir, e.getMessage()});
        }
      }
    }
  }

  /**
   * 选出应删除的目录：按 mtime 倒序保留最近 {@code maxKeep} 个，其余中排除活跃/时间不可判定者。
   *
   * <p>纯函数（不修改入参、不做文件 IO），包级私有便于单测覆盖数量策略与安全护栏。
   */
  static List<Path> selectDirsToRemove(
      List<Path> dirs,
      Map<Path, Long> lastModified,
      int maxKeep,
      long activeProtectionMillis,
      long nowMillis) {
    List<Path> sorted = new ArrayList<>(dirs);
    sorted.sort((left, right) -> Long.compare(
        lastModified.getOrDefault(right, 0L), lastModified.getOrDefault(left, 0L)));

    List<Path> toRemove = new ArrayList<>();
    for (int i = maxKeep; i < sorted.size(); i++) {
      Path dir = sorted.get(i);
      long modified = lastModified.getOrDefault(dir, 0L);
      // 时间不可判定或近期活跃：保守跳过
      if (modified <= 0L || nowMillis - modified < activeProtectionMillis) {
        continue;
      }
      toRemove.add(dir);
    }
    return toRemove;
  }

  /**
   * 按目录名前缀归类到 {@link DirType}；无法归类的返回 null。
   *
   * <p>注意 {@code arthas-} 是另外两个前缀的公共前缀，必须按长度倒序匹配。
   */
  @Nullable
  static DirType dirTypeOf(String name) {
    if (name.startsWith(DirType.SPY_JAR.getPrefix())) {
      return DirType.SPY_JAR;
    }
    if (name.startsWith(DirType.LOGS.getPrefix())) {
      return DirType.LOGS;
    }
    if (name.startsWith(DirType.ARTHAS_HOME.getPrefix())) {
      return DirType.ARTHAS_HOME;
    }
    return null;
  }

  private static Map<DirType, List<Path>> groupByType(List<Path> candidates) {
    Map<DirType, List<Path>> grouped = new EnumMap<>(DirType.class);
    for (Path dir : candidates) {
      DirType type = dirTypeOf(dir.getFileName().toString());
      if (type == null) {
        continue;
      }
      grouped.computeIfAbsent(type, ignored -> new ArrayList<>()).add(dir);
    }
    return grouped;
  }

  private static int resolveMaxKeepPerType() {
    String raw = System.getProperty(PROP_MAX_KEEP_PER_TYPE);
    if (raw == null || raw.isEmpty()) {
      return DEFAULT_MAX_KEEP_PER_TYPE;
    }
    try {
      int value = Integer.parseInt(raw);
      return value >= 0 ? value : DEFAULT_MAX_KEEP_PER_TYPE;
    } catch (NumberFormatException e) {
      logger.log(Level.FINE, "[ArthasTempDir] Invalid max keep per type: {0}", raw);
      return DEFAULT_MAX_KEEP_PER_TYPE;
    }
  }

  /** 取目录自身与其一级子项中最近的修改时间，无有效时间返回 0。 */
  static long latestModifiedMillis(Path dir) {
    long latest = lastModifiedMillis(dir);
    try (Stream<Path> entries = Files.list(dir)) {
      java.util.Iterator<Path> iterator = entries.iterator();
      while (iterator.hasNext()) {
        long t = lastModifiedMillis(iterator.next());
        if (t > latest) {
          latest = t;
        }
      }
    } catch (IOException | SecurityException e) {
      logger.log(Level.FINE, "[ArthasTempDir] Failed to inspect dir {0}: {1}",
          new Object[] {dir, e.getMessage()});
    }
    return latest;
  }

  static long lastModifiedMillis(Path path) {
    try {
      return Files.getLastModifiedTime(path).toMillis();
    } catch (IOException | SecurityException e) {
      return 0L;
    }
  }

  /**
   * 仅用于测试：重置所有缓存状态
   */
  void reset() {
    dirCache.clear();
    sharedBundle = null;
    staleDirsScanned = false;
  }

  /**
   * 获取当前缓存的目录数量（用于诊断）
   *
   * @return 缓存目录数量
   */
  int getCachedDirCount() {
    return dirCache.size();
  }

  /**
   * 获取指定类型的缓存目录路径（用于诊断，不创建）
   *
   * @param type 目录类型
   * @return 缓存路径，可能为 null
   */
  @Nullable
  Path getCachedDir(DirType type) {
    return dirCache.get(type);
  }

  // ===== 私有方法 =====

  /**
   * 递归删除目录
   */
  static void deleteRecursively(Path path) throws IOException {
    if (Files.isDirectory(path)) {
      try (java.util.stream.Stream<Path> entries = Files.list(path)) {
        for (Path entry : entries.toArray(Path[]::new)) {
          deleteRecursively(entry);
        }
      }
    }
    Files.deleteIfExists(path);
  }

  /**
   * 检查文件是否存在且非空
   */
  private static boolean isFileNonEmpty(Path file) {
    try {
      return Files.size(file) > 0;
    } catch (IOException e) {
      return false;
    }
  }

  // ===== 目录类型枚举 =====

  /**
   * 临时目录类型
   *
   * <p>扩展点：新增资源类型只需在此添加枚举值。
   */
  public enum DirType {
    /**
     * Arthas Home 目录（arthas-{timestamp}-p{pid}-{random}）
     *
     * <p>包含：
     * <ul>
     *   <li>arthas-core.jar</li>
     *   <li>arthas-client.jar（可选）</li>
     *   <li>async-profiler/（native library 子目录）</li>
     *   <li>lib/（JNI library 子目录）</li>
     *   <li>logback.xml</li>
     *   <li>.arthas-dir.meta</li>
     * </ul>
     */
    ARTHAS_HOME("arthas-"),

    /**
     * Spy JAR 目录（arthas-spy-jar-{timestamp}-p{pid}-{random}）
     *
     * <p>包含 arthas-spy.jar，需要单独隔离以便
     * {@link java.lang.instrument.Instrumentation#appendToBootstrapClassLoaderSearch}
     */
    SPY_JAR("arthas-spy-jar-"),

    /**
     * 日志目录（arthas-logs-{timestamp}-p{pid}-{random}）
     *
     * <p>包含 Arthas 的 AnsiLog 输出重定向文件和 Arthas 内部 logback 日志文件。
     */
    LOGS("arthas-logs-");

    private final String prefix;

    DirType(String prefix) {
      this.prefix = prefix;
    }

    /**
     * 获取临时目录前缀（用于 {@code Files.createTempDirectory(prefix)}）
     */
    String getPrefix() {
      return prefix;
    }
  }
}
