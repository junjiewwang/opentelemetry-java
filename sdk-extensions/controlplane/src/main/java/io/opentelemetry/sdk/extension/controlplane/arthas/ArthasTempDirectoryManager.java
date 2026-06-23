/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 临时目录统一管理者（JVM 级 Singleton）
 *
 * <p>统一管理 Arthas 运行时所需的所有临时目录，支持跨 attach 周期复用，避免每次
 * attach/detach 周期在 /tmp 下堆积新的临时目录。
 *
 * <p>管理的目录类型：
 * <ul>
 *   <li>{@link DirType#ARTHAS_HOME} — 核心 JARs、native libraries、logback.xml</li>
 *   <li>{@link DirType#SPY_JAR} — SpyAPI JAR</li>
 *   <li>{@link DirType#LOGS} — Arthas 日志文件</li>
 * </ul>
 *
 * <p>设计原则：
 * <ul>
 *   <li><b>SRP</b>：只管理临时目录生命周期</li>
 *   <li><b>高内聚</b>：所有目录操作（创建、缓存、复用、校验、清理）集中于此</li>
 *   <li><b>低耦合</b>：不依赖任何 Arthas 组件，通过 {@link #getInstance()} 暴露</li>
 *   <li><b>健壮</b>：ConcurrentHashMap 线程安全 + 文件有效性校验 + deleteOnExit 兜底</li>
 *   <li><b>可扩展</b>：新增资源类型只需添加 {@link DirType} 枚举值</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * ArthasTempDirectoryManager manager = ArthasTempDirectoryManager.getInstance();
 *
 * // 获取或创建 arthas-home 目录（首次创建，后续复用）
 * Path arthasHome = manager.getOrCreateDir(DirType.ARTHAS_HOME);
 *
 * // 获取或创建 spy jar 目录（首次创建，后续复用）
 * Path spyDir = manager.getOrCreateDir(DirType.SPY_JAR);
 *
 * // 获取或创建日志目录（首次创建，后续复用）
 * Path logDir = manager.getOrCreateDir(DirType.LOGS);
 *
 * // JVM 关闭前主动清理
 * manager.cleanupAll();
 * }</pre>
 */
public final class ArthasTempDirectoryManager {

  private static final Logger logger =
      Logger.getLogger(ArthasTempDirectoryManager.class.getName());

  private static final ArthasTempDirectoryManager INSTANCE =
      new ArthasTempDirectoryManager();

  /** 已缓存的目录路径（线程安全，跨 attach 周期持久） */
  private final Map<DirType, Path> dirCache = new ConcurrentHashMap<>();

  private ArthasTempDirectoryManager() {}

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
   *   <li>缓存命中且目录有效 → 直接复用</li>
   *   <li>缓存未命中或目录失效 → 创建新目录并缓存（旧目录标记 deleteOnExit）</li>
   * </ol>
   *
   * @param type 目录类型
   * @return 目录路径，创建失败抛出 RuntimeException
   */
  public Path getOrCreateDir(DirType type) {
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
      Path newDir = Files.createTempDirectory(type.getPrefix());
      newDir.toFile().deleteOnExit();
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
   * 主动清理所有临时目录
   *
   * <p>应在 JVM 关闭前调用（如 shutdown hook），用于主动清理临时文件。
   * {@link java.io.File#deleteOnExit()} 作为兜底机制。
   *
   * <p>注意：调用此方法后，下次 {@link #getOrCreateDir(DirType)} 将创建新目录。
   */
  public void cleanupAll() {
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
   * 仅用于测试：重置所有缓存状态
   */
  void reset() {
    dirCache.clear();
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
  private static void deleteRecursively(Path path) throws IOException {
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
     * Arthas Home 目录（arthas-{random}）
     *
     * <p>包含：
     * <ul>
     *   <li>arthas-core.jar</li>
     *   <li>arthas-client.jar（可选）</li>
     *   <li>async-profiler/（native library 子目录）</li>
     *   <li>lib/（JNI library 子目录）</li>
     *   <li>logback.xml</li>
     * </ul>
     */
    ARTHAS_HOME("arthas-"),

    /**
     * Spy JAR 目录（arthas-spy-jar-{random}）
     *
     * <p>包含 arthas-spy.jar，需要单独隔离以便
     * {@link java.lang.instrument.Instrumentation#appendToBootstrapClassLoaderSearch}
     */
    SPY_JAR("arthas-spy-jar-"),

    /**
     * 日志目录（arthas-logs-{random}）
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
