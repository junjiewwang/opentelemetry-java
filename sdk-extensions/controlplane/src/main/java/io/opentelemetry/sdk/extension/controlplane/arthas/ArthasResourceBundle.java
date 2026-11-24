/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import io.opentelemetry.sdk.extension.controlplane.profiler.AsyncProfilerResourceExtractor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 资源包（内容寻址、不可变）。
 *
 * <p>把 classpath 内嵌的 Arthas 资源发布到固定根目录下的 {@code bundles/b-<digest>/}，
 * 供多个 JVM 进程只读复用。<b>目录一旦发布，永不修改、永不原地覆盖</b>：这保护了运行中的
 * {@link java.net.URLClassLoader} 懒加载与已 mmap 的 native library。
 *
 * <p>三条不变式：
 * <ol>
 *   <li>I1：bundle 目录发布后不可变；资源升级只会产生新 digest 目录</li>
 *   <li>I2：完整性以「整个 staging 目录原子 move」为准，不以单文件存在为准</li>
 *   <li>I3：bundle 是跨进程共享资源，不参与任何 JVM 退出清理</li>
 * </ol>
 *
 * <p>任何失败都返回 {@code null}，由 {@link ArthasTempDirectoryManager} 降级到原有的
 * 每进程私有目录路径，保证 attach 不因缓存机制失败。
 */
final class ArthasResourceBundle {

  private static final Logger logger = Logger.getLogger(ArthasResourceBundle.class.getName());

  /** 总开关，默认开启；设为 false 可完全回滚到每进程私有目录 */
  static final String PROP_ENABLED = "otel.controlplane.arthas.bundle.enabled";

  /** 固定根目录，默认 ${java.io.tmpdir}/otel-arthas */
  static final String PROP_ROOT = "otel.controlplane.arthas.bundle.root";

  /** 严格校验：对全部文件（含 16MB core jar）重算 SHA-256，默认关闭 */
  static final String PROP_STRICT_VERIFY = "otel.controlplane.arthas.bundle.strict.verify";

  private static final String DEFAULT_ROOT_NAME = "otel-arthas";
  private static final String BUNDLES_SUBDIR = "bundles";
  private static final String USAGE_SUBDIR = "usage";
  private static final String BUNDLE_PREFIX = "b-";
  private static final String STAGING_PREFIX = ".staging-";
  private static final String MANIFEST_FILE = "manifest.txt";
  private static final String MANIFEST_FORMAT_KEY = "format";
  private static final String MANIFEST_FORMAT_VALUE = "1";
  private static final String MANIFEST_DIGEST_KEY = "digest";
  private static final String ABSENT = "absent";

  /** 小文件重算 SHA-256 的阈值；core jar 默认只比 size */
  private static final long SMALL_FILE_VERIFY_LIMIT = 1024L * 1024L;

  /** 近期活跃保护窗口，与临时目录清理保持一致 */
  static final long ACTIVE_PROTECTION_MILLIS = 10L * 60 * 1000L;

  private static final Object RESOLVE_LOCK = new Object();

  @Nullable private static volatile Path resolvedBundle;
  @Nullable private static volatile String resolvedDigest;

  private ArthasResourceBundle() {}

  /**
   * 解析可用 bundle 目录；失败返回 {@code null}（调用方降级）。
   *
   * <p>进程内结果缓存，跨 attach 周期只解析一次。
   */
  @Nullable
  static Path resolve() {
    if (!Boolean.parseBoolean(System.getProperty(PROP_ENABLED, "true"))) {
      return null;
    }
    Path cached = resolvedBundle;
    if (cached != null && isPublishedBundle(cached)) {
      touchUsage(cached);
      return cached;
    }

    synchronized (RESOLVE_LOCK) {
      cached = resolvedBundle;
      if (cached != null && isPublishedBundle(cached)) {
        touchUsage(cached);
        return cached;
      }
      try {
        List<Spec> specs = buildSpecs();
        String digest = computeDigest(specs);
        Path root = resolveRoot();
        if (root == null) {
          return null;
        }
        Path bundlesDir = root.resolve(BUNDLES_SUBDIR);
        Files.createDirectories(bundlesDir);
        Path target = bundlesDir.resolve(BUNDLE_PREFIX + digest);

        if (Files.isDirectory(target)) {
          if (!isPublishedBundle(target)) {
            // 不修复别人的目录：降级，让调用方使用私有目录
            logger.log(Level.WARNING,
                "[ArthasBundle] Existing bundle invalid, falling back: {0}", target);
            return null;
          }
          logger.log(Level.INFO, "[ArthasBundle] Hit shared bundle: {0}", target);
        } else {
          Path published = publish(bundlesDir, target, specs, digest);
          if (published == null) {
            return null;
          }
          target = published;
          logger.log(Level.INFO, "[ArthasBundle] Published shared bundle: {0}", target);
        }

        resolvedBundle = target;
        resolvedDigest = digest;
        touchUsage(target);
        return target;
      } catch (IOException | RuntimeException e) {
        logger.log(Level.WARNING,
            "[ArthasBundle] Failed to resolve bundle, falling back to private dir: {0}",
            e.getMessage());
        return null;
      }
    }
  }

  /** 判断目录是否为已发布且校验通过的 bundle（供资源提取器跳过重写）。 */
  static boolean isPublishedBundle(Path dir) {
    if (!Files.isDirectory(dir) || !dir.getFileName().toString().startsWith(BUNDLE_PREFIX)) {
      return false;
    }
    Map<String, String> manifest = readManifest(dir.resolve(MANIFEST_FILE));
    if (manifest == null
        || !MANIFEST_FORMAT_VALUE.equals(manifest.get(MANIFEST_FORMAT_KEY))) {
      return false;
    }
    String declaredDigest = manifest.get(MANIFEST_DIGEST_KEY);
    if (declaredDigest == null
        || !declaredDigest.equals(dir.getFileName().toString().substring(BUNDLE_PREFIX.length()))) {
      return false;
    }
    boolean strict = Boolean.parseBoolean(
        System.getProperty(PROP_STRICT_VERIFY, "false"));
    for (Map.Entry<String, String> entry : manifest.entrySet()) {
      if (isMetaKey(entry.getKey())) {
        continue;
      }
      String value = entry.getValue();
      int sep = value.indexOf(':');
      if (sep <= 0 || sep == value.length() - 1) {
        return false;
      }
      long declaredSize;
      try {
        declaredSize = Long.parseLong(value.substring(0, sep));
      } catch (NumberFormatException e) {
        return false;
      }
      String declaredSha = value.substring(sep + 1);
      Path file = dir.resolve(entry.getKey());
      try {
        if (!Files.isRegularFile(file) || Files.size(file) != declaredSize) {
          return false;
        }
        if (strict || declaredSize <= SMALL_FILE_VERIFY_LIMIT) {
          if (!sha256Hex(file).equals(declaredSha)) {
            return false;
          }
        }
      } catch (IOException e) {
        return false;
      }
    }
    return true;
  }

  /**
   * 回收过期 bundle 与崩溃残留的 staging 目录。
   *
   * <p>按「最近使用」倒序保留 {@code maxKeep} 个，其余在活跃保护窗口之外才删除。
   * 只处理固定根内的目录，不触碰 {@code /tmp} 下其他内容。
   */
  static void cleanup(int maxKeep, long nowMillis) {
    Path root;
    try {
      root = resolveRoot();
    } catch (IOException | RuntimeException e) {
      return;
    }
    if (root == null) {
      return;
    }
    Path bundlesDir = root.resolve(BUNDLES_SUBDIR);
    Path usageDir = root.resolve(USAGE_SUBDIR);
    if (!Files.isDirectory(bundlesDir)) {
      return;
    }

    try (java.util.stream.Stream<Path> entries = Files.list(bundlesDir)) {
      List<Path> staging = new ArrayList<>();
      List<Path> bundles = new ArrayList<>();
      java.util.Iterator<Path> iterator = entries.iterator();
      while (iterator.hasNext()) {
        Path path = iterator.next();
        String name = path.getFileName().toString();
        if (!Files.isDirectory(path)) {
          continue;
        }
        if (name.startsWith(STAGING_PREFIX)) {
          staging.add(path);
        } else if (name.startsWith(BUNDLE_PREFIX)) {
          bundles.add(path);
        }
      }
      cleanupStaging(staging, nowMillis);
      cleanupBundles(bundles, usageDir, maxKeep, nowMillis);
    } catch (IOException | RuntimeException e) {
      logger.log(Level.FINE, "[ArthasBundle] Cleanup failed: {0}", e.getMessage());
    }
  }

  private static void cleanupStaging(List<Path> staging, long nowMillis) {
    for (Path dir : staging) {
      long modified = ArthasTempDirectoryManager.latestModifiedMillis(dir);
      if (modified <= 0L || nowMillis - modified < ACTIVE_PROTECTION_MILLIS) {
        continue;
      }
      try {
        ArthasTempDirectoryManager.deleteRecursively(dir);
        logger.log(Level.INFO, "[ArthasBundle] Removed stale staging dir: {0}", dir);
      } catch (IOException | RuntimeException e) {
        logger.log(Level.FINE, "[ArthasBundle] Failed to remove staging {0}: {1}",
            new Object[] {dir, e.getMessage()});
      }
    }
  }

  private static void cleanupBundles(
      List<Path> bundles, Path usageDir, int maxKeep, long nowMillis) {
    if (bundles.size() <= maxKeep) {
      return;
    }
    Map<Path, Long> lastUsed = new LinkedHashMap<>();
    for (Path bundle : bundles) {
      Path usage = usageDir.resolve(bundle.getFileName().toString());
      long used = Files.isRegularFile(usage)
          ? ArthasTempDirectoryManager.lastModifiedMillis(usage)
          : ArthasTempDirectoryManager.latestModifiedMillis(bundle);
      lastUsed.put(bundle, used);
    }
    List<Path> toRemove = ArthasTempDirectoryManager.selectDirsToRemove(
        bundles, lastUsed, maxKeep, ACTIVE_PROTECTION_MILLIS, nowMillis);
    for (Path bundle : toRemove) {
      try {
        ArthasTempDirectoryManager.deleteRecursively(bundle);
        Files.deleteIfExists(usageDir.resolve(bundle.getFileName().toString()));
        logger.log(Level.INFO, "[ArthasBundle] Removed stale bundle: {0}", bundle);
      } catch (IOException | RuntimeException e) {
        logger.log(Level.FINE, "[ArthasBundle] Failed to remove bundle {0}: {1}",
            new Object[] {bundle, e.getMessage()});
      }
    }
  }

  /** 记录最近使用时间；写在 bundle 目录之外，保持 I1 不可变。 */
  private static void touchUsage(Path bundle) {
    try {
      Path root = resolveRoot();
      if (root == null) {
        return;
      }
      Path usageDir = root.resolve(USAGE_SUBDIR);
      Files.createDirectories(usageDir);
      Path usage = usageDir.resolve(bundle.getFileName().toString());
      Files.write(usage, Long.toString(System.currentTimeMillis())
          .getBytes(StandardCharsets.UTF_8));
    } catch (IOException | RuntimeException e) {
      logger.log(Level.FINE, "[ArthasBundle] Failed to touch usage: {0}", e.getMessage());
    }
  }

  /** 解析并校验固定根目录；不可写或符号链接时返回 null。 */
  @Nullable
  private static Path resolveRoot() throws IOException {
    String configured = System.getProperty(PROP_ROOT);
    Path root = configured != null && !configured.isEmpty()
        ? Paths.get(configured)
        : Paths.get(System.getProperty("java.io.tmpdir", "/tmp"), DEFAULT_ROOT_NAME);
    root = root.toAbsolutePath().normalize();

    if (Files.isSymbolicLink(root)) {
      logger.log(Level.WARNING, "[ArthasBundle] Bundle root is a symbolic link, refusing: {0}",
          root);
      return null;
    }
    Files.createDirectories(root);
    if (!Files.isDirectory(root) || !Files.isWritable(root)) {
      return null;
    }
    // owner-only，避免其他用户预置或改写共享资源
    java.io.File rootFile = root.toFile();
    rootFile.setReadable(true, false);
    rootFile.setWritable(true, false);
    rootFile.setExecutable(true, false);
    return root;
  }

  // ===== 发布 =====

  @Nullable
  private static Path publish(Path bundlesDir, Path target, List<Spec> specs, String digest)
      throws IOException {
    String pid = pid();
    Path staging = bundlesDir.resolve(
        STAGING_PREFIX + digest + "-p" + pid + "-" + Long.toUnsignedString(
            java.util.concurrent.ThreadLocalRandom.current().nextLong()));
    Files.createDirectories(staging);
    try {
      Map<String, String> manifest = new LinkedHashMap<>();
      manifest.put(MANIFEST_FORMAT_KEY, MANIFEST_FORMAT_VALUE);
      manifest.put(MANIFEST_DIGEST_KEY, digest);
      for (Spec spec : specs) {
        writeSpec(staging, spec, manifest);
      }
      writeManifest(staging.resolve(MANIFEST_FILE), manifest);

      try {
        Files.move(staging, target);
      } catch (IOException e) {
        // 另一个 JVM 抢先发布：目标已存在时平台可能抛 FileAlreadyExistsException 或
        // DirectoryNotEmptyException，统一按「已有目录可复用则复用」处理
        if (!Files.isDirectory(target)) {
          throw e;
        }
        ArthasTempDirectoryManager.deleteRecursively(staging);
        return isPublishedBundle(target) ? target : null;
      }
      return target;
    } catch (IOException | RuntimeException e) {
      try {
        ArthasTempDirectoryManager.deleteRecursively(staging);
      } catch (IOException | RuntimeException ignored) {
        // best-effort
      }
      logger.log(Level.WARNING,
          "[ArthasBundle] Publish failed, falling back to private dir: {0}", e.getMessage());
      return null;
    }
  }

  // Java 8 不支持 ErrorProne 建议的 `try (source; ...)` 语法（那是 Java 9+），
  // 这里保留等价的显式资源引用写法。
  @SuppressWarnings("TryWithResourcesVariable")
  private static void writeSpec(Path staging, Spec spec, Map<String, String> manifest)
      throws IOException {
    InputStream source = openStream(spec);
    if (source == null) {
      return;
    }
    Path output = staging.resolve(spec.targetPath);
    Files.createDirectories(output.getParent());
    MessageDigest md = newDigest();
    long size;
    try (InputStream is = source; OutputStream os = Files.newOutputStream(output)) {
      byte[] buffer = new byte[8192];
      int read;
      long total = 0;
      while ((read = is.read(buffer)) != -1) {
        os.write(buffer, 0, read);
        md.update(buffer, 0, read);
        total += read;
      }
      size = total;
    }
    if (spec.executable) {
      output.toFile().setExecutable(true, false);
      output.toFile().setReadable(true, false);
    }
    manifest.put(spec.targetPath, size + ":" + hex(md.digest()));
  }

  private static void writeManifest(Path file, Map<String, String> manifest) throws IOException {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : manifest.entrySet()) {
      sb.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
    }
    Files.write(file, sb.toString().getBytes(StandardCharsets.UTF_8));
  }

  @Nullable
  private static Map<String, String> readManifest(Path file) {
    if (!Files.isRegularFile(file)) {
      return null;
    }
    Map<String, String> result = new LinkedHashMap<>();
    try {
      for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
        int sep = line.indexOf('=');
        if (sep > 0) {
          result.put(line.substring(0, sep), line.substring(sep + 1));
        }
      }
    } catch (IOException e) {
      return null;
    }
    return result;
  }

  private static boolean isMetaKey(String key) {
    return MANIFEST_FORMAT_KEY.equals(key) || MANIFEST_DIGEST_KEY.equals(key);
  }

  // ===== 摘要 =====

  private static String computeDigest(List<Spec> specs) throws IOException {
    StringBuilder aggregate = new StringBuilder();
    for (Spec spec : specs) {
      aggregate.append(spec.targetPath).append(':').append(digestOf(spec)).append('\n');
    }
    MessageDigest md = newDigest();
    byte[] bytes = md.digest(aggregate.toString().getBytes(StandardCharsets.UTF_8));
    return hex(bytes).substring(0, 16);
  }

  private static String digestOf(Spec spec) throws IOException {
    MessageDigest md = newDigest();
    if (spec.inMemory != null) {
      md.update(spec.inMemory);
      return hex(md.digest());
    }
    try (InputStream is = openStream(spec)) {
      if (is == null) {
        return ABSENT;
      }
      byte[] buffer = new byte[8192];
      int read;
      while ((read = is.read(buffer)) != -1) {
        md.update(buffer, 0, read);
      }
    }
    return hex(md.digest());
  }

  @Nullable
  private static InputStream openStream(Spec spec) {
    if (spec.inMemory != null) {
      return new ByteArrayInputStream(spec.inMemory);
    }
    return ArthasResourceBundle.class.getResourceAsStream(spec.resourcePath);
  }

  private static String sha256Hex(Path file) throws IOException {
    MessageDigest md = newDigest();
    try (InputStream is = Files.newInputStream(file)) {
      byte[] buffer = new byte[8192];
      int read;
      while ((read = is.read(buffer)) != -1) {
        md.update(buffer, 0, read);
      }
    }
    return hex(md.digest());
  }

  private static MessageDigest newDigest() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  private static String hex(byte[] bytes) {
    char[] digits = "0123456789abcdef".toCharArray();
    char[] out = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      int value = bytes[i] & 0xff;
      out[i * 2] = digits[value >>> 4];
      out[i * 2 + 1] = digits[value & 0x0f];
    }
    return new String(out);
  }

  private static String pid() {
    try {
      String value = io.opentelemetry.sdk.extension.controlplane.identity
          .AgentIdentityProvider.getProcessId();
      return value != null && value.matches("[0-9A-Za-z_-]{1,32}") ? value : "unknown";
    } catch (RuntimeException e) {
      return "unknown";
    }
  }

  // ===== 资源清单 =====

  private static List<Spec> buildSpecs() {
    List<Spec> specs = new ArrayList<>();
    specs.add(new Spec("arthas-core.jar",
        ArthasResourceExtractor.ARTHAS_CORE_JAR_RESOURCE, null, /* executable= */ false));
    specs.add(new Spec("arthas-client.jar",
        ArthasResourceExtractor.ARTHAS_CLIENT_JAR_RESOURCE, null, /* executable= */ false));
    specs.add(new Spec("logback.xml", null,
        ArthasResourceExtractor.ARTHAS_LOGBACK_XML_CONTENT.getBytes(StandardCharsets.UTF_8),
        /* executable= */ false));

    String profilerLib = AsyncProfilerResourceExtractor.detectPlatform().getLibraryFileName();
    if (profilerLib != null) {
      specs.add(new Spec("async-profiler/" + profilerLib,
          "/async-profiler/" + profilerLib, null, /* executable= */ true));
    }
    String jniLib = ArthasJniLibraryExtractor.detectPlatform().getLibraryFileName();
    if (jniLib != null) {
      specs.add(new Spec("lib/" + jniLib, "/arthas/lib/" + jniLib, null, /* executable= */ true));
    }
    return specs;
  }

  /** 单个资源描述：目标相对路径 + classpath 资源或内存内容。 */
  private static final class Spec {
    private final String targetPath;
    @Nullable private final String resourcePath;
    @Nullable private final byte[] inMemory;
    private final boolean executable;

    Spec(String targetPath, @Nullable String resourcePath, @Nullable byte[] inMemory,
        boolean executable) {
      this.targetPath = targetPath;
      this.resourcePath = resourcePath;
      this.inMemory = inMemory;
      this.executable = executable;
    }
  }

  /** 仅用于测试：清除进程内解析缓存。 */
  static void resetForTesting() {
    synchronized (RESOLVE_LOCK) {
      resolvedBundle = null;
      resolvedDigest = null;
    }
  }

  /** 仅用于诊断/测试：当前解析到的 digest。 */
  @Nullable
  static String getResolvedDigestForTesting() {
    return resolvedDigest;
  }

  static String getBundlePrefix() {
    return BUNDLE_PREFIX;
  }

  static String getManifestFileName() {
    return MANIFEST_FILE;
  }

  static String getStagingPrefix() {
    return STAGING_PREFIX;
  }
}
