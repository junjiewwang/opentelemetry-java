/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.extension.controlplane.arthas.ArthasTempDirectoryManager.DirType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArthasTempDirectoryManagerTest {

  private static final Pattern IDENTIFIABLE_NAME = Pattern.compile(
      "^arthas-logs-\\d{8}-\\d{6}-p[0-9A-Za-z_-]{1,32}-\\d+$");

  private final ArthasTempDirectoryManager manager = ArthasTempDirectoryManager.getInstance();

  @Nullable private String savedBundleEnabled;

  @BeforeEach
  void setUp() {
    // 本测试只验证「每进程私有目录」行为，隔离共享 bundle；bundle 另有专项测试
    savedBundleEnabled = System.getProperty(ArthasResourceBundle.PROP_ENABLED);
    System.setProperty(ArthasResourceBundle.PROP_ENABLED, "false");
    ArthasResourceBundle.resetForTesting();
    manager.reset();
  }

  @AfterEach
  void tearDown() {
    manager.cleanupAll();
    if (savedBundleEnabled == null) {
      System.clearProperty(ArthasResourceBundle.PROP_ENABLED);
    } else {
      System.setProperty(ArthasResourceBundle.PROP_ENABLED, savedBundleEnabled);
    }
    ArthasResourceBundle.resetForTesting();
  }

  @Test
  void dirNameContainsTimestampAndPid() {
    Path dir = manager.getOrCreateDir(DirType.LOGS);

    assertThat(dir.getFileName().toString()).matches(IDENTIFIABLE_NAME);
    assertThat(Files.isDirectory(dir)).isTrue();
  }

  @Test
  void metaFileContainsOwnerTypePidAndCreatedAt() throws IOException {
    Path dir = manager.getOrCreateDir(DirType.LOGS);
    Path meta = dir.resolve(".arthas-dir.meta");

    assertThat(Files.isRegularFile(meta)).isTrue();
    String content = new String(Files.readAllBytes(meta), StandardCharsets.UTF_8);
    assertThat(content).contains("owner=otel-controlplane");
    assertThat(content).contains("dir.type=LOGS");
    assertThat(content).contains("pid=");
    assertThat(content).contains("created.at=");
  }

  @Test
  void sameTypeDirIsReused() {
    Path first = manager.getOrCreateDir(DirType.LOGS);
    Path second = manager.getOrCreateDir(DirType.LOGS);

    assertThat(second).isEqualTo(first);
    assertThat(manager.getCachedDirCount()).isEqualTo(1);
    assertThat(manager.getCachedDir(DirType.LOGS)).isEqualTo(first);
  }

  @Test
  void differentTypesUseDistinctDirs() {
    Path home = manager.getOrCreateDir(DirType.ARTHAS_HOME);
    Path logs = manager.getOrCreateDir(DirType.LOGS);
    Path spy = manager.getOrCreateDir(DirType.SPY_JAR);

    assertThat(home).isNotEqualTo(logs).isNotEqualTo(spy);
    assertThat(logs).isNotEqualTo(spy);
    assertThat(manager.getCachedDirCount()).isEqualTo(3);
  }

  @Test
  void cleanupAllRemovesDirsAndClearsCache() {
    Path dir = manager.getOrCreateDir(DirType.LOGS);

    manager.cleanupAll();

    assertThat(Files.exists(dir)).isFalse();
    assertThat(manager.getCachedDirCount()).isZero();
    // 幂等：重复调用不抛异常
    manager.cleanupAll();
  }

  @Test
  void invalidCachedDirIsRecreated() throws IOException {
    // SPY_JAR 的有效性依赖 arthas-spy.jar 存在且非空
    Path first = manager.getOrCreateDir(DirType.SPY_JAR);
    Files.write(first.resolve("arthas-spy.jar"), new byte[] {1, 2, 3});
    assertThat(manager.getOrCreateDir(DirType.SPY_JAR)).isEqualTo(first);

    // 模拟核心文件被外部清理，缓存应判定失效并重建
    Files.deleteIfExists(first.resolve("arthas-spy.jar"));
    Path second = manager.getOrCreateDir(DirType.SPY_JAR);

    assertThat(second).isNotEqualTo(first);
    assertThat(Files.isDirectory(second)).isTrue();
  }

  @Test
  void isManagedDirNameAcceptsLegacyAndNewFormats() {
    // 旧格式（现场实际目录名）
    assertThat(ArthasTempDirectoryManager.isManagedDirName("arthas-1383043882854248061"))
        .isTrue();
    assertThat(ArthasTempDirectoryManager.isManagedDirName("arthas-logs-1879688763010489576"))
        .isTrue();
    assertThat(ArthasTempDirectoryManager.isManagedDirName("arthas-spy-jar-3633232488185242177"))
        .isTrue();

    // 新格式
    assertThat(ArthasTempDirectoryManager.isManagedDirName(
        "arthas-logs-20261124-164530-p12345-6921006666689707971")).isTrue();
    assertThat(ArthasTempDirectoryManager.isManagedDirName(
        "arthas-20261124-164530-punknown-6492605466621856301")).isTrue();
  }

  @Test
  void isManagedDirNameRejectsUnrelatedDirs() {
    assertThat(ArthasTempDirectoryManager.isManagedDirName("tomcat.8080.6300636697295415223"))
        .isFalse();
    assertThat(ArthasTempDirectoryManager.isManagedDirName("otel-agent")).isFalse();
    assertThat(ArthasTempDirectoryManager.isManagedDirName("arthas-logs-backup")).isFalse();
    assertThat(ArthasTempDirectoryManager.isManagedDirName("arthas-logs-")).isFalse();
    assertThat(ArthasTempDirectoryManager.isManagedDirName(null)).isFalse();
  }

  @Test
  void metaFailureDoesNotBlockDirCreation() throws IOException {
    // LOGS 目录有效性只要求目录存在；即使 meta 被删除，复用仍应成立
    Path dir = manager.getOrCreateDir(DirType.LOGS);
    assertThat(Files.deleteIfExists(dir.resolve(".arthas-dir.meta"))).isTrue();
    assertThat(manager.getOrCreateDir(DirType.LOGS)).isEqualTo(dir);
  }

  @Test
  void dirTypeOfHandlesCommonPrefix() {
    assertThat(ArthasTempDirectoryManager.dirTypeOf("arthas-1383043882854248061"))
        .isEqualTo(DirType.ARTHAS_HOME);
    assertThat(ArthasTempDirectoryManager.dirTypeOf("arthas-logs-1879688763010489576"))
        .isEqualTo(DirType.LOGS);
    assertThat(ArthasTempDirectoryManager.dirTypeOf("arthas-spy-jar-3633232488185242177"))
        .isEqualTo(DirType.SPY_JAR);
    assertThat(ArthasTempDirectoryManager.dirTypeOf("tomcat.8080.6300636697295415223"))
        .isNull();
  }

  @Test
  void keepsTwoMostRecentPerTypeAndRemovesOlder() {
    long now = 1_000_000L;
    long protection = 10_000L;
    Path newest = Paths.get("/tmp/arthas-logs-3");
    Path second = Paths.get("/tmp/arthas-logs-2");
    Path oldest = Paths.get("/tmp/arthas-logs-1");
    List<Path> dirs = java.util.Arrays.asList(oldest, newest, second);
    Map<Path, Long> mtimes = new HashMap<>();
    mtimes.put(newest, now - protection - 10);
    mtimes.put(second, now - protection - 20);
    mtimes.put(oldest, now - protection - 30);

    List<Path> toRemove = ArthasTempDirectoryManager.selectDirsToRemove(
        dirs, mtimes, 2, protection, now);

    // 三个都已超出活跃保护窗口，按 mtime 倒序保留 newest/second，删除 oldest
    assertThat(toRemove).containsExactly(oldest);
  }

  @Test
  void activeDirsAreNeverRemovedEvenWhenOverLimit() {
    long now = 1_000_000L;
    long protection = 10_000L;
    Path active = Paths.get("/tmp/arthas-logs-active");
    Path old1 = Paths.get("/tmp/arthas-logs-old1");
    Path old2 = Paths.get("/tmp/arthas-logs-old2");
    Path old3 = Paths.get("/tmp/arthas-logs-old3");
    List<Path> dirs = java.util.Arrays.asList(active, old1, old2, old3);
    Map<Path, Long> mtimes = new HashMap<>();
    mtimes.put(active, now - 1); // 近期活跃
    mtimes.put(old1, now - protection - 40);
    mtimes.put(old2, now - protection - 30);
    mtimes.put(old3, now - protection - 20);

    List<Path> toRemove = ArthasTempDirectoryManager.selectDirsToRemove(
        dirs, mtimes, 2, protection, now);

    // 排序后：active(最新)、old3、old2、old1；保留前 2 个，old2/old1 应删除
    assertThat(toRemove).containsExactlyInAnyOrder(old1, old2);
    assertThat(toRemove).doesNotContain(active);
  }

  @Test
  void undeterminedMtimeIsKept() {
    long now = 1_000_000L;
    long protection = 10_000L;
    Path unknown = Paths.get("/tmp/arthas-logs-unknown");
    Path old1 = Paths.get("/tmp/arthas-logs-old1");
    Path old2 = Paths.get("/tmp/arthas-logs-old2");
    List<Path> dirs = java.util.Arrays.asList(unknown, old1, old2);
    Map<Path, Long> mtimes = new HashMap<>();
    mtimes.put(unknown, 0L); // 时间不可判定
    mtimes.put(old1, now - protection - 30);
    mtimes.put(old2, now - protection - 20);

    List<Path> toRemove = ArthasTempDirectoryManager.selectDirsToRemove(
        dirs, mtimes, 2, protection, now);

    assertThat(toRemove).isEmpty();
  }

  @Test
  void noRemovalWhenWithinLimit() {
    long now = 1_000_000L;
    Path a = Paths.get("/tmp/arthas-1");
    Path b = Paths.get("/tmp/arthas-2");
    Map<Path, Long> mtimes = new HashMap<>();
    mtimes.put(a, now - 100_000L);
    mtimes.put(b, now - 200_000L);

    assertThat(ArthasTempDirectoryManager.selectDirsToRemove(
        java.util.Arrays.asList(a, b), mtimes, 2, 10_000L, now)).isEmpty();
  }
}
