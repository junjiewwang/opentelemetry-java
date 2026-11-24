/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ArthasResourceBundleTest {

  @TempDir Path tempRoot;

  @Nullable private String savedEnabled;
  @Nullable private String savedRoot;

  @BeforeEach
  void setUp() {
    savedEnabled = System.getProperty(ArthasResourceBundle.PROP_ENABLED);
    savedRoot = System.getProperty(ArthasResourceBundle.PROP_ROOT);
    System.setProperty(ArthasResourceBundle.PROP_ENABLED, "true");
    System.setProperty(ArthasResourceBundle.PROP_ROOT, tempRoot.resolve("bundle-root").toString());
    ArthasResourceBundle.resetForTesting();
  }

  @AfterEach
  void tearDown() {
    restore(ArthasResourceBundle.PROP_ENABLED, savedEnabled);
    restore(ArthasResourceBundle.PROP_ROOT, savedRoot);
    ArthasResourceBundle.resetForTesting();
  }

  private static void restore(String key, @Nullable String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  @Test
  void publishesBundleWithManifestAndResources() throws IOException {
    Path bundle = ArthasResourceBundle.resolve();

    assertThat(bundle).isNotNull();
    assertThat(bundle.getFileName().toString())
        .startsWith(ArthasResourceBundle.getBundlePrefix());
    assertThat(Files.isRegularFile(bundle.resolve("arthas-core.jar"))).isTrue();
    assertThat(Files.isRegularFile(bundle.resolve("logback.xml"))).isTrue();
    assertThat(Files.isRegularFile(bundle.resolve(ArthasResourceBundle.getManifestFileName())))
        .isTrue();
    assertThat(ArthasResourceBundle.isPublishedBundle(bundle)).isTrue();
    assertThat(ArthasResourceBundle.getResolvedDigestForTesting())
        .isEqualTo(bundle.getFileName().toString().substring(
            ArthasResourceBundle.getBundlePrefix().length()));
  }

  @Test
  void resolveIsIdempotentAcrossResets() {
    Path first = ArthasResourceBundle.resolve();
    // 模拟新 JVM：清掉进程内缓存，仍应命中同一个共享目录
    ArthasResourceBundle.resetForTesting();
    Path second = ArthasResourceBundle.resolve();

    assertThat(second).isEqualTo(first);
    assertThat(listBundles()).hasSize(1);
  }

  @Test
  void publishedBundleIsImmutable() throws IOException {
    Path bundle = ArthasResourceBundle.resolve();
    Path coreJar = bundle.resolve("arthas-core.jar");
    long size = Files.size(coreJar);
    long mtime = Files.getLastModifiedTime(coreJar).toMillis();

    ArthasResourceBundle.resetForTesting();
    ArthasResourceBundle.resolve();

    // 命中不重写：大小与 mtime 均不变（I1）
    assertThat(Files.size(coreJar)).isEqualTo(size);
    assertThat(Files.getLastModifiedTime(coreJar).toMillis()).isEqualTo(mtime);
  }

  @Test
  void disabledFallsBackToNull() {
    System.setProperty(ArthasResourceBundle.PROP_ENABLED, "false");

    assertThat(ArthasResourceBundle.resolve()).isNull();
  }

  @Test
  void tamperedManifestIsRejected() throws IOException {
    Path bundle = ArthasResourceBundle.resolve();
    Path logback = bundle.resolve("logback.xml");
    Files.write(logback, "tampered".getBytes(StandardCharsets.UTF_8));

    // 小文件重算 SHA-256，篡改应被检出
    assertThat(ArthasResourceBundle.isPublishedBundle(bundle)).isFalse();
  }

  @Test
  void symbolicLinkRootIsRejected() throws IOException {
    if (isWindows()) {
      return;
    }
    Path real = tempRoot.resolve("real-root");
    Files.createDirectories(real);
    Path link = tempRoot.resolve("link-root");
    Files.createSymbolicLink(link, real);
    System.setProperty(ArthasResourceBundle.PROP_ROOT, link.toString());

    assertThat(ArthasResourceBundle.resolve()).isNull();
  }

  @Test
  void noStagingResidueAfterPublish() {
    Path bundle = ArthasResourceBundle.resolve();
    assertThat(bundle).isNotNull();

    Path bundlesDir = bundle.getParent();
    try (java.util.stream.Stream<Path> entries = Files.list(bundlesDir)) {
      assertThat(entries.noneMatch(
          p -> p.getFileName().toString().startsWith(ArthasResourceBundle.getStagingPrefix())))
          .isTrue();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  void extractCoreJarsDoesNotRewritePublishedBundle() throws IOException {
    Path bundle = ArthasResourceBundle.resolve();
    Path coreJar = bundle.resolve("arthas-core.jar");
    long mtime = Files.getLastModifiedTime(coreJar).toMillis();

    java.net.URL[] urls = ArthasResourceExtractor.extractCoreJarsTo(bundle);

    assertThat(urls).isNotNull().isNotEmpty();
    assertThat(urls[0].toString()).contains("arthas-core.jar");
    // 只读复用：不重写，mtime 不变（I1）
    assertThat(Files.getLastModifiedTime(coreJar).toMillis()).isEqualTo(mtime);
  }

  @Test
  void managerRoutesArthasHomeToSharedBundle() {
    ArthasTempDirectoryManager manager = ArthasTempDirectoryManager.getInstance();
    manager.reset();
    try {
      Path home = manager.getOrCreateDir(
          ArthasTempDirectoryManager.DirType.ARTHAS_HOME);

      assertThat(home.getFileName().toString())
          .startsWith(ArthasResourceBundle.getBundlePrefix());
      assertThat(ArthasResourceBundle.isPublishedBundle(home)).isTrue();

      // shutdown hook 语义：cleanupAll 不得删除共享 bundle
      manager.cleanupAll();
      assertThat(Files.isDirectory(home)).isTrue();
    } finally {
      manager.reset();
    }
  }

  @Test
  void manifestDeclaresCoreJarSha256() throws IOException {
    Path bundle = ArthasResourceBundle.resolve();
    List<String> lines = Files.readAllLines(
        bundle.resolve(ArthasResourceBundle.getManifestFileName()), StandardCharsets.UTF_8);
    String coreEntry = lines.stream()
        .filter(line -> line.startsWith("arthas-core.jar="))
        .collect(Collectors.toList()).get(0);

    assertThat(coreEntry).matches("arthas-core\\.jar=\\d+:[0-9a-f]{64}");
  }

  private List<Path> listBundles() {
    Path bundlesDir = tempRoot.resolve("bundle-root").resolve("bundles");
    try (java.util.stream.Stream<Path> entries = Files.list(bundlesDir)) {
      return entries
          .filter(Files::isDirectory)
          .filter(p -> p.getFileName().toString()
              .startsWith(ArthasResourceBundle.getBundlePrefix()))
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static boolean isWindows() {
    return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
  }
}
