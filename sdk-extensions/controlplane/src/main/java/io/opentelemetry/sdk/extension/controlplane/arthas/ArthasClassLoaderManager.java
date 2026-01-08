/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas ClassLoader 管理器
 *
 * <p>负责 Arthas ClassLoader 的创建、缓存和重置。
 * 与官方 {@code AgentBootstrap.resetArthasClassLoader()} 模式对齐。
 *
 * <p>设计原则：
 * <ul>
 *   <li><b>单一职责</b>：只负责 ClassLoader 生命周期管理</li>
 *   <li><b>线程安全</b>：使用 volatile + synchronized 保证并发安全</li>
 *   <li><b>可重置</b>：支持重置以便重新 attach</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * ArthasClassLoaderManager manager = new ArthasClassLoaderManager();
 *
 * // 获取或创建 ClassLoader
 * ClassLoader loader = manager.getOrCreate(config);
 *
 * // 重置（下次 attach 重新创建）
 * manager.reset();
 *
 * // 关闭
 * manager.close();
 * }</pre>
 */
public final class ArthasClassLoaderManager {

  private static final Logger logger = Logger.getLogger(ArthasClassLoaderManager.class.getName());

  /**
   * 全局持有 ClassLoader 用于隔离 Arthas 实现，防止多次 attach 重复初始化。
   * 与官方 AgentBootstrap 的设计一致。
   */
  @Nullable private volatile ClassLoader arthasClassLoader;

  /** 当前使用的父 ClassLoader */
  @Nullable private volatile ClassLoader parentClassLoader;

  public ArthasClassLoaderManager() {
    this.parentClassLoader = getClass().getClassLoader();
  }

  /**
   * 设置父 ClassLoader
   *
   * @param parent 父 ClassLoader
   */
  public void setParentClassLoader(@Nullable ClassLoader parent) {
    this.parentClassLoader = parent;
  }

  /**
   * 获取或创建 Arthas ClassLoader
   *
   * <p>如果 ClassLoader 已存在，直接返回；否则创建新的。
   *
   * @param config Arthas 配置
   * @return ClassLoader，如果创建失败返回 null
   */
  @Nullable
  public ClassLoader getOrCreate(ArthasConfig config) {
    ClassLoader loader = this.arthasClassLoader;
    if (loader != null) {
      logger.log(Level.FINE, "Returning cached Arthas ClassLoader");
      return loader;
    }

    synchronized (this) {
      loader = this.arthasClassLoader;
      if (loader != null) {
        return loader;
      }

      loader = createClassLoader(config);
      if (loader != null) {
        this.arthasClassLoader = loader;
        logger.log(Level.INFO, "Created Arthas ClassLoader: {0}", loader);
      }
      return loader;
    }
  }

  /**
   * 获取当前 ClassLoader（不创建）
   *
   * @return 当前 ClassLoader，可能为 null
   */
  @Nullable
  public ClassLoader get() {
    return arthasClassLoader;
  }

  /**
   * 重置 ClassLoader
   *
   * <p>让下次再次启动时有机会重新加载。与官方 {@code resetArthasClassLoader()} 对齐。
   */
  public void reset() {
    synchronized (this) {
      ClassLoader loader = this.arthasClassLoader;
      if (loader != null) {
        closeClassLoader(loader);
        this.arthasClassLoader = null;
        logger.log(Level.INFO, "Arthas ClassLoader reset");
      }
    }
  }

  /**
   * 关闭 ClassLoader
   *
   * <p>释放资源，与 reset() 类似但语义更明确。
   */
  public void close() {
    reset();
  }

  /**
   * 检查 ClassLoader 是否已创建
   *
   * @return 是否已创建
   */
  public boolean isCreated() {
    return arthasClassLoader != null;
  }

  // ===== 私有方法 =====

  /**
   * 创建 Arthas ClassLoader
   *
   * @param config Arthas 配置
   * @return ClassLoader，如果创建失败返回 null
   */
  @Nullable
  private ClassLoader createClassLoader(ArthasConfig config) {
    // 1. 首先尝试从配置的外部路径加载
    String libPath = config.getLibPath();
    if (libPath != null && !libPath.isEmpty()) {
      ClassLoader loader = loadFromExternalPath(libPath);
      if (loader != null) {
        logger.log(Level.INFO, "Loaded Arthas from external path: {0}", libPath);
        return loader;
      }
    }

    // 2. 尝试从 classpath 资源加载
    ClassLoader loader = loadFromClasspathResources();
    if (loader != null) {
      logger.log(Level.INFO, "Loaded Arthas from classpath resources");
      return loader;
    }

    // 3. 如果都失败，返回当前 ClassLoader（假设 Arthas 已在 classpath 中）
    logger.log(
        Level.INFO, "Using current ClassLoader for Arthas (assuming Arthas is in classpath)");
    return parentClassLoader;
  }

  /**
   * 从外部路径加载 Arthas jar
   *
   * @param libPath 库路径
   * @return ClassLoader
   */
  @Nullable
  private ClassLoader loadFromExternalPath(String libPath) {
    URL[] urls = ArthasResourceExtractor.loadFromExternalPath(libPath);
    if (urls == null) {
      return null;
    }

    // 创建 URLClassLoader 用于动态加载外部 Arthas jar
    @SuppressWarnings("BanClassLoader")
    URLClassLoader loader = new URLClassLoader(urls, parentClassLoader);
    return loader;
  }

  /**
   * 从 classpath 资源加载 Arthas jar
   *
   * @return ClassLoader
   */
  @Nullable
  private ClassLoader loadFromClasspathResources() {
    URL[] urls = ArthasResourceExtractor.extractCoreJars();
    if (urls == null) {
      return null;
    }

    // 创建 URLClassLoader 用于动态加载 classpath 中的 Arthas jar
    @SuppressWarnings("BanClassLoader")
    URLClassLoader loader = new URLClassLoader(urls, parentClassLoader);
    return loader;
  }

  /**
   * 关闭 ClassLoader（如果是 URLClassLoader）
   *
   * @param loader ClassLoader
   */
  private static void closeClassLoader(ClassLoader loader) {
    if (loader instanceof URLClassLoader) {
      try {
        ((URLClassLoader) loader).close();
        logger.log(Level.FINE, "Closed URLClassLoader: {0}", loader);
      } catch (IOException e) {
        logger.log(Level.WARNING, "Error closing Arthas ClassLoader: {0}", e.getMessage());
      }
    }
  }
}
