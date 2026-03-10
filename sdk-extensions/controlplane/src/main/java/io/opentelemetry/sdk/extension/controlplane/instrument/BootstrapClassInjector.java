/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.Instrumentation;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Bootstrap ClassLoader 类注入器
 *
 * <p>解决动态增强的 <b>ClassLoader 可见性问题</b>：ByteBuddy Advice 将代码内联到目标方法中，
 * 目标类的 ClassLoader（通常是 App ClassLoader）无法看到 Agent ClassLoader 中的类
 * （如 {@link AdviceDispatcher}、{@link DynamicInstrumentLogger} 等），
 * 导致运行时抛出 {@code NoClassDefFoundError}，被 suppress 静默吞掉。
 *
 * <p><b>解决方案</b>：在首次动态增强之前，将 Advice 内联代码引用的所有类打包成临时 JAR，
 * 通过 {@link Instrumentation#appendToBootstrapClassLoaderSearch(JarFile)} 注入到
 * Bootstrap ClassLoader。Bootstrap CL 的类对所有 ClassLoader 可见，且静态字段全局唯一。
 *
 * <p><b>需要注入的类</b>（Advice 内联代码的完整依赖链）：
 * <ul>
 *   <li>{@link AdviceDispatcher} — Advice 入口/出口分发</li>
 *   <li>{@link DynamicInstrumentLogger} — Advice catch 块中的日志记录</li>
 *   <li>{@link InstrumentationType} — 增强类型枚举</li>
 *   <li>{@link DynamicTraceAdvice} — TRACE 类型实现</li>
 *   <li>{@link DynamicMetricAdvice} — METRIC 类型实现</li>
 *   <li>{@link DynamicLogAdvice} — LOG 类型实现</li>
 *   <li>{@link InstrumentationRule} — 规则模型（被 Advice 类引用）</li>
 *   <li>{@link InstrumentationRule.Builder} — 规则 Builder 内部类</li>
 * </ul>
 *
 * <p><b>不需要注入的类</b>：
 * <ul>
 *   <li>OTel API 类（{@code Tracer}、{@code Span} 等）— 已在 Bootstrap CL（OTel Agent 启动时注入）</li>
 *   <li>JDK 类（{@code Logger}、{@code ConcurrentHashMap} 等）— 已在 Bootstrap CL</li>
 *   <li>{@link TransformerManager}、{@link ByteBuddyTransformerFactory} 等 — 不被 Advice 内联代码引用</li>
 * </ul>
 *
 * <p><b>关键设计</b>：Bootstrap 注入发生在 Agent CL 已经加载这些类之后，因此 Agent CL 和
 * Bootstrap CL 中存在两份同名类。Advice 内联代码在目标类的 ClassLoader 中运行时，
 * 通过双亲委派会找到 Bootstrap CL 中的版本。而 {@link TransformerManager} 在 Agent CL 中运行，
 * 看到的是 Agent CL 的版本。因此 {@link TransformerManager} 注册/注销规则时，需要通过
 * <b>反射</b>操作 Bootstrap CL 中的 {@link AdviceDispatcher}，确保状态写入到 Advice 内联
 * 代码运行时能看到的那份静态字段中。
 *
 * @see TransformerManager#applyRule(InstrumentationRule)
 * @see DynamicInstrumentationIntegration#create()
 */
final class BootstrapClassInjector {

  private static final Logger logger =
      DynamicInstrumentLogger.getLogger("bootstrap-injector");

  /** 是否已完成注入 */
  private static volatile boolean injected = false;

  /**
   * 需要注入到 Bootstrap ClassLoader 的类名列表。
   *
   * <p>这些类是 {@link DynamicByteBuddyAdvice} 内联代码在运行时解析的完整依赖链。
   * 任何遗漏都会导致 {@code NoClassDefFoundError}。
   *
   * <p>使用字符串而非 {@code Class<?>} 引用，避免 private 内部类的编译时访问限制
   * （如 {@code DynamicMetricAdvice$MetricInstruments}）。
   */
  private static final String[] CLASS_NAMES_TO_INJECT = {
      // Advice 内联代码直接引用的类
      AdviceDispatcher.class.getName(),
      DynamicInstrumentLogger.class.getName(),
      // AdviceDispatcher 调用链引用的类
      InstrumentationType.class.getName(),
      DynamicTraceAdvice.class.getName(),
      DynamicMetricAdvice.class.getName(),
      // DynamicMetricAdvice 的 private 内部类（字符串方式引用避免编译限制）
      DynamicMetricAdvice.class.getName() + "$MetricInstruments",
      DynamicLogAdvice.class.getName(),
      // 参数/返回值采集相关类（被 DynamicByteBuddyCaptureAdvice 引用）
      DynamicByteBuddyCaptureAdvice.class.getName(),
      CaptureConfig.class.getName(),
      // CaptureConfig 的 private 内部类（字符串方式引用避免编译限制）
      CaptureConfig.class.getName() + "$CaptureArgsResult",
      CaptureProcessor.class.getName(),
      // 规则模型（被各 Advice 类引用）
      InstrumentationRule.class.getName(),
      InstrumentationRule.Builder.class.getName(),
  };

  private BootstrapClassInjector() {}

  /**
   * 检查是否已完成 Bootstrap 注入
   *
   * @return 是否已注入
   */
  static boolean isInjected() {
    return injected;
  }

  /**
   * 将 Advice 相关类注入到 Bootstrap ClassLoader
   *
   * <p>此方法应在首次动态增强之前调用。线程安全，重复调用无副作用。
   *
   * <p>实现步骤：
   * <ol>
   *   <li>从当前 ClassLoader 读取各类的 .class 文件字节码</li>
   *   <li>将字节码打包成临时 JAR 文件</li>
   *   <li>通过 {@link Instrumentation#appendToBootstrapClassLoaderSearch(JarFile)} 注入</li>
   *   <li>验证注入成功（尝试从 Bootstrap CL 加载类）</li>
   * </ol>
   *
   * @param inst Instrumentation 实例
   * @return 是否注入成功
   */
  static synchronized boolean inject(Instrumentation inst) {
    if (injected) {
      logger.log(Level.FINE, "[BOOTSTRAP-INJECTOR] Already injected, skipping");
      return true;
    }

    logger.log(Level.INFO, "[BOOTSTRAP-INJECTOR] Starting Bootstrap ClassLoader injection...");

    File tempJar = null;
    try {
      // 1. 创建临时 JAR 文件
      tempJar = createTempJar();

      // 2. 将类字节码写入 JAR
      int classCount = writeClassesToJar(tempJar);
      if (classCount == 0) {
        logger.log(Level.WARNING,
            "[BOOTSTRAP-INJECTOR] No classes were written to JAR, injection skipped");
        return false;
      }

      // 3. 注入到 Bootstrap ClassLoader
      inst.appendToBootstrapClassLoaderSearch(new JarFile(tempJar));

      logger.log(Level.INFO,
          "[BOOTSTRAP-INJECTOR] Successfully injected {0} classes to Bootstrap ClassLoader, "
              + "jar={1}",
          new Object[] {classCount, tempJar.getAbsolutePath()});

      // 4. 验证注入结果
      boolean verified = verifyInjection();
      if (verified) {
        // 5. 初始化 Bootstrap CL 中的 DynamicInstrumentLogger
        //    确保 Bootstrap CL 中的 Advice 类在运行时能正常输出日志到独立文件
        initializeBootstrapLogger();

        injected = true;
        logger.log(Level.INFO,
            "[BOOTSTRAP-INJECTOR] Injection verified and logger initialized successfully");
      } else {
        logger.log(Level.WARNING,
            "[BOOTSTRAP-INJECTOR] Injection completed but verification failed");
      }

      return verified;

    } catch (IOException e) {
      logger.log(Level.SEVERE,
          "[BOOTSTRAP-INJECTOR] Failed to inject classes: " + e.getMessage(), e);
      return false;
    } catch (RuntimeException e) {
      logger.log(Level.SEVERE,
          "[BOOTSTRAP-INJECTOR] Unexpected error during injection: " + e.getMessage(), e);
      return false;
    }
    // 注意：不删除临时 JAR 文件，因为 JVM 可能在后续类加载时仍需要读取它
    // JVM 关闭时由操作系统清理
  }

  /**
   * 创建临时 JAR 文件
   */
  private static File createTempJar() throws IOException {
    File tempFile = File.createTempFile("otel-dynamic-instrument-bootstrap-", ".jar");
    // 注册 JVM 关闭时删除（最终清理）
    tempFile.deleteOnExit();
    return tempFile;
  }

  /**
   * 将所有需要注入的类写入 JAR
   *
   * @return 成功写入的类数量
   */
  private static int writeClassesToJar(File jarFile) throws IOException {
    int count = 0;
    // 使用当前类的 ClassLoader（Agent ClassLoader）来读取 .class 资源
    ClassLoader agentClassLoader = BootstrapClassInjector.class.getClassLoader();

    try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile))) {
      for (String className : CLASS_NAMES_TO_INJECT) {
        // 写入类本体
        if (writeOneClass(jos, agentClassLoader, className)) {
          count++;
        }

        // 兜底：自动扫描编译器可能生成的 synthetic 内部类（$1, $2, ..., $5）
        // 如 switch(enum) 的 $SwitchMap 类、private 构造器的访问桥接类等
        for (int i = 1; i <= 5; i++) {
          String syntheticName = className + "$" + i;
          if (writeOneClass(jos, agentClassLoader, syntheticName)) {
            count++;
            logger.log(Level.INFO,
                "[BOOTSTRAP-INJECTOR] Auto-discovered synthetic class: {0}", syntheticName);
          }
        }
      }
    }

    return count;
  }

  /**
   * 将单个类写入 JAR
   *
   * @return 是否写入成功
   */
  private static boolean writeOneClass(
      JarOutputStream jos, ClassLoader classLoader, String className) throws IOException {
    String classResourcePath = className.replace('.', '/') + ".class";
    InputStream classStream = classLoader.getResourceAsStream(classResourcePath);

    if (classStream == null) {
      return false;
    }

    try {
      JarEntry entry = new JarEntry(classResourcePath);
      jos.putNextEntry(entry);

      byte[] buffer = new byte[4096];
      int bytesRead;
      while ((bytesRead = classStream.read(buffer)) != -1) {
        jos.write(buffer, 0, bytesRead);
      }

      jos.closeEntry();

      logger.log(Level.FINE,
          "[BOOTSTRAP-INJECTOR] Added class to JAR: {0}", className);
      return true;
    } finally {
      classStream.close();
    }
  }

  /**
   * 验证注入是否成功
   *
   * <p>尝试从 Bootstrap ClassLoader（null ClassLoader）加载 {@link AdviceDispatcher}。
   */
  private static boolean verifyInjection() {
    try {
      // 使用 null ClassLoader（Bootstrap ClassLoader）尝试加载关键类
      Class<?> bootstrapClass = Class.forName(AdviceDispatcher.class.getName(), false, null);
      logger.log(Level.INFO,
          "[BOOTSTRAP-INJECTOR] Verification: {0} loaded from Bootstrap CL, classLoader={1}",
          new Object[] {bootstrapClass.getName(), bootstrapClass.getClassLoader()});
      return true;
    } catch (ClassNotFoundException e) {
      logger.log(Level.WARNING,
          "[BOOTSTRAP-INJECTOR] Verification failed: AdviceDispatcher not found in Bootstrap CL");
      return false;
    }
  }

  /**
   * 初始化 Bootstrap CL 中的 DynamicInstrumentLogger
   *
   * <p>Bootstrap CL 中的 Advice 类（如 {@code AdviceDispatcher}、{@code DynamicTraceAdvice} 等）
   * 在静态字段初始化时会调用 {@code DynamicInstrumentLogger.getLogger()}。Bootstrap CL 中的
   * {@code DynamicInstrumentLogger} 是独立的类实例，需要单独初始化，否则日志无法输出到独立文件。
   *
   * <p>通过反射调用 Bootstrap CL 中的 {@code DynamicInstrumentLogger.initialize()} 方法。
   */
  private static void initializeBootstrapLogger() {
    try {
      Class<?> bootstrapLoggerClass = Class.forName(
          DynamicInstrumentLogger.class.getName(), true, null);
      java.lang.reflect.Method initMethod =
          bootstrapLoggerClass.getMethod("initialize");
      initMethod.invoke(null);
      logger.log(Level.INFO,
          "[BOOTSTRAP-INJECTOR] Bootstrap CL DynamicInstrumentLogger initialized");
    } catch (Exception e) {
      // 日志初始化失败不应阻塞主流程
      logger.log(Level.WARNING,
          "[BOOTSTRAP-INJECTOR] Failed to initialize Bootstrap CL logger: " + e.getMessage(), e);
    }
  }
}
