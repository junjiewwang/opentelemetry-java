/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas 终端桥接器
 *
 * <p>负责管理与 Arthas 会话的终端交互，包括：
 * <ul>
 *   <li>将用户输入转发给 Arthas
 *   <li>收集 Arthas 输出并通过回调发送
 *   <li>管理终端尺寸
 *   <li>会话缓冲区管理
 * </ul>
 */
public final class ArthasTerminalBridge implements Closeable {

  private static final Logger logger = Logger.getLogger(ArthasTerminalBridge.class.getName());

  // Arthas 相关类名
  private static final String TERM_CLASS = "com.taobao.arthas.core.shell.term.Term";

  // 时间格式化器
  private static final DateTimeFormatter TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", java.util.Locale.ROOT).withZone(ZoneOffset.UTC);

  private final ArthasConfig config;
  private final OutputHandler outputHandler;
  private final Map<String, SessionTerminal> sessionTerminals = new ConcurrentHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  @Nullable private ArthasBootstrap arthasBootstrap;
  @Nullable private ArthasLifecycleManager lifecycleManager;
  @Nullable private ScheduledExecutorService scheduler;
  @Nullable private ScheduledFuture<?> flushTask;

  /**
   * 创建终端桥接器
   *
   * @param config Arthas 配置
   * @param outputHandler 输出处理器
   */
  public ArthasTerminalBridge(ArthasConfig config, OutputHandler outputHandler) {
    this.config = config;
    this.outputHandler = outputHandler;
  }

  /**
   * 设置 Arthas Bootstrap
   *
   * @param bootstrap Arthas Bootstrap 实例
   */
  public void setArthasBootstrap(ArthasBootstrap bootstrap) {
    this.arthasBootstrap = bootstrap;
  }

  /**
   * 设置 Arthas 生命周期管理器
   *
   * @param lifecycleManager 生命周期管理器
   */
  public void setLifecycleManager(ArthasLifecycleManager lifecycleManager) {
    this.lifecycleManager = lifecycleManager;
  }

  /**
   * 启动终端桥接器
   *
   * @param scheduler 调度器
   */
  public void start(ScheduledExecutorService scheduler) {
    this.scheduler = scheduler;
    startFlushTask();
    logger.log(Level.INFO, "Arthas terminal bridge started");
  }

  /**
   * 创建会话终端
   *
   * @param session 会话信息
   * @return 是否创建成功
   */
  public boolean createSessionTerminal(ArthasSession session) {
    if (closed.get()) {
      return false;
    }

    String sessionId = session.getSessionId();
    if (sessionTerminals.containsKey(sessionId)) {
      logger.log(Level.WARNING, "Session terminal already exists: {0}", sessionId);
      return false;
    }

    SessionTerminal terminal = new SessionTerminal(
        sessionId,
        session.getCols(),
        session.getRows(),
        config.getOutputBufferSize()
    );

    // 尝试绑定到 Arthas 会话
    boolean bound = tryBindToArthasSession(terminal, session);
    if (!bound) {
      logger.log(Level.WARNING, "Failed to bind to Arthas session, running in echo mode: {0}", sessionId);
      // 即使绑定失败也创建终端，用于 echo 模式测试
    }

    sessionTerminals.put(sessionId, terminal);
    logger.log(Level.INFO, "Session terminal created: {0}, bound: {1}", new Object[]{sessionId, bound});
    
    // 发送欢迎消息
    sendWelcomeMessage(terminal);
    
    return true;
  }

  /**
   * 销毁会话终端
   *
   * @param sessionId 会话 ID
   */
  public void destroySessionTerminal(String sessionId) {
    SessionTerminal terminal = sessionTerminals.remove(sessionId);
    if (terminal != null) {
      terminal.close();
      logger.log(Level.INFO, "Session terminal destroyed: {0}", sessionId);
    }
  }

  /**
   * 处理终端输入
   *
   * @param sessionId 会话 ID
   * @param input 输入数据
   */
  public void handleInput(String sessionId, String input) {
    SessionTerminal terminal = sessionTerminals.get(sessionId);
    if (terminal == null) {
      logger.log(Level.WARNING, "Session terminal not found for input: {0}", sessionId);
      return;
    }

    terminal.onInput(input);
  }

  /**
   * 处理终端尺寸调整
   *
   * @param sessionId 会话 ID
   * @param cols 列数
   * @param rows 行数
   */
  public void handleResize(String sessionId, int cols, int rows) {
    SessionTerminal terminal = sessionTerminals.get(sessionId);
    if (terminal == null) {
      logger.log(Level.WARNING, "Session terminal not found for resize: {0}", sessionId);
      return;
    }

    terminal.resize(cols, rows);
    logger.log(Level.FINE, "Terminal resized: session={0}, cols={1}, rows={2}", 
        new Object[]{sessionId, cols, rows});
  }

  /**
   * 获取会话终端
   *
   * @param sessionId 会话 ID
   * @return 终端，如果不存在返回 null
   */
  @Nullable
  public SessionTerminal getSessionTerminal(String sessionId) {
    return sessionTerminals.get(sessionId);
  }

  /**
   * 检查是否有活跃的会话终端
   *
   * @return 是否有活跃终端
   */
  public boolean hasActiveTerminals() {
    return !sessionTerminals.isEmpty();
  }

  /**
   * 获取活跃终端数量
   *
   * @return 终端数量
   */
  public int getActiveTerminalCount() {
    return sessionTerminals.size();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      // 停止刷新任务
      if (flushTask != null) {
        flushTask.cancel(false);
        flushTask = null;
      }

      // 关闭所有终端
      for (SessionTerminal terminal : sessionTerminals.values()) {
        terminal.close();
      }
      sessionTerminals.clear();

      logger.log(Level.INFO, "Arthas terminal bridge closed");
    }
  }

  // ===== 私有方法 =====

  /** 启动输出刷新任务 */
  private void startFlushTask() {
    if (scheduler == null) {
      return;
    }

    long flushIntervalMs = config.getOutputFlushInterval().toMillis();
    flushTask = scheduler.scheduleWithFixedDelay(
        this::flushAllOutputs,
        flushIntervalMs,
        flushIntervalMs,
        TimeUnit.MILLISECONDS
    );
  }

  /** 刷新所有终端的输出缓冲区 */
  private void flushAllOutputs() {
    for (SessionTerminal terminal : sessionTerminals.values()) {
      byte[] data = terminal.flushOutputBuffer();
      if (data != null && data.length > 0) {
        outputHandler.onOutput(terminal.getSessionId(), data);
      }
    }
  }

  /**
   * 尝试绑定到 Arthas 会话
   *
   * @param terminal 终端
   * @param session 会话信息（预留参数，可用于传递会话元数据）
   * @return 是否绑定成功
   */
  @SuppressWarnings("UnusedVariable")
  private boolean tryBindToArthasSession(SessionTerminal terminal, ArthasSession session) {
    // 优先检查 LifecycleManager 状态（更准确）
    if (lifecycleManager != null) {
      ArthasLifecycleManager.State state = lifecycleManager.getState();
      if (state != ArthasLifecycleManager.State.RUNNING 
          && state != ArthasLifecycleManager.State.REGISTERED
          && state != ArthasLifecycleManager.State.IDLE) {
        logger.log(Level.FINE, 
            "Arthas not in running state (via LifecycleManager), current state: {0}", state);
        return false;
      }
    } else if (arthasBootstrap == null || !arthasBootstrap.isRunning()) {
      // 降级检查 ArthasBootstrap
      logger.log(Level.FINE, "Arthas bootstrap not available or not running");
      return false;
    }

    // 获取 ArthasBootstrap（优先从 lifecycleManager 获取）
    ArthasBootstrap effectiveBootstrap = arthasBootstrap;
    if (lifecycleManager != null) {
      effectiveBootstrap = lifecycleManager.getArthasBootstrap();
    }
    
    if (effectiveBootstrap == null) {
      logger.log(Level.FINE, "Effective ArthasBootstrap is null");
      return false;
    }

    Object bootstrap = effectiveBootstrap.getBootstrapInstance();
    if (bootstrap == null) {
      logger.log(Level.FINE, "Arthas bootstrap instance is null");
      return false;
    }

    // 检查是否是 Mock 模式（Mock 模式下无法真正绑定会话）
    if (bootstrap.getClass().getSimpleName().contains("Mock")) {
      logger.log(Level.INFO, 
          "Running in mock mode, real Arthas binding not available. " +
          "Session will operate in echo mode for testing.");
      return false;
    }

    ClassLoader loader = effectiveBootstrap.getArthasClassLoader();
    if (loader == null) {
      logger.log(Level.FINE, "Arthas ClassLoader is null");
      return false;
    }

    // 使用重试机制绑定会话
    try {
      return RetryHelper.executeWithRetry(
          () -> doBindToArthasSession(bootstrap, loader, terminal),
          RetryHelper.RetryConfig.builder()
              .setMaxRetries(2)
              .setInitialDelayMs(100)
              .retryOnRecoverableArthasException()
              .build(),
          new RetryHelper.RetryListener() {
            @Override
            public void onRetryAttempt(int attempt, Exception exception, boolean willRetry) {
              logger.log(
                  Level.FINE,
                  "Arthas session bind attempt {0} failed: {1}, willRetry: {2}",
                  new Object[] {attempt, exception.getMessage(), willRetry});
            }
          });
    } catch (ArthasException e) {
      logger.log(Level.WARNING, "Failed to bind to Arthas session: " + e.getMessage());
      return false;
    }
  }

  /**
   * 执行实际的 Arthas 会话绑定
   *
   * @param bootstrap Arthas Bootstrap 实例
   * @param loader Arthas ClassLoader
   * @param terminal 会话终端
   * @return 是否绑定成功
   */
  @SuppressWarnings("UnusedVariable")
  private static boolean doBindToArthasSession(
      Object bootstrap, ClassLoader loader, SessionTerminal terminal) {
    try {
      // 通过反射获取 ShellServer
      Method getShellServerMethod =
          ArthasReflectionHelper.findMethod(bootstrap.getClass(), "getShellServer");
      if (getShellServerMethod == null) {
        logger.log(Level.WARNING, "getShellServer method not found");
        return false;
      }

      Object shellServer = ArthasReflectionHelper.invokeMethodQuietly(bootstrap, getShellServerMethod);
      if (shellServer == null) {
        logger.log(Level.WARNING, "ShellServer is null");
        throw ArthasException.sessionBindFailed("ShellServer is null", null);
      }

      // 创建自定义 Term 实现
      Object term = createCustomTerm(loader, terminal);
      if (term == null) {
        throw ArthasException.sessionBindFailed("Failed to create Term proxy", null);
      }

      // 尝试创建会话
      // Arthas 的 createSession(Term term) 方法
      Method createSessionMethod =
          ArthasReflectionHelper.findMethodByName(shellServer.getClass(), "createSession");
      if (createSessionMethod == null) {
        logger.log(Level.WARNING, "createSession method not found in ShellServer");
        return false;
      }

      Object arthasSession =
          ArthasReflectionHelper.invokeMethodQuietly(shellServer, createSessionMethod, term);
      if (arthasSession != null) {
        // 获取 Arthas 会话 ID
        String arthasSessionId = ArthasReflectionHelper.getStringProperty(arthasSession, "id");
        if (arthasSessionId == null) {
          // 尝试调用 id() 方法
          Method idMethod = ArthasReflectionHelper.findMethodByName(arthasSession.getClass(), "id");
          if (idMethod != null) {
            Object idValue = ArthasReflectionHelper.invokeMethodQuietly(arthasSession, idMethod);
            arthasSessionId = idValue != null ? idValue.toString() : null;
          }
        }

        // 设置 arthasSessionId（可能为 null）
        if (arthasSessionId != null) {
          terminal.setArthasSessionId(arthasSessionId);
        }
        terminal.setArthasSession(arthasSession);
        logger.log(Level.INFO, "Bound to Arthas session: {0}", arthasSessionId);
        return true;
      }

      return false;

    } catch (ArthasException e) {
      throw e;
    } catch (RuntimeException e) {
      throw ArthasException.sessionBindFailed("Unexpected error during session bind", e);
    }
  }

  /**
   * 创建自定义 Term 实现（使用动态代理）
   *
   * @param loader Arthas ClassLoader
   * @param terminal 会话终端
   * @return Term 代理对象
   */
  @Nullable
  private static Object createCustomTerm(ClassLoader loader, SessionTerminal terminal) {
    try {
      Class<?> termClass = loader.loadClass(TERM_CLASS);
      
      // 创建代理
      return Proxy.newProxyInstance(
          loader,
          new Class<?>[]{termClass},
          new TermInvocationHandler(terminal)
      );

    } catch (ClassNotFoundException e) {
      logger.log(Level.WARNING, "Term class not found", e);
      return null;
    }
  }

  /**
   * 查找方法（忽略参数类型）
   *
   * @param clazz 类
   * @param name 方法名
   * @return 方法，如果未找到返回 null
   */
  @Nullable
  static Method findMethod(Class<?> clazz, String name) {
    for (Method method : clazz.getMethods()) {
      if (method.getName().equals(name)) {
        return method;
      }
    }
    return null;
  }

  /** 发送欢迎消息 */
  private void sendWelcomeMessage(SessionTerminal terminal) {
    String welcome = "\r\n" +
        "  ,---.  ,------. ,--------.,--.  ,--.  ,---.   ,---.\r\n" +
        " /  O  \\ |  .--. ''--.  .--'|  '--'  | /  O  \\ '   .-'\r\n" +
        "|  .-.  ||  '--'.'   |  |   |  .--.  ||  .-.  |`.  `-.\r\n" +
        "|  | |  ||  |\\  \\    |  |   |  |  |  ||  | |  |.-'    |\r\n" +
        "`--' `--'`--' '--'   `--'   `--'  `--'`--' `--'`-----'\r\n" +
        "\r\n" +
        "wiki: https://arthas.aliyun.com/doc\r\n" +
        "tutorials: https://arthas.aliyun.com/doc/arthas-tutorials.html\r\n" +
        "version: " + config.getVersion() + "\r\n" +
        "pid: " + getCurrentPid() + "\r\n" +
        "time: " + TIME_FORMATTER.format(Instant.now()) + "\r\n" +
        "\r\n" +
        "[arthas@" + getCurrentPid() + "]$ ";

    terminal.appendOutput(welcome.getBytes(StandardCharsets.UTF_8));
    
    // 立即刷新欢迎消息
    byte[] data = terminal.flushOutputBuffer();
    if (data != null && data.length > 0) {
      outputHandler.onOutput(terminal.getSessionId(), data);
    }
  }

  /** 获取当前 PID */
  private static long getCurrentPid() {
    String jvmName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
    int atIndex = jvmName.indexOf('@');
    if (atIndex > 0) {
      try {
        return Long.parseLong(jvmName.substring(0, atIndex));
      } catch (NumberFormatException e) {
        // ignore
      }
    }
    return -1;
  }

  // ===== 内部类 =====

  /** 会话终端 */
  public final class SessionTerminal {
    private final String sessionId;
    private volatile int cols;
    private volatile int rows;
    private final ByteArrayOutputStream outputBuffer;
    private final int maxBufferSize;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Nullable private volatile String arthasSessionId;
    @Nullable private volatile Object arthasSession;
    @Nullable private volatile Object inputHandler; // Arthas 的输入处理器

    SessionTerminal(String sessionId, int cols, int rows, int maxBufferSize) {
      this.sessionId = sessionId;
      this.cols = cols;
      this.rows = rows;
      this.maxBufferSize = maxBufferSize;
      this.outputBuffer = new ByteArrayOutputStream(Math.min(maxBufferSize, 8192));
    }

    public String getSessionId() {
      return sessionId;
    }

    public int getCols() {
      return cols;
    }

    public int getRows() {
      return rows;
    }

    @Nullable
    public String getArthasSessionId() {
      return arthasSessionId;
    }

    void setArthasSessionId(String id) {
      this.arthasSessionId = id;
    }

    void setArthasSession(Object session) {
      this.arthasSession = session;
    }

    void setInputHandler(Object handler) {
      this.inputHandler = handler;
    }

    /** 处理输入 */
    void onInput(String input) {
      if (closed.get()) {
        return;
      }

      // 如果有 Arthas 输入处理器，转发输入
      if (inputHandler != null) {
        try {
          Method handleMethod = inputHandler.getClass().getMethod("handle", String.class);
          handleMethod.invoke(inputHandler, input);
          return;
        } catch (ReflectiveOperationException e) {
          logger.log(Level.FINE, "Failed to forward input to Arthas", e);
        }
      }

      // 如果有 Arthas 会话，尝试直接写入
      if (arthasSession != null) {
        try {
          Method readline = arthasSession.getClass().getMethod("readline", String.class);
          readline.invoke(arthasSession, input);
          return;
        } catch (ReflectiveOperationException e) {
          logger.log(Level.FINE, "Failed to write input to Arthas session", e);
        }
      }

      // 回显模式（用于测试）
      String echo = input;
      if (input.endsWith("\r") || input.endsWith("\n")) {
        echo = input + "echo: " + input.trim() + "\r\n[arthas@" + getCurrentPid() + "]$ ";
      }
      appendOutput(echo.getBytes(StandardCharsets.UTF_8));
    }

    /** 调整终端尺寸 */
    void resize(int newCols, int newRows) {
      this.cols = newCols;
      this.rows = newRows;

      // 通知 Arthas 会话
      if (arthasSession != null) {
        try {
          Method resizeMethod = findMethod(arthasSession.getClass(), "resize");
          if (resizeMethod != null) {
            resizeMethod.invoke(arthasSession, newCols, newRows);
          }
        } catch (ReflectiveOperationException e) {
          logger.log(Level.FINE, "Failed to resize Arthas session", e);
        }
      }
    }

    /** 追加输出到缓冲区 */
    synchronized void appendOutput(byte[] data) {
      if (closed.get()) {
        return;
      }

      // 检查缓冲区大小限制
      if (outputBuffer.size() + data.length > maxBufferSize) {
        // 强制刷新
        byte[] existing = flushOutputBuffer();
        if (existing != null && existing.length > 0) {
          outputHandler.onOutput(sessionId, existing);
        }
      }

      outputBuffer.write(data, 0, data.length);
    }

    /** 刷新并返回输出缓冲区 */
    @Nullable
    synchronized byte[] flushOutputBuffer() {
      if (outputBuffer.size() == 0) {
        return null;
      }

      byte[] data = outputBuffer.toByteArray();
      outputBuffer.reset();
      return data;
    }

    /** 关闭终端 */
    void close() {
      if (closed.compareAndSet(false, true)) {
        // 关闭 Arthas 会话
        if (arthasSession != null) {
          try {
            Method closeMethod = arthasSession.getClass().getMethod("close");
            closeMethod.invoke(arthasSession);
          } catch (ReflectiveOperationException e) {
            logger.log(Level.FINE, "Failed to close Arthas session", e);
          }
        }

        // 刷新剩余输出
        byte[] remaining = flushOutputBuffer();
        if (remaining != null && remaining.length > 0) {
          outputHandler.onOutput(sessionId, remaining);
        }
      }
    }
  }

  /** Term 接口的动态代理处理器 */
  private static final class TermInvocationHandler implements InvocationHandler {
    private final SessionTerminal terminal;

    TermInvocationHandler(SessionTerminal terminal) {
      this.terminal = terminal;
    }

    @Override
    @SuppressWarnings({"CheckedExceptionNotThrown", "NullAway"})
    @Nullable
    public Object invoke(Object proxy, Method method, @Nullable Object[] args) {
      String methodName = method.getName();

      switch (methodName) {
        case "write":
          // 处理输出
          if (args != null && args.length > 0) {
            if (args[0] instanceof String) {
              terminal.appendOutput(((String) args[0]).getBytes(StandardCharsets.UTF_8));
            } else if (args[0] instanceof byte[]) {
              terminal.appendOutput((byte[]) args[0]);
            }
          }
          return null;

        case "width":
          return terminal.getCols();

        case "height":
          return terminal.getRows();

        case "setStdinHandler":
          // 保存输入处理器
          if (args != null && args.length > 0) {
            terminal.setInputHandler(args[0]);
          }
          return null;

        case "close":
          // 终端关闭
          terminal.close();
          return null;

        case "toString":
          return "ProxyTerm[" + terminal.getSessionId() + "]";

        case "hashCode":
          return terminal.hashCode();

        case "equals":
          return args != null && proxy == args[0];

        default:
          // 其他方法返回默认值
          return getDefaultReturnValue(method.getReturnType());
      }
    }

    /** 获取方法返回类型的默认值 */
    @Nullable
    private static Object getDefaultReturnValue(Class<?> returnType) {
      if (returnType == boolean.class) {
        return false;
      } else if (returnType == int.class) {
        return 0;
      } else if (returnType == long.class) {
        return 0L;
      }
      return null;
    }
  }

  /** 输出处理器接口 */
  public interface OutputHandler {
    /**
     * 处理输出数据
     *
     * @param sessionId 会话 ID
     * @param data 输出数据
     */
    void onOutput(String sessionId, byte[] data);
  }
}
