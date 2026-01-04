
package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.lang.reflect.Method;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Arthas Tunnel 状态桥接器
 *
 * <p>【模式2核心组件】通过反射从 Arthas 内部获取官方 TunnelClient 的状态，
 * 桥接到 OTel 的状态事件总线和就绪判定机制。
 *
 * <p>职责：
 * <ul>
 *   <li>轮询 Arthas 内部 TunnelClient 的连接/注册状态</li>
 *   <li>将状态变化桥接到 {@link ArthasStateEventBus}</li>
 *   <li>为 {@link ArthasLifecycleManager#markRegistered()} 提供触发源</li>
 *   <li>记录关键事件到 {@link ArthasLifecycleManager.StartupLogCollector}</li>
 * </ul>
 *
 * <p>这个桥接器是"最小侵入"方案：不修改 Arthas 内部代码，只通过反射观察状态。
 */
public final class ArthasTunnelStatusBridge {

  private static final Logger logger = Logger.getLogger(ArthasTunnelStatusBridge.class.getName());


  /** Tunnel 状态 */
  public enum TunnelStatus {
    /** 未知（无法获取状态） */
    UNKNOWN,
    /** 未连接 */
    DISCONNECTED,
    /** 连接中 */
    CONNECTING,
    /** 已连接（等待注册） */
    CONNECTED,
    /** 已注册（tunnel 完全就绪） */
    REGISTERED
  }

  private final ArthasBootstrap arthasBootstrap;
  private final TunnelStatusListener listener;
  private final ArthasLifecycleManager.StartupLogCollector startupLogCollector;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicReference<TunnelStatus> currentStatus =
      new AtomicReference<>(TunnelStatus.UNKNOWN);
  @Nullable private ScheduledFuture<?> pollTask;

  // 反射缓存
  @Nullable private Method getTunnelClientMethod;
  @Nullable private Method isConnectedMethod;
  @Nullable private Method getIdMethod;

  // 诊断：连续未观察到状态变化的轮询次数
  private int unchangedPollCount = 0;

  // 诊断：避免重复刷屏
  private volatile boolean loggedInitFailure = false;

  /**
   * 创建 Tunnel 状态桥接器
   *
   * @param arthasBootstrap Arthas 引导器（提供 ClassLoader 和 Bootstrap 实例）
   * @param listener 状态监听器
   * @param startupLogCollector 启动日志收集器
   */
  public ArthasTunnelStatusBridge(
      ArthasBootstrap arthasBootstrap,
      TunnelStatusListener listener,
      ArthasLifecycleManager.StartupLogCollector startupLogCollector) {
    this.arthasBootstrap = arthasBootstrap;
    this.listener = listener;
    this.startupLogCollector = startupLogCollector;
  }

  /**
   * 启动状态轮询
   *
   * @param scheduler 调度器
   * @param pollIntervalMillis 轮询间隔（毫秒）
   */
  public void start(ScheduledExecutorService scheduler, long pollIntervalMillis) {
    if (!running.compareAndSet(false, true)) {
      logger.log(Level.FINE, "Tunnel status bridge already running");
      return;
    }

    // 新一轮启动：重置诊断标记，避免仅记录一次后永久沉默
    loggedInitFailure = false;
    unchangedPollCount = 0;

    startupLogCollector.addLog("INFO", "Tunnel status bridge starting");

    // 初始化反射（延迟到 Arthas 启动后）
    pollTask =
        scheduler.scheduleWithFixedDelay(
            this::pollTunnelStatus,
            1000, // 首次延迟 1 秒（等待 Arthas 启动）
            pollIntervalMillis,
            TimeUnit.MILLISECONDS);

    logger.log(Level.INFO, "Tunnel status bridge started, poll interval: {0}ms", pollIntervalMillis);
  }

  /** 停止状态轮询 */
  public void stop() {
    if (!running.compareAndSet(true, false)) {
      return;
    }

    if (pollTask != null) {
      pollTask.cancel(false);
      pollTask = null;
    }

    logger.log(Level.INFO, "Tunnel status bridge stopped");
  }

  /**
   * 获取当前 Tunnel 状态
   *
   * @return 当前状态
   */
  public TunnelStatus getCurrentStatus() {
    TunnelStatus status = currentStatus.get();
    return status != null ? status : TunnelStatus.UNKNOWN;
  }

  /**
   * 是否已注册（tunnel 完全就绪）
   *
   * @return 是否已注册
   */
  public boolean isRegistered() {
    return getCurrentStatus() == TunnelStatus.REGISTERED;
  }

  /** 轮询 Tunnel 状态 */
  private void pollTunnelStatus() {
    if (!running.get()) {
      return;
    }

    try {
      TunnelStatus newStatus = fetchTunnelStatus();
      TunnelStatus oldStatus = currentStatus.getAndSet(newStatus);

      // 检测状态变化
      if (oldStatus != newStatus) {
        unchangedPollCount = 0;
        handleStatusChange(oldStatus, newStatus);
      } else {
        unchangedPollCount++;
        // 每隔一段时间输出一次可见的诊断信息，便于定位“为什么一直不注册”
        if (unchangedPollCount % 15 == 0) {
          startupLogCollector.addLog(
              "INFO",
              "Tunnel status unchanged for "
                  + unchangedPollCount
                  + " polls, current="
                  + newStatus
                  + ", arthasRunning="
                  + arthasBootstrap.isRunning());
        }
      }

    } catch (RuntimeException e) {
      // 这里不应吞掉关键信息：至少记录一次 INFO，避免排查困难
      if (!loggedInitFailure) {
        loggedInitFailure = true;
        startupLogCollector.addLog("WARN", "Error polling tunnel status: " + e);
      }
      logger.log(Level.FINE, "Error polling tunnel status: {0}", e.getMessage());
    }
  }

  /**
   * 从 Arthas 内部获取 Tunnel 状态
   *
   * @return Tunnel 状态
   */
  private TunnelStatus fetchTunnelStatus() {
    // 检查 Arthas 是否已启动
    if (!arthasBootstrap.isRunning()) {
      return TunnelStatus.DISCONNECTED;
    }

    Object bootstrapInstance = arthasBootstrap.getBootstrapInstance();
    if (bootstrapInstance == null) {
      return TunnelStatus.DISCONNECTED;
    }

    try {
      // 懒加载反射方法
      if (getTunnelClientMethod == null) {
        initReflection(bootstrapInstance);
      }

      if (getTunnelClientMethod == null) {
        // 反射初始化失败，可能 Arthas 版本不支持
        return TunnelStatus.UNKNOWN;
      }

      // 获取 TunnelClient 实例
      // 注意：getTunnelClient 是 com.taobao.arthas.core.server.ArthasBootstrap 上的实例方法，
      // 其接收者应该是 ArthasBootstrap.getInstance(...) 返回的对象，而不是 tunnelClient 本身。
      Object tunnelClient = getTunnelClientMethod.invoke(bootstrapInstance);

      if (tunnelClient == null) {
        // TunnelClient 未创建（可能未配置 tunnel-server）
        return TunnelStatus.DISCONNECTED;
      }

      // 检查连接状态
      if (isConnectedMethod != null) {
        Boolean connected = (Boolean) isConnectedMethod.invoke(tunnelClient);
        if (connected == null || !connected) {
          // 未连接应视为 DISCONNECTED（连接中无法可靠区分，这里保持保守）
          return TunnelStatus.DISCONNECTED;
        }
      }

      // 检查是否已注册（通过 agentId 是否已分配来判断）
      if (getIdMethod != null) {
        String agentId = (String) getIdMethod.invoke(tunnelClient);
        if (agentId != null && !agentId.isEmpty()) {
          return TunnelStatus.REGISTERED;
        }
      }

      // 已连接但未注册
      return TunnelStatus.CONNECTED;

    } catch (ReflectiveOperationException e) {
      // 反射异常通常意味着 classloader/版本不匹配，需要可见日志
      if (!loggedInitFailure) {
        loggedInitFailure = true;
        startupLogCollector.addLog("WARN", "Reflection error fetching tunnel status: " + e);
      }
      logger.log(Level.FINE, "Reflection error fetching tunnel status: {0}", e.getMessage());
      return TunnelStatus.UNKNOWN;
    }
  }

  /**
   * 初始化反射方法
   *
   * @param bootstrapInstance Arthas Bootstrap 实例
   */
  private void initReflection(Object bootstrapInstance) {
    try {
      Class<?> bootstrapClass = bootstrapInstance.getClass();

      // 尝试获取 getTunnelClient 方法（不同版本可能方法名不同）
      for (String methodName : new String[]{"getTunnelClient", "tunnelClient"}) {
        try {
          getTunnelClientMethod = bootstrapClass.getMethod(methodName);
          break;
        } catch (NoSuchMethodException e) {
          // 尝试下一个方法名
        }
      }

      if (getTunnelClientMethod == null) {
        logger.log(Level.FINE, "TunnelClient getter method not found in ArthasBootstrap");
        startupLogCollector.addLog(
            "WARN", "TunnelClient getter not found in ArthasBootstrap (unsupported Arthas version?)");
        loggedInitFailure = true;
        return;
      }

      // 获取 TunnelClient 类的方法
      ClassLoader arthasLoader = arthasBootstrap.getArthasClassLoader();
      if (arthasLoader != null) {
        Class<?> tunnelClientClass = null;

        // Arthas 3.x/4.x 不同包名：优先 com.alibaba（当前资源包），再兼容 com.taobao
        for (String cn :
            new String[] {"com.alibaba.arthas.tunnel.client.TunnelClient",
                "com.taobao.arthas.tunnel.client.TunnelClient"}) {
          try {
            tunnelClientClass = arthasLoader.loadClass(cn);
            break;
          } catch (ClassNotFoundException e) {
            // try next
          }
        }

        if (tunnelClientClass == null) {
          startupLogCollector.addLog(
              "WARN",
              "TunnelClient class not found in Arthas ClassLoader (checked com.alibaba/com.taobao). "
                  + "This usually means arthas-core.jar resources are incomplete.");
          loggedInitFailure = true;
          return;
        }

        try {
          isConnectedMethod = tunnelClientClass.getMethod("isConnected");
        } catch (NoSuchMethodException e) {
          startupLogCollector.addLog("WARN", "TunnelClient.isConnected not found");
        }

        try {
          getIdMethod = tunnelClientClass.getMethod("getId");
        } catch (NoSuchMethodException e) {
          startupLogCollector.addLog("WARN", "TunnelClient.getId not found");
        }

        startupLogCollector.addLog(
            "INFO",
            "TunnelClient reflection initialized: class="
                + tunnelClientClass.getName()
                + ", hasIsConnected="
                + (isConnectedMethod != null)
                + ", hasGetId="
                + (getIdMethod != null));
      } else {
        startupLogCollector.addLog("WARN", "Arthas ClassLoader not available for tunnel reflection");
        loggedInitFailure = true;
      }

    } catch (RuntimeException e) {
      loggedInitFailure = true;
      startupLogCollector.addLog("WARN", "Failed to initialize tunnel status reflection: " + e);
      logger.log(Level.WARNING, "Failed to initialize tunnel status reflection", e);
    }
  }

  /**
   * 处理状态变化
   *
   * @param oldStatus 旧状态
   * @param newStatus 新状态
   */
  private void handleStatusChange(TunnelStatus oldStatus, TunnelStatus newStatus) {
    logger.log(
        Level.INFO,
        "Tunnel status changed: {0} -> {1}",
        new Object[]{oldStatus, newStatus});

    startupLogCollector.addLog(
        "INFO",
        String.format("Tunnel status: %s -> %s", oldStatus, newStatus));

    // 通知监听器
    switch (newStatus) {
      case CONNECTED:
        listener.onTunnelConnected();
        break;
      case REGISTERED:
        listener.onTunnelRegistered();
        break;
      case DISCONNECTED:
        if (oldStatus == TunnelStatus.CONNECTED || oldStatus == TunnelStatus.REGISTERED) {
          listener.onTunnelDisconnected("status_change");
        }
        break;
      default:
        // UNKNOWN, CONNECTING - 不触发特定事件
        break;
    }
  }

  /** Tunnel 状态监听器 */
  public interface TunnelStatusListener {
    /** Tunnel 已连接（WebSocket 建立） */
    void onTunnelConnected();

    /** Tunnel 已注册（收到 REGISTER_ACK） */
    void onTunnelRegistered();

    /** Tunnel 已断开 */
    void onTunnelDisconnected(String reason);
  }
}
