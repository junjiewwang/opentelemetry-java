# Arthas 状态管理与任务执行经验文档

## 目录

1. [整体架构概览](#1-整体架构概览)
2. [核心组件职责](#2-核心组件职责)
3. [状态管理机制](#3-状态管理机制)
4. [任务执行流程](#4-任务执行流程)
5. [自愈机制设计](#5-自愈机制设计)
6. [关键设计决策与经验教训](#6-关键设计决策与经验教训)
7. [常见问题与排查指南](#7-常见问题与排查指南)

---

## 1. 整体架构概览

### 1.1 模式2架构说明

当前采用**模式2架构**：由 Arthas 内部 TunnelClient(Netty) 负责 tunnel 连接，OTel 侧通过状态观测和事件驱动进行协调。

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           OTel Control Plane Agent                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────┐     ┌─────────────────────┐     ┌──────────────────┐  │
│  │ TaskDispatcher  │────►│ ArthasAttachExecutor│────►│ ArthasIntegration│  │
│  │                 │     │ ArthasDetachExecutor│     │                  │  │
│  └────────┬────────┘     └─────────────────────┘     └────────┬─────────┘  │
│           │                                                    │            │
│           │ task dispatch                                      │            │
│           ▼                                                    ▼            │
│  ┌─────────────────┐     ┌─────────────────────┐     ┌──────────────────┐  │
│  │TaskLongPollHdlr │     │ArthasLifecycleMgr   │◄───►│ ArthasStateEventBus│ │
│  │                 │     │                     │     │                  │  │
│  └─────────────────┘     └────────┬────────────┘     └────────┬─────────┘  │
│                                   │                           │            │
│                                   │                           │            │
│                                   ▼                           ▼            │
│                          ┌─────────────────┐         ┌──────────────────┐  │
│                          │ ArthasBootstrap │         │ArthasTunnelStatus│  │
│                          │                 │◄───────►│     Bridge       │  │
│                          └────────┬────────┘         └──────────────────┘  │
│                                   │                                        │
│                                   │ 反射调用                                │
│                                   ▼                                        │
│                          ┌─────────────────┐                               │
│                          │   Arthas Core   │                               │
│                          │ (内部TunnelClient)│                              │
│                          └─────────────────┘                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ WebSocket (ws://...)
                                    ▼
                          ┌─────────────────┐
                          │  Tunnel Server  │
                          │  (Control Plane)│
                          └─────────────────┘
```

### 1.2 核心设计原则

| 原则 | 说明 |
|------|------|
| **状态事件驱动** | 使用 `ArthasStateEventBus` 统一发布/订阅状态事件，避免轮询和竞态 |
| **最小侵入** | 通过反射观察 Arthas 内部状态，不修改 Arthas 源码 |
| **单一职责** | 每个组件只负责一项核心职责，通过事件总线解耦 |
| **渐进降级** | 支持多种启动方式、多种检测手段，确保兼容不同 Arthas 版本 |
| **自愈能力** | 检测异常状态并主动修复，提升系统鲁棒性 |

---

## 2. 核心组件职责

### 2.1 组件职责矩阵

| 组件 | 文件 | 核心职责 |
|------|------|----------|
| **ArthasBootstrap** | `ArthasBootstrap.java` | Arthas 动态加载、反射启动/停止、SpyAPI 加载 |
| **ArthasLifecycleManager** | `ArthasLifecycleManager.java` | 生命周期状态机管理、定时任务调度 |
| **ArthasIntegration** | `ArthasIntegration.java` | 组件协调、健康检查、事件监听器实现 |
| **ArthasTunnelStatusBridge** | `ArthasTunnelStatusBridge.java` | 从 Arthas 内部获取 tunnel 状态 |
| **ArthasStateEventBus** | `ArthasStateEventBus.java` | 统一状态事件发布/订阅/等待 |
| **ArthasReadinessGate** | `ArthasReadinessGate.java` | 就绪判定、等待就绪 |
| **ArthasAttachExecutor** | `ArthasAttachExecutor.java` | 执行 attach 任务、自愈清理 |
| **ArthasDetachExecutor** | `ArthasDetachExecutor.java` | 执行 detach 任务 |
| **TaskDispatcher** | `TaskDispatcher.java` | 任务分发、超时控制、结果上报 |
| **TaskLongPollHandler** | `TaskLongPollHandler.java` | 长轮询获取任务、任务验证 |

### 2.2 组件依赖关系

```
ArthasAttachExecutor/DetachExecutor
         │
         │ 依赖
         ▼
  ArthasIntegration
         │
         ├──────────► ArthasLifecycleManager ─────► ArthasBootstrap
         │                     │
         │                     │ 发布事件
         │                     ▼
         ├──────────► ArthasStateEventBus ◄───────── ArthasTunnelStatusBridge
         │                     │
         │                     │ 观察状态
         │                     ▼
         └──────────► ArthasReadinessGate
```

---

## 3. 状态管理机制

### 3.1 Arthas 生命周期状态机

```
                    ┌──────────────────────────────────────────────────┐
                    │                                                  │
                    ▼                                                  │
             ┌──────────┐         ┌──────────┐         ┌─────────┐   │
     ──────► │  STOPPED │────────►│ STARTING │────────►│ RUNNING │◄──┤
             └──────────┘         └──────────┘         └────┬────┘   │
                  ▲                     │                    │        │
                  │                     │ watchdog timeout   │        │
                  │                     │ / start failed     │ markIdle
                  │                     ▼                    ▼        │
                  │                ┌──────────┐         ┌─────────┐   │
                  │                │ STOPPED  │         │   IDLE  │───┘
                  │                └──────────┘         └────┬────┘
                  │                                          │
                  │        ┌──────────┐                      │ idle timeout
                  │        │ STOPPING │◄─────────────────────┘ / stop()
                  │        └────┬─────┘
                  │             │
                  └─────────────┘
```

**状态说明：**

| 状态 | 含义 | 触发条件 |
|------|------|----------|
| `STOPPED` | 已停止 | 初始状态 / stop() 完成 / 启动失败 |
| `STARTING` | 启动中 | tryStart() 调用 |
| `RUNNING` | 运行中 | ArthasBootstrap.start() 成功 |
| `IDLE` | 空闲 | 无活跃会话且超过空闲时间 |
| `STOPPING` | 停止中 | stop() 调用 |

### 3.2 Tunnel 状态

```
  UNKNOWN ──────► DISCONNECTED ◄─────────┐
                       │                 │
                       │ TunnelClient    │
                       │ connected       │
                       ▼                 │
                 ┌──────────┐            │
                 │CONNECTING│            │
                 └────┬─────┘            │
                      │ WebSocket open   │
                      ▼                  │
                 ┌──────────┐            │
                 │CONNECTED │            │ 断开连接
                 └────┬─────┘            │
                      │ REGISTER_ACK     │
                      ▼                  │
                 ┌──────────┐            │
                 │REGISTERED│────────────┘
                 └──────────┘
```

**Tunnel 状态说明：**

| 状态 | 含义 | 检测方式 |
|------|------|----------|
| `UNKNOWN` | 无法获取状态 | 反射失败或 Arthas 未启动 |
| `DISCONNECTED` | 未连接 | TunnelClient.isConnected() = false |
| `CONNECTING` | 连接中 | - |
| `CONNECTED` | 已连接等待注册 | isConnected = true, getId() = null |
| `REGISTERED` | tunnel 完全就绪 | isConnected = true, getId() != null |

### 3.3 状态事件总线 (ArthasStateEventBus)

**核心能力：**

```java
// 1. 发布事件
stateEventBus.publishTunnelRegistered();
stateEventBus.publishArthasState(State.RUNNING);

// 2. 订阅事件（支持 replay）
Subscription sub = stateEventBus.subscribe(listener, /* replay= */ true);

// 3. 事件驱动等待（非轮询）
CompletableFuture<State> future = stateEventBus.await(
    state -> state.isTunnelRegistered(),
    Duration.ofSeconds(30)
);
```

**State 快照结构：**

```java
public static final class State {
    boolean tunnelConnected;    // tunnel WebSocket 是否连接
    boolean tunnelRegistered;   // tunnel 是否已注册（收到 REGISTER_ACK）
    ArthasLifecycleManager.State arthasState;  // Arthas 本地生命周期状态
}
```

### 3.4 就绪门闩 (ArthasReadinessGate)

**就绪等级：**

| Readiness | 含义 |
|-----------|------|
| `TERMINAL_READY` | Arthas 本地可用 + Tunnel 已注册，可创建 Terminal |
| `ARTHAS_READY_BUT_TUNNEL_NOT_READY` | Arthas 本地可用，但 Tunnel 未就绪 |
| `TUNNEL_READY_BUT_ARTHAS_NOT_READY` | Tunnel 已就绪，但 Arthas 本地不可用 |
| `NOT_READY` | 两者都不可用 |

**使用方式：**

```java
// 同步判断
Result result = readinessGate.evaluateNow();
if (result.isTerminalReady()) {
    // 可以创建 terminal
}

// 异步等待
CompletableFuture<Result> future = readinessGate.awaitTerminalReady(Duration.ofSeconds(30));
```

---

## 4. 任务执行流程

### 4.1 任务获取与分发

```
┌───────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ Control Plane │────►│TaskLongPollHandler│────►│  TaskDispatcher  │
│    Server     │     │                  │     │                  │
└───────────────┘     └──────────────────┘     └────────┬─────────┘
                                                        │
                           ┌────────────────────────────┼───────────────────┐
                           │                            │                   │
                           ▼                            ▼                   ▼
                   ┌───────────────┐          ┌───────────────┐    ┌───────────────┐
                   │ArthasAttachExe│          │ArthasDetachExe│    │  Other Exes   │
                   └───────────────┘          └───────────────┘    └───────────────┘
```

### 4.2 Attach 任务执行流程

```
ArthasAttachExecutor.execute()
         │
         ▼
┌─────────────────────────────────────┐
│ 1. 检查前置条件                      │
│    - ArthasIntegration != null      │
│    - 获取超时参数                    │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│ 2. 自愈健康检查                      │
│    cleanupUnhealthyArthasIfNeeded() │
│    - 检查 lifecycle 状态             │
│    - 检查 tunnel 状态                │
│    - 检查断线时长 vs grace           │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│ 3. 快速路径检查                      │
│    if (isReadyNow()) return SUCCESS │
└────────────────┬────────────────────┘
                 │ 未就绪
                 ▼
┌─────────────────────────────────────┐
│ 4. 事件驱动模式执行                  │
│    - 订阅 tunnel 注册事件            │
│    - 订阅启动失败事件                │
│    - 发起启动请求（非阻塞）          │
│    - 等待事件回调                    │
└────────────────┬────────────────────┘
                 │
         ┌───────┴───────┐
         │               │
         ▼               ▼
    注册成功         超时/失败
    SUCCESS        TIMEOUT/FAILED
```

### 4.3 Detach 任务执行流程

```
ArthasDetachExecutor.execute()
         │
         ▼
┌─────────────────────────────────────┐
│ 1. 前置检查                          │
│    - bootstrap.isRunning() == false │
│      → 同步状态并返回 SUCCESS        │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│ 2. 检查生命周期状态                  │
│    - STOPPED → 返回 SUCCESS         │
│    - STOPPING → 等待完成            │
│    - RUNNING/IDLE → 发起 stop()     │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│ 3. 等待停止完成 (waitForStopped)     │
│    双重检查条件：                    │
│    - lifecycle 状态 == STOPPED      │
│    - bootstrap.isRunning() == false │
│    任一满足即视为停止成功            │
└────────────────┬────────────────────┘
                 │
         ┌───────┴───────┐
         │               │
         ▼               ▼
     停止成功         超时/取消
     SUCCESS       TIMEOUT/CANCELLED
```

### 4.4 任务状态上报

```
任务执行 ─────► TaskDispatcher ─────► ControlPlaneClient.reportTaskResult()
                    │
                    │ 上报时机：
                    │ 1. 任务开始 → RUNNING
                    │ 2. 任务成功 → SUCCESS
                    │ 3. 任务失败 → FAILED
                    │ 4. 任务超时 → TIMEOUT
                    │ 5. 任务取消 → CANCELLED
                    ▼
            ┌───────────────┐
            │ Control Plane │
            │    Server     │
            └───────────────┘
```

---

## 5. 自愈机制设计

### 5.1 自愈触发条件

**不健康的定义：**
- Arthas 本地状态为 RUNNING 或 IDLE
- Tunnel 未就绪（未连接或未注册）
- 断线时长超过宽限期（grace period）

### 5.2 Grace 动态计算

**计算公式：**

```
grace = min(MAX_GRACE, max(MIN_GRACE, effectiveTimeout - RESTART_BUDGET))
```

**参数说明：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `RESTART_BUDGET_MILLIS` | 15s | 自愈后重启预算 |
| `MIN_GRACE_PERIOD_MILLIS` | 5s | grace 最小值，避免误杀 |
| `MAX_GRACE_PERIOD_MILLIS` | 30s | grace 最大值 |

**计算示例：**

| effectiveTimeout | 计算过程 | grace 结果 |
|-----------------|----------|-----------|
| 30s | 30-15=15 | **15s** |
| 60s | 60-15=45 → MAX限制 | **30s** |
| 18s | 18-15=3 → MIN限制 | **5s** |

### 5.3 自愈流程

```
cleanupUnhealthyArthasIfNeeded()
         │
         ▼
┌─────────────────────────────────────┐
│ 1. 检查 lifecycle 状态              │
│    if (!isRunning()) return false   │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│ 2. 检查 tunnel 状态                 │
│    if (isTunnelReady()) return false│
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│ 3. 检查断线时长                     │
│    if (断线时长 < grace) return false│
└────────────────┬────────────────────┘
                 │ 超过 grace
                 ▼
┌─────────────────────────────────────┐
│ 4. 执行清理                         │
│    syncStoppedFromExternalSignal()  │
│    → 销毁旧 Arthas 实例             │
│    → 后续流程重新启动新实例         │
└─────────────────────────────────────┘
```

### 5.4 时间参数关系图

```
├────────────────────── effectiveTimeout (30s) ──────────────────────┤
│                                                                    │
├──────── grace (15s) ────────┤──────── RESTART_BUDGET (15s) ────────┤
│                             │                                      │
│  等待 tunnel 自动重连       │  销毁 + 重启 + 注册                   │
│  如果超过 grace 仍未恢复    │                                      │
│  则触发自愈清理             │                                      │
│                             │                                      │
└─────────────────────────────┴──────────────────────────────────────┘
```

---

## 6. 关键设计决策与经验教训

### 6.1 状态同步问题

**问题：** 当用户在 Arthas Terminal 中执行 `stop` 命令时，Arthas 内部直接调用 `destroy()`，不经过 `ArthasLifecycleManager.stop()` 路径，导致生命周期状态不同步。

**解决方案：**

1. **ArthasBootstrap.isRunning() 增强检查**
   ```java
   public boolean isRunning() {
       if (!running.get()) return false;
       Object bootstrap = arthasBootstrapInstance.get();
       if (bootstrap == null) return false;
       // 调用 isBind() 检查真实状态
       return isArthasActuallyRunning(bootstrap);
   }
   ```

2. **waitForStopped() 双重检查**
   ```java
   // 条件1：lifecycle 状态
   if (state == State.STOPPED) return SUCCESS;
   // 条件2：Arthas 实际运行状态
   if (!bootstrap.isRunning()) {
       manager.syncStoppedFromExternalSignal(...);
       return SUCCESS;
   }
   ```

3. **syncStoppedFromExternalSignal() 状态回灌**
   - 由 `ArthasTunnelStatusBridge.onTunnelDisconnected()` 检测并触发
   - best-effort 调用 `bootstrap.stop()`
   - 重置所有状态标志
   - 发布 STOPPED 事件

### 6.2 Arthas 单例重置问题

**问题：** Arthas 使用静态单例模式，`stop()` 后如果不重置单例字段，下次 `getInstance()` 会返回"已死"的旧实例。

**解决方案：**

```java
private static void stopArthasViaReflection(Object bootstrap) {
    // 1. 调用 destroy()
    Method destroyMethod = bootstrap.getClass().getMethod("destroy");
    destroyMethod.invoke(bootstrap);
    
    // 2. 重置静态单例字段
    resetArthasSingletonField(bootstrap.getClass());
}

private static void resetArthasSingletonField(Class<?> bootstrapClass) {
    String[] possibleFieldNames = {"arthasBootstrap", "INSTANCE", "instance"};
    for (String fieldName : possibleFieldNames) {
        try {
            Field instanceField = bootstrapClass.getDeclaredField(fieldName);
            instanceField.setAccessible(true);
            if (Modifier.isStatic(instanceField.getModifiers())) {
                instanceField.set(null, null);
                return;
            }
        } catch (NoSuchFieldException e) {
            // try next
        }
    }
}
```

### 6.3 事件驱动 vs 轮询

**为什么采用事件驱动：**

| 对比维度 | 轮询方式 | 事件驱动 |
|---------|---------|---------|
| 资源消耗 | 持续占用 CPU | 按需触发 |
| 响应延迟 | 取决于轮询间隔 | 实时 |
| 代码复杂度 | 简单但分散 | 集中但需要事件总线 |
| 竞态风险 | 高（状态可能在两次轮询之间变化） | 低（订阅后不丢事件） |

**ArthasStateEventBus.await() 实现要点：**

```java
public CompletableFuture<State> await(Predicate<State> predicate, Duration timeout) {
    // 1. 先检查当前状态（避免已达成但仍等待）
    State current = state.get();
    if (predicate.test(current)) {
        return CompletableFuture.completedFuture(current);
    }
    
    // 2. 订阅事件
    Subscription subscription = subscribe(listener, false);
    
    // 3. 订阅后再检查一次（防止并发漏事件）
    State after = state.get();
    if (predicate.test(after)) {
        subscription.close();
        return CompletableFuture.completedFuture(after);
    }
    
    // 4. 设置超时
    scheduler.schedule(() -> {
        future.completeExceptionally(new TimeoutException(...));
        subscription.close();
    }, timeout);
    
    return future;
}
```

### 6.4 Grace 与 Timeout 冲突问题

**问题：** 当 `grace = startTimeout` 时，自愈几乎帮不上忙——在预算末尾才触发清理，无法在本次 attach 内完成重启。

**解决方案：** grace 动态计算，预留重启预算

```java
private static long calculateGracePeriod(long effectiveTimeout) {
    long remaining = effectiveTimeout - RESTART_BUDGET_MILLIS;
    return Math.min(MAX_GRACE_PERIOD_MILLIS, Math.max(MIN_GRACE_PERIOD_MILLIS, remaining));
}
```

### 6.5 启动看门狗 (Watchdog)

**问题：** `ArthasBootstrap.start()` 内部反射调用可能阻塞（例如死锁或无限等待）。

**解决方案：**

```java
// 启动看门狗：25秒后如果仍在 STARTING 状态，则强制回退到 STOPPED
startupWatchdogTask = scheduler.schedule(() -> {
    if (state.get() == State.STARTING) {
        logger.log(Level.WARNING, "Arthas startup watchdog fired");
        state.set(State.STOPPED);
        listener.onArthasStopped();
    }
}, STARTUP_WATCHDOG_MILLIS, TimeUnit.MILLISECONDS);
```

---

## 7. 常见问题与排查指南

### 7.1 Attach 任务超时 (EXECUTION_TIMEOUT)

**症状：**
```
[TASK-FAILED] errorCode=EXECUTION_TIMEOUT, errorMessage=Tunnel registration timeout after 30000ms
```

**排查步骤：**

1. **检查 Startup Logs**
   - 查看 `Startup logs: [...]` 中的关键事件
   - 关注 `Tunnel status: CONNECTED -> DISCONNECTED` 等状态变化

2. **确认 tunnel server 可达**
   - 检查 `arthas.tunnelServer` 配置
   - 确认网络连通性

3. **确认 Arthas 内部 TunnelClient 状态**
   - 观察 `TunnelStatus unchanged for X polls` 日志
   - 如果长时间 `DISCONNECTED`，可能是 tunnel server 问题

4. **检查是否触发自愈**
   - 查找 `[ATTACH-CLEANUP]` 日志
   - 如果 grace 时间过短/过长，调整参数

### 7.2 Detach 任务等待超时

**症状：**
```
[ARTHAS-DETACH] Stop timeout after Xms, current state: STOPPING
```

**排查步骤：**

1. **检查是否外部 stop**
   - 用户是否在 Terminal 中执行了 `stop` 命令
   - 此时 lifecycle 状态可能未同步

2. **检查 bootstrap.isRunning()**
   - 如果返回 false 但状态不是 STOPPED，说明状态未同步
   - 应该触发 `syncStoppedFromExternalSignal()`

3. **检查 onTunnelDisconnected 是否触发回灌**
   - 如果 `lifecycleManager.isRunning()` 返回 true 但 `bootstrap.isRunning()` 返回 false
   - 应该调用 `syncStoppedFromExternalSignal()`

### 7.3 Attach 快速返回成功但 Terminal 不可用

**症状：**
- Attach 任务返回 SUCCESS
- 但 Terminal 无法连接或无响应

**排查步骤：**

1. **检查 ReadinessGate 判定**
   - `isTerminalReady()` 返回 true 的条件：
     - `tunnelConnected = true`
     - `tunnelRegistered = true`
     - `arthasState = RUNNING/IDLE`

2. **检查 isTunnelReady() 判定**
   - `ArthasTunnelStatusBridge.isRegistered()` 返回 true 的条件：
     - `TunnelClient.isConnected() = true`
     - `TunnelClient.getId() != null`

3. **确认 attach 成功判定的双重检查**
   ```java
   // 方式1：ReadinessGate 完整检查
   if (isReadyNow()) return SUCCESS;
   
   // 方式2：直接检查（快速路径）
   if (arthasState in {RUNNING, IDLE} && isTunnelReady()) return SUCCESS;
   ```

### 7.4 日志关键字索引

| 日志前缀 | 含义 |
|---------|------|
| `[ARTHAS-ATTACH]` | Attach 执行器日志 |
| `[ARTHAS-DETACH]` | Detach 执行器日志 |
| `[ATTACH-CLEANUP]` | 自愈清理日志 |
| `[MODE2]` | 模式2（内部 tunnel）相关日志 |
| `[TASK-*]` | 任务分发/执行/上报日志 |
| `Tunnel status changed` | Tunnel 状态变化 |
| `Lifecycle state ->` | 生命周期状态变化 |

---

## 附录：配置参数参考

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `start_timeout_millis` | 30000 | Arthas 启动超时 |
| `connect_timeout_millis` | 10000 | Tunnel 连接超时 |
| `health_check_grace_period_millis` | 动态计算 | 自愈宽限期 |
| `stop_timeout_millis` | 10000 | 停止等待超时 |
| `force` (detach) | false | 是否强制停止 |

---

*文档版本：v1.0*  
*最后更新：2026-01-06*
