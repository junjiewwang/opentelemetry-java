# ReadinessGate 增加交互会话可输出确认

## 需求

`ArthasReadinessGate.TERMINAL_READY` 会在「Arthas 本地交互服务尚未完成 listen」时误报。
现状只检查 `tunnelConnected && tunnelRegistered && (RUNNING|IDLE)`，其中 tunnel 状态来自
`ArthasTunnelStatusBridge` 反射 `TunnelClient.isConnected()` / `getId()`，只反映
`tunnelClient.start()` 完成，不反映 `shellServer.listen()` 完成。

目标：追加「ShellServer 已完成 listen」这一维度，让 `TERMINAL_READY`、
`arthas_attach` 任务成功判定、会话/命令类任务就绪判定都等到真实可交互。

## 源码依据（已用本仓库内嵌 arthas-core.jar 的 javap 复核）

| 事实 | 验证结果 |
|---|---|
| `ShellServerImpl.closed` 私有字段、初值 true | ✅ `private boolean closed` + 构造 `iconst_1/putfield` |
| `setClosed(boolean)` public synchronized、无 `isClosed()` | ✅ |
| `ArthasBootstrap.getShellServer()` public | ✅ |
| `isBindRef` 是 `AtomicBoolean`，`isBind()` 从 bind 开头即 true | ✅ 不能作就绪信号 |
| `TermServerListenHandler.handle` 在 `count.decrementAndGet()==0 && !failed` 时 `setClosed(false)` | ✅ 字节码 |
| `ShellServerImpl.listen()` 在 `termServers.isEmpty()` 时立即 `setClosed(false)` | ✅ 新增发现，需明确语义 |
| `closed` **非 volatile**，`setClosed` 是 synchronized | ✅ 反射读存在 JMM 可见性限制 |

## 边界

- 不改 Arthas 源码、不重新打包 `arthas-core.jar`。
- 不改 `collector` / 前端仓库（H 项标为未完成）。
- 不替换现有三个就绪条件，只追加一层；既有枚举值语义不变。
- 反射失败一律保守 `false`，不抛到轮询线程。
- 文档与日志不记录 token、凭据、内网地址等敏感信息。

## 方案（实际落地 A~G）

| 层 | 内容 |
|---|---|
| A | 新增 `ShellServerReadinessProbe`（public 接口 + `Reflective` 实现）：判空、字段名兼容（`closed`/`listening`，按语义取反）、异常保守 false、独立懒加载与日志去重；只有 `stateField` 成功赋值才算初始化完成，ShellServer 尚未创建时下次轮询重试；不依赖 `isRunning()` 早退分支 |
| B | `ArthasStateEventBus`：新增 `Event.SHELL_SERVER_READY`、`State.shellServerReady`（含全部 `withXxx`）、`publishShellServerReady(boolean)`、replay、`toString`；**断连 / STOPPED / STOPPING / STARTING 一律清场** |
| C | `ArthasTunnelStatusBridge` 在 `pollTunnelStatus()` 入口独立探测，**每次轮询都通知** listener；`TunnelStatusListener` 新增 `onShellServerReady(boolean)`；新增可注入 probe 的构造函数供测试用 fake 桩 |
| D | `ArthasReadinessGate`：`evaluateNow` 加条件并重构为「条件优先级 → ReasonCode/Readiness 一一对应」；新增 `Readiness.TERMINAL_NOT_LISTENING` 与 `ReasonCode.TERMINAL_NOT_LISTENING`；`toErrorCode` 补 case；`Snapshot` 加字段与 `toString`；`awaitTerminalReady` predicate 同步 |
| E | `ArthasAsyncExecutorSupport` 与 `ArthasExecSyncExecutor` 两处 `mapReadinessErrorCode` 补映射；`ArthasTaskProtocol.ErrorCode` 新增 `TERMINAL_NOT_LISTENING` |
| F | `ArthasAttachExecutor`：完成条件从 `isTunnelRegistered` 改为「tunnel 连接+注册、Arthas RUNNING/IDLE、ShellServer ready」四维度；direct-check 兜底同步追加 `isShellServerReady()` |
| G | `ArthasIntegration` 实现 `onShellServerReady`、**按总线状态去重发布**、暴露 `isShellServerReady()` 并在 Javadoc 澄清与 `isTunnelReady()` 的语义差异 |

**评审发现并一并修复的关联问题**：

1. `publishTunnelRegistered()` 原先只置 `registered=true`，而现场 Bridge 会从 `DISCONNECTED`
   直接跳到 `REGISTERED`（跳过 CONNECTED），导致 `tunnelConnected` 恒 false —— 这正是日志里
   走 `direct check` 而非 `isReadyNow()` 的原因。改为「registered 蕴含 connected」。
2. Bridge 若只按「本地观测值变化」通知，总线被断连/重启清场后会永久卡在 false。改为
   **每次轮询通知 + Integration 按总线状态去重发布**，既保证事件驱动唤醒 `await`，又能自恢复。
3. `ArthasBootstrap.isArthasActuallyRunning()` 的 Javadoc 关于 `isBind()` 的描述与事实不符，
   已修正注释（不改行为）。

**开关**：`otel.controlplane.arthas.shellserver.probe.enabled`（默认 true）。设为 `false` 时
`Reflective.isListening()` 恒 true，等价于回滚到旧的三维就绪判定，用于 Arthas 升级后
字段名不兼容等场景的可用性兜底。

## 实施进展

- [x] 建立本文档。
- [x] A：探测接口与反射实现。
- [x] B：状态总线维度、事件、清场。
- [x] C：Bridge 轮询接入与可注入构造。
- [x] D：ReadinessGate 追加条件与细分原因。
- [x] E/F/G：错误码映射、attach 完成条件、Integration 暴露与去重。
- [x] 单测 11 个（含 publish 驱动、清场、重连、attach 条件、反射降级、既有语义回归）。
- [x] 编译、全量测试、Javadoc、完整构建回归。
- [x] 更新本文档验收记录。

## 变更清单

| 文件 | 改动摘要 |
|---|---|
| `arthas/ShellServerReadinessProbe.java`（新增） | 可注入探测接口 + 反射实现（字段兼容、保守失败、重试、开关） |
| `arthas/ArthasStateEventBus.java` | 新增维度/事件/publish；全部 `withXxx` 保留新维度；registered 蕴含 connected；断连与停止清场；replay/toString |
| `arthas/ArthasTunnelStatusBridge.java` | 每次轮询独立探测并通知；`TunnelStatusListener` 加回调；可注入 probe 构造 |
| `arthas/ArthasReadinessGate.java` | 四维度判定重构；新增 Readiness/ReasonCode；toErrorCode 补 case；Snapshot；await predicate |
| `arthas/ArthasIntegration.java` | 实现 `onShellServerReady` 去重发布；暴露 `isShellServerReady()` |
| `arthas/ArthasTaskProtocol.java` | 新增 `TERMINAL_NOT_LISTENING` 错误码 |
| `arthas/ArthasBootstrap.java` | 修正 `isBind()` 相关 Javadoc 注释（不改行为） |
| `task/executor/ArthasAttachExecutor.java` | 完成条件改为四维度；direct-check 兜底追加 ShellServer 条件 |
| `task/executor/ArthasAsyncExecutorSupport.java` | `mapReadinessErrorCode` 补 case |
| `task/executor/ArthasExecSyncExecutor.java` | `mapReadinessErrorCode` 补 case |
| `test/.../ArthasShellServerReadinessTest.java`（新增） | 11 个用例 |
| `docs/2026-11-25/arthas-shell-server-readiness.md` | 本实施记录 |

## 验收记录

| 项目 | 命令 | 结果 |
|---|---|---|
| 主代码 + 测试编译 | `:controlplane:compileJava :controlplane:compileTestJava` | **BUILD SUCCESSFUL**（`-Werror` + ErrorProne + NullAway） |
| 编译期修复 | multi-catch 子类重复、`NullAway` 可空字段、`UnnecessaryFinal`、两处 `MissingCasesInEnumSwitch`、`RemoveUnusedImports`、`BooleanParameter`×18、`RethrowReflectiveOperationExceptionAsLinkageError`、`StringCaseLocaleUsage` | 均按提示修正 |
| 定向单测 | `--tests ArthasShellServerReadinessTest` | **BUILD SUCCESSFUL**，11 个用例全通过 |
| 全量测试 + Javadoc + 发布构建 | `:controlplane:test :controlplane:javadoc publishToMavenLocal` | **BUILD SUCCESSFUL in 16s**，318 tasks |
| 全量测试汇总 | JUnit XML | **28 suites / 252 tests / 0 failures / 0 errors / 0 skipped** |
| IDE 静态诊断 | `read_lints` | 剩余为既有「未使用方法」类提示与 IDE 索引滞后行号（当前 `evaluateNow` 已无 `RUNNING/IDLE` case）；编译与全量构建已验证 |

构建日志中反复出现远程 build cache 拉取失败（`develocity`），Gradle 自动禁用远程缓存，
不影响任何结果。

## 未完成任务

| # | 任务 | 状态 |
|---|------|------|
| 1 | **H：collector / 前端** `attachArthas()` 后固定等 2 秒改为等待可交互就绪；`connectArthas` 在 `agentRegister` 即发 `startTunnel` 的链路改为看 attach 结果或 `terminalBindable` | 不在本仓库，本轮未实施；**此项不完成则终端竞态仍存在** |
| 2 | MCP `tunnelReady` 与 `arthas_status.tunnelReady` 语义对齐 | 未单独改；已通过 `isTerminalBindable()` / `getTerminalNotBindableReason()` 反映新维度，并在 `ArthasIntegration` Javadoc 澄清二者差异 |
| 3 | 部署新版本后实机验证：反射 `closed` 字段在真实 Arthas 4.1.5 上可读、1 秒轮询下的可见性、attach 任务耗时变化 | 待部署侧操作 |
| 4 | 首帧看门狗 + 自动重连兜底（collector 侧） | 建议保留，见遗留问题 |

## 遗留问题与风险

- **终端竞态未彻底消除**：本轮只让 Java Agent 侧的状态与 attach 成功判定变准；collector
  的 `connectArthas` 仍在 `agentRegister` 出现即发 `startTunnel`，不看 attach 任务结果。
  需 H 项配合，或保留前端首帧看门狗 + 自动重连兜底。
- **可用性风险**：若 Arthas 升级后 `closed`/`listening` 字段名都找不到，探测恒 false，
  `TERMINAL_READY` 永不达成，attach 与会话类任务会超时。回滚开关
  `otel.controlplane.arthas.shellserver.probe.enabled=false` 必须保留并可下发。
- **可见性**：`closed` 非 volatile，反射读不参与 synchronized；JMM 上无保证，叠加 1 秒
  轮询，信号最坏晚 1 秒可见，需计入 `awaitTerminalReady` 超时预算。
- **空 termServers 边界**：`listen()` 在 `termServers.isEmpty()` 时会立即 `setClosed(false)`，
  此时语义是「无本地服务需要监听」而非「交互已就绪」。当前 tunnel 模式仍有
  `HttpTermServer`，主路径不受影响；若未来配置变化需重新评估。
- **反射异常类型**：Arthas 类由自定义 URLClassLoader 加载，不受 JDK `--add-opens` 影响；
  真实风险是字段改名 / SecurityManager，已由保守 false + 开关兜底覆盖。
- `ArthasStatusInfo.tunnelReady` 仍来自 `tunnelStatusBridge.isRegistered()`，与
  `terminalBindable`（含新维度）并存，二者语义差异已在 `ArthasIntegration` Javadoc 说明。
- `animalSnifferMain` 存在既有 64 条 Java SE-only API 违规，本轮未重跑，与本次改动无关。
