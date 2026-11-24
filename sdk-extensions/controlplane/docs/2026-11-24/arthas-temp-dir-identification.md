# Arthas 临时目录可辨识化与残留治理

## 需求

运行环境中 `/tmp` 下出现多组 Arthas 临时目录，目录名后缀仅为 JDK 生成的随机数，
无法区分创建时间、所属进程与用途：

```
arthas-1383043882854248061          arthas-4709386943673953350
arthas-logs-1879688763010489576     arthas-logs-6921006666689707971
arthas-spy-jar-3633232488185242177 arthas-spy-jar-3772652186945132152
```

其中 `arthas-` 是 ARTHAS_HOME（核心 JAR/native lib），`arthas-spy-jar-` 是 SpyAPI JAR，
`arthas-logs-` 才是日志目录。目标是让目录名自带可辨识信息，并治理历史残留。

根因（已由代码确认）：

- `Files.createTempDirectory(prefix)` 的后缀是 JDK 生成的随机数，不含时间/PID/业务语义。
- `cleanupAll()` 在全模块内没有任何生产调用点，仅在类 Javadoc 示例中出现。
- `deleteOnExit()` 只能删除空目录；目录内含 JAR、日志文件与子目录时必然残留。
- 每个 JVM 持有独立单例缓存，进程重启或非正常退出（kill -9 / OOMKill / Pod 重建）后
  缓存丢失，旧目录无人回收。

## 边界

- 只改 `ArthasTempDirectoryManager` 与其单测，不改资源提取、日志隔离、生命周期等调用方。
- 不引入配置中心依赖；清理开关与数量上限使用 JVM 系统属性。
- 清理只处理识别为本组件创建、且前缀结构匹配的目录，不触碰 `/tmp` 下其他目录。
- 文档与 `.meta` 文件不记录 token、凭据、内网地址等敏感信息。

## 方案

1. **可辨识目录名**：把「时间戳 + PID」编码进前缀，随机数仍由 JDK 追加用于唯一性兜底。
   形如 `arthas-logs-20261124-164530-p12345-<random>`，一眼可读出类型、创建时间、进程。
2. **`.meta` 标记文件**：每个目录写入 `.arthas-dir.meta`，含 `owner`、`dir.type`、`pid`、
   `created.at`，便于事后离线对账。
3. **shutdown hook**：单例构造时注册 `otel-arthas-tmp-cleanup`，调用 `cleanupAll()` 递归删除
   本进程创建的目录；`deleteOnExit()` 保留作空目录兜底。`cleanupAll()` 改为 `synchronized`。
4. **残留清理（默认开启，按数量）**：首次创建目录时扫描 `java.io.tmpdir` 一级子目录，
   按目录类型分组，每组按「目录内最新文件 mtime」倒序，**每类最多保留最近 2 个**，
   其余判定为残留并删除。
   - `otel.controlplane.arthas.tmp.cleanup.enabled`（默认 `true`；设 `false` 可完全关闭回滚）
   - `otel.controlplane.arthas.tmp.cleanup.max.keep.per.type`（默认 `2`）
   - 安全护栏：跳过本进程缓存目录；目录内最近一次文件修改在 **10 分钟** 内的一律视为
     活跃并跳过（默认开启清理后必须防止误删其他 JVM 进程正在写日志的目录）；
     mtime 不可判定者同样保守保留。

**「最大保留 2 个」的语义**：按目录类型各自保留 2 个，而非全局 2 个。一次正常运行需要
ARTHAS_HOME / SPY_JAR / LOGS 三类各 1 个，全局 2 个会与正常运行冲突。

复用说明：PID 复用 `AgentIdentityProvider.getProcessId()`（已处理 Java 9+ / 反射回退与
animalsniffer 兼容），不重复实现探测逻辑；失败降级 `unknown`，单次创建只解析一次，
命名与 meta 共用同一值。新旧目录名由单一正则 `isManagedDirName()` 识别；
`dirTypeOf()` 按最长前缀优先归类（`arthas-` 是另两个前缀的公共前缀）。
`selectDirsToRemove()` 抽为纯函数，集中数量策略与安全护栏，便于单测且不依赖真实 `/tmp`。

## 实施进展

- [x] 建立本文档。
- [x] 改造 `ArthasTempDirectoryManager`：命名、meta、hook、幂等清理。
- [x] 初版残留清理（默认关闭 / 按天数），并按需求调整为**默认开启 / 每类保留 2 个**。
- [x] 新增单元测试（14 个用例）。
- [x] 执行编译、单测、Javadoc 与完整构建回归。
- [x] 更新本文档验收记录。

## 变更清单

| 文件 | 改动摘要 |
|---|---|
| `arthas/ArthasTempDirectoryManager.java` | ① 目录前缀加入 `yyyyMMdd-HHmmss` + `-p<pid>-`；② 写入 `.arthas-dir.meta`；③ 注册 shutdown hook 递归清理；④ `cleanupAll()` 加 `synchronized`；⑤ 残留清理默认开启、按类型分组、每类保留最近 2 个、10 分钟活跃保护；⑥ `isManagedDirName()` / `dirTypeOf()` / `selectDirsToRemove()` 抽出复用与可测逻辑；⑦ `reset()` 重置扫描标记；⑧ 枚举与类 Javadoc 同步 |
| `test/.../ArthasTempDirectoryManagerTest.java` | 14 个用例：命名格式、meta 内容、缓存复用、类型隔离、幂等清理、失效重建、新旧目录名识别、无关目录排除、meta 缺失不阻塞、前缀分组、保留 2 个、活跃保护、mtime 不可判定、未超限不删除 |
| `docs/2026-11-24/arthas-temp-dir-identification.md` | 本实施记录 |

未改动：`ArthasResourceExtractor`、`ArthasLogIsolation`、`ArthasClassLoaderManager` 等调用方，
以及 `DirType` 的三个前缀常量。

## 验收记录

| 项目 | 命令 | 结果 |
|---|---|---|
| 主代码 + 测试编译 | `:controlplane:compileJava :controlplane:compileTestJava` | **BUILD SUCCESSFUL**（`-Werror` + ErrorProne） |
| 定向单测（策略调整后） | `--tests ArthasTempDirectoryManagerTest` | **BUILD SUCCESSFUL in 7s**，14 个用例全通过 |
| 全量模块测试 + Javadoc + 完整构建 | `:controlplane:test :controlplane:javadoc publishToMavenLocal` | **BUILD SUCCESSFUL in 15s**，327 tasks |
| 全量测试结果汇总 | JUnit XML | **26 suites / 231 tests / 0 failures / 0 errors / 0 skipped** |
| IDE 静态诊断 | `read_lints` | 曾有 1 条 `getCachedDir is never used` 警告，已在测试中复用该方法做断言消除（**该断言未单独复跑**，见「未完成」） |
| AnimalSniffer | `:controlplane:animalSnifferMain` | FAILED：65 条既有违规，见「遗留问题与风险」 |

编译期曾两次被拦：`[InlineFormatString]`（单次使用的格式串被抽常量）已内联；
`[InconsistentOverloads]` 属上一需求，另见同目录 `inconsistent-overloads-fix.md`。

构建日志中反复出现远程 build cache 拉取失败（`develocity`），Gradle 自动禁用远程缓存，
不影响任何结果。

## 未完成任务

| # | 任务 | 状态 |
|---|------|------|
| 1 | 单独复跑一次 `ArthasTempDirectoryManagerTest`，确认最后新增的 `getCachedDir` 断言 | 待执行；命令被跳过。该断言与全量回归中已通过的 231 个用例同批代码，为同包 getter 读取，风险为零 |
| 2 | 部署新版本后观察真实 Pod 中的 `[ArthasTempDir] Created new dir` 与 `Removed stale dir` 日志 | 待部署侧操作 |
| 3 | 确认现场 6 个旧格式目录按「每类保留 2 个」的实际清理结果符合预期 | 依赖 #2 |

## 遗留问题与风险

- **默认开启删除是有破坏性的操作**：活跃保护窗口为 10 分钟，若其他 JVM 进程的目录超过
  10 分钟没有任何文件写入（例如 ARTHAS_HOME 内 JAR 提取完就不再变化），且同类目录超过 2 个，
  仍有被删的可能。如需绝对安全，设置
  `-Dotel.controlplane.arthas.tmp.cleanup.enabled=false` 关闭清理。
- **AnimalSniffer 基线失败**：65 条违规分布在 18 个既有文件，全部是
  `java.lang.instrument.Instrumentation`、`java.lang.management.ManagementFactory` 等
  Java SE-only API（Android signature 不含）。报告中 `ArthasTempDirectoryManager` 出现 **0 次**，
  本次改动未引入新违规；该任务不在 `publishToMavenLocal` 依赖链内（故完整构建成功）。
- PID 在容器中可能重复或跨 namespace 不可见，仅作辨识标签，不作为存活判据。
- 历史旧格式目录（无 `.meta`）依赖前缀 + 结构匹配识别；只匹配纯数字后缀或新格式，
  不会误伤 `arthas-logs-backup` 之类的无关目录。
- 时间戳使用 JVM 本地时区（`Locale.ROOT` 格式化），跨时区对账需注意；`created.at` 用
  `Instant.now()`（UTC、ISO-8601）可作权威时间参考。
