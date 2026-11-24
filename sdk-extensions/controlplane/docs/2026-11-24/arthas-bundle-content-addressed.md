# Arthas 资源包内容寻址（方案 C）实施记录

## 需求

现状每个 JVM 进程都会把内嵌 Arthas 资源（`arthas-core.jar` 16MB、native lib 等）全量
重写到「每进程随机目录」，导致：

- 目录数量随 JVM 数量线性增长，路径不稳定、排查困难；
- 每个进程都要写约 17MB 到 `/tmp`（现场日志约 230ms）；
- **版本静默错配**：native lib 的幂等判断只检查「文件存在且 size>0」，若目录被复用而
  Agent 内嵌资源已升级，旧 jar / 旧 `.so` 会被继续使用且无告警；
- `extractResource()` 使用 `REPLACE_EXISTING` 无条件覆盖并注册 `deleteOnExit()`，与
  「跨进程共享」的目录语义冲突。

目标：把资源发布改为**固定根 + 内容寻址的不可变子目录**，跨进程只读复用，靠内容摘要
判定是否需要发布新版本。

## 边界与不变式

- **I1**：`<digest>` 目录一旦发布，永不修改、永不原地覆盖（保护运行中的 `URLClassLoader`
  懒加载与已 mmap 的 `.so`）。
- **I2**：完整性以「整个 staging 目录原子发布」为准，不以单文件存在为准。
- **I3**：共享 bundle 与进程私有目录生命周期隔离；shutdown hook 不得删除共享目录。
- 本阶段实施 **S2 接管 ARTHAS_HOME**；`SPY_JAR` 与 `LOGS` 保持每进程独立（S3 收敛留待后续）。
- `ArthasConfig.getLibPath()` 显式外部路径模式优先级最高，完全绕过 bundle，行为不变。
- 日志、`meta`、manifest 中不记录 token、凭据、内网地址等敏感信息。

## 方案（实际落地）

目录布局：

```
${java.io.tmpdir}/otel-arthas/                 ← 固定根（可配置，默认开启）
├── bundles/
│   ├── b-<16hex>/                            ← 内容寻址，不可变
│   │   ├── manifest.txt                     ← 每项 name=size:sha256
│   │   ├── arthas-core.jar
│   │   ├── arthas-client.jar（资源存在时）
│   │   ├── logback.xml
│   │   ├── async-profiler/<platform lib>
│   │   └── lib/<platform lib>
│   └── .staging-b-<16hex>-p<pid>-<random>/  ← 发布中目录，崩溃残留可回收
└── usage/<bundleName>                        ← 最近使用时间（写在 bundle 外，保持 I1）

SPY_JAR / LOGS → 仍走原有 arthas-spy-jar-... / arthas-logs-... 每进程私有目录
```

发布协议：

1. 构建资源清单（core jar、可选 client jar、logback 内容、当前平台 async-profiler lib
   与 JNI lib），按固定顺序对每项内容算 SHA-256 并聚合，取前 16 hex 作为 digest
2. `bundles/b-<digest>` 存在且 `validate()` 通过 → 命中，touch `usage`，直接返回
3. 未命中 → 写 `bundles/.staging-...`（全部资源 + manifest，边写边算摘要）
4. `Files.move(staging, target)`，**不带 `REPLACE_EXISTING`、不用 `ATOMIC_MOVE`**
   - 成功 → 返回
   - `IOException` 且目标已存在（不同平台可能是 `FileAlreadyExistsException` 或
     `DirectoryNotEmptyException`）→ 删 staging，对已有目录 validate 后复用
5. 任何失败 → 删除 staging → **降级到原有每进程私有目录路径**，保证 attach 不失败

校验：`manifest.txt` 记录每项 `size:sha256`；默认对小文件（≤1MB）重算 SHA-256，
16MB core jar 只比 size；`otel.controlplane.arthas.bundle.strict.verify=true` 可全量重算。
固定根校验非符号链接、设置 owner-only 权限；符号链接根直接拒绝并降级。

配置项：

| 属性 | 默认值 | 说明 |
|---|---|---|
| `otel.controlplane.arthas.bundle.enabled` | `true` | 设 `false` 完全回滚到每进程私有目录 |
| `otel.controlplane.arthas.bundle.root` | `${java.io.tmpdir}/otel-arthas` | 固定根路径 |
| `otel.controlplane.arthas.bundle.strict.verify` | `false` | 全量重算 SHA-256 |

**与最初设计的差异（如实记录）**：

- 未引入 Gradle 构建期 manifest（避免改动构建脚本与 configuration cache），改为
  **运行时计算真实 SHA-256**。命中时省去写 17MB 磁盘，但仍需从已打开的 agent jar
  读流算摘要；构建期 manifest 保留为后续可选优化。
- S1 独立「只观测不改写入」阶段未单独发版，观测能力以 INFO 级
  `[ArthasBundle] Hit shared bundle` / `Published shared bundle` 日志内建提供。
- bundle 默认开启（与残留清理的默认策略一致），通过 `enabled=false` 一键回滚。

## 实施进展

- [x] 建立本文档。
- [x] 新增 `ArthasResourceBundle`：资源清单、摘要、原子发布、校验、降级、固定根安全、清理。
- [x] 改造 `ArthasTempDirectoryManager`：`ARTHAS_HOME` 委托 bundle；共享目录与 `dirCache`
      分离，shutdown hook 不删共享目录；bundle/staging 回收接入首次扫描。
- [x] 改造 `ArthasResourceExtractor`：bundle 命中时只读复用不重写；暴露资源常量供复用。
- [x] 新增 / 调整单元测试（新增 10 个 bundle 用例；既有测试隔离 bundle 开关）。
- [x] 执行编译、单测、Javadoc、完整构建回归。
- [x] 更新本文档验收记录。

## 变更清单

| 文件 | 改动摘要 |
|---|---|
| `arthas/ArthasResourceBundle.java`（新增） | 内容寻址发布器：清单、SHA-256 摘要、staging 原子发布、manifest 校验、符号链接/权限安全、usage 记录、bundle/staging 回收、开关与降级 |
| `arthas/ArthasTempDirectoryManager.java` | `getOrCreateDir(ARTHAS_HOME)` 优先委托 bundle；新增 `sharedBundle` 字段与 `dirCache` 分离；`cleanupAll()` 明确不删共享目录；`reset()` 同步清理；`scanStaleDirsOnce()` 接入 `ArthasResourceBundle.cleanup()`；`deleteRecursively` / `latestModifiedMillis` / `lastModifiedMillis` 改为包级私有供复用；类与方法 Javadoc 同步 |
| `arthas/ArthasResourceExtractor.java` | `extractCoreJarsTo()` 增加「已发布 bundle 只读」快速路径 + `buildUrlsFromPublished()`；资源路径常量与 logback 模板改为包级私有供 bundle 复用 |
| `test/.../ArthasResourceBundleTest.java`（新增） | 10 个用例：发布/manifest、跨 reset 命中幂等、不可变（mtime 不变）、开关降级、篡改检出、符号链接拒绝、staging 无残留、`extractCoreJarsTo` 只读不重写、管理器路由共享目录且 cleanupAll 不误删、manifest 声明摘要 |
| `test/.../ArthasTempDirectoryManagerTest.java` | `@BeforeEach`/`@AfterEach` 隔离 bundle 开关并恢复系统属性，保证既有 14 个私有目录用例语义不变 |
| `docs/2026-11-24/arthas-bundle-content-addressed.md` | 本实施记录 |

未改动：`ArthasConfig`、`ArthasClassLoaderManager`、`SpyApiManager`、两个 native extractor、
proto 与 collector。`ArthasClassLoaderManager.loadFromClasspathResources()` 仍按原路径调用
`getOrCreateDir(ARTHAS_HOME)` + `extractCoreJarsTo()`，自动获得 bundle 语义，无需改动。

## 验收记录

| 项目 | 命令 | 结果 |
|---|---|---|
| 主代码 + 测试编译 | `:controlplane:compileJava :controlplane:compileTestJava` | **BUILD SUCCESSFUL**（`-Werror` + ErrorProne） |
| 编译期修复 | `[TryWithResourcesVariable]`（EP 建议的 Java 9 语法不适用于本项目 Java 8）、5 处 `[BooleanParameter]`、`[StringCaseLocaleUsage]` | 分别改为 Java 8 兼容写法 + 局部抑制、布尔参数注释、`Locale.ROOT` |
| 定向单测 | `--tests ArthasResourceBundleTest --tests ArthasTempDirectoryManagerTest` | **BUILD SUCCESSFUL**，24 个用例全通过 |
| 全量测试 + Javadoc + 发布构建 | `:controlplane:test :controlplane:javadoc publishToMavenLocal` | **BUILD SUCCESSFUL in 10s**，318 tasks |
| 全量测试汇总 | JUnit XML | **27 suites / 241 tests / 0 failures / 0 errors / 0 skipped** |
| IDE 静态诊断 | `read_lints`（3 个改动的主代码文件） | **0 问题** |
| AnimalSniffer | `:controlplane:animalSnifferMain` | **FAILED：64 条既有违规**；报告中 `ArthasResourceBundle` / `ArthasTempDirectoryManager` / `ArthasResourceExtractor` 均 **0 条**，本次未引入新违规 |

构建日志中反复出现远程 build cache 拉取失败（`develocity`），Gradle 自动禁用远程缓存，
不影响任何结果。

## 未完成任务

| # | 任务 | 状态 |
|---|------|------|
| 1 | S3：把 `SPY_JAR` 并入同一 bundle、统一两个 native extractor 的幂等逻辑 | 未做，按分阶段计划留待后续；SpyAPI 注入不可逆，需单独设计 |
| 2 | Gradle 构建期 manifest，消除运行时读 17MB 算摘要 | 未做，可选性能优化 |
| 3 | 部署新版本后观察真实 Pod 中的 `Hit shared bundle` / `Published shared bundle` 日志与命中率 | 待部署侧操作 |
| 4 | 多 JVM 并发启动的实机验证（当前由单测覆盖「跨 reset 命中」与 `Files.move` 冲突分支，未做真实多进程压测） | 待实机验证 |

## 遗留问题与风险

- **AnimalSniffer 基线失败**：实际重跑后报告 64 条违规，分布在既有文件
  （`java.lang.instrument.*`、`ManagementFactory` 等 Java SE-only API）；
  本次改动的三个文件均 0 条。该任务不在 `publishToMavenLocal` 依赖链内（完整构建成功），
  属既有基线问题，建议后续单独治理。
- **大文件默认只比 size**：16MB `arthas-core.jar` 默认不重算 SHA-256，若同大小不同内容的
  极端情况不会被检出；可用 `strict.verify=true` 全量重算，代价是每次命中多一次 16MB 哈希。
- **bundle 校验失败不自愈**：固定根下已存在但校验不通过的 bundle 不会原地修复，会降级到
  每进程私有目录并记录 WARN；需人工清理坏目录或等待清理策略回收（保留最近 2 个 + 10 分钟
  活跃保护）。
- **固定根副作用**：残留扫描开启时会调用 `resolveRoot()`，可能创建空的 `otel-arthas` 根目录，
  即使 bundle 被 `enabled=false` 关闭。属无害副作用，但会在 `/tmp` 留下一个空目录。
- **SpyAPI 不可逆**：digest 切换只对新 JVM 生效；同一 JVM 内永远使用首次注入的 SpyAPI。
- **容器 emptyDir**：Pod 重建即清空 `/tmp`，跨重启复用消失，跨进程复用仍在。
- **多用户共享 `/tmp`**：owner-only 权限可能导致其他用户进程无法读共享 bundle，此时降级到
  私有目录，功能不中断但失去复用收益。
- 历史遗留的旧格式私有目录仍由原有「每类保留 2 个」清理策略处理，与 bundle 清理互不干扰
  （固定根名 `otel-arthas` 不匹配 `isManagedDirName` 正则）。
