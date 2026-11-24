# TunnelUrlGenerator 重载参数顺序修复（-Werror 编译阻塞）

## 需求

`publishToMavenLocal` 构建在 `:sdk-extensions:controlplane:compileJava` 失败：ErrorProne
`InconsistentOverloads` 对 `TunnelUrlGenerator.resolveEffectiveEndpoint` 的两个重载各报 1 个
警告，项目开启了 `-Werror`，警告被升级为错误。

根因：新增「运营端下发 tunnel 端点」能力时，`opsTunnelEndpoint` 参数被插入到既有参数序列
中间，导致同名重载之间共有参数的相对顺序不一致。

> 构建日志中远程构建缓存条目加载失败（`develocity` 拉取失败）与本失败无关，Gradle 只是
> 自动禁用了远程缓存；真正导致 FAILED 的是 `-Werror`。

## 边界

- 只调整 `resolveEffectiveEndpoint` 重载的参数顺序与调用点，不改变端点优先级语义，不改变
  任何运行时行为。
- 保留既有 3 参数 / 2 参数便捷重载，保证向后兼容。
- 不引入新类、不引入新配置、不改 proto、不改 collector。
- 文档不记录任何 token、凭据、真实内网地址或 agentId。

## 方案

按编译器给出的建议（日志中已明确打印 `A consistent order would be`），把新增参数
`opsTunnelEndpoint` 统一追加到参数列表末尾：

| 重载 | 修改前 | 修改后 |
|---|---|---|
| 全参数入口 | `(String explicit, String ops, String baseOtlp, Integer port)` | `(String explicit, String baseOtlp, Integer port, String ops)` |
| config 便捷（带 ops） | `(ArthasConfig config, String ops, Integer port)` | `(ArthasConfig config, Integer port, String ops)` |
| 3 参数 legacy | `(String, String, Integer)` | 不变 |
| config 便捷（不带 ops） | `(ArthasConfig, Integer)` | 不变 |

配套重构（DRY）：不带 ops 的 config 便捷重载改为委托给带 ops 的 config 重载，使
`config.getExplicitTunnelEndpoint()` / `getBaseOtlpEndpoint()` 的展开逻辑只保留一处，
避免今后新增参数时漏改；其余便捷重载统一收敛到全参数入口。

参数顺序约定写入全参数入口的 Javadoc：后续新增参数一律追加到末尾，避免再次触发
`InconsistentOverloads`。

## 实施进展

- [x] 建立本文档。
- [x] 调整 `TunnelUrlGenerator` 四个重载的参数顺序与委托关系。
- [x] 同步 `ArthasIntegration` 两处调用点。
- [x] 同步 `TunnelUrlGeneratorOpsTest` 三个四参数用例。
- [x] 执行编译与单测验证（见「验收记录」）。
- [x] 更新本文档验收记录。

## 变更清单

| 文件 | 改动摘要 |
|---|---|
| `arthas/TunnelUrlGenerator.java` | 两个重载参数顺序调整；三处委托调用同步；3 参数 legacy 与 2 参数 config 重载签名不变；不带 ops 的 config 重载改为委托给带 ops 版本（消除重复的 config 展开）；Javadoc 补充参数顺序约定与 `@param` |
| `arthas/ArthasIntegration.java` | `start()` 与 `onServerMetadataChanged()` 两处调用参数顺序同步 |
| `test/.../TunnelUrlGeneratorOpsTest.java` | 三个四参数用例按新顺序传参；`legacyOverloadsRemainBackwardCompatible` 三参数用例保持不变 |
| `docs/2026-11-24/inconsistent-overloads-fix.md` | 本实施记录 |

`ArthasConfig` 的 `{@link ...(ArthasConfig, Integer)}` 与 2 参数调用不受影响，未改动。

## 验收记录

| 项目 | 命令 | 结果 |
|---|---|---|
| 主代码 + 测试代码编译（`compileJava` 含 `-Werror` + ErrorProne、`compileTestJava`） | `./gradlew :sdk-extensions:controlplane:compileJava :sdk-extensions:controlplane:compileTestJava --console=plain --no-daemon` | **BUILD SUCCESSFUL in 18s**，两个 `InconsistentOverloads` 警告已消失 |
| 单元测试 | `./gradlew :sdk-extensions:controlplane:test --tests 'io.opentelemetry.sdk.extension.controlplane.arthas.TunnelUrlGeneratorOpsTest' --console=plain --no-daemon` | **BUILD SUCCESSFUL in 12s**，4 个用例通过（优先级 3 个 + 向后兼容 1 个） |
| 原始失败命令完整回归 | `./gradlew publishToMavenLocal --console=plain --no-daemon` | **BUILD SUCCESSFUL in 13s**，321 tasks；`:sdk-extensions:controlplane:javadoc` / `javadocJar` / `publishToMavenLocal` 全部通过，说明新增 Javadoc 无语法问题 |
| IDE 静态诊断 | `read_lints` 针对 `TunnelUrlGenerator.java` | 0 error；5 个 IDE 级 warning，详见「遗留问题与风险」 |

三次构建均出现 `Could not load entry ... from remote build cache` 并提示远程缓存被禁用，
但不影响结果，属环境网络问题。

## 未完成任务

| # | 任务 | 状态 |
|---|------|------|
| 1 | 无阻塞项；本次编译阻塞已解除 | 完成 |
| 2 | 全量 `:sdk-extensions:controlplane:test`（未限定 `--tests`） | 未执行；本次按最小范围验证，若需全量回归可另行执行 |

## 遗留问题与风险

- 三个 `@Nullable String` 参数（explicit / baseOtlp / ops）仍靠位置区分语义，存在误传风险；
  根治方案是引入不可变参数对象收敛为单入口 + 具名构建器，本次为解除编译阻塞未做，
  建议后续专项重构。
- `Integer serverHttpPort` 位于参数列表中间，与末尾的 `String ops` 形成类型锚点；Java 重载
  解析会对参数顺序错误直接报编译错，可作为该约定被破坏时的第一道防线。
- IDE 诊断中 `3 参数 legacy 重载 is never used` 属 IntelliJ 单模块静态分析误报：该重载由
  `TunnelUrlGeneratorOpsTest.legacyOverloadsRemainBackwardCompatible()` 显式覆盖，是刻意保留
  的兼容 API，不删除。
- IDE 另有 `Link specified as plain text` / `HTTP links are not secure` 等 Javadoc 措辞类
  warning，均为既有内容，非本次引入；Gradle `javadoc` 任务已通过，不影响构建。
