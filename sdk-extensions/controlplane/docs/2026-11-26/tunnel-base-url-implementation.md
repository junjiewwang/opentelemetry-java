# Arthas Tunnel Endpoint 推导改造 — Java 探针侧实施记录

## 需求与决策

按 `Arthas Tunnel Endpoint 推导改造` 指引实施，分两个阶段：阶段 1 引入 `ops.arthas.tunnel_base_url` 部分覆盖并修正端口推导；阶段 2 在用户确认前置条件后移除 `ops.arthas.tunnel_endpoint`，只保留单一运营端覆盖入口。改动全部落在 SDK 仓库 `sdk-extensions/controlplane`。

最终决策：

- 优先级链收敛为 3 层：`local -D`（`otel.agent.arthas.tunnel.endpoint`）> `ops.arthas.tunnel_base_url` > 由控制面 endpoint 推导。来源枚举仍为 `local / ops / derived / none`，`source=ops` 由 base_url 支撑。
- 默认推导仅在原 URL 显式带 port 时才应用服务端下发的 `http_port`；域名 / Ingress 场景由 scheme 决定默认 80/443，不注入监听端口。
- 默认推导继续丢弃原 path；path 前缀只由 base_url 覆盖分支承载，其后固定拼 `/v1/arthas/ws`。
- `derived` 来源上报不变；**用户明确放弃「心跳里带上 tunnel 地址用于检查校验」这一需求**，`getOpsStatusAttributes()` 保持现状（仅 `arthas.tunnel.source` 与 `ops.revision.applied`），不加任何新字段。
- 不考虑兼容性；`ops.arthas.tunnel_endpoint` 整条链路删除（key、字段、优先级层、重载、测试、文档）。

用户确认的三项前置条件（2026-11-26）：网关 path 前缀之后固定跟 `/v1/arthas/ws`，不存在「路径前缀本身即 ws 终点」或目标路径非固定值的情况；网关路由不需要额外 query 参数；collector 侧已完成改造。因此 `generateFromBaseUrl` 的强制拼接与 query/fragment/userinfo 拒绝规则均不需要放宽。

评审阶段对指引的三处修正：指引 5.3 要求「不要修改 `resolveEndpointSource`」会让 base_url 命中时误报 `derived`，实施改为识别 ops 覆盖；指引 5.2(c) 建议改重载为 `(ArthasConfig, Integer, ServerMetadata)` 会重蹈 `docs/2026-11-24/inconsistent-overloads-fix.md` 记载的 ErrorProne `InconsistentOverloads` + `-Werror` 故障，实施改为「不可变值对象 + 单一私有核心实现 + 新参数只追加到末尾」；指引未定义 base_url 分支是否应用 `serverHttpPort`，实施明确「完全不碰端口」。

安全边界：base_url 由服务端下发且探针主动外连，校验 scheme 白名单（http/https/ws/wss）、拒绝 userinfo / 空 host / query / fragment / 固定 tunnel path；无效值回落到 derived 并记录不含 URL 值的日志。不按 IP 段拦截，因为单测口径把 `10.0.0.1:4318` 这类直连地址视为合法部署。移除完整 endpoint 后，所有运营端下发都经过校验，不再存在「无校验、原样透传」的入口。

## 实施进展

- [x] 需求、现状与评审结论取证
- [x] 记录文档落盘
- [x] 阶段 1：`OpsTunnelOverride` / `ServerMetadata` / `TunnelUrlGenerator` / `ArthasIntegration` 改动与单测
- [x] 阶段 2：移除 `ops.arthas.tunnel_endpoint` 全链路
- [x] README 优先级链与端口描述修正
- [x] 全量回归验证与验收记录回填

## 变更清单（最终状态）

| 文件 | 改动 |
| --- | --- |
| `core/model/ServerMetadata.java` | 仅保留 `http_port` / `ops.arthas.tunnel_base_url` / `ops.revision` 三个 key；字段 `opsTunnelBaseUrl`、`normalize` 解析与 `getOpsTunnelBaseUrl()`（因 ErrorProne `IdentifierName` + `-Werror`，按编译器建议用 `BaseUrl` 而非 `BaseURL`） |
| `core/model/OpsTunnelOverride.java`（新增） | 不可变单字段值对象（baseUrl）；`empty()` / `of(String)` / `from(ServerMetadata)` / `hasBaseUrl()`；`toString()` 只输出字段存在性，不打印值 |
| `arthas/TunnelUrlGenerator.java` | 新增 `generateFromBaseUrl`（scheme 白名单、保留 host[:port] 与 path 前缀、拒绝非法输入）、`toWebSocketScheme` / `withTunnelPath` / `hasExplicitPort` helper；`generateDefaultEndpoint` 端口替换加显式端口条件；保留 3 个重载（3 参 String legacy、2 参 config、3 参 config + `OpsTunnelOverride`）并统一委托私有 `resolveCore`；日志不再打印完整 URL |
| `arthas/ArthasIntegration.java` | 用单个 `volatile OpsTunnelOverride opsOverride` 承载运营端快照（整体替换，避免中间态）；`start()` / `onServerMetadataChanged()` 传快照；`resolveEndpointSource()` 用 `hasBaseUrl()` 保证 `source=ops`；早退逻辑与 `getOpsStatusAttributes()` 不变 |
| `README.md` | 优先级链改为 3 层，补充 base_url 配置项，修正「端口可被 http_port 覆盖」为「仅显式端口才替换」 |
| `test/.../ServerMetadataOpsTest.java` | 3 个用例：base_url 解析、空值归一、已移除 endpoint key 被当未知 key 忽略 |
| `test/.../OpsTunnelOverrideTest.java`（新增） | 7 个用例：空值归一、metadata 投影、删除回落、旧 key 忽略、未知 key 兼容、toString 脱敏 |
| `test/.../TunnelUrlGeneratorOpsTest.java` | 4 个用例：local 最高、ops base_url 胜过 derived、缺失回落、legacy 重载兼容 |
| `test/.../TunnelUrlGeneratorDerivationTest.java`（新增） | 17 个用例：derived 四类端口场景、默认丢弃 path、非正端口忽略、base_url 的 scheme / path / port / 结尾斜杠、3 层优先级、删除回落、9 类非法 base_url 回落、旧完整 endpoint 形态拒绝、legacy 与 2 参 config 重载兼容 |
| `docs/2026-11-26/tunnel-base-url-implementation.md`（新增） | 本记录 |

## 未完成任务

- 发布链：`publishToMavenLocal` 覆盖 `io.opentelemetry:opentelemetry-sdk-extension-controlplane:1.57.0`（同版本覆盖建议 `--refresh-dependencies` 或升版本），再回 instrumentation 仓库重建 javaagent；只改 SDK 不重建 javaagent 不生效。
- 端到端验证：minikube 下 http/protobuf 直连与 Ingress 域名、grpc 4317 三类启动；运营端下发/删除 base_url 后重连与回落；心跳 `arthas.tunnel.source` 四类统计。
- 运营端确认：collector 已改造，需确认运营端配置不再写入 `ops.arthas.tunnel_endpoint`，避免残留配置静默失效（探针会当未知 key 忽略）。

## 遗留问题

- 指引引用的 `instrumentation/docs/2026-11-26/controlplane-auth-and-token-bridge.md`、`tunnel-endpoint-derivation-implementation.md` 与 collector 仓库路径在本机不存在，collector 侧 validator / `ops_metadata.go` 断言未独立核实。
- `resolveEndpointSource()` 的 `source=ops` 由 `OpsTunnelOverride.hasBaseUrl()` 保证；`ArthasIntegration` 构造依赖 lifecycle/scheduler，未加直接单测，靠端到端验证口径覆盖。
- 端到端验证前需确认部署侧没有残留 `-Dotel.agent.arthas.tunnel.endpoint`（`docs/2026-11-24/tunnel-endpoint-autoconfig.md:51` 记录过待注入），否则 source 恒为 `local`。
- 指引文档 D1 与第 7 节「完整 URL 逃逸阀」描述已被本轮改动废弃；该文档不在本机，需在指引侧同步。

## 回滚方式

改动集中在 `sdk-extensions/controlplane` 的 4 个主文件、4 个测试与文档，可用 git 回退该批提交。运行期删除 `ops.arthas.tunnel_base_url` 自动回落 derived；应急时仍可用 local `-D` 显式指定完整 WS URL（需改 JVM 参数并重启探针）。

## 验收记录

- 阶段 1 首次编译失败（已修复）：ErrorProne `IdentifierName` 对 `opsTunnelBaseURL` / `getOpsTunnelBaseURL()` 告警，`-Werror` 升级为错误；按编译器建议改名 `opsTunnelBaseUrl` / `getOpsTunnelBaseUrl()` 后通过。
- 阶段 2 全量构建与测试：`./gradlew :sdk-extensions:controlplane:test --console=plain` → `BUILD SUCCESSFUL in 15s`，`81 actionable tasks: 10 executed, 71 up-to-date`；`-Werror` 下 `compileJava` 通过，无 `InconsistentOverloads` 告警。
- 测试报告：`build/test-results/test` 下 30 个 `TEST-*.xml`，检索 `failures="[1-9]` / `errors="[1-9]` 命中 0；本轮相关测试类 `TunnelUrlGeneratorDerivationTest` 17 用例、`TunnelUrlGeneratorOpsTest` 4 用例均 `failures=0 errors=0`；`OpsTunnelOverrideTest`、`ServerMetadataOpsTest` 及既有回归全部通过。
- 残留检查：全模块检索 `hasAny()` / `getOpsTunnelEndpoint` / `opsTunnelEndpoint` / `KEY_OPS_TUNNEL_ENDPOINT` / 双参 `OpsTunnelOverride.of(...)` 命中 0（仅剩无关的 `ControlPlaneConfig.getEndpoint()`）。
- 静态检查：改动文件 IDE 诊断 0 条。
- 安全：URL 不写入日志，`OpsTunnelOverride.toString()` 不含值；无效 base_url 只记录固定告警文本并回落 derived；心跳属性保持低基数枚举 + revision，未新增任何地址字段。

验收状态：DONE（源码与单测层面）；发布链与线上端到端验证仍为未完成任务。
