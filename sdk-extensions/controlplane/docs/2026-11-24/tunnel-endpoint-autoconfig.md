# Tunnel 端点自动装配修复记录

## 需求

Java Agent 的 Arthas Tunnel 注册地址无法显式指定：`otel.agent.arthas.tunnel.endpoint` 已定义但自动装配路径从未调用 `ArthasConfig.create(ConfigProperties)`，实际地址只能由 `ControlPlaneConfig.getEndpoint()` 推导，导致 Arthas TunnelClient 不注册到 collector。

目标地址：`ws://custom-collector.devcloud/v1/arthas/ws`（经 OpenResty → minikube Ingress → agent_gateway 4318）。

## 边界

- 只改 Java Agent 仓库的自动装配、配置读取与文档，不改 collector 协议、不改控制面下发的 proto。
- 保留现有开关语义：`otel.agent.control.arthas.enabled`（默认 true）继续作为自动装配总开关；`otel.agent.arthas.enabled` 作为显式覆盖。
- 显式 endpoint 优先于推导地址；未配置时行为保持不变（仍由 control/OTLP endpoint 推导）。
- 文档不记录任何 token 或凭据值。

## 实施进展

- [x] 建立本文档。
- [x] 补接线并保留开关语义。
- [x] 补单测（代码完成，执行受环境影响，见「未完成」）。
- [x] 验证 appName 鉴权与目标 URL 链路。
- [x] 补齐 README 配置说明。
- [x] 更新本文档。

## 变更清单

| 文件 | 改动摘要 |
|---|---|
| `arthas/ArthasConfig.java` | 新增 `create(ConfigProperties, boolean defaultEnabled)` 与 `Builder.fromConfigProperties(ConfigProperties, boolean)`；单参版本行为不变 |
| `spi/ControlPlaneAutoConfigurationProvider.java` | 自动装配改用 `ArthasConfig.create(config, controlConfig.isArthasEnabled())` + `setArthasConfig(...)`，替换原来只构造空配置的 `enableArthas()`；日志标明是否使用显式端点 |
| `arthas/ArthasConfigTest.java`（测试） | 新增 4 个用例：默认开关、显式关闭优先、显式 tunnel endpoint 生效、单参 `create` 默认禁用 |
| `README.md` | 新增 Arthas Tunnel 配置章节：地址优先级、配置键、示例、appName 鉴权约定、命令链路不依赖 Tunnel |
| `docs/2026-11-24/tunnel-endpoint-autoconfig.md` | 本实施记录 |

`ControlPlaneManager.Builder.enableArthas()` 保留为 public 兼容 API，自动装配不再调用它。

## 验收记录

| 项目 | 结果 |
|---|---|
| 代码静态诊断 | 修改的 Java 文件 IDE 诊断 0 问题 |
| 目标链路（只读模拟） | 经 OpenResty/Ingress，用合法 app token 作为 `appName` 请求 `/v1/arthas/ws?method=agentRegister` → **101**；无 token 且无 appName → **401** |
| 配置语义复核 | 未设置 `otel.agent.arthas.enabled` 时 enabled 继承控制面开关（默认 true）；显式 `false` 覆盖；显式 endpoint 不再被推导地址覆盖 |
| 单测执行 | **未完成**（见下） |

## 未完成任务（环境/部署侧）

| # | 任务 | 状态 |
|---|------|------|
| 1 | 运行 `:sdk-extensions:controlplane:test --tests ArthasConfigTest` | 阻塞：Gradle `buildSrc` 依赖 `slf4j-api:2.0.12`、`commons-validator:1.10.0` 从 Maven Central 下载返回 429，本地缓存无对应版本，`--offline` 亦失败。非代码错误，需网络恢复后重试 |
| 2 | 在运行中的 Java Agent JVM 参数上增加 `-Dotel.agent.arthas.tunnel.endpoint=ws://custom-collector.devcloud/v1/arthas/ws` | 待部署侧操作：本机 Docker/K8s 中未定位到该 Agent 实例，无法直接注入 |
| 3 | 重启 Agent 并确认 collector 日志出现 `WebSocket connection established` / `method=agentRegister` | 依赖 #2 |

## 遗留问题与风险

- `custom-collector.devcloud` 的解析依赖客户端 DNS/hosts；Agent 侧需能解析到 OpenResty 所在主机。
- WS 鉴权依赖 `appName` query 等于合法 app token 的隐式约定；`otel.agent.control.token` 不合法时仍会静默 401。
- 控制面下发 `server_metadata.http_port` 只在未显式配置 endpoint 时参与推导；使用显式配置后不再覆盖端口。
- `ArthasIntegration.start()` 只注入地址，Arthas 仍由 `arthas_attach` 任务懒启动；只有执行过 attach/命令后 TunnelClient 才会连接。
- 旧 `config-template-render/config/tcs/service_provider.yaml` 中的 `tunnel_server` 段无人引用（指向不存在的旧服务），本次未改动，建议后续清理。
- collector 侧 WS 401 目前几乎不留日志，静态排查困难；建议后续加 warn（不含 token）。
