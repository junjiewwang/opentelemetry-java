# Arthas 临时目录复用优化

## 背景

每次 Arthas attach/detach 周期都会在 `/tmp` 下创建新的临时目录（`arthas-{random}`、`arthas-spy-{random}`、`arthas-logs-{random}`），旧的不会在 detach 时清理，仅依赖 `deleteOnExit` 在 JVM 退出时清理。导致长期运行的 JVM 中堆积大量临时目录。

## 根因

1. `ArthasResourceExtractor` 是纯静态工具类，每次调用 `createTempDirectory()` 生成新随机目录
2. `ArthasClassLoaderManager` destroy 时重置 ClassLoader 引用，丢失旧 temp 目录路径
3. 缺少统一的临时目录生命周期管理者

## 方案

引入 `ArthasTempDirectoryManager`（JVM 级 Singleton）统一管理 Arthas 运行时临时目录的创建、缓存、复用和清理。

## 涉及文件

| 文件 | 变更类型 | 说明 |
|------|----------|------|
| `ArthasTempDirectoryManager.java` | 新增 | 临时目录统一管理者 |
| `ArthasResourceExtractor.java` | 修改 | 增加 `targetPath` 参数重载 |
| `ArthasClassLoaderManager.java` | 修改 | 使用 TempDirectoryManager 获取路径 |
| `SpyApiManager.java` | 修改 | 使用 TempDirectoryManager 获取 spy jar 路径 |
| `ArthasLogIsolation.java` | 修改 | 删除自身 tempDir 缓存，委托给 TempDirectoryManager |

## 实施进展

- [x] 创建 ArthasTempDirectoryManager
- [x] 修改 ArthasResourceExtractor（新增 extractCoreJarsTo、修改 extractSpyJar）
- [x] 修改 ArthasClassLoaderManager（使用 TempDirectoryManager 获取路径）
- [x] 修改 ArthasLogIsolation（删除自身 tempDir 缓存，委托给 TempDirectoryManager）
- [x] SpyApiManager（无需修改，extractSpyJar 已内部委托）

## 待完成

- 无

## 变更总结

| 文件 | 变更量 | 说明 |
|------|--------|------|
| `ArthasTempDirectoryManager.java` | +230 行 | 新增：临时目录统一管理者，JVM 级 Singleton |
| `ArthasResourceExtractor.java` | +20 / -15 行 | 新增 `extractCoreJarsTo(Path)`；`extractCoreJars()` / `extractSpyJar()` 委托给 TempDirManager |
| `ArthasClassLoaderManager.java` | +5 / -1 行 | `loadFromClasspathResources()` 通过 TempDirManager 获取可复用目录 |
| `ArthasLogIsolation.java` | +10 / -30 行 | 删除 `tempLogDir` 字段和自行管理逻辑，委托给 TempDirManager |
| `SpyApiManager.java` | 0 行 | 无需修改，`extractSpyJar(config)` 已内部委托 |

## 设计原则验证

- **SRP**: TempDirectoryManager 只管理临时目录；ResourceExtractor 只提取文件
- **OCP**: 新增资源类型只需加 DirType 枚举
- **DIP**: 调用方依赖 DirType 抽象，不依赖路径生成策略
- **高内聚低耦合**: 3 种目录管理收敛在一个类中，通过 getInstance() 暴露
