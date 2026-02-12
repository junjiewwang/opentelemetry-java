# Async-Profiler 任务下发指导文档

## 一、概述

本文档用于指导服务端下发 async-profiler 采样任务时的参数配置。Agent 端收到任务后，由 `AsyncProfilerProfileExecutor` 解析参数并执行采样。

### 核心设计

`EventType` 枚举封装了不同事件类型的 `interval` 参数语义差异，**不同事件的 `interval` 含义、单位和默认值各不相同**：

| 事件 | `interval` 含义 | 单位 | 默认值 | 合法范围 |
|------|----------------|------|--------|---------|
| `cpu` | 采样时间间隔 | 纳秒（ns） | 10,000,000（10ms） | 1,000,000 ~ 1,000,000,000（1ms ~ 1s） |
| `wall` | 采样时间间隔 | 纳秒（ns） | 10,000,000（10ms） | 1,000,000 ~ 1,000,000,000（1ms ~ 1s） |
| `alloc` | 内存分配字节阈值 | 字节（bytes） | 524,288（512KB） | 4,096 ~ 104,857,600（4KB ~ 100MB） |
| `lock` | 锁等待时间阈值 | 纳秒（ns） | 10,000,000（10ms） | 1,000 ~ 10,000,000,000（1μs ~ 10s） |

> ⚠️ **重要变更**：`interval_ns` 参数已更名为 `interval`，因为 alloc 事件的单位是字节而非纳秒。

---

## 二、任务参数说明

### 完整参数列表

| 参数名 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| `event` | string | 否 | `"cpu"` | 事件类型，支持 `cpu`、`wall`、`alloc`、`lock` |
| `interval` | long | 否 | 由事件类型决定 | 采样间隔/阈值（见上表），如未指定则使用事件类型的默认值 |
| `duration_ms` | long | 否 | `30000` | 采样持续时长（毫秒），范围 1,000 ~ 120,000 |
| `format` | string | 否 | `"collapsed"` | 输出格式，支持 `collapsed`（文本）和 `jfr`（二进制） |
| `threads` | boolean | 否 | `false` | 是否按线程分组 |

### 参数校验规则

1. **event** — 必须是 `[cpu, wall, alloc, lock]` 之一（不区分大小写），否则返回 `INVALID_PARAMETERS`
2. **interval** — 根据事件类型有不同的合法范围（见上表），超出范围返回类似：
   > `interval for event 'alloc' must be between 4096 and 104857600 bytes (allocation size threshold in bytes), got 10000000000`
3. **duration_ms** — 范围 `[1000, 120000]`
4. **format** — 必须是 `collapsed` 或 `jfr`

---

## 三、各事件类型任务参数示例

### 1. CPU 采样

最常用的场景，分析 CPU 热点方法。

**最简参数**（全部使用默认值）：
```json
{
  "event": "cpu"
}
```

**推荐参数**：
```json
{
  "event": "cpu",
  "interval": 10000000,
  "duration_ms": 30000,
  "format": "collapsed",
  "threads": true
}
```

| 参数 | 值 | 含义 |
|------|-----|------|
| `interval` | `10000000` | 每 **10ms** 采样一次 CPU 堆栈 |
| `duration_ms` | `30000` | 采样持续 30 秒 |

**高频采样场景**（需要更细粒度）：
```json
{
  "event": "cpu",
  "interval": 1000000,
  "duration_ms": 10000,
  "format": "jfr",
  "threads": true
}
```
> `interval=1000000` 表示每 1ms 采样一次，数据量更大但精度更高，建议 duration 不要太长。

---

### 2. Wall-Clock 采样

分析包含等待/阻塞时间的全景视图，适合排查 I/O 瓶颈、锁等待、sleep 等问题。

**推荐参数**：
```json
{
  "event": "wall",
  "interval": 10000000,
  "duration_ms": 30000,
  "format": "collapsed",
  "threads": true
}
```

| 参数 | 值 | 含义 |
|------|-----|------|
| `interval` | `10000000` | 每 **10ms** 采样一次全量线程堆栈 |
| `threads` | `true` | **强烈建议开启**，wall 模式按线程分组更有意义 |

> 💡 **CPU vs Wall 的区别**：CPU 只在线程 **运行时** 采样；Wall 在线程 **等待/睡眠** 时也采样。

---

### 3. 内存分配采样（Alloc）

分析内存分配热点，排查内存抖动和 GC 压力问题。

**推荐参数**：
```json
{
  "event": "alloc",
  "interval": 524288,
  "duration_ms": 20000,
  "format": "collapsed",
  "threads": true
}
```

| 参数 | 值 | 含义 |
|------|-----|------|
| `interval` | `524288` | 每分配 **512KB** 记录一次堆栈（⚠️ **单位是字节，不是纳秒**） |
| `duration_ms` | `20000` | 采样持续 20 秒 |

**低分配压力场景**（应用分配量较小时）：
```json
{
  "event": "alloc",
  "interval": 131072,
  "duration_ms": 30000,
  "format": "collapsed",
  "threads": true
}
```
> `interval=131072` 表示每分配 128KB 记录一次，能捕获更多分配事件。

**高分配压力场景**（应用分配量很大，避免过多采样影响性能）：
```json
{
  "event": "alloc",
  "interval": 4194304,
  "duration_ms": 20000,
  "format": "collapsed",
  "threads": true
}
```
> `interval=4194304` 表示每分配 4MB 记录一次。

> ⚠️ **历史教训**：旧版本所有事件统一使用 `interval_ns=10000000`，对于 alloc 事件相当于设置了 ~10MB 的阈值，导致短时间内无分配事件被捕获，输出文件为空（`OUTPUT_FILE_ERROR`）。现在默认值为 512KB，不再有此问题。

---

### 4. 锁竞争采样（Lock）

分析锁竞争热点，排查线程阻塞和并发性能问题。

**推荐参数**：
```json
{
  "event": "lock",
  "interval": 10000000,
  "duration_ms": 30000,
  "format": "collapsed",
  "threads": true
}
```

| 参数 | 值 | 含义 |
|------|-----|------|
| `interval` | `10000000` | 锁等待超过 **10ms** 时记录堆栈 |

**细粒度锁竞争分析**：
```json
{
  "event": "lock",
  "interval": 1000000,
  "duration_ms": 30000,
  "format": "collapsed",
  "threads": true
}
```
> `interval=1000000` 表示锁等待超过 1ms 就记录。

**只关注严重锁竞争**：
```json
{
  "event": "lock",
  "interval": 100000000,
  "duration_ms": 60000,
  "format": "collapsed",
  "threads": true
}
```
> `interval=100000000` 表示只记录锁等待超过 100ms 的场景。

---

## 四、输出格式说明

### Collapsed 格式

```json
{ "format": "collapsed" }
```

- 文本格式，每行一个堆栈 + 计数
- 适合直接生成火焰图
- Content-Type: `text/plain; charset=utf-8`
- 文件扩展名: `.collapsed`
- **async-profiler 命令**：start 时不指定 file，stop 时指定 `stop,collapsed,file=...`

### JFR 格式

```json
{ "format": "jfr" }
```

- 二进制格式，信息更丰富（包含时间戳、线程信息等）
- 可用 JDK Mission Control (JMC) 分析
- Content-Type: `application/x-jfr`
- 文件扩展名: `.jfr`
- **async-profiler 命令**：start 时指定 `start,jfr,file=...`

---

## 五、执行流程

```
服务端下发任务 (type=async-profiler, params={event, interval, ...})
    │
    ▼
Agent 接收任务 (TaskDispatcher)
    │
    ▼
ProfileRequest.fromContext() 解析参数
    │
    ├── event 无效? ──→ ❌ INVALID_PARAMETERS: Unsupported event type
    │
    ▼
    interval 是否指定?
    ├── 已指定 ──→ 使用指定值
    └── 未指定 ──→ 使用 EventType 默认值
    │
    ▼
ProfileRequest.validate() 参数校验
    │
    ├── interval 超出范围? ──→ ❌ INVALID_PARAMETERS: 详细错误信息
    ├── duration 超出范围? ──→ ❌ INVALID_PARAMETERS
    ├── format 不支持?    ──→ ❌ INVALID_PARAMETERS
    │
    ▼
提取 native library (AsyncProfilerResourceExtractor)
    │
    ▼
执行 profiling (AsyncProfilerRunner.profile())
    │  start 命令示例:
    │    CPU:   start,event=cpu,interval=10000000,threads,file=/tmp/xxx.jfr
    │    Alloc: start,event=alloc,interval=524288,threads
    │
    ▼
等待 duration_ms
    │
    ▼
stop profiling
    │
    ▼
验证输出文件
    ├── 文件不存在/为空? ──→ ❌ OUTPUT_FILE_ERROR
    │
    ▼
流式上传到服务端
    │
    ▼
清理本地文件
    │
    ▼
✅ 返回 SUCCESS
```

---

## 六、错误码速查

| 错误码 | 触发条件 | 建议处理 |
|--------|---------|---------|
| `INVALID_PARAMETERS` | event 不支持 / interval 超出范围 / duration 超出范围 / format 不支持 | 检查下发参数 |
| `PROFILER_BUSY` | 已有采样任务在进行中 | 等待当前任务完成后重试 |
| `OUTPUT_FILE_ERROR` | 输出文件不存在或为空 | 检查 interval 设置是否合理（alloc 事件尤其注意） |
| `PROFILER_EXEC_FAILED` | async-profiler 执行返回错误 | 查看 Agent 日志获取详细错误 |
| `PROFILER_INTERRUPTED` | 采样过程被中断 | 重试 |
| `UNSUPPORTED_PLATFORM` | 不支持的操作系统/架构 | 确认运行环境 |
| `UPLOAD_FAILED` | 上传失败 | 检查网络连接，文件保留在本地可手动获取 |

---

## 七、快速参考卡片

### 最简参数（只指定 event，其他全部使用默认值）

```json
// CPU（默认 interval=10ms, duration=30s, collapsed, 不按线程分组）
{ "event": "cpu" }

// Wall
{ "event": "wall" }

// Alloc（默认 interval=512KB, duration=30s）
{ "event": "alloc" }

// Lock（默认 interval=10ms, duration=30s）
{ "event": "lock" }
```

### 生产推荐参数

```json
// CPU 火焰图
{ "event": "cpu", "interval": 10000000, "duration_ms": 30000, "format": "collapsed", "threads": true }

// Wall-clock 分析
{ "event": "wall", "interval": 10000000, "duration_ms": 30000, "format": "collapsed", "threads": true }

// 内存分配热点
{ "event": "alloc", "interval": 524288, "duration_ms": 20000, "format": "collapsed", "threads": true }

// 锁竞争分析
{ "event": "lock", "interval": 10000000, "duration_ms": 30000, "format": "collapsed", "threads": true }
```

### 上报元数据字段（uploadMetadata）

Agent 上报结果时携带以下元数据：

| 字段 | 示例值 | 说明 |
|------|--------|------|
| `event` | `"cpu"` | 事件类型 |
| `format` | `"collapsed"` | 输出格式 |
| `duration_ms` | `"30000"` | 请求的采样时长 |
| `interval` | `"10000000"` | 实际使用的 interval 值 |
| `interval_unit` | `"ns"` / `"bytes"` | interval 的单位 |
| `threads` | `"true"` | 是否按线程分组 |
| `file_size` | `"102400"` | 输出文件大小（字节） |
| `actual_duration_ms` | `"30123"` | 实际采样耗时 |

---

## 八、相关代码文件

| 文件 | 职责 |
|------|------|
| `EventType.java` | 事件类型枚举，封装 interval 语义、默认值、范围和校验 |
| `ProfileRequest.java` | 采样请求参数，参数解析和校验逻辑 |
| `DirectAsyncProfilerRunner.java` | 直接调用 AsyncProfiler Java API 执行采样 |
| `AsyncProfilerProfileExecutor.java` | 任务执行器，串联参数解析→采样→上传完整流程 |
| `ProfilerResult.java` | 采样结果封装 |
| `AsyncProfilerRunner.java` | 采样运行器接口 |
