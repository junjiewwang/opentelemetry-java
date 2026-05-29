# ControlPlane 数据分流（Routing）方案设计

## 一、架构总览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ControlPlane Server                                   │
│                                                                               │
│  下发 ExporterRoutingConfig（路由规则配置）via UnifiedPoll / GetConfig        │
└───────────────────────────────────┬───────────────────────────────────────────┘
                                    │ gRPC
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      DynamicConfigManager                                     │
│                                                                               │
│  ConfigKeys.EXPORTER_ROUTING → RoutingConfigData                             │
│  applyConfig() → routingExporter.update(routingConfigData)                   │
└───────────────────────────────────┬───────────────────────────────────────────┘
                                    │ update()
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│               RoutingSpanExporter (implements SpanExporter,                   │
│                                    HotUpdatableComponent<RoutingConfigData>)  │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐     │
│  │           RoutingEngine (策略引擎)                                    │     │
│  │  AtomicReference<List<RoutingRule>> rules                            │     │
│  │  route(SpanData) → List<String> endpoints                            │     │
│  └─────────────────────────────────────────────────────────────────────┘     │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐     │
│  │           ExporterRegistry (Exporter 注册表)                         │     │
│  │  Map<String, SpanExporter> exporters                                 │     │
│  └─────────────────────────────────────────────────────────────────────┘     │
│                                                                               │
│  export(spans):                                                               │
│    spans → RoutingEngine.dispatch(spans)                                     │
│         → Map<String, List<SpanData>>                                        │
│         → 并行 export 到各 endpoint 的 exporter                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 二、核心设计原则

| 原则 | 体现 |
|------|------|
| **单一职责 (SRP)** | RoutingEngine 只负责路由决策；ExporterRegistry 只管 exporter 生命周期；RoutingExporter 只负责编排 |
| **开闭原则 (OCP)** | 新增路由条件匹配器只需实现 `RoutingMatcher` 接口，无需修改已有代码 |
| **依赖倒置 (DIP)** | RoutingExporter 依赖抽象接口（SpanExporter, RoutingMatcher），不依赖具体实现 |
| **高内聚低耦合** | 路由逻辑、exporter 管理、配置热更新三者分离 |
| **复用已有模式** | 完全遵循 `DynamicSampler` 的 HotUpdatableComponent + AtomicReference 模式 |

---

## 三、类图设计

```
                    ┌──────────────────┐
                    │ SpanExporter     │ (SDK 接口)
                    │ (interface)      │
                    └────────┬─────────┘
                             │
                             │ implements
                             ▼
┌─────────────────────────────────────────────┐
│        RoutingSpanExporter                   │
│ implements SpanExporter,                     │
│           HotUpdatableComponent<RoutingCfg>  │
├─────────────────────────────────────────────┤
│ - routingEngine: RoutingEngine<SpanData>     │
│ - exporterRegistry: ExporterRegistry<SE>     │
│ - fallbackExporter: SpanExporter             │
├─────────────────────────────────────────────┤
│ + export(Collection<SpanData>)               │
│ + update(RoutingConfigData)                  │
│ + flush() / shutdown()                       │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│        RoutingEngine<T>                      │ ← 泛型，可复用于 Span/Metric/Log
├─────────────────────────────────────────────┤
│ - rules: AtomicReference<List<RoutingRule>>  │
├─────────────────────────────────────────────┤
│ + dispatch(Collection<T>)                    │
│   → Map<String, List<T>>                     │
│ + updateRules(List<RoutingRule>)             │
└──────────────────┬──────────────────────────┘
                   │ uses
                   ▼
┌─────────────────────────────────────────────┐
│        RoutingRule                            │
├─────────────────────────────────────────────┤
│ - name: String                               │
│ - matcher: RoutingMatcher<T>                 │
│ - targetEndpoints: List<String>              │
│ - priority: int                              │
│ - terminal: boolean (是否短路)               │
└──────────────────┬──────────────────────────┘
                   │ has-a
                   ▼
┌─────────────────────────────────────────────┐
│        RoutingMatcher<T> (interface)         │ ← 策略模式
├─────────────────────────────────────────────┤
│ + matches(T data): boolean                   │
└────────────────┬────────────────────────────┘
                 │ implementations
        ┌────────┼────────────────┐
        ▼        ▼                ▼
┌──────────┐ ┌──────────┐ ┌──────────────┐
│Attribute │ │ Resource │ │  Composite   │
│ Matcher  │ │ Matcher  │ │ Matcher(AND/ │
│          │ │          │ │  OR/NOT)     │
└──────────┘ └──────────┘ └──────────────┘
```

---

## 四、包结构设计

```
io.opentelemetry.sdk.extension.controlplane.routing
├── RoutingEngine.java              // 路由引擎（泛型）
├── RoutingRule.java                // 路由规则
├── ExporterRegistry.java           // Exporter 注册表
├── RoutingConfigData.java          // 路由配置数据接口
├── RoutingSpanExporter.java        // Trace 分流 Exporter
├── RoutingMetricExporter.java      // Metric 分流 Exporter
├── RoutingLogRecordExporter.java   // Log 分流 Exporter
└── matcher/
    ├── RoutingMatcher.java         // 匹配器接口
    ├── AttributeMatcher.java       // 属性匹配
    ├── ResourceMatcher.java        // 资源属性匹配
    ├── SpanKindMatcher.java        // SpanKind 匹配
    ├── SeverityMatcher.java        // Log 严重级别匹配
    ├── InstrumentScopeMatcher.java // Instrument Scope 匹配
    └── CompositeMatcher.java       // 组合匹配（AND/OR/NOT）
```

---

## 五、核心接口与类设计

### 5.1 RoutingMatcher（策略接口）

```java
/**
 * 路由匹配器 - 判断遥测数据是否匹配路由规则
 * @param <T> 遥测数据类型（SpanData / MetricData / LogRecordData）
 */
@FunctionalInterface
public interface RoutingMatcher<T> {
    boolean matches(T data);
    
    // 组合操作的默认方法
    default RoutingMatcher<T> and(RoutingMatcher<T> other) {
        return data -> this.matches(data) && other.matches(data);
    }
    
    default RoutingMatcher<T> or(RoutingMatcher<T> other) {
        return data -> this.matches(data) || other.matches(data);
    }
    
    default RoutingMatcher<T> negate() {
        return data -> !this.matches(data);
    }
    
    /** 匹配所有数据 */
    static <T> RoutingMatcher<T> alwaysMatch() { return data -> true; }
}
```

### 5.2 RoutingRule

```java
/**
 * 路由规则 - 包含匹配条件和目标 endpoint
 */
public final class RoutingRule<T> {
    private final String name;
    private final RoutingMatcher<T> matcher;
    private final List<String> targetEndpoints;  // 匹配后发送到哪些 endpoint
    private final int priority;                  // 优先级（越小越优先）
    private final boolean terminal;              // 是否短路（匹配后不再继续匹配后续规则）
    
    // Builder pattern ...
}
```

### 5.3 RoutingEngine（核心引擎，泛型复用）

```java
/**
 * 路由引擎 - 根据规则将遥测数据分发到不同 endpoint
 * 
 * 线程安全：使用 AtomicReference 实现无锁规则热替换
 */
public final class RoutingEngine<T> {
    private final AtomicReference<List<RoutingRule<T>>> rulesRef;
    private final String fallbackEndpoint;
    
    /**
     * 将数据分发到对应的 endpoint
     * @return endpoint → 数据列表的映射
     */
    public Map<String, List<T>> dispatch(Collection<T> data) {
        List<RoutingRule<T>> rules = rulesRef.get();
        Map<String, List<T>> result = new HashMap<>();
        
        for (T item : data) {
            List<String> targets = resolveTargets(item, rules);
            for (String target : targets) {
                result.computeIfAbsent(target, k -> new ArrayList<>()).add(item);
            }
        }
        return result;
    }
    
    private List<String> resolveTargets(T item, List<RoutingRule<T>> rules) {
        List<String> targets = new ArrayList<>();
        for (RoutingRule<T> rule : rules) {
            if (rule.getMatcher().matches(item)) {
                targets.addAll(rule.getTargetEndpoints());
                if (rule.isTerminal()) break;
            }
        }
        // 没有任何规则匹配时走 fallback
        return targets.isEmpty() ? List.of(fallbackEndpoint) : targets;
    }
    
    /** 原子更新规则 */
    public void updateRules(List<RoutingRule<T>> newRules) {
        // 按 priority 排序后替换
        List<RoutingRule<T>> sorted = newRules.stream()
            .sorted(Comparator.comparingInt(RoutingRule::getPriority))
            .collect(Collectors.toUnmodifiableList());
        rulesRef.set(sorted);
    }
}
```

### 5.4 RoutingSpanExporter

```java
/**
 * 路由 SpanExporter - 根据路由规则将 span 分发到不同 exporter
 * 
 * 设计模式：Proxy + Strategy + Observer
 * - Proxy: 代理原始 exporter
 * - Strategy: 可替换的路由策略
 * - Observer: 通过 HotUpdatableComponent 监听配置变更
 */
public final class RoutingSpanExporter implements SpanExporter, 
        DynamicConfigManager.HotUpdatableComponent<RoutingConfigData> {
    
    private static final Logger logger = Logger.getLogger(RoutingSpanExporter.class.getName());
    
    private final RoutingEngine<SpanData> routingEngine;
    private final ExporterRegistry<SpanExporter> exporterRegistry;
    private final SpanExporter fallbackExporter;  // 原始 exporter，作为 fallback
    
    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
        if (spans.isEmpty()) return CompletableResultCode.ofSuccess();
        
        Map<String, List<SpanData>> dispatched = routingEngine.dispatch(spans);
        List<CompletableResultCode> results = new ArrayList<>(dispatched.size());
        
        for (Map.Entry<String, List<SpanData>> entry : dispatched.entrySet()) {
            SpanExporter exporter = exporterRegistry.get(entry.getKey());
            if (exporter == null) {
                exporter = fallbackExporter;
            }
            try {
                results.add(exporter.export(entry.getValue()));
            } catch (RuntimeException e) {
                logger.log(Level.WARNING, 
                    "Export to endpoint [" + entry.getKey() + "] failed", e);
                results.add(CompletableResultCode.ofFailure());
            }
        }
        return CompletableResultCode.ofAll(results);
    }
    
    @Override
    public void update(RoutingConfigData config) {
        // 1. 更新路由规则
        List<RoutingRule<SpanData>> rules = RoutingRuleFactory.createSpanRules(config);
        routingEngine.updateRules(rules);
        
        // 2. 更新 exporter 注册表（如有 endpoint 变更）
        exporterRegistry.reconcile(config.getEndpoints());
        
        logger.info("Routing rules updated: " + rules.size() + " rules active");
    }
    
    @Override
    public CompletableResultCode flush() {
        return CompletableResultCode.ofAll(exporterRegistry.flushAll());
    }
    
    @Override
    public CompletableResultCode shutdown() {
        return CompletableResultCode.ofAll(exporterRegistry.shutdownAll());
    }
}
```

---

## 六、Proto 配置模型扩展

在 `config.proto` 的 `AgentConfig` 中新增字段：

```protobuf
message AgentConfig {
    // ... existing fields ...
    
    // 路由配置
    ExporterRoutingConfig exporter_routing = 7;
}

message ExporterRoutingConfig {
    repeated EndpointConfig endpoints = 1;
    repeated RoutingRuleConfig trace_rules = 2;
    repeated RoutingRuleConfig metric_rules = 3;
    repeated RoutingRuleConfig log_rules = 4;
}

message EndpointConfig {
    string name = 1;           // endpoint 唯一标识
    string url = 2;            // OTLP endpoint URL
    string protocol = 3;       // grpc / http
    map<string, string> headers = 4;
    CompressionType compression = 5;
}

message RoutingRuleConfig {
    string name = 1;
    int32 priority = 2;
    bool terminal = 3;         // 匹配后是否终止后续规则
    repeated string target_endpoints = 4;
    RoutingCondition condition = 5;
}

message RoutingCondition {
    oneof condition_type {
        AttributeCondition attribute = 1;
        ResourceCondition resource = 2;
        SpanKindCondition span_kind = 3;
        SeverityCondition severity = 4;
        InstrumentScopeCondition scope = 5;
        CompositeCondition composite = 6;
    }
}

message AttributeCondition {
    string key = 1;
    MatchOperator operator = 2;
    string value = 3;
}

message ResourceCondition {
    string key = 1;
    MatchOperator operator = 2;
    string value = 3;
}

message SpanKindCondition {
    repeated string kinds = 1;  // SERVER, CLIENT, INTERNAL, PRODUCER, CONSUMER
}

message SeverityCondition {
    string min_severity = 1;    // TRACE, DEBUG, INFO, WARN, ERROR, FATAL
}

message InstrumentScopeCondition {
    string name_pattern = 1;    // 支持通配符
}

message CompositeCondition {
    enum LogicOperator {
        AND = 0;
        OR = 1;
        NOT = 2;
    }
    LogicOperator operator = 1;
    repeated RoutingCondition conditions = 2;
}

enum MatchOperator {
    EQUALS = 0;
    NOT_EQUALS = 1;
    CONTAINS = 2;
    STARTS_WITH = 3;
    ENDS_WITH = 4;
    REGEX = 5;
    EXISTS = 6;
}
```

---

## 七、注册集成点

在 `ControlPlaneAutoConfigurationProvider` 中新增注册：

```java
@Override
public void customize(AutoConfigurationCustomizer autoConfiguration) {
    // ... existing customizers ...
    
    // Trace 路由
    autoConfiguration.addSpanExporterCustomizer((exporter, config) -> {
        RoutingSpanExporter routingExporter = new RoutingSpanExporter(exporter);
        configManager.registerComponent(ConfigKeys.EXPORTER_ROUTING, routingExporter);
        return routingExporter;
    });
    
    // Metric 路由
    autoConfiguration.addMetricExporterCustomizer((exporter, config) -> {
        RoutingMetricExporter routingExporter = new RoutingMetricExporter(exporter);
        configManager.registerComponent(ConfigKeys.METRIC_ROUTING, routingExporter);
        return routingExporter;
    });
    
    // Log 路由
    autoConfiguration.addLogRecordExporterCustomizer((exporter, config) -> {
        RoutingLogRecordExporter routingExporter = new RoutingLogRecordExporter(exporter);
        configManager.registerComponent(ConfigKeys.LOG_ROUTING, routingExporter);
        return routingExporter;
    });
}
```

---

## 八、泛型复用设计

三种信号（Trace/Metric/Log）的路由逻辑高度相似，通过泛型 + 模板方法避免重复：

```java
/**
 * 抽象路由 Exporter 基类 - 提取公共逻辑
 */
abstract class AbstractRoutingExporter<T, E> implements DynamicConfigManager.HotUpdatableComponent<RoutingConfigData> {
    
    protected final RoutingEngine<T> routingEngine;
    protected final ExporterRegistry<E> exporterRegistry;
    protected final E fallbackExporter;
    
    /** 子类实现：执行实际的 export 调用 */
    protected abstract CompletableResultCode doExport(E exporter, List<T> data);
    
    /** 子类实现：从配置创建路由规则 */
    protected abstract List<RoutingRule<T>> createRules(RoutingConfigData config);
    
    protected CompletableResultCode routeAndExport(Collection<T> data) {
        if (data.isEmpty()) return CompletableResultCode.ofSuccess();
        
        Map<String, List<T>> dispatched = routingEngine.dispatch(data);
        List<CompletableResultCode> results = new ArrayList<>(dispatched.size());
        
        for (Map.Entry<String, List<T>> entry : dispatched.entrySet()) {
            E exporter = exporterRegistry.getOrDefault(entry.getKey(), fallbackExporter);
            try {
                results.add(doExport(exporter, entry.getValue()));
            } catch (RuntimeException e) {
                logger.log(Level.WARNING, "Export failed for endpoint: " + entry.getKey(), e);
                results.add(CompletableResultCode.ofFailure());
            }
        }
        return CompletableResultCode.ofAll(results);
    }
    
    @Override
    public void update(RoutingConfigData config) {
        routingEngine.updateRules(createRules(config));
        exporterRegistry.reconcile(config.getEndpoints());
    }
}
```

---

## 九、典型使用场景示例

### 场景 1：按 service.name 分流 Trace 到不同后端

```json
{
  "endpoints": [
    {"name": "primary", "url": "https://primary-backend:4317", "protocol": "grpc"},
    {"name": "secondary", "url": "https://secondary-backend:4318", "protocol": "http"}
  ],
  "trace_rules": [
    {
      "name": "critical-services-to-primary",
      "priority": 1,
      "terminal": true,
      "target_endpoints": ["primary"],
      "condition": {
        "resource": {"key": "service.name", "operator": "REGEX", "value": "payment.*|order.*"}
      }
    },
    {
      "name": "all-others-to-secondary",
      "priority": 100,
      "terminal": true,
      "target_endpoints": ["secondary"],
      "condition": {"attribute": {"key": "", "operator": "EXISTS", "value": ""}}
    }
  ]
}
```

### 场景 2：ERROR 级别 Log 双写（高优 + 普通通道）

```json
{
  "log_rules": [
    {
      "name": "error-logs-dual-write",
      "priority": 1,
      "terminal": true,
      "target_endpoints": ["primary", "alert-channel"],
      "condition": {"severity": {"min_severity": "ERROR"}}
    },
    {
      "name": "normal-logs",
      "priority": 50,
      "terminal": true,
      "target_endpoints": ["primary"],
      "condition": {}
    }
  ]
}
```

---

## 十、健壮性设计

| 场景 | 处理策略 |
|------|---------|
| endpoint 不可达 | 单个 endpoint 失败不影响其他；返回 partial failure |
| 规则配置为空 | 所有数据走 fallbackExporter（原始 exporter） |
| 规则匹配无命中 | 走 fallbackEndpoint |
| 热更新期间的请求 | AtomicReference 保证读写不竞争，无锁切换 |
| endpoint 新增/移除 | ExporterRegistry.reconcile() 安全增删 |
| exporter 创建失败 | 记录日志，该 endpoint 临时降级为 noop |
| 恶意规则（如 regex 灾难回溯） | RoutingMatcher 包装超时保护 |

---

## 十一、实施计划

| Sprint | 内容 | 产出 |
|--------|------|------|
| **Sprint 1** | 核心路由引擎 + RoutingSpanExporter + 静态规则配置 | 可通过 extension_config_json 下发规则实现 Trace 分流 |
| **Sprint 2** | Proto 扩展 + RoutingMetricExporter + RoutingLogRecordExporter | 三种信号全部支持分流 |
| **Sprint 3** | ExporterRegistry 动态创建 exporter + endpoint 热增删 | 完整的动态路由能力 |

---

## 十二、实施进展

- [ ] Sprint 1：核心路由引擎 + RoutingSpanExporter
- [ ] Sprint 2：Proto 扩展 + Metric/Log Routing Exporter
- [ ] Sprint 3：动态 Endpoint 管理

## 十三、遗留问题

1. ExporterRegistry 动态创建 exporter 时需要哪些参数（TLS、认证等），需与 ControlPlane Server 协商协议
2. 是否需要支持路由规则的灰度生效（如按百分比逐步切换）
3. Metric 信号的路由粒度：按 instrument 还是按 data point
