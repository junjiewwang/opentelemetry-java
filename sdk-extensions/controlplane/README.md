# Control Plane Extension Module

This is an extension module for OpenTelemetry Java SDK that provides control plane functionality.

## Features

- **Dynamic Configuration**: Hot-update sampling rates, batch settings, and resource attributes
- **Remote Task Execution**: Execute diagnostic tasks (thread dump, heap info, etc.) remotely
- **Status Reporting**: Report agent status, JVM metrics, and task results
- **OTLP Health Monitoring**: Link control plane connection with OTLP export health

## Installation

### Option 1: Add to existing OpenTelemetry Java build

Add the following line to `settings.gradle.kts`:

```kotlin
include(":sdk-extensions:controlplane")
```

### Option 2: Build as standalone JAR

```bash
cd sdk-extensions/controlplane
../../gradlew build
```

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `otel.agent.control.enabled` | `false` | Enable control plane |
| `otel.exporter.otlp.endpoint` | `http://localhost:4317` | OTLP endpoint (reused) |
| `otel.exporter.otlp.protocol` | `grpc` | Protocol: `grpc` or `http/protobuf` |
| `otel.agent.control.http.base.path` | `/v1/control` | HTTP base path |
| `otel.agent.control.config.poll.interval` | `30s` | Config poll interval |
| `otel.agent.control.task.poll.interval` | `10s` | Task poll interval |
| `otel.agent.control.status.report.interval` | `60s` | Status report interval |
| `otel.agent.control.task.result.compression.threshold` | `1KB` | Compression threshold |
| `otel.agent.control.task.result.chunked.threshold` | `50MB` | Chunked upload threshold |
| `otel.agent.control.task.result.max.size` | `200MB` | Max result size |

## Usage

Once enabled, the extension automatically:

1. Wraps the sampler with `DynamicSampler` for hot updates
2. Monitors OTLP export health status
3. Starts config/task polling and status reporting
4. Links control plane connection with OTLP health

### Programmatic Access

```java
// Get the control plane manager
ControlPlaneManager manager = ControlPlaneAutoConfigurationProvider.getControlPlaneManager();

// Get the dynamic sampler
DynamicSampler sampler = ControlPlaneAutoConfigurationProvider.getDynamicSampler();
sampler.updateRatio(0.5); // Update sampling rate

// Get health monitor
OtlpHealthMonitor monitor = ControlPlaneAutoConfigurationProvider.getHealthMonitor();
System.out.println("OTLP Health: " + monitor.getState());
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Control Plane Server                      │
│  ┌─────────────────────────────────────────────────────────┐│
│  │   HTTP/gRPC Server (same port as OTLP Collector)        ││
│  │   ├─ GET /v1/control/config  (long polling)             ││
│  │   ├─ GET /v1/control/tasks   (long polling)             ││
│  │   └─ POST /v1/control/status                            ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │ Protobuf (HTTP/gRPC)
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Agent Extension                           │
│  ┌───────────────────────────────────────────────────────┐  │
│  │             ControlPlaneManager                        │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────────────────┐   │  │
│  │  │ConfigPoll│ │TaskPoll  │ │StatusReporter        │   │  │
│  │  └──────────┘ └──────────┘ └──────────────────────┘   │  │
│  │          │          │                │                │  │
│  │          └──────────┼────────────────┘                │  │
│  │                     ▼                                 │  │
│  │  ┌──────────────────────────────────────────────────┐ │  │
│  │  │         OtlpHealthMonitor (联动)                  │ │  │
│  │  └──────────────────────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Proto Files

Proto definitions are located in `src/main/proto/controlplane/v1/`:

- `common.proto` - Common messages (AgentIdentity, ConfigVersion, etc.)
- `config.proto` - Configuration messages
- `task.proto` - Task messages
- `status.proto` - Status reporting messages
- `service.proto` - gRPC service definition

## Development

### Build

```bash
./gradlew :sdk-extensions:controlplane:build
```

### Test

```bash
./gradlew :sdk-extensions:controlplane:test
```
