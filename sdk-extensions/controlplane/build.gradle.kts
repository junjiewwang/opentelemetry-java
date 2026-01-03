plugins {
  id("otel.protobuf-conventions")
  id("otel.publish-conventions")
  id("otel.animalsniffer-conventions")
  id("com.squareup.wire")
}

description = "OpenTelemetry SDK Extension - Control Plane"
otelJava.moduleName.set("io.opentelemetry.sdk.extension.controlplane")

dependencies {
  // OpenTelemetry SDK 依赖
  api(project(":sdk:all"))
  api(project(":sdk-extensions:autoconfigure-spi"))

  // 编译时依赖
  compileOnly(project(":api:incubator"))
  compileOnly(project(":sdk-extensions:autoconfigure"))
  compileOnly(project(":sdk-extensions:incubator"))

  // HTTP 客户端
  implementation(project(":exporters:common"))
  implementation(project(":exporters:sender:okhttp"))
  implementation("com.squareup.okhttp3:okhttp")

  // JSON 序列化 (用于 Arthas Tunnel 协议)
  implementation("com.fasterxml.jackson.core:jackson-databind")

  // ByteBuddy Agent（可选依赖，用于自动获取 Instrumentation）
  // 使用 compileOnly 是因为：
  // 1. InstrumentationHolder 使用反射调用，编译时不需要
  // 2. 在 opentelemetry-java-instrumentation 中，ByteBuddy 已存在
  // 3. 如果 classpath 中没有此依赖，会优雅降级（需要手动设置 Instrumentation）
  compileOnly("net.bytebuddy:byte-buddy-agent:1.14.18")

  // gRPC 支持（可选）
  compileOnly("io.grpc:grpc-api")
  compileOnly("io.grpc:grpc-protobuf")
  compileOnly("io.grpc:grpc-stub")

  // Protobuf
  implementation("com.google.protobuf:protobuf-java")

  // 测试依赖
  testImplementation(project(":sdk:testing"))
  testImplementation(project(":sdk-extensions:autoconfigure"))
  testImplementation("com.google.guava:guava")
  testImplementation("com.linecorp.armeria:armeria-junit5")
  testImplementation("com.linecorp.armeria:armeria-grpc-protocol")
}

wire {
  custom {
    schemaHandlerFactoryClass = "io.opentelemetry.gradle.ProtoFieldsWireHandlerFactory"
  }
}

tasks {
  compileJava {
    with(options) {
      // Generated code, do not control serialization
      compilerArgs.add("-Xlint:-serial")
    }
  }

  // ===== Custom Control Plane Extension: Skip checkstyle for custom extension module =====
  checkstyleMain {
    enabled = false
  }
  checkstyleTest {
    enabled = false
  }
  // ===== Custom Control Plane Extension: End =====
}
