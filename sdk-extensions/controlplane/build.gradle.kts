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

  // JSON 序列化
  implementation("com.fasterxml.jackson.core:jackson-databind")

  // ByteBuddy Agent（可选依赖，用于自动获取 Instrumentation）
  // 使用 compileOnly 是因为：
  // 1. InstrumentationHolder 使用反射调用，编译时不需要
  // 2. 在 opentelemetry-java-instrumentation 中，ByteBuddy 已存在
  // 3. 如果 classpath 中没有此依赖，会优雅降级（需要手动设置 Instrumentation）
  compileOnly("net.bytebuddy:byte-buddy-agent:1.14.18")

  // gRPC 传输使用 OkHttp gRPC sender（不再依赖 grpc-java）

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

// Disable grpc-java stub generation for this module. We use OkHttp-based gRPC transport and
// don't need grpc-java types on the classpath.
protobuf {
  generateProtoTasks {
    all().configureEach {
      // Do not generate grpc-java stubs for this module.
      // Kotlin DSL note: the plugins container is a NamedDomainObjectContainer.
      // We remove the grpc plugin by name if it was added by conventions.
      plugins.removeIf { it.name == "grpc" }
    }
  }
}
