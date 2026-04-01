/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URL;
import java.net.URLClassLoader;
import org.junit.jupiter.api.Test;

class ArthasStructuredCommandBridgeTest {

  @Test
  void loadRequiredClassFallsBackToShadedFastjson2Class() throws Exception {
    URL arthasCoreJar =
        ArthasStructuredCommandBridgeTest.class.getResource("/arthas/arthas-core.jar");
    assertThat(arthasCoreJar).isNotNull();

    try (@SuppressWarnings("BanClassLoader")
        URLClassLoader loader = new URLClassLoader(new URL[] {arthasCoreJar}, null)) {
      Class<?> jsonClass =
          ArthasStructuredCommandBridge.loadRequiredClass(
              loader,
              "fastjson2 JSON",
              "com.alibaba.fastjson2.JSON",
              "com.alibaba.arthas.deps.com.alibaba.fastjson2.JSON");

      assertThat(jsonClass.getName())
          .isEqualTo("com.alibaba.arthas.deps.com.alibaba.fastjson2.JSON");
    }
  }

  @Test
  void loadRequiredClassIncludesComponentNameAndCandidatesWhenMissing() {
    ClassLoader emptyLoader =
        new ClassLoader(null) {
          @Override
          protected Class<?> findClass(String name) throws ClassNotFoundException {
            throw new ClassNotFoundException(name);
          }
        };

    assertThatThrownBy(
            () ->
                ArthasStructuredCommandBridge.loadRequiredClass(
                    emptyLoader,
                    "fastjson2 JSON",
                    "com.alibaba.fastjson2.JSON",
                    "com.alibaba.arthas.deps.com.alibaba.fastjson2.JSON"))
        .isInstanceOf(ClassNotFoundException.class)
        .hasMessageContaining("fastjson2 JSON 类不存在")
        .hasMessageContaining("com.alibaba.fastjson2.JSON")
        .hasMessageContaining("com.alibaba.arthas.deps.com.alibaba.fastjson2.JSON");
  }
}
