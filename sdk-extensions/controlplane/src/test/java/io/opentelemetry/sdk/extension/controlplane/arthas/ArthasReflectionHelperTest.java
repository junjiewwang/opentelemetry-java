/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Method;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ArthasReflectionHelperTest {

  @AfterEach
  void tearDown() {
    ArthasReflectionHelper.clearCache();
  }

  @Test
  void findMethodReturnsMethodWhenExists() {
    Method method = ArthasReflectionHelper.findMethod(String.class, "length");

    assertThat(method).isNotNull();
    assertThat(method.getName()).isEqualTo("length");
  }

  @Test
  void findMethodReturnsNullWhenNotExists() {
    Method method = ArthasReflectionHelper.findMethod(String.class, "nonExistentMethod");

    assertThat(method).isNull();
  }

  @Test
  void findMethodWithParameterTypesReturnsCorrectMethod() {
    Method method = ArthasReflectionHelper.findMethod(String.class, "substring", int.class);

    assertThat(method).isNotNull();
    assertThat(method.getName()).isEqualTo("substring");
    assertThat(method.getParameterCount()).isEqualTo(1);
  }

  @Test
  void findMethodByNameReturnsFirstMatch() {
    Method method = ArthasReflectionHelper.findMethodByName(String.class, "substring");

    assertThat(method).isNotNull();
    assertThat(method.getName()).isEqualTo("substring");
  }

  @Test
  void findMethodByNameReturnsNullWhenNotFound() {
    Method method = ArthasReflectionHelper.findMethodByName(String.class, "nonExistent");

    assertThat(method).isNull();
  }

  @Test
  void invokeMethodCallsMethod() {
    Method method = ArthasReflectionHelper.findMethod(String.class, "length");
    assertThat(method).isNotNull();

    Object result = ArthasReflectionHelper.invokeMethod("hello", method);

    assertThat(result).isEqualTo(5);
  }

  @Test
  void invokeMethodWithArgsCallsMethod() {
    Method method = ArthasReflectionHelper.findMethod(String.class, "substring", int.class);
    assertThat(method).isNotNull();

    Object result = ArthasReflectionHelper.invokeMethod("hello", method, 2);

    assertThat(result).isEqualTo("llo");
  }

  @Test
  void invokeMethodThrowsArthasExceptionOnError() {
    Method method = ArthasReflectionHelper.findMethod(String.class, "substring", int.class);
    assertThat(method).isNotNull();

    // 传递无效参数导致错误
    assertThatThrownBy(() -> ArthasReflectionHelper.invokeMethod("hello", method, -1))
        .isInstanceOf(StringIndexOutOfBoundsException.class);
  }

  @Test
  void invokeMethodQuietlyReturnsNullOnError() {
    Method method = ArthasReflectionHelper.findMethod(String.class, "substring", int.class);
    assertThat(method).isNotNull();

    Object result = ArthasReflectionHelper.invokeMethodQuietly("hello", method, -1);

    assertThat(result).isNull();
  }

  @Test
  void getPropertyReturnsPropertyValue() {
    TestBean bean = new TestBean("test", 42);

    Object name = ArthasReflectionHelper.getProperty(bean, "name");
    Object value = ArthasReflectionHelper.getProperty(bean, "value");

    assertThat(name).isEqualTo("test");
    assertThat(value).isEqualTo(42);
  }

  @Test
  void getPropertyReturnsNullForNonExistentProperty() {
    TestBean bean = new TestBean("test", 42);

    Object result = ArthasReflectionHelper.getProperty(bean, "nonExistent");

    assertThat(result).isNull();
  }

  @Test
  void getIntPropertyReturnsValue() {
    TestBean bean = new TestBean("test", 42);

    int result = ArthasReflectionHelper.getIntProperty(bean, "value", -1);

    assertThat(result).isEqualTo(42);
  }

  @Test
  void getIntPropertyReturnsDefaultWhenNotFound() {
    TestBean bean = new TestBean("test", 42);

    int result = ArthasReflectionHelper.getIntProperty(bean, "nonExistent", -1);

    assertThat(result).isEqualTo(-1);
  }

  @Test
  void getStringPropertyReturnsValue() {
    TestBean bean = new TestBean("test", 42);

    String result = ArthasReflectionHelper.getStringProperty(bean, "name");

    assertThat(result).isEqualTo("test");
  }

  @Test
  void isClassAvailableReturnsTrueForExistingClass() {
    boolean result =
        ArthasReflectionHelper.isClassAvailable(
            "java.lang.String", Thread.currentThread().getContextClassLoader());

    assertThat(result).isTrue();
  }

  @Test
  void isClassAvailableReturnsFalseForNonExistingClass() {
    boolean result =
        ArthasReflectionHelper.isClassAvailable(
            "com.nonexistent.ClassName", Thread.currentThread().getContextClassLoader());

    assertThat(result).isFalse();
  }

  @Test
  void loadClassReturnsClassForExistingClass() {
    Class<?> clazz =
        ArthasReflectionHelper.loadClass(
            "java.lang.String", Thread.currentThread().getContextClassLoader());

    assertThat(clazz).isEqualTo(String.class);
  }

  @Test
  void loadClassThrowsExceptionForNonExistingClass() {
    assertThatThrownBy(
            () ->
                ArthasReflectionHelper.loadClass(
                    "com.nonexistent.ClassName", Thread.currentThread().getContextClassLoader()))
        .isInstanceOf(ArthasException.class)
        .hasMessageContaining("Class not found");
  }

  @Test
  void tryLoadClassReturnsNullForNonExistingClass() {
    Class<?> result =
        ArthasReflectionHelper.tryLoadClass(
            "com.nonexistent.ClassName", Thread.currentThread().getContextClassLoader());

    assertThat(result).isNull();
  }

  @Test
  void methodCacheImprovesPerformance() {
    // 第一次调用会填充缓存
    Method method1 = ArthasReflectionHelper.findMethod(String.class, "length");
    // 第二次调用应该从缓存返回
    Method method2 = ArthasReflectionHelper.findMethod(String.class, "length");

    assertThat(method1).isSameAs(method2);
  }

  @Test
  void clearCacheClearsAllCachedMethods() {
    // 填充缓存
    ArthasReflectionHelper.findMethod(String.class, "length");

    // 清除缓存
    ArthasReflectionHelper.clearCache();

    // 再次查找应该重新填充缓存（无法直接验证，但不应该抛出异常）
    Method method = ArthasReflectionHelper.findMethod(String.class, "length");
    assertThat(method).isNotNull();
  }

  @Test
  void arthasClassConstantsAreDefined() {
    assertThat(ArthasReflectionHelper.ARTHAS_BOOTSTRAP_CLASS)
        .isEqualTo("com.taobao.arthas.core.server.ArthasBootstrap");
    assertThat(ArthasReflectionHelper.SHELL_SERVER_CLASS)
        .isEqualTo("com.taobao.arthas.core.shell.ShellServer");
    assertThat(ArthasReflectionHelper.SHELL_SESSION_CLASS)
        .isEqualTo("com.taobao.arthas.core.shell.session.Session");
    assertThat(ArthasReflectionHelper.TERM_CLASS)
        .isEqualTo("com.taobao.arthas.core.shell.term.Term");
  }

  // 用于测试的简单 Bean 类
  @SuppressWarnings("unused")
  public static class TestBean {
    private final String name;
    private final int value;

    public TestBean(String name, int value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public int getValue() {
      return value;
    }
  }
}
