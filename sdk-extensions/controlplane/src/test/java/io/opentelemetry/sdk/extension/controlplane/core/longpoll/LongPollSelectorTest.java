/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Tests for {@link LongPollSelector}. */
class LongPollSelectorTest {

  /** 测试用的简单 Handler 实现 */
  @SuppressWarnings("UnusedVariable")
  private static class TestHandler<R> implements LongPollHandler<R> {
    private final LongPollType type;
    private final AtomicReference<R> resultRef = new AtomicReference<>();
    private final AtomicReference<Throwable> errorRef = new AtomicReference<>();
    private final AtomicBoolean shouldContinue = new AtomicBoolean(true);
    private final Runnable onResponse;
    private volatile String currentTaskId = "";

    TestHandler(LongPollType type) {
      this(type, () -> {});
    }

    TestHandler(LongPollType type, Runnable onResponse) {
      this.type = type;
      this.onResponse = onResponse;
    }

    @Override
    public LongPollType getType() {
      return type;
    }

    @Override
    public Map<String, Object> buildRequestParams() {
      return new HashMap<>();
    }

    @Override
    public CompletableFuture<R> poll() {
      // 测试用，返回一个空的 Future（实际使用时通过 register 传入）
      return new CompletableFuture<>();
    }

    @Override
    public HandlerResult handleResponse(R response) {
      resultRef.set(response);
      onResponse.run();
      return HandlerResult.changed("test");
    }

    @Override
    public void handleError(Throwable error) {
      errorRef.set(error);
    }

    @Override
    public boolean shouldContinue() {
      return shouldContinue.get();
    }

    @Override
    public void setCurrentTaskId(String taskId) {
      this.currentTaskId = taskId;
    }

    R getResult() {
      return resultRef.get();
    }

    Throwable getError() {
      return errorRef.get();
    }
  }

  @Test
  void testRegisterAndSelectSingleChannel() {
    LongPollSelector selector = new LongPollSelector();
    CompletableFuture<String> future = CompletableFuture.completedFuture("test-result");
    TestHandler<String> handler = new TestHandler<>(LongPollType.CONFIG);

    selector.register(handler, future);

    LongPollSelector.SelectResult selectResult = selector.selectAll(1000);

    assertThat(selectResult.getSuccessCount()).isEqualTo(1);
    assertThat(selectResult.getFailureCount()).isEqualTo(0);
    assertThat(selectResult.isAllSuccess()).isTrue();
    assertThat(handler.getResult()).isEqualTo("test-result");
  }

  @Test
  void testRegisterMultipleChannels() {
    LongPollSelector selector = new LongPollSelector();
    CompletableFuture<String> configFuture = CompletableFuture.completedFuture("config");
    CompletableFuture<String> taskFuture = CompletableFuture.completedFuture("task");

    TestHandler<String> configHandler = new TestHandler<>(LongPollType.CONFIG);
    TestHandler<String> taskHandler = new TestHandler<>(LongPollType.TASK);

    selector
        .register(configHandler, configFuture)
        .register(taskHandler, taskFuture);

    LongPollSelector.SelectResult result = selector.selectAll(1000);

    assertThat(result.getSuccessCount()).isEqualTo(2);
    assertThat(result.isAllSuccess()).isTrue();
    assertThat(configHandler.getResult()).isEqualTo("config");
    assertThat(taskHandler.getResult()).isEqualTo("task");
  }

  @Test
  void testSelectWithDelayedResponse() throws InterruptedException {
    LongPollSelector selector = new LongPollSelector();
    CompletableFuture<String> delayedFuture = new CompletableFuture<>();
    TestHandler<String> handler = new TestHandler<>(LongPollType.CONFIG);

    selector.register(handler, delayedFuture);

    // 延迟 100ms 后完成
    new Thread(
            () -> {
              try {
                Thread.sleep(100);
                delayedFuture.complete("delayed-result");
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            })
        .start();

    LongPollSelector.SelectResult selectResult = selector.selectAll(5000);

    assertThat(selectResult.getSuccessCount()).isEqualTo(1);
    assertThat(selectResult.isAllSuccess()).isTrue();
    assertThat(handler.getResult()).isEqualTo("delayed-result");
  }

  @Test
  void testSelectWithTimeout() {
    LongPollSelector selector = new LongPollSelector();
    CompletableFuture<String> neverCompleteFuture = new CompletableFuture<>();
    TestHandler<String> handler = new TestHandler<>(LongPollType.CONFIG);

    selector.register(handler, neverCompleteFuture);

    LongPollSelector.SelectResult result = selector.selectAll(100);

    assertThat(result.getSuccessCount()).isEqualTo(0);
    assertThat(result.getTimeoutCount()).isEqualTo(1);
    assertThat(result.isAllSuccess()).isFalse();
    assertThat(handler.getResult()).isNull();
  }

  @Test
  void testSelectWithFailure() {
    LongPollSelector selector = new LongPollSelector();
    CompletableFuture<String> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new RuntimeException("test error"));

    TestHandler<String> handler = new TestHandler<>(LongPollType.CONFIG);

    selector.register(handler, failedFuture);

    LongPollSelector.SelectResult result = selector.selectAll(1000);

    assertThat(result.getSuccessCount()).isEqualTo(0);
    assertThat(result.getFailureCount()).isEqualTo(1);
    assertThat(result.isAllSuccess()).isFalse();
    assertThat(handler.getResult()).isNull();
    assertThat(handler.getError()).isNotNull();
  }

  @Test
  void testSelectNow() {
    LongPollSelector selector = new LongPollSelector();
    CompletableFuture<String> completedFuture = CompletableFuture.completedFuture("result");
    CompletableFuture<String> pendingFuture = new CompletableFuture<>();

    AtomicInteger callCount = new AtomicInteger(0);

    TestHandler<String> configHandler =
        new TestHandler<>(LongPollType.CONFIG, callCount::incrementAndGet);
    TestHandler<String> taskHandler =
        new TestHandler<>(LongPollType.TASK, callCount::incrementAndGet);

    selector
        .register(configHandler, completedFuture)
        .register(taskHandler, pendingFuture);

    int readyCount = selector.selectNow();

    assertThat(readyCount).isEqualTo(1);
    assertThat(callCount.get()).isEqualTo(1);
    assertThat(selector.getProcessedCount()).isEqualTo(1);
  }

  @Test
  void testParallelProcessing() throws InterruptedException {
    LongPollSelector selector = new LongPollSelector();

    CompletableFuture<String> fastFuture = new CompletableFuture<>();
    CompletableFuture<String> slowFuture = new CompletableFuture<>();

    CountDownLatch fastLatch = new CountDownLatch(1);
    CountDownLatch slowLatch = new CountDownLatch(1);
    AtomicReference<Long> fastTime = new AtomicReference<>();
    AtomicReference<Long> slowTime = new AtomicReference<>();

    TestHandler<String> configHandler =
        new TestHandler<>(
            LongPollType.CONFIG,
            () -> {
              fastTime.set(System.currentTimeMillis());
              fastLatch.countDown();
            });

    TestHandler<String> taskHandler =
        new TestHandler<>(
            LongPollType.TASK,
            () -> {
              slowTime.set(System.currentTimeMillis());
              slowLatch.countDown();
            });

    selector.register(configHandler, fastFuture).register(taskHandler, slowFuture);

    long startTime = System.currentTimeMillis();

    // 快响应 100ms 后返回
    new Thread(
            () -> {
              try {
                Thread.sleep(100);
                fastFuture.complete("fast");
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            })
        .start();

    // 慢响应 500ms 后返回
    new Thread(
            () -> {
              try {
                Thread.sleep(500);
                slowFuture.complete("slow");
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            })
        .start();

    LongPollSelector.SelectResult result = selector.selectAll(2000);

    assertThat(result.isAllSuccess()).isTrue();
    assertThat(result.getSuccessCount()).isEqualTo(2);

    // 验证快响应先被处理
    assertThat(fastTime.get()).isLessThan(slowTime.get());
    // 验证快响应在约 100ms 时被处理
    assertThat(fastTime.get() - startTime).isLessThan(200);
  }

  @Test
  void testSelectResultToString() {
    LongPollSelector selector = new LongPollSelector();
    TestHandler<String> handler = new TestHandler<>(LongPollType.CONFIG);
    selector.register(handler, CompletableFuture.completedFuture("test"));

    LongPollSelector.SelectResult result = selector.selectAll(100);

    assertThat(result.toString()).contains("success=1");
    assertThat(result.toString()).contains("failure=0");
  }

  @Test
  void testClear() {
    LongPollSelector selector = new LongPollSelector();
    TestHandler<String> handler = new TestHandler<>(LongPollType.CONFIG);
    selector.register(handler, CompletableFuture.completedFuture("test"));

    assertThat(selector.getChannelCount()).isEqualTo(1);

    selector.clear();

    assertThat(selector.getChannelCount()).isEqualTo(0);
  }

  @Test
  void testGetChannelState() {
    LongPollSelector selector = new LongPollSelector();
    CompletableFuture<String> future = CompletableFuture.completedFuture("test");
    TestHandler<String> handler = new TestHandler<>(LongPollType.CONFIG);

    selector.register(handler, future);

    // 处理前是 PENDING
    assertThat(selector.getChannelState(LongPollType.CONFIG)).isEqualTo("PENDING");

    selector.selectAll(100);

    // 处理后是 PROCESSED
    assertThat(selector.getChannelState(LongPollType.CONFIG)).isEqualTo("PROCESSED");
  }

  @Test
  void testHasAnySuccess() {
    LongPollSelector selector = new LongPollSelector();
    CompletableFuture<String> successFuture = CompletableFuture.completedFuture("success");
    CompletableFuture<String> failFuture = new CompletableFuture<>();
    failFuture.completeExceptionally(new RuntimeException("error"));

    TestHandler<String> configHandler = new TestHandler<>(LongPollType.CONFIG);
    TestHandler<String> taskHandler = new TestHandler<>(LongPollType.TASK);

    selector.register(configHandler, successFuture).register(taskHandler, failFuture);

    LongPollSelector.SelectResult result = selector.selectAll(100);

    assertThat(result.hasAnySuccess()).isTrue();
    assertThat(result.isAllSuccess()).isFalse();
    assertThat(result.getSuccessCount()).isEqualTo(1);
    assertThat(result.getFailureCount()).isEqualTo(1);
  }

  @Test
  void testTotalCount() {
    LongPollSelector selector = new LongPollSelector();
    TestHandler<String> configHandler = new TestHandler<>(LongPollType.CONFIG);
    TestHandler<String> taskHandler = new TestHandler<>(LongPollType.TASK);

    selector
        .register(configHandler, CompletableFuture.completedFuture("1"))
        .register(taskHandler, CompletableFuture.completedFuture("2"));

    LongPollSelector.SelectResult result = selector.selectAll(100);

    assertThat(result.getTotalCount()).isEqualTo(2);
  }

  @Test
  void testHandlerResultHasChanges() {
    LongPollSelector selector = new LongPollSelector();
    TestHandler<String> handler = new TestHandler<>(LongPollType.CONFIG);
    selector.register(handler, CompletableFuture.completedFuture("test"));

    LongPollSelector.SelectResult result = selector.selectAll(100);

    assertThat(result.hasAnyChanges()).isTrue();
    assertThat(result.getHandlerResults()).hasSize(1);
    assertThat(result.getHandlerResults().get(0).hasChanges()).isTrue();
  }
}
