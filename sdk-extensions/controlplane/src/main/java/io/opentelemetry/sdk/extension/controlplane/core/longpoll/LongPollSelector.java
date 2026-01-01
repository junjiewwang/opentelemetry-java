/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core.longpoll;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 长轮询选择器
 *
 * <p>类似 Java NIO 的 Selector 模式，用于管理多个长轮询通道。支持：
 *
 * <ul>
 *   <li>注册多个轮询通道（Channel）并绑定 LongPollHandler
 *   <li>等待任意一个通道就绪（select）
 *   <li>通过 Handler 处理就绪的响应
 *   <li>超时控制
 * </ul>
 *
 * <p>使用示例：
 *
 * <pre>{@code
 * LongPollSelector selector = new LongPollSelector();
 *
 * // 注册轮询通道（绑定 Handler）
 * selector.register(configHandler, configFuture);
 * selector.register(taskHandler, taskFuture);
 *
 * // 处理所有就绪响应
 * SelectResult result = selector.selectAll(60000);
 * }</pre>
 */
public final class LongPollSelector {

  private static final Logger logger = Logger.getLogger(LongPollSelector.class.getName());

  /** 轮询通道 */
  private final Map<LongPollType, PollChannel<?>> channels;

  /** 选择结果 */
  public static final class SelectResult {
    private final int successCount;
    private final int failureCount;
    private final int timeoutCount;
    private final long elapsedMillis;
    private final List<Throwable> errors;
    private final List<LongPollHandler.HandlerResult> handlerResults;

    private SelectResult(Builder builder) {
      this.successCount = builder.successCount;
      this.failureCount = builder.failureCount;
      this.timeoutCount = builder.timeoutCount;
      this.elapsedMillis = builder.elapsedMillis;
      this.errors = Collections.unmodifiableList(new ArrayList<>(builder.errors));
      this.handlerResults =
          Collections.unmodifiableList(new ArrayList<>(builder.handlerResults));
    }

    public int getSuccessCount() {
      return successCount;
    }

    public int getFailureCount() {
      return failureCount;
    }

    public int getTimeoutCount() {
      return timeoutCount;
    }

    public long getElapsedMillis() {
      return elapsedMillis;
    }

    public List<Throwable> getErrors() {
      return errors;
    }

    public List<LongPollHandler.HandlerResult> getHandlerResults() {
      return handlerResults;
    }

    public boolean isAllSuccess() {
      return failureCount == 0 && timeoutCount == 0;
    }

    public boolean hasAnySuccess() {
      return successCount > 0;
    }

    public boolean hasAnyChanges() {
      for (LongPollHandler.HandlerResult result : handlerResults) {
        if (result.hasChanges()) {
          return true;
        }
      }
      return false;
    }

    public int getTotalCount() {
      return successCount + failureCount + timeoutCount;
    }

    @Override
    public String toString() {
      return String.format(
          Locale.ROOT,
          "SelectResult{success=%d, failure=%d, timeout=%d, elapsed=%dms}",
          successCount, failureCount, timeoutCount, elapsedMillis);
    }

    /** Builder for SelectResult */
    static class Builder {
      int successCount;
      int failureCount;
      int timeoutCount;
      long elapsedMillis;
      final List<Throwable> errors = new ArrayList<>();
      final List<LongPollHandler.HandlerResult> handlerResults = new ArrayList<>();

      Builder success() {
        successCount++;
        return this;
      }

      Builder success(LongPollHandler.HandlerResult result) {
        successCount++;
        handlerResults.add(result);
        return this;
      }

      Builder failure() {
        failureCount++;
        return this;
      }

      Builder failure(Throwable error) {
        failureCount++;
        errors.add(error);
        return this;
      }

      Builder timeout() {
        timeoutCount++;
        return this;
      }

      Builder elapsed(long millis) {
        elapsedMillis = millis;
        return this;
      }

      SelectResult build() {
        return new SelectResult(this);
      }
    }
  }

  /**
   * 轮询通道（类似 NIO 的 Channel）
   *
   * @param <R> 响应类型
   */
  private static final class PollChannel<R> {
    private final LongPollHandler<R> handler;
    private final CompletableFuture<R> future;
    private volatile boolean processed;
    private volatile ChannelState state;

    enum ChannelState {
      PENDING,
      READY,
      PROCESSED,
      FAILED,
      TIMEOUT
    }

    PollChannel(LongPollHandler<R> handler, CompletableFuture<R> future) {
      this.handler = handler;
      this.future = future;
      this.processed = false;
      this.state = ChannelState.PENDING;
    }

    LongPollType getType() {
      return handler.getType();
    }

    LongPollHandler<R> getHandler() {
      return handler;
    }

    boolean isReady() {
      return future.isDone() && !processed;
    }

    boolean isProcessed() {
      return processed;
    }

    ChannelState getState() {
      return state;
    }

    /** 处理结果 */
    static final class ProcessResult {
      enum Status {
        /** 处理成功 */
        SUCCESS,
        /** 处理失败 */
        FAILURE,
        /** 未就绪（未处理） */
        NOT_READY
      }

      private final Status status;
      @Nullable private final LongPollHandler.HandlerResult handlerResult;

      private ProcessResult(Status status, @Nullable LongPollHandler.HandlerResult handlerResult) {
        this.status = status;
        this.handlerResult = handlerResult;
      }

      static ProcessResult notReady() {
        return new ProcessResult(Status.NOT_READY, null);
      }

      static ProcessResult success(LongPollHandler.HandlerResult handlerResult) {
        return new ProcessResult(Status.SUCCESS, handlerResult);
      }

      static ProcessResult failure() {
        return new ProcessResult(Status.FAILURE, null);
      }

      Status getStatus() {
        return status;
      }

      @Nullable
      LongPollHandler.HandlerResult getHandlerResult() {
        return handlerResult;
      }
    }

    /**
     * 处理响应（如果就绪）
     *
     * @return 处理结果
     */
    ProcessResult tryProcess() {
      if (processed || !future.isDone()) {
        return ProcessResult.notReady();
      }

      processed = true;

      try {
        R result = future.getNow(null);
        if (result != null) {
          state = ChannelState.READY;
          // 使用 Handler 处理响应
          LongPollHandler.HandlerResult handlerResult = handler.handleResponse(result);
          state = ChannelState.PROCESSED;
          return ProcessResult.success(handlerResult);
        } else if (future.isCompletedExceptionally()) {
          state = ChannelState.FAILED;
          // 获取异常并交给 Handler 处理
          try {
            future.join(); // 会抛出异常
          } catch (RuntimeException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            handler.handleError(cause);
          }
          return ProcessResult.failure();
        }
        return ProcessResult.success(LongPollHandler.HandlerResult.noChange());
      } catch (RuntimeException e) {
        state = ChannelState.FAILED;
        handler.handleError(e);
        return ProcessResult.failure();
      }
    }

    /** 标记为超时 */
    void markTimeout() {
      if (!processed) {
        processed = true;
        state = ChannelState.TIMEOUT;
        future.cancel(false);
      }
    }
  }

  /** 创建长轮询选择器 */
  public LongPollSelector() {
    this.channels = new ConcurrentHashMap<>();
  }

  /**
   * 注册轮询通道（使用 LongPollHandler）
   *
   * @param handler 长轮询处理器
   * @param future 异步响应
   * @param <R> 响应类型
   * @return this（支持链式调用）
   */
  public <R> LongPollSelector register(LongPollHandler<R> handler, CompletableFuture<R> future) {
    channels.put(handler.getType(), new PollChannel<>(handler, future));
    logger.log(Level.FINE, "Registered poll channel with handler: {0}", handler.getType());
    return this;
  }

  /**
   * 等待任意一个通道就绪（非阻塞检查）
   *
   * @return 就绪的通道数量
   */
  public int selectNow() {
    int readyCount = 0;
    for (PollChannel<?> channel : channels.values()) {
      if (channel.isReady()) {
        PollChannel.ProcessResult result = channel.tryProcess();
        if (result.getStatus() != PollChannel.ProcessResult.Status.NOT_READY) {
          readyCount++;
          logger.log(
              Level.FINE,
              "Channel {0} processed, result: {1}",
              new Object[] {channel.getType(), result.getStatus()});
        }
      }
    }
    return readyCount;
  }

  /**
   * 等待任意一个通道就绪（阻塞等待）
   *
   * @param timeoutMillis 超时时间（毫秒）
   * @return 就绪的通道数量
   * @throws InterruptedException 如果等待被中断
   */
  public int select(long timeoutMillis) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMillis;

    while (System.currentTimeMillis() < deadline) {
      int readyCount = selectNow();
      if (readyCount > 0) {
        return readyCount;
      }

      // 检查是否所有通道都已处理
      if (isAllProcessed()) {
        return 0;
      }

      // 检查是否所有 Handler 都不再继续
      if (!shouldAnyContinue()) {
        return 0;
      }

      // 短暂休眠，避免 CPU 空转
      Thread.sleep(10);
    }

    return 0; // 超时
  }

  /**
   * 等待并处理所有通道（核心方法）
   *
   * <p>持续轮询直到所有通道都处理完成或超时。每当有通道就绪时立即通过 Handler 处理，不会阻塞其他通道。
   *
   * @param timeoutMillis 总超时时间（毫秒）
   * @return 选择结果
   */
  public SelectResult selectAll(long timeoutMillis) {
    long startTime = System.currentTimeMillis();
    long deadline = startTime + timeoutMillis;
    SelectResult.Builder resultBuilder = new SelectResult.Builder();

    logger.log(
        Level.FINE,
        "selectAll started, channels: {0}, timeout: {1}ms",
        new Object[] {channels.size(), timeoutMillis});

    // 创建合并的 Future，用于高效等待
    List<CompletableFuture<?>> pendingFutures = new ArrayList<>();
    for (PollChannel<?> channel : channels.values()) {
      if (!channel.isProcessed()) {
        pendingFutures.add(channel.future);
      }
    }

    if (pendingFutures.isEmpty()) {
      return resultBuilder.elapsed(0).build();
    }

    // 使用 anyOf 高效等待任意完成
    while (!isAllProcessed() && System.currentTimeMillis() < deadline) {
      try {
        // 计算剩余超时时间
        long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0) {
          break;
        }

        // 收集未处理的 Future
        pendingFutures.clear();
        for (PollChannel<?> channel : channels.values()) {
          if (!channel.isProcessed()) {
            pendingFutures.add(channel.future);
          }
        }

        if (pendingFutures.isEmpty()) {
          break;
        }

        // 等待任意一个完成
        @SuppressWarnings("rawtypes")
        CompletableFuture[] futureArray = pendingFutures.toArray(new CompletableFuture[0]);
        CompletableFuture.anyOf(futureArray).get(remaining, TimeUnit.MILLISECONDS);

        // 处理所有就绪的通道
        processReadyChannels(resultBuilder);

      } catch (java.util.concurrent.TimeoutException e) {
        // 超时，标记未完成的通道
        break;
      } catch (java.util.concurrent.ExecutionException e) {
        // 某个 Future 异常完成，处理所有就绪的通道（包括成功和失败的）
        processReadyChannels(resultBuilder);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // 标记所有未处理的通道为超时
        markRemainingAsTimeout(resultBuilder);
        return resultBuilder.elapsed(System.currentTimeMillis() - startTime).build();
      }
    }

    // 标记剩余未完成的通道为超时
    markRemainingAsTimeout(resultBuilder);

    long elapsed = System.currentTimeMillis() - startTime;
    resultBuilder.elapsed(elapsed);

    SelectResult result = resultBuilder.build();
    logger.log(Level.FINE, "selectAll completed: {0}", result);

    return result;
  }

  /** 处理所有就绪的通道 */
  private void processReadyChannels(SelectResult.Builder resultBuilder) {
    for (PollChannel<?> channel : channels.values()) {
      if (channel.isReady()) {
        PollChannel.ProcessResult processResult = channel.tryProcess();
        if (processResult.getStatus() != PollChannel.ProcessResult.Status.NOT_READY) {
          if (processResult.getStatus() == PollChannel.ProcessResult.Status.SUCCESS) {
            LongPollHandler.HandlerResult handlerResult = processResult.getHandlerResult();
            if (handlerResult != null) {
              resultBuilder.success(handlerResult);
            } else {
              resultBuilder.success();
            }
          } else {
            resultBuilder.failure();
          }
          logger.log(
              Level.FINE,
              "Channel {0} completed via handler, result: {1}",
              new Object[] {channel.getType(), processResult.getStatus()});
        }
      }
    }
  }

  /** 标记剩余通道为超时 */
  private void markRemainingAsTimeout(SelectResult.Builder resultBuilder) {
    for (PollChannel<?> channel : channels.values()) {
      if (!channel.isProcessed()) {
        channel.markTimeout();
        resultBuilder.timeout();
        logger.log(Level.FINE, "Channel {0} marked as timeout", channel.getType());
      }
    }
  }

  /** 检查是否所有通道都已处理 */
  private boolean isAllProcessed() {
    for (PollChannel<?> channel : channels.values()) {
      if (!channel.isProcessed()) {
        return false;
      }
    }
    return true;
  }

  /** 检查是否有任意 Handler 需要继续 */
  private boolean shouldAnyContinue() {
    for (PollChannel<?> channel : channels.values()) {
      if (!channel.isProcessed() && channel.getHandler().shouldContinue()) {
        return true;
      }
    }
    return false;
  }

  /** 获取已注册的通道数量 */
  public int getChannelCount() {
    return channels.size();
  }

  /** 获取已处理的通道数量 */
  public int getProcessedCount() {
    int count = 0;
    for (PollChannel<?> channel : channels.values()) {
      if (channel.isProcessed()) {
        count++;
      }
    }
    return count;
  }

  /** 清除所有通道（用于重用） */
  public void clear() {
    channels.clear();
  }

  /**
   * 获取通道状态（用于调试）
   *
   * @param type 轮询类型
   * @return 通道状态，如果未注册返回 "UNKNOWN"
   */
  public String getChannelState(LongPollType type) {
    PollChannel<?> channel = channels.get(type);
    if (channel == null) {
      return "UNKNOWN";
    }
    return channel.getState().name();
  }
}
