/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.instrument;

import io.opentelemetry.sdk.extension.controlplane.core.InstrumentationProvider;
import io.opentelemetry.sdk.extension.controlplane.core.InstrumentationSnapshot;
import io.opentelemetry.sdk.extension.controlplane.instrument.dto.DynamicInstrumentListRequest;
import io.opentelemetry.sdk.extension.controlplane.instrument.dto.DynamicInstrumentListResponse;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionContext;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutionResult;
import io.opentelemetry.sdk.extension.controlplane.task.executor.TaskExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 动态增强规则列表任务执行器
 *
 * <p>处理 {@code dynamic_instrument_list} 类型的只读查询任务，用于返回当前探针内
 * 动态增强规则的运行时快照，包括规则列表、生命周期状态、是否真实生效以及
 * Instrumentation 能力摘要。
 *
 * <p>入参通过 {@link DynamicInstrumentListRequest} 统一管理，出参通过
 * {@link DynamicInstrumentListResponse} 统一管理。
 */
public final class DynamicInstrumentListExecutor implements TaskExecutor {

  private static final Logger logger = DynamicInstrumentLogger.getLogger("list-executor");

  /** 任务类型常量 */
  public static final String TASK_TYPE = "dynamic_instrument_list";

  private final EnhancementStateRegistry stateRegistry;
  private final TransformerManager transformerManager;
  private final InstrumentationProvider instrumentationProvider;

  DynamicInstrumentListExecutor(
      EnhancementStateRegistry stateRegistry,
      TransformerManager transformerManager,
      InstrumentationProvider instrumentationProvider) {
    this.stateRegistry = stateRegistry;
    this.transformerManager = transformerManager;
    this.instrumentationProvider = instrumentationProvider;
  }

  @Override
  public String getTaskType() {
    return TASK_TYPE;
  }

  @Override
  public CompletableFuture<TaskExecutionResult> execute(TaskExecutionContext context) {
    return CompletableFuture.supplyAsync(
        () -> {
          long startTime = System.currentTimeMillis();
          try {
            DynamicInstrumentListRequest request = DynamicInstrumentListRequest.fromContext(context);
            logger.log(
                Level.INFO,
                "[DYNAMIC-INSTRUMENT-LIST] Executing list task: taskId={0}, request={1}",
                new Object[] {context.getTaskId(), request});

            InstrumentationSnapshot snapshot = instrumentationProvider.getSnapshot();
            List<EnhancementState> matchedStates = new ArrayList<>();
            for (EnhancementState state : stateRegistry.getAllStates()) {
              if (matches(state, request)) {
                matchedStates.add(state);
              }
            }
            matchedStates.sort(DynamicInstrumentListExecutor::compareStates);

            int total = matchedStates.size();
            int fromIndex = Math.min(request.getOffset(), total);
            int toIndex = Math.min(fromIndex + request.getLimit(), total);

            List<DynamicInstrumentListResponse.Item> items = new ArrayList<>(toIndex - fromIndex);
            for (int i = fromIndex; i < toIndex; i++) {
              items.add(
                  DynamicInstrumentListResponse.buildItem(
                      matchedStates.get(i), request.isIncludeConfig(), transformerManager));
            }

            DynamicInstrumentListResponse response =
                DynamicInstrumentListResponse.of(
                    DynamicInstrumentListResponse.buildSummary(
                        matchedStates, stateRegistry, transformerManager, snapshot),
                    items,
                    DynamicInstrumentListResponse.buildPaging(request, total, items.size()));

            logger.log(
                Level.INFO,
                "[DYNAMIC-INSTRUMENT-LIST] Query completed: matched={0}, returned={1}",
                new Object[] {total, items.size()});
            return TaskExecutionResult.success(
                response.toJson(), System.currentTimeMillis() - startTime);

          } catch (IllegalArgumentException e) {
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
            logger.log(Level.WARNING, "[DYNAMIC-INSTRUMENT-LIST] Invalid parameters: " + msg, e);
            return TaskExecutionResult.failed(
                "INVALID_PARAMETERS", msg, System.currentTimeMillis() - startTime);
          } catch (RuntimeException e) {
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
            logger.log(Level.SEVERE, "[DYNAMIC-INSTRUMENT-LIST] Unexpected error: " + msg, e);
            return TaskExecutionResult.failed(
                "LIST_QUERY_ERROR", msg, System.currentTimeMillis() - startTime);
          } catch (Throwable t) {
            String msg = t.getClass().getName() + ": " + t.getMessage();
            logger.log(Level.SEVERE, "[DYNAMIC-INSTRUMENT-LIST] Fatal error: " + msg, t);
            return TaskExecutionResult.failed(
                "FATAL_ERROR", msg, System.currentTimeMillis() - startTime);
          }
        });
  }

  @Override
  public String getDescription() {
    return "Dynamic instrumentation runtime rule list executor";
  }

  private boolean matches(EnhancementState state, DynamicInstrumentListRequest request) {
    InstrumentationRule rule = state.getRule();

    if (!request.getRuleId().isEmpty() && !request.getRuleId().equals(rule.getRuleId())) {
      return false;
    }
    if (!request.getClassName().isEmpty()
        && !request.getClassName().equals(rule.getClassName())) {
      return false;
    }
    if (!request.getMethodName().isEmpty()
        && !request.getMethodName().equals(rule.getMethodName())) {
      return false;
    }
    if (request.getType() != null && request.getType() != rule.getType()) {
      return false;
    }
    if (request.getStatus() != null && request.getStatus() != state.getStatus()) {
      return false;
    }

    boolean applied = transformerManager.isApplied(rule.getRuleId());
    return !request.isActiveOnly() || isEffective(state, applied);
  }

  private static boolean isEffective(EnhancementState state, boolean applied) {
    return applied && state.getStatus() == EnhancementState.Status.ACTIVE;
  }

  private static int compareStates(EnhancementState left, EnhancementState right) {
    int statusCompare = Integer.compare(statusOrder(left.getStatus()), statusOrder(right.getStatus()));
    if (statusCompare != 0) {
      return statusCompare;
    }

    int timeCompare = Long.compare(sortTimestamp(right), sortTimestamp(left));
    if (timeCompare != 0) {
      return timeCompare;
    }

    return left.getRuleId().compareTo(right.getRuleId());
  }

  private static int statusOrder(EnhancementState.Status status) {
    switch (status) {
      case ACTIVE:
        return 0;
      case PENDING:
        return 1;
      case REVERTING:
        return 2;
      case FAILED:
        return 3;
      case REVERTED:
        return 4;
    }
    throw new AssertionError("Unexpected enhancement status: " + status);
  }

  private static long sortTimestamp(EnhancementState state) {
    if (state.getActivatedAtMillis() > 0) {
      return state.getActivatedAtMillis();
    }
    return state.getCreatedAtMillis();
  }
}
