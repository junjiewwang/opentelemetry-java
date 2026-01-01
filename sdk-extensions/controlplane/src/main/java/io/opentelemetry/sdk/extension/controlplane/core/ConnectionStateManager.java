/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.core;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 连接状态管理器。
 *
 * <p>负责管理控制平面的连接状态，包括：
 *
 * <ul>
 *   <li>状态机管理
 *   <li>状态转换逻辑
 *   <li>状态变更监听
 * </ul>
 */
public final class ConnectionStateManager {

  private static final Logger logger = Logger.getLogger(ConnectionStateManager.class.getName());

  /** 连接状态枚举 */
  public enum ConnectionState {
    /** Connected - successfully communicated with control plane server. */
    CONNECTED,
    /** Connecting - attempting to connect but not yet verified. */
    CONNECTING,
    /** Disconnected - connection failed or not started. */
    DISCONNECTED,
    /** Waiting for OTLP recovery - paused due to OTLP health issues. */
    WAITING_FOR_OTLP,
    /** Server unavailable - server endpoint exists but control plane API not available. */
    SERVER_UNAVAILABLE
  }

  /** 状态变更监听器 */
  @FunctionalInterface
  public interface StateChangeListener {
    /**
     * 状态变更回调
     *
     * @param previousState 变更前状态
     * @param newState 变更后状态
     */
    void onStateChanged(ConnectionState previousState, ConnectionState newState);
  }

  private final AtomicReference<ConnectionState> currentState;
  private final List<StateChangeListener> listeners;

  /** 创建连接状态管理器 */
  public ConnectionStateManager() {
    this.currentState = new AtomicReference<>(ConnectionState.DISCONNECTED);
    this.listeners = new CopyOnWriteArrayList<>();
  }

  /**
   * 获取当前连接状态
   *
   * @return 当前状态
   */
  public ConnectionState getState() {
    ConnectionState state = currentState.get();
    return state != null ? state : ConnectionState.DISCONNECTED;
  }

  /**
   * 设置连接状态
   *
   * @param newState 新状态
   * @return 变更前的状态
   */
  public ConnectionState setState(ConnectionState newState) {
    ConnectionState previousState = currentState.getAndSet(newState);
    if (previousState != newState) {
      notifyListeners(previousState, newState);
    }
    return previousState;
  }

  /**
   * 尝试转换状态（仅当当前状态匹配时）
   *
   * @param expectedState 期望的当前状态
   * @param newState 新状态
   * @return 是否转换成功
   */
  public boolean compareAndSetState(ConnectionState expectedState, ConnectionState newState) {
    if (currentState.compareAndSet(expectedState, newState)) {
      notifyListeners(expectedState, newState);
      return true;
    }
    return false;
  }

  /**
   * 标记为已连接
   *
   * @return 变更前的状态
   */
  public ConnectionState markConnected() {
    ConnectionState previousState = setState(ConnectionState.CONNECTED);
    if (previousState != ConnectionState.CONNECTED) {
      logger.log(
          Level.INFO,
          "Control plane connected, state changed: {0} -> CONNECTED",
          previousState);
    }
    return previousState;
  }

  /**
   * 标记为断开连接
   *
   * @param reason 断开原因
   * @return 变更前的状态
   */
  public ConnectionState markDisconnected(@Nullable String reason) {
    ConnectionState previousState = setState(ConnectionState.DISCONNECTED);
    if (previousState != ConnectionState.DISCONNECTED) {
      logger.log(
          Level.WARNING,
          "Control plane disconnected, state changed: {0} -> DISCONNECTED, reason: {1}",
          new Object[] {previousState, reason});
    }
    return previousState;
  }

  /**
   * 标记为服务端不可用
   *
   * @return 变更前的状态
   */
  public ConnectionState markServerUnavailable() {
    ConnectionState previousState = currentState.get();
    ConnectionState previous = previousState != null ? previousState : ConnectionState.DISCONNECTED;
    if (previous != ConnectionState.SERVER_UNAVAILABLE) {
      setState(ConnectionState.SERVER_UNAVAILABLE);
      logger.log(
          Level.WARNING,
          "Control plane server unavailable, state changed: {0} -> SERVER_UNAVAILABLE",
          previous);
    }
    return previous;
  }

  /**
   * 标记为等待 OTLP 恢复
   *
   * @return 变更前的状态
   */
  public ConnectionState markWaitingForOtlp() {
    ConnectionState previousState = currentState.get();
    ConnectionState previous = previousState != null ? previousState : ConnectionState.DISCONNECTED;
    if (previous != ConnectionState.WAITING_FOR_OTLP) {
      setState(ConnectionState.WAITING_FOR_OTLP);
      logger.log(
          Level.INFO,
          "Waiting for OTLP recovery, state changed: {0} -> WAITING_FOR_OTLP",
          previous);
    }
    return previous;
  }

  /**
   * 标记为正在连接
   *
   * @return 变更前的状态
   */
  public ConnectionState markConnecting() {
    ConnectionState previousState = setState(ConnectionState.CONNECTING);
    if (previousState != ConnectionState.CONNECTING) {
      logger.log(
          Level.INFO,
          "Connecting to control plane, state changed: {0} -> CONNECTING",
          previousState);
    }
    return previousState;
  }

  /**
   * 检查是否已连接
   *
   * @return 是否已连接
   */
  public boolean isConnected() {
    return getState() == ConnectionState.CONNECTED;
  }

  /**
   * 检查是否可以进行业务操作（已连接状态）
   *
   * @return 是否可以进行业务操作
   */
  public boolean canPerformOperations() {
    return getState() == ConnectionState.CONNECTED;
  }

  /**
   * 添加状态变更监听器
   *
   * @param listener 监听器
   */
  public void addListener(StateChangeListener listener) {
    if (listener != null) {
      listeners.add(listener);
    }
  }

  /**
   * 移除状态变更监听器
   *
   * @param listener 监听器
   */
  public void removeListener(StateChangeListener listener) {
    listeners.remove(listener);
  }

  /** 通知所有监听器 */
  private void notifyListeners(ConnectionState previousState, ConnectionState newState) {
    for (StateChangeListener listener : listeners) {
      try {
        listener.onStateChanged(previousState, newState);
      } catch (RuntimeException e) {
        logger.log(
            Level.WARNING,
            "Error notifying state change listener: {0}",
            e.getMessage());
      }
    }
  }
}
