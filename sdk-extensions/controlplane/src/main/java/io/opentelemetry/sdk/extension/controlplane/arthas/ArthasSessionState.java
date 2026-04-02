package io.opentelemetry.sdk.extension.controlplane.arthas;

/** Arthas 异步会话状态。 */
public enum ArthasSessionState {
  OPEN,
  EXECUTING,
  IDLE,
  INTERRUPTED,
  CLOSED,
  EXPIRED,
  FAILED
}
