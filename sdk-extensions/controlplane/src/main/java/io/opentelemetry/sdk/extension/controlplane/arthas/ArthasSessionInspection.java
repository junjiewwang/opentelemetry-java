package io.opentelemetry.sdk.extension.controlplane.arthas;

import javax.annotation.Nullable;

/** Arthas 会话探针结果。 */
public final class ArthasSessionInspection {

  private final String sessionId;
  private final boolean hasForegroundJob;
  @Nullable private final Integer jobId;
  @Nullable private final String jobStatus;

  public ArthasSessionInspection(
      String sessionId,
      boolean hasForegroundJob,
      @Nullable Integer jobId,
      @Nullable String jobStatus) {
    this.sessionId = sessionId;
    this.hasForegroundJob = hasForegroundJob;
    this.jobId = jobId;
    this.jobStatus = jobStatus;
  }

  public String getSessionId() {
    return sessionId;
  }

  public boolean hasForegroundJob() {
    return hasForegroundJob;
  }

  @Nullable
  public Integer getJobId() {
    return jobId;
  }

  @Nullable
  public String getJobStatus() {
    return jobStatus;
  }
}
