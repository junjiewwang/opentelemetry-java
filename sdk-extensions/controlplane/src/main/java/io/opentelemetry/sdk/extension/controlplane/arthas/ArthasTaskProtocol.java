/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.arthas;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Arthas Collector / Agent 任务协议常量。
 *
 * <p>该类用于冻结双端共享的协议标识，避免 task type、参数键、结果字段和错误码在多个模块中散落定义。
 *
 * <p><b>设计约束</b>：
 * <ul>
 *   <li>这里只定义协议层常量，不承载执行逻辑</li>
 *   <li>允许先冻结未来 Phase 使用的 task type，但 Agent 能力上报仍应只声明当前真实已支持的执行器</li>
 *   <li>后续新增 Arthas 任务能力时，应优先复用此类而不是重新硬编码字符串</li>
 * </ul>
 */
public final class ArthasTaskProtocol {

  private ArthasTaskProtocol() {}

  /**
   * 冻结的 Arthas task type 列表。
   *
   * <p>该列表表达协议边界，不代表当前 Agent 已全部实现。
   */
  public static final List<String> FROZEN_TASK_TYPES =
      Collections.unmodifiableList(
          Arrays.asList(
              TaskType.ATTACH,
              TaskType.DETACH,
              TaskType.EXEC_SYNC,
              TaskType.SESSION_OPEN,
              TaskType.SESSION_EXEC,
              TaskType.SESSION_PULL,
              TaskType.SESSION_INTERRUPT,
              TaskType.SESSION_CLOSE));

  /** Arthas 任务类型。 */
  public static final class TaskType {
    public static final String ATTACH = "arthas_attach";
    public static final String DETACH = "arthas_detach";
    public static final String EXEC_SYNC = "arthas_exec_sync";
    public static final String SESSION_OPEN = "arthas_session_open";
    public static final String SESSION_EXEC = "arthas_session_exec";
    public static final String SESSION_PULL = "arthas_session_pull";
    public static final String SESSION_INTERRUPT = "arthas_session_interrupt";
    public static final String SESSION_CLOSE = "arthas_session_close";

    private TaskType() {}
  }

  /** Arthas 任务公共参数键。 */
  public static final class ParameterKey {
    public static final String REQUEST_ID = "request_id";
    public static final String TRACE_ID = "trace_id";
    public static final String USER_ID = "user_id";
    public static final String AUTH_SUBJECT = "auth_subject";
    public static final String COMMAND = "command";
    public static final String ACTION = "action";
    public static final String REASON = "reason";
    public static final String SESSION_ID = "session_id";
    public static final String CONSUMER_ID = "consumer_id";
    public static final String FORCE = "force";
    public static final String AUTO_ATTACH = "auto_attach";
    public static final String REQUIRE_TUNNEL_READY = "require_tunnel_ready";
    public static final String TIMEOUT_MILLIS = "timeout_ms";
    public static final String WAIT_TIMEOUT_MILLIS = "wait_timeout_ms";
    public static final String RESULT_LIMIT_BYTES = "result_limit_bytes";
    public static final String MAX_ITEMS = "max_items";
    public static final String MAX_BYTES = "max_bytes";
    public static final String TTL_MILLIS = "ttl_ms";
    public static final String IDLE_TIMEOUT_MILLIS = "idle_timeout_ms";

    public static final String START_TIMEOUT_MILLIS = "start_timeout_millis";
    public static final String CONNECT_TIMEOUT_MILLIS = "connect_timeout_millis";
    public static final String STOP_TIMEOUT_MILLIS = "stop_timeout_millis";
    public static final String HEALTH_CHECK_GRACE_PERIOD_MILLIS =
        "health_check_grace_period_millis";

    private ParameterKey() {}
  }

  /** Arthas 任务结果 JSON 字段。 */
  public static final class ResultField {
    public static final String TASK_TYPE = "taskType";
    public static final String SUCCESS = "success";
    public static final String COMMAND = "command";
    public static final String SESSION_ID = "sessionId";
    public static final String CONSUMER_ID = "consumerId";
    public static final String STATE = "state";
    public static final String TIMEOUT = "timeout";
    public static final String ERROR_CODE = "errorCode";
    public static final String ERROR_MESSAGE = "errorMessage";
    public static final String PAYLOAD = "payload";
    public static final String RAW_JSON = "rawJson";
    public static final String META = "meta";
    public static final String DELTA = "delta";
    public static final String CLOSED = "closed";
    public static final String INTERRUPTED = "interrupted";
    public static final String ARTHAS_STATE = "arthas_state";
    public static final String TUNNEL_READY = "tunnel_ready";

    private ResultField() {}
  }

  /** Arthas 协议级错误码。 */
  public static final class ErrorCode {
    public static final String INVALID_PARAMETERS = "INVALID_PARAMETERS";
    public static final String ARTHAS_NOT_CONFIGURED = "ARTHAS_NOT_CONFIGURED";
    public static final String ARTHAS_NOT_RUNNING = "ARTHAS_NOT_RUNNING";
    public static final String ARTHAS_NOT_READY = "ARTHAS_NOT_READY";
    public static final String TUNNEL_NOT_READY = "TUNNEL_NOT_READY";
    public static final String ARTHAS_CLASSLOADER_UNAVAILABLE =
        "ARTHAS_CLASSLOADER_UNAVAILABLE";
    public static final String ARTHAS_BOOTSTRAP_UNAVAILABLE =
        "ARTHAS_BOOTSTRAP_UNAVAILABLE";
    public static final String SESSION_MANAGER_UNAVAILABLE = "SESSION_MANAGER_UNAVAILABLE";
    public static final String COMMAND_EXECUTOR_INIT_FAILED = "COMMAND_EXECUTOR_INIT_FAILED";
    public static final String COMMAND_EXECUTION_FAILED = "COMMAND_EXECUTION_FAILED";
    public static final String COMMAND_TIMEOUT = "COMMAND_TIMEOUT";
    public static final String COMMAND_JSON_SERIALIZATION_FAILED =
        "RESULT_JSON_SERIALIZATION_FAILED";
    public static final String COMMAND_JSON_PARSE_FAILED = "RESULT_JSON_PARSE_FAILED";
    public static final String SESSION_NOT_FOUND = "SESSION_NOT_FOUND";
    public static final String SESSION_ALREADY_CLOSED = "SESSION_ALREADY_CLOSED";
    public static final String SESSION_NOT_IDLE = "SESSION_NOT_IDLE";
    public static final String SESSION_EXPIRED = "SESSION_EXPIRED";
    public static final String SESSION_TTL_EXCEEDED = "SESSION_TTL_EXCEEDED";
    public static final String SESSION_IDLE_TIMEOUT = "SESSION_IDLE_TIMEOUT";
    public static final String ASYNC_JOB_INTERRUPTED = "ASYNC_JOB_INTERRUPTED";
    public static final String PULL_RESULT_FAILED = "PULL_RESULT_FAILED";
    public static final String RESULT_TOO_LARGE = "RESULT_TOO_LARGE";

    public static final String NO_SCHEDULER = "NO_SCHEDULER";
    public static final String ARTHAS_START_FAILED = "ARTHAS_START_FAILED";
    public static final String ARTHAS_ATTACH_ERROR = "ARTHAS_ATTACH_ERROR";
    public static final String ARTHAS_ATTACH_STATE_INVALID = "ARTHAS_ATTACH_STATE_INVALID";
    public static final String ARTHAS_STOPPED = "ARTHAS_STOPPED";
    public static final String ARTHAS_DETACH_ERROR = "ARTHAS_DETACH_ERROR";
    public static final String STOP_REQUEST_FAILED = "STOP_REQUEST_FAILED";
    public static final String STOP_CANCELLED = "STOP_CANCELLED";
    public static final String INTERRUPTED = "INTERRUPTED";

    private ErrorCode() {}
  }
}
