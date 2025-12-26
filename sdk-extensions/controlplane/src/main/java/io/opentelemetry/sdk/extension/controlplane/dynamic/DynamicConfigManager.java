/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.dynamic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * 动态配置管理器
 *
 * <p>管理从控制平面下发的配置，所有配置均支持热更新。
 */
public final class DynamicConfigManager {

  private static final Logger logger = Logger.getLogger(DynamicConfigManager.class.getName());

  // 可热更新的组件注册表
  private final Map<String, HotUpdatableComponent> components;

  // 配置变更监听器
  private final List<ConfigChangeListener> listeners;

  // 当前配置版本
  private volatile String currentConfigVersion;

  // 配置应用状态
  @Nullable private volatile ConfigApplyResult lastApplyResult;

  public DynamicConfigManager() {
    this.components = new ConcurrentHashMap<>();
    this.listeners = new CopyOnWriteArrayList<>();
    this.currentConfigVersion = "";
  }

  /**
   * 注册可热更新组件
   *
   * @param name 组件名称
   * @param component 组件实例
   */
  public void registerComponent(String name, HotUpdatableComponent component) {
    components.put(name, component);
    logger.log(Level.INFO, "Registered hot-updatable component: {0}", name);
  }

  /**
   * 取消注册组件
   *
   * @param name 组件名称
   */
  public void unregisterComponent(String name) {
    components.remove(name);
    logger.log(Level.INFO, "Unregistered hot-updatable component: {0}", name);
  }

  /**
   * 添加配置变更监听器
   *
   * @param listener 监听器
   */
  public void addListener(ConfigChangeListener listener) {
    listeners.add(listener);
  }

  /**
   * 移除配置变更监听器
   *
   * @param listener 监听器
   */
  public void removeListener(ConfigChangeListener listener) {
    listeners.remove(listener);
  }

  /**
   * 应用配置
   *
   * @param config 配置数据
   * @return 应用结果
   */
  public ConfigApplyResult applyConfig(AgentConfigData config) {
    String newVersion = config.getConfigVersion();

    // 版本检查
    if (newVersion.equals(currentConfigVersion)) {
      logger.log(Level.FINE, "Config version {0} already applied, skipping", newVersion);
      return ConfigApplyResult.noChange(newVersion);
    }

    List<String> appliedFields = new ArrayList<>();
    List<String> failedFields = new ArrayList<>();

    // 1. 应用采样配置
    if (config.hasSamplerConfig()) {
      applyWithTracking(
          "sampler",
          () -> applySamplerConfig(config.getSamplerConfig()),
          appliedFields,
          failedFields);
    }

    // 2. 应用批处理配置
    if (config.hasBatchConfig()) {
      applyWithTracking(
          "batch", () -> applyBatchConfig(config.getBatchConfig()), appliedFields, failedFields);
    }

    // 3. 应用动态资源属性
    if (config.hasDynamicResourceAttributes()) {
      applyWithTracking(
          "resource_attributes",
          () -> applyResourceAttributes(config.getDynamicResourceAttributes()),
          appliedFields,
          failedFields);
    }

    // 4. 应用扩展配置
    if (config.hasExtensionConfig()) {
      applyWithTracking(
          "extension",
          () -> applyExtensionConfig(config.getExtensionConfigJson()),
          appliedFields,
          failedFields);
    }

    // 更新当前版本
    currentConfigVersion = newVersion;

    // 构建结果
    ConfigApplyResult.Status status;
    if (failedFields.isEmpty()) {
      status = ConfigApplyResult.Status.APPLIED;
    } else if (appliedFields.isEmpty()) {
      status = ConfigApplyResult.Status.FAILED;
    } else {
      status = ConfigApplyResult.Status.PARTIAL;
    }

    ConfigApplyResult result =
        new ConfigApplyResult(status, newVersion, appliedFields, failedFields, "");

    lastApplyResult = result;

    logger.log(
        Level.INFO,
        "Config {0} applied, status: {1}, applied: {2}, failed: {3}",
        new Object[] {newVersion, status, appliedFields, failedFields});

    // 通知监听器
    notifyListeners(config, appliedFields, failedFields);

    return result;
  }

  /**
   * 获取当前配置版本
   *
   * @return 配置版本
   */
  public String getCurrentConfigVersion() {
    return currentConfigVersion;
  }

  /**
   * 获取最后一次应用结果
   *
   * @return 应用结果，如果从未应用过配置则返回 null
   */
  @Nullable
  public ConfigApplyResult getLastApplyResult() {
    return lastApplyResult;
  }

  private static void applyWithTracking(
      String fieldName, Runnable applier, List<String> appliedFields, List<String> failedFields) {
    try {
      applier.run();
      appliedFields.add(fieldName);
    } catch (RuntimeException e) {
      logger.log(Level.WARNING, "Failed to apply config field: " + fieldName, e);
      failedFields.add(fieldName);
    }
  }

  private void applySamplerConfig(SamplerConfigData config) {
    HotUpdatableComponent sampler = components.get("sampler");
    if (sampler != null) {
      // DynamicSampler 实现了 HotUpdatableComponent，直接调用 update 方法
      sampler.update(config);
    }
  }

  private void applyBatchConfig(BatchConfigData config) {
    HotUpdatableComponent processor = components.get("batch_processor");
    if (processor != null) {
      processor.update(config);
    }
  }

  private void applyResourceAttributes(Map<String, String> attributes) {
    HotUpdatableComponent resource = components.get("resource");
    if (resource != null) {
      resource.update(attributes);
    }
  }

  private void applyExtensionConfig(String json) {
    HotUpdatableComponent extension = components.get("extension");
    if (extension != null) {
      extension.update(json);
    }
  }

  private void notifyListeners(AgentConfigData config, List<String> applied, List<String> failed) {
    for (ConfigChangeListener listener : listeners) {
      try {
        listener.onConfigChanged(config, applied, failed);
      } catch (RuntimeException e) {
        logger.log(Level.WARNING, "Config change listener failed", e);
      }
    }
  }

  /** 可热更新组件接口 */
  public interface HotUpdatableComponent {
    /**
     * 更新组件配置
     *
     * @param config 配置对象
     */
    void update(Object config);
  }

  /** 配置变更监听器 */
  @FunctionalInterface
  public interface ConfigChangeListener {
    /**
     * 配置变更回调
     *
     * @param config 新配置
     * @param appliedFields 已应用的字段
     * @param failedFields 失败的字段
     */
    void onConfigChanged(
        AgentConfigData config, List<String> appliedFields, List<String> failedFields);
  }

  /** 配置应用结果 */
  public static final class ConfigApplyResult {

    /** 应用状态 */
    public enum Status {
      /** 完全生效 */
      APPLIED,
      /** 部分生效 */
      PARTIAL,
      /** 应用失败 */
      FAILED,
      /** 无变更 */
      NO_CHANGE
    }

    private final Status status;
    private final String configVersion;
    private final List<String> appliedFields;
    private final List<String> failedFields;
    private final String errorMessage;

    public ConfigApplyResult(
        Status status,
        String configVersion,
        List<String> appliedFields,
        List<String> failedFields,
        String errorMessage) {
      this.status = status;
      this.configVersion = configVersion;
      this.appliedFields = Collections.unmodifiableList(new ArrayList<>(appliedFields));
      this.failedFields = Collections.unmodifiableList(new ArrayList<>(failedFields));
      this.errorMessage = errorMessage;
    }

    public static ConfigApplyResult noChange(String version) {
      return new ConfigApplyResult(
          Status.NO_CHANGE, version, Collections.emptyList(), Collections.emptyList(), "");
    }

    public Status getStatus() {
      return status;
    }

    public String getConfigVersion() {
      return configVersion;
    }

    public List<String> getAppliedFields() {
      return appliedFields;
    }

    public List<String> getFailedFields() {
      return failedFields;
    }

    public String getErrorMessage() {
      return errorMessage;
    }
  }

  /** Agent 配置数据 (DTO) */
  public interface AgentConfigData {
    String getConfigVersion();

    boolean hasSamplerConfig();

    SamplerConfigData getSamplerConfig();

    boolean hasBatchConfig();

    BatchConfigData getBatchConfig();

    boolean hasDynamicResourceAttributes();

    Map<String, String> getDynamicResourceAttributes();

    boolean hasExtensionConfig();

    String getExtensionConfigJson();
  }

  /** 采样器配置数据 */
  public interface SamplerConfigData {
    SamplerType getType();

    double getRatio();

    enum SamplerType {
      ALWAYS_ON,
      ALWAYS_OFF,
      TRACE_ID_RATIO,
      PARENT_BASED,
      RULE_BASED
    }
  }

  /** 批处理配置数据 */
  public interface BatchConfigData {
    int getMaxExportBatchSize();

    int getMaxQueueSize();

    long getScheduleDelayMillis();

    long getExportTimeoutMillis();
  }
}
