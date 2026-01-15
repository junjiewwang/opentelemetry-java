/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.controlplane.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * JSON 工具类
 *
 * <p>基于 Jackson 库提供 JSON 序列化和反序列化功能。
 * 支持基本类型、Map、List、嵌套对象的序列化和反序列化。
 */
public final class JsonUtils {

  /** 单例 ObjectMapper，线程安全，配置一次复用 */
  private static final ObjectMapper MAPPER = createObjectMapper();

  /** 用于泛型类型转换的 TypeReference */
  private static final TypeReference<Map<String, Object>> MAP_TYPE_REF = 
      new TypeReference<Map<String, Object>>() {};

  private JsonUtils() {
    // 工具类，禁止实例化
  }

  private static ObjectMapper createObjectMapper() {
    return new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
  }

  // ==================== 序列化方法 ====================

  /**
   * 将对象序列化为 JSON 字节数组
   *
   * <p>推荐使用此方法将 DTO 对象序列化为请求体，提供类型安全和编译期检查。
   *
   * @param obj 要序列化的对象（通常是带有 @JsonProperty 注解的 DTO）
   * @return JSON 格式的字节数组
   * @throws IllegalArgumentException 如果序列化失败
   */
  public static byte[] toBytes(Object obj) {
    try {
      return MAPPER.writeValueAsBytes(obj);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("JSON serialization failed", e);
    }
  }

  /**
   * 将 Map 序列化为 JSON 字节数组
   *
   * @param data 数据 Map
   * @return JSON 格式的字节数组
   */
  public static byte[] toJsonBytes(Map<String, Object> data) {
    try {
      return MAPPER.writeValueAsBytes(data);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("JSON serialization failed", e);
    }
  }

  /**
   * 将对象序列化为 JSON 字节数组
   *
   * @param entries 键值对
   * @return JSON 字节数组
   */
  public static byte[] toJsonBytes(Object... entries) {
    return toJsonObject(entries).getBytes(StandardCharsets.UTF_8);
  }

  /**
   * 将 Map 序列化为 JSON 字符串
   *
   * @param data 数据 Map
   * @return JSON 字符串
   */
  public static String toJsonString(Map<String, Object> data) {
    try {
      return MAPPER.writeValueAsString(data);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("JSON serialization failed", e);
    }
  }

  /**
   * 序列化简单键值对为 JSON 对象
   *
   * <p>用于构建简单的请求体，如 {"agent_id":"xxx","timeout_millis":30000}
   *
   * @param entries 键值对，格式为 [key1, value1, key2, value2, ...]
   * @return JSON 字符串
   */
  public static String toJsonObject(Object... entries) {
    if (entries.length % 2 != 0) {
      throw new IllegalArgumentException("Entries must be key-value pairs");
    }

    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < entries.length; i += 2) {
      String key = String.valueOf(entries[i]);
      Object value = entries[i + 1];
      // 处理 RawJson 类型
      if (value instanceof RawJson) {
        try {
          map.put(key, MAPPER.readTree(((RawJson) value).getValue()));
        } catch (JsonProcessingException e) {
          map.put(key, value);
        }
      } else {
        map.put(key, value);
      }
    }
    return toJsonString(map);
  }

  // ==================== 反序列化/解析方法 ====================

  /**
   * 从 JSON 字符串中提取字符串字段
   *
   * @param json JSON 字符串
   * @param fieldName 字段名
   * @return 字段值，如果不存在则返回 null
   */
  @Nullable
  public static String extractString(String json, String fieldName) {
    try {
      JsonNode node = MAPPER.readTree(json).get(fieldName);
      return node != null && !node.isNull() ? node.asText() : null;
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  /**
   * 从 JSON 字符串中提取整数字段
   *
   * @param json JSON 字符串
   * @param fieldName 字段名
   * @param defaultValue 默认值
   * @return 字段值
   */
  public static int extractInt(String json, String fieldName, int defaultValue) {
    try {
      return MAPPER.readTree(json).path(fieldName).asInt(defaultValue);
    } catch (JsonProcessingException e) {
      return defaultValue;
    }
  }

  /**
   * 从 JSON 字符串中提取长整数字段
   *
   * @param json JSON 字符串
   * @param fieldName 字段名
   * @param defaultValue 默认值
   * @return 字段值
   */
  public static long extractLong(String json, String fieldName, long defaultValue) {
    try {
      return MAPPER.readTree(json).path(fieldName).asLong(defaultValue);
    } catch (JsonProcessingException e) {
      return defaultValue;
    }
  }

  /**
   * 从 JSON 字符串中提取布尔字段
   *
   * @param json JSON 字符串
   * @param fieldName 字段名
   * @param defaultValue 默认值
   * @return 字段值
   */
  public static boolean extractBoolean(String json, String fieldName, boolean defaultValue) {
    try {
      JsonNode node = MAPPER.readTree(json).get(fieldName);
      if (node != null && node.isBoolean()) {
        return node.asBoolean();
      }
      return defaultValue;
    } catch (JsonProcessingException e) {
      return defaultValue;
    }
  }

  /**
   * 从 JSON 字符串中提取嵌套对象字段（返回原始 JSON 字符串）
   *
   * @param json JSON 字符串
   * @param fieldName 字段名
   * @return 嵌套对象的 JSON 字符串，如果不存在则返回 null
   */
  @Nullable
  public static String extractObject(String json, String fieldName) {
    try {
      JsonNode node = MAPPER.readTree(json).get(fieldName);
      if (node != null && node.isObject()) {
        return node.toString();
      }
      return null;
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  /**
   * 解析简单的 JSON 对象为 Map
   *
   * @param json JSON 字符串
   * @return 解析后的 Map
   */
  public static Map<String, Object> parseSimpleObject(@Nullable String json) {
    if (json == null || json.isEmpty() || "{}".equals(json.trim())) {
      return new HashMap<>();
    }
    try {
      return MAPPER.readValue(json, MAP_TYPE_REF);
    } catch (JsonProcessingException e) {
      return new HashMap<>();
    }
  }

  /**
   * 将 JSON 字节数组反序列化为指定类型的对象
   *
   * @param json JSON 字节数组
   * @param clazz 目标类型
   * @param <T> 泛型类型
   * @return 反序列化后的对象
   * @throws java.io.IOException 如果解析失败
   */
  public static <T> T parseObject(byte[] json, Class<T> clazz) throws java.io.IOException {
    return MAPPER.readValue(json, clazz);
  }

  /**
   * 将 JSON 字符串反序列化为指定类型的对象
   *
   * @param json JSON 字符串
   * @param clazz 目标类型
   * @param <T> 泛型类型
   * @return 反序列化后的对象
   * @throws JsonProcessingException 如果解析失败
   */
  public static <T> T parseObject(String json, Class<T> clazz) throws JsonProcessingException {
    return MAPPER.readValue(json, clazz);
  }

  /**
   * 安全地将 JSON 字节数组反序列化为指定类型的对象
   *
   * @param json JSON 字节数组
   * @param clazz 目标类型
   * @param <T> 泛型类型
   * @return 反序列化后的对象，如果解析失败则返回 null
   */
  @Nullable
  public static <T> T parseObjectSafe(byte[] json, Class<T> clazz) {
    try {
      return MAPPER.readValue(json, clazz);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * 安全地将 JSON 字符串反序列化为指定类型的对象
   *
   * @param json JSON 字符串
   * @param clazz 目标类型
   * @param <T> 泛型类型
   * @return 反序列化后的对象，如果解析失败则返回 null
   */
  @Nullable
  public static <T> T parseObjectSafe(String json, Class<T> clazz) {
    try {
      return MAPPER.readValue(json, clazz);
    } catch (Exception e) {
      return null;
    }
  }

  // ==================== 字符串处理辅助方法 ====================

  /**
   * JSON 字符串转义
   *
   * @param value 原始字符串
   * @return 转义后的字符串
   */
  public static String escapeJson(@Nullable String value) {
    if (value == null) {
      return "";
    }
    try {
      // 使用 Jackson 序列化后去掉首尾引号
      String escaped = MAPPER.writeValueAsString(value);
      return escaped.substring(1, escaped.length() - 1);
    } catch (JsonProcessingException e) {
      return value;
    }
  }

  /**
   * 去除字符串两端的引号
   *
   * @param s 原始字符串
   * @return 去除引号后的字符串
   */
  public static String removeQuotes(String s) {
    if (s != null && s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
      return s.substring(1, s.length() - 1);
    }
    return s;
  }

  // ==================== 原始 JSON 值包装类 ====================

  /**
   * 原始 JSON 值包装类
   *
   * <p>用于在序列化时直接嵌入已有的 JSON 字符串，不做额外转义
   */
  public static final class RawJson {
    private final String value;

    private RawJson(String value) {
      this.value = value != null ? value : "null";
    }

    /**
     * 创建原始 JSON 值
     *
     * @param json JSON 字符串
     * @return RawJson 实例
     */
    public static RawJson of(String json) {
      return new RawJson(json);
    }

    public String getValue() {
      return value;
    }
  }

  // ==================== 便捷构建方法 ====================

  /**
   * 创建 JSON 对象构建器
   *
   * @return 构建器实例
   */
  public static JsonObjectBuilder objectBuilder() {
    return new JsonObjectBuilder();
  }

  /**
   * JSON 对象构建器
   *
   * <p>提供流式 API 构建 JSON 对象
   */
  public static final class JsonObjectBuilder {
    private final ObjectNode node = MAPPER.createObjectNode();

    private JsonObjectBuilder() {}

    public JsonObjectBuilder put(String key, Object value) {
      node.putPOJO(key, value);
      return this;
    }

    public JsonObjectBuilder putIfNotNull(String key, @Nullable Object value) {
      if (value != null) {
        node.putPOJO(key, value);
      }
      return this;
    }

    /**
     * 放入原始 JSON 值（不做转义）
     */
    public JsonObjectBuilder putRawJson(String key, @Nullable String json) {
      if (json != null) {
        try {
          node.set(key, MAPPER.readTree(json));
        } catch (JsonProcessingException e) {
          // 解析失败，忽略此字段
        }
      }
      return this;
    }

    public String build() {
      return node.toString();
    }

    public byte[] buildBytes() {
      return build().getBytes(StandardCharsets.UTF_8);
    }
  }
}
