/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.lance.ReadOptions;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Immutable wrapper around a Lance object-store storage options map, the
 * same shape Lance's Rust {@code object_store} accepts via
 * {@link ReadOptions.Builder#setStorageOptions(Map)}. The plugin surface
 * uses the Lance-native key names (e.g. {@code aws_access_key_id},
 * {@code aws_region}, {@code aws_endpoint}, {@code allow_http}) so what
 * the caller writes on {@code POST /_lance/attach} is what Lance's
 * object store sees; there is no OpenSearch-side translation.
 *
 * <p>The value type carries only {@link String} keys and values. Nested
 * objects, arrays, and non-string primitives are rejected at parse time
 * so bad payloads surface as 400 instead of reaching Lance with a
 * corrupted map.
 */
public final class StorageOptions {

    /** Index-settings prefix under which entries are persisted. */
    public static final String INDEX_SETTING_PREFIX = "index.lance.storage_options.";

    private static final StorageOptions EMPTY = new StorageOptions(Collections.emptyMap());

    private final Map<String, String> options;

    private StorageOptions(Map<String, String> options) {
        // TreeMap so equals / hashCode / toString / persisted settings do
        // not depend on the caller's iteration order, and callers can
        // compare two StorageOptions without worrying about ordering.
        this.options = Collections.unmodifiableMap(new TreeMap<>(options));
    }

    public static StorageOptions empty() {
        return EMPTY;
    }

    /**
     * Wrap a caller-supplied map. Values are copied so later mutation of
     * the input does not leak into the returned {@code StorageOptions}.
     * Throws {@link IllegalArgumentException} on null keys or values.
     */
    public static StorageOptions of(Map<String, String> options) {
        if (options == null || options.isEmpty()) {
            return EMPTY;
        }
        Map<String, String> copy = new LinkedHashMap<>(options.size());
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("storage_options keys must be non-empty strings");
            }
            if (value == null) {
                throw new IllegalArgumentException("storage_options value for [" + key + "] must not be null");
            }
            copy.put(key, value);
        }
        return new StorageOptions(copy);
    }

    /**
     * Parse the {@code storage_options} field from a request body map.
     * Accepts a JSON object whose values are strings; rejects anything
     * else. Returns {@link #empty()} when the caller omits the field.
     */
    public static StorageOptions parseFromRequestField(Object rawValue, String errorPrefix) {
        if (rawValue == null) {
            return EMPTY;
        }
        if (!(rawValue instanceof Map<?, ?> rawMap)) {
            throw new IllegalArgumentException(errorPrefix + " [storage_options] must be a JSON object");
        }
        Map<String, String> parsed = new LinkedHashMap<>(rawMap.size());
        for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();
            if (!(key instanceof String stringKey) || stringKey.isEmpty()) {
                throw new IllegalArgumentException(errorPrefix + " [storage_options] keys must be non-empty strings");
            }
            if (!(value instanceof String stringValue)) {
                throw new IllegalArgumentException(
                    errorPrefix
                        + " [storage_options."
                        + stringKey
                        + "] must be a string (nested objects / arrays / numbers / booleans are not accepted)"
                );
            }
            parsed.put(stringKey, stringValue);
        }
        return of(parsed);
    }

    /**
     * Read the {@code index.lance.storage_options.*} group from index
     * settings back into a {@link StorageOptions}. Returns {@link #empty()}
     * when the group is missing.
     */
    public static StorageOptions fromIndexSettings(Settings settings) {
        Settings group = settings.getByPrefix(INDEX_SETTING_PREFIX);
        Map<String, String> map = new LinkedHashMap<>();
        for (String key : group.keySet()) {
            map.put(key, group.get(key));
        }
        return of(map);
    }

    /**
     * Write the options into an OpenSearch settings builder under the
     * canonical {@link #INDEX_SETTING_PREFIX}. No-op when empty.
     */
    public void writeToSettings(Settings.Builder builder) {
        for (Map.Entry<String, String> entry : options.entrySet()) {
            builder.put(INDEX_SETTING_PREFIX + entry.getKey(), entry.getValue());
        }
    }

    public static StorageOptions readFromStream(StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size == 0) {
            return EMPTY;
        }
        Map<String, String> map = new LinkedHashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            String value = in.readString();
            map.put(key, value);
        }
        return of(map);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(options.size());
        for (Map.Entry<String, String> entry : options.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
    }

    /** Returns an unmodifiable view of the underlying map. */
    public Map<String, String> asMap() {
        return options;
    }

    public boolean isEmpty() {
        return options.isEmpty();
    }

    /**
     * Build a {@link ReadOptions} instance carrying these options.
     * Returns {@code null} when {@link #isEmpty()} so callers can pass
     * the {@link org.lance.OpenDatasetBuilder} the null and skip the
     * {@code readOptions} configuration entirely — Lance uses its own
     * defaults in that case.
     */
    public ReadOptions toReadOptionsOrNull() {
        if (options.isEmpty()) {
            return null;
        }
        return new ReadOptions.Builder().setStorageOptions(options).build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StorageOptions other)) return false;
        return options.equals(other.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(options);
    }

    /**
     * Human-readable representation with credential-like values redacted.
     * Anything whose key contains "secret", "key", "password", or "token"
     * (case-insensitive) is shown as {@code ***REDACTED***}. Used for
     * exception messages and log lines that might otherwise leak
     * credentials to shard-failed responses.
     */
    @Override
    public String toString() {
        if (options.isEmpty()) {
            return "StorageOptions{}";
        }
        StringBuilder sb = new StringBuilder("StorageOptions{");
        boolean first = true;
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(entry.getKey()).append("=").append(redactedValue(entry.getKey(), entry.getValue()));
        }
        sb.append("}");
        return sb.toString();
    }

    private static String redactedValue(String key, String value) {
        String lower = key.toLowerCase(java.util.Locale.ROOT);
        if (lower.contains("secret") || lower.contains("password") || lower.contains("token") || lower.equals("aws_access_key_id")) {
            return "***REDACTED***";
        }
        return value;
    }
}
