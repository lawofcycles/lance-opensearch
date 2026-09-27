/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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
 * the caller writes on {@code POST /_plugins/_lance/attach} is what Lance's
 * object store sees; there is no OpenSearch-side translation.
 *
 * <p>The value type carries only {@link String} keys and values. Nested
 * objects, arrays, and non-string primitives are rejected at parse time
 * so bad payloads surface as 400 instead of reaching Lance with a
 * corrupted map.
 */
public final class StorageOptions {

    /** Index-settings prefix under which entries are persisted. */
    public static final String INDEX_SETTING_PREFIX = "index.plugins.lance.storage_options.";

    /**
     * Index-settings prefix the first preview releases persisted the
     * entries under. Still read, through the fallback of the group setting
     * registered in {@code LancePlugin}, so an index created under it keeps
     * its options; never written.
     */
    public static final String DEPRECATED_INDEX_SETTING_PREFIX = "index.lance.storage_options.";

    /**
     * The words that mark a storage option or namespace config key as a
     * credential, matched case-insensitively as substrings of the key
     * name. {@code authorization} covers the {@code header.Authorization}
     * property the REST catalog client reads its bearer credential from;
     * {@code credential} covers the Iceberg REST client's
     * {@code credential} property (an OAuth client id and secret pair),
     * which none of the other substrings match. {@link #isSensitiveKey}
     * and {@link #SENSITIVE_INDEX_SETTING_PATTERNS} are both derived from
     * this one list so the redaction in logs and listings and the filter
     * on the settings APIs cannot drift apart.
     */
    public static final List<String> SENSITIVE_KEY_WORDS = List.of("secret", "password", "token", "key", "authorization", "credential");

    /**
     * Glob patterns for OpenSearch's {@code SettingsFilter} that remove
     * the credential entries of {@link #INDEX_SETTING_PREFIX} and of
     * {@link #DEPRECATED_INDEX_SETTING_PREFIX} from the settings and
     * cluster state APIs. Three patterns per prefix and word of
     * {@link #SENSITIVE_KEY_WORDS}: lower case ({@code aws_secret_access_key}),
     * upper case ({@code AWS_SECRET_ACCESS_KEY}) and capitalised
     * ({@code header.Authorization}). The filter matches
     * case-sensitively, so a key that spells the word in another mix of
     * cases ({@code aws_SeCrEt_access_key}) is not filtered; Lance's
     * object store does not recognise such a key either.
     */
    public static final List<String> SENSITIVE_INDEX_SETTING_PATTERNS = sensitiveIndexSettingPatterns();

    private static List<String> sensitiveIndexSettingPatterns() {
        List<String> patterns = new ArrayList<>(SENSITIVE_KEY_WORDS.size() * 6);
        for (String prefix : List.of(INDEX_SETTING_PREFIX, DEPRECATED_INDEX_SETTING_PREFIX)) {
            for (String word : SENSITIVE_KEY_WORDS) {
                patterns.add(prefix + "*" + word + "*");
                patterns.add(prefix + "*" + word.toUpperCase(Locale.ROOT) + "*");
                patterns.add(prefix + "*" + word.substring(0, 1).toUpperCase(Locale.ROOT) + word.substring(1) + "*");
            }
        }
        return Collections.unmodifiableList(patterns);
    }

    /**
     * True for keys whose value must never appear in logs, listings,
     * {@code toString} or the settings APIs: any key whose name contains
     * one of {@link #SENSITIVE_KEY_WORDS}, case-insensitively.
     */
    public static boolean isSensitiveKey(String key) {
        String lower = key.toLowerCase(Locale.ROOT);
        for (String word : SENSITIVE_KEY_WORDS) {
            if (lower.contains(word)) {
                return true;
            }
        }
        return false;
    }

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
     * Read the {@code index.plugins.lance.storage_options.*} group from
     * index settings back into a {@link StorageOptions}; an index created
     * under the deprecated {@code index.lance.storage_options.*} prefix is
     * read through the group setting's fallback. Returns {@link #empty()}
     * when the group is missing.
     */
    public static StorageOptions fromIndexSettings(Settings settings) {
        Settings group = LancePlugin.STORAGE_OPTIONS_SETTING.get(settings);
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
     * Anything whose key {@link #isSensitiveKey} classifies as sensitive
     * is shown as {@code ***REDACTED***}. Used for exception messages
     * and log lines that might otherwise leak credentials to
     * shard-failed responses.
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
            sb.append(entry.getKey()).append("=").append(isSensitiveKey(entry.getKey()) ? "***REDACTED***" : entry.getValue());
        }
        sb.append("}");
        return sb.toString();
    }
}
