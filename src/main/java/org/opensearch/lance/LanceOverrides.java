/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.rest.RestAttachAction;

/**
 * Per-column mapping override rules an operator declared on the attach
 * or namespace-register body. One column may carry a base type override
 * ({@code type: date} on an integer column stored as epoch millis,
 * {@code type: keyword} on a Utf8 column that also has a Lance inverted
 * index), a {@code format} (with {@code type: date} only) and keyword
 * sub-field declarations ({@code fields}).
 *
 * <p>The canonical JSON persisted in the {@code index.lance.overrides}
 * setting uses the attach-body shape:
 *
 * <pre>
 *   {"ts": {"type": "date", "format": "epoch_millis"},
 *    "body": {"type": "keyword", "fields": {"raw": {"type": "keyword"}}}}
 * </pre>
 *
 * <p>{@link #of(Settings)} is the one accessor every setting reader
 * goes through: it reads {@code index.lance.overrides} first and falls
 * back to the legacy {@code index.lance.multi_fields} setting when the
 * new one is empty, so indexes attached before the overrides framework
 * existed keep resolving their sub-fields.
 *
 * <p>Immutable. Structural validation (allowed keys, allowed type
 * values, format syntax) happens in {@link #parseAttachClauses};
 * schema-dependent validation (does the column exist, does its Arrow
 * type admit the override) happens in
 * {@link RestAttachAction#derive} where the dataset is open.
 */
public final class LanceOverrides {

    /** Base column types an override may declare. */
    public static final String TYPE_DATE = "date";
    public static final String TYPE_KEYWORD = "keyword";

    /** Default mapping format of a {@code type: date} override on an integer column. */
    public static final String DEFAULT_DATE_FORMAT = "epoch_millis";

    public static final LanceOverrides EMPTY = new LanceOverrides(Collections.emptyMap());

    /**
     * The override of one column. {@code type} and {@code format} are
     * {@code null} when not declared; {@code subFields} is empty when
     * the column declares no sub-fields.
     */
    public record Column(String type, String format, LinkedHashMap<String, String> subFields) {
        public Column {
            subFields = subFields == null ? new LinkedHashMap<>() : subFields;
        }
    }

    private final Map<String, Column> columns;

    private LanceOverrides(Map<String, Column> columns) {
        this.columns = Collections.unmodifiableMap(columns);
    }

    /** Column name to its override, in declaration order. */
    public Map<String, Column> columns() {
        return columns;
    }

    public boolean isEmpty() {
        return columns.isEmpty();
    }

    /**
     * The sub-field declarations in the legacy multi-fields shape
     * (base column to an ordered sub-field name to sub-field type map),
     * which is what the reader schema, the aggregation pushdown and the
     * planner resolve dotted field names through. Columns without
     * sub-fields are absent.
     */
    public Map<String, LinkedHashMap<String, String>> subFields() {
        LinkedHashMap<String, LinkedHashMap<String, String>> out = new LinkedHashMap<>();
        for (Map.Entry<String, Column> entry : columns.entrySet()) {
            if (!entry.getValue().subFields().isEmpty()) {
                out.put(entry.getKey(), entry.getValue().subFields());
            }
        }
        return out;
    }

    /** Columns overridden to {@code keyword}. */
    public Set<String> keywordColumns() {
        Set<String> out = new LinkedHashSet<>();
        for (Map.Entry<String, Column> entry : columns.entrySet()) {
            if (TYPE_KEYWORD.equals(entry.getValue().type())) {
                out.add(entry.getKey());
            }
        }
        return out;
    }

    /** Columns overridden to {@code date}, mapped to their declared format ({@code null} when none). */
    public Map<String, String> dateColumns() {
        LinkedHashMap<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, Column> entry : columns.entrySet()) {
            if (TYPE_DATE.equals(entry.getValue().type())) {
                out.put(entry.getKey(), entry.getValue().format());
            }
        }
        return out;
    }

    /**
     * Resolve the overrides of an index from its settings:
     * {@code index.lance.overrides} when present, else the legacy
     * {@code index.lance.multi_fields} setting folded into sub-field
     * only overrides, else {@link #EMPTY}.
     */
    public static LanceOverrides of(Settings settings) {
        String json = settings.get(LanceEngineFactory.OVERRIDES_SETTING, "");
        if (!json.isEmpty()) {
            return parse(json);
        }
        String legacy = settings.get(LanceEngineFactory.MULTI_FIELDS_SETTING, "");
        if (!legacy.isEmpty()) {
            return fromSubFields(RestAttachAction.deserialiseMultiFields(legacy));
        }
        return EMPTY;
    }

    /** Overrides that carry only sub-field declarations (the legacy multi-fields shape). */
    public static LanceOverrides fromSubFields(Map<String, LinkedHashMap<String, String>> subFields) {
        if (subFields == null || subFields.isEmpty()) {
            return EMPTY;
        }
        LinkedHashMap<String, Column> columns = new LinkedHashMap<>();
        for (Map.Entry<String, LinkedHashMap<String, String>> entry : subFields.entrySet()) {
            columns.put(entry.getKey(), new Column(null, null, new LinkedHashMap<>(entry.getValue())));
        }
        return new LanceOverrides(columns);
    }

    /**
     * Compact canonical JSON for the {@code index.lance.overrides}
     * setting; empty string on empty overrides so the caller can skip
     * writing the setting at all.
     */
    public String toJson() {
        if (columns.isEmpty()) {
            return "";
        }
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            for (Map.Entry<String, Column> entry : columns.entrySet()) {
                builder.startObject(entry.getKey());
                Column column = entry.getValue();
                if (column.type() != null) {
                    builder.field("type", column.type());
                }
                if (column.format() != null) {
                    builder.field("format", column.format());
                }
                if (!column.subFields().isEmpty()) {
                    builder.startObject("fields");
                    for (Map.Entry<String, String> sub : column.subFields().entrySet()) {
                        builder.startObject(sub.getKey()).field("type", sub.getValue()).endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder.toString();
        } catch (Exception e) {
            throw new IllegalStateException("failed to serialise lance overrides", e);
        }
    }

    /**
     * Parse the canonical JSON back. Empty on empty or null input,
     * {@link IllegalArgumentException} on malformed JSON so a corrupted
     * setting surfaces loudly on shard open rather than as an empty map.
     */
    public static LanceOverrides parse(String json) {
        if (json == null || json.isEmpty()) {
            return EMPTY;
        }
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            return parseAttachClauses(parser.map(), null);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse index.lance.overrides JSON: " + e.getMessage(), e);
        }
    }

    /**
     * Parse and merge the two attach-body clauses that declare
     * overrides. {@code overridesRaw} is the {@code overrides} object;
     * {@code multiFieldsRaw} is the legacy {@code multi_fields} object,
     * folded into {@code overrides.[col].fields.[sub]} entries. A body
     * that declares sub-fields for the same column through both clauses
     * is rejected: which one wins would be a rule the operator did not
     * know about.
     *
     * <p>Structural validation only, everything a 400 without opening
     * the table: per-column values must be objects whose keys come from
     * {@code type} / {@code format} / {@code fields}, {@code type} must
     * be {@code date} or {@code keyword}, {@code format} needs
     * {@code type: date} and must parse through
     * {@link DateFormatter#forPattern}, and sub-field entries must be
     * objects with a string {@code type}.
     *
     * @throws IllegalArgumentException on any structural violation; the
     *     REST layer surfaces it as a 400
     */
    public static LanceOverrides parseAttachClauses(Object overridesRaw, Object multiFieldsRaw) {
        LinkedHashMap<String, Column> columns = new LinkedHashMap<>();
        if (overridesRaw != null) {
            if (!(overridesRaw instanceof Map<?, ?> rawMap)) {
                throw new IllegalArgumentException("[overrides] must be an object; per-column override rules");
            }
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                if (!(entry.getKey() instanceof String baseName) || baseName.isEmpty()) {
                    throw new IllegalArgumentException("[overrides] keys must be non-empty column names");
                }
                columns.put(baseName, parseColumn(baseName, entry.getValue()));
            }
        }
        if (multiFieldsRaw != null) {
            Map<String, LinkedHashMap<String, String>> legacy = RestAttachAction.parseMultiFields(multiFieldsRaw);
            for (Map.Entry<String, LinkedHashMap<String, String>> entry : legacy.entrySet()) {
                String baseName = entry.getKey();
                Column existing = columns.get(baseName);
                if (existing != null && !existing.subFields().isEmpty()) {
                    throw new IllegalArgumentException(
                        "attach body carries both [multi_fields] and [overrides."
                            + baseName
                            + ".fields] for column ["
                            + baseName
                            + "]; use [overrides] and drop the duplicate [multi_fields] entry"
                    );
                }
                if (existing != null) {
                    columns.put(baseName, new Column(existing.type(), existing.format(), new LinkedHashMap<>(entry.getValue())));
                } else {
                    columns.put(baseName, new Column(null, null, new LinkedHashMap<>(entry.getValue())));
                }
            }
        }
        return columns.isEmpty() ? EMPTY : new LanceOverrides(columns);
    }

    private static Column parseColumn(String baseName, Object rawSpec) {
        if (!(rawSpec instanceof Map<?, ?> spec)) {
            throw new IllegalArgumentException("[overrides." + baseName + "] must be an object");
        }
        for (Object key : spec.keySet()) {
            if (!"type".equals(key) && !"format".equals(key) && !"fields".equals(key)) {
                throw new IllegalArgumentException(
                    "[overrides." + baseName + "] has unknown key [" + key + "]; accepted keys are [type], [format], [fields]"
                );
            }
        }
        String type = null;
        Object rawType = spec.get("type");
        if (rawType != null) {
            if (!(rawType instanceof String typeStr) || typeStr.isEmpty()) {
                throw new IllegalArgumentException("[overrides." + baseName + ".type] must be a non-empty string");
            }
            if (!TYPE_DATE.equals(typeStr) && !TYPE_KEYWORD.equals(typeStr)) {
                throw new IllegalArgumentException(
                    "[overrides." + baseName + ".type=" + typeStr + "] is not supported; accepted types are [date], [keyword]"
                );
            }
            type = typeStr;
        }
        String format = null;
        Object rawFormat = spec.get("format");
        if (rawFormat != null) {
            if (!(rawFormat instanceof String formatStr) || formatStr.isEmpty()) {
                throw new IllegalArgumentException("[overrides." + baseName + ".format] must be a non-empty string");
            }
            if (!TYPE_DATE.equals(type)) {
                throw new IllegalArgumentException("[overrides." + baseName + ".format] is only accepted together with [type: date]");
            }
            try {
                DateFormatter.forPattern(formatStr);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "[overrides." + baseName + ".format=" + formatStr + "] is not a valid date format: " + e.getMessage(),
                    e
                );
            }
            format = formatStr;
        }
        LinkedHashMap<String, String> subFields = new LinkedHashMap<>();
        Object rawFields = spec.get("fields");
        if (rawFields != null) {
            if (!(rawFields instanceof Map<?, ?> fieldsMap)) {
                throw new IllegalArgumentException("[overrides." + baseName + ".fields] must be an object");
            }
            for (Map.Entry<?, ?> subEntry : fieldsMap.entrySet()) {
                if (!(subEntry.getKey() instanceof String subName) || subName.isEmpty()) {
                    throw new IllegalArgumentException("[overrides." + baseName + ".fields] sub-field names must be non-empty strings");
                }
                if (!(subEntry.getValue() instanceof Map<?, ?> subDefMap)) {
                    throw new IllegalArgumentException("[overrides." + baseName + ".fields." + subName + "] must be an object");
                }
                Object typeValue = subDefMap.get("type");
                if (!(typeValue instanceof String typeStr) || typeStr.isEmpty()) {
                    throw new IllegalArgumentException(
                        "[overrides." + baseName + ".fields." + subName + ".type] is required and must be a string"
                    );
                }
                subFields.put(subName, typeStr);
            }
        }
        if (type == null && subFields.isEmpty()) {
            throw new IllegalArgumentException("[overrides." + baseName + "] must declare at least one of [type], [fields]");
        }
        return new Column(type, format, subFields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LanceOverrides other)) {
            return false;
        }
        return columns.equals(other.columns);
    }

    @Override
    public int hashCode() {
        return columns.hashCode();
    }

    @Override
    public String toString() {
        return "LanceOverrides" + columns;
    }
}
