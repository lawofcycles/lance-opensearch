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
import org.opensearch.lance.rest.RestAttachAction;

/**
 * Per-column mapping override rules an operator declared on the attach
 * or namespace-register body. One column may carry a base type override
 * ({@code type: date} on an integer column stored as epoch millis,
 * {@code type: keyword} on a Utf8 column that also has a Lance inverted
 * index, {@code type: ip} on a Utf8 column holding IP address strings,
 * {@code type: lance_text} on a Utf8 column without an inverted index),
 * a {@code format} (with {@code type: date} only) and keyword
 * sub-field declarations ({@code fields}).
 *
 * <p>The canonical JSON persisted in the {@code index.plugins.lance.overrides}
 * setting uses the attach-body shape:
 *
 * <pre>
 *   {"ts": {"type": "date", "format": "epoch_millis"},
 *    "body": {"type": "keyword", "fields": {"raw": {"type": "keyword"}}}}
 * </pre>
 *
 * <p>{@link #of(Settings)} is the one accessor every setting reader
 * goes through: it reads {@code index.plugins.lance.overrides} first and falls
 * back to the legacy {@code index.plugins.lance.multi_fields} setting when the
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
    public static final String TYPE_IP = "ip";
    public static final String TYPE_WILDCARD = "wildcard";
    public static final String TYPE_GEO_POINT = "geo_point";
    public static final String TYPE_LANCE_TEXT = "lance_text";

    /** Default mapping format of a {@code type: date} override on an integer column. */
    public static final String DEFAULT_DATE_FORMAT = "epoch_millis";

    /** Order values a {@code type: geo_point} override on a FixedSizeList&lt;Float64&gt;[2] column may declare. */
    public static final String ORDER_LAT_LON = "lat_lon";
    public static final String ORDER_LON_LAT = "lon_lat";

    public static final LanceOverrides EMPTY = new LanceOverrides(Collections.emptyMap());

    /**
     * The override of one column. {@code type} and {@code format} are
     * {@code null} when not declared; {@code order} is {@code null}
     * unless the column declares a {@code type: geo_point} override on
     * a FixedSizeList&lt;Float64&gt;[2] column (where the operator picks
     * the storage order); {@code subFields} is empty when the column
     * declares no sub-fields.
     */
    public record Column(String type, String format, String order, LinkedHashMap<String, String> subFields) {
        public Column {
            subFields = subFields == null ? new LinkedHashMap<>() : subFields;
        }

        /** Back-compat constructor: no {@code order}. */
        public Column(String type, String format, LinkedHashMap<String, String> subFields) {
            this(type, format, null, subFields);
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

    /** Columns overridden to {@code ip}. */
    public Set<String> ipColumns() {
        Set<String> out = new LinkedHashSet<>();
        for (Map.Entry<String, Column> entry : columns.entrySet()) {
            if (TYPE_IP.equals(entry.getValue().type())) {
                out.add(entry.getKey());
            }
        }
        return out;
    }

    /**
     * Columns overridden to {@code wildcard}. Served exactly like a
     * {@code keyword} override (the fragment reader has no postings for
     * the n gram accelerated {@code wildcard} field type of OpenSearch
     * core, and the keyword doc values path already answers wildcard,
     * prefix, regexp and term queries); the mapping records the
     * operator's intent in {@code meta.lance_override_type}.
     */
    public Set<String> wildcardColumns() {
        Set<String> out = new LinkedHashSet<>();
        for (Map.Entry<String, Column> entry : columns.entrySet()) {
            if (TYPE_WILDCARD.equals(entry.getValue().type())) {
                out.add(entry.getKey());
            }
        }
        return out;
    }

    /**
     * Columns overridden to {@code lance_text}. A Utf8 column so
     * declared maps to {@code lance_text} whether or not the table
     * carries an inverted index on it: with one Lance searches the
     * index, without one Lance tokenises the scanned rows and scores
     * them with BM25 (its flat path), and with an index that covers
     * some fragments only Lance unions the two. The mapping records
     * the declaration in {@code meta.lance_override_type}, and the
     * freshness check never flips the column between {@code keyword}
     * and {@code lance_text} when the table gains or loses the index.
     * A column may carry one {@code type}, so this set and
     * {@link #keywordColumns()}, {@link #wildcardColumns()} and
     * {@link #ipColumns()} never share a column.
     */
    public Set<String> lanceTextColumns() {
        Set<String> out = new LinkedHashSet<>();
        for (Map.Entry<String, Column> entry : columns.entrySet()) {
            if (TYPE_LANCE_TEXT.equals(entry.getValue().type())) {
                out.add(entry.getKey());
            }
        }
        return out;
    }

    /**
     * Columns overridden to {@code geo_point}, mapped to their declared
     * order ({@link #ORDER_LAT_LON} or {@link #ORDER_LON_LAT}). A Struct
     * column carries {@code null} — the order comes from the child
     * names, not the operator.
     */
    public Map<String, String> geoPointColumns() {
        LinkedHashMap<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, Column> entry : columns.entrySet()) {
            if (TYPE_GEO_POINT.equals(entry.getValue().type())) {
                out.put(entry.getKey(), entry.getValue().order());
            }
        }
        return out;
    }

    /**
     * Resolve the overrides of an index from its settings:
     * {@code index.plugins.lance.overrides} when present, else the legacy
     * {@code index.plugins.lance.multi_fields} setting folded into sub-field
     * only overrides, else {@link #EMPTY}.
     */
    public static LanceOverrides of(Settings settings) {
        String json = LanceSettings.OVERRIDES_SETTING.get(settings);
        if (!json.isEmpty()) {
            return parse(json);
        }
        String legacy = LanceSettings.MULTI_FIELDS_SETTING.get(settings);
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
            columns.put(entry.getKey(), new Column(null, null, null, new LinkedHashMap<>(entry.getValue())));
        }
        return new LanceOverrides(columns);
    }

    /** Overrides from already-validated column entries, for callers that filter an existing instance. */
    public static LanceOverrides fromColumns(Map<String, Column> columns) {
        if (columns == null || columns.isEmpty()) {
            return EMPTY;
        }
        return new LanceOverrides(new LinkedHashMap<>(columns));
    }

    /**
     * The same overrides with every column key that appears in
     * {@code renames} moved to its new name, preserving declaration
     * order. The namespace poll calls this when the Lance table renamed
     * a column (same field id, same Arrow type, new name), so the
     * operator's {@code type} / {@code format} / {@code fields} rules
     * follow the column instead of dangling on the old name. Keys not
     * named in {@code renames} are untouched. A rename whose target
     * name already carries its own declared entry loses to that
     * declaration (the operator named the new column explicitly).
     * Returns {@code this} when nothing changes.
     */
    public LanceOverrides withRenamedColumns(Map<String, String> renames) {
        if (renames == null || renames.isEmpty() || isEmpty()) {
            return this;
        }
        boolean changed = false;
        LinkedHashMap<String, Column> outColumns = new LinkedHashMap<>();
        for (Map.Entry<String, Column> entry : columns.entrySet()) {
            String newName = renames.get(entry.getKey());
            if (newName == null || newName.equals(entry.getKey())) {
                outColumns.put(entry.getKey(), entry.getValue());
                continue;
            }
            changed = true;
            if (columns.containsKey(newName)) {
                // The target name has its own declared override; the
                // renamed entry folds away rather than clobbering it.
                continue;
            }
            outColumns.put(newName, entry.getValue());
        }
        return changed ? new LanceOverrides(outColumns) : this;
    }

    /**
     * The same overrides without the named column's {@code type} /
     * {@code format} / {@code fields} entry, or {@code this} when the
     * column declares none. The namespace poll calls this when a schema
     * reset gave the column an Arrow type its override no longer fits.
     */
    public LanceOverrides withoutColumn(String baseName) {
        if (!columns.containsKey(baseName)) {
            return this;
        }
        LinkedHashMap<String, Column> out = new LinkedHashMap<>(columns);
        out.remove(baseName);
        return out.isEmpty() ? EMPTY : new LanceOverrides(out);
    }

    /**
     * Compact canonical JSON for the {@code index.plugins.lance.overrides}
     * setting; empty string on empty overrides so the caller can skip
     * writing the setting at all.
     */
    public String toJson() {
        if (isEmpty()) {
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
                if (column.order() != null) {
                    builder.field("order", column.order());
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
            // mapOrdered keeps the declaration order, which drives the
            // order of the emitted mapping fields.
            return parseAttachClauses(parser.mapOrdered(), null);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse index.plugins.lance.overrides JSON: " + e.getMessage(), e);
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
     * {@code type} / {@code format} / {@code order} / {@code fields},
     * {@code type} must be {@code date}, {@code keyword}, {@code ip},
     * {@code wildcard}, {@code geo_point} or {@code lance_text}, {@code format} needs
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
                    columns.put(
                        baseName,
                        new Column(existing.type(), existing.format(), existing.order(), new LinkedHashMap<>(entry.getValue()))
                    );
                } else {
                    columns.put(baseName, new Column(null, null, null, new LinkedHashMap<>(entry.getValue())));
                }
            }
        }
        return columns.isEmpty() ? EMPTY : new LanceOverrides(columns);
    }

    /**
     * Refuse a sub-field name the mapping cannot carry. The name is
     * joined to its base column with a dot ({@code body.raw}) and
     * resolved by splitting on that dot wherever a search body names a
     * field, so it must be non-empty and free of {@code .}; OpenSearch's
     * own mapping parser refuses a multi-field name with a dot for the
     * same reason. Whitespace and control characters are refused too:
     * they have no use in a field name and in an attach body they mark
     * a typo that would otherwise surface only as a field no query
     * resolves. Both attach clauses that declare sub-fields
     * ({@code multi_fields} and {@code overrides.[col].fields}) call this
     * so the two agree.
     *
     * @param clausePath the attach body path of the enclosing object,
     *     for the message ({@code multi_fields.body} or
     *     {@code overrides.body.fields})
     * @param subName the declared sub-field name
     * @throws IllegalArgumentException naming the clause and the name
     */
    public static void validateSubFieldName(String clausePath, String subName) {
        if (subName == null || subName.isEmpty()) {
            throw new IllegalArgumentException("[" + clausePath + "] sub-field names must be non-empty strings");
        }
        if (subName.indexOf('.') >= 0) {
            throw new IllegalArgumentException("[" + clausePath + "] sub-field name [" + subName + "] must not contain [.]");
        }
        for (int i = 0; i < subName.length(); i++) {
            char c = subName.charAt(i);
            if (Character.isWhitespace(c) || Character.isISOControl(c)) {
                throw new IllegalArgumentException(
                    "[" + clausePath + "] sub-field name [" + subName + "] must not contain whitespace or control characters"
                );
            }
        }
    }

    private static Column parseColumn(String baseName, Object rawSpec) {
        if (!(rawSpec instanceof Map<?, ?> spec)) {
            throw new IllegalArgumentException("[overrides." + baseName + "] must be an object");
        }
        for (Object key : spec.keySet()) {
            if (!"type".equals(key) && !"format".equals(key) && !"order".equals(key) && !"fields".equals(key)) {
                throw new IllegalArgumentException(
                    "[overrides." + baseName + "] has unknown key [" + key + "]; accepted keys are [type], [format], [order], [fields]"
                );
            }
        }
        String type = null;
        Object rawType = spec.get("type");
        if (rawType != null) {
            if (!(rawType instanceof String typeStr) || typeStr.isEmpty()) {
                throw new IllegalArgumentException("[overrides." + baseName + ".type] must be a non-empty string");
            }
            if (!TYPE_DATE.equals(typeStr)
                && !TYPE_KEYWORD.equals(typeStr)
                && !TYPE_IP.equals(typeStr)
                && !TYPE_WILDCARD.equals(typeStr)
                && !TYPE_GEO_POINT.equals(typeStr)
                && !TYPE_LANCE_TEXT.equals(typeStr)) {
                throw new IllegalArgumentException(
                    "[overrides."
                        + baseName
                        + ".type="
                        + typeStr
                        + "] is not supported; accepted types are [date], [keyword], [ip], [wildcard], [geo_point], [lance_text]"
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
        String order = null;
        Object rawOrder = spec.get("order");
        if (rawOrder != null) {
            if (!(rawOrder instanceof String orderStr) || orderStr.isEmpty()) {
                throw new IllegalArgumentException("[overrides." + baseName + ".order] must be a non-empty string");
            }
            if (!TYPE_GEO_POINT.equals(type)) {
                throw new IllegalArgumentException("[overrides." + baseName + ".order] is only accepted together with [type: geo_point]");
            }
            if (!ORDER_LAT_LON.equals(orderStr) && !ORDER_LON_LAT.equals(orderStr)) {
                throw new IllegalArgumentException(
                    "[overrides."
                        + baseName
                        + ".order="
                        + orderStr
                        + "] is not supported; accepted values are ["
                        + ORDER_LAT_LON
                        + "], ["
                        + ORDER_LON_LAT
                        + "]"
                );
            }
            order = orderStr;
        }
        LinkedHashMap<String, String> subFields = new LinkedHashMap<>();
        Object rawFields = spec.get("fields");
        if (rawFields != null) {
            if (!(rawFields instanceof Map<?, ?> fieldsMap)) {
                throw new IllegalArgumentException("[overrides." + baseName + ".fields] must be an object");
            }
            for (Map.Entry<?, ?> subEntry : fieldsMap.entrySet()) {
                if (!(subEntry.getKey() instanceof String subName)) {
                    throw new IllegalArgumentException("[overrides." + baseName + ".fields] sub-field names must be non-empty strings");
                }
                validateSubFieldName("overrides." + baseName + ".fields", subName);
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
        return new Column(type, format, order, subFields);
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
