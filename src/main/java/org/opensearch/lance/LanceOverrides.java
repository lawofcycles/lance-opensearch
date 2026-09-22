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
import java.util.TreeSet;

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
 * index, {@code type: ip} on a Utf8 column holding IP address strings),
 * a {@code format} (with {@code type: date} only) and keyword
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
    public static final String TYPE_IP = "ip";
    public static final String TYPE_WILDCARD = "wildcard";
    public static final String TYPE_GEO_POINT = "geo_point";

    /** Default mapping format of a {@code type: date} override on an integer column. */
    public static final String DEFAULT_DATE_FORMAT = "epoch_millis";

    /** Order values a {@code type: geo_point} override on a FixedSizeList&lt;Float64&gt;[2] column may declare. */
    public static final String ORDER_LAT_LON = "lat_lon";
    public static final String ORDER_LON_LAT = "lon_lat";

    /**
     * The {@code indexes} value that suppresses the automatic index on a
     * column: no scalar or vector index is built for it even when the
     * auto-build would have created one.
     */
    public static final String INDEX_NONE = "none";

    /**
     * Scalar Lance index types the {@code indexes} clause accepts, each
     * mapped to the {@code params} keys its Lance option class takes.
     * The names are the ones Lance's scalar index plugin registry
     * resolves ({@code ScalarIndexParams.create(name, json)}).
     */
    public static final Map<String, Set<String>> SCALAR_INDEX_TYPES = Map.of(
        "btree",
        Set.of("zone_size"),
        "bitmap",
        Set.of(),
        "zonemap",
        Set.of("rows_per_zone"),
        "bloomfilter",
        Set.of("number_of_items", "probability"),
        "ngram",
        Set.of(),
        "labellist",
        Set.of()
    );

    /**
     * Vector Lance index types the {@code indexes} clause accepts, each
     * mapped to the {@code params} keys the corresponding
     * {@code VectorIndexParams} build accepts. {@code num_partitions}
     * and {@code sample_rate} steer the IVF training, {@code num_bits}
     * the quantizer ({@code PQ} / {@code SQ} / {@code RQ}),
     * {@code num_sub_vectors} the product quantizer, {@code m} and
     * {@code ef_construction} the HNSW graph.
     */
    public static final Map<String, Set<String>> VECTOR_INDEX_TYPES = Map.of(
        "ivf_flat",
        Set.of("num_partitions", "sample_rate"),
        "ivf_pq",
        Set.of("num_partitions", "num_sub_vectors", "num_bits", "sample_rate"),
        "ivf_sq",
        Set.of("num_partitions", "num_bits", "sample_rate"),
        "ivf_rq",
        Set.of("num_partitions", "num_bits"),
        "ivf_hnsw_pq",
        Set.of("num_partitions", "m", "ef_construction", "num_sub_vectors", "num_bits", "sample_rate"),
        "ivf_hnsw_sq",
        Set.of("num_partitions", "m", "ef_construction", "num_bits", "sample_rate")
    );

    /**
     * Key the persisted {@code index.lance.overrides} JSON stores the
     * index type preferences under, next to the per-column mapping
     * override objects. Reserved: a mapping override cannot target a
     * column with this name (see {@link #parseAttachClauses}).
     */
    static final String INDEXES_KEY = "indexes";

    public static final LanceOverrides EMPTY = new LanceOverrides(Collections.emptyMap(), Collections.emptyMap());

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

    /**
     * The index type preference of one column, from the attach or
     * namespace body's {@code indexes} clause. Exactly one of
     * {@code scalar} and {@code vector} is non-null (the clause parser
     * enforces it); {@code params} maps the option keys of the chosen
     * type to their numeric values and is empty when none were given.
     * The value {@link #INDEX_NONE} means "build no index on this
     * column".
     */
    public record IndexPreference(String scalar, String vector, LinkedHashMap<String, Number> params) {
        public IndexPreference {
            params = params == null ? new LinkedHashMap<>() : params;
        }

        /** The declared type name ({@code scalar} or {@code vector}, whichever is set). */
        public String typeName() {
            return scalar != null ? scalar : vector;
        }
    }

    private final Map<String, Column> columns;
    private final Map<String, IndexPreference> indexPreferences;

    private LanceOverrides(Map<String, Column> columns, Map<String, IndexPreference> indexPreferences) {
        this.columns = Collections.unmodifiableMap(columns);
        this.indexPreferences = Collections.unmodifiableMap(indexPreferences);
    }

    /** Column name to its override, in declaration order. */
    public Map<String, Column> columns() {
        return columns;
    }

    /**
     * Column name to its index type preference from the {@code indexes}
     * clause, in declaration order. Empty when the body declared none.
     */
    public Map<String, IndexPreference> indexPreferences() {
        return indexPreferences;
    }

    public boolean isEmpty() {
        return columns.isEmpty() && indexPreferences.isEmpty();
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
            columns.put(entry.getKey(), new Column(null, null, null, new LinkedHashMap<>(entry.getValue())));
        }
        return new LanceOverrides(columns, Collections.emptyMap());
    }

    /** Overrides from already-validated column entries, for callers that filter an existing instance. */
    public static LanceOverrides fromColumns(Map<String, Column> columns) {
        if (columns == null || columns.isEmpty()) {
            return EMPTY;
        }
        return new LanceOverrides(new LinkedHashMap<>(columns), Collections.emptyMap());
    }

    /**
     * The same overrides with every column key that appears in
     * {@code renames} moved to its new name, preserving declaration
     * order. The namespace poll calls this when the Lance table renamed
     * a column (same field id, same Arrow type, new name), so the
     * operator's {@code type} / {@code format} / {@code fields} rules
     * and the {@code indexes} clause's per-column index preference
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
        LinkedHashMap<String, IndexPreference> outPreferences = new LinkedHashMap<>();
        for (Map.Entry<String, IndexPreference> entry : indexPreferences.entrySet()) {
            String newName = renames.get(entry.getKey());
            if (newName == null || newName.equals(entry.getKey())) {
                outPreferences.put(entry.getKey(), entry.getValue());
                continue;
            }
            changed = true;
            if (indexPreferences.containsKey(newName)) {
                continue;
            }
            outPreferences.put(newName, entry.getValue());
        }
        return changed ? new LanceOverrides(outColumns, outPreferences) : this;
    }

    /**
     * The same overrides without the named column's {@code type} /
     * {@code format} / {@code fields} entry, or {@code this} when the
     * column declares none. The namespace poll calls this when a schema
     * reset gave the column an Arrow type its override no longer fits.
     * The column's {@code indexes} preference, if any, stays: index
     * preferences validate against the schema on their own and wait for
     * a column shape that admits them.
     */
    public LanceOverrides withoutColumn(String baseName) {
        if (!columns.containsKey(baseName)) {
            return this;
        }
        LinkedHashMap<String, Column> out = new LinkedHashMap<>(columns);
        out.remove(baseName);
        return out.isEmpty() && indexPreferences.isEmpty() ? EMPTY : new LanceOverrides(out, new LinkedHashMap<>(indexPreferences));
    }

    /**
     * Compact canonical JSON for the {@code index.lance.overrides}
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
            if (!indexPreferences.isEmpty()) {
                builder.startObject(INDEXES_KEY);
                writeIndexPreferences(builder, indexPreferences);
                builder.endObject();
            }
            builder.endObject();
            return builder.toString();
        } catch (Exception e) {
            throw new IllegalStateException("failed to serialise lance overrides", e);
        }
    }

    private static void writeIndexPreferences(XContentBuilder builder, Map<String, IndexPreference> preferences) throws Exception {
        for (Map.Entry<String, IndexPreference> entry : preferences.entrySet()) {
            builder.startObject(entry.getKey());
            IndexPreference preference = entry.getValue();
            if (preference.scalar() != null) {
                builder.field("scalar", preference.scalar());
            }
            if (preference.vector() != null) {
                builder.field("vector", preference.vector());
            }
            if (!preference.params().isEmpty()) {
                builder.startObject("params");
                for (Map.Entry<String, Number> param : preference.params().entrySet()) {
                    builder.field(param.getKey(), param.getValue());
                }
                builder.endObject();
            }
            builder.endObject();
        }
    }

    /**
     * Compact canonical JSON of one preference map alone, the shape the
     * {@code indexes} object has on the attach body. Carries a one-shot
     * {@code build_indexes} preference over the wire; empty string on an
     * empty map.
     */
    public static String indexPreferencesToJson(Map<String, IndexPreference> preferences) {
        if (preferences == null || preferences.isEmpty()) {
            return "";
        }
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            writeIndexPreferences(builder, preferences);
            builder.endObject();
            return builder.toString();
        } catch (Exception e) {
            throw new IllegalStateException("failed to serialise lance index preferences", e);
        }
    }

    /** Parse {@link #indexPreferencesToJson} back. Empty map on empty or null input. */
    public static Map<String, IndexPreference> indexPreferencesFromJson(String json) {
        if (json == null || json.isEmpty()) {
            return Collections.emptyMap();
        }
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            return parseIndexesClause(parser.mapOrdered());
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse lance index preferences JSON: " + e.getMessage(), e);
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
            Map<String, Object> raw = parser.mapOrdered();
            Object indexesRaw = raw.remove(INDEXES_KEY);
            return parseAttachClauses(raw, null, indexesRaw);
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
     * be {@code date}, {@code keyword}, {@code ip} or {@code wildcard},
     * {@code format} needs {@code type: date} and must parse through
     * {@link DateFormatter#forPattern}, and sub-field entries must be
     * objects with a string {@code type}.
     *
     * @throws IllegalArgumentException on any structural violation; the
     *     REST layer surfaces it as a 400
     */
    public static LanceOverrides parseAttachClauses(Object overridesRaw, Object multiFieldsRaw) {
        return parseAttachClauses(overridesRaw, multiFieldsRaw, null);
    }

    /**
     * Same as {@link #parseAttachClauses(Object, Object)} with the
     * body's {@code indexes} clause: per-column Lance index type
     * preferences ({@code scalar} / {@code vector} / {@code params}),
     * parsed through {@link #parseIndexesClause}. Because the
     * preferences persist inside the same {@code index.lance.overrides}
     * JSON under a top-level {@code indexes} key, a mapping override
     * cannot target a column literally named {@code indexes}; declaring
     * one is rejected here.
     */
    public static LanceOverrides parseAttachClauses(Object overridesRaw, Object multiFieldsRaw, Object indexesRaw) {
        LinkedHashMap<String, Column> columns = new LinkedHashMap<>();
        if (overridesRaw != null) {
            if (!(overridesRaw instanceof Map<?, ?> rawMap)) {
                throw new IllegalArgumentException("[overrides] must be an object; per-column override rules");
            }
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                if (!(entry.getKey() instanceof String baseName) || baseName.isEmpty()) {
                    throw new IllegalArgumentException("[overrides] keys must be non-empty column names");
                }
                if (INDEXES_KEY.equals(baseName)) {
                    throw new IllegalArgumentException(
                        "[overrides.indexes] is not accepted: [indexes] is reserved for the index type preferences "
                            + "persisted next to the overrides; a mapping override cannot target a column with that name"
                    );
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
                    columns.put(baseName, new Column(existing.type(), existing.format(), existing.order(), new LinkedHashMap<>(entry.getValue())));
                } else {
                    columns.put(baseName, new Column(null, null, null, new LinkedHashMap<>(entry.getValue())));
                }
            }
        }
        Map<String, IndexPreference> preferences = indexesRaw == null ? Collections.emptyMap() : parseIndexesClause(indexesRaw);
        return columns.isEmpty() && preferences.isEmpty() ? EMPTY : new LanceOverrides(columns, preferences);
    }

    /**
     * Parse the {@code indexes} clause of an attach, namespace-register
     * or {@code build_indexes} body into per-column preferences.
     * Structural validation only, everything a 400 without opening the
     * table: each column entry must be an object whose keys come from
     * {@code scalar} / {@code vector} / {@code params}; exactly one of
     * {@code scalar} and {@code vector} must be declared, its value one
     * of the type names Lance's Java SDK can build for that kind (or
     * {@link #INDEX_NONE}); {@code params} keys are checked against the
     * chosen type's option class and values must be numbers.
     * Schema-dependent validation (does the column exist, is it a
     * scalar or vector column) happens where the dataset is open.
     *
     * @throws IllegalArgumentException on any structural violation; the
     *     REST layer surfaces it as a 400
     */
    public static LinkedHashMap<String, IndexPreference> parseIndexesClause(Object raw) {
        if (!(raw instanceof Map<?, ?> rawMap)) {
            throw new IllegalArgumentException("[indexes] must be an object; per-column Lance index type preferences");
        }
        LinkedHashMap<String, IndexPreference> preferences = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
            if (!(entry.getKey() instanceof String column) || column.isEmpty()) {
                throw new IllegalArgumentException("[indexes] keys must be non-empty column names");
            }
            preferences.put(column, parseIndexPreference(column, entry.getValue()));
        }
        return preferences;
    }

    private static IndexPreference parseIndexPreference(String column, Object rawSpec) {
        if (!(rawSpec instanceof Map<?, ?> spec)) {
            throw new IllegalArgumentException("[indexes." + column + "] must be an object");
        }
        for (Object key : spec.keySet()) {
            if (!"scalar".equals(key) && !"vector".equals(key) && !"params".equals(key)) {
                throw new IllegalArgumentException(
                    "[indexes." + column + "] has unknown key [" + key + "]; accepted keys are [scalar], [vector], [params]"
                );
            }
        }
        String scalar = indexTypeName(column, "scalar", spec.get("scalar"), SCALAR_INDEX_TYPES.keySet());
        String vector = indexTypeName(column, "vector", spec.get("vector"), VECTOR_INDEX_TYPES.keySet());
        if (scalar != null && vector != null) {
            throw new IllegalArgumentException(
                "[indexes." + column + "] declares both [scalar] and [vector]; a column carries one index kind"
            );
        }
        if (scalar == null && vector == null) {
            throw new IllegalArgumentException("[indexes." + column + "] must declare one of [scalar], [vector]");
        }
        String typeName = scalar != null ? scalar : vector;
        LinkedHashMap<String, Number> params = new LinkedHashMap<>();
        Object rawParams = spec.get("params");
        if (rawParams != null) {
            if (INDEX_NONE.equals(typeName)) {
                throw new IllegalArgumentException("[indexes." + column + ".params] is not accepted together with [" + INDEX_NONE + "]");
            }
            if (!(rawParams instanceof Map<?, ?> paramsMap)) {
                throw new IllegalArgumentException("[indexes." + column + ".params] must be an object of numeric values");
            }
            Set<String> accepted = scalar != null ? SCALAR_INDEX_TYPES.get(scalar) : VECTOR_INDEX_TYPES.get(vector);
            for (Map.Entry<?, ?> param : paramsMap.entrySet()) {
                if (!(param.getKey() instanceof String key) || !accepted.contains(key)) {
                    throw new IllegalArgumentException(
                        "[indexes."
                            + column
                            + ".params] has unknown key ["
                            + param.getKey()
                            + "] for ["
                            + typeName
                            + "]; "
                            + (accepted.isEmpty() ? "[" + typeName + "] accepts no params" : "accepted keys are " + new TreeSet<>(accepted))
                    );
                }
                if (!(param.getValue() instanceof Number value)) {
                    throw new IllegalArgumentException(
                        "[indexes." + column + ".params." + param.getKey() + "] must be a number, got " + param.getValue()
                    );
                }
                params.put(key, value);
            }
        }
        return new IndexPreference(scalar, vector, params);
    }

    private static String indexTypeName(String column, String kind, Object raw, Set<String> accepted) {
        if (raw == null) {
            return null;
        }
        if (!(raw instanceof String name) || name.isEmpty()) {
            throw new IllegalArgumentException("[indexes." + column + "." + kind + "] must be a non-empty string");
        }
        if (!INDEX_NONE.equals(name) && !accepted.contains(name)) {
            throw new IllegalArgumentException(
                "[indexes."
                    + column
                    + "."
                    + kind
                    + "="
                    + name
                    + "] is not supported; accepted values are "
                    + new TreeSet<>(accepted)
                    + " or ["
                    + INDEX_NONE
                    + "]"
            );
        }
        return name;
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
                && !TYPE_GEO_POINT.equals(typeStr)) {
                throw new IllegalArgumentException(
                    "[overrides."
                        + baseName
                        + ".type="
                        + typeStr
                        + "] is not supported; accepted types are [date], [keyword], [ip], [wildcard], [geo_point]"
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
                throw new IllegalArgumentException(
                    "[overrides." + baseName + ".order] is only accepted together with [type: geo_point]"
                );
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
        return columns.equals(other.columns) && indexPreferences.equals(other.indexPreferences);
    }

    @Override
    public int hashCode() {
        return columns.hashCode() * 31 + indexPreferences.hashCode();
    }

    @Override
    public String toString() {
        return "LanceOverrides" + columns + (indexPreferences.isEmpty() ? "" : " indexes" + indexPreferences);
    }
}
