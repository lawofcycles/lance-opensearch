/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.search.Query;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lance.mapper.LanceVectorFieldMapper;

/**
 * DSL query {@code lance_knn}: shard-level nearest-neighbour search over a
 * Lance vector column.
 *
 * <pre>{@code
 * {
 *   "lance_knn": {
 *     "field": "vec",
 *     "vector": [...],
 *     "k": 5,
 *     "nprobes": 10,
 *     "refine_factor": 4,
 *     "ef": 64,
 *     "metric": "cosine",
 *     "use_index": true,
 *     "boost": 1.2,
 *     "_name": "primary_knn"
 *   }
 * }
 * }</pre>
 *
 * <p>{@code field}, {@code vector}, and {@code k} are required (k defaults
 * to 10 if the client omits it). {@code nprobes}, {@code refine_factor},
 * {@code ef}, {@code metric}, {@code use_index} are Lance search knobs
 * forwarded to {@code org.lance.ipc.Query.Builder}; they control recall
 * vs. cost and let the caller override the metric baked into the vector
 * index. Unknown properties are rejected so typos surface as 400s
 * instead of being silently ignored.
 */
public class LanceKnnQueryBuilder extends AbstractQueryBuilder<LanceKnnQueryBuilder> {

    public static final String NAME = "lance_knn";

    private final String field;
    private final float[] vector;
    private final int k;
    private Integer nprobes;
    private Integer refineFactor;
    private Integer ef;
    private String metric;
    private Boolean useIndex;
    private org.opensearch.index.query.QueryBuilder filter;

    public LanceKnnQueryBuilder(String field, float[] vector, int k) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("field is required");
        }
        if (vector == null || vector.length == 0) {
            throw new IllegalArgumentException("vector is required");
        }
        if (k <= 0) {
            throw new IllegalArgumentException("k must be > 0, got " + k);
        }
        this.field = field;
        this.vector = vector;
        this.k = k;
    }

    public LanceKnnQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.vector = in.readFloatArray();
        this.k = in.readVInt();
        this.nprobes = in.readOptionalVInt();
        this.refineFactor = in.readOptionalVInt();
        this.ef = in.readOptionalVInt();
        this.metric = in.readOptionalString();
        this.useIndex = in.readOptionalBoolean();
        this.filter = in.readOptionalNamedWriteable(org.opensearch.index.query.QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeFloatArray(vector);
        out.writeVInt(k);
        out.writeOptionalVInt(nprobes);
        out.writeOptionalVInt(refineFactor);
        out.writeOptionalVInt(ef);
        out.writeOptionalString(metric);
        out.writeOptionalBoolean(useIndex);
        out.writeOptionalNamedWriteable(filter);
    }

    public LanceKnnQueryBuilder nprobes(int nprobes) {
        if (nprobes <= 0) {
            throw new IllegalArgumentException("nprobes must be > 0, got " + nprobes);
        }
        this.nprobes = nprobes;
        return this;
    }

    public LanceKnnQueryBuilder refineFactor(int refineFactor) {
        if (refineFactor <= 0) {
            throw new IllegalArgumentException("refine_factor must be > 0, got " + refineFactor);
        }
        this.refineFactor = refineFactor;
        return this;
    }

    public LanceKnnQueryBuilder ef(int ef) {
        if (ef <= 0) {
            throw new IllegalArgumentException("ef must be > 0, got " + ef);
        }
        this.ef = ef;
        return this;
    }

    public LanceKnnQueryBuilder metric(String metric) {
        this.metric = metric;
        return this;
    }

    public LanceKnnQueryBuilder useIndex(boolean useIndex) {
        this.useIndex = useIndex;
        return this;
    }

    public LanceKnnQueryBuilder filter(org.opensearch.index.query.QueryBuilder filter) {
        this.filter = filter;
        return this;
    }

    Integer nprobes() {
        return nprobes;
    }

    Integer refineFactor() {
        return refineFactor;
    }

    Integer ef() {
        return ef;
    }

    String metric() {
        return metric;
    }

    Boolean useIndex() {
        return useIndex;
    }

    org.opensearch.index.query.QueryBuilder filter() {
        return filter;
    }

    String field() {
        return field;
    }

    float[] vector() {
        return vector;
    }

    int k() {
        return k;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("field", field);
        builder.field("vector", vector);
        builder.field("k", k);
        if (nprobes != null) {
            builder.field("nprobes", nprobes);
        }
        if (refineFactor != null) {
            builder.field("refine_factor", refineFactor);
        }
        if (ef != null) {
            builder.field("ef", ef);
        }
        if (metric != null) {
            builder.field("metric", metric);
        }
        if (useIndex != null) {
            builder.field("use_index", useIndex);
        }
        if (filter != null) {
            builder.field("filter");
            filter.toXContent(builder, params);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @SuppressWarnings("unchecked")
    public static LanceKnnQueryBuilder fromXContent(XContentParser parser) throws IOException {
        // Parse the object as a Map so we can validate every property in one
        // pass rather than juggling the streaming XContentParser state. The
        // schema is small and does not benefit from ObjectParser here.
        Map<String, Object> map = parser.map();

        String field = requireString(parser, map, "field");
        float[] vector = requireFloatArray(parser, map, "vector");

        int k = 10;
        if (map.containsKey("k")) {
            k = intValue(parser, map.get("k"), "k");
            if (k <= 0) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_knn] k must be > 0, got " + k);
            }
        }

        LanceKnnQueryBuilder builder = new LanceKnnQueryBuilder(field, vector, k);

        if (map.containsKey("nprobes")) {
            builder.nprobes(intValue(parser, map.get("nprobes"), "nprobes"));
        }
        if (map.containsKey("refine_factor")) {
            builder.refineFactor(intValue(parser, map.get("refine_factor"), "refine_factor"));
        }
        if (map.containsKey("ef")) {
            builder.ef(intValue(parser, map.get("ef"), "ef"));
        }
        if (map.containsKey("metric")) {
            Object m = map.get("metric");
            if (!(m instanceof String)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_knn] metric must be a string");
            }
            builder.metric((String) m);
        }
        if (map.containsKey("use_index")) {
            Object u = map.get("use_index");
            if (!(u instanceof Boolean)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_knn] use_index must be a boolean");
            }
            builder.useIndex((Boolean) u);
        }
        if (map.containsKey("boost")) {
            builder.boost(((Number) map.get("boost")).floatValue());
        }
        if (map.containsKey("_name")) {
            Object name = map.get("_name");
            if (!(name instanceof String)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_knn] _name must be a string");
            }
            builder.queryName((String) name);
        }
        if (map.containsKey("filter")) {
            Object f = map.get("filter");
            if (!(f instanceof Map<?, ?>)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_knn] filter must be a query object");
            }
            // The rest of fromXContent operates on the Map view of the
            // body; for `filter` we need to hand it back to
            // AbstractQueryBuilder.parseInnerQueryBuilder, which is
            // streaming. Serialise the sub-map to JSON and hand it a
            // scoped XContentParser that inherits our registry.
            try {
                XContentBuilder tmp = org.opensearch.common.xcontent.XContentFactory.jsonBuilder();
                tmp.map((Map<String, Object>) f);
                try (
                    XContentParser innerParser = org.opensearch.core.xcontent.MediaTypeRegistry.JSON.xContent()
                        .createParser(parser.getXContentRegistry(), parser.getDeprecationHandler(), tmp.toString())
                ) {
                    innerParser.nextToken();
                    builder.filter(org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder(innerParser));
                }
            } catch (IOException e) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_knn] filter parse failed: " + e.getMessage(), e);
            }
        }

        // Reject typos so silent property loss does not translate into wrong
        // recall or a metric that was never applied.
        for (String key : map.keySet()) {
            if (!KNOWN_KEYS.contains(key)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_knn] unknown parameter [" + key + "]");
            }
        }

        return builder;
    }

    private static final java.util.Set<String> KNOWN_KEYS = java.util.Set.of(
        "field",
        "vector",
        "k",
        "nprobes",
        "refine_factor",
        "ef",
        "metric",
        "use_index",
        "filter",
        "boost",
        "_name"
    );

    private static String requireString(XContentParser parser, Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_knn] [" + key + "] is required");
        }
        if (!(value instanceof String)) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_knn] [" + key + "] must be a string");
        }
        return (String) value;
    }

    @SuppressWarnings("unchecked")
    private static float[] requireFloatArray(XContentParser parser, Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_knn] [" + key + "] is required");
        }
        if (!(value instanceof List<?>)) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_knn] [" + key + "] must be an array of numbers");
        }
        List<?> list = (List<?>) value;
        if (list.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_knn] [" + key + "] must not be empty");
        }
        float[] arr = new float[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Object v = list.get(i);
            if (!(v instanceof Number)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_knn] [" + key + "] element " + i + " is not a number");
            }
            arr[i] = ((Number) v).floatValue();
        }
        return arr;
    }

    private static int intValue(XContentParser parser, Object value, String key) {
        if (!(value instanceof Number)) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "[lance_knn] [" + key + "] must be a number, got " + (value == null ? "null" : value.getClass().getSimpleName())
            );
        }
        return ((Number) value).intValue();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        // Reject queries against fields that aren't mapped as lance_vector.
        // Without this check, a typo or a scalar field name reaches Lance
        // and comes back as a 500. `context.fieldMapper` is null when the
        // mapping has no entry for the requested field.
        org.opensearch.index.mapper.MappedFieldType fieldType = context.fieldMapper(field);
        if (fieldType == null) {
            throw new IllegalArgumentException("[lance_knn] no such field [" + field + "]");
        }
        if (!(fieldType instanceof LanceVectorFieldMapper.LanceVectorFieldType vectorType)) {
            throw new IllegalArgumentException(
                "[lance_knn] field [" + field + "] is mapped as [" + fieldType.typeName() + "], not [lance_vector]"
            );
        }
        if (vectorType.isDropped()) {
            throw new IllegalArgumentException(
                "[lance_knn] field [" + field + "] no longer exists in the underlying Lance table; recreate the OpenSearch index to drop it"
            );
        }
        if (vectorType.dimension() != vector.length) {
            throw new IllegalArgumentException(
                "[lance_knn] vector length "
                    + vector.length
                    + " does not match the dimension of field ["
                    + field
                    + "] ("
                    + vectorType.dimension()
                    + ")"
            );
        }
        String filterSql = filter == null ? null : LanceKnnFilterTranslator.toLanceSql(filter, name -> {
            // Resolve the field's OpenSearch mapping type through
            // the QueryShardContext. Returns null for unmapped
            // fields (the translator falls back to shape heuristics
            // there). This is what makes numeric-epoch-millis on
            // date columns and ISO-8601 strings on non-date
            // columns route through the correct SQL literal form
            // for the pre-filter path. A dotted name only resolves
            // when its parent path is a plain object mapper (a
            // Struct child, which Lance's SQL parser reads as a
            // nested field access); a multi-field sub-field
            // (body.raw) and a nested (List<Struct>) child return
            // null so the translator's dotted-path guard keeps both
            // off the Lance SQL path (DataFusion cannot address a
            // list element in a filter).
            MappedFieldType mft = context.fieldMapper(name);
            if (mft == null) {
                return null;
            }
            int dot = name.lastIndexOf('.');
            if (dot > 0) {
                ObjectMapper parent = context.getObjectMapper(name.substring(0, dot));
                if (parent == null || parent.nested().isNested()) {
                    return null;
                }
            }
            String typeName = mft.typeName();
            if ("date".equals(typeName)) {
                // A date field the attach body overrode onto an integer
                // column compares as a number on the Lance SQL side;
                // the field meta carries the real Arrow type.
                String arrowType = mft.meta() == null ? null : mft.meta().get("lance_arrow_type");
                if (arrowType != null && arrowType.startsWith("Int(")) {
                    return LanceKnnFilterTranslator.DATE_ON_INTEGER;
                }
            }
            if ("ip".equals(typeName)) {
                // An ip field always sits on a Utf8 column in this
                // plugin; its predicates never push as string
                // comparisons (see LanceKnnFilterTranslator.IP_ON_UTF8).
                return LanceKnnFilterTranslator.IP_ON_UTF8;
            }
            return typeName;
        });
        return new LanceKnnQuery(field, vector, k, nprobes, refineFactor, ef, parseDistance(metric), useIndex, filterSql);
    }

    private static org.lance.index.DistanceType parseDistance(String metric) {
        if (metric == null) {
            return null;
        }
        switch (metric.toLowerCase(Locale.ROOT)) {
            case "l2":
                return org.lance.index.DistanceType.L2;
            case "cosine":
                return org.lance.index.DistanceType.Cosine;
            case "dot":
                return org.lance.index.DistanceType.Dot;
            case "hamming":
                return org.lance.index.DistanceType.Hamming;
            default:
                throw new IllegalArgumentException("[lance_knn] unknown metric [" + metric + "]");
        }
    }

    @Override
    protected boolean doEquals(LanceKnnQueryBuilder other) {
        return field.equals(other.field)
            && java.util.Arrays.equals(vector, other.vector)
            && k == other.k
            && Objects.equals(nprobes, other.nprobes)
            && Objects.equals(refineFactor, other.refineFactor)
            && Objects.equals(ef, other.ef)
            && Objects.equals(metric, other.metric)
            && Objects.equals(useIndex, other.useIndex)
            && Objects.equals(filter, other.filter);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, java.util.Arrays.hashCode(vector), k, nprobes, refineFactor, ef, metric, useIndex, filter);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
