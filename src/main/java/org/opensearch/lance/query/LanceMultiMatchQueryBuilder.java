/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.search.Query;
import org.lance.ipc.FullTextQuery;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lance.mapper.LanceTextFieldMapper;

/**
 * DSL query {@code lance_multi_match}: full-text match across multiple
 * {@code lance_text} columns, running Lance's native multi-match engine
 * with optional per-field boosts.
 *
 * <pre>{@code
 * {
 *   "lance_multi_match": {
 *     "fields": ["body", "title"],
 *     "query": "quick brown fox",
 *     "operator": "or",
 *     "boosts": [1.0, 2.0],
 *     "boost": 1.2,
 *     "_name": "mm_hit"
 *   }
 * }
 * }</pre>
 *
 * <p>Why this exists as a separate DSL: OpenSearch's stock
 * {@code multi_match} query rewrites into a Lucene bool of per-field
 * term queries after analysing on the OpenSearch side, so — as with
 * {@code match} on {@code lance_text} — every token collapses to a
 * single term before Lance sees it. {@code lance_multi_match} hands
 * the raw text and column list straight to
 * {@link FullTextQuery#multiMatch}, which tokenises internally using
 * each column's FTS-index analyzer and scores across fields together.
 *
 * <p>{@code fields} and {@code query} are required. {@code fields}
 * must reference {@code lance_text} columns whose Lance FTS index
 * exists; unknown or non-text fields return 400. {@code boosts}, when
 * present, must list one float per field in the same order. Unknown
 * parameters are rejected so typos surface as 400s rather than silent
 * degradation.
 */
public class LanceMultiMatchQueryBuilder extends AbstractQueryBuilder<LanceMultiMatchQueryBuilder> implements LanceFtsQueryBuilder {

    public static final String NAME = "lance_multi_match";

    private static final Set<String> KNOWN_KEYS = Set.of("fields", "query", "operator", "boosts", "boost", "_name");

    private final List<String> fields;
    private final String query;
    private FullTextQuery.Operator operator = FullTextQuery.Operator.OR;
    private List<Float> boosts = null;

    public LanceMultiMatchQueryBuilder(List<String> fields, String query) {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("[lance_multi_match] fields is required and must be non-empty");
        }
        for (String f : fields) {
            if (f == null || f.isEmpty()) {
                throw new IllegalArgumentException("[lance_multi_match] fields entries must be non-empty strings");
            }
        }
        if (query == null || query.isEmpty()) {
            throw new IllegalArgumentException("[lance_multi_match] query is required");
        }
        // Copy to guard against caller mutation and to normalise into an
        // immutable list for equality.
        this.fields = List.copyOf(fields);
        this.query = query;
    }

    public LanceMultiMatchQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fields = in.readStringList();
        this.query = in.readString();
        this.operator = FullTextQuery.Operator.valueOf(in.readString());
        boolean hasBoosts = in.readBoolean();
        if (hasBoosts) {
            int size = in.readVInt();
            List<Float> b = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                b.add(in.readFloat());
            }
            this.boosts = List.copyOf(b);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeStringCollection(fields);
        out.writeString(query);
        out.writeString(operator.name());
        out.writeBoolean(boosts != null);
        if (boosts != null) {
            out.writeVInt(boosts.size());
            for (Float b : boosts) {
                out.writeFloat(b);
            }
        }
    }

    public LanceMultiMatchQueryBuilder operator(FullTextQuery.Operator operator) {
        this.operator = Objects.requireNonNull(operator, "operator must not be null");
        return this;
    }

    FullTextQuery.Operator operator() {
        return operator;
    }

    public LanceMultiMatchQueryBuilder boosts(List<Float> boosts) {
        if (boosts == null) {
            this.boosts = null;
            return this;
        }
        if (boosts.size() != fields.size()) {
            throw new IllegalArgumentException(
                "[lance_multi_match] boosts must have one entry per field (fields="
                    + fields.size()
                    + ", boosts="
                    + boosts.size()
                    + ")"
            );
        }
        for (Float b : boosts) {
            if (b == null) {
                throw new IllegalArgumentException("[lance_multi_match] boosts entries must not be null");
            }
        }
        this.boosts = List.copyOf(boosts);
        return this;
    }

    List<Float> boosts() {
        return boosts;
    }

    List<String> fields() {
        return fields;
    }

    String query() {
        return query;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("fields", fields);
        builder.field("query", query);
        if (operator != FullTextQuery.Operator.OR) {
            builder.field("operator", operator.name().toLowerCase(Locale.ROOT));
        }
        if (boosts != null) {
            builder.field("boosts", boosts);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static LanceMultiMatchQueryBuilder fromXContent(XContentParser parser) throws IOException {
        Map<String, Object> map = parser.map();

        Object fieldsVal = map.get("fields");
        if (fieldsVal == null) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_multi_match] [fields] is required");
        }
        if (!(fieldsVal instanceof List)) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_multi_match] [fields] must be an array of strings");
        }
        List<?> rawFields = (List<?>) fieldsVal;
        List<String> fieldNames = new ArrayList<>(rawFields.size());
        for (Object f : rawFields) {
            if (!(f instanceof String)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_multi_match] [fields] entries must be strings");
            }
            fieldNames.add((String) f);
        }

        Object queryVal = map.get("query");
        if (queryVal == null) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_multi_match] [query] is required");
        }
        if (!(queryVal instanceof String)) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_multi_match] [query] must be a string");
        }

        LanceMultiMatchQueryBuilder builder = new LanceMultiMatchQueryBuilder(fieldNames, (String) queryVal);

        if (map.containsKey("operator")) {
            Object opVal = map.get("operator");
            if (!(opVal instanceof String)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_multi_match] [operator] must be a string");
            }
            String op = ((String) opVal).toUpperCase(Locale.ROOT);
            if (op.equals("AND")) {
                builder.operator(FullTextQuery.Operator.AND);
            } else if (op.equals("OR")) {
                builder.operator(FullTextQuery.Operator.OR);
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[lance_multi_match] [operator] must be 'and' or 'or', got [" + opVal + "]"
                );
            }
        }

        if (map.containsKey("boosts")) {
            Object boostsVal = map.get("boosts");
            if (!(boostsVal instanceof List)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_multi_match] [boosts] must be an array of numbers");
            }
            List<?> rawBoosts = (List<?>) boostsVal;
            List<Float> parsedBoosts = new ArrayList<>(rawBoosts.size());
            for (Object b : rawBoosts) {
                if (!(b instanceof Number)) {
                    throw new ParsingException(parser.getTokenLocation(), "[lance_multi_match] [boosts] entries must be numbers");
                }
                parsedBoosts.add(((Number) b).floatValue());
            }
            builder.boosts(parsedBoosts);
        }

        if (map.containsKey("boost")) {
            Object boostVal = map.get("boost");
            if (!(boostVal instanceof Number)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_multi_match] [boost] must be a number");
            }
            builder.boost(((Number) boostVal).floatValue());
        }
        if (map.containsKey("_name")) {
            Object name = map.get("_name");
            if (!(name instanceof String)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_multi_match] [_name] must be a string");
            }
            builder.queryName((String) name);
        }
        for (String key : map.keySet()) {
            if (!KNOWN_KEYS.contains(key)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_multi_match] unknown parameter [" + key + "]");
            }
        }
        return builder;
    }

    @Override
    public FullTextQuery toLanceFullTextQuery(QueryShardContext context) {
        for (String field : fields) {
            MappedFieldType fieldType = context.fieldMapper(field);
            if (fieldType == null) {
                throw new IllegalArgumentException("[lance_multi_match] no such field [" + field + "]");
            }
            if (!(fieldType instanceof LanceTextFieldMapper.LanceTextFieldType textType)) {
                throw new IllegalArgumentException(
                    "[lance_multi_match] field [" + field + "] is mapped as [" + fieldType.typeName() + "], not [lance_text]"
                );
            }
            if ("true".equals(textType.meta().get("lance_dropped"))) {
                throw new IllegalArgumentException(
                    "[lance_multi_match] field ["
                        + field
                        + "] no longer exists in the underlying Lance table; recreate the OpenSearch index to drop it"
                );
            }
        }
        return FullTextQuery.multiMatch(query, fields, boosts, operator);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        FullTextQuery ftq = toLanceFullTextQuery(context);
        return new LanceFtsQuery(ftq, new LinkedHashSet<>(fields));
    }

    @Override
    protected boolean doEquals(LanceMultiMatchQueryBuilder other) {
        return fields.equals(other.fields)
            && query.equals(other.query)
            && operator == other.operator
            && Objects.equals(boosts, other.boosts);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fields, query, operator, boosts);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
