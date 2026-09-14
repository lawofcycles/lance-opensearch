/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.search.Query;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.lance.ipc.FullTextQuery;

/**
 * DSL query {@code lance_match}: full-text match over a {@code lance_text}
 * column that pushes AND / OR operator semantics directly into Lance's
 * FTS engine.
 *
 * <pre>{@code
 * {
 *   "lance_match": {
 *     "field": "body",
 *     "query": "quick brown fox",
 *     "operator": "and",
 *     "boost": 1.2,
 *     "_name": "match_hit"
 *   }
 * }
 * }</pre>
 *
 * <p>Why this exists as a separate DSL: OpenSearch's stock {@code match}
 * query analyses the query text on the OpenSearch side, splits it into
 * tokens, and applies the {@code operator} clause to a Lucene
 * {@link org.apache.lucene.search.BooleanQuery} that combines one
 * {@code termQuery} per token. For a field advertising
 * {@link org.opensearch.index.mapper.TextSearchInfo#SIMPLE_MATCH_ONLY} the
 * analyzer only lowercases, so all tokens end up in one term and Lance
 * — which tokenises internally with the FTS index's analyzer — never sees
 * the operator. {@code lance_match} bypasses that layer by handing the
 * raw text and operator to {@link FullTextQuery#match} on Lance directly.
 *
 * <p>{@code field} and {@code query} are required. {@code operator}
 * accepts {@code "or"} (default) and {@code "and"}. Unknown keys and
 * parameters that Lance does not yet expose ({@code fuzziness},
 * {@code minimum_should_match}, {@code prefix_length}, etc.) return
 * 400 — better than silently degrading recall.
 */
public class LanceMatchQueryBuilder extends AbstractQueryBuilder<LanceMatchQueryBuilder> {

    public static final String NAME = "lance_match";

    private static final Set<String> KNOWN_KEYS = Set.of("field", "query", "operator", "boost", "_name");

    private final String field;
    private final String query;
    private FullTextQuery.Operator operator = FullTextQuery.Operator.OR;

    public LanceMatchQueryBuilder(String field, String query) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("[lance_match] field is required");
        }
        if (query == null || query.isEmpty()) {
            throw new IllegalArgumentException("[lance_match] query is required");
        }
        this.field = field;
        this.query = query;
    }

    public LanceMatchQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.query = in.readString();
        this.operator = FullTextQuery.Operator.valueOf(in.readString());
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeString(query);
        out.writeString(operator.name());
    }

    public LanceMatchQueryBuilder operator(FullTextQuery.Operator operator) {
        this.operator = Objects.requireNonNull(operator, "operator must not be null");
        return this;
    }

    FullTextQuery.Operator operator() {
        return operator;
    }

    String field() {
        return field;
    }

    String query() {
        return query;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("field", field);
        builder.field("query", query);
        if (operator != FullTextQuery.Operator.OR) {
            builder.field("operator", operator.name().toLowerCase(Locale.ROOT));
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static LanceMatchQueryBuilder fromXContent(XContentParser parser) throws IOException {
        Map<String, Object> map = parser.map();

        Object fieldVal = map.get("field");
        if (fieldVal == null) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_match] [field] is required");
        }
        if (!(fieldVal instanceof String)) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_match] [field] must be a string");
        }
        Object queryVal = map.get("query");
        if (queryVal == null) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_match] [query] is required");
        }
        if (!(queryVal instanceof String)) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_match] [query] must be a string");
        }
        LanceMatchQueryBuilder builder = new LanceMatchQueryBuilder((String) fieldVal, (String) queryVal);
        if (map.containsKey("operator")) {
            Object opVal = map.get("operator");
            if (!(opVal instanceof String)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_match] [operator] must be a string");
            }
            String op = ((String) opVal).toUpperCase(Locale.ROOT);
            if (op.equals("AND")) {
                builder.operator(FullTextQuery.Operator.AND);
            } else if (op.equals("OR")) {
                builder.operator(FullTextQuery.Operator.OR);
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[lance_match] [operator] must be 'and' or 'or', got [" + opVal + "]"
                );
            }
        }
        if (map.containsKey("boost")) {
            Object boostVal = map.get("boost");
            if (!(boostVal instanceof Number)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_match] [boost] must be a number");
            }
            builder.boost(((Number) boostVal).floatValue());
        }
        if (map.containsKey("_name")) {
            Object name = map.get("_name");
            if (!(name instanceof String)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_match] [_name] must be a string");
            }
            builder.queryName((String) name);
        }
        for (String key : map.keySet()) {
            if (!KNOWN_KEYS.contains(key)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_match] unknown parameter [" + key + "]");
            }
        }
        return builder;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        MappedFieldType fieldType = context.fieldMapper(field);
        if (fieldType == null) {
            throw new IllegalArgumentException("[lance_match] no such field [" + field + "]");
        }
        if (!(fieldType instanceof LanceTextFieldMapper.LanceTextFieldType textType)) {
            throw new IllegalArgumentException(
                "[lance_match] field [" + field + "] is mapped as [" + fieldType.typeName() + "], not [lance_text]"
            );
        }
        if ("true".equals(textType.meta().get("lance_dropped"))) {
            throw new IllegalArgumentException(
                "[lance_match] field ["
                    + field
                    + "] no longer exists in the underlying Lance table; recreate the OpenSearch index to drop it"
            );
        }
        return new LanceFtsQuery(field, query, operator);
    }

    @Override
    protected boolean doEquals(LanceMatchQueryBuilder other) {
        return field.equals(other.field) && query.equals(other.query) && operator == other.operator;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, query, operator);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
