/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
import org.opensearch.lance.mapper.LanceTextFieldMapper;
import org.lance.ipc.FullTextQuery;

/**
 * DSL query {@code lance_match}: full-text match over a {@code lance_text}
 * column that pushes AND / OR operator semantics, fuzziness, prefix length,
 * and max term expansions directly into Lance's FTS engine.
 *
 * <pre>{@code
 * {
 *   "lance_match": {
 *     "field": "body",
 *     "query": "quick brown fox",
 *     "operator": "and",
 *     "fuzziness": 1,
 *     "prefix_length": 2,
 *     "max_expansions": 20,
 *     "boost": 1.2,
 *     "_name": "match_hit"
 *   }
 * }
 * }</pre>
 *
 * <p>This is the explicit form of OpenSearch's stock {@code match} on a
 * {@code lance_text} field: the coordinator rewrites such a
 * {@code match} into this builder with the same text, operator,
 * fuzziness, prefix length and expansion bound before planning
 * ({@code StockTextQueryRewriter}), so both spellings plan and score
 * the same. Written directly it carries the parameters the stock
 * parser has no place for outside the planner's path, and it is what a
 * {@code lance_fts_bool} or {@code lance_fts_boost} composes. The stock
 * {@code match} analyses nothing on the OpenSearch side for this field
 * type (the field advertises a keyword search analyzer), so the whole
 * text reaches Lance as one query and Lance tokenises it with the FTS
 * index's analyzer; the operator, applied by OpenSearch's
 * {@code MatchQuery} only across several tokens, would be lost without
 * the rewrite.
 *
 * <p>{@code field} and {@code query} are required. Optional parameters:
 * <ul>
 *   <li>{@code operator}: {@code "or"} (default) or {@code "and"}</li>
 *   <li>{@code fuzziness}: non-negative integer (edit distance). Omit for
 *       an exact match. OpenSearch's {@code "AUTO"} is not supported</li>
 *   <li>{@code prefix_length}: non-negative integer, default 0</li>
 *   <li>{@code max_expansions}: positive integer, default 50</li>
 * </ul>
 * Unknown parameters return 400 — better than silently degrading recall.
 */
public class LanceMatchQueryBuilder extends AbstractQueryBuilder<LanceMatchQueryBuilder> implements LanceFtsQueryBuilder {

    public static final String NAME = "lance_match";
    static final int DEFAULT_MAX_EXPANSIONS = 50;
    static final int DEFAULT_PREFIX_LENGTH = 0;

    private static final Set<String> KNOWN_KEYS = Set.of(
        "field",
        "query",
        "operator",
        "fuzziness",
        "prefix_length",
        "max_expansions",
        "boost",
        "_name"
    );

    private final String field;
    private final String query;
    private FullTextQuery.Operator operator = FullTextQuery.Operator.OR;
    private Integer fuzziness = null;
    private int prefixLength = DEFAULT_PREFIX_LENGTH;
    private int maxExpansions = DEFAULT_MAX_EXPANSIONS;

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
        this.fuzziness = in.readOptionalVInt();
        this.prefixLength = in.readVInt();
        this.maxExpansions = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeString(query);
        out.writeString(operator.name());
        out.writeOptionalVInt(fuzziness);
        out.writeVInt(prefixLength);
        out.writeVInt(maxExpansions);
    }

    public LanceMatchQueryBuilder operator(FullTextQuery.Operator operator) {
        this.operator = Objects.requireNonNull(operator, "operator must not be null");
        return this;
    }

    FullTextQuery.Operator operator() {
        return operator;
    }

    public LanceMatchQueryBuilder fuzziness(Integer fuzziness) {
        if (fuzziness != null && fuzziness < 0) {
            throw new IllegalArgumentException("[lance_match] fuzziness must be >= 0, got " + fuzziness);
        }
        this.fuzziness = fuzziness;
        return this;
    }

    Integer fuzziness() {
        return fuzziness;
    }

    public LanceMatchQueryBuilder prefixLength(int prefixLength) {
        if (prefixLength < 0) {
            throw new IllegalArgumentException("[lance_match] prefix_length must be >= 0, got " + prefixLength);
        }
        this.prefixLength = prefixLength;
        return this;
    }

    int prefixLength() {
        return prefixLength;
    }

    public LanceMatchQueryBuilder maxExpansions(int maxExpansions) {
        if (maxExpansions <= 0) {
            throw new IllegalArgumentException("[lance_match] max_expansions must be > 0, got " + maxExpansions);
        }
        this.maxExpansions = maxExpansions;
        return this;
    }

    int maxExpansions() {
        return maxExpansions;
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
        if (fuzziness != null) {
            builder.field("fuzziness", fuzziness);
        }
        if (prefixLength != DEFAULT_PREFIX_LENGTH) {
            builder.field("prefix_length", prefixLength);
        }
        if (maxExpansions != DEFAULT_MAX_EXPANSIONS) {
            builder.field("max_expansions", maxExpansions);
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
        if (map.containsKey("fuzziness")) {
            // OpenSearch's stock match accepts "AUTO" as a fuzziness
            // value; Lance's FullTextQuery.fuzziness is Optional<Integer>
            // and has no auto mode. Reject non-integer values so callers
            // do not assume they behave like stock match.
            Object fuzzVal = map.get("fuzziness");
            if (!(fuzzVal instanceof Number)) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[lance_match] [fuzziness] must be a non-negative integer (AUTO is not supported)"
                );
            }
            builder.fuzziness(((Number) fuzzVal).intValue());
        }
        if (map.containsKey("prefix_length")) {
            Object plVal = map.get("prefix_length");
            if (!(plVal instanceof Number)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_match] [prefix_length] must be a number");
            }
            builder.prefixLength(((Number) plVal).intValue());
        }
        if (map.containsKey("max_expansions")) {
            Object meVal = map.get("max_expansions");
            if (!(meVal instanceof Number)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_match] [max_expansions] must be a number");
            }
            builder.maxExpansions(((Number) meVal).intValue());
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
    public FullTextQuery toLanceFullTextQuery(QueryShardContext context) {
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
        return FullTextQuery.match(
            textType.searchText(context, query),
            textType.lanceColumn(),
            1f,
            fuzziness == null ? Optional.empty() : Optional.of(fuzziness),
            maxExpansions,
            operator,
            prefixLength
        );
    }

    @Override
    public Set<String> referencedFields() {
        return Set.of(field);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        FullTextQuery ftq = toLanceFullTextQuery(context);
        return new LanceFtsQuery(ftq, Set.of(field));
    }

    @Override
    protected boolean doEquals(LanceMatchQueryBuilder other) {
        return field.equals(other.field)
            && query.equals(other.query)
            && operator == other.operator
            && Objects.equals(fuzziness, other.fuzziness)
            && prefixLength == other.prefixLength
            && maxExpansions == other.maxExpansions;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, query, operator, fuzziness, prefixLength, maxExpansions);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
