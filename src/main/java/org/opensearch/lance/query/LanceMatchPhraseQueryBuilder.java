/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;
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
import org.opensearch.lance.mapper.LanceTextFieldMapper;

/**
 * DSL query {@code lance_match_phrase}: phrase-order full-text search over
 * a {@code lance_text} column, running Lance's native phrase engine.
 *
 * <pre>{@code
 * {
 *   "lance_match_phrase": {
 *     "field": "body",
 *     "query": "quick brown fox",
 *     "slop": 0,
 *     "boost": 1.2,
 *     "_name": "phrase_hit"
 *   }
 * }
 * }</pre>
 *
 * <p>Why this exists as a separate DSL: OpenSearch's stock
 * {@code match_phrase} relies on the field's search analyzer to tokenise
 * the query text and then hands the resulting {@code TokenStream} to
 * {@link MappedFieldType#phraseQuery}. {@code lance_text} advertises
 * {@link org.opensearch.index.mapper.TextSearchInfo#SIMPLE_MATCH_ONLY},
 * whose keyword analyzer emits the whole query as a single token — so
 * Lucene's {@code QueryBuilder.createFieldQuery} sees one token, skips
 * {@code phraseQuery}, and falls back to {@code termQuery}. That path
 * ignores phrase order. Providing {@code lance_match_phrase} lets the
 * caller push a phrase (with slop) straight into
 * {@code FullTextQuery.phrase(text, column, slop)} on Lance, which does
 * its own tokenisation using the analyzer baked into the FTS index.
 *
 * <p>{@code field} and {@code query} are required. {@code slop} defaults
 * to 0 (strict phrase order). Unknown properties are rejected so typos
 * surface as 400s.
 */
public class LanceMatchPhraseQueryBuilder extends AbstractQueryBuilder<LanceMatchPhraseQueryBuilder> implements LanceFtsQueryBuilder {

    public static final String NAME = "lance_match_phrase";

    private static final Set<String> KNOWN_KEYS = Set.of("field", "query", "slop", "boost", "_name");

    private final String field;
    private final String query;
    private int slop;

    public LanceMatchPhraseQueryBuilder(String field, String query) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("[lance_match_phrase] field is required");
        }
        if (query == null || query.isEmpty()) {
            throw new IllegalArgumentException("[lance_match_phrase] query is required");
        }
        this.field = field;
        this.query = query;
        this.slop = 0;
    }

    public LanceMatchPhraseQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.query = in.readString();
        this.slop = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeString(query);
        out.writeVInt(slop);
    }

    public LanceMatchPhraseQueryBuilder slop(int slop) {
        if (slop < 0) {
            throw new IllegalArgumentException("[lance_match_phrase] slop must be >= 0, got " + slop);
        }
        this.slop = slop;
        return this;
    }

    int slop() {
        return slop;
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
        if (slop != 0) {
            builder.field("slop", slop);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static LanceMatchPhraseQueryBuilder fromXContent(XContentParser parser) throws IOException {
        // Map-based parse mirrors LanceKnnQueryBuilder so validation errors
        // arrive as a single readable batch.
        Map<String, Object> map = parser.map();

        Object fieldVal = map.get("field");
        if (fieldVal == null) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_match_phrase] [field] is required");
        }
        if (!(fieldVal instanceof String)) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_match_phrase] [field] must be a string");
        }
        Object queryVal = map.get("query");
        if (queryVal == null) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_match_phrase] [query] is required");
        }
        if (!(queryVal instanceof String)) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_match_phrase] [query] must be a string");
        }
        LanceMatchPhraseQueryBuilder builder = new LanceMatchPhraseQueryBuilder((String) fieldVal, (String) queryVal);
        if (map.containsKey("slop")) {
            Object slopVal = map.get("slop");
            if (!(slopVal instanceof Number)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_match_phrase] [slop] must be a number");
            }
            builder.slop(((Number) slopVal).intValue());
        }
        if (map.containsKey("boost")) {
            Object boostVal = map.get("boost");
            if (!(boostVal instanceof Number)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_match_phrase] [boost] must be a number");
            }
            builder.boost(((Number) boostVal).floatValue());
        }
        if (map.containsKey("_name")) {
            Object name = map.get("_name");
            if (!(name instanceof String)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_match_phrase] [_name] must be a string");
            }
            builder.queryName((String) name);
        }
        for (String key : map.keySet()) {
            if (!KNOWN_KEYS.contains(key)) {
                throw new ParsingException(parser.getTokenLocation(), "[lance_match_phrase] unknown parameter [" + key + "]");
            }
        }
        return builder;
    }

    @Override
    public org.lance.ipc.FullTextQuery toLanceFullTextQuery(QueryShardContext context) {
        MappedFieldType fieldType = context.fieldMapper(field);
        if (fieldType == null) {
            throw new IllegalArgumentException("[lance_match_phrase] no such field [" + field + "]");
        }
        if (!(fieldType instanceof LanceTextFieldMapper.LanceTextFieldType textType)) {
            throw new IllegalArgumentException(
                "[lance_match_phrase] field [" + field + "] is mapped as [" + fieldType.typeName() + "], not [lance_text]"
            );
        }
        if ("true".equals(textType.meta().get("lance_dropped"))) {
            throw new IllegalArgumentException(
                "[lance_match_phrase] field ["
                    + field
                    + "] no longer exists in the underlying Lance table; recreate the OpenSearch index to drop it"
            );
        }
        return org.lance.ipc.FullTextQuery.phrase(query, field, slop);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        org.lance.ipc.FullTextQuery ftq = toLanceFullTextQuery(context);
        return new LanceFtsQuery(ftq, java.util.Set.of(field));
    }

    @Override
    protected boolean doEquals(LanceMatchPhraseQueryBuilder other) {
        return field.equals(other.field) && query.equals(other.query) && slop == other.slop;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, query, slop);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
