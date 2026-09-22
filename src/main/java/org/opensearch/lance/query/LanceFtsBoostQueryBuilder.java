/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.search.Query;
import org.lance.ipc.FullTextQuery;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

/**
 * DSL query {@code lance_fts_boost}: composes two Lance FTS clauses so
 * matches on the {@code positive} clause score normally while matches
 * on the {@code negative} clause have their score multiplied by
 * {@code negative_boost}. Maps 1:1 onto Lance's
 * {@link FullTextQuery#boost} so scoring composes on Lance's side
 * rather than being layered on top of Lucene.
 *
 * <pre>{@code
 * {
 *   "lance_fts_boost": {
 *     "positive": { "lance_match": { "field": "body", "query": "hello" } },
 *     "negative": { "lance_match": { "field": "body", "query": "outdated" } },
 *     "negative_boost": 0.2,
 *     "boost": 1.0,
 *     "_name": "boosted"
 *   }
 * }
 * }</pre>
 *
 * <p>Both {@code positive} and {@code negative} are required and must
 * themselves be Lance FTS DSLs (any builder implementing
 * {@link LanceFtsQueryBuilder} — {@code lance_match},
 * {@code lance_match_phrase}, {@code lance_multi_match},
 * {@code lance_fts_boost}, {@code lance_fts_bool}). A non-Lance-FTS
 * clause returns 400 because Lance's boost engine cannot accept a
 * Lucene Query; wrapping such a clause into a bool at the Lucene layer
 * would silently break score composition.
 *
 * <p>{@code negative_boost} defaults to Lance's own default (0.5) when
 * omitted and must be a non-negative float when provided.
 */
public class LanceFtsBoostQueryBuilder extends AbstractQueryBuilder<LanceFtsBoostQueryBuilder> implements LanceFtsQueryBuilder {

    public static final String NAME = "lance_fts_boost";

    private final QueryBuilder positive;
    private final QueryBuilder negative;
    private Float negativeBoost = null;

    public LanceFtsBoostQueryBuilder(QueryBuilder positive, QueryBuilder negative) {
        if (positive == null) {
            throw new IllegalArgumentException("[lance_fts_boost] positive clause must not be null");
        }
        if (negative == null) {
            throw new IllegalArgumentException("[lance_fts_boost] negative clause must not be null");
        }
        this.positive = positive;
        this.negative = negative;
    }

    public LanceFtsBoostQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.positive = in.readNamedWriteable(QueryBuilder.class);
        this.negative = in.readNamedWriteable(QueryBuilder.class);
        boolean hasNegativeBoost = in.readBoolean();
        if (hasNegativeBoost) {
            this.negativeBoost = in.readFloat();
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(positive);
        out.writeNamedWriteable(negative);
        out.writeBoolean(negativeBoost != null);
        if (negativeBoost != null) {
            out.writeFloat(negativeBoost);
        }
    }

    public LanceFtsBoostQueryBuilder negativeBoost(Float negativeBoost) {
        if (negativeBoost != null && negativeBoost < 0f) {
            throw new IllegalArgumentException("[lance_fts_boost] negative_boost must be >= 0, got " + negativeBoost);
        }
        this.negativeBoost = negativeBoost;
        return this;
    }

    Float negativeBoost() {
        return negativeBoost;
    }

    QueryBuilder positive() {
        return positive;
    }

    QueryBuilder negative() {
        return negative;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("positive");
        positive.toXContent(builder, params);
        builder.field("negative");
        negative.toXContent(builder, params);
        if (negativeBoost != null) {
            builder.field("negative_boost", negativeBoost);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static LanceFtsBoostQueryBuilder fromXContent(XContentParser parser) throws IOException {
        QueryBuilder positive = null;
        QueryBuilder negative = null;
        Float negativeBoost = null;
        float boost = DEFAULT_BOOST;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("positive".equals(currentFieldName)) {
                    positive = parseInnerQueryBuilder(parser);
                } else if ("negative".equals(currentFieldName)) {
                    negative = parseInnerQueryBuilder(parser);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[lance_fts_boost] unknown object parameter [" + currentFieldName + "]"
                    );
                }
            } else if (token.isValue()) {
                if ("negative_boost".equals(currentFieldName)) {
                    negativeBoost = parser.floatValue();
                } else if (BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[lance_fts_boost] unknown parameter [" + currentFieldName + "]");
                }
            }
        }

        if (positive == null) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_fts_boost] [positive] is required");
        }
        if (negative == null) {
            throw new ParsingException(parser.getTokenLocation(), "[lance_fts_boost] [negative] is required");
        }

        LanceFtsBoostQueryBuilder builder = new LanceFtsBoostQueryBuilder(positive, negative);
        if (negativeBoost != null) {
            builder.negativeBoost(negativeBoost);
        }
        builder.boost(boost);
        if (queryName != null) {
            builder.queryName(queryName);
        }
        return builder;
    }

    @Override
    public FullTextQuery toLanceFullTextQuery(QueryShardContext context) {
        if (!(positive instanceof LanceFtsQueryBuilder pos)) {
            throw new IllegalArgumentException(
                "[lance_fts_boost] [positive] must be a Lance FTS query (lance_match, lance_match_phrase,"
                    + " lance_multi_match, lance_fts_boost, or lance_fts_bool); got ["
                    + positive.getName()
                    + "]"
            );
        }
        if (!(negative instanceof LanceFtsQueryBuilder neg)) {
            throw new IllegalArgumentException(
                "[lance_fts_boost] [negative] must be a Lance FTS query (lance_match, lance_match_phrase,"
                    + " lance_multi_match, lance_fts_boost, or lance_fts_bool); got ["
                    + negative.getName()
                    + "]"
            );
        }
        FullTextQuery positiveFtq = pos.toLanceFullTextQuery(context);
        FullTextQuery negativeFtq = neg.toLanceFullTextQuery(context);
        return negativeBoost != null
            ? FullTextQuery.boost(positiveFtq, negativeFtq, negativeBoost)
            : FullTextQuery.boost(positiveFtq, negativeFtq);
    }

    @Override
    public Set<String> referencedFields() {
        Set<String> fields = new java.util.LinkedHashSet<>();
        if (positive instanceof LanceFtsQueryBuilder pos) {
            fields.addAll(pos.referencedFields());
        }
        if (negative instanceof LanceFtsQueryBuilder neg) {
            fields.addAll(neg.referencedFields());
        }
        return fields;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        FullTextQuery ftq = toLanceFullTextQuery(context);
        Set<String> columns = LanceFtsQuery.collectColumns(ftq);
        return new LanceFtsQuery(ftq, columns);
    }

    @Override
    protected boolean doEquals(LanceFtsBoostQueryBuilder other) {
        return positive.equals(other.positive) && negative.equals(other.negative) && Objects.equals(negativeBoost, other.negativeBoost);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(positive, negative, negativeBoost);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
