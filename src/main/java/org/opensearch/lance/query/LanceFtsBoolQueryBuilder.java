/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
 * DSL query {@code lance_fts_bool}: composes Lance FTS clauses using
 * {@code must} / {@code should} / {@code must_not} lists that map onto
 * Lance's native {@link FullTextQuery#booleanQuery} with
 * {@link FullTextQuery.Occur} labels. BM25 scoring composes on Lance's
 * side across all clauses.
 *
 * <pre>{@code
 * {
 *   "lance_fts_bool": {
 *     "must": [
 *       { "lance_match": { "field": "body", "query": "hello" } }
 *     ],
 *     "should": [
 *       { "lance_match": { "field": "title", "query": "sunny" } }
 *     ],
 *     "must_not": [
 *       { "lance_match": { "field": "body", "query": "stale" } }
 *     ],
 *     "boost": 1.0,
 *     "_name": "bool_hit"
 *   }
 * }
 * }</pre>
 *
 * <p>Every clause must itself be a Lance FTS DSL (any builder
 * implementing {@link LanceFtsQueryBuilder}). A non-Lance-FTS clause
 * returns 400 because Lance's boolean engine only accepts
 * {@code FullTextQuery} subclauses; wrapping such a clause into a
 * Lucene bool would silently break score composition and skip Lance's
 * native execution.
 *
 * <p>At least one clause across the three lists is required — Lance's
 * {@code booleanQuery} constructor rejects an empty clause list, so we
 * catch that at parse time.
 */
public class LanceFtsBoolQueryBuilder extends AbstractQueryBuilder<LanceFtsBoolQueryBuilder> implements LanceFtsQueryBuilder {

    public static final String NAME = "lance_fts_bool";

    private final List<QueryBuilder> mustClauses = new ArrayList<>();
    private final List<QueryBuilder> shouldClauses = new ArrayList<>();
    private final List<QueryBuilder> mustNotClauses = new ArrayList<>();

    public LanceFtsBoolQueryBuilder() {}

    public LanceFtsBoolQueryBuilder(StreamInput in) throws IOException {
        super(in);
        int mustCount = in.readVInt();
        for (int i = 0; i < mustCount; i++) {
            mustClauses.add(in.readNamedWriteable(QueryBuilder.class));
        }
        int shouldCount = in.readVInt();
        for (int i = 0; i < shouldCount; i++) {
            shouldClauses.add(in.readNamedWriteable(QueryBuilder.class));
        }
        int mustNotCount = in.readVInt();
        for (int i = 0; i < mustNotCount; i++) {
            mustNotClauses.add(in.readNamedWriteable(QueryBuilder.class));
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(mustClauses.size());
        for (QueryBuilder clause : mustClauses) {
            out.writeNamedWriteable(clause);
        }
        out.writeVInt(shouldClauses.size());
        for (QueryBuilder clause : shouldClauses) {
            out.writeNamedWriteable(clause);
        }
        out.writeVInt(mustNotClauses.size());
        for (QueryBuilder clause : mustNotClauses) {
            out.writeNamedWriteable(clause);
        }
    }

    public LanceFtsBoolQueryBuilder must(QueryBuilder clause) {
        mustClauses.add(Objects.requireNonNull(clause, "must clause must not be null"));
        return this;
    }

    public LanceFtsBoolQueryBuilder should(QueryBuilder clause) {
        shouldClauses.add(Objects.requireNonNull(clause, "should clause must not be null"));
        return this;
    }

    public LanceFtsBoolQueryBuilder mustNot(QueryBuilder clause) {
        mustNotClauses.add(Objects.requireNonNull(clause, "must_not clause must not be null"));
        return this;
    }

    List<QueryBuilder> mustClauses() {
        return Collections.unmodifiableList(mustClauses);
    }

    List<QueryBuilder> shouldClauses() {
        return Collections.unmodifiableList(shouldClauses);
    }

    List<QueryBuilder> mustNotClauses() {
        return Collections.unmodifiableList(mustNotClauses);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        writeClauseArray(builder, params, "must", mustClauses);
        writeClauseArray(builder, params, "should", shouldClauses);
        writeClauseArray(builder, params, "must_not", mustNotClauses);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    private static void writeClauseArray(XContentBuilder builder, Params params, String field, List<QueryBuilder> clauses)
        throws IOException {
        if (clauses.isEmpty()) {
            return;
        }
        builder.startArray(field);
        for (QueryBuilder clause : clauses) {
            clause.toXContent(builder, params);
        }
        builder.endArray();
    }

    public static LanceFtsBoolQueryBuilder fromXContent(XContentParser parser) throws IOException {
        LanceFtsBoolQueryBuilder builder = new LanceFtsBoolQueryBuilder();
        float boost = DEFAULT_BOOST;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("must".equals(currentFieldName)) {
                    parseClauseArray(parser, builder::must);
                } else if ("should".equals(currentFieldName)) {
                    parseClauseArray(parser, builder::should);
                } else if ("must_not".equals(currentFieldName)) {
                    parseClauseArray(parser, builder::mustNot);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[lance_fts_bool] unknown array parameter [" + currentFieldName + "]"
                    );
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                // Accept the single-clause shortcut ({"must": {...}}) that
                // stock bool also accepts, so callers can drop the array
                // brackets when there is exactly one clause.
                if ("must".equals(currentFieldName)) {
                    builder.must(parseInnerQueryBuilder(parser));
                } else if ("should".equals(currentFieldName)) {
                    builder.should(parseInnerQueryBuilder(parser));
                } else if ("must_not".equals(currentFieldName)) {
                    builder.mustNot(parseInnerQueryBuilder(parser));
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[lance_fts_bool] unknown object parameter [" + currentFieldName + "]"
                    );
                }
            } else if (token.isValue()) {
                if (BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[lance_fts_bool] unknown parameter [" + currentFieldName + "]");
                }
            }
        }

        if (builder.mustClauses.isEmpty() && builder.shouldClauses.isEmpty() && builder.mustNotClauses.isEmpty()) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "[lance_fts_bool] requires at least one clause in must, should, or must_not"
            );
        }
        builder.boost(boost);
        if (queryName != null) {
            builder.queryName(queryName);
        }
        return builder;
    }

    private static void parseClauseArray(XContentParser parser, java.util.function.Consumer<QueryBuilder> sink) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[lance_fts_bool] clause entries must be objects, saw [" + token + "]"
                );
            }
            sink.accept(parseInnerQueryBuilder(parser));
        }
    }

    @Override
    public FullTextQuery toLanceFullTextQuery(QueryShardContext context) {
        List<FullTextQuery.BooleanClause> clauses = new ArrayList<>(mustClauses.size() + shouldClauses.size() + mustNotClauses.size());
        collectClauses(context, mustClauses, FullTextQuery.Occur.MUST, "must", clauses);
        collectClauses(context, shouldClauses, FullTextQuery.Occur.SHOULD, "should", clauses);
        collectClauses(context, mustNotClauses, FullTextQuery.Occur.MUST_NOT, "must_not", clauses);
        if (clauses.isEmpty()) {
            // Structurally rejected at parse time, but the runtime guard
            // keeps the plugin honest against directly-constructed builders.
            throw new IllegalArgumentException("[lance_fts_bool] requires at least one clause");
        }
        return FullTextQuery.booleanQuery(clauses);
    }

    private static void collectClauses(
        QueryShardContext context,
        List<QueryBuilder> raw,
        FullTextQuery.Occur occur,
        String occurName,
        List<FullTextQuery.BooleanClause> out
    ) {
        for (QueryBuilder clause : raw) {
            if (!(clause instanceof LanceFtsQueryBuilder lanceClause)) {
                throw new IllegalArgumentException(
                    "[lance_fts_bool] ["
                        + occurName
                        + "] clauses must be Lance FTS queries (lance_match, lance_match_phrase,"
                        + " lance_multi_match, lance_fts_boost, or lance_fts_bool); got ["
                        + clause.getName()
                        + "]"
                );
            }
            FullTextQuery ftq = lanceClause.toLanceFullTextQuery(context);
            // The clause's own boost has no Lucene BoostQuery to ride on
            // inside a Lance tree; fold it into the Lance factor.
            out.add(new FullTextQuery.BooleanClause(occur, LanceFtsQueryBuilder.boosted(ftq, clause.boost(), NAME)));
        }
    }

    @Override
    public Set<String> referencedFields() {
        Set<String> fields = new java.util.LinkedHashSet<>();
        for (List<QueryBuilder> clauses : List.of(mustClauses, shouldClauses, mustNotClauses)) {
            for (QueryBuilder clause : clauses) {
                if (clause instanceof LanceFtsQueryBuilder lanceClause) {
                    fields.addAll(lanceClause.referencedFields());
                }
            }
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
    protected boolean doEquals(LanceFtsBoolQueryBuilder other) {
        return mustClauses.equals(other.mustClauses)
            && shouldClauses.equals(other.shouldClauses)
            && mustNotClauses.equals(other.mustNotClauses);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(mustClauses, shouldClauses, mustNotClauses);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
