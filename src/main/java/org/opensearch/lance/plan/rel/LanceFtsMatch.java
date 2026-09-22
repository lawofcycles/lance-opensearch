/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Full-text match over a Lance table: the logical form of one Lance
 * FTS clause ({@code lance_match}, {@code lance_match_phrase},
 * {@code lance_multi_match}, {@code lance_fts_bool},
 * {@code lance_fts_boost}) at the top of a request's {@code query}
 * clause. The input is the index's scan, or a {@code Filter} over it
 * carrying the scalar {@code filter} / {@code must_not} clauses of the
 * enclosing {@code bool}; the fuse rule folds both shapes into the
 * scan as a pushed operation so the Lance dataset scan runs the
 * inverted-index lookup (with the filter as a Lance prefilter when it
 * prints as SQL).
 *
 * <p>The node carries the parsed DSL builder itself rather than a
 * parallel parameter record: the builder already holds every FTS
 * parameter (operator, fuzziness, slop, per-field boosts, negative
 * boost, clause boost), prints them all as its JSON, and the executor
 * reuses its mapping validation and {@code FullTextQuery} construction
 * when it builds the Lucene-side query for the pushed scan. The row
 * type is the input's plus the {@code _score} column the Lance FTS
 * scan returns.
 */
public final class LanceFtsMatch extends SingleRel {

    /** Name of the BM25 score column the FTS scan adds to the row type. */
    public static final String SCORE_FIELD = "_score";

    /** Which FTS shape the clause is; one value per Lance FTS DSL query. */
    public enum Kind {
        MATCH,
        MATCH_PHRASE,
        MULTI_MATCH,
        FTS_BOOL,
        FTS_BOOST
    }

    private final Kind kind;
    private final List<String> columns;
    private final QueryBuilder ftsClause;
    private final String queryJson;

    /**
     * @param kind the FTS shape of {@code ftsClause}
     * @param columns the Lance columns the clause references, in
     *     declaration order
     * @param ftsClause the parsed DSL builder; implements
     *     {@code LanceFtsQueryBuilder} and prints every parameter as
     *     its JSON
     */
    public LanceFtsMatch(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        Kind kind,
        List<String> columns,
        QueryBuilder ftsClause
    ) {
        super(cluster, traitSet, input);
        this.kind = Objects.requireNonNull(kind, "kind");
        this.columns = List.copyOf(Objects.requireNonNull(columns, "columns"));
        this.ftsClause = Objects.requireNonNull(ftsClause, "ftsClause");
        this.queryJson = compactJson(ftsClause);
    }

    public Kind kind() {
        return kind;
    }

    /** The Lance columns the clause references. */
    public List<String> columns() {
        return columns;
    }

    /** The parsed DSL builder the executor turns into the Lance {@code FullTextQuery}. */
    public QueryBuilder ftsClause() {
        return ftsClause;
    }

    /** The clause as one-line JSON: every FTS parameter, stable across instances. */
    public String queryJson() {
        return queryJson;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LanceFtsMatch(getCluster(), traitSet, sole(inputs), kind, columns, ftsClause);
    }

    /** The input's row type plus the {@link #SCORE_FIELD} column the scan returns. */
    @Override
    protected RelDataType deriveRowType() {
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.addAll(getInput().getRowType().getFieldList());
        builder.add(SCORE_FIELD, typeFactory.createSqlType(SqlTypeName.REAL));
        return builder.build();
    }

    /** Prints every parameter, so the digest and the explain output carry them. */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("kind", kind).item("columns", columns).item("query", queryJson);
    }

    /**
     * One-line JSON of the clause. {@code QueryBuilder.toString} pretty
     * prints with newlines, which would break the one-node-per-line
     * plan text, so the JSON is rendered compact here.
     */
    static String compactJson(QueryBuilder builder) {
        try {
            XContentBuilder xContent = XContentFactory.jsonBuilder();
            builder.toXContent(xContent, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(xContent).utf8ToString();
        } catch (IOException e) {
            throw new IllegalStateException("cannot render the query clause as JSON", e);
        }
    }
}
