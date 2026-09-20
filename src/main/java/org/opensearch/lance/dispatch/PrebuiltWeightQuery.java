/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * Query that stands in for a {@link Weight} the executor has already
 * created, so the Weight can take part in a Lucene query tree.
 *
 * <p>{@code post_filter} makes the hits query a {@code BooleanQuery}
 * of the top-level query and the filter. Lucene's {@code BooleanWeight}
 * creates its clause Weights itself through
 * {@link IndexSearcher#createWeight}, so the Lance Weight the executor
 * built up front (and already ran the shard scan for) would be
 * replaced by a fresh one, and the scan would run again for the hits
 * and once more for the count. Putting this query in the clause's
 * place hands the prebuilt Weight back from {@link #createWeight}, so
 * every phase of the request drives the one scan.
 *
 * <p>{@link #createWeight} checks what the parent asks for against
 * what the prebuilt Weight can give. The boost must be {@code 1f}: the
 * Weight already carries the boost it was created with and cannot
 * apply another, so a parent that pushes a boost down (a
 * {@code BoostQuery} around this query) fails instead of silently
 * dropping it. The score mode must not need more than the Weight
 * produces: a Weight created with a scoring mode serves a parent that
 * needs scores and one that does not (a no-scores parent simply never
 * calls {@code score()}), a Weight created without scores cannot serve
 * a parent that needs them. Both the hits page ({@code TOP_SCORES} or
 * the sort collector's mode) and {@code IndexSearcher.count}
 * ({@code COMPLETE_NO_SCORES}) therefore reuse the executor's
 * {@link ScoreMode#COMPLETE} Weight. Identity is the Weight's
 * identity: the query is built once per request and never cached (the
 * fragment searcher disables the query cache).
 */
final class PrebuiltWeightQuery extends Query {

    private final Weight weight;
    private final ScoreMode createdWith;

    /**
     * @param weight      the Weight to hand back from {@link #createWeight}
     * @param createdWith the score mode {@code weight} was created with
     */
    PrebuiltWeightQuery(Weight weight, ScoreMode createdWith) {
        this.weight = weight;
        this.createdWith = createdWith;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        if (boost != 1f) {
            throw new IllegalArgumentException(
                "prebuilt Weight for " + weight.getQuery() + " cannot take boost " + boost + "; it already carries its own boost"
            );
        }
        if (scoreMode.needsScores() && !createdWith.needsScores()) {
            throw new IllegalArgumentException(
                "prebuilt Weight for " + weight.getQuery() + " was created with " + createdWith + " and cannot serve " + scoreMode
            );
        }
        return weight;
    }

    @Override
    public String toString(String field) {
        return weight.getQuery().toString(field);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && ((PrebuiltWeightQuery) other).weight == weight;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(weight);
    }
}
