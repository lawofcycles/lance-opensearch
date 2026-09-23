/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.opensearch.lance.plan.traits.TieStability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The top-k of a hits page over a Lance backed index: the sort order
 * the page is cut by, how many rows the executor keeps ({@code fetch},
 * the per-node page the coordinator asked for) and the
 * {@code search_after} cursor of a continuation page. The input is the
 * request's query root (the scan, a {@code Filter} over it, or a
 * {@link LanceFtsMatch} / {@link LanceKnnSearch} node); the pushdown
 * rule folds the node into the scan as a
 * {@link PushedOperation.PushedTopK} when the Lance dataset scan can
 * return the page already ordered and cut.
 *
 * <p>The collations index the input row type. An empty list means the
 * page is ordered by score (or by row address when nothing scores),
 * the order the request without a {@code sort} clause gets. A score
 * sort spells as a collation on the input's {@code _score} column
 * (descending, from {@link LanceFtsMatch}) or {@code _distance} column
 * (ascending, from {@link LanceKnnSearch}). {@code offset} counts rows
 * skipped before the page starts and stays 0 today: the coordinator
 * folds {@code from} into the per-node size, and {@code search_after}
 * travels as the cursor values, not as a skip count.
 *
 * <p>The row type is the input's: the node reorders and cuts rows,
 * it does not reshape them ({@link LanceHitShape} does that).
 */
public final class LanceTopK extends SingleRel {

    private final List<RelFieldCollation> collations;
    private final int fetch;
    private final int offset;
    private final List<Object> searchAfter;

    /**
     * @param collations sort order over the input row type, outermost
     *     first; empty for a score / row address ordered page
     * @param fetch rows the page keeps, at least 1
     * @param offset rows skipped before the page, 0 today
     * @param searchAfter the request's {@code search_after} cursor
     *     values, parallel to {@code collations}; null when the
     *     request carries none
     */
    public LanceTopK(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<RelFieldCollation> collations,
        int fetch,
        int offset,
        List<Object> searchAfter
    ) {
        super(cluster, traitSet, input);
        this.collations = List.copyOf(Objects.requireNonNull(collations, "collations"));
        if (fetch < 1) {
            throw new IllegalArgumentException("fetch must be at least 1, got " + fetch);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative, got " + offset);
        }
        this.fetch = fetch;
        this.offset = offset;
        // Not List.copyOf: a search_after array may carry a JSON null,
        // which the rule answers by keeping the shape on Lucene.
        this.searchAfter = searchAfter == null ? null : Collections.unmodifiableList(new ArrayList<>(searchAfter));
    }

    /** The sort order over the input row type; empty for a score / row address ordered page. */
    public List<RelFieldCollation> collations() {
        return collations;
    }

    /** Rows the page keeps. */
    public int fetch() {
        return fetch;
    }

    /** Rows skipped before the page; 0 today. */
    public int offset() {
        return offset;
    }

    /** The {@code search_after} cursor values, or null when the request carries none. */
    public List<Object> searchAfter() {
        return searchAfter;
    }

    /** The same node with {@code newCollations} in place of the current sort order. */
    public LanceTopK withCollations(List<RelFieldCollation> newCollations) {
        return new LanceTopK(getCluster(), getTraitSet(), getInput(), newCollations, fetch, offset, searchAfter);
    }

    /**
     * The {@link TieStability} either physical form of this page
     * declares, read from the collations over the input row type. A
     * page whose last collation is a stored column is
     * {@link TieStability#STABLE_KEY}: the pushed scan orders by the
     * column through a Lance ordering and Lucene's collector through
     * the same column's doc values, and both resolve ties the same way
     * on every call, so a cursor typed against the column continues
     * from the same rows. A page ordered by score alone ({@code _score}
     * descending or {@code _distance} ascending, or no collation over a
     * full text / knn input) is {@link TieStability#UNSTABLE}: equal
     * scores land in whatever order the scanner's batches arrive. A
     * page with no collation over a scalar input is
     * {@link TieStability#STABLE_ROWADDR}, the scan's own row address
     * order. The value is a property of the page, not of the form that
     * cuts it, so {@code LanceTableScan.withPushedTopK} and
     * {@code HeapTopKExec} both read it from here.
     */
    public TieStability tieStability() {
        List<String> fieldNames = getRowType().getFieldNames();
        if (collations.isEmpty()) {
            boolean scored = fieldNames.contains(LanceFtsMatch.SCORE_FIELD) || fieldNames.contains(LanceKnnSearch.DISTANCE_FIELD);
            return scored ? TieStability.UNSTABLE : TieStability.STABLE_ROWADDR;
        }
        String last = fieldNames.get(collations.get(collations.size() - 1).getFieldIndex());
        boolean scoreOrdered = LanceFtsMatch.SCORE_FIELD.equals(last) || LanceKnnSearch.DISTANCE_FIELD.equals(last);
        return scoreOrdered ? TieStability.UNSTABLE : TieStability.STABLE_KEY;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LanceTopK(getCluster(), traitSet, sole(inputs), collations, fetch, offset, searchAfter);
    }

    /** At most {@code fetch + offset} rows pass, fewer when the input has fewer. */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return Math.min((double) fetch + offset, mq.getRowCount(getInput()));
    }

    /** Prints the collations, the page bounds and the cursor, so the digest and the explain output carry them. */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("collations", collations)
            .item("fetch", fetch)
            .item("offset", offset)
            .itemIf("searchAfter", searchAfter, searchAfter != null);
    }
}
