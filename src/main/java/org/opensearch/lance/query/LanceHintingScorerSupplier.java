/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;

import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.opensearch.lance.engine.LanceFragmentLeafReader;

/**
 * {@link ScorerSupplier} of the Lance-side scorers ({@link LanceFtsQuery},
 * {@link LanceKnnQuery}) for one leaf. Hands out the prebuilt
 * {@link Scorer} and tells the leaf reader when the hit set it was
 * hinted with is the whole set of docs the search will collect there.
 *
 * <p>The Weight reports its per-leaf hit set to
 * {@link LanceFragmentLeafReader#hintMatchedOffsets} as soon as it has
 * sorted the hits, so numeric doc values can fetch the sort or
 * aggregation column for those rows only. Ordinal-based doc values
 * need more: they must know that no doc outside the hit set will be
 * requested, because their ordinal space cannot grow once a consumer
 * has read it. {@link #bulkScorer()} is where that knowledge exists.
 * Lucene calls {@code bulkScorer()} on the supplier that drives
 * collection for a leaf: {@code IndexSearcher.searchLeaf} calls it on
 * the top-level supplier, and {@code BooleanScorerSupplier} forwards
 * the call to a child only when that child is the single required
 * clause or the single optional clause of a boolean with no required
 * clauses, in both cases wrapping the result in nothing more than a
 * {@code must_not} exclusion. Every other composition (a disjunction
 * of several clauses, a conjunction of several required clauses, a
 * required clause with an optional one) obtains a {@link Scorer}
 * through {@link #get} and never calls {@code bulkScorer()}. A
 * {@code BulkScorer} obtained here therefore pushes only this
 * supplier's docs, possibly fewer, to the collector, and the leaf can
 * treat the hint as exclusive.
 *
 * <p>The exclusive mark is withheld when the leaf sits under a reader
 * wrapper the plugin does not know ({@code mayMarkExclusive} false):
 * such a wrapper can hide rows the Lance scan returned, and a keyword
 * dictionary built from every hit row would expose terms of hidden
 * rows through its value count. The hint itself is still delivered,
 * since numeric doc values read it only for the docs that are
 * actually collected.
 *
 * <p>The scorer is built once and handed out once. {@link #get} and
 * {@link #bulkScorer()} (which consumes {@link #get}) are the two ways
 * to obtain it, and Lucene calls exactly one of them per supplier; a
 * second call would hand a caller an iterator another consumer has
 * already advanced, so it fails instead.
 */
final class LanceHintingScorerSupplier extends ScorerSupplier {

    private final Scorer scorer;
    private final LanceFragmentLeafReader leaf;
    private final int[] sortedDocIds;
    private final boolean mayMarkExclusive;
    private boolean handedOut;

    /**
     * @param scorer           the scorer over {@code sortedDocIds}
     * @param leaf             the Lance leaf the scorer belongs to
     * @param sortedDocIds     ascending doc ids the scorer produces; the
     *                         same array the caller passed to
     *                         {@code hintMatchedOffsets(ids, false)}
     * @param mayMarkExclusive whether {@link #bulkScorer()} may report
     *                         the hint as exclusive; false when the
     *                         leaf is reached through a reader wrapper
     *                         other than the plugin's or OpenSearch's
     *                         own (see
     *                         {@link LanceFragmentLeafReader#wrappedOnlyByOwnReaders})
     */
    LanceHintingScorerSupplier(Scorer scorer, LanceFragmentLeafReader leaf, int[] sortedDocIds, boolean mayMarkExclusive) {
        this.scorer = scorer;
        this.leaf = leaf;
        this.sortedDocIds = sortedDocIds;
        this.mayMarkExclusive = mayMarkExclusive;
    }

    @Override
    public Scorer get(long leadCost) {
        if (handedOut) {
            throw new IllegalStateException(
                "scorer for fragment " + leaf.fragmentId() + " was already handed out; a ScorerSupplier serves one consumer"
            );
        }
        handedOut = true;
        return scorer;
    }

    @Override
    public long cost() {
        return sortedDocIds.length;
    }

    @Override
    public BulkScorer bulkScorer() throws IOException {
        if (mayMarkExclusive) {
            leaf.hintMatchedOffsets(sortedDocIds, true);
        }
        return super.bulkScorer();
    }
}
