/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.lance.engine.LanceFragmentLeafReader;

/**
 * A {@link org.apache.lucene.search.Weight} of a Lance-side query
 * ({@link LanceFtsQuery}, {@link LanceKnnQuery}) that can tell a
 * fragment reader, ahead of collection, exactly which docs the search
 * will collect on it.
 *
 * <p>The Weights deliver their per-leaf hit set to
 * {@link LanceFragmentLeafReader#hintMatchedOffsets} from
 * {@code scorerSupplier}, and mark it exclusive once Lucene asks their
 * supplier for a {@code BulkScorer}. Both happen inside
 * {@code IndexSearcher.searchLeaf}, after the collector has obtained
 * its leaf collector. Two consumers read keyword ordinals before that
 * point: OpenSearch's {@code TermsAggregatorFactory} builds the global
 * ordinal map from every leaf's {@code SortedSetDocValues} when the
 * aggregator is created, and {@code BytesRefFieldComparatorSource}
 * reads the value count when the leaf comparator is created. Without a
 * hint at that time both load the full dictionary of every fragment.
 *
 * <p>{@link #hintExclusive} lets the fragment executor, which knows
 * from the shape of the request that this Weight alone drives
 * collection on every leaf (the top-level query is the bare Lance
 * clause, no reader wrapper is installed, and the scan is not clipped
 * to a page), run the scan and deliver the exclusive hint before it
 * builds aggregators and comparators. The scorer the Weight later
 * produces for the leaf hands the reader the same hit array, so the
 * reader keeps the sparse structures taken under the early hint.
 */
public interface LanceHintingWeight {

    /**
     * Report the docs this Weight matches on {@code context}'s leaf to
     * the leaf's {@link LanceFragmentLeafReader} as an exclusive hint
     * (see {@link LanceFragmentLeafReader#hintMatchedOffsets}). Runs
     * the Weight's Lance scan if it has not run yet. Does nothing for a
     * leaf that is not Lance-backed. When a reader wrapper other than
     * the plugin's or OpenSearch's own sits between {@code context}'s
     * reader and the fragment reader
     * ({@link LanceFragmentLeafReader#wrappedOnlyByOwnReaders} false)
     * the hint is delivered without the exclusive mark, for the reason
     * given on {@link LanceHintingScorerSupplier}.
     *
     * <p>The caller asserts that every doc the search collects on the
     * leaf is one the Weight matches: the Weight is the top-level
     * scorer, or the only scoring clause of a conjunction whose other
     * clauses can only narrow the doc set.
     */
    void hintExclusive(LeafReaderContext context) throws IOException;
}
