/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import org.apache.lucene.search.TotalHits;
import org.opensearch.lance.plan.execute.MergeReducer;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link MergeReducer#totalHits} composes
 * {@code hits.total} from the summed per-node matched count, the
 * lower-bound flag any executor raised, and the request's
 * {@code track_total_hits}. The contract is the shard path's
 * {@code SearchPhaseController.TopDocsStats#getTotalHits}: whenever the
 * relation is {@code gte} the value is the bound itself.
 */
public class CoordinatorTotalHitsTests extends OpenSearchTestCase {

    private static final int BOUND = 10_000;

    public void testLowerBoundBelowTheBoundReportsTheBound() {
        // Three executors each counted their own share of a scan
        // limited to 10,001 rows, and the shares added up to 9,991
        // because Lance returned different tied rows to each of them.
        TotalHits total = MergeReducer.totalHits(9_991L, true, BOUND);

        assertEquals(BOUND, total.value());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, total.relation());
    }

    public void testLowerBoundAtOrAboveTheBoundReportsTheBound() {
        for (long sum : new long[] { 10_000L, 10_001L, 12_000L }) {
            TotalHits total = MergeReducer.totalHits(sum, true, BOUND);
            assertEquals("sum " + sum, BOUND, total.value());
            assertEquals("sum " + sum, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, total.relation());
        }
    }

    public void testExactSumAboveTheBoundReportsTheBound() {
        // Every executor counted exactly (metadata counts, or a scan
        // that saw every match) but together they exceed the bound.
        TotalHits total = MergeReducer.totalHits(12_000L, false, BOUND);

        assertEquals(BOUND, total.value());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, total.relation());
    }

    public void testExactSumWithinTheBoundIsExact() {
        for (long sum : new long[] { 0L, 500L, 10_000L }) {
            TotalHits total = MergeReducer.totalHits(sum, false, BOUND);
            assertEquals("sum " + sum, sum, total.value());
            assertEquals("sum " + sum, TotalHits.Relation.EQUAL_TO, total.relation());
        }
    }

    public void testAccurateTrackingReportsTheSumExactly() {
        TotalHits total = MergeReducer.totalHits(2_502_753L, false, SearchContext.TRACK_TOTAL_HITS_ACCURATE);

        assertEquals(2_502_753L, total.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, total.relation());
    }

    public void testAccurateTrackingWithALowerBoundKeepsTheSumAsGte() {
        // An executor must not stop counting under track_total_hits:
        // true; if one does, the sum is all the coordinator has and it
        // must not be presented as exact.
        TotalHits total = MergeReducer.totalHits(9_991L, true, SearchContext.TRACK_TOTAL_HITS_ACCURATE);

        assertEquals(9_991L, total.value());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, total.relation());
    }

    public void testDisabledTrackingOmitsTheTotal() {
        assertNull(MergeReducer.totalHits(9_991L, true, SearchContext.TRACK_TOTAL_HITS_DISABLED));
        assertNull(MergeReducer.totalHits(0L, false, SearchContext.TRACK_TOTAL_HITS_DISABLED));
    }

    public void testTerminatedEarlyIsAbsentUntilAnExecutorReportsItAndOrsTheFlags() {
        // Without terminate_after no executor reports the flag and the
        // response leaves terminated_early out; with it the flag is the
        // OR over the executors, as SearchPhaseController folds shards.
        assertNull(MergeReducer.mergeTerminatedEarly(null, null));
        assertEquals(Boolean.FALSE, MergeReducer.mergeTerminatedEarly(null, false));
        assertEquals(Boolean.TRUE, MergeReducer.mergeTerminatedEarly(null, true));
        assertEquals(Boolean.FALSE, MergeReducer.mergeTerminatedEarly(false, false));
        assertEquals(Boolean.TRUE, MergeReducer.mergeTerminatedEarly(false, true));
        assertEquals(Boolean.TRUE, MergeReducer.mergeTerminatedEarly(true, false));
        assertEquals(Boolean.TRUE, MergeReducer.mergeTerminatedEarly(true, null));
    }
}
