/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The admission decision for unbounded full-text scans, the shape
 * classification the fragment executor gates on, and the 429 the gate
 * answers with.
 */
public class FtsAdmissionTests extends OpenSearchTestCase {

    private static final long GB = 1L << 30;

    @Override
    public void tearDown() throws Exception {
        FtsAdmission.resetForTests();
        super.tearDown();
    }

    public void testFittingIndexIsAdmittedWithEstimateZero() {
        // 1M rows is 52 MB, within an 8 GiB shard share, so the cached
        // entry is not rebuilt per scan and free memory is not judged.
        FtsAdmission.Decision decision = FtsAdmission.decide(1_000_000L, 8 * GB, 0L, true, 8 * GB);
        assertTrue(decision.admitted());
        assertEquals(0L, decision.estimateBytes());
    }

    public void testNonFittingIndexIsAdmittedWhenFreeMemoryHoldsTheEstimate() {
        long rows = 1_000_000_000L;
        long estimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows);
        FtsAdmission.Decision decision = FtsAdmission.decide(rows, 8 * GB, estimate + 16 * GB, true, 8 * GB);
        assertTrue(decision.admitted());
        assertEquals(estimate, decision.estimateBytes());
        assertEquals(estimate + 8 * GB, decision.availableBytes());
    }

    public void testNonFittingIndexIsRejectedWhenFreeMemoryIsSmall() {
        long rows = 1_000_000_000L;
        FtsAdmission.Decision decision = FtsAdmission.decide(rows, 8 * GB, 16 * GB, true, 8 * GB);
        assertFalse(decision.admitted());
        assertEquals(NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows), decision.estimateBytes());
        assertEquals(8 * GB, decision.availableBytes());
    }

    public void testDisabledGateAdmitsRegardless() {
        FtsAdmission.Decision decision = FtsAdmission.decide(1_000_000_000L, 8 * GB, 0L, false, 8 * GB);
        assertTrue(decision.admitted());
        assertTrue(decision.estimateBytes() > 0L);
    }

    public void testHeadroomEqualToFreeMemoryRejectsANonZeroEstimate() {
        // available is exactly zero: nothing is left for the rebuild.
        FtsAdmission.Decision decision = FtsAdmission.decide(1_000_000_000L, 8 * GB, 8 * GB, true, 8 * GB);
        assertFalse(decision.admitted());
        assertEquals(0L, decision.availableBytes());
    }

    public void testHeadroomAboveFreeMemoryStillAdmitsAFittingIndex() {
        FtsAdmission.Decision decision = FtsAdmission.decide(1_000L, 8 * GB, 0L, true, 8 * GB);
        assertTrue(decision.admitted());
        assertEquals(0L, decision.estimateBytes());
        assertTrue(decision.availableBytes() < 0L);
    }

    public void testRejectionNamesTheIndexTheEstimateAndTheSettings() {
        CircuitBreakingException rejection = FtsAdmission.rejection("perf1b", 48 * GB, 8 * GB, -2 * GB, 8 * GB);
        assertEquals(CircuitBreaker.Durability.TRANSIENT, rejection.getDurability());
        assertEquals(48 * GB, rejection.getBytesWanted());
        String message = rejection.getMessage();
        assertTrue(message, message.startsWith("[" + FtsAdmission.LABEL + "] unbounded full text scan over [perf1b]"));
        assertTrue(message, message.contains("[48gb]"));
        assertTrue(message, message.contains("[8gb]"));
        assertTrue(message, message.contains("[0b] available"));
        assertTrue(message, message.contains("lance.fts.admission.headroom"));
        assertTrue(message, message.contains("lance.fts.admission.enabled"));
    }

    public void testAdmitRecordsTheEstimateAndCountsARejection() {
        // Shard share of one byte makes the fixture's estimate count in
        // full, and a headroom of half of Long.MAX_VALUE makes the
        // available memory negative on any host.
        FtsAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        FtsAdmission.setHeadroom(new ByteSizeValue(Long.MAX_VALUE / 2, ByteSizeUnit.BYTES));
        long before = FtsAdmission.rejections();
        CircuitBreakingException rejection = expectThrows(CircuitBreakingException.class, () -> FtsAdmission.admit("demo", 1_000L));
        assertTrue(rejection.getMessage(), rejection.getMessage().contains(FtsAdmission.LABEL));
        assertEquals(before + 1, FtsAdmission.rejections());
        assertEquals(NativeMemoryLimit.invertedIndexEntryEstimateBytes(1_000L), FtsAdmission.lastEstimateBytes());

        FtsAdmission.setEnabled(false);
        FtsAdmission.admit("demo", 1_000L);
        assertEquals("a disabled gate admits and does not count", before + 1, FtsAdmission.rejections());
    }

    public void testQueriesWithoutAFullTextClauseAreNotGated() {
        assertFalse(FtsAdmission.runsUnboundedFtsScan(MatchAllDocsQuery.INSTANCE, true));
    }

    public void testBareUnboundedFtsQueryIsGated() {
        assertTrue(FtsAdmission.runsUnboundedFtsScan(new LanceFtsQuery("body", "hello"), false));
    }

    public void testBoundedTopKPageIsNotGated() {
        assertFalse(FtsAdmission.runsUnboundedFtsScan(new LanceFtsQuery("body", "hello").withScanLimit(10), false));
    }

    public void testBoundedPageWithAnExactCountIsGated() {
        // track_total_hits: true runs the unbounded count-only scan
        // when the bounded page's scan filled its limit.
        assertTrue(FtsAdmission.runsUnboundedFtsScan(new LanceFtsQuery("body", "hello").withScanLimit(10), true));
    }

    public void testFtsClauseNestedInABoolIsGated() {
        // A nested clause keeps the unbounded sentinel whatever the
        // request's size, including under must_not, which the default
        // QueryVisitor would skip.
        BooleanClause.Occur occur = randomFrom(BooleanClause.Occur.MUST, BooleanClause.Occur.SHOULD, BooleanClause.Occur.MUST_NOT);
        BooleanQuery bool = new BooleanQuery.Builder().add(MatchAllDocsQuery.INSTANCE, BooleanClause.Occur.MUST)
            .add(new LanceFtsQuery("body", "hello"), occur)
            .build();
        assertTrue(occur.toString(), FtsAdmission.runsUnboundedFtsScan(bool, false));
    }

    public void testBoostWrappedBoundedFtsQueryIsNotGated() {
        BoostQuery boosted = new BoostQuery(new LanceFtsQuery("body", "hello").withScanLimit(10), 2f);
        assertFalse(FtsAdmission.runsUnboundedFtsScan(boosted, false));
    }
}
