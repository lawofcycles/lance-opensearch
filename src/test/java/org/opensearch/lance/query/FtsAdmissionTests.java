/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.List;

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
 * The admission decision for full-text scans, the shape
 * classification the fragment executor gates on, and the 429 the gate
 * answers with.
 */
public class FtsAdmissionTests extends OpenSearchTestCase {

    private static final long GB = 1L << 30;

    private static final FtsAdmission.Shape UNBOUNDED = new FtsAdmission.Shape(true, true, 0L);

    @Override
    public void tearDown() throws Exception {
        FtsAdmission.resetForTests();
        super.tearDown();
    }

    public void testFittingIndexIsAdmittedWithEstimateZero() {
        // 1M rows is 52 MB, within an 8 GiB shard share, so the cached
        // entry is not rebuilt per scan and free memory is not judged.
        FtsAdmission.Decision decision = FtsAdmission.decide(1_000_000L, 0L, 8 * GB, 0L, true, 8 * GB);
        assertTrue(decision.admitted());
        assertEquals(0L, decision.estimateBytes());
    }

    public void testFittingIndexZeroesTheScanBufferTooBecauseNothingIsRebuilt() {
        FtsAdmission.Decision decision = FtsAdmission.decide(1_000_000L, 64 * GB, 8 * GB, 0L, true, 8 * GB);
        assertTrue(decision.admitted());
        assertEquals(0L, decision.estimateBytes());
    }

    public void testNonFittingIndexIsAdmittedWhenFreeMemoryHoldsTheEstimate() {
        long rows = 1_000_000_000L;
        long estimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows);
        FtsAdmission.Decision decision = FtsAdmission.decide(rows, 0L, 8 * GB, estimate + 16 * GB, true, 8 * GB);
        assertTrue(decision.admitted());
        assertEquals(estimate, decision.estimateBytes());
        assertEquals(estimate + 8 * GB, decision.availableBytes());
    }

    public void testNonFittingIndexIsRejectedWhenFreeMemoryIsSmall() {
        long rows = 1_000_000_000L;
        FtsAdmission.Decision decision = FtsAdmission.decide(rows, 0L, 8 * GB, 16 * GB, true, 8 * GB);
        assertFalse(decision.admitted());
        assertEquals(NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows), decision.estimateBytes());
        assertEquals(8 * GB, decision.availableBytes());
    }

    public void testScanBufferTipsAnOtherwiseFittingEstimateOverFreeMemory() {
        long rows = 1_000_000_000L;
        long entry = NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows);
        // Free memory holds the document set alone but not the scan
        // buffers on top of it.
        long free = entry + 9 * GB;
        FtsAdmission.Decision withoutBuffer = FtsAdmission.decide(rows, 0L, 8 * GB, free, true, 8 * GB);
        assertTrue(withoutBuffer.admitted());
        FtsAdmission.Decision withBuffer = FtsAdmission.decide(rows, 2 * GB, 8 * GB, free, true, 8 * GB);
        assertFalse(withBuffer.admitted());
        assertEquals(entry + 2 * GB, withBuffer.estimateBytes());
    }

    public void testDisabledGateAdmitsRegardless() {
        FtsAdmission.Decision decision = FtsAdmission.decide(1_000_000_000L, 0L, 8 * GB, 0L, false, 8 * GB);
        assertTrue(decision.admitted());
        assertTrue(decision.estimateBytes() > 0L);
    }

    public void testHeadroomEqualToFreeMemoryRejectsANonZeroEstimate() {
        // available is exactly zero: nothing is left for the rebuild.
        FtsAdmission.Decision decision = FtsAdmission.decide(1_000_000_000L, 0L, 8 * GB, 8 * GB, true, 8 * GB);
        assertFalse(decision.admitted());
        assertEquals(0L, decision.availableBytes());
    }

    public void testHeadroomAboveFreeMemoryStillAdmitsAFittingIndex() {
        FtsAdmission.Decision decision = FtsAdmission.decide(1_000L, 0L, 8 * GB, 0L, true, 8 * GB);
        assertTrue(decision.admitted());
        assertEquals(0L, decision.estimateBytes());
        assertTrue(decision.availableBytes() < 0L);
    }

    public void testScanBufferEstimateOfAnUnboundedShapeGrowsWithTheTable() {
        // One row in ten assumed matched, 12 bytes per returned row,
        // doubled for the batches held at once.
        long expected = (long) ((long) (1_000_000_000L * 0.1) * 12L * 2.0);
        assertEquals(expected, FtsAdmission.scanBufferEstimateBytes(1_000_000_000L, UNBOUNDED));
    }

    public void testScanBufferEstimateOfABoundedPageUsesTheTopKLimit() {
        FtsAdmission.Shape bounded = new FtsAdmission.Shape(true, false, 10L);
        assertEquals((long) (10L * 12L * 2.0), FtsAdmission.scanBufferEstimateBytes(1_000_000_000L, bounded));
    }

    public void testRejectionNamesTheIndexTheEstimateAndTheSettings() {
        CircuitBreakingException rejection = FtsAdmission.rejection("perf1b", true, 48 * GB, 8 * GB, -2 * GB, 8 * GB);
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

    public void testBoundedRejectionNamesTheBoundedGatingSetting() {
        CircuitBreakingException rejection = FtsAdmission.rejection("perf1b", false, 48 * GB, 8 * GB, -2 * GB, 8 * GB);
        String message = rejection.getMessage();
        assertTrue(message, message.startsWith("[" + FtsAdmission.LABEL + "] bounded full text page over [perf1b]"));
        assertTrue(message, message.contains("lance.fts.admission.bounded_shapes_gated"));
    }

    public void testAdmitRecordsTheEstimateAndCountsARejection() {
        // Shard share of one byte makes the fixture's estimate count in
        // full, and a headroom of half of Long.MAX_VALUE makes the
        // available memory negative on any host.
        FtsAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        FtsAdmission.setHeadroom(new ByteSizeValue(Long.MAX_VALUE / 2, ByteSizeUnit.BYTES));
        long before = FtsAdmission.rejections();
        CircuitBreakingException rejection = expectThrows(
            CircuitBreakingException.class,
            () -> FtsAdmission.admit("demo", 1_000L, UNBOUNDED)
        );
        assertTrue(rejection.getMessage(), rejection.getMessage().contains(FtsAdmission.LABEL));
        assertEquals(before + 1, FtsAdmission.rejections());
        long expectedEstimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(1_000L) + FtsAdmission.scanBufferEstimateBytes(
            1_000L,
            UNBOUNDED
        );
        assertEquals(expectedEstimate, FtsAdmission.lastEstimateBytes());

        FtsAdmission.setEnabled(false);
        FtsAdmission.admit("demo", 1_000L, UNBOUNDED);
        assertEquals("a disabled gate admits and does not count", before + 1, FtsAdmission.rejections());
    }

    public void testAdmitJudgesABoundedPageOnTheDocumentSetItStillRebuilds() {
        FtsAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        FtsAdmission.setHeadroom(new ByteSizeValue(Long.MAX_VALUE / 2, ByteSizeUnit.BYTES));
        FtsAdmission.Shape bounded = new FtsAdmission.Shape(true, false, 10L);
        CircuitBreakingException rejection = expectThrows(
            CircuitBreakingException.class,
            () -> FtsAdmission.admit("demo", 1_000L, bounded)
        );
        assertTrue(rejection.getMessage(), rejection.getMessage().contains("bounded full text page"));
        long expectedEstimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(1_000L) + FtsAdmission.scanBufferEstimateBytes(
            1_000L,
            bounded
        );
        assertEquals(expectedEstimate, FtsAdmission.lastEstimateBytes());
    }

    public void testAdmitJudgesTheProbedAvailableMemory() {
        // A shard share of one byte makes the estimate count in full.
        FtsAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        FtsAdmission.setHeadroom(new ByteSizeValue(8, ByteSizeUnit.GB));
        long estimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(1_000L) + FtsAdmission.scanBufferEstimateBytes(1_000L, UNBOUNDED);

        // A MemAvailable-sized reading (page cache reclaimable) admits
        // the scan even though the same host's MemFree could be zero.
        FtsAdmission.setMemoryProbeForTests(() -> 8 * GB + estimate);
        FtsAdmission.admit("demo", 1_000L, UNBOUNDED);

        // One byte less and the estimate no longer fits.
        FtsAdmission.setMemoryProbeForTests(() -> 8 * GB + estimate - 1);
        CircuitBreakingException rejection = expectThrows(
            CircuitBreakingException.class,
            () -> FtsAdmission.admit("demo", 1_000L, UNBOUNDED)
        );
        assertTrue(rejection.getMessage(), rejection.getMessage().contains(FtsAdmission.LABEL));
    }

    public void testAvailableMemoryReportsTheProbeReading() {
        FtsAdmission.setMemoryProbeForTests(() -> 123L);
        assertEquals(123L, FtsAdmission.availablePhysicalMemoryBytes());
    }

    public void testParseMemAvailableConvertsTheKernelValueToBytes() {
        // A warm node: MemFree near zero while MemAvailable holds the
        // reclaimable page cache. The gate must read the latter.
        List<String> memInfo = List.of(
            "MemTotal:       263846076 kB",
            "MemFree:          1130308 kB",
            "MemAvailable:   224240304 kB",
            "Buffers:             4448 kB",
            "Cached:         219884036 kB"
        );
        assertEquals(224_240_304L * 1024L, FtsAdmission.parseMemAvailableBytes(memInfo));
    }

    public void testParseMemAvailableWithoutTheLineIsUnavailable() {
        // Kernels before 3.14 and non-Linux shims have no MemAvailable;
        // the probe then falls back to the free physical memory.
        assertEquals(-1L, FtsAdmission.parseMemAvailableBytes(List.of("MemTotal:       263846076 kB", "MemFree:  1130308 kB")));
        assertEquals(-1L, FtsAdmission.parseMemAvailableBytes(List.of()));
    }

    public void testParseMemAvailableWithAMalformedLineIsUnavailable() {
        assertEquals(-1L, FtsAdmission.parseMemAvailableBytes(List.of("MemAvailable:")));
        assertEquals(-1L, FtsAdmission.parseMemAvailableBytes(List.of("MemAvailable:   lots kB")));
    }

    public void testReadAvailablePhysicalMemoryAnswersOnEveryPlatform() {
        // /proc/meminfo on Linux, the OsProbe fallback elsewhere:
        // either way a live host reports a positive reading.
        assertTrue(FtsAdmission.readAvailablePhysicalMemory() > 0L);
    }

    public void testQueriesWithoutAFullTextClauseAreNotGated() {
        assertFalse(FtsAdmission.runsUnboundedFtsScan(MatchAllDocsQuery.INSTANCE, true));
        assertFalse(FtsAdmission.gates(FtsAdmission.classify(MatchAllDocsQuery.INSTANCE, true)));
    }

    public void testBareUnboundedFtsQueryIsGated() {
        assertTrue(FtsAdmission.runsUnboundedFtsScan(new LanceFtsQuery("body", "hello"), false));
    }

    public void testBoundedTopKPageDoesNotRunAnUnboundedScan() {
        assertFalse(FtsAdmission.runsUnboundedFtsScan(new LanceFtsQuery("body", "hello").withScanLimit(10), false));
    }

    public void testBoundedTopKPageIsClassifiedWithItsScanLimit() {
        FtsAdmission.Shape shape = FtsAdmission.classify(new LanceFtsQuery("body", "hello").withScanLimit(10), false);
        assertTrue(shape.hasFtsClause());
        assertFalse(shape.unbounded());
        assertEquals(10L, shape.boundedScanRows());
    }

    public void testBoundedTopKPageIsGatedByDefaultAndTheSettingOptsOut() {
        FtsAdmission.Shape shape = FtsAdmission.classify(new LanceFtsQuery("body", "hello").withScanLimit(10), false);
        assertTrue(FtsAdmission.gates(shape));
        FtsAdmission.setBoundedShapesGated(false);
        assertFalse(FtsAdmission.gates(shape));
        // The opt-out leaves the unbounded shapes gated.
        assertTrue(FtsAdmission.gates(UNBOUNDED));
    }

    public void testBoundedPageWithAnExactCountIsGated() {
        // track_total_hits: true runs the unbounded count-only scan
        // when the bounded page's scan filled its limit.
        assertTrue(FtsAdmission.runsUnboundedFtsScan(new LanceFtsQuery("body", "hello").withScanLimit(10), true));
    }

    public void testFtsClauseNestedInABoolIsGated() {
        // A nested clause keeps the unbounded sentinel whatever the
        // request's size, including under must_not, which the default
        // QueryVisitor would skip. Every occur is asserted so a
        // regression in one branch cannot hide behind the others.
        for (BooleanClause.Occur occur : new BooleanClause.Occur[] {
            BooleanClause.Occur.MUST,
            BooleanClause.Occur.SHOULD,
            BooleanClause.Occur.MUST_NOT }) {
            BooleanQuery bool = new BooleanQuery.Builder().add(MatchAllDocsQuery.INSTANCE, BooleanClause.Occur.MUST)
                .add(new LanceFtsQuery("body", "hello"), occur)
                .build();
            assertTrue(occur.toString(), FtsAdmission.runsUnboundedFtsScan(bool, false));
        }
    }

    public void testBoostWrappedBoundedFtsQueryIsClassifiedBounded() {
        BoostQuery boosted = new BoostQuery(new LanceFtsQuery("body", "hello").withScanLimit(10), 2f);
        assertFalse(FtsAdmission.runsUnboundedFtsScan(boosted, false));
        FtsAdmission.Shape shape = FtsAdmission.classify(boosted, false);
        assertTrue(shape.hasFtsClause());
        assertEquals(10L, shape.boundedScanRows());
    }
}
