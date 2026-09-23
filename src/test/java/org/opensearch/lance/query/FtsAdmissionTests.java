/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.Constants;
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
        FtsAdmission.Decision decision = FtsAdmission.decide(1_000_000L, 0L, 8 * GB, 0L, true, 8 * GB, 0L);
        assertTrue(decision.admitted());
        assertEquals(0L, decision.estimateBytes());
    }

    public void testFittingIndexZeroesTheScanBufferTooBecauseNothingIsRebuilt() {
        FtsAdmission.Decision decision = FtsAdmission.decide(1_000_000L, 64 * GB, 8 * GB, 0L, true, 8 * GB, 0L);
        assertTrue(decision.admitted());
        assertEquals(0L, decision.estimateBytes());
    }

    public void testNonFittingIndexIsAdmittedWhenFreeMemoryHoldsTheEstimate() {
        long rows = 1_000_000_000L;
        long estimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows);
        FtsAdmission.Decision decision = FtsAdmission.decide(rows, 0L, 8 * GB, estimate + 16 * GB, true, 8 * GB, 0L);
        assertTrue(decision.admitted());
        assertEquals(estimate, decision.estimateBytes());
        assertEquals(estimate + 8 * GB, decision.availableBytes());
    }

    public void testNonFittingIndexIsRejectedWhenFreeMemoryIsSmall() {
        long rows = 1_000_000_000L;
        FtsAdmission.Decision decision = FtsAdmission.decide(rows, 0L, 8 * GB, 16 * GB, true, 8 * GB, 0L);
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
        FtsAdmission.Decision withoutBuffer = FtsAdmission.decide(rows, 0L, 8 * GB, free, true, 8 * GB, 0L);
        assertTrue(withoutBuffer.admitted());
        FtsAdmission.Decision withBuffer = FtsAdmission.decide(rows, 2 * GB, 8 * GB, free, true, 8 * GB, 0L);
        assertFalse(withBuffer.admitted());
        assertEquals(entry + 2 * GB, withBuffer.estimateBytes());
    }

    public void testDisabledGateAdmitsRegardless() {
        FtsAdmission.Decision decision = FtsAdmission.decide(1_000_000_000L, 0L, 8 * GB, 0L, false, 8 * GB, 0L);
        assertTrue(decision.admitted());
        assertTrue(decision.estimateBytes() > 0L);
    }

    public void testHeadroomEqualToFreeMemoryRejectsANonZeroEstimate() {
        // available is exactly zero: nothing is left for the rebuild.
        FtsAdmission.Decision decision = FtsAdmission.decide(1_000_000_000L, 0L, 8 * GB, 8 * GB, true, 8 * GB, 0L);
        assertFalse(decision.admitted());
        assertEquals(0L, decision.availableBytes());
    }

    public void testHeadroomAboveFreeMemoryStillAdmitsAFittingIndex() {
        FtsAdmission.Decision decision = FtsAdmission.decide(1_000L, 0L, 8 * GB, 0L, true, 8 * GB, 0L);
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
        CircuitBreakingException rejection = FtsAdmission.rejection("perf1b", true, 48 * GB, 8 * GB, -2 * GB, 8 * GB, 0L);
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
        CircuitBreakingException rejection = FtsAdmission.rejection("perf1b", false, 48 * GB, 8 * GB, -2 * GB, 8 * GB, 0L);
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

    // ---- the retained pool: credit, bound, decay, guard ----

    public void testRetainedCreditAdmitsWhatTheRawAvailableMemoryRefuses() {
        long rows = 1_000_000_000L;
        long estimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows);
        // After the first scan the node reads 4 GiB short of the
        // estimate once the headroom is taken; 7 GiB retained by that
        // scan closes the gap.
        long available = estimate + 8 * GB - 4 * GB;
        assertFalse(FtsAdmission.decide(rows, 0L, 8 * GB, available, true, 8 * GB, 0L).admitted());
        FtsAdmission.Decision credited = FtsAdmission.decide(rows, 0L, 8 * GB, available, true, 8 * GB, 7 * GB);
        assertTrue(credited.admitted());
        assertEquals(7 * GB, credited.retainedCreditBytes());
        assertEquals("the available figure stays the raw one after the headroom", available - 8 * GB, credited.availableBytes());
        // One byte short of the gap and the credit does not admit.
        assertFalse(FtsAdmission.decide(rows, 0L, 8 * GB, available, true, 8 * GB, 4 * GB - 1).admitted());
        // A negative credit counts as none.
        assertEquals(0L, FtsAdmission.decide(rows, 0L, 8 * GB, available, true, 8 * GB, -1L).retainedCreditBytes());
    }

    public void testPoolRecordsWhatOneScanLeftBehind() {
        FtsAdmission.RetainedPool pool = new FtsAdmission.RetainedPool();
        assertEquals(0L, pool.creditBytes(80 * GB));
        pool.scanAdmitted(80 * GB, -1L, 50 * GB);
        assertTrue(pool.awaitingCompletion());
        assertEquals("nothing is credited before the completion sample", 0L, pool.creditBytes(30 * GB));
        pool.scanCompleted(73 * GB, -1L);
        assertFalse(pool.awaitingCompletion());
        assertEquals(7 * GB, pool.retainedBytes());
        assertEquals(7 * GB, pool.creditBytes(73 * GB));
    }

    public void testPoolAccumulatesOverScansAndStaysWithinTheLargestEstimate() {
        FtsAdmission.RetainedPool pool = new FtsAdmission.RetainedPool();
        pool.scanAdmitted(80 * GB, -1L, 10 * GB);
        pool.scanCompleted(74 * GB, -1L);
        assertEquals(6 * GB, pool.retainedBytes());
        // The second scan is admitted below the baseline, so the pool
        // stands and its bound grows to the larger estimate.
        pool.scanAdmitted(74 * GB, -1L, 12 * GB);
        pool.scanCompleted(66 * GB, -1L);
        assertEquals(12 * GB, pool.boundBytes());
        assertEquals("6 + 8 clamped to the 12 GiB bound", 12 * GB, pool.retainedBytes());
        assertEquals(12 * GB, pool.creditBytes(66 * GB));
        // A third scan that frees memory shrinks the pool.
        pool.scanAdmitted(66 * GB, -1L, 10 * GB);
        pool.scanCompleted(70 * GB, -1L);
        assertEquals(8 * GB, pool.retainedBytes());
    }

    public void testPoolCreditDecaysAsAvailableMemoryRecovers() {
        FtsAdmission.RetainedPool pool = new FtsAdmission.RetainedPool();
        pool.scanAdmitted(80 * GB, -1L, 50 * GB);
        pool.scanCompleted(73 * GB, -1L);
        assertEquals(7 * GB, pool.creditBytes(73 * GB));
        // Two of the seven come back (an eviction, a restart of another
        // process): only five are still retained.
        assertEquals(5 * GB, pool.creditBytes(75 * GB));
        // Everything came back: nothing is credited.
        assertEquals(0L, pool.creditBytes(80 * GB));
        assertEquals(0L, pool.creditBytes(90 * GB));
        // The next admission at or above the baseline starts over.
        pool.scanAdmitted(81 * GB, -1L, 50 * GB);
        assertEquals(0L, pool.retainedBytes());
        pool.scanCompleted(81 * GB, -1L);
        assertEquals(0L, pool.creditBytes(81 * GB));
    }

    public void testPoolDoesNotCreditAnUnrelatedDropAfterTheCompletionSample() {
        FtsAdmission.RetainedPool pool = new FtsAdmission.RetainedPool();
        pool.scanAdmitted(80 * GB, -1L, 50 * GB);
        pool.scanCompleted(73 * GB, -1L);
        // Something else took 20 GiB since: the credit stays what the
        // scan left behind, never the whole drop.
        assertEquals(7 * GB, pool.creditBytes(53 * GB));
    }

    public void testPoolCreditNeverExceedsTheDropSinceTheBaseline() {
        // The completion sample saw the scan's memory still resident
        // (it was released a moment later): the recorded pool is
        // inflated, but a decision made after the release sees the
        // recovery and credits only the real drop.
        FtsAdmission.RetainedPool pool = new FtsAdmission.RetainedPool();
        pool.scanAdmitted(80 * GB, -1L, 50 * GB);
        pool.scanCompleted(40 * GB, -1L);
        assertEquals(40 * GB, pool.retainedBytes());
        assertEquals(7 * GB, pool.creditBytes(73 * GB));
    }

    public void testPoolIgnoresACompletionWithoutAnAdmission() {
        FtsAdmission.RetainedPool pool = new FtsAdmission.RetainedPool();
        pool.scanCompleted(10 * GB, -1L);
        assertEquals(0L, pool.retainedBytes());
        assertEquals(0L, pool.creditBytes(5 * GB));
    }

    public void testPoolContributionIsCappedByTheProcessResidentSetGrowth() {
        // MemAvailable fell 7 GiB over the scan but this process grew
        // by 3 GiB: another process took the other 4 GiB, which the
        // next scan cannot reuse.
        FtsAdmission.RetainedPool pool = new FtsAdmission.RetainedPool();
        pool.scanAdmitted(80 * GB, 60 * GB, 50 * GB);
        pool.scanCompleted(73 * GB, 63 * GB);
        assertEquals(3 * GB, pool.retainedBytes());
        // A resident set that shrank gave retained memory back: the pool
        // shrinks by that much even though MemAvailable fell.
        pool.scanAdmitted(73 * GB, 63 * GB, 50 * GB);
        pool.scanCompleted(70 * GB, 62 * GB);
        assertEquals(2 * GB, pool.retainedBytes());
        // Unknown on one side (the probe failed): the MemAvailable
        // difference stands on its own.
        pool.scanAdmitted(70 * GB, -1L, 50 * GB);
        pool.scanCompleted(68 * GB, 70 * GB);
        assertEquals(4 * GB, pool.retainedBytes());
    }

    public void testResidentSetGuardBlocksTheCreditWhenSomethingElseHoldsMemory() {
        // Estimate 52 KB plus 24 bytes of scan buffer, admitted once
        // at 100 GiB available and completed 30 MB lower.
        long rows = 1_000L;
        FtsAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        FtsAdmission.setHeadroom(new ByteSizeValue(8, ByteSizeUnit.GB));
        FtsAdmission.setNativeLimitProbeForTests(() -> 40 * GB);
        long heapMax = Runtime.getRuntime().maxMemory();
        long[] available = { 100 * GB };
        FtsAdmission.setMemoryProbeForTests(() -> available[0]);
        FtsAdmission.setResidentSetProbeForTests(() -> -1L);
        FtsAdmission.admit("demo", rows, UNBOUNDED);
        FtsAdmission.scanStarted();
        available[0] = 100 * GB - 30 * 1024 * 1024;
        FtsAdmission.scanFinished();
        FtsAdmission.requestEnded();
        long estimate = FtsAdmission.lastEstimateBytes();
        assertEquals("the pool is bounded by the estimate", estimate, FtsAdmission.retainedCreditBytes());

        // Resident set within the limit plus the heap: credited.
        FtsAdmission.setResidentSetProbeForTests(() -> 40 * GB + heapMax);
        assertEquals(estimate, FtsAdmission.retainedCreditBytes());
        // Resident set above the limit plus the heap by exactly the
        // pool: still credited; one byte more and it is not.
        FtsAdmission.setResidentSetProbeForTests(() -> 40 * GB + heapMax + estimate);
        assertEquals(estimate, FtsAdmission.retainedCreditBytes());
        FtsAdmission.setResidentSetProbeForTests(() -> 40 * GB + heapMax + estimate + 1);
        assertEquals(0L, FtsAdmission.retainedCreditBytes());
        // No breaker installed (limit unknown): the guard never blocks.
        FtsAdmission.setNativeLimitProbeForTests(() -> -1L);
        assertEquals(estimate, FtsAdmission.retainedCreditBytes());
    }

    public void testAdmitCreditsThePoolOnTheRepeatAndNamesItInTheRejection() {
        long rows = 1_000_000_000L;
        long estimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows) + FtsAdmission.scanBufferEstimateBytes(rows, UNBOUNDED);
        FtsAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        FtsAdmission.setHeadroom(new ByteSizeValue(8, ByteSizeUnit.GB));
        FtsAdmission.setResidentSetProbeForTests(() -> -1L);
        long[] available = { estimate + 8 * GB + 2 * GB };
        FtsAdmission.setMemoryProbeForTests(() -> available[0]);

        // First request: admitted with 2 GiB to spare, the scan runs
        // and leaves 7 GiB behind, the request ends.
        FtsAdmission.admit("perf1b", rows, UNBOUNDED);
        assertEquals(1, FtsAdmission.inFlightForTests());
        FtsAdmission.scanStarted();
        assertEquals("nothing is credited while the scan runs", 0L, FtsAdmission.retainedCreditBytes());
        available[0] -= 7 * GB;
        FtsAdmission.scanFinished();
        assertEquals("nothing is credited while the request is in flight", 0L, FtsAdmission.retainedCreditBytes());
        FtsAdmission.requestEnded();
        assertEquals(0, FtsAdmission.inFlightForTests());
        assertEquals(7 * GB, FtsAdmission.retainedCreditBytes());

        // Second identical request: 5 GiB short on the raw reading,
        // admitted on the credit, and the pool stands afterwards.
        FtsAdmission.admit("perf1b", rows, UNBOUNDED);
        FtsAdmission.scanStarted();
        FtsAdmission.scanFinished();
        FtsAdmission.requestEnded();
        assertEquals(0L, FtsAdmission.rejections());
        assertEquals(7 * GB, FtsAdmission.retainedCreditBytes());

        // Another consumer takes 3 GiB: the credit no longer covers the
        // gap and the 429 names both figures.
        available[0] -= 3 * GB;
        CircuitBreakingException rejection = expectThrows(
            CircuitBreakingException.class,
            () -> FtsAdmission.admit("perf1b", rows, UNBOUNDED)
        );
        String message = rejection.getMessage();
        assertTrue(message, message.contains("[7gb] retained by earlier full text scans"));
        assertTrue(message, message.contains("[8gb] headroom"));
        assertEquals(1L, FtsAdmission.rejections());
        assertEquals("the byte limit on the wire carries the credit", estimate - 8 * GB + 7 * GB, rejection.getByteLimit());
        assertEquals("a refused request is not in flight", 0, FtsAdmission.inFlightForTests());
    }

    public void testRequestEndReleasesAnAdmissionWhoseScanNeverRan() {
        FtsAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        FtsAdmission.setHeadroom(new ByteSizeValue(8, ByteSizeUnit.GB));
        FtsAdmission.setMemoryProbeForTests(() -> 100 * GB);
        FtsAdmission.admit("demo", 1_000L, UNBOUNDED);
        assertEquals(1, FtsAdmission.inFlightForTests());
        // The request failed before its Weight scanned (a sort the
        // planner rejected): the executor still closes the search
        // context on this thread.
        FtsAdmission.requestEnded();
        assertEquals(0, FtsAdmission.inFlightForTests());
        // A thread without an admission is a no-op.
        FtsAdmission.requestEnded();
        assertEquals(0, FtsAdmission.inFlightForTests());
        // A fitting index (estimate zero) is not counted in flight.
        FtsAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.GB));
        FtsAdmission.admit("demo", 1_000L, UNBOUNDED);
        assertEquals(0, FtsAdmission.inFlightForTests());
    }

    public void testConcurrentScansSuspendTheCreditAndSkipTheSample() throws Exception {
        long rows = 1_000_000_000L;
        long estimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows) + FtsAdmission.scanBufferEstimateBytes(rows, UNBOUNDED);
        FtsAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        FtsAdmission.setHeadroom(new ByteSizeValue(8, ByteSizeUnit.GB));
        FtsAdmission.setResidentSetProbeForTests(() -> -1L);
        long[] available = { 2 * estimate + 8 * GB };
        FtsAdmission.setMemoryProbeForTests(() -> available[0]);

        // Two requests admitted back to back on two search threads (a
        // burst), both scans run at once. The other thread ends its
        // request on its own thread, the way the executor does.
        CountDownLatch otherAdmitted = new CountDownLatch(1);
        CountDownLatch scansDone = new CountDownLatch(1);
        Thread other = new Thread(() -> {
            FtsAdmission.admit("perf1b", rows, UNBOUNDED);
            otherAdmitted.countDown();
            try {
                scansDone.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            FtsAdmission.requestEnded();
        });
        other.start();
        otherAdmitted.await();
        FtsAdmission.admit("perf1b", rows, UNBOUNDED);
        assertEquals(2, FtsAdmission.inFlightForTests());
        FtsAdmission.scanStarted();
        FtsAdmission.scanStarted();
        assertEquals(2, FtsAdmission.activeScansForTests());
        available[0] -= 20 * GB;
        assertEquals("nothing is credited while requests are in flight", 0L, FtsAdmission.retainedCreditBytes());
        FtsAdmission.scanFinished();
        FtsAdmission.scanFinished();
        assertEquals(0, FtsAdmission.activeScansForTests());
        scansDone.countDown();
        other.join();
        assertEquals(1, FtsAdmission.inFlightForTests());
        FtsAdmission.requestEnded();
        assertEquals(0, FtsAdmission.inFlightForTests());
        // With two requests in flight when the scans completed, the
        // 20 GiB drop could not be attributed to one of them: nothing
        // was recorded and nothing is credited.
        assertEquals(0L, FtsAdmission.retainedCreditBytes());
    }

    public void testParseKibibyteLineReadsTheResidentSet() {
        List<String> status = List.of("Name:\tjava", "VmPeak:\t 90000000 kB", "VmRSS:\t 69730304 kB", "Threads:\t512");
        assertEquals(69_730_304L * 1024L, FtsAdmission.parseKibibyteLine(status, "VmRSS:"));
        assertEquals(-1L, FtsAdmission.parseKibibyteLine(status, "VmSwap:"));
    }

    public void testReadResidentSetAnswersOnLinuxAndIsUnknownElsewhere() {
        long rss = FtsAdmission.readResidentSetBytes();
        if (Constants.LINUX) {
            assertTrue("a live Linux process reports its resident set: " + rss, rss > 0L);
        } else {
            assertEquals(-1L, rss);
        }
        // Without a breaker installed the excess is unknown and never blocks.
        FtsAdmission.setNativeLimitProbeForTests(() -> -1L);
        assertEquals(Long.MIN_VALUE, FtsAdmission.residentSetExcessBytes());
    }
}
