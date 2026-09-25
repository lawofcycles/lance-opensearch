/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.Constants;
import org.lance.Dataset;
import org.lance.index.IndexType;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.plan.metadata.ColumnStatistics;
import org.opensearch.lance.plan.metadata.TableStatistics;
import org.opensearch.lance.plan.metadata.TableStatisticsCache;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * The admission decision, the estimator of every gated kind against
 * hand computed numbers, the full text shape classification the
 * fragment executor gates on, the retained pool, the per kind counters
 * and the 429 the gate answers with.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ScanAdmissionTests extends OpenSearchTestCase {

    private static final long GB = 1L << 30;

    private static final ScanAdmission.Shape UNBOUNDED = new ScanAdmission.Shape(true, true, 0L);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Another test class in the same JVM may have admitted a scan
        // and left the static last kind and counters behind.
        ScanAdmission.resetForTests();
    }

    @Override
    public void tearDown() throws Exception {
        ScanAdmission.resetForTests();
        super.tearDown();
    }

    public void testFittingIndexIsAdmittedWithEstimateZero() {
        // 1M rows is 52 MB, within an 8 GiB shard share, so the cached
        // entry is not rebuilt per scan and free memory is not judged.
        ScanAdmission.Decision decision = ScanAdmission.decideFts(1_000_000L, 0L, 8 * GB, 0L, true, 8 * GB, 0L);
        assertTrue(decision.admitted());
        assertEquals(0L, decision.estimateBytes());
    }

    public void testFittingIndexZeroesTheScanBufferTooBecauseNothingIsRebuilt() {
        ScanAdmission.Decision decision = ScanAdmission.decideFts(1_000_000L, 64 * GB, 8 * GB, 0L, true, 8 * GB, 0L);
        assertTrue(decision.admitted());
        assertEquals(0L, decision.estimateBytes());
    }

    public void testNonFittingIndexIsAdmittedWhenFreeMemoryHoldsTheEstimate() {
        long rows = 1_000_000_000L;
        long estimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows);
        ScanAdmission.Decision decision = ScanAdmission.decideFts(rows, 0L, 8 * GB, estimate + 16 * GB, true, 8 * GB, 0L);
        assertTrue(decision.admitted());
        assertEquals(estimate, decision.estimateBytes());
        assertEquals(estimate + 8 * GB, decision.availableBytes());
    }

    public void testNonFittingIndexIsRejectedWhenFreeMemoryIsSmall() {
        long rows = 1_000_000_000L;
        ScanAdmission.Decision decision = ScanAdmission.decideFts(rows, 0L, 8 * GB, 16 * GB, true, 8 * GB, 0L);
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
        ScanAdmission.Decision withoutBuffer = ScanAdmission.decideFts(rows, 0L, 8 * GB, free, true, 8 * GB, 0L);
        assertTrue(withoutBuffer.admitted());
        ScanAdmission.Decision withBuffer = ScanAdmission.decideFts(rows, 2 * GB, 8 * GB, free, true, 8 * GB, 0L);
        assertFalse(withBuffer.admitted());
        assertEquals(entry + 2 * GB, withBuffer.estimateBytes());
    }

    public void testDisabledGateAdmitsRegardless() {
        ScanAdmission.Decision decision = ScanAdmission.decideFts(1_000_000_000L, 0L, 8 * GB, 0L, false, 8 * GB, 0L);
        assertTrue(decision.admitted());
        assertTrue(decision.estimateBytes() > 0L);
    }

    public void testHeadroomEqualToFreeMemoryRejectsANonZeroEstimate() {
        // available is exactly zero: nothing is left for the rebuild.
        ScanAdmission.Decision decision = ScanAdmission.decideFts(1_000_000_000L, 0L, 8 * GB, 8 * GB, true, 8 * GB, 0L);
        assertFalse(decision.admitted());
        assertEquals(0L, decision.availableBytes());
    }

    public void testHeadroomAboveFreeMemoryStillAdmitsAFittingIndex() {
        ScanAdmission.Decision decision = ScanAdmission.decideFts(1_000L, 0L, 8 * GB, 0L, true, 8 * GB, 0L);
        assertTrue(decision.admitted());
        assertEquals(0L, decision.estimateBytes());
        assertTrue(decision.availableBytes() < 0L);
    }

    public void testScanBufferEstimateOfAnUnboundedShapeGrowsWithTheTable() {
        // One row in ten assumed matched, 12 bytes per returned row,
        // doubled for the batches held at once.
        long expected = (long) ((long) (1_000_000_000L * 0.1) * 12L * 2.0);
        assertEquals(expected, ScanAdmission.scanBufferEstimateBytes(1_000_000_000L, UNBOUNDED));
    }

    public void testScanBufferEstimateOfABoundedPageUsesTheTopKLimit() {
        ScanAdmission.Shape bounded = new ScanAdmission.Shape(true, false, 10L);
        assertEquals((long) (10L * 12L * 2.0), ScanAdmission.scanBufferEstimateBytes(1_000_000_000L, bounded));
    }

    public void testRejectionNamesTheKindTheEstimateAndTheAvailableMemory() {
        ScanAdmission.Decision decision = new ScanAdmission.Decision(false, 48 * GB, 0L, -2 * GB, 0L, false);
        CircuitBreakingException rejection = ScanAdmission.rejection(
            ScanAdmission.Kind.FTS,
            decision,
            Long.MAX_VALUE,
            8 * GB,
            "unbounded full text scan over [perf1b]",
            "Relax lance.admission.headroom / lance.admission.enabled."
        );
        assertEquals(CircuitBreaker.Durability.TRANSIENT, rejection.getDurability());
        assertEquals(48 * GB, rejection.getBytesWanted());
        String message = rejection.getMessage();
        assertTrue(
            message,
            message.startsWith("[" + ScanAdmission.LABEL + "] fts estimate [48gb] exceeds available [0b] minus headroom [8gb]")
        );
        assertTrue(message, message.contains("unbounded full text scan over [perf1b]"));
        assertTrue(message, message.contains("lance.admission.headroom"));
        assertTrue(message, message.contains("lance.admission.enabled"));
    }

    public void testHeapRejectionNamesTheBreakerRoom() {
        ScanAdmission.Decision decision = new ScanAdmission.Decision(false, 0L, 3 * GB, 100 * GB, 0L, true);
        CircuitBreakingException rejection = ScanAdmission.rejection(
            ScanAdmission.Kind.FILTER_SCAN,
            decision,
            2 * GB,
            8 * GB,
            "filter scan over [perf10b]",
            "Narrow the filter."
        );
        String message = rejection.getMessage();
        assertTrue(
            message,
            message.startsWith("[" + ScanAdmission.LABEL + "] filter_scan heap estimate [3gb] exceeds the request breaker's room [2gb]")
        );
        assertEquals(3 * GB, rejection.getBytesWanted());
        assertEquals(2 * GB, rejection.getByteLimit());
    }

    public void testAdmitRecordsTheEstimateAndCountsARejection() {
        // Shard share of one byte makes the fixture's estimate count in
        // full, and a headroom of half of Long.MAX_VALUE makes the
        // available memory negative on any host.
        ScanAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        ScanAdmission.setHeadroom(new ByteSizeValue(Long.MAX_VALUE / 2, ByteSizeUnit.BYTES));
        long before = ScanAdmission.rejections();
        CircuitBreakingException rejection = expectThrows(
            CircuitBreakingException.class,
            () -> ScanAdmission.admit("demo", 1_000L, UNBOUNDED)
        );
        assertTrue(rejection.getMessage(), rejection.getMessage().contains(ScanAdmission.LABEL));
        assertEquals(before + 1, ScanAdmission.rejections());
        long expectedEstimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(1_000L) + ScanAdmission.scanBufferEstimateBytes(
            1_000L,
            UNBOUNDED
        );
        assertEquals(expectedEstimate, ScanAdmission.lastEstimateBytes());

        ScanAdmission.setEnabled(false);
        ScanAdmission.admit("demo", 1_000L, UNBOUNDED);
        assertEquals("a disabled gate admits and does not count", before + 1, ScanAdmission.rejections());
    }

    public void testAdmitJudgesABoundedPageOnTheDocumentSetItStillRebuilds() {
        ScanAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        ScanAdmission.setHeadroom(new ByteSizeValue(Long.MAX_VALUE / 2, ByteSizeUnit.BYTES));
        ScanAdmission.Shape bounded = new ScanAdmission.Shape(true, false, 10L);
        CircuitBreakingException rejection = expectThrows(
            CircuitBreakingException.class,
            () -> ScanAdmission.admit("demo", 1_000L, bounded)
        );
        assertTrue(rejection.getMessage(), rejection.getMessage().contains("bounded full text page"));
        assertTrue(rejection.getMessage(), rejection.getMessage().contains("lance.admission.bounded_shapes_gated"));
        long expectedEstimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(1_000L) + ScanAdmission.scanBufferEstimateBytes(
            1_000L,
            bounded
        );
        assertEquals(expectedEstimate, ScanAdmission.lastEstimateBytes());
    }

    public void testAdmitJudgesTheProbedAvailableMemory() {
        // A shard share of one byte makes the estimate count in full.
        ScanAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        ScanAdmission.setHeadroom(new ByteSizeValue(8, ByteSizeUnit.GB));
        long estimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(1_000L) + ScanAdmission.scanBufferEstimateBytes(
            1_000L,
            UNBOUNDED
        );

        // A MemAvailable-sized reading (page cache reclaimable) admits
        // the scan even though the same host's MemFree could be zero.
        ScanAdmission.setMemoryProbeForTests(() -> 8 * GB + estimate);
        ScanAdmission.admit("demo", 1_000L, UNBOUNDED);

        // One byte less and the estimate no longer fits.
        ScanAdmission.setMemoryProbeForTests(() -> 8 * GB + estimate - 1);
        CircuitBreakingException rejection = expectThrows(
            CircuitBreakingException.class,
            () -> ScanAdmission.admit("demo", 1_000L, UNBOUNDED)
        );
        assertTrue(rejection.getMessage(), rejection.getMessage().contains(ScanAdmission.LABEL));
    }

    public void testAvailableMemoryReportsTheProbeReading() {
        ScanAdmission.setMemoryProbeForTests(() -> 123L);
        assertEquals(123L, ScanAdmission.availablePhysicalMemoryBytes());
    }

    public void testScriptedReadingsAreHandedOutOnePerReadWithTheLastRepeating() {
        ScanAdmission.setMemoryProbeForTests(() -> 123L);
        ScanAdmission.setAvailableMemoryOverride(List.of("2000b", "1000b"));
        assertEquals(2000L, ScanAdmission.availablePhysicalMemoryBytes());
        assertEquals(1000L, ScanAdmission.availablePhysicalMemoryBytes());
        assertEquals(1000L, ScanAdmission.availablePhysicalMemoryBytes());
        // The resident set is unknown while the script is in force, so
        // neither the cap nor the guard reads the host.
        ScanAdmission.setResidentSetProbeForTests(() -> 500 * GB);
        ScanAdmission.setNativeLimitProbeForTests(() -> 1L);
        assertEquals(Long.MIN_VALUE, ScanAdmission.residentSetExcessBytes());
        // An empty list clears the override and the probe answers again.
        ScanAdmission.setAvailableMemoryOverride(List.of());
        assertEquals(123L, ScanAdmission.availablePhysicalMemoryBytes());
        assertTrue(ScanAdmission.residentSetExcessBytes() > 0L);
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
        assertEquals(224_240_304L * 1024L, ScanAdmission.parseMemAvailableBytes(memInfo));
    }

    public void testParseMemAvailableWithoutTheLineIsUnavailable() {
        // Kernels before 3.14 and non-Linux shims have no MemAvailable;
        // the probe then falls back to the free physical memory.
        assertEquals(-1L, ScanAdmission.parseMemAvailableBytes(List.of("MemTotal:       263846076 kB", "MemFree:  1130308 kB")));
        assertEquals(-1L, ScanAdmission.parseMemAvailableBytes(List.of()));
    }

    public void testParseMemAvailableWithAMalformedLineIsUnavailable() {
        assertEquals(-1L, ScanAdmission.parseMemAvailableBytes(List.of("MemAvailable:")));
        assertEquals(-1L, ScanAdmission.parseMemAvailableBytes(List.of("MemAvailable:   lots kB")));
    }

    public void testReadAvailablePhysicalMemoryAnswersOnEveryPlatform() {
        // /proc/meminfo on Linux, the OsProbe fallback elsewhere:
        // either way a live host reports a positive reading.
        assertTrue(ScanAdmission.readAvailablePhysicalMemory() > 0L);
    }

    public void testQueriesWithoutAFullTextClauseAreNotGated() {
        assertFalse(ScanAdmission.runsUnboundedFtsScan(MatchAllDocsQuery.INSTANCE, true));
        assertFalse(ScanAdmission.gates(ScanAdmission.classify(MatchAllDocsQuery.INSTANCE, true)));
    }

    public void testBareUnboundedFtsQueryIsGated() {
        assertTrue(ScanAdmission.runsUnboundedFtsScan(new LanceFtsQuery("body", "hello"), false));
    }

    public void testBoundedTopKPageDoesNotRunAnUnboundedScan() {
        assertFalse(ScanAdmission.runsUnboundedFtsScan(new LanceFtsQuery("body", "hello").withScanLimit(10), false));
    }

    public void testBoundedTopKPageIsClassifiedWithItsScanLimit() {
        ScanAdmission.Shape shape = ScanAdmission.classify(new LanceFtsQuery("body", "hello").withScanLimit(10), false);
        assertTrue(shape.hasFtsClause());
        assertFalse(shape.unbounded());
        assertEquals(10L, shape.boundedScanRows());
    }

    public void testBoundedTopKPageIsGatedByDefaultAndTheSettingOptsOut() {
        ScanAdmission.Shape shape = ScanAdmission.classify(new LanceFtsQuery("body", "hello").withScanLimit(10), false);
        assertTrue(ScanAdmission.gates(shape));
        ScanAdmission.setBoundedShapesGated(false);
        assertFalse(ScanAdmission.gates(shape));
        // The opt-out leaves the unbounded shapes gated.
        assertTrue(ScanAdmission.gates(UNBOUNDED));
    }

    public void testBoundedPageWithAnExactCountIsGated() {
        // track_total_hits: true runs the unbounded count-only scan
        // when the bounded page's scan filled its limit.
        assertTrue(ScanAdmission.runsUnboundedFtsScan(new LanceFtsQuery("body", "hello").withScanLimit(10), true));
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
            assertTrue(occur.toString(), ScanAdmission.runsUnboundedFtsScan(bool, false));
        }
    }

    public void testBoostWrappedBoundedFtsQueryIsClassifiedBounded() {
        BoostQuery boosted = new BoostQuery(new LanceFtsQuery("body", "hello").withScanLimit(10), 2f);
        assertFalse(ScanAdmission.runsUnboundedFtsScan(boosted, false));
        ScanAdmission.Shape shape = ScanAdmission.classify(boosted, false);
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
        assertFalse(ScanAdmission.decideFts(rows, 0L, 8 * GB, available, true, 8 * GB, 0L).admitted());
        ScanAdmission.Decision credited = ScanAdmission.decideFts(rows, 0L, 8 * GB, available, true, 8 * GB, 7 * GB);
        assertTrue(credited.admitted());
        assertEquals(7 * GB, credited.retainedCreditBytes());
        assertEquals("the available figure stays the raw one after the headroom", available - 8 * GB, credited.availableBytes());
        // One byte short of the gap and the credit does not admit.
        assertFalse(ScanAdmission.decideFts(rows, 0L, 8 * GB, available, true, 8 * GB, 4 * GB - 1).admitted());
        // A negative credit counts as none.
        assertEquals(0L, ScanAdmission.decideFts(rows, 0L, 8 * GB, available, true, 8 * GB, -1L).retainedCreditBytes());
    }

    public void testPoolRecordsWhatOneScanLeftBehind() {
        ScanAdmission.RetainedPool pool = new ScanAdmission.RetainedPool();
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
        ScanAdmission.RetainedPool pool = new ScanAdmission.RetainedPool();
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
        ScanAdmission.RetainedPool pool = new ScanAdmission.RetainedPool();
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
        ScanAdmission.RetainedPool pool = new ScanAdmission.RetainedPool();
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
        ScanAdmission.RetainedPool pool = new ScanAdmission.RetainedPool();
        pool.scanAdmitted(80 * GB, -1L, 50 * GB);
        pool.scanCompleted(40 * GB, -1L);
        assertEquals(40 * GB, pool.retainedBytes());
        assertEquals(7 * GB, pool.creditBytes(73 * GB));
    }

    public void testPoolIgnoresACompletionWithoutAnAdmission() {
        ScanAdmission.RetainedPool pool = new ScanAdmission.RetainedPool();
        pool.scanCompleted(10 * GB, -1L);
        assertEquals(0L, pool.retainedBytes());
        assertEquals(0L, pool.creditBytes(5 * GB));
    }

    public void testPoolContributionIsCappedByTheProcessResidentSetGrowth() {
        // MemAvailable fell 7 GiB over the scan but this process grew
        // by 3 GiB: another process took the other 4 GiB, which the
        // next scan cannot reuse.
        ScanAdmission.RetainedPool pool = new ScanAdmission.RetainedPool();
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
        ScanAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        ScanAdmission.setHeadroom(new ByteSizeValue(8, ByteSizeUnit.GB));
        ScanAdmission.setNativeLimitProbeForTests(() -> 40 * GB);
        long heapMax = Runtime.getRuntime().maxMemory();
        long[] available = { 100 * GB };
        ScanAdmission.setMemoryProbeForTests(() -> available[0]);
        ScanAdmission.setResidentSetProbeForTests(() -> -1L);
        ScanAdmission.admit("demo", rows, UNBOUNDED);
        ScanAdmission.scanStarted();
        available[0] = 100 * GB - 30 * 1024 * 1024;
        ScanAdmission.scanFinished();
        ScanAdmission.requestEnded();
        long estimate = ScanAdmission.lastEstimateBytes();
        assertEquals("the pool is bounded by the estimate", estimate, ScanAdmission.retainedCreditBytes());

        // Resident set within the limit plus the heap: credited.
        ScanAdmission.setResidentSetProbeForTests(() -> 40 * GB + heapMax);
        assertEquals(estimate, ScanAdmission.retainedCreditBytes());
        // Resident set above the limit plus the heap by exactly the
        // pool: still credited; one byte more and it is not.
        ScanAdmission.setResidentSetProbeForTests(() -> 40 * GB + heapMax + estimate);
        assertEquals(estimate, ScanAdmission.retainedCreditBytes());
        ScanAdmission.setResidentSetProbeForTests(() -> 40 * GB + heapMax + estimate + 1);
        assertEquals(0L, ScanAdmission.retainedCreditBytes());
        // No breaker installed (limit unknown): the guard never blocks.
        ScanAdmission.setNativeLimitProbeForTests(() -> -1L);
        assertEquals(estimate, ScanAdmission.retainedCreditBytes());
    }

    public void testAdmitCreditsThePoolOnTheRepeatAndNamesItInTheRejection() {
        long rows = 1_000_000_000L;
        long estimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows) + ScanAdmission.scanBufferEstimateBytes(rows, UNBOUNDED);
        ScanAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        ScanAdmission.setHeadroom(new ByteSizeValue(8, ByteSizeUnit.GB));
        ScanAdmission.setResidentSetProbeForTests(() -> -1L);
        long[] available = { estimate + 8 * GB + 2 * GB };
        ScanAdmission.setMemoryProbeForTests(() -> available[0]);

        // First request: admitted with 2 GiB to spare, the scan runs
        // and leaves 7 GiB behind, the request ends.
        ScanAdmission.admit("perf1b", rows, UNBOUNDED);
        assertEquals(1, ScanAdmission.inFlightForTests());
        ScanAdmission.scanStarted();
        assertEquals("nothing is credited while the scan runs", 0L, ScanAdmission.retainedCreditBytes());
        available[0] -= 7 * GB;
        ScanAdmission.scanFinished();
        assertEquals("nothing is credited while the request is in flight", 0L, ScanAdmission.retainedCreditBytes());
        ScanAdmission.requestEnded();
        assertEquals(0, ScanAdmission.inFlightForTests());
        assertEquals(7 * GB, ScanAdmission.retainedCreditBytes());

        // Second identical request: 5 GiB short on the raw reading,
        // admitted on the credit, and the pool stands afterwards.
        ScanAdmission.admit("perf1b", rows, UNBOUNDED);
        ScanAdmission.scanStarted();
        ScanAdmission.scanFinished();
        ScanAdmission.requestEnded();
        assertEquals(0L, ScanAdmission.rejections());
        assertEquals(7 * GB, ScanAdmission.retainedCreditBytes());

        // Another consumer takes 3 GiB: the credit no longer covers the
        // gap and the 429 names both figures.
        available[0] -= 3 * GB;
        CircuitBreakingException rejection = expectThrows(
            CircuitBreakingException.class,
            () -> ScanAdmission.admit("perf1b", rows, UNBOUNDED)
        );
        String message = rejection.getMessage();
        assertTrue(message, message.contains("[7gb] retained by earlier admitted scans"));
        assertTrue(message, message.contains("minus headroom [8gb]"));
        assertEquals(1L, ScanAdmission.rejections());
        assertEquals("the byte limit on the wire carries the credit", estimate - 8 * GB + 7 * GB, rejection.getByteLimit());
        assertEquals("a refused request is not in flight", 0, ScanAdmission.inFlightForTests());
    }

    public void testRequestEndReleasesAnAdmissionWhoseScanNeverRan() {
        ScanAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        ScanAdmission.setHeadroom(new ByteSizeValue(8, ByteSizeUnit.GB));
        ScanAdmission.setMemoryProbeForTests(() -> 100 * GB);
        ScanAdmission.admit("demo", 1_000L, UNBOUNDED);
        assertEquals(1, ScanAdmission.inFlightForTests());
        // The request failed before its Weight scanned (a sort the
        // planner rejected): the executor still closes the search
        // context on this thread.
        ScanAdmission.requestEnded();
        assertEquals(0, ScanAdmission.inFlightForTests());
        // A thread without an admission is a no-op.
        ScanAdmission.requestEnded();
        assertEquals(0, ScanAdmission.inFlightForTests());
        // A fitting index (estimate zero) is not counted in flight.
        ScanAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.GB));
        ScanAdmission.admit("demo", 1_000L, UNBOUNDED);
        assertEquals(0, ScanAdmission.inFlightForTests());
    }

    public void testConcurrentScansSuspendTheCreditAndSkipTheSample() throws Exception {
        long rows = 1_000_000_000L;
        long estimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows) + ScanAdmission.scanBufferEstimateBytes(rows, UNBOUNDED);
        ScanAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        ScanAdmission.setHeadroom(new ByteSizeValue(8, ByteSizeUnit.GB));
        ScanAdmission.setResidentSetProbeForTests(() -> -1L);
        long[] available = { 2 * estimate + 8 * GB };
        ScanAdmission.setMemoryProbeForTests(() -> available[0]);

        // Two requests admitted back to back on two search threads (a
        // burst), both scans run at once. The other thread ends its
        // request on its own thread, the way the executor does.
        CountDownLatch otherAdmitted = new CountDownLatch(1);
        CountDownLatch scansDone = new CountDownLatch(1);
        Thread other = new Thread(() -> {
            ScanAdmission.admit("perf1b", rows, UNBOUNDED);
            otherAdmitted.countDown();
            try {
                scansDone.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            ScanAdmission.requestEnded();
        });
        other.start();
        otherAdmitted.await();
        ScanAdmission.admit("perf1b", rows, UNBOUNDED);
        assertEquals(2, ScanAdmission.inFlightForTests());
        ScanAdmission.scanStarted();
        ScanAdmission.scanStarted();
        assertEquals(2, ScanAdmission.activeScansForTests());
        available[0] -= 20 * GB;
        assertEquals("nothing is credited while requests are in flight", 0L, ScanAdmission.retainedCreditBytes());
        ScanAdmission.scanFinished();
        ScanAdmission.scanFinished();
        assertEquals(0, ScanAdmission.activeScansForTests());
        scansDone.countDown();
        other.join();
        assertEquals(1, ScanAdmission.inFlightForTests());
        ScanAdmission.requestEnded();
        assertEquals(0, ScanAdmission.inFlightForTests());
        // With two requests in flight when the scans completed, the
        // 20 GiB drop could not be attributed to one of them: nothing
        // was recorded and nothing is credited.
        assertEquals(0L, ScanAdmission.retainedCreditBytes());
    }

    public void testParseKibibyteLineReadsTheResidentSet() {
        List<String> status = List.of("Name:\tjava", "VmPeak:\t 90000000 kB", "VmRSS:\t 69730304 kB", "Threads:\t512");
        assertEquals(69_730_304L * 1024L, ScanAdmission.parseKibibyteLine(status, "VmRSS:"));
        assertEquals(-1L, ScanAdmission.parseKibibyteLine(status, "VmSwap:"));
    }

    public void testReadResidentSetAnswersOnLinuxAndIsUnknownElsewhere() {
        long rss = ScanAdmission.readResidentSetBytes();
        if (Constants.LINUX) {
            assertTrue("a live Linux process reports its resident set: " + rss, rss > 0L);
        } else {
            assertEquals(-1L, rss);
        }
        // Without a breaker installed the excess is unknown and never blocks.
        ScanAdmission.setNativeLimitProbeForTests(() -> -1L);
        assertEquals(Long.MIN_VALUE, ScanAdmission.residentSetExcessBytes());
    }

    // ---- the estimators of the other kinds, against hand computed numbers ----

    private static final long TEN_BILLION = 10_000_000_000L;

    public void testBtreeLoadThatDoesNotFitTheShardShareIsRefusedAtFiveHundredGigabytes() {
        // A 10B row BTree without a manifest size: 160 GB of pages;
        // an equality on a column without cardinality selects one row
        // in five, so 32 GB of pages are read, above an 8 GiB shard.
        long estimate = ScanAdmission.scalarIndexEstimateBytes(
            IndexType.BTREE,
            OptionalLong.empty(),
            TEN_BILLION,
            ScanAdmission.FILTER_MATCH_RATIO_UNKNOWN,
            8 * GB
        );
        assertEquals((long) (TEN_BILLION * ScanAdmission.BTREE_BYTES_PER_ROW * 0.2), estimate);
        assertTrue(estimate > 0L);
        // 512 GB available leaves room for the pages; the filter scan
        // below is what refuses the request.
        assertTrue(ScanAdmission.decide(estimate, 0L, 512 * GB, Long.MAX_VALUE, true, 8 * GB, 0L).admitted());
        // The manifest size is preferred over the fallback.
        assertEquals(
            (long) (100 * GB * 0.2),
            ScanAdmission.scalarIndexEstimateBytes(IndexType.BTREE, OptionalLong.of(100 * GB), TEN_BILLION, 0.2, 8 * GB)
        );
    }

    public void testBtreeLoadThatFitsTheShardShareIsEstimateZero() {
        // 1B rows: 16 GB of pages, one fifth read is 3.2 GB, within 8 GiB.
        assertEquals(0L, ScanAdmission.scalarIndexEstimateBytes(IndexType.BTREE, OptionalLong.empty(), 1_000_000_000L, 0.2, 8 * GB));
    }

    public void testBitmapLoadIsTheMatchingBitmapsHeldTwice() {
        // 100 distinct values, one selected: 1 GB of bitmaps, 10 MB read
        // and cloned into the cache.
        long size = 1L << 30;
        assertEquals(
            (long) (size * 0.01) * ScanAdmission.BITMAP_LOAD_FACTOR,
            ScanAdmission.scalarIndexEstimateBytes(IndexType.BITMAP, OptionalLong.of(size), TEN_BILLION, 0.01, 1L)
        );
        // A zone map is read whole whatever the predicate: one entry
        // per zone, the last zone partial.
        assertEquals(
            ((TEN_BILLION + ScanAdmission.ZONEMAP_ROWS_PER_ZONE - 1) / ScanAdmission.ZONEMAP_ROWS_PER_ZONE)
                * ScanAdmission.ZONEMAP_BYTES_PER_ZONE,
            ScanAdmission.scalarIndexEstimateBytes(IndexType.ZONEMAP, OptionalLong.empty(), TEN_BILLION, 0.01, 1L)
        );
    }

    public void testFilterScanOverTenBillionRowsAtOneFifthReachesTheMeasuredResidentSet() {
        // term rating=5 size 0 on the 16xlarge: 10B rows on the node,
        // 2B expected to match at 256 bytes each is 512 GB, the row
        // address batches in flight and the 2 GiB read queue on top; the
        // node had about 470 GB available and was killed above 500 GB.
        long matching = ScanAdmission.filterScanMatchingRows(TEN_BILLION, ScanAdmission.FILTER_MATCH_RATIO_UNKNOWN, 0L, false);
        assertEquals(2_000_000_000L, matching);
        long estimate = ScanAdmission.filterScanEstimateBytes(TEN_BILLION, matching, ScanAdmission.ROW_ADDRESS_BYTES, 64, 8 * GB);
        long materialised = matching * ScanAdmission.FILTER_SCAN_BYTES_PER_MATCHING_ROW;
        long batches = (long) (64 * ScanAdmission.SCAN_BATCH_ROWS * ScanAdmission.ROW_ADDRESS_BYTES * ScanAdmission.SCAN_BUFFER_FACTOR);
        assertEquals(materialised + batches + ScanAdmission.IO_BUFFER_BYTES_PER_SCAN, estimate);
        assertTrue(estimate >= 500L * 1_000_000_000L);
        assertFalse(ScanAdmission.decide(estimate, 0L, 470 * GB, Long.MAX_VALUE, true, 8 * GB, 0L).admitted());
        // The bit sets of the node's fragments are heap: 10B bits.
        assertEquals(TEN_BILLION / 8L + 252L * 64L, ScanAdmission.filterScanHeapBytes(TEN_BILLION, 252));
    }

    public void testFilterScanOverThreeBillionRowsRefusesA128GbNode() {
        // 3 x r7gd.4xlarge: 3.3B rows per node, 128 GB physical.
        long nodeRows = 3_300_000_000L;
        long matching = ScanAdmission.filterScanMatchingRows(nodeRows, 0.2, 0L, false);
        long estimate = ScanAdmission.filterScanEstimateBytes(nodeRows, matching, ScanAdmission.ROW_ADDRESS_BYTES, 16, 8 * GB);
        assertTrue(estimate > 128 * GB);
        assertFalse(ScanAdmission.decide(estimate, 0L, 90 * GB, Long.MAX_VALUE, true, 8 * GB, 0L).admitted());
    }

    public void testSmallFilterScanFitsTheShardShareAndIsEstimateZero() {
        long matching = ScanAdmission.filterScanMatchingRows(16L, 0.2, 0L, false);
        assertEquals(3L, matching);
        assertEquals(0L, ScanAdmission.filterScanEstimateBytes(16L, matching, ScanAdmission.ROW_ADDRESS_BYTES, 8, 8 * GB));
        // Under a one byte share the same scan counts in full.
        assertTrue(ScanAdmission.filterScanEstimateBytes(16L, matching, ScanAdmission.ROW_ADDRESS_BYTES, 8, 1L) > 0L);
    }

    public void testBoundedFilterPageIsCappedOnlyWhenTheOptOutAsks() {
        assertEquals(2_000_000_000L, ScanAdmission.filterScanMatchingRows(TEN_BILLION, 0.2, 10L, false));
        assertEquals(10L, ScanAdmission.filterScanMatchingRows(TEN_BILLION, 0.2, 10L, true));
        assertEquals(2_000_000_000L, ScanAdmission.filterScanMatchingRows(TEN_BILLION, 0.2, 0L, true));
    }

    public void testFilterSelectivityReadsTheDistinctCountOfAnEqualityOnly() {
        ColumnStatistics.IndexSummary bitmap = new ColumnStatistics.IndexSummary(
            "category_idx",
            Optional.of(IndexType.BITMAP),
            1,
            1,
            OptionalLong.of(4096L),
            OptionalLong.of(16L),
            OptionalLong.of(0L),
            OptionalLong.of(5L),
            true
        );
        ColumnStatistics.IndexSummary btree = new ColumnStatistics.IndexSummary(
            "rating_idx",
            Optional.of(IndexType.BTREE),
            1,
            1,
            OptionalLong.of(1024L),
            OptionalLong.of(16L),
            OptionalLong.of(0L),
            OptionalLong.empty(),
            true
        );
        TableStatistics statistics = new TableStatistics(
            16L,
            0L,
            List.of(new TableStatistics.FragmentStats(0, 16L, 1)),
            Map.of("category", new ColumnStatistics("category", List.of(bitmap)), "rating", new ColumnStatistics("rating", List.of(btree))),
            1L,
            Instant.EPOCH
        );
        assertEquals(0.2d, ScanAdmission.filterSelectivity("category = 'c1'", statistics), 1e-9);
        assertEquals(0.2d, ScanAdmission.filterSelectivity("`category` = 'c1'", statistics), 1e-9);
        // A range on the bitmap column, an equality on the BTree column
        // (no cardinality) and an unindexed column all fall back.
        assertEquals(ScanAdmission.FILTER_MATCH_RATIO_UNKNOWN, ScanAdmission.filterSelectivity("category >= 'c1'", statistics), 1e-9);
        assertEquals(ScanAdmission.FILTER_MATCH_RATIO_UNKNOWN, ScanAdmission.filterSelectivity("rating = 5", statistics), 1e-9);
        assertEquals(ScanAdmission.FILTER_MATCH_RATIO_UNKNOWN, ScanAdmission.filterSelectivity("price = 5", statistics), 1e-9);
        assertEquals(ScanAdmission.FILTER_MATCH_RATIO_UNKNOWN, ScanAdmission.filterSelectivity("subcategory = 'c1'", statistics), 1e-9);
        // The index a filter is answered from is the referenced column's.
        assertEquals("rating_idx", ScanAdmission.scalarIndexFor("rating = 5 AND price > 3", statistics).get().name());
        assertEquals("category_idx", ScanAdmission.scalarIndexFor("category = 'c1'", statistics).get().name());
        assertTrue(ScanAdmission.scalarIndexFor("price > 3", statistics).isEmpty());
        assertTrue(ScanAdmission.scalarIndexFor("", statistics).isEmpty());
    }

    public void testVectorIndexEstimateIsTheWholeIndexTwiceWhenThePartitionCountIsUnknown() {
        // perf10b: a 10B row IVF_PQ index without a size in the manifest,
        // 240 GB of codes and row addresses, read and concatenated: 480
        // GB, above what a fresh 512 GB node has after the headroom.
        long estimate = ScanAdmission.vectorIndexEstimateBytes(OptionalLong.empty(), TEN_BILLION, 200, 0L, 10, 0, 128, 8 * GB);
        assertEquals(TEN_BILLION * ScanAdmission.VECTOR_INDEX_BYTES_PER_ROW * ScanAdmission.IVF_PARTITION_LOAD_FACTOR, estimate);
        // The 16xlarge's MemFree at boot was 474 GB (decimal, the
        // kernel's kB), 466 GB after the headroom: below 480 GB.
        long freshNodeAvailable = 474L * 1_000_000_000L;
        assertFalse(ScanAdmission.decide(estimate, 0L, freshNodeAvailable, Long.MAX_VALUE, true, 8 * GB, 0L).admitted());
        // With the partition count known the probed share is small and
        // the refine step adds k x refine x dimension x 4.
        long probed = ScanAdmission.vectorIndexEstimateBytes(OptionalLong.of(240 * GB), TEN_BILLION, 200, 65_536L, 10, 10, 128, 1L);
        long expectedProbed = (long) (240 * GB * (200 / 65_536d)) * ScanAdmission.IVF_PARTITION_LOAD_FACTOR;
        assertEquals(expectedProbed + 10L * 10L * 128L * ScanAdmission.FLOAT32_BYTES, probed);
        // Probed partitions that fit the shard are cached: zero.
        assertEquals(0L, ScanAdmission.vectorIndexEstimateBytes(OptionalLong.of(240 * GB), TEN_BILLION, 200, 65_536L, 10, 0, 128, 8 * GB));
    }

    /** perf1b's IVF_PQ index as the statistics summarise it, with or without its partition count. */
    private static TableStatistics perf1bVectorStatistics(long rows, long indexSizeBytes, OptionalLong partitions) {
        ColumnStatistics.IndexSummary index = new ColumnStatistics.IndexSummary(
            "embedding_idx",
            Optional.of(IndexType.IVF_PQ),
            1,
            1,
            OptionalLong.of(indexSizeBytes),
            OptionalLong.of(rows),
            OptionalLong.of(0L),
            OptionalLong.empty(),
            partitions,
            true
        );
        return new TableStatistics(
            rows,
            0L,
            List.of(new TableStatistics.FragmentStats(0, rows, 1)),
            Map.of("embedding", new ColumnStatistics("embedding", List.of(index))),
            1L,
            Instant.EPOCH
        );
    }

    public void testVectorIndexEstimateScalesTheIndexByTheProbedShareOfTheStatisticsPartitionCount() {
        // perf1b: a 1B row IVF_PQ index of 19.5 GB in the manifest, on
        // a 128 GB node with 34 GB available after earlier scans. The
        // statistics report 1024 partitions; a lance_knn with nprobes
        // 200 probes one fifth of them, so the load is a fifth of the
        // index, doubled: 7.6 GB, within the 26 GB left after the
        // headroom.
        long rows = 1_000_000_000L;
        long size = 19_500_000_000L;
        TableStatistics statistics = perf1bVectorStatistics(rows, size, OptionalLong.of(1024L));
        ColumnStatistics.IndexSummary index = ScanAdmission.vectorIndexFor("embedding", statistics).get();
        assertEquals(OptionalLong.of(1024L), index.partitions());
        long estimate = ScanAdmission.vectorIndexEstimateBytes(
            index.sizeBytes(),
            rows,
            200,
            index.partitions().orElse(0L),
            1000,
            0,
            128,
            8 * GB
        );
        long expected = (long) (size * (200 / 1024d)) * ScanAdmission.IVF_PARTITION_LOAD_FACTOR;
        assertEquals(expected, estimate);
        assertEquals("about a fifth of the index, loaded twice", 0.39d, estimate / (double) size, 0.001d);
        assertTrue(ScanAdmission.decide(estimate, 0L, 34 * GB, Long.MAX_VALUE, true, 8 * GB, 0L).admitted());
    }

    public void testVectorIndexEstimateIsTheWholeIndexTwiceWhenTheStatisticsReportNoPartitionCount() {
        // The same index summarised without a partition count (the
        // statistics could not be read, or name none): the whole 19.5
        // GB doubled, 39 GB, refused at 34 GB available.
        long rows = 1_000_000_000L;
        long size = 19_500_000_000L;
        TableStatistics statistics = perf1bVectorStatistics(rows, size, OptionalLong.empty());
        ColumnStatistics.IndexSummary index = ScanAdmission.vectorIndexFor("embedding", statistics).get();
        assertEquals(OptionalLong.empty(), index.partitions());
        long estimate = ScanAdmission.vectorIndexEstimateBytes(
            index.sizeBytes(),
            rows,
            200,
            index.partitions().orElse(0L),
            1000,
            0,
            128,
            8 * GB
        );
        assertEquals(size * ScanAdmission.IVF_PARTITION_LOAD_FACTOR, estimate);
        assertFalse(ScanAdmission.decide(estimate, 0L, 34 * GB, Long.MAX_VALUE, true, 8 * GB, 0L).admitted());
    }

    public void testAdmitVectorSearchReadsThePartitionCountOfTheTableStatistics() throws Exception {
        // The indexed fixture trains one IVF partition, so with the
        // statistics installed the nearest scan is judged over one
        // partition and named as such, and the estimate is the index's
        // manifest size loaded twice under a one byte shard share.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeIndexedFixtureTable(scratchDir, "admission-" + getTestName(), 2, 150);
        ScanAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        ScanAdmission.setHeadroom(new ByteSizeValue(Long.MAX_VALUE / 2, ByteSizeUnit.BYTES));
        TableStatisticsCache cache = new TableStatisticsCache();
        ScanAdmission.setTableStatistics(cache);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            TableStatistics statistics = cache.forDataset(dataset);
            ColumnStatistics.IndexSummary index = ScanAdmission.vectorIndexFor("embedding", statistics).get();
            assertEquals(OptionalLong.of(1L), index.partitions());
            assertTrue(index.sizeBytes().isPresent());
            CircuitBreakingException refused = expectThrows(
                CircuitBreakingException.class,
                () -> ScanAdmission.admitVectorSearch("demo", dataset, "embedding", 3, 1, 0, 8, null)
            );
            String message = refused.getMessage();
            assertTrue(message, message.contains("ivf_pq index [embedding_ivf]"));
            assertTrue(message, message.contains("probed with nprobes 1 over 1 partitions"));
            assertEquals(index.sizeBytes().getAsLong() * ScanAdmission.IVF_PARTITION_LOAD_FACTOR, ScanAdmission.lastEstimateBytes());
            assertEquals(1L, ScanAdmission.rejections(ScanAdmission.Kind.VECTOR_INDEX));
        }
    }

    public void testAggregateScanEstimateIsTheReadQueueAndBatchesOfEveryParallelScan() {
        // terms(category) over 3.3B rows in 8 parallel scans on a 16
        // vCPU node: each scan reads 412M rows of 40 bytes (a 32 byte
        // key and the 8 byte count), capped at the 2 GiB read queue,
        // plus 16 batches of 8192 rows in flight, doubled.
        long rows = 3_300_000_000L;
        long width = ScanAdmission.UTF8_COLUMN_BYTES_PER_ROW + 8L;
        long estimate = ScanAdmission.aggregateScanEstimateBytes(8, rows, width, 16, 8 * GB);
        long perScan = ScanAdmission.IO_BUFFER_BYTES_PER_SCAN + (long) (16 * ScanAdmission.SCAN_BATCH_ROWS * width
            * ScanAdmission.SCAN_BUFFER_FACTOR);
        assertEquals(perScan * 8, estimate);
        // 16 scans on the 16xlarge.
        assertEquals(
            (ScanAdmission.IO_BUFFER_BYTES_PER_SCAN + (long) (64 * ScanAdmission.SCAN_BATCH_ROWS * width
                * ScanAdmission.SCAN_BUFFER_FACTOR)) * 16,
            ScanAdmission.aggregateScanEstimateBytes(16, TEN_BILLION, width, 64, 8 * GB)
        );
        // A 16 row fixture reads 640 bytes per scan: within the shard.
        assertEquals(0L, ScanAdmission.aggregateScanEstimateBytes(8, 16L, width, 16, 8 * GB));
        assertTrue(ScanAdmission.aggregateScanEstimateBytes(8, 16L, width, 16, 1L) > 0L);
        // The group state is heap: groups x (64 + 24 per metric) per scan.
        assertEquals(1_000_000L * (64L + 24L * 2) * 8, ScanAdmission.aggregateScanHeapBytes(1_000_000L, 2, 8));
    }

    public void testHeapTermIsJudgedAgainstTheBreakerRoomNotPhysicalMemory() {
        ScanAdmission.Decision fits = ScanAdmission.decide(0L, 3 * GB, 100 * GB, 4 * GB, true, 8 * GB, 0L);
        assertTrue(fits.admitted());
        ScanAdmission.Decision refused = ScanAdmission.decide(0L, 3 * GB, 100 * GB, 2 * GB, true, 8 * GB, 0L);
        assertFalse(refused.admitted());
        assertTrue(refused.heapRefused());
        // A native refusal is reported as such even with a heap term.
        ScanAdmission.Decision nativeRefused = ScanAdmission.decide(50 * GB, 1 * GB, 10 * GB, 4 * GB, true, 8 * GB, 0L);
        assertFalse(nativeRefused.admitted());
        assertFalse(nativeRefused.heapRefused());
        // A disabled gate admits both.
        assertTrue(ScanAdmission.decide(50 * GB, 3 * GB, 10 * GB, 2 * GB, false, 8 * GB, 0L).admitted());
    }

    public void testColumnWidthsFollowTheArrowType() {
        assertEquals(8L, ScanAdmission.columnWidthBytes(new Field("ts", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        assertEquals(4L, ScanAdmission.columnWidthBytes(new Field("i", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        assertEquals(1L, ScanAdmission.columnWidthBytes(new Field("b", FieldType.nullable(new ArrowType.Bool()), null)));
        assertEquals(
            ScanAdmission.UTF8_COLUMN_BYTES_PER_ROW,
            ScanAdmission.columnWidthBytes(new Field("s", FieldType.nullable(new ArrowType.Utf8()), null))
        );
        Field vector = new Field(
            "embedding",
            FieldType.nullable(new ArrowType.FixedSizeList(128)),
            List.of(new Field("item", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null))
        );
        assertEquals(512L, ScanAdmission.columnWidthBytes(vector));
        assertEquals(128, ScanAdmission.vectorDimension(List.of(vector), "embedding"));
        assertEquals(0, ScanAdmission.vectorDimension(List.of(vector), "other"));
        assertEquals(ScanAdmission.OTHER_COLUMN_BYTES_PER_ROW, ScanAdmission.columnWidthBytes(List.of(vector), "missing"));
    }

    // ---- the per kind counters and the request ticket ----

    public void testRejectionsAreCountedPerKindAndTheLastKindIsRecorded() {
        ScanAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        ScanAdmission.setHeadroom(new ByteSizeValue(Long.MAX_VALUE / 2, ByteSizeUnit.BYTES));
        assertEquals("none", ScanAdmission.lastKind());
        Map<String, Long> zero = ScanAdmission.rejectionsByKind();
        assertEquals(
            List.of("fts", "scalar_index", "vector_index", "filter_scan", "aggregate_scan", "column_load"),
            List.copyOf(zero.keySet())
        );
        for (long count : zero.values()) {
            assertEquals(0L, count);
        }
        expectThrows(CircuitBreakingException.class, () -> ScanAdmission.admit("demo", 1_000L, UNBOUNDED));
        assertEquals("fts", ScanAdmission.lastKind());
        assertEquals(1L, ScanAdmission.rejections(ScanAdmission.Kind.FTS));
        CircuitBreakingException filter = expectThrows(
            CircuitBreakingException.class,
            () -> ScanAdmission.admitFilterScan("demo", null, "rating = 5", 1_000L, 2, 0L, 8L, Long.MAX_VALUE, null)
        );
        assertTrue(filter.getMessage(), filter.getMessage().startsWith("[" + ScanAdmission.LABEL + "] filter_scan estimate"));
        assertEquals("filter_scan", ScanAdmission.lastKind());
        assertEquals(1L, ScanAdmission.rejections(ScanAdmission.Kind.FILTER_SCAN));
        // Without statistics the vector index has no size and no rows to
        // estimate from: the estimate is zero and the scan is admitted,
        // the decision recorded under its kind.
        ScanAdmission.admitVectorSearch("demo", null, "embedding", 10, 200, 0, 128, null);
        assertEquals("vector_index", ScanAdmission.lastKind());
        assertEquals(0L, ScanAdmission.lastEstimateBytes());
        expectThrows(
            CircuitBreakingException.class,
            () -> ScanAdmission.admitAggregateScan("demo", null, null, 8, 1_000L, 40L, 10L, 1, Long.MAX_VALUE)
        );
        assertEquals("aggregate_scan", ScanAdmission.lastKind());
        ScanAdmission.recordColumnLoad(4096L, true);
        assertEquals("column_load", ScanAdmission.lastKind());
        assertEquals(4096L, ScanAdmission.lastEstimateBytes());
        Map<String, Long> counts = ScanAdmission.rejectionsByKind();
        assertEquals(1L, (long) counts.get("fts"));
        assertEquals(0L, (long) counts.get("scalar_index"));
        assertEquals(0L, (long) counts.get("vector_index"));
        assertEquals(1L, (long) counts.get("filter_scan"));
        assertEquals(1L, (long) counts.get("aggregate_scan"));
        assertEquals(1L, (long) counts.get("column_load"));
        assertEquals(4L, ScanAdmission.rejections());
        // No statistics installed and no dataset: the sorted page is
        // judged on its rows alone, and a disabled gate admits it.
        expectThrows(
            CircuitBreakingException.class,
            () -> ScanAdmission.admitExecutorFilterScan("demo", null, "", 1_000L, 10L, 16L, "sorted page scan")
        );
        ScanAdmission.setEnabled(false);
        ScanAdmission.admitExecutorFilterScan("demo", null, "", 1_000L, 10L, 16L, "sorted page scan");
        ScanAdmission.admitFilterScan("demo", null, "rating = 5", 1_000L, 2, 0L, 8L, Long.MAX_VALUE, null);
        assertEquals(5L, ScanAdmission.rejections());
    }

    public void testOneRequestWithTwoAdmittedPathsCountsOnceInFlight() {
        ScanAdmission.setIndexCacheShardShareOverride(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        ScanAdmission.setHeadroom(new ByteSizeValue(8, ByteSizeUnit.GB));
        ScanAdmission.setMemoryProbeForTests(() -> 100 * GB);
        ScanAdmission.setResidentSetProbeForTests(() -> -1L);
        LanceHitsAccounting ticket = LanceHitsAccounting.unlimited();
        // A filter under a knn: the filter scan and the vector index are
        // admitted for the same request.
        ScanAdmission.admitFilterScan("demo", null, "rating = 5", 1_000L, 2, 0L, 8L, Long.MAX_VALUE, ticket);
        assertEquals(1, ScanAdmission.inFlightForTests());
        ScanAdmission.admitExecutorFilterScan("demo", null, "", 1_000L, 10L, 16L, "sorted page scan");
        assertEquals("a ticketless path on the same thread counts on the thread", 2, ScanAdmission.inFlightForTests());
        ScanAdmission.requestEnded();
        assertEquals(1, ScanAdmission.inFlightForTests());
        ScanAdmission.admitFilterScan("demo", null, "category = 'c1'", 1_000L, 2, 0L, 8L, Long.MAX_VALUE, ticket);
        assertEquals("the second path of the request does not double count", 1, ScanAdmission.inFlightForTests());
        // A second request on another ticket is another admission.
        LanceHitsAccounting other = LanceHitsAccounting.unlimited();
        ScanAdmission.admitFilterScan("demo", null, "rating = 5", 1_000L, 2, 0L, 8L, Long.MAX_VALUE, other);
        assertEquals(2, ScanAdmission.inFlightForTests());
        // Closing the accounting ends the request, once.
        ticket.close();
        assertEquals(1, ScanAdmission.inFlightForTests());
        ticket.close();
        assertEquals(1, ScanAdmission.inFlightForTests());
        other.close();
        assertEquals(0, ScanAdmission.inFlightForTests());
        // A ticketless admission on this thread is released by the
        // ticket's close on the same thread too.
        ScanAdmission.admit("demo", 1_000L, UNBOUNDED);
        assertEquals(1, ScanAdmission.inFlightForTests());
        LanceHitsAccounting.unlimited().close();
        assertEquals(0, ScanAdmission.inFlightForTests());
    }

    public void testBreakerRoomOfAnUnlimitedAccountingNeverRefusesHeap() {
        assertEquals(Long.MAX_VALUE, LanceHitsAccounting.unlimited().breakerRoomBytes());
    }
}
