/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import org.opensearch.OpenSearchParseException;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link NativeMemoryLimit}. The parser has two flavours: an
 * absolute {@code ByteSizeValue} form (e.g. {@code "10gb"}) and a
 * percentage form (e.g. {@code "40%"}). The percentage flavour uses the
 * live host's memory numbers, so this class covers both the deterministic
 * absolute case with exact assertions and the percentage case with a
 * relaxed assertion tied to the same {@link OsProbe} / {@link JvmInfo}
 * calls that the parser makes at runtime.
 */
public class NativeMemoryLimitTests extends OpenSearchTestCase {

    private static final String KEY = "lance.native_memory.limit";

    public void testParsesAbsoluteByteValues() {
        assertEquals(10L * 1024 * 1024 * 1024, NativeMemoryLimit.parse("10gb", KEY));
        assertEquals(512L * 1024 * 1024, NativeMemoryLimit.parse("512mb", KEY));
        assertEquals(1L * 1024, NativeMemoryLimit.parse("1kb", KEY));
        assertEquals(0L, NativeMemoryLimit.parse("0b", KEY));
    }

    public void testParsesPercentageAgainstEligibleMemory() {
        long physical = OsProbe.getInstance().getTotalPhysicalMemorySize();
        long heap = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        long eligible = Math.max(0L, physical - heap);

        long fortyPercent = NativeMemoryLimit.parse("40%", KEY);
        assertEquals((long) (0.40 * eligible), fortyPercent);

        long fullEligible = NativeMemoryLimit.parse("100%", KEY);
        assertEquals(eligible, fullEligible);

        assertEquals(0L, NativeMemoryLimit.parse("0%", KEY));
    }

    public void testPercentageOutOfRangeRejected() {
        // Above 100% would exceed the host's eligible memory and is a
        // configuration error, so surface the malformed input rather
        // than silently clamp.
        OpenSearchParseException high = expectThrows(OpenSearchParseException.class, () -> NativeMemoryLimit.parse("150%", KEY));
        assertTrue(high.getMessage(), high.getMessage().contains("percentage must be in [0, 100]"));

        OpenSearchParseException low = expectThrows(OpenSearchParseException.class, () -> NativeMemoryLimit.parse("-1%", KEY));
        assertTrue(low.getMessage(), low.getMessage().contains("percentage must be in [0, 100]"));
    }

    public void testPercentageMustBeNumeric() {
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> NativeMemoryLimit.parse("abc%", KEY));
        assertTrue(e.getMessage(), e.getMessage().contains("percentage must be numeric"));
    }

    public void testNullValueRejected() {
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> NativeMemoryLimit.parse(null, KEY));
        assertTrue(e.getMessage(), e.getMessage().contains("cannot be null"));
    }

    public void testSplitBetweenIndexAndMetadataFollowsLanceDefaultRatio() {
        // Lance's own defaults are 6 GiB index cache and 1 GiB metadata
        // cache, so we split the shared limit 6:1 to preserve that
        // ratio when operators configure a single number.
        long total = 7_000_000L;
        long indexBytes = NativeMemoryLimit.indexCacheBudgetBytes(total);
        long metadataBytes = NativeMemoryLimit.metadataCacheBytes(total);

        assertEquals(6_000_000L, indexBytes);
        assertEquals(1_000_000L, metadataBytes);
        assertEquals("index budget and metadata cache add up to the session part", total, indexBytes + metadataBytes);
    }

    private static final long GIB = 1L << 30;

    /** 16 vCPUs, the r7gd.4xlarge the measurements were taken on. */
    private static final int CPUS_16 = 16;

    /**
     * Measured index cache weight of the inverted index of a 100M row
     * table: {@code session.size_bytes()} read 4.82 GiB once it was
     * retained.
     */
    private static final long ENTRY_100M = (long) (4.82 * GIB);

    public void testRecommendedShardsFollowsLanceRule() {
        // capacity / 4 GiB, capped by cpus / 2, rounded down to a power
        // of two, never below 1.
        assertEquals(1, NativeMemoryLimit.recommendedShards(0L, CPUS_16));
        assertEquals(1, NativeMemoryLimit.recommendedShards(4 * GIB - 1, CPUS_16));
        assertEquals(1, NativeMemoryLimit.recommendedShards(4 * GIB, CPUS_16));
        assertEquals(1, NativeMemoryLimit.recommendedShards(8 * GIB - 1, CPUS_16));
        assertEquals(2, NativeMemoryLimit.recommendedShards(8 * GIB, CPUS_16));
        assertEquals(2, NativeMemoryLimit.recommendedShards(12 * GIB, CPUS_16));
        assertEquals(2, NativeMemoryLimit.recommendedShards(16 * GIB - 1, CPUS_16));
        assertEquals(4, NativeMemoryLimit.recommendedShards(16 * GIB, CPUS_16));
        assertEquals(4, NativeMemoryLimit.recommendedShards(19 * GIB, CPUS_16));
        assertEquals(4, NativeMemoryLimit.recommendedShards(31 * GIB, CPUS_16));
        assertEquals(8, NativeMemoryLimit.recommendedShards(32 * GIB, CPUS_16));
        // The CPU term caps at 8 on 16 vCPUs however large the capacity.
        assertEquals(8, NativeMemoryLimit.recommendedShards(64 * GIB, CPUS_16));
        assertEquals(8, NativeMemoryLimit.recommendedShards(1024 * GIB, CPUS_16));
        // 64 vCPUs allow 32 shards; 6 vCPUs allow 2 (6 / 2 = 3, rounded down).
        assertEquals(32, NativeMemoryLimit.recommendedShards(1024 * GIB, 64));
        assertEquals(2, NativeMemoryLimit.recommendedShards(1024 * GIB, 6));
        // One or two CPUs give one shard; the 1024 clamp holds for huge machines.
        assertEquals(1, NativeMemoryLimit.recommendedShards(1024 * GIB, 1));
        assertEquals(1, NativeMemoryLimit.recommendedShards(1024 * GIB, 2));
        assertEquals(1024, NativeMemoryLimit.recommendedShards(8192 * GIB, 4096));
    }

    public void testShardShareReproducesTheMeasuredAdmissionTable() {
        // pylance on the 16 vCPU host: the 100M inverted index stayed
        // cached at 20 to 31 GiB and at 40 GiB and above, and was dropped
        // at 16 to 19 GiB and at 32 to 36 GiB. Fits when the entry is not
        // heavier than one shard's share.
        assertFits(false, 16 * GIB);
        assertFits(false, 18 * GIB);
        assertFits(false, 19 * GIB);
        assertFits(true, 20 * GIB);
        assertFits(true, 24 * GIB);
        assertFits(true, 28 * GIB);
        assertFits(true, 30 * GIB);
        assertFits(true, 31 * GIB);
        assertFits(false, 32 * GIB);
        assertFits(false, 36 * GIB);
        assertFits(true, 40 * GIB);
        assertFits(true, 60 * GIB);
        // The shares behind the table.
        assertEquals(4 * GIB, NativeMemoryLimit.shardShareBytes(16 * GIB, CPUS_16));
        assertEquals(19 * GIB / 4, NativeMemoryLimit.shardShareBytes(19 * GIB, CPUS_16));
        assertEquals(5 * GIB, NativeMemoryLimit.shardShareBytes(20 * GIB, CPUS_16));
        assertEquals(4 * GIB, NativeMemoryLimit.shardShareBytes(32 * GIB, CPUS_16));
        assertEquals(5 * GIB, NativeMemoryLimit.shardShareBytes(40 * GIB, CPUS_16));
    }

    private static void assertFits(boolean expected, long capacity) {
        long share = NativeMemoryLimit.shardShareBytes(capacity, CPUS_16);
        assertEquals(
            "capacity " + NativeMemoryLimit.humanReadable(capacity) + " gives a share of " + NativeMemoryLimit.humanReadable(share),
            expected,
            ENTRY_100M <= share
        );
    }

    public void testSizeIndexCachePicksTheLargestShareWithinTheBudget() {
        // r7gd.4xlarge defaults: limit 37 GiB, column_share 0.4 leaves a
        // 22.2 GiB session, 1/7 of it (3.17 GiB) is metadata, the index
        // budget is 19.03 GiB. The budget itself would be 4 shards of
        // 4.76 GiB and drop the 4.82 GiB entry; 16 GiB - 1 is 2 shards of
        // 8 GiB - 1 and keeps it.
        long limit = 37 * GIB;
        long session = NativeMemoryLimit.sessionCacheBytes(limit, 0.4);
        long budget = NativeMemoryLimit.indexCacheBudgetBytes(session);
        assertTrue(NativeMemoryLimit.humanReadable(budget), budget > 19 * GIB && budget < 19 * GIB + GIB / 10);
        assertTrue("the budget as is drops the entry", ENTRY_100M > NativeMemoryLimit.shardShareBytes(budget, CPUS_16));

        NativeMemoryLimit.IndexCacheSizing sizing = NativeMemoryLimit.sizeIndexCache(budget, CPUS_16);
        assertEquals(budget, sizing.budgetBytes());
        assertEquals(16 * GIB - 1, sizing.capacityBytes());
        assertEquals(2, sizing.shards());
        assertEquals(8 * GIB - 1, sizing.shardShareBytes());
        assertEquals(budget - (16 * GIB - 1), sizing.unusedBytes());
        assertTrue("the chosen capacity keeps the 100M entry", ENTRY_100M <= sizing.shardShareBytes());

        // Below 8 GiB there is one shard and the budget is the share.
        NativeMemoryLimit.IndexCacheSizing small = NativeMemoryLimit.sizeIndexCache(6 * GIB, CPUS_16);
        assertEquals(6 * GIB, small.capacityBytes());
        assertEquals(1, small.shards());
        assertEquals(6 * GIB, small.shardShareBytes());
        assertEquals(0L, small.unusedBytes());

        // Between 8 GiB and 16 GiB the budget has 2 shards; 8 GiB - 1 with
        // one shard ties or wins on share, so a 12 GiB budget (6 GiB per
        // shard) is cut to 8 GiB - 1 and a 16 GiB - 1 budget is kept.
        assertEquals(8 * GIB - 1, NativeMemoryLimit.sizeIndexCache(12 * GIB, CPUS_16).capacityBytes());
        assertEquals(16 * GIB - 1, NativeMemoryLimit.sizeIndexCache(16 * GIB - 1, CPUS_16).capacityBytes());

        // A budget of 20 GiB has 4 shards of 5 GiB; 16 GiB - 1 has 2 of
        // 8 GiB - 1, so the smaller capacity wins on share.
        NativeMemoryLimit.IndexCacheSizing twenty = NativeMemoryLimit.sizeIndexCache(20 * GIB, CPUS_16);
        assertEquals(16 * GIB - 1, twenty.capacityBytes());
        assertEquals(4 * GIB + 1, twenty.unusedBytes());

        // Once the CPU cap holds the shard count at 8, a larger capacity
        // means a larger share again: 64 GiB is 8 shards of 8 GiB, which
        // ties 64 GiB - 1 and wins as the larger capacity; 80 GiB is 8 of
        // 10 GiB and is taken as is.
        assertEquals(64 * GIB, NativeMemoryLimit.sizeIndexCache(64 * GIB, CPUS_16).capacityBytes());
        NativeMemoryLimit.IndexCacheSizing eighty = NativeMemoryLimit.sizeIndexCache(80 * GIB, CPUS_16);
        assertEquals(80 * GIB, eighty.capacityBytes());
        assertEquals(8, eighty.shards());
        assertEquals(10 * GIB, eighty.shardShareBytes());
        assertEquals(0L, eighty.unusedBytes());

        // Zero budget stays zero and does not divide by zero.
        NativeMemoryLimit.IndexCacheSizing zero = NativeMemoryLimit.sizeIndexCache(0L, CPUS_16);
        assertEquals(0L, zero.capacityBytes());
        assertEquals(1, zero.shards());
        assertEquals(0L, zero.shardShareBytes());
    }

    public void testSizeIndexCacheOnSixtyFourCpus() {
        // r7gd.16xlarge with a 148 GiB limit: session 88.8 GiB, metadata
        // 12.69 GiB, index budget 76.11 GiB. On 64 vCPUs the CPU term is
        // 32, so the budget as is would be 16 shards of 4.76 GiB. Every
        // k * 4 GiB - 1 candidate has a share of 8 GiB - 1 (the integer
        // part of 8 GiB - 2 / k), so the largest of them, 64 GiB - 1 with
        // 8 shards, is chosen.
        long limit = 148 * GIB;
        long budget = NativeMemoryLimit.indexCacheBudgetBytes(NativeMemoryLimit.sessionCacheBytes(limit, 0.4));
        assertEquals(16, NativeMemoryLimit.recommendedShards(budget, 64));
        NativeMemoryLimit.IndexCacheSizing sizing = NativeMemoryLimit.sizeIndexCache(budget, 64);
        assertEquals(64 * GIB - 1, sizing.capacityBytes());
        assertEquals(8, sizing.shards());
        assertEquals(8 * GIB - 1, sizing.shardShareBytes());
        assertTrue(ENTRY_100M <= sizing.shardShareBytes());
        assertTrue("about 12 GiB stay unused", sizing.unusedBytes() > 12 * GIB && sizing.unusedBytes() < 13 * GIB);
    }

    public void testInvertedIndexEntryEstimateCoversTheMeasuredEntry() {
        // 52 bytes per row: the 100M row measurement (4.82 GiB, 51.75
        // bytes per row) rounded up, so the estimate is never below it.
        long estimate = NativeMemoryLimit.invertedIndexEntryEstimateBytes(100_000_000L);
        assertEquals(5_200_000_000L, estimate);
        assertTrue(estimate >= ENTRY_100M);
        assertTrue("the estimate is within 2% of the measurement", estimate - ENTRY_100M < ENTRY_100M / 50);
        // 1B rows need 52 GB in one shard; on a 16 vCPU node (8 shards
        // at most) that is a capacity of 416 GB or more.
        assertEquals(52_000_000_000L, NativeMemoryLimit.invertedIndexEntryEstimateBytes(1_000_000_000L));
        assertEquals(0L, NativeMemoryLimit.invertedIndexEntryEstimateBytes(0L));
        // The default sizing on the 16 vCPU host keeps 100M and not 1B.
        long budget = NativeMemoryLimit.indexCacheBudgetBytes(NativeMemoryLimit.sessionCacheBytes(37 * GIB, 0.4));
        long share = NativeMemoryLimit.sizeIndexCache(budget, CPUS_16).shardShareBytes();
        assertTrue(NativeMemoryLimit.invertedIndexEntryEstimateBytes(100_000_000L) <= share);
        assertTrue(NativeMemoryLimit.invertedIndexEntryEstimateBytes(1_000_000_000L) > share);
    }

    public void testHumanReadableRoundTripsAcrossUnits() {
        assertEquals("1gb", NativeMemoryLimit.humanReadable(1L * 1024 * 1024 * 1024));
        assertEquals("512mb", NativeMemoryLimit.humanReadable(512L * 1024 * 1024));
        assertEquals("0b", NativeMemoryLimit.humanReadable(0L));
    }
}
