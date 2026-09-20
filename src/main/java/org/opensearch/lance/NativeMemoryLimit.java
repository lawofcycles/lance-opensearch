/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import org.opensearch.OpenSearchParseException;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsProbe;

/**
 * Parse the {@code lance.native_memory.limit} node setting. The setting
 * accepts either an absolute {@link ByteSizeValue} (for example
 * {@code "10gb"}, {@code "512mb"}) or a percentage suffix
 * (for example {@code "40%"}) that resolves to a fraction of the memory
 * left on the host once the JVM heap is subtracted, matching the k-NN
 * plugin's {@code knn.memory.circuit_breaker.limit} semantics.
 *
 * <p>Formally, for a percentage {@code p} the resolved byte count is
 * {@code p / 100 * (physicalMemory - jvmHeapMax)}. The subtraction is
 * what makes the default useful across heterogeneous instance sizes:
 * on r7g.4xlarge (128 GiB physical / 31 GiB heap) 40% resolves to
 * roughly 38.8 GiB, while on t3.medium (4 GiB / 2 GiB heap) the same
 * 40% resolves to roughly 800 MiB.
 *
 * <p>The parsed value is split in two: a {@code lance.cache.column_share}
 * fraction goes to the fragment path's off-heap column cache, and the
 * rest to the two Lance {@link org.lance.Session} caches using Lance's own
 * default ratio of 6:1 (index cache to metadata cache), so operators
 * configure one number rather than tracking three caches separately.
 *
 * <p>The index cache is not handed its whole budget. Lance backs it with
 * a sharded {@code quick_cache} whose shards do not borrow capacity from
 * one another, and an entry heavier than one shard's share is refused
 * admission without an error. The shard count grows with the capacity
 * ({@link #recommendedShards}), so a larger capacity can hold a smaller
 * largest entry: 19 GiB on 16 CPUs is 4 shards of 4.75 GiB, while
 * 16 GiB minus one byte is 2 shards of 8 GiB. {@link #sizeIndexCache}
 * therefore picks, within the budget, the capacity whose per-shard
 * share is largest, and the difference to the budget stays unused.
 */
public final class NativeMemoryLimit {

    /** Index cache share of the Session part of the limit (6 / 7 in Lance's own defaults). */
    static final double INDEX_SHARE = 6.0 / 7.0;

    /** Metadata cache share of the Session part of the limit (1 / 7 in Lance's own defaults). */
    static final double METADATA_SHARE = 1.0 / 7.0;

    /**
     * Lance's minimum weight budget per index cache shard, 4 GiB
     * ({@code MIN_SHARD_SHARE} in {@code lance-core/src/cache/quick.rs}).
     * The capacity divided by this value is the capacity term of the
     * shard count.
     */
    static final long MIN_SHARD_SHARE_BYTES = 4L << 30;

    /** Upper clamp of Lance's shard count. */
    static final int MAX_SHARDS = 1024;

    /**
     * Upper estimate of the index cache weight of one inverted index per
     * table row. Lance keeps the whole {@code DocSet} of an inverted
     * index as one cache entry: {@code row_ids} (u64, 8 bytes),
     * {@code num_tokens} (u32, 4 bytes), {@code inv} ((u64, u32), 16
     * bytes with padding) and {@code doc_indices} (u32 per row per
     * coordinate rank, 4 bytes for a single rank), 32 bytes per row
     * before the slack that {@code Vec} growth leaves behind and the
     * weighter's key footprint. The measured entry of a 100M row table
     * is 4.82 GiB, which is 51.75 bytes per row; 52 rounds that up so
     * the estimate stays above the measurement.
     */
    static final long INVERTED_INDEX_BYTES_PER_ROW = 52L;

    private NativeMemoryLimit() {}

    /**
     * What the plugin hands Lance for the index cache and what Lance
     * makes of it: the budget the plugin had, the capacity it chose
     * within that budget, the shard count Lance derives from the
     * capacity and the resulting weight budget of one shard, which is
     * the largest entry the cache admits.
     */
    public record IndexCacheSizing(long budgetBytes, long capacityBytes, int shards, long shardShareBytes) {

        /** Part of the budget the plugin left unused to keep the shard share large. */
        public long unusedBytes() {
            return budgetBytes - capacityBytes;
        }

        /**
         * Describe a capacity handed to Lance as is, without choosing a
         * smaller one: budget and capacity are equal.
         */
        public static IndexCacheSizing ofCapacity(long capacityBytes, int cpus) {
            return new IndexCacheSizing(
                capacityBytes,
                capacityBytes,
                NativeMemoryLimit.recommendedShards(capacityBytes, cpus),
                NativeMemoryLimit.shardShareBytes(capacityBytes, cpus)
            );
        }
    }

    /**
     * Number of CPUs Lance sees when it sizes the index cache shards.
     * Lance reads {@code std::thread::available_parallelism}, which like
     * {@link Runtime#availableProcessors()} honours the process affinity
     * mask and a container's CPU quota, so on a node with a cgroup CPU
     * limit both report the limit and not the host's core count. The
     * two can still differ when the JVM is started with
     * {@code -XX:ActiveProcessorCount}, which only the JVM honours.
     */
    public static int availableCpus() {
        return Runtime.getRuntime().availableProcessors();
    }

    /**
     * Lance's {@code recommended_cache_shards}: the smaller of
     * {@code cpus / 2} and {@code capacity / 4 GiB}, at least 1, rounded
     * down to a power of two and clamped to [1, 1024]. The CPU term
     * bounds lock contention, the capacity term keeps each shard's
     * budget at 4 GiB or more.
     */
    public static int recommendedShards(long capacityBytes, int cpus) {
        long byCapacity = Math.max(0L, capacityBytes) / MIN_SHARD_SHARE_BYTES;
        long byCpu = Math.max(0, cpus) / 2;
        long shards = Math.max(1L, Math.min(byCapacity, byCpu));
        // Rounding down to a power of two: the highest set bit.
        shards = Long.highestOneBit(shards);
        return (int) Math.min(MAX_SHARDS, shards);
    }

    /**
     * Weight budget of one shard of an index cache of the given
     * capacity, which is the heaviest entry Lance admits. Shards do not
     * borrow from one another, so a 5 GiB inverted index is refused by a
     * 19 GiB cache on 16 CPUs (4 shards of 4.75 GiB) and kept by a
     * 20 GiB one (4 shards of 5 GiB).
     */
    public static long shardShareBytes(long capacityBytes, int cpus) {
        return Math.max(0L, capacityBytes) / recommendedShards(capacityBytes, cpus);
    }

    /**
     * Choose the index cache capacity within {@code budgetBytes} whose
     * shard share is largest. The candidates are the budget itself and
     * every {@code k * 4 GiB - 1} for {@code k = 2, 4, 8, ...} that is
     * at most the budget: one byte below {@code k * 4 GiB} the capacity
     * term of the shard count drops from {@code k} to {@code k - 1} and
     * the power of two rounding halves the shard count, so these are the
     * capacities right before the share falls. On a tie the larger
     * capacity wins.
     *
     * <p>With the default settings on a 16 CPU node with 128 GiB
     * (budget 19 GiB) this picks 16 GiB minus one byte: 2 shards of
     * 8 GiB instead of the budget's 4 shards of 4.75 GiB, so the 4.82 GiB
     * inverted index of a 100M row table stays cached between queries.
     */
    public static IndexCacheSizing sizeIndexCache(long budgetBytes, int cpus) {
        long budget = Math.max(0L, budgetBytes);
        long bestCapacity = budget;
        long bestShare = shardShareBytes(budget, cpus);
        for (long k = 2; k <= MAX_SHARDS; k <<= 1) {
            long candidate = k * MIN_SHARD_SHARE_BYTES - 1;
            if (candidate > budget) {
                break;
            }
            long share = shardShareBytes(candidate, cpus);
            if (share > bestShare || (share == bestShare && candidate > bestCapacity)) {
                bestCapacity = candidate;
                bestShare = share;
            }
        }
        return new IndexCacheSizing(budget, bestCapacity, recommendedShards(bestCapacity, cpus), bestShare);
    }

    /**
     * Upper estimate of the largest index cache entry of an inverted
     * index over {@code rows} rows, {@link #INVERTED_INDEX_BYTES_PER_ROW}
     * per row. Compared with {@link #shardShareBytes} to tell whether
     * the index can stay cached at all.
     */
    public static long invertedIndexEntryEstimateBytes(long rows) {
        return Math.max(0L, rows) * INVERTED_INDEX_BYTES_PER_ROW;
    }

    /**
     * Resolve a raw setting string into a byte count. Percentage-formatted
     * inputs are evaluated against the current host's physical memory
     * minus the JVM heap. Absolute inputs are parsed as
     * {@link ByteSizeValue}.
     */
    public static long parse(String rawValue, String settingName) {
        if (rawValue == null) {
            throw new OpenSearchParseException("[{}] cannot be null", settingName);
        }
        String trimmed = rawValue.trim();
        if (trimmed.endsWith("%")) {
            return parsePercent(trimmed, settingName);
        }
        return ByteSizeValue.parseBytesSizeValue(trimmed, null, settingName).getBytes();
    }

    /**
     * Off-heap budget of the fragment path's column cache: the
     * {@code lance.cache.column_share} fraction of the total limit. The
     * remainder ({@link #sessionCacheBytes}) goes to the Lance Session.
     */
    public static long columnCacheBytes(long totalBytes, double columnShare) {
        return (long) (totalBytes * columnShare);
    }

    /**
     * Part of the total limit left for the Lance Session's index and
     * metadata caches once the column cache has taken its share. Split
     * further by {@link #indexCacheBudgetBytes} and {@link #metadataCacheBytes}.
     */
    public static long sessionCacheBytes(long totalBytes, double columnShare) {
        return totalBytes - columnCacheBytes(totalBytes, columnShare);
    }

    /**
     * Index-cache byte budget: the Session part of the limit minus the
     * metadata cache, which is the {@link #INDEX_SHARE} ratio up to
     * rounding. This is the budget {@link #sizeIndexCache} chooses the
     * capacity within, not the capacity itself.
     */
    public static long indexCacheBudgetBytes(long sessionBytes) {
        return sessionBytes - metadataCacheBytes(sessionBytes);
    }

    /**
     * Metadata-cache byte budget derived from the Session part of the
     * limit using the {@link #METADATA_SHARE} ratio.
     */
    public static long metadataCacheBytes(long sessionBytes) {
        return (long) (sessionBytes * METADATA_SHARE);
    }

    private static long parsePercent(String rawValue, String settingName) {
        String percentAsString = rawValue.substring(0, rawValue.length() - 1);
        double percent;
        try {
            percent = Double.parseDouble(percentAsString);
        } catch (NumberFormatException e) {
            throw new OpenSearchParseException("[{}] percentage must be numeric, got [{}]", settingName, percentAsString);
        }
        if (percent < 0 || percent > 100) {
            throw new OpenSearchParseException("[{}] percentage must be in [0, 100], got [{}]", settingName, percentAsString);
        }
        long physicalBytes = OsProbe.getInstance().getTotalPhysicalMemorySize();
        if (physicalBytes <= 0) {
            throw new IllegalStateException("physical memory size could not be determined for [" + settingName + "]");
        }
        long heapMaxBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        long eligible = Math.max(0L, physicalBytes - heapMaxBytes);
        return (long) ((percent / 100.0) * eligible);
    }

    /**
     * Format a byte count as a human-readable string. Used purely for
     * log lines when the plugin reports the resolved limit at startup.
     */
    public static String humanReadable(long bytes) {
        return new ByteSizeValue(bytes, ByteSizeUnit.BYTES).toString();
    }
}
