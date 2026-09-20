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
 */
public final class NativeMemoryLimit {

    /** Index cache share of the Session part of the limit (6 / 7 in Lance's own defaults). */
    static final double INDEX_SHARE = 6.0 / 7.0;

    /** Metadata cache share of the Session part of the limit (1 / 7 in Lance's own defaults). */
    static final double METADATA_SHARE = 1.0 / 7.0;

    private NativeMemoryLimit() {}

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
     * further by {@link #indexCacheBytes} and {@link #metadataCacheBytes}.
     */
    public static long sessionCacheBytes(long totalBytes, double columnShare) {
        return totalBytes - columnCacheBytes(totalBytes, columnShare);
    }

    /**
     * Index-cache byte budget derived from the Session part of the limit
     * using the {@link #INDEX_SHARE} ratio.
     */
    public static long indexCacheBytes(long sessionBytes) {
        return (long) (sessionBytes * INDEX_SHARE);
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
