/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import java.util.List;
import java.util.Set;

import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.lance.NativeMemoryLimit.IndexCacheSizing;
import org.opensearch.lance.rest.RestAttachAction.Derivation;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The attach time warning that a table's inverted index is heavier than
 * one index cache shard admits. The decision is a pure function of the
 * derivation and the Session's index cache sizing, so it is tested on
 * its message rather than on a captured log appender.
 */
public class TransportLanceAttachActionTests extends OpenSearchTestCase {

    private static final long GIB = 1L << 30;

    /** The default sizing on a 16 vCPU node: 2 shards of 8 GiB - 1. */
    private static final IndexCacheSizing DEFAULT_16_CPUS = NativeMemoryLimit.sizeIndexCache(19 * GIB, 16);

    private static Derivation derivation(long rows, Set<String> ftsColumns) {
        return new Derivation("{}", "", "long", "", 3L, rows, 4, List.of(), ftsColumns, Set.of("rating"), Set.of());
    }

    public void testWarnsWhenTheEstimateExceedsTheShardShare() {
        // 1B rows estimate to 52 GB, far above the 8 GiB share.
        String warning = TransportLanceAttachAction.invertedIndexShardShareWarning(
            "perf1b",
            derivation(1_000_000_000L, Set.of("body")),
            DEFAULT_16_CPUS
        );
        assertNotNull(warning);
        assertEquals(
            "inverted index of [perf1b] (~48.4gb) may not fit one index cache shard (7.9gb); "
                + "raise lance.native_memory.limit or lower lance.cache.column_share",
            warning
        );

        // The 19 GiB cache the plugin used to hand Lance as is: 4 shards
        // of 4.75 GiB, below the 100M estimate of 4.84 GiB.
        String at19 = TransportLanceAttachAction.invertedIndexShardShareWarning(
            "perf100m",
            derivation(100_000_000L, Set.of("body")),
            IndexCacheSizing.ofCapacity(19 * GIB, 16)
        );
        assertNotNull(at19);
        assertTrue(at19, at19.startsWith("inverted index of [perf100m] (~4.8gb) may not fit one index cache shard (4.7gb)"));
    }

    public void testSilentWhenTheEstimateFitsTheShardShare() {
        // 100M rows estimate to 4.84 GiB, within the 8 GiB share.
        assertNull(
            TransportLanceAttachAction.invertedIndexShardShareWarning("perf100m", derivation(100_000_000L, Set.of("body")), DEFAULT_16_CPUS)
        );
        // Exactly the share still fits: the cache refuses entries heavier
        // than the share, not entries equal to it.
        long rowsAtShare = DEFAULT_16_CPUS.shardShareBytes() / NativeMemoryLimit.invertedIndexEntryEstimateBytes(1L);
        assertNull(
            TransportLanceAttachAction.invertedIndexShardShareWarning("edge", derivation(rowsAtShare, Set.of("body")), DEFAULT_16_CPUS)
        );
        assertNotNull(
            TransportLanceAttachAction.invertedIndexShardShareWarning("edge", derivation(rowsAtShare + 1, Set.of("body")), DEFAULT_16_CPUS)
        );
    }

    public void testSilentWithoutAFullTextColumnOrASession() {
        // No inverted index, nothing to fit.
        assertNull(
            TransportLanceAttachAction.invertedIndexShardShareWarning("keywords", derivation(1_000_000_000L, Set.of()), DEFAULT_16_CPUS)
        );
        // No Session installed (unit test JVMs), so no share to compare with.
        assertNull(TransportLanceAttachAction.invertedIndexShardShareWarning("perf1b", derivation(1_000_000_000L, Set.of("body")), null));
    }
}
