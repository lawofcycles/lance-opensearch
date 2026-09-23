/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;

/**
 * Tombstone bookkeeping for the re-surface guard. When an operator
 * deletes a Lance-backed index through {@code DELETE /{index}}, the
 * poll cycle would otherwise recreate it on its next pass because the
 * table is still in the catalog. This guard records a tombstone for
 * every deleted Lance-backed index and answers whether a surface for
 * that name should be skipped: within the grace period the delete is
 * honoured, after it the tombstone is dropped and surfacing proceeds.
 */
final class LanceResurfaceGuard {

    private static final Logger LOG = LogManager.getLogger(LanceResurfaceGuard.class);

    /**
     * Deleted-index tombstones. Maps a Lance-backed index name to the
     * millisecond timestamp at which the {@code DELETE /{index}} was
     * observed on the cluster state. Entries only get added for
     * indexes carrying {@code index.lance.table} so plain OpenSearch
     * indexes never accumulate here.
     */
    private final Map<String, Long> tombstones = new ConcurrentHashMap<>();

    /**
     * Current grace period. Held in an {@link AtomicReference} so a
     * dynamic setting update from {@link org.opensearch.lance.LancePlugin}
     * can atomically swap it in without racing against the poll cycle.
     */
    private final AtomicReference<TimeValue> grace;

    LanceResurfaceGuard(TimeValue initialGrace) {
        this.grace = new AtomicReference<>(initialGrace);
    }

    /**
     * Swap in a new grace period. Zero or negative disables the guard
     * (the poll re-surfaces immediately).
     */
    void setGrace(TimeValue newGrace) {
        grace.set(newGrace);
    }

    /**
     * Record that a Lance-backed index was deleted at {@code now}
     * milliseconds, so the poll skips re-surfacing it until the grace
     * period expires.
     */
    void recordTombstone(String indexName, String table, long now) {
        tombstones.put(indexName, now);
        LOG.info("recording resurface tombstone for deleted Lance-backed index {} (table {})", indexName, table);
    }

    /**
     * Drop the tombstone for {@code indexName}. Called when the index
     * is present again (a snapshot restore, or the poll adopted it),
     * so the tombstone no longer describes anything.
     */
    void clearTombstone(String indexName) {
        tombstones.remove(indexName);
    }

    /**
     * Whether the surface of {@code indexName} should be skipped
     * because the index was deleted within the grace period. When the
     * grace has expired or the guard is disabled the tombstone is
     * cleared, so the map does not grow without bound, and surfacing
     * proceeds.
     */
    boolean shouldSkipSurface(String indexName, String table) {
        Long tombstonedAt = tombstones.get(indexName);
        if (tombstonedAt == null) {
            return false;
        }
        long graceMs = grace.get().millis();
        if (graceMs > 0 && System.currentTimeMillis() - tombstonedAt < graceMs) {
            LOG.debug("skipping surface of {} at {}: index was deleted within the resurface guard window", indexName, table);
            return true;
        }
        tombstones.remove(indexName);
        return false;
    }
}
