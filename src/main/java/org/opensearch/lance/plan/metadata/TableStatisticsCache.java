/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Node scoped cache of {@link TableStatistics}, one entry per
 * {@code (table URI, manifest version)}. A manifest version is immutable,
 * so an entry never goes stale; it is dropped when the version stops
 * being read. Two callers key into it: the coordinator, which opens the
 * table itself when it enumerates fragments and looks the statistics up
 * under the version it observed, and the plan construction on a data
 * node, which looks them up under the version of the warm cache snapshot
 * it holds. The two run on the same node when a data node coordinates,
 * and then share the entry.
 *
 * <p>Bounds: {@link #release} drops the entry of a version whose warm
 * cache snapshot closed; inserting a version of a table keeps at most
 * the previous version of the same table next to it, so a table that
 * follows its manifest holds two entries at most (the version in flight
 * and the one requests may still be finishing on); and the least
 * recently used entry beyond {@code maxEntries} is evicted, which bounds
 * the coordinator side where no snapshot lifecycle releases entries.
 */
public final class TableStatisticsCache {

    private static final Logger LOGGER = LogManager.getLogger(TableStatisticsCache.class);

    /** Default bound on the number of entries. */
    public static final int DEFAULT_MAX_ENTRIES = 1024;

    /** Identity of an entry: the table at one manifest version. */
    public record Key(String tableUri, long version) {
        @Override
        public String toString() {
            return tableUri + "@v" + version;
        }
    }

    private final int maxEntries;
    /** Access ordered, guarded by {@code this}. */
    private final LinkedHashMap<Key, TableStatistics> entries = new LinkedHashMap<>(16, 0.75f, true);
    /** Serialises the collection of one key so concurrent first lookups collect once. */
    private final Map<Key, Object> collectLocks = new ConcurrentHashMap<>();
    private final AtomicLong collects = new AtomicLong();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong collectMillisTotal = new AtomicLong();

    public TableStatisticsCache() {
        this(DEFAULT_MAX_ENTRIES);
    }

    /** @param maxEntries entries kept before the least recently used one is evicted */
    public TableStatisticsCache(int maxEntries) {
        this.maxEntries = Math.max(1, maxEntries);
    }

    /**
     * The statistics of {@code dataset} at its current version, keyed on
     * {@code dataset.uri()} and {@code dataset.version()}, collected
     * from it on a miss.
     */
    public TableStatistics forDataset(Dataset dataset) {
        return forVersion(dataset.uri(), dataset.version(), dataset);
    }

    /**
     * The statistics under {@code (tableUri, version)}, collected from
     * {@code dataset} on a miss. {@code dataset} must be open at
     * {@code version}; it is only read when the entry is missing.
     */
    public TableStatistics forVersion(String tableUri, long version, Dataset dataset) {
        Key key = new Key(tableUri, version);
        TableStatistics cached = lookup(key);
        if (cached != null) {
            hits.incrementAndGet();
            return cached;
        }
        Object lock = collectLocks.computeIfAbsent(key, k -> new Object());
        try {
            synchronized (lock) {
                cached = lookup(key);
                if (cached != null) {
                    hits.incrementAndGet();
                    return cached;
                }
                long startNanos = System.nanoTime();
                TableStatistics collected = TableStatisticsCollector.collect(dataset);
                collectMillisTotal.addAndGet(wholeMillis(System.nanoTime() - startNanos));
                collects.incrementAndGet();
                put(key, collected);
                return collected;
            }
        } finally {
            collectLocks.remove(key, lock);
        }
    }

    /**
     * {@code nanos} as milliseconds rounded up, so a collection that
     * finished inside a millisecond still counts as one: the total is a
     * counter of time spent that must grow with every collection.
     */
    static long wholeMillis(long nanos) {
        return Math.max(1L, (nanos + 999_999L) / 1_000_000L);
    }

    /** The cached entry for {@code (tableUri, version)}, or {@code null}; does not collect. */
    public synchronized TableStatistics peek(String tableUri, long version) {
        return entries.get(new Key(tableUri, version));
    }

    private synchronized TableStatistics lookup(Key key) {
        return entries.get(key);
    }

    private synchronized void put(Key key, TableStatistics statistics) {
        entries.put(key, statistics);
        // Keep the previous version of the same table and drop older ones.
        List<Key> older = new ArrayList<>();
        long previous = Long.MIN_VALUE;
        for (Key other : entries.keySet()) {
            if (other.tableUri().equals(key.tableUri()) && other.version() < key.version()) {
                older.add(other);
                previous = Math.max(previous, other.version());
            }
        }
        for (Key other : older) {
            if (other.version() != previous) {
                entries.remove(other);
                LOGGER.debug("dropped table statistics {} (superseded by version {})", other, key.version());
            }
        }
        Iterator<Key> it = entries.keySet().iterator();
        while (entries.size() > maxEntries && it.hasNext()) {
            Key victim = it.next();
            if (victim.equals(key)) {
                continue;
            }
            it.remove();
            LOGGER.debug("evicted table statistics {} (cache holds {} entries)", victim, maxEntries);
        }
    }

    /**
     * Drop the entry of {@code (tableUri, version)}; called when the warm
     * cache closes the snapshot that read this version. A miss is a
     * no-op. The next lookup of the same version collects again.
     */
    public void release(String tableUri, long version) {
        TableStatistics removed;
        synchronized (this) {
            removed = entries.remove(new Key(tableUri, version));
        }
        if (removed != null) {
            LOGGER.debug("released table statistics {}", new Key(tableUri, version));
        }
    }

    /** Entries currently held. */
    public synchronized int size() {
        return entries.size();
    }

    /** Collections performed (misses). */
    public long collectCount() {
        return collects.get();
    }

    /** Lookups answered from the cache. */
    public long hitCount() {
        return hits.get();
    }

    /** Milliseconds spent collecting, summed over every collection; each collection counts at least one. */
    public long collectMillisTotal() {
        return collectMillisTotal.get();
    }

    /** Drop every entry. */
    public synchronized void clear() {
        entries.clear();
    }
}
