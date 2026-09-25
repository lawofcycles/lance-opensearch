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
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

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
 * <p>A lookup never collects on the calling thread. {@link #lookup}
 * answers the entry when it is held and {@code null} when it is not,
 * and on a miss starts one collection of the key on the executor the
 * cache was given (the node's generic pool), which opens the table
 * through the caller's opener, reads the statistics and stores them.
 * The caller plans without statistics this once; the next lookup of the
 * same version finds the entry. On a table of ten billion rows the
 * collection takes minutes (Lance assembles the index statistics from
 * the index files), which is why it must not sit on the request's
 * thread. {@link #prefetch} starts the same collection without a
 * lookup, so a node that holds the table's shard can have the entry
 * ready before the first request (the freshness check calls it when the
 * shard starts and when the manifest advances). A key is collected once
 * at a time: a second lookup or prefetch of a key whose collection is
 * running starts nothing.
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
    private final Executor executor;
    /** Access ordered, guarded by {@code this}. */
    private final LinkedHashMap<Key, TableStatistics> entries = new LinkedHashMap<>(16, 0.75f, true);
    /** The keys whose collection is queued or running; one collection per key at a time. */
    private final Map<Key, Boolean> pending = new ConcurrentHashMap<>();
    private final AtomicLong collects = new AtomicLong();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong collectMillisTotal = new AtomicLong();
    /** Milliseconds a collection waits before it reads the table; a test hook, zero on a real node. */
    private volatile long collectDelayMillis;

    /**
     * A cache whose collections run on the calling thread of
     * {@link #lookup} or {@link #prefetch}. For tests without a thread
     * pool: {@code lookup} still answers {@code null} on the miss that
     * started the collection, and the next lookup hits.
     */
    public TableStatisticsCache() {
        this(DEFAULT_MAX_ENTRIES, Runnable::run);
    }

    /**
     * @param maxEntries entries kept before the least recently used one is evicted
     * @param executor runs the collections; the node's generic pool in production
     */
    public TableStatisticsCache(int maxEntries, Executor executor) {
        this.maxEntries = Math.max(1, maxEntries);
        this.executor = executor;
    }

    /**
     * The statistics under {@code (tableUri, version)} when the cache
     * holds them, else {@code null}. A miss is counted as a plan made
     * without statistics and starts one collection of the key in the
     * background unless one is already queued or running: the
     * collection opens the table through {@code opener} (which must open
     * it at {@code version}), reads the statistics and closes it.
     */
    public TableStatistics lookup(String tableUri, long version, Supplier<Dataset> opener) {
        Key key = new Key(tableUri, version);
        TableStatistics cached = lookup(key);
        if (cached != null) {
            hits.incrementAndGet();
            return cached;
        }
        misses.incrementAndGet();
        start(key, opener);
        return null;
    }

    /**
     * Start collecting the statistics under {@code (tableUri, version)}
     * in the background when the cache neither holds them nor is
     * collecting them; {@code opener} opens the table at
     * {@code version} for the collection. Returns whether a collection
     * was started.
     */
    public boolean prefetch(String tableUri, long version, Supplier<Dataset> opener) {
        Key key = new Key(tableUri, version);
        if (lookup(key) != null) {
            return false;
        }
        return start(key, opener);
    }

    private boolean start(Key key, Supplier<Dataset> opener) {
        if (pending.putIfAbsent(key, Boolean.TRUE) != null) {
            return false;
        }
        try {
            executor.execute(() -> collect(key, opener));
        } catch (RejectedExecutionException e) {
            pending.remove(key);
            LOGGER.warn("table statistics of {} not collected: {}", key, e.getMessage());
            return false;
        }
        return true;
    }

    private void collect(Key key, Supplier<Dataset> opener) {
        try {
            long delay = collectDelayMillis;
            if (delay > 0L) {
                Thread.sleep(delay);
            }
            long startNanos = System.nanoTime();
            TableStatistics collected;
            try (Dataset dataset = opener.get()) {
                collected = TableStatisticsCollector.collect(dataset);
            }
            collectMillisTotal.addAndGet(wholeMillis(System.nanoTime() - startNanos));
            collects.incrementAndGet();
            put(key, collected);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.debug("table statistics collection of {} interrupted", key);
        } catch (RuntimeException e) {
            // Statistics are an input to plan quality, not to
            // correctness: the requests keep planning without them and
            // the next lookup tries again.
            LOGGER.warn("table statistics of {} could not be collected; requests plan without them", key, e);
        } finally {
            pending.remove(key);
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

    /** The cached entry for {@code (tableUri, version)}, or {@code null}; neither collects nor counts. */
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

    /** Keys whose collection is queued or running. */
    public int pendingCount() {
        return pending.size();
    }

    /** Collections completed. */
    public long collectCount() {
        return collects.get();
    }

    /** Lookups answered from the cache. */
    public long hitCount() {
        return hits.get();
    }

    /** Lookups that found no entry: plans made without statistics. */
    public long missCount() {
        return misses.get();
    }

    /** Milliseconds spent collecting, summed over every collection; each collection counts at least one. */
    public long collectMillisTotal() {
        return collectMillisTotal.get();
    }

    /**
     * Make every collection started from now on wait {@code millis}
     * before it reads the table; zero clears the wait. A test hook
     * ({@code lance.test.statistics_collect_delay}) that lets a request
     * against a small table observe the plan made without statistics.
     */
    public void setCollectDelayMillis(long millis) {
        this.collectDelayMillis = Math.max(0L, millis);
    }

    /** Drop every entry. */
    public synchronized void clear() {
        entries.clear();
    }
}
