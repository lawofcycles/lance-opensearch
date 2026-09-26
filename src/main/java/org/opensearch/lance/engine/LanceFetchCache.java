/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.lance.stats.LanceNodeStats;

/**
 * The node's cache of the rows behind the hits: one cell per entry,
 * keyed on {@code (index uuid, manifest version, row address, column)},
 * holding the value {@link LanceStoredFields} decoded from a
 * {@code _rowaddr IN (...)} take. A page whose rows the node took for
 * an earlier request (a dashboard refresh, a step back in a pagination,
 * the same search again) renders {@code _id}, {@code _source} and
 * {@code fields} from these entries and issues no take for them; on
 * object storage every take is a round trip of tens of milliseconds, so
 * the second page and every one after it lose that share of their time.
 *
 * <p>A Lance fragment is immutable and an append, a deletion or a
 * compaction writes a new manifest version, so the value of a row's
 * cell at one version never changes: an entry is valid for as long as
 * the key names a version that exists, and freshness is a matter of
 * the key alone, the same way {@link LanceWarmCache} keys its
 * snapshots. The entries of a version are dropped eagerly when its
 * snapshot closes ({@link #invalidate}) and when the index is deleted
 * (the cache listens to cluster state), and otherwise leave the cache
 * least recently used at {@code lance.fetch_cache.size}.
 *
 * <p>The key is per column because every request projects its own
 * columns (its {@code _source} filter, its {@code fields}, the primary
 * key): a request that took the key column alone leaves entries a later
 * request for every column finds for that column, and a row whose every
 * projected column is held is served without a take. A row with some
 * columns held is taken whole (a take of the missing columns alone would
 * give one request two projections), and the take writes every column
 * back, so the next request finds the whole row.
 *
 * <p>A row the take did not return (deleted between the scan that
 * produced the hit and the take, which the pinned version makes
 * unlikely but not impossible) is held as a negative entry
 * ({@link Entry#MISSING}) per column, so the fetch does not take it
 * again either.
 *
 * <p>An index with a reader wrapper (the security plugin's document and
 * field level security) is never read from or written to the cache: the
 * wrapper hides fields after the row is rendered, so the cache would
 * hold what one user must not see and hand it to the next. The
 * fragment hits phase decides per leaf from the reader chain
 * ({@link LanceFragmentLeafReader#wrappedOnlyByOwnReaders}) and marks
 * the leaf ({@link LanceFragmentLeafReader#setFetchCacheEligible}); the
 * rows of an ineligible leaf count as {@code skipped}.
 *
 * <p>The store is OpenSearch's {@link Cache}, weighed by an estimate of
 * the heap the key and the decoded value cost ({@link #weightOf}), so a
 * long text column or a wide struct takes the room it uses and the
 * eviction keeps the budget honest. A cell above
 * {@code lance.fetch_cache.max_entry_size} is not stored; its row is
 * then taken on every request. The cache is per node: two data nodes
 * that execute the same fragment (after the node list changed) each
 * take and hold their own copy.
 */
public final class LanceFetchCache implements ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(LanceFetchCache.class);

    /** What a lookup returns for a row the table did not have at the version: an empty row. */
    public static final Object[] MISSING_ROW = new Object[0];

    /**
     * The cache key: one cell of one row of one table version. The row
     * address is Lance's {@code (fragment id << 32) | row offset}.
     */
    public record Key(String indexUuid, long version, long rowAddress, String column) {
        public Key {
            Objects.requireNonNull(indexUuid, "indexUuid");
            Objects.requireNonNull(column, "column");
        }

        /**
         * The heap the key costs: the record with two longs and two
         * references. The strings are shared with the snapshot key and
         * the schema, so they are not counted again.
         */
        static long weight() {
            return 48L;
        }
    }

    /**
     * One held cell: the decoded value (null for an Arrow null), the
     * estimate of its heap, and when it was stored, for
     * {@code lance.fetch_cache.expire}. {@link #MISSING} stands for a
     * row the take did not return.
     */
    static final class Entry {
        static final Entry MISSING = new Entry(null, 0L, 0L, true);

        final Object value;
        final long bytes;
        final long storedAtMillis;
        final boolean missing;

        Entry(Object value, long bytes, long storedAtMillis) {
            this(value, bytes, storedAtMillis, false);
        }

        private Entry(Object value, long bytes, long storedAtMillis, boolean missing) {
            this.value = value;
            this.bytes = bytes;
            this.storedAtMillis = storedAtMillis;
            this.missing = missing;
        }

        /** What the entry weighs next to its key: the object and its value. */
        long weight() {
            return 32L + bytes;
        }
    }

    /**
     * The cache seen from one table version: what a leaf over a
     * fragment of that version reads and writes. Holds the index uuid
     * and the version so a leaf forms keys without knowing them; one
     * per {@link LanceWarmCache.Snapshot}, shared by every leaf over it.
     */
    public final class Table {
        private final String indexUuid;
        private final long version;

        private Table(String indexUuid, long version) {
            this.indexUuid = indexUuid;
            this.version = version;
        }

        public String indexUuid() {
            return indexUuid;
        }

        public long version() {
            return version;
        }

        /**
         * The row at {@code rowAddress} projected to {@code columns}, in
         * that order, when the cache holds every one of them; the
         * {@link #MISSING_ROW} when the cache knows the take did not
         * return the row; null when at least one column is not held (a
         * miss, per column). Every column looked up counts as a hit or
         * a miss; a row found whole or found missing counts as served.
         * Null when the cache is disabled, without counting.
         */
        public Object[] lookup(long rowAddress, List<String> columns) {
            if (!enabled || columns.isEmpty()) {
                return null;
            }
            Object[] row = new Object[columns.size()];
            int absent = 0;
            for (int c = 0; c < row.length; c++) {
                Entry entry = get(new Key(indexUuid, version, rowAddress, columns.get(c)));
                if (entry == null) {
                    absent++;
                    continue;
                }
                if (entry.missing) {
                    // The row was not in the table at this version, so no
                    // column of it is: the columns not looked at yet are
                    // hits as well.
                    hits.add(row.length - c - 1);
                    rowsServed.increment();
                    return MISSING_ROW;
                }
                row[c] = entry.value;
            }
            if (absent > 0) {
                return null;
            }
            rowsServed.increment();
            return row;
        }

        /**
         * Stores the cells of {@code row}, one per column of
         * {@code columns} in that order. A cell above the entry bound is
         * not stored; the others are. Nothing is stored while the cache
         * is disabled.
         */
        public void put(long rowAddress, List<String> columns, Object[] row) {
            if (!enabled) {
                return;
            }
            long now = System.currentTimeMillis();
            for (int c = 0; c < columns.size() && c < row.length; c++) {
                long bytes = weightOf(row[c]);
                if (bytes > maxEntryBytes) {
                    LOGGER.debug(
                        "lance.fetch_cache: cell of column [{}] weighs {} bytes, above the entry bound of {} bytes, not cached",
                        columns.get(c),
                        bytes,
                        maxEntryBytes
                    );
                    continue;
                }
                cache.put(new Key(indexUuid, version, rowAddress, columns.get(c)), new Entry(row[c], bytes, now));
            }
        }

        /** Records that the take did not return the row at {@code rowAddress}, for every column of {@code columns}. */
        public void putMissing(long rowAddress, List<String> columns) {
            if (!enabled) {
                return;
            }
            for (String column : columns) {
                cache.put(new Key(indexUuid, version, rowAddress, column), Entry.MISSING);
            }
        }

        /** Counts {@code rows} rows a leaf took without the cache because its reader chain carries a wrapper. */
        public void skipped(int rows) {
            if (enabled) {
                skipped.add(rows);
            }
        }

        @Override
        public String toString() {
            return "LanceFetchCache.Table[" + indexUuid + "@v" + version + "]";
        }
    }

    private final Cache<Key, Entry> cache;
    private final long limitBytes;
    private final long maxEntryBytes;
    private volatile boolean enabled;
    private volatile long expireMillis;
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder evictions = new LongAdder();
    private final LongAdder invalidations = new LongAdder();
    private final LongAdder skipped = new LongAdder();
    private final LongAdder rowsServed = new LongAdder();

    /**
     * @param limitBytes    the most the entries may weigh together
     * @param maxEntryBytes the heaviest cell stored
     * @param enabled       the initial {@code lance.fetch_cache.enabled}
     * @param expire        the initial {@code lance.fetch_cache.expire}; zero or null for none
     */
    public LanceFetchCache(long limitBytes, long maxEntryBytes, boolean enabled, TimeValue expire) {
        this.limitBytes = limitBytes;
        this.maxEntryBytes = maxEntryBytes;
        this.enabled = enabled;
        this.expireMillis = expire == null ? 0L : expire.millis();
        this.cache = CacheBuilder.<Key, Entry>builder()
            .setMaximumWeight(limitBytes)
            .weigher((key, entry) -> Key.weight() + entry.weight())
            .removalListener(notification -> {
                if (notification.getRemovalReason() == RemovalReason.EVICTED) {
                    evictions.increment();
                }
            })
            .build();
    }

    /** The view of the table version {@code key} names. */
    public Table table(LanceWarmCache.SnapshotKey key) {
        return new Table(key.indexUuid(), key.version());
    }

    /** The view of {@code indexUuid} at {@code version}. */
    public Table table(String indexUuid, long version) {
        return new Table(indexUuid, version);
    }

    /** Current {@code lance.fetch_cache.enabled}. */
    public boolean isEnabled() {
        return enabled;
    }

    /** Applies {@code lance.fetch_cache.enabled}; turning the cache off drops every entry. */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (!enabled) {
            cache.invalidateAll();
        }
    }

    /** Applies {@code lance.fetch_cache.expire}; zero keeps entries until their version is dropped or they are evicted. */
    public void setExpire(TimeValue expire) {
        this.expireMillis = expire == null ? 0L : expire.millis();
    }

    /** The entry under {@code key}, or null; an expired entry is dropped and counts as a miss and an eviction. */
    Entry get(Key key) {
        Entry entry = cache.get(key);
        if (entry != null && expireMillis > 0L && !entry.missing && System.currentTimeMillis() - entry.storedAtMillis > expireMillis) {
            cache.invalidate(key, entry);
            evictions.increment();
            entry = null;
        }
        if (entry == null) {
            misses.increment();
        } else {
            hits.increment();
        }
        return entry;
    }

    /**
     * The heap {@code value} roughly costs, for the weigher and the
     * entry bound: a string two bytes a character plus its object, a
     * byte array its length plus the header, a boxed number its box, a
     * decoded struct (a map) or nested list its elements plus the
     * container, a geo point its two doubles, a keyword array its
     * strings. Null costs nothing beyond the key.
     */
    static long weightOf(Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof String s) {
            return 48L + 2L * s.length();
        }
        if (value instanceof byte[] b) {
            return 16L + b.length;
        }
        if (value instanceof Boolean) {
            return 16L;
        }
        if (value instanceof Number) {
            return 24L;
        }
        if (value instanceof double[] d) {
            return 16L + 8L * d.length;
        }
        if (value instanceof long[] l) {
            return 16L + 8L * l.length;
        }
        if (value instanceof Object[] array) {
            long total = 16L + 4L * array.length;
            for (Object element : array) {
                total += weightOf(element);
            }
            return total;
        }
        if (value instanceof Map<?, ?> map) {
            long total = 48L;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                total += 32L + weightOf(entry.getKey()) + weightOf(entry.getValue());
            }
            return total;
        }
        if (value instanceof List<?> list) {
            long total = 48L;
            for (Object element : list) {
                total += weightOf(element);
            }
            return total;
        }
        // A type the decoder does not produce today; count a box worth.
        return 64L;
    }

    /**
     * Drops every entry of {@code indexUuid} at {@code version} and
     * returns how many were dropped; each counts as an invalidation. The
     * snapshot cache calls this when the snapshot of that version closes.
     */
    public int invalidate(String indexUuid, long version) {
        return invalidateMatching(key -> key.indexUuid().equals(indexUuid) && key.version() == version);
    }

    /**
     * Drops every entry of the indexes with {@code indexUuids} and
     * returns how many were dropped; each counts as an invalidation.
     */
    public int invalidateIndexes(Collection<String> indexUuids) {
        if (indexUuids.isEmpty()) {
            return 0;
        }
        Set<String> uuids = Set.copyOf(indexUuids);
        return invalidateMatching(key -> uuids.contains(key.indexUuid()));
    }

    private int invalidateMatching(Predicate<Key> matches) {
        // The keys are collected before anything is invalidated: the
        // cache's key iteration is undefined under a concurrent mutation.
        List<Key> matching = new ArrayList<>();
        for (Key key : cache.keys()) {
            if (matches.test(key)) {
                matching.add(key);
            }
        }
        for (Key key : matching) {
            cache.invalidate(key);
        }
        invalidations.add(matching.size());
        return matching.size();
    }

    /** Drops the entries of every index that left the cluster state. */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        List<Index> deleted = event.indicesDeleted();
        if (deleted.isEmpty()) {
            return;
        }
        List<String> uuids = new ArrayList<>(deleted.size());
        for (Index index : deleted) {
            uuids.add(index.getUUID());
        }
        int dropped = invalidateIndexes(uuids);
        if (dropped > 0) {
            LOGGER.debug("lance.fetch_cache: dropped {} entries of deleted indexes {}", dropped, deleted);
        }
    }

    /** Entries held right now. */
    public int count() {
        return cache.count();
    }

    /** Bytes the entries weigh right now. */
    public long weight() {
        return cache.weight();
    }

    /** Cells served from the cache since the node started. */
    public long hitCount() {
        return hits.sum();
    }

    /** Cells looked up and not held since the node started. */
    public long missCount() {
        return misses.sum();
    }

    /** Rows rendered without a take since the node started. */
    public long rowsServedCount() {
        return rowsServed.sum();
    }

    /** Rows taken without the cache because their leaf's reader chain carries a wrapper. */
    public long skippedCount() {
        return skipped.sum();
    }

    /**
     * The cache's figures for {@code GET /_lance/stats}: the setting in
     * force, the bytes held against the limit, the entries, and since
     * the node started the cells served ({@code hits}), the cells looked
     * up and not held ({@code misses}), the entries dropped for room or
     * age ({@code evictions}), the entries dropped with their version or
     * their index ({@code invalidations}), the rows not cached because
     * of a reader wrapper ({@code skipped}) and the rows rendered
     * without a take ({@code rowsServed}).
     */
    public LanceNodeStats.FetchCacheStats stats() {
        return new LanceNodeStats.FetchCacheStats(
            enabled,
            cache.weight(),
            limitBytes,
            cache.count(),
            hits.sum(),
            misses.sum(),
            evictions.sum(),
            invalidations.sum(),
            skipped.sum(),
            rowsServed.sum()
        );
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "LanceFetchCache[entries=%d, bytes=%d, limit=%d]", cache.count(), cache.weight(), limitBytes);
    }
}
