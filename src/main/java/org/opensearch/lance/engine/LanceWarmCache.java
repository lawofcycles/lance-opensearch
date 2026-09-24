/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.FixedBitSet;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.plan.metadata.TableStatisticsCache;

/**
 * Node scoped cache of everything a fragment path request needs to know
 * about a Lance table at one manifest version before it reads rows: the
 * open {@link Dataset}, the fragment list with each fragment's row count
 * and live-row bitmap, the set of full-text indexed columns and the
 * {@link LanceFragmentSchema}. One {@link Snapshot} per
 * {@code (index uuid, Lance version)}; the leaves of a request are light
 * views over it (see {@link LanceDirectoryReader#openForSnapshot}), so a
 * second request against the same version opens no dataset, asks Lance for
 * no index description and runs no schema pass. The shard engine
 * ({@link LanceEngineFactory.LanceReadOnlyEngine}) takes its whole-table
 * reader from the same snapshot, so GET, {@code _stats} and the fragment
 * path share one dataset and one column store per version on a node.
 *
 * <p>Numeric and boolean column data lives next to the snapshots in a
 * {@link ColumnStore}, off-heap, keyed by the same snapshot key, so it
 * disappears with the snapshot.
 *
 * <p>Lifecycle: {@link #acquire} hands out a {@link Lease} that holds a
 * reference on the snapshot until {@link Lease#release()}. A snapshot is
 * closed only when nothing references it: when it was retired (the table
 * moved on, the index was deleted, the cache was disabled) or when it is
 * the least recently used one above {@code max_snapshots}. The version a
 * request keys on is the one the coordinator observed when it enumerated
 * the fragments, so an index that follows the latest manifest sees a new
 * version on the request after an append exactly as it did when every
 * request opened the table itself, and the executor makes no Lance call
 * to find out.
 */
public final class LanceWarmCache implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(LanceWarmCache.class);

    /** Identity of a cached snapshot: the OpenSearch index (its settings decide the schema) at one Lance version. */
    public record SnapshotKey(String indexUuid, long version) {
        @Override
        public String toString() {
            return indexUuid + "@v" + version;
        }
    }

    /**
     * Fragment metadata the leaf views read: id, physical row count and,
     * for fragments with a deletion file, the live-row bitmap, computed on
     * first use with one {@code _rowaddr} scan and kept for the life of
     * the snapshot.
     */
    public static final class FragmentMeta {
        private final int id;
        private final int physicalRows;
        private final boolean hasDeletionFile;
        private volatile boolean liveDocsResolved;
        private FixedBitSet liveDocs;
        private int numDocs;
        private volatile boolean nestedLayoutResolved;
        private NestedDocLayout nestedLayout;

        FragmentMeta(int id, long physicalRows, boolean hasDeletionFile) {
            this.id = id;
            this.physicalRows = (int) physicalRows;
            this.hasDeletionFile = hasDeletionFile;
            if (!hasDeletionFile) {
                this.numDocs = this.physicalRows;
                this.liveDocs = null;
                this.liveDocsResolved = true;
            }
        }

        public int id() {
            return id;
        }

        /** Physical rows in the fragment, which is the leaf's {@code maxDoc}. */
        public int physicalRows() {
            return physicalRows;
        }

        public boolean hasDeletionFile() {
            return hasDeletionFile;
        }

        /**
         * Resolve the live-row bitmap once. Fragments without a deletion
         * file have no deleted rows and never scan. Fragments with one run
         * a {@code _rowaddr}-only scan (8 bytes per row, no payload
         * columns) to learn which offsets survive; Lucene's
         * {@code MatchAllDocsQuery} and doc value iterators walk
         * {@code 0..maxDoc} directly, so deleted physical rows must be
         * masked for them (Lance driven scans skip deleted rows on their
         * own).
         */
        void resolveLiveDocs(Dataset dataset) throws IOException {
            if (liveDocsResolved) {
                return;
            }
            synchronized (this) {
                if (liveDocsResolved) {
                    return;
                }
                FixedBitSet live = new FixedBitSet(physicalRows);
                int liveCount = 0;
                ScanOptions options = new ScanOptions.Builder().fragmentIds(Collections.singletonList(id))
                    .columns(Collections.emptyList())
                    .withRowAddress(true)
                    .build();
                try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                        for (int i = 0; i < root.getRowCount(); i++) {
                            int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                            live.set(offset);
                            liveCount++;
                        }
                    }
                } catch (Exception e) {
                    throw new IOException(e);
                }
                numDocs = liveCount;
                liveDocs = liveCount == physicalRows ? null : live;
                liveDocsResolved = true;
            }
        }

        /** Live-row bitmap, or {@code null} when every physical row is live. Call {@link #resolveLiveDocs} first. */
        FixedBitSet liveDocs() {
            return liveDocs;
        }

        int numDocs() {
            return numDocs;
        }

        /**
         * Resolve the nested doc id layout once for fragments of a table
         * with {@code List<Struct>} columns; a no-op when the schema has
         * none. One scan of the fragment projecting the nested columns
         * reads each row's element counts from the list offsets; the
         * layout is kept for the life of the snapshot, like the live-row
         * bitmap. The scan skips rows a deletion file hides, so deleted
         * rows contribute no child docs.
         */
        void resolveNestedLayout(Dataset dataset, LanceFragmentSchema schema) throws IOException {
            if (nestedLayoutResolved) {
                return;
            }
            synchronized (this) {
                if (nestedLayoutResolved) {
                    return;
                }
                if (schema.nestedColumns().isEmpty()) {
                    nestedLayout = null;
                    nestedLayoutResolved = true;
                    return;
                }
                String[] columns = schema.nestedColumns().toArray(new String[0]);
                int[][] counts = new int[columns.length][physicalRows];
                ScanOptions options = new ScanOptions.Builder().fragmentIds(Collections.singletonList(id))
                    .columns(Arrays.asList(columns))
                    .withRowAddress(true)
                    .build();
                try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                        for (int c = 0; c < columns.length; c++) {
                            ListVector list = (ListVector) root.getVector(columns[c]);
                            for (int i = 0; i < root.getRowCount(); i++) {
                                if (list.isNull(i)) {
                                    continue;
                                }
                                int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                                counts[c][offset] = list.getElementEndIndex(i) - list.getElementStartIndex(i);
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new IOException(e);
                }
                nestedLayout = NestedDocLayout.build(columns, physicalRows, counts);
                nestedLayoutResolved = true;
            }
        }

        /**
         * The nested doc id layout, or {@code null} when the schema has
         * no nested columns. Call {@link #resolveNestedLayout} first.
         */
        NestedDocLayout nestedLayout() {
            return nestedLayout;
        }
    }

    /**
     * Everything shared by every leaf view over one table version. The
     * dataset stays open for the life of the snapshot and is shared by
     * every request that holds a lease; Lance's {@code Dataset} is safe
     * for concurrent scans, as the shard engine's reader manager already
     * relies on.
     */
    public static final class Snapshot {
        private final SnapshotKey key;
        private final Dataset dataset;
        private final List<FragmentMeta> fragments;
        private final Map<Integer, FragmentMeta> fragmentsById;
        private final Set<String> ftsColumns;
        private final LanceFragmentSchema schema;
        private final LanceDirectoryReader.DataFileSizes dataFileSizes;
        private final boolean cached;
        private final AtomicInteger refCount = new AtomicInteger();
        private final AtomicBoolean retired = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();
        private volatile long lastAccessNanos;

        Snapshot(
            SnapshotKey key,
            Dataset dataset,
            List<FragmentMeta> fragments,
            Set<String> ftsColumns,
            LanceFragmentSchema schema,
            LanceDirectoryReader.DataFileSizes dataFileSizes,
            boolean cached
        ) {
            this.key = key;
            this.dataset = dataset;
            this.fragments = Collections.unmodifiableList(fragments);
            Map<Integer, FragmentMeta> byId = new HashMap<>(fragments.size() * 2);
            for (FragmentMeta meta : fragments) {
                byId.put(meta.id(), meta);
            }
            this.fragmentsById = Collections.unmodifiableMap(byId);
            this.ftsColumns = Collections.unmodifiableSet(ftsColumns);
            this.schema = schema;
            this.dataFileSizes = dataFileSizes;
            this.cached = cached;
            this.lastAccessNanos = System.nanoTime();
        }

        public SnapshotKey key() {
            return key;
        }

        /** Lance version this snapshot reads. */
        public long version() {
            return key.version();
        }

        /** Shared dataset; callers must not close it. */
        public Dataset dataset() {
            return dataset;
        }

        /** Fragments of the table at this version, in manifest order. */
        public List<FragmentMeta> fragments() {
            return fragments;
        }

        /** Fragment metadata by id, or {@code null} for an id the manifest does not list. */
        public FragmentMeta fragment(int fragmentId) {
            return fragmentsById.get(fragmentId);
        }

        /** Utf8 columns that carry a Lance full-text index. */
        public Set<String> ftsColumns() {
            return ftsColumns;
        }

        public LanceFragmentSchema schema() {
            return schema;
        }

        /**
         * Manifest-recorded byte total of the data files behind
         * {@link #fragments()}, read from the manifest when the snapshot
         * was built. The shard engine reports it as
         * {@code _stats} {@code docs.total_size_in_bytes}.
         */
        public LanceDirectoryReader.DataFileSizes dataFileSizes() {
            return dataFileSizes;
        }

        /** Whether the cache keeps this snapshot after the lease ends (false when the cache is disabled). */
        public boolean isCached() {
            return cached;
        }

        /** Requests currently holding a lease. */
        public int refCount() {
            return refCount.get();
        }

        public boolean isRetired() {
            return retired.get();
        }

        public boolean isClosed() {
            return closed.get();
        }

        private void closeNow() {
            if (closed.compareAndSet(false, true)) {
                try {
                    dataset.close();
                } catch (RuntimeException e) {
                    LOGGER.warn("failed to close the dataset of snapshot {}", key, e);
                }
            }
        }
    }

    /**
     * A request's hold on a snapshot. Release exactly once when the
     * request has closed its reader; the snapshot and its columns stay in
     * the cache for the next request.
     */
    public final class Lease implements Closeable {
        private final Snapshot snapshot;
        private final AtomicBoolean released = new AtomicBoolean();

        private Lease(Snapshot snapshot) {
            this.snapshot = snapshot;
        }

        public Snapshot snapshot() {
            return snapshot;
        }

        public void release() {
            if (released.compareAndSet(false, true)) {
                LanceWarmCache.this.release(snapshot);
            }
        }

        @Override
        public void close() {
            release();
        }
    }

    private final ColumnStore columnStore;
    private final TableStatisticsCache tableStatistics = new TableStatisticsCache();
    private final int maxSnapshots;
    private volatile boolean enabled;
    /** Guarded by {@code this}. */
    private final Map<SnapshotKey, Snapshot> snapshots = new LinkedHashMap<>();
    /** Serialises the build of one snapshot key so concurrent first requests open the table once. */
    private final Map<SnapshotKey, Object> buildLocks = new ConcurrentHashMap<>();
    private final AtomicLong datasetOpens = new AtomicLong();
    private final AtomicLong snapshotBuilds = new AtomicLong();
    private final AtomicLong snapshotHits = new AtomicLong();
    private final AtomicLong snapshotCloses = new AtomicLong();

    /**
     * @param allocator        parent of the column store's child allocator
     * @param columnLimitBytes off-heap budget of the column store
     * @param maxSnapshots     how many snapshots to keep before evicting
     *                         the least recently used unreferenced one
     * @param enabled          initial value of {@code lance.cache.enabled}
     */
    public LanceWarmCache(BufferAllocator allocator, long columnLimitBytes, int maxSnapshots, boolean enabled) {
        this.columnStore = new ColumnStore(allocator, columnLimitBytes);
        this.maxSnapshots = Math.max(1, maxSnapshots);
        this.enabled = enabled;
    }

    /**
     * Obtain the snapshot for {@code indexUuid} at {@code version},
     * building it on first use. The coordinator resolves the version
     * once per request when it enumerates the fragments (the pinned or
     * tag version, or the latest manifest it observed) and ships it, so
     * a warm request makes no Lance call here at all and every executor
     * of one request reads the same manifest. When no version arrives
     * the table's latest version is resolved locally: from the newest
     * cached dataset's {@link Dataset#latestVersion()} when there is
     * one, otherwise by opening the table. When the cache is disabled
     * the returned lease wraps a snapshot that is not stored and is
     * closed on release, so the request runs the way it did before the
     * cache existed: one dataset open, one schema pass, heap column
     * loads; concurrent requests for the same index each do their own.
     *
     * @param indexUuid      {@code IndexMetadata.getIndexUUID()} of the
     *                       Lance-backed index
     * @param tableUri       Lance table URI from the index settings
     * @param storageOptions object store options from the index settings
     * @param version        manifest version the coordinator enumerated
     *                       fragments from, or empty when none was
     *                       resolved
     * @param pkField        primary key column, or empty
     * @param pkType         primary key type family
     * @param overrides      per-column mapping overrides from the index
     *                       settings, nullable
     */
    public Lease acquire(
        String indexUuid,
        String tableUri,
        StorageOptions storageOptions,
        Optional<Long> version,
        String pkField,
        LancePrimaryKeyType pkType,
        LanceOverrides overrides
    ) throws IOException {
        // node_local placement: read this node's shallow clone instead of
        // the source. The version the caller resolved refers to the
        // source's manifest chain; the clone was created at that version
        // and index builds have advanced its own chain past it, so the
        // clone is opened at its latest version (which is where the built
        // indexes live) and the snapshot is keyed on that. A re-clone
        // (source version advance) retires every older snapshot of the
        // index so no request can be handed a snapshot whose dataset reads
        // a deleted clone directory.
        LanceLocalClones clones = LanceLocalClones.instance();
        if (clones != null) {
            Optional<LanceLocalClones.CloneLocation> clone = clones.locateForRead(indexUuid, tableUri, storageOptions, version);
            if (clone.isPresent()) {
                if (clone.get().recreated()) {
                    retireAll(indexUuid);
                }
                tableUri = clone.get().uri();
                storageOptions = StorageOptions.empty();
                version = Optional.empty();
            }
        }
        if (!enabled) {
            Dataset dataset = openDataset(tableUri, storageOptions, version);
            Snapshot transientSnapshot;
            try {
                transientSnapshot = build(new SnapshotKey(indexUuid, dataset.version()), dataset, pkField, pkType, overrides, false);
            } catch (IOException | RuntimeException e) {
                dataset.close();
                throw e;
            }
            transientSnapshot.refCount.incrementAndGet();
            return new Lease(transientSnapshot);
        }
        long resolved = version.orElse(-1L);
        if (resolved < 0) {
            Snapshot latestKnown = leaseLatestOf(indexUuid);
            if (latestKnown != null) {
                // No version on the request: ask the open dataset for
                // the table's latest version instead of opening the
                // table again. The probe holds a reference so a
                // concurrent retire cannot close the dataset underneath
                // it. A probe that fails (the table moved or vanished
                // since the snapshot was built, which a shard reopened
                // after its table went missing runs into) falls through
                // to the open below, so the caller sees Lance's regular
                // open error for the table rather than the probe's.
                try {
                    resolved = latestKnown.dataset.latestVersion();
                } catch (Exception e) {
                    LOGGER.debug("latest version probe on snapshot {} failed; opening the table instead", latestKnown.key, e);
                } finally {
                    release(latestKnown);
                }
            }
        }
        if (resolved >= 0) {
            Snapshot existing = lease(new SnapshotKey(indexUuid, resolved));
            if (existing != null) {
                snapshotHits.incrementAndGet();
                return new Lease(existing);
            }
        }
        // Build. When the version is still unknown the open resolves the
        // latest manifest and its version becomes the key.
        Dataset dataset = openDataset(tableUri, storageOptions, resolved >= 0 ? Optional.of(resolved) : Optional.empty());
        SnapshotKey key = new SnapshotKey(indexUuid, dataset.version());
        Object lock = buildLocks.computeIfAbsent(key, k -> new Object());
        synchronized (lock) {
            Snapshot existing = lease(key);
            if (existing != null) {
                dataset.close();
                snapshotHits.incrementAndGet();
                return new Lease(existing);
            }
            Snapshot built;
            try {
                built = build(key, dataset, pkField, pkType, overrides, true);
            } catch (IOException | RuntimeException e) {
                dataset.close();
                throw e;
            }
            List<Snapshot> evicted;
            synchronized (this) {
                built.refCount.incrementAndGet();
                built.lastAccessNanos = System.nanoTime();
                snapshots.put(key, built);
                evicted = evictOverflow();
            }
            closeAll(evicted);
            return new Lease(built);
        }
    }

    private Dataset openDataset(String tableUri, StorageOptions storageOptions, Optional<Long> version) {
        datasetOpens.incrementAndGet();
        return LanceRegistry.openDataset(tableUri, storageOptions, version);
    }

    private Snapshot build(
        SnapshotKey key,
        Dataset dataset,
        String pkField,
        LancePrimaryKeyType pkType,
        LanceOverrides overrides,
        boolean cached
    ) throws IOException {
        snapshotBuilds.incrementAndGet();
        Set<String> ftsColumns = LanceFragmentSchema.resolveFtsColumns(dataset);
        LanceFragmentSchema schema = LanceFragmentSchema.derive(dataset, pkField, pkType, overrides, ftsColumns);
        List<Fragment> lanceFragments = dataset.getFragments();
        List<FragmentMeta> fragments = new ArrayList<>(lanceFragments.size());
        for (Fragment fragment : lanceFragments) {
            fragments.add(
                new FragmentMeta(fragment.getId(), fragment.metadata().getPhysicalRows(), fragment.metadata().getDeletionFile() != null)
            );
        }
        LOGGER.debug("built snapshot {} with {} fragments", key, fragments.size());
        return new Snapshot(key, dataset, fragments, ftsColumns, schema, LanceDirectoryReader.sumDataFileSizes(lanceFragments), cached);
    }

    /** Take a reference on the cached snapshot for {@code key}, or return {@code null} when there is none usable. */
    private synchronized Snapshot lease(SnapshotKey key) {
        Snapshot snapshot = snapshots.get(key);
        if (snapshot == null || snapshot.isRetired() || snapshot.isClosed()) {
            return null;
        }
        snapshot.refCount.incrementAndGet();
        snapshot.lastAccessNanos = System.nanoTime();
        return snapshot;
    }

    /** Take a reference on the newest open snapshot of {@code indexUuid}, or return {@code null} when there is none. */
    private synchronized Snapshot leaseLatestOf(String indexUuid) {
        Snapshot latest = null;
        for (Snapshot snapshot : snapshots.values()) {
            if (snapshot.isClosed() || !snapshot.key.indexUuid().equals(indexUuid)) {
                continue;
            }
            if (latest == null || snapshot.version() > latest.version()) {
                latest = snapshot;
            }
        }
        if (latest != null) {
            latest.refCount.incrementAndGet();
        }
        return latest;
    }

    private void release(Snapshot snapshot) {
        boolean closeIt;
        synchronized (this) {
            int remaining = snapshot.refCount.decrementAndGet();
            closeIt = remaining == 0 && (!snapshot.cached || snapshot.isRetired());
            if (closeIt && snapshot.cached) {
                snapshots.remove(snapshot.key, snapshot);
            }
        }
        if (closeIt) {
            closeSnapshot(snapshot);
        }
    }

    /**
     * Least recently used unreferenced snapshots beyond {@code maxSnapshots}.
     * Caller holds {@code this} and closes the result outside the lock.
     */
    private List<Snapshot> evictOverflow() {
        List<Snapshot> evicted = new ArrayList<>();
        if (snapshots.size() <= maxSnapshots) {
            return evicted;
        }
        List<Snapshot> idle = new ArrayList<>();
        for (Snapshot snapshot : snapshots.values()) {
            if (snapshot.refCount() == 0) {
                idle.add(snapshot);
            }
        }
        idle.sort((a, b) -> Long.compare(a.lastAccessNanos, b.lastAccessNanos));
        Iterator<Snapshot> it = idle.iterator();
        while (snapshots.size() > maxSnapshots && it.hasNext()) {
            Snapshot victim = it.next();
            snapshots.remove(victim.key, victim);
            victim.retired.set(true);
            evicted.add(victim);
        }
        return evicted;
    }

    private void closeAll(List<Snapshot> toClose) {
        for (Snapshot snapshot : toClose) {
            closeSnapshot(snapshot);
        }
    }

    private void closeSnapshot(Snapshot snapshot) {
        if (snapshot.isClosed()) {
            return;
        }
        if (snapshot.cached && !keyServedByAnother(snapshot)) {
            columnStore.dropSnapshot(snapshot.key);
            releaseTableStatistics(snapshot);
        }
        snapshot.closeNow();
        snapshotCloses.incrementAndGet();
        LOGGER.debug("closed snapshot {}", snapshot.key);
    }

    /**
     * Drop the planner's table statistics of the version this snapshot
     * read: the statistics cache keys on the dataset's URI and version,
     * so the entry the plan construction filled while it held a lease
     * on this snapshot goes when the snapshot does. Read before the
     * dataset closes, because the URI comes from the open dataset.
     */
    private void releaseTableStatistics(Snapshot snapshot) {
        try {
            tableStatistics.release(snapshot.dataset.uri(), snapshot.version());
        } catch (RuntimeException e) {
            LOGGER.debug("could not release the table statistics of snapshot {}", snapshot.key, e);
        }
    }

    /**
     * Whether a different, open snapshot is filed under {@code snapshot}'s
     * key. That happens when a retired snapshot is still leased (the
     * shard engine's reader after {@code lance.cache.enabled} went off and
     * on again) while a later acquire built a replacement for the same
     * version. The store keys columns by {@code (index uuid, version)},
     * which names the same rows for both, so the columns stay with the
     * replacement instead of being dropped from under its readers.
     */
    private synchronized boolean keyServedByAnother(Snapshot snapshot) {
        Snapshot current = snapshots.get(snapshot.key);
        return current != null && current != snapshot && !current.isClosed();
    }

    /**
     * Retire every snapshot of {@code indexUuid} whose version is not
     * {@code currentVersion}: the freshness check saw the table (or its
     * tag) move there, so requests will key on the new version from now
     * on. Snapshots nobody references close at once; the others close
     * when their last lease is released.
     */
    public void retire(String indexUuid, long currentVersion) {
        retireMatching(indexUuid, currentVersion);
    }

    /** Retire every snapshot of {@code indexUuid}; used when the index is deleted. */
    public void retireAll(String indexUuid) {
        retireMatching(indexUuid, null);
    }

    private void retireMatching(String indexUuid, Long keepVersion) {
        List<Snapshot> toClose = new ArrayList<>();
        synchronized (this) {
            Iterator<Snapshot> it = snapshots.values().iterator();
            while (it.hasNext()) {
                Snapshot snapshot = it.next();
                if (indexUuid != null && !snapshot.key.indexUuid().equals(indexUuid)) {
                    continue;
                }
                if (keepVersion != null && snapshot.version() == keepVersion) {
                    continue;
                }
                snapshot.retired.set(true);
                if (snapshot.refCount() == 0) {
                    it.remove();
                    toClose.add(snapshot);
                }
            }
        }
        closeAll(toClose);
    }

    /**
     * Dynamic {@code lance.cache.enabled}. Turning the cache off retires
     * every snapshot; requests from then on build a transient snapshot
     * each and close it when they end.
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (!enabled) {
            retireMatching(null, null);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** Off-heap store of numeric and boolean columns, shared by every snapshot. */
    public ColumnStore columnStore() {
        return columnStore;
    }

    /**
     * The node's cache of planner table statistics, keyed on (table URI,
     * manifest version). Filled by the plan construction from the
     * dataset of the snapshot it holds, and by the coordinator from the
     * dataset it opens to enumerate fragments; an entry is dropped when
     * the snapshot of its version closes.
     */
    public TableStatisticsCache tableStatistics() {
        return tableStatistics;
    }

    /** Off-heap bytes the column store holds, for the circuit breaker sampler. */
    public long columnCacheBytes() {
        return columnStore.allocatedBytes();
    }

    /** Snapshots currently held (referenced or idle). */
    public synchronized int snapshotCount() {
        return snapshots.size();
    }

    /**
     * Held snapshots that were retired (the table moved on, the index was
     * deleted, the cache was disabled) but are still referenced by a
     * reader, most often the shard engine's previous reader waiting for
     * its last searcher. They close when that reference is released.
     */
    public synchronized int retiredSnapshotCount() {
        int retired = 0;
        for (Snapshot snapshot : snapshots.values()) {
            if (snapshot.isRetired()) {
                retired++;
            }
        }
        return retired;
    }

    /** Snapshot of {@code key} if held, for tests. */
    synchronized Snapshot snapshot(SnapshotKey key) {
        return snapshots.get(key);
    }

    /** Times the cache opened a dataset (snapshot builds and latest-version resolution). */
    public long datasetOpenCount() {
        return datasetOpens.get();
    }

    /** Times a snapshot was built (schema pass and index description). */
    public long snapshotBuildCount() {
        return snapshotBuilds.get();
    }

    /** Acquires served by an existing snapshot. */
    public long snapshotHitCount() {
        return snapshotHits.get();
    }

    /** Snapshots closed through retire, eviction or disable. */
    public long snapshotCloseCount() {
        return snapshotCloses.get();
    }

    @Override
    public void close() {
        List<Snapshot> toClose;
        synchronized (this) {
            toClose = new ArrayList<>(snapshots.values());
            snapshots.clear();
        }
        for (Snapshot snapshot : toClose) {
            snapshot.retired.set(true);
            closeSnapshot(snapshot);
        }
        tableStatistics.clear();
        columnStore.close();
    }
}
