/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.fragment.DataFile;
import org.lance.index.IndexDescription;
import org.lance.schema.LanceField;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.query.LanceHitsAccounting;

/** DirectoryReader whose leaves are Lance fragments. */
public final class LanceDirectoryReader extends DirectoryReader {

    /**
     * Byte total the Lance manifest records for the data files behind a
     * set of fragments.
     *
     * @param knownBytes       sum of {@code DataFile.getFileSizeBytes()} over
     *                         every data file whose size the manifest
     *                         records
     * @param filesWithoutSize number of data files the manifest lists
     *                         without a size (older writers did not record
     *                         one); those files contribute nothing to
     *                         {@code knownBytes}, so the total is a lower
     *                         bound whenever this is non-zero
     */
    public record DataFileSizes(long knownBytes, int filesWithoutSize) {
        public static final DataFileSizes NONE = new DataFileSizes(0L, 0);
    }

    /**
     * Sum the manifest-recorded sizes of every data file the given fragments
     * reference. Reads only the in-memory manifest ({@code Fragment.metadata()}
     * is a field access on an already materialised {@code FragmentMetadata});
     * no object-store request is made, which is why this runs on every
     * reader open rather than {@code Dataset.calculateDataSize()}, which
     * fetches each data file's footer. Data overlay files are included
     * through {@code getReferencedLanceFiles()}; deletion files and index
     * files are not data files and are left out.
     */
    public static DataFileSizes sumDataFileSizes(List<Fragment> fragments) {
        long knownBytes = 0L;
        int filesWithoutSize = 0;
        for (Fragment fragment : fragments) {
            for (DataFile dataFile : fragment.metadata().getReferencedLanceFiles()) {
                Long size = dataFile.getFileSizeBytes();
                if (size == null) {
                    filesWithoutSize++;
                } else {
                    knownBytes += size;
                }
            }
        }
        return new DataFileSizes(knownBytes, filesWithoutSize);
    }

    /**
     * Cut a sequence of fragments, given as their physical row counts in
     * order, into contiguous groups whose row total stays within
     * {@code maxDocs}, and return the end index (exclusive) of every
     * group. A single Lucene composite reader refuses leaves whose
     * {@code maxDoc} sum exceeds {@link IndexWriter#MAX_DOCS}, and a
     * fragment leaf's {@code maxDoc} is its physical row count, so this
     * is the unit a reader may hold. A fragment alone above the bound
     * forms a group of its own (a reader over it fails; attach refuses
     * such a table). An empty input yields no group.
     */
    public static int[] groupEnds(long[] physicalRows, long maxDocs) {
        List<Integer> ends = new ArrayList<>();
        long inGroup = 0L;
        for (int i = 0; i < physicalRows.length; i++) {
            long rows = physicalRows[i];
            if (inGroup > 0L && inGroup + rows > maxDocs) {
                ends.add(i);
                inGroup = 0L;
            }
            inGroup += rows;
        }
        if (physicalRows.length > 0) {
            ends.add(physicalRows.length);
        }
        int[] result = new int[ends.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = ends.get(i);
        }
        return result;
    }

    /**
     * How many leading fragments of {@code physicalRows} one reader may
     * hold under {@code maxDocs}: the first group of
     * {@link #groupEnds}, zero for no fragments.
     */
    public static int leadingFragmentsWithinBound(long[] physicalRows, long maxDocs) {
        int[] ends = groupEnds(physicalRows, maxDocs);
        return ends.length == 0 ? 0 : ends[0];
    }

    private final IndexCommit commit;
    // Every row of the table version this reader was opened over, live or
    // deleted, whether or not the reader holds it. Equal to maxDoc()
    // unless the bound cut the reader short.
    private final long totalRows;
    // Live rows of the table version (Dataset.countRows), or -1 when the
    // reader holds every fragment and numDocs() is that count already.
    private final long liveRows;
    // Data file byte total of the fragments this reader exposes, computed
    // once at open from the manifest the reader was built from. The engine
    // reports it through DocsStats.totalSizeInBytes; a refresh that swaps
    // in a new reader recomputes it for the new manifest version.
    private final DataFileSizes dataFileSizes;
    // The engine hands us a freshly opened Dataset when it builds a new reader,
    // so this reader takes ownership of it and closes it when the reader is
    // closed. Lucene's ReferenceManager releases the previous reader once the
    // last in-flight searcher completes, which is the point where we also want
    // to release the Lance native handle it was reading from. A reader built
    // over a LanceWarmCache snapshot (openForSnapshot) borrows the snapshot's
    // dataset instead and leaves it open for the next request.
    private final Dataset dataset;
    private final boolean ownsDataset;
    // Hold on the LanceWarmCache snapshot this reader was opened over, or
    // null. The shard engine's whole-table reader owns its lease and
    // releases it in doClose, which is when Lucene's ReferenceManager has
    // seen the last searcher of a swapped out reader go away; a fragment
    // path reader does not own one because its request releases the lease
    // itself after closing the reader.
    private final LanceWarmCache.Lease lease;
    // Column coordinator of this reader's leaves; its store pins are
    // released when the reader closes.
    private final LanceShardColumnCache columnCache;
    // Bridge to Lucene's cache lifecycle at the composite reader level. See the
    // matching field on LanceFragmentLeafReader for the rationale: OpenSearch's
    // request cache keys entries by IndexReader.CacheKey, and only Lucene's own
    // org.apache.lucene.index classes can construct one. Holding a one-doc
    // Lucene reader whose lifetime tracks this reader's lifetime lets us surface
    // a real CacheHelper without reimplementing Lucene's cache internals.
    private final DirectoryReader cacheLifetimeBridge;

    /**
     * Open a reader whose leaves are every fragment of {@code dataset},
     * opening the table for itself. The shard-level engine
     * ({@link LanceEngineFactory.LanceReadOnlyEngine}) uses this only when
     * it has no {@link LanceWarmCache} to take a snapshot from; with one it
     * goes through {@link #openForSnapshot(Directory, IndexCommit,
     * LanceWarmCache.Lease, ColumnStore, CircuitBreaker)} instead. The RFC's
     * shard-partitioning scheme (fragment id modulo shard count) was
     * retired when {@code number_of_shards} was dropped from attach;
     * {@link #openForFragments} is the fan-out variant used by the fragment
     * path.
     *
     * <p>Every fragment in the dataset is surfaced as a leaf.
     *
     * @param directory the Lucene {@link Directory} the reader reports to
     *                  Lucene's own bookkeeping.
     * @param commit    marker retained through the reader lifecycle.
     * @param dataset   the Lance dataset; the returned reader takes
     *                  ownership and closes it on {@link #close()}.
     * @param intField  primary key column name for {@code _id} lookups; empty
     *                  string when the Lance table has no declared primary
     *                  key.
     * @param pkType    Arrow type family of the declared primary key. When
     *                  it is {@link LancePrimaryKeyType#KEYWORD}
     *                  the reader holds string PK values so {@code _id}
     *                  echoes them verbatim; otherwise (including
     *                  {@link LancePrimaryKeyType#NONE})
     *                  the reader falls back to the integer / synthesised
     *                  paths.
     * @param requestBreaker breaker every column the leaves materialise
     *                  in heap is charged to before allocation and given
     *                  back on {@link #close()}; the node's request
     *                  breaker, or a {@link NoopCircuitBreaker}
     */
    public static LanceDirectoryReader open(
        Directory directory,
        IndexCommit commit,
        Dataset dataset,
        String intField,
        LancePrimaryKeyType pkType,
        LanceOverrides overrides,
        CircuitBreaker requestBreaker
    ) throws IOException {
        return open(directory, commit, dataset, intField, pkType, overrides, requestBreaker, IndexWriter.MAX_DOCS);
    }

    /**
     * Same as {@link #open(Directory, IndexCommit, Dataset, String,
     * LancePrimaryKeyType, LanceOverrides, CircuitBreaker)} with the row bound of the reader
     * given: when the table's fragments hold more physical rows than
     * {@code maxDocs} together, only the leading fragments that fit
     * become leaves and the reader reports {@link #luceneBoundExceeded()}.
     * {@code IndexWriter.MAX_DOCS} is the bound Lucene enforces; a smaller
     * value only serves tests.
     */
    public static LanceDirectoryReader open(
        Directory directory,
        IndexCommit commit,
        Dataset dataset,
        String intField,
        LancePrimaryKeyType pkType,
        LanceOverrides overrides,
        CircuitBreaker requestBreaker,
        long maxDocs
    ) throws IOException {
        List<LeafReader> leaves = new ArrayList<>();
        List<LanceFragmentLeafReader> rawLeaves = new ArrayList<>();
        // One describeIndices sweep for the whole reader; every leaf's
        // schema pass reads the resulting set instead of calling into
        // Lance per (leaf, Utf8 column).
        java.util.Set<String> ftsColumns = LanceFragmentLeafReader.resolveFtsColumns(dataset);
        List<Fragment> fragments = dataset.getFragments();
        long[] physicalRows = new long[fragments.size()];
        for (int i = 0; i < fragments.size(); i++) {
            physicalRows[i] = fragments.get(i).metadata().getPhysicalRows();
        }
        int held = leadingFragmentsWithinBound(physicalRows, maxDocs);
        for (Fragment fragment : fragments.subList(0, held)) {
            LanceFragmentLeafReader raw = new LanceFragmentLeafReader(
                dataset,
                fragment.getId(),
                fragment.metadata().getPhysicalRows(),
                fragment.metadata().getDeletionFile() != null,
                intField,
                pkType,
                overrides,
                ftsColumns,
                null
            );
            rawLeaves.add(raw);
            leaves.add(LanceSequentialLeafReader.wrap(raw));
        }
        // Install the shard-level column materialisation coordinator
        // so every leaf's ensureXxxLoaded delegates through one
        // dataset.newScan per column. See LanceShardColumnCache
        // javadoc for the rationale.
        LanceShardColumnCache cache = new LanceShardColumnCache(
            dataset,
            null,
            rawLeaves,
            null,
            null,
            requestBreaker,
            FragmentGroupScan.SEQUENTIAL
        );
        for (LanceFragmentLeafReader raw : rawLeaves) {
            raw.setShardColumnCache(cache);
        }
        return openWithLeaves(
            directory,
            commit,
            dataset,
            true,
            null,
            cache,
            leaves,
            sumDataFileSizes(fragments),
            tableRows(dataset, physicalRows, held)
        );
    }

    /**
     * Row totals of the table version behind a whole table reader that
     * holds the first {@code held} of the fragments with
     * {@code physicalRows}: every physical row, and the live row count
     * when the reader was cut short (the leaves alone cannot tell it
     * then; one metadata read of the manifest can).
     */
    private static TableRows tableRows(Dataset dataset, long[] physicalRows, int held) {
        long total = 0L;
        for (long rows : physicalRows) {
            total += rows;
        }
        long live = held < physicalRows.length ? dataset.countRows() : -1L;
        return new TableRows(total, live);
    }

    /** Physical and live row totals of a table version; live is -1 when the leaves already give it. */
    private record TableRows(long physical, long live) {
        static final TableRows FROM_LEAVES = new TableRows(-1L, -1L);
    }

    /**
     * Open a reader whose leaves are the explicit list of Lance fragment ids
     * — the fan-out unit the shard-free (direction 1) fragment path receives
     * from the coordinator. Contrast with the whole-table variant above,
     * which returns every fragment for the shard-level engine to serve GET
     * by _id and stats. This variant lets the per-node handler open exactly
     * the fragments it was told to scan, so the resulting DirectoryReader is
     * usable directly by an {@link org.apache.lucene.search.IndexSearcher}
     * that drives stock OpenSearch aggregators against per-fragment leaves.
     *
     * <p>Fragments listed in {@code fragmentIds} that do not exist in the
     * dataset are silently skipped (the coordinator may occasionally send a
     * fragment id whose fragment has been compacted away between assignment
     * and open; the handler handles the resulting empty leaf list by
     * returning an empty response).
     *
     * @param directory   the Lucene {@link Directory} the reader reports to
     *                    Lucene's own bookkeeping; the reader does not actually
     *                    write to it (fragments live in Lance).
     * @param commit      opaque marker retained through the reader lifecycle;
     *                    {@code null} is accepted for the direction 1 path
     *                    which does not consult it.
     * @param dataset     the Lance dataset; the returned reader takes
     *                    ownership and closes it on {@link #close()}.
     * @param intField    primary key column name for {@code _id} lookups; empty
     *                    string when the Lance table has no declared primary
     *                    key.
     * @param pkType      Arrow type family of the declared primary key
     *                    (see the sibling {@link #open} overload).
     * @param fragmentIds Lance fragment ids this reader should expose as
     *                    leaves. Non-null, may be empty (empty means "no
     *                    fragments assigned"; the returned reader has zero
     *                    leaves and behaves as a valid empty reader).
     */
    public static LanceDirectoryReader openForFragments(
        Directory directory,
        IndexCommit commit,
        Dataset dataset,
        String intField,
        LancePrimaryKeyType pkType,
        LanceOverrides overrides,
        List<Integer> fragmentIds
    ) throws IOException {
        return openForFragments(directory, commit, dataset, intField, pkType, overrides, fragmentIds, null);
    }

    /**
     * Same as
     * {@link #openForFragments(Directory, IndexCommit, Dataset, String,
     * LancePrimaryKeyType, LanceOverrides, List)}, plus a Lance SQL predicate the caller
     * wants attached to every per-column Lance scan the resulting
     * leaves issue. See {@link LanceColumnLoader#filterSql} for
     * the rationale and semantics; {@code filterSql} is nullable and
     * absent by default so existing callers (whole-table {@code open}
     * used by the shard engine, tests that build a reader without
     * a top-level filter) continue to run unfiltered column scans.
     * Heap column loads of the resulting reader are not charged to a
     * request breaker: this open has no production caller (the fragment
     * executor opens through {@link #openForSnapshot}), so it hands the
     * cache a {@link NoopCircuitBreaker}.
     */
    public static LanceDirectoryReader openForFragments(
        Directory directory,
        IndexCommit commit,
        Dataset dataset,
        String intField,
        LancePrimaryKeyType pkType,
        LanceOverrides overrides,
        List<Integer> fragmentIds,
        String filterSql
    ) throws IOException {
        java.util.Set<Integer> wanted = new java.util.HashSet<>(fragmentIds);
        List<LeafReader> leaves = new ArrayList<>(wanted.size());
        List<LanceFragmentLeafReader> rawLeaves = new ArrayList<>(wanted.size());
        // One describeIndices sweep and one schema pass for the whole
        // reader; every leaf shares the result instead of calling into
        // Lance per (leaf, Utf8 column).
        java.util.Set<String> ftsColumns = LanceFragmentSchema.resolveFtsColumns(dataset);
        LanceFragmentSchema schema = LanceFragmentSchema.derive(dataset, intField, pkType, overrides, ftsColumns);
        for (Fragment fragment : dataset.getFragments()) {
            if (!wanted.contains(fragment.getId())) {
                continue;
            }
            LanceFragmentLeafReader raw = new LanceFragmentLeafReader(
                dataset,
                fragment.getId(),
                fragment.metadata().getPhysicalRows(),
                fragment.metadata().getDeletionFile() != null,
                schema,
                filterSql
            );
            rawLeaves.add(raw);
            leaves.add(LanceSequentialLeafReader.wrap(raw));
        }
        // Wire the shard-level column cache with the same filterSql
        // the leaves themselves layer in singleColumnScan. See
        // LanceShardColumnCache javadoc for the rationale.
        LanceShardColumnCache cache = new LanceShardColumnCache(dataset, filterSql, rawLeaves);
        for (LanceFragmentLeafReader raw : rawLeaves) {
            raw.setShardColumnCache(cache);
        }
        // Per-request fragment readers do not report shard stats, so skip
        // the manifest walk here.
        return openWithLeaves(directory, commit, dataset, true, null, cache, leaves, DataFileSizes.NONE, TableRows.FROM_LEAVES);
    }

    /**
     * Open a reader over {@code fragmentIds} of a cached
     * {@link LanceWarmCache.Snapshot}. Every leaf is a view over the
     * snapshot's shared dataset, fragment metadata and schema, so this
     * call allocates Lucene-side objects only; the single Lance call it
     * can make is the one {@code _rowaddr} scan that resolves the live-row
     * bitmap of a fragment with a deletion file the first time any request
     * opens it. The returned reader does not own the dataset (the snapshot
     * does) and unpins the store columns its leaves were served when it
     * closes; the caller releases its lease on the snapshot after closing
     * the reader.
     *
     * <p>Fragment ids the snapshot does not list are skipped, as in
     * {@link #openForFragments}.
     *
     * @param directory      Lucene directory the reader reports to Lucene's
     *                       bookkeeping; never written
     * @param snapshot       snapshot the caller holds a lease on
     * @param columnStore    off-heap store to serve numeric and boolean
     *                       columns from, or {@code null} to load into heap
     *                       for this request (cache disabled)
     * @param fragmentIds    Lance fragment ids to expose as leaves
     * @param filterSql      predicate for request scoped heap column loads,
     *                       or {@code null}; never applied to store loads
     * @param requestBreaker breaker every column the leaves materialise in
     *                       heap (no store, or no room in it) is charged to
     *                       before allocation and given back when the
     *                       reader closes; the node's request breaker
     */
    public static LanceDirectoryReader openForSnapshot(
        Directory directory,
        LanceWarmCache.Snapshot snapshot,
        ColumnStore columnStore,
        List<Integer> fragmentIds,
        String filterSql,
        CircuitBreaker requestBreaker
    ) throws IOException {
        return openForSnapshot(directory, snapshot, columnStore, fragmentIds, filterSql, requestBreaker, FragmentGroupScan.SEQUENTIAL);
    }

    /**
     * Same as {@link #openForSnapshot(Directory, LanceWarmCache.Snapshot,
     * ColumnStore, List, String, CircuitBreaker)}, with the column scans
     * of the reader cut into fragment groups by {@code groupScan} so a
     * request over many fragments loads a column on several threads (see
     * {@link FragmentGroupScan}). The fragment executor passes the node's
     * {@code index_searcher} pool and {@code lance.fragment_path.parallelism};
     * the six argument form scans on the calling thread.
     */
    public static LanceDirectoryReader openForSnapshot(
        Directory directory,
        LanceWarmCache.Snapshot snapshot,
        ColumnStore columnStore,
        List<Integer> fragmentIds,
        String filterSql,
        CircuitBreaker requestBreaker,
        FragmentGroupScan groupScan
    ) throws IOException {
        java.util.Set<Integer> wanted = new java.util.HashSet<>(fragmentIds);
        List<LeafReader> leaves = new ArrayList<>(wanted.size());
        List<LanceFragmentLeafReader> rawLeaves = new ArrayList<>(wanted.size());
        Dataset dataset = snapshot.dataset();
        for (LanceWarmCache.FragmentMeta meta : snapshot.fragments()) {
            if (!wanted.contains(meta.id())) {
                continue;
            }
            // One live-row scan per fragment with a deletion file; a
            // cancelled request does not start the next one. Tables with
            // nested columns also resolve the fragment's doc id layout
            // once per snapshot here.
            groupScan.cancellation().checkCancelled();
            meta.resolveLiveDocs(dataset);
            meta.resolveNestedLayout(dataset, snapshot.schema());
            LanceFragmentLeafReader raw = new LanceFragmentLeafReader(dataset, meta.id(), meta, snapshot.schema(), filterSql);
            // The rows behind the hits of this leaf may come from, and go
            // to, the node's fetch cache under this snapshot's version;
            // the hits phase decides per request whether they do.
            raw.setFetchCache(snapshot.fetchTable());
            rawLeaves.add(raw);
            leaves.add(LanceSequentialLeafReader.wrap(raw));
        }
        LanceShardColumnCache cache = new LanceShardColumnCache(
            dataset,
            filterSql,
            rawLeaves,
            columnStore,
            snapshot.key(),
            requestBreaker,
            groupScan
        );
        for (LanceFragmentLeafReader raw : rawLeaves) {
            raw.setShardColumnCache(cache);
        }
        return openWithLeaves(directory, null, dataset, false, null, cache, leaves, DataFileSizes.NONE, TableRows.FROM_LEAVES);
    }

    /**
     * Open the shard engine's whole-table reader over a cached
     * {@link LanceWarmCache.Snapshot}: every fragment the snapshot lists
     * becomes a leaf, the same views {@link #openForSnapshot(Directory,
     * LanceWarmCache.Snapshot, ColumnStore, List, String, CircuitBreaker)}
     * builds for the fragment path, so GET, {@code _stats} and the
     * fragment path read one dataset and one column store per table
     * version on a node. The returned reader owns {@code lease} and
     * releases it when it closes, which for the engine is when Lucene's
     * {@code ReferenceManager} has released the last searcher of a
     * swapped out reader; the caller must not release the lease itself.
     * The data file total the engine reports through {@code _stats}
     * comes from the snapshot.
     *
     * @param directory      Lucene directory of the shard's store, reported
     *                       to Lucene's bookkeeping and never written
     * @param commit         the shard's bootstrap commit, returned by
     *                       {@link #getIndexCommit()}
     * @param lease          lease on the snapshot to read; owned by the
     *                       returned reader on success, released here on
     *                       failure
     * @param columnStore    off-heap store to serve columns from, or
     *                       {@code null} to load into heap (cache disabled,
     *                       the snapshot is then transient)
     * @param requestBreaker breaker the heap column loads of this reader
     *                       are charged to; a heap column of the engine's
     *                       reader stays charged until the next refresh
     *                       swaps the reader out and it closes
     */
    public static LanceDirectoryReader openForSnapshot(
        Directory directory,
        IndexCommit commit,
        LanceWarmCache.Lease lease,
        ColumnStore columnStore,
        CircuitBreaker requestBreaker
    ) throws IOException {
        return openForSnapshot(directory, commit, lease, columnStore, requestBreaker, IndexWriter.MAX_DOCS);
    }

    /**
     * Same as {@link #openForSnapshot(Directory, IndexCommit,
     * LanceWarmCache.Lease, ColumnStore, CircuitBreaker)} with the row
     * bound of the reader given, as for {@link #open(Directory,
     * IndexCommit, Dataset, String, LancePrimaryKeyType, LanceOverrides,
     * CircuitBreaker, long)}: the reader holds the leading
     * fragments of the snapshot whose physical rows fit in
     * {@code maxDocs} and reports {@link #luceneBoundExceeded()} when
     * that is not every fragment.
     */
    public static LanceDirectoryReader openForSnapshot(
        Directory directory,
        IndexCommit commit,
        LanceWarmCache.Lease lease,
        ColumnStore columnStore,
        CircuitBreaker requestBreaker,
        long maxDocs
    ) throws IOException {
        LanceWarmCache.Snapshot snapshot = lease.snapshot();
        Dataset dataset = snapshot.dataset();
        try {
            List<LanceWarmCache.FragmentMeta> fragments = snapshot.fragments();
            long[] physicalRows = new long[fragments.size()];
            for (int i = 0; i < fragments.size(); i++) {
                physicalRows[i] = fragments.get(i).physicalRows();
            }
            int held = leadingFragmentsWithinBound(physicalRows, maxDocs);
            List<LeafReader> leaves = new ArrayList<>(held);
            List<LanceFragmentLeafReader> rawLeaves = new ArrayList<>(held);
            for (LanceWarmCache.FragmentMeta meta : fragments.subList(0, held)) {
                meta.resolveLiveDocs(dataset);
                meta.resolveNestedLayout(dataset, snapshot.schema());
                LanceFragmentLeafReader raw = new LanceFragmentLeafReader(dataset, meta.id(), meta, snapshot.schema(), null);
                rawLeaves.add(raw);
                leaves.add(LanceSequentialLeafReader.wrap(raw));
            }
            LanceShardColumnCache cache = new LanceShardColumnCache(
                dataset,
                null,
                rawLeaves,
                columnStore,
                snapshot.key(),
                requestBreaker,
                FragmentGroupScan.SEQUENTIAL
            );
            for (LanceFragmentLeafReader raw : rawLeaves) {
                raw.setShardColumnCache(cache);
            }
            return openWithLeaves(
                directory,
                commit,
                dataset,
                false,
                lease,
                cache,
                leaves,
                snapshot.dataFileSizes(),
                tableRows(dataset, physicalRows, held)
            );
        } catch (Throwable t) {
            // The reader never came to own the lease; give the snapshot
            // reference back so a failed engine open does not pin it.
            lease.release();
            throw t;
        }
    }

    private static LanceDirectoryReader openWithLeaves(
        Directory directory,
        IndexCommit commit,
        Dataset dataset,
        boolean ownsDataset,
        LanceWarmCache.Lease lease,
        LanceShardColumnCache columnCache,
        List<LeafReader> leaves,
        DataFileSizes dataFileSizes,
        TableRows tableRows
    ) throws IOException {
        ByteBuffersDirectory bridgeDir = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(bridgeDir, new IndexWriterConfig())) {
            writer.addDocument(new Document());
            writer.commit();
        }
        DirectoryReader bridge = DirectoryReader.open(bridgeDir);
        return new LanceDirectoryReader(
            directory,
            leaves.toArray(new LeafReader[0]),
            commit,
            dataset,
            ownsDataset,
            lease,
            columnCache,
            bridge,
            dataFileSizes,
            tableRows
        );
    }

    private LanceDirectoryReader(
        Directory directory,
        LeafReader[] leaves,
        IndexCommit commit,
        Dataset dataset,
        boolean ownsDataset,
        LanceWarmCache.Lease lease,
        LanceShardColumnCache columnCache,
        DirectoryReader cacheLifetimeBridge,
        DataFileSizes dataFileSizes,
        TableRows tableRows
    ) throws IOException {
        super(directory, leaves, null);
        this.commit = commit;
        this.dataset = dataset;
        this.ownsDataset = ownsDataset;
        this.lease = lease;
        this.columnCache = columnCache;
        this.cacheLifetimeBridge = cacheLifetimeBridge;
        this.dataFileSizes = dataFileSizes;
        this.totalRows = tableRows.physical() < 0 ? maxDoc() : tableRows.physical();
        this.liveRows = tableRows.live();
    }

    /**
     * Whether the table version behind this reader has more physical
     * rows than one Lucene reader may hold, so that the reader holds
     * only the leading fragments that fit. Only a whole table reader
     * ({@link #open} or the lease form of {@link #openForSnapshot}) can
     * report {@code true}; a fragment path reader is opened over a
     * fragment group the coordinator already cut to the bound.
     */
    public boolean luceneBoundExceeded() {
        return totalRows > maxDoc();
    }

    /**
     * Live rows of the whole table version this reader was opened over,
     * whether or not the reader holds them: {@link #numDocs()} when it
     * holds every fragment, else the manifest's count.
     */
    public long tableRows() {
        return liveRows < 0 ? numDocs() : liveRows;
    }

    /**
     * Physical rows (live and deleted) of the whole table version,
     * whether or not the reader holds them; {@link #maxDoc()} unless
     * {@link #luceneBoundExceeded()}.
     */
    public long tablePhysicalRows() {
        return totalRows;
    }

    /**
     * The Lance index types present per column, from one
     * {@code describeIndices} call on the dataset this reader was opened
     * over (the warm cache snapshot's dataset on the fragment path, the
     * engine's own dataset otherwise; no new {@code Dataset.open}
     * happens). The type strings are what Lance reports
     * ({@code BTree}, {@code Bitmap}, {@code ZoneMap}, {@code Inverted},
     * {@code IVF_FLAT}, ...). Columns without an index are absent.
     * {@code GET /_lance/stats} reports the result per index so an
     * operator can see whether an {@code indexes} preference took
     * effect.
     */
    public Map<String, List<String>> columnIndexTypes() throws IOException {
        try {
            Map<Integer, String> columnsByFieldId = new HashMap<>();
            for (LanceField field : dataset.getLanceSchema().fields()) {
                columnsByFieldId.put(field.getId(), field.getName());
            }
            Map<String, List<String>> types = new TreeMap<>();
            for (IndexDescription description : dataset.describeIndices()) {
                for (Integer fieldId : description.getFieldIds()) {
                    String column = columnsByFieldId.get(fieldId);
                    if (column != null) {
                        types.computeIfAbsent(column, ignored -> new ArrayList<>()).add(description.getIndexType());
                    }
                }
            }
            return types;
        } catch (RuntimeException e) {
            throw new IOException(e);
        }
    }

    /**
     * The {@link LanceDirectoryReader} behind an arbitrary reader handed
     * out by the engine (unwrapping the {@link FilterDirectoryReader}
     * chain as {@link #dataFileSizesOf} does), or {@code null} when the
     * innermost reader is not one.
     */
    public static LanceDirectoryReader unwrap(IndexReader reader) {
        if (reader instanceof DirectoryReader directoryReader
            && FilterDirectoryReader.unwrap(directoryReader) instanceof LanceDirectoryReader lanceReader) {
            return lanceReader;
        }
        return null;
    }

    /**
     * Hand the reader's column cache the admission ticket of the request
     * it serves ({@link LanceShardColumnCache#attachAdmissionTicket}), so
     * the column scans the request's aggregations and sorts fault in are
     * judged as paths of that request. The fragment path's searcher calls
     * this once it owns the request's {@link LanceHitsAccounting}; a
     * reader without a column cache ignores it.
     */
    public void attachAdmissionTicket(LanceHitsAccounting ticket) {
        if (columnCache != null) {
            columnCache.attachAdmissionTicket(ticket);
        }
    }

    /**
     * Manifest-recorded data file byte total of the fragments this reader
     * exposes. {@link DataFileSizes#NONE} for readers opened through
     * {@link #openForFragments}.
     */
    public DataFileSizes dataFileSizes() {
        return dataFileSizes;
    }

    /**
     * Resolve the {@link DataFileSizes} behind an arbitrary reader handed
     * out by the engine. The shard searcher's reader is an
     * {@code OpenSearchDirectoryReader} (and, with the security plugin, a
     * further DLS / FLS wrapper) around the {@link LanceDirectoryReader}, so
     * unwrap the {@link FilterDirectoryReader} chain first. Returns
     * {@link DataFileSizes#NONE} when the innermost reader is not a
     * {@link LanceDirectoryReader}.
     */
    public static DataFileSizes dataFileSizesOf(IndexReader reader) {
        if (reader instanceof DirectoryReader directoryReader
            && FilterDirectoryReader.unwrap(directoryReader) instanceof LanceDirectoryReader lanceReader) {
            return lanceReader.dataFileSizes();
        }
        return DataFileSizes.NONE;
    }

    /**
     * The {@link LanceWarmCache.Snapshot} this reader was opened over
     * through {@link #openForSnapshot(Directory, IndexCommit,
     * LanceWarmCache.Lease, ColumnStore, CircuitBreaker)}, or {@code null} for a reader
     * that opened its own dataset or does not own its lease.
     */
    public LanceWarmCache.Snapshot snapshot() {
        return lease == null ? null : lease.snapshot();
    }

    /**
     * Lance version of the snapshot behind an arbitrary reader handed out
     * by the engine (unwrapping the {@link FilterDirectoryReader} chain as
     * {@link #dataFileSizesOf} does), or {@code -1} when the innermost
     * reader holds no snapshot lease.
     */
    public static long snapshotVersionOf(IndexReader reader) {
        if (reader instanceof DirectoryReader directoryReader
            && FilterDirectoryReader.unwrap(directoryReader) instanceof LanceDirectoryReader lanceReader
            && lanceReader.snapshot() != null) {
            return lanceReader.snapshot().version();
        }
        return -1L;
    }

    @Override
    protected DirectoryReader doOpenIfChanged() {
        return null;
    }

    @Override
    protected DirectoryReader doOpenIfChanged(ExecutorService executorService) {
        return null;
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexCommit commit) {
        return null;
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexCommit commit, ExecutorService executorService) {
        return null;
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) {
        return null;
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes, ExecutorService executorService) {
        return null;
    }

    @Override
    public long getVersion() {
        return 1;
    }

    @Override
    public boolean isCurrent() {
        return true;
    }

    @Override
    public IndexCommit getIndexCommit() {
        return commit;
    }

    @Override
    protected void doClose() throws IOException {
        // a3be9f6 introduced an explicit decRef of every leaf here so
        // OpenSearch's IndicesRequestCache would drop entries keyed on
        // the departed leaves as soon as the DirectoryReader was
        // swapped out. That backfired in two ways at once: the
        // build-tools' forbiddenApis pass flags any direct call to
        // IndexReader#decRef / incRef / tryIncRef ("Reference
        // management is tricky, leave it to SearcherManager") so
        // ./gradlew build stopped short of running tests, and a
        // TransportShardRefreshAction that landed after the swap
        // could still hold a reference to one of the just-closed
        // leaves. That inbound refresh then observed
        // AlreadyClosedException and the replication retry loop
        // waited its full 60-second budget before giving up, which
        // in turn stalled the namespace poll thread that was queued
        // behind the same refresh call. Cache invalidation at the
        // fragment level now waits for the JVM to reclaim the leaf
        // via GC, which is acceptable because the composite-level
        // cacheLifetimeBridge below still fires the CacheHelper
        // listeners the request cache actually keys off at the
        // DirectoryReader level.
        IOException first = null;
        try {
            cacheLifetimeBridge.close();
        } catch (IOException e) {
            first = e;
        }
        if (columnCache != null) {
            // Unpins the store entries and gives the request breaker back
            // every heap column charge of this reader's loads.
            columnCache.release();
        }
        if (ownsDataset) {
            try {
                dataset.close();
            } catch (Exception e) {
                if (first == null && e instanceof IOException) {
                    first = (IOException) e;
                }
            }
        }
        // After the pins: the snapshot may close on release (retired, or
        // transient with the cache disabled), and closing it drops its
        // store columns, which must not happen while this reader still
        // pins them.
        if (lease != null) {
            lease.release();
        }
        if (first != null) {
            throw first;
        }
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return cacheLifetimeBridge.getReaderCacheHelper();
    }
}
