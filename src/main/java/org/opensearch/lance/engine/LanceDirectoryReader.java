/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    private final IndexCommit commit;
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
     * Open a reader whose leaves are every fragment of {@code dataset}. Used
     * by the shard-level engine ({@link LanceEngineFactory.LanceReadOnlyEngine})
     * to expose the whole table through the single primary shard that a
     * Lance-backed attach always produces. The RFC's shard-partitioning
     * scheme (fragment id modulo shard count) was retired when
     * {@code number_of_shards} was dropped from attach; {@link #openForFragments}
     * is the fan-out variant used by the fragment path.
     */
    /**
     * Open a whole-table reader for the shard-level engine's GET / stats /
     * refresh needs. Every fragment in the dataset is surfaced as a leaf.
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
     *                  it is {@link org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType#KEYWORD}
     *                  the reader holds string PK values so {@code _id}
     *                  echoes them verbatim; otherwise (including
     *                  {@link org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType#NONE})
     *                  the reader falls back to the integer / synthesised
     *                  paths.
     */
    public static LanceDirectoryReader open(
        Directory directory,
        IndexCommit commit,
        Dataset dataset,
        String intField,
        org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType pkType,
        java.util.Map<String, java.util.LinkedHashMap<String, String>> multiFields
    ) throws IOException {
        List<LeafReader> leaves = new ArrayList<>();
        List<LanceFragmentLeafReader> rawLeaves = new ArrayList<>();
        // One describeIndices sweep for the whole reader; every leaf's
        // schema pass reads the resulting set instead of calling into
        // Lance per (leaf, Utf8 column).
        java.util.Set<String> ftsColumns = LanceFragmentLeafReader.resolveFtsColumns(dataset);
        List<Fragment> fragments = dataset.getFragments();
        for (Fragment fragment : fragments) {
            LanceFragmentLeafReader raw = new LanceFragmentLeafReader(
                dataset,
                fragment.getId(),
                fragment.metadata().getPhysicalRows(),
                fragment.metadata().getDeletionFile() != null,
                intField,
                pkType,
                multiFields,
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
        LanceShardColumnCache cache = new LanceShardColumnCache(dataset, null, rawLeaves);
        for (LanceFragmentLeafReader raw : rawLeaves) {
            raw.setShardColumnCache(cache);
        }
        return openWithLeaves(directory, commit, dataset, true, cache, leaves, sumDataFileSizes(fragments));
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
        org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType pkType,
        java.util.Map<String, java.util.LinkedHashMap<String, String>> multiFields,
        List<Integer> fragmentIds
    ) throws IOException {
        return openForFragments(directory, commit, dataset, intField, pkType, multiFields, fragmentIds, null);
    }

    /**
     * Same as
     * {@link #openForFragments(Directory, IndexCommit, Dataset, String,
     * org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType,
     * java.util.Map, List)}, plus a Lance SQL predicate the caller
     * wants attached to every per-column Lance scan the resulting
     * leaves issue. See {@link LanceFragmentLeafReader#filterSql} for
     * the rationale and semantics; {@code filterSql} is nullable and
     * absent by default so existing callers (whole-table {@code open}
     * used by the shard engine, tests that build a reader without
     * a top-level filter) continue to run unfiltered column scans.
     */
    public static LanceDirectoryReader openForFragments(
        Directory directory,
        IndexCommit commit,
        Dataset dataset,
        String intField,
        org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType pkType,
        java.util.Map<String, java.util.LinkedHashMap<String, String>> multiFields,
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
        LanceFragmentSchema schema = LanceFragmentSchema.derive(dataset, intField, pkType, multiFields, ftsColumns);
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
        return openWithLeaves(directory, commit, dataset, true, cache, leaves, DataFileSizes.NONE);
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
     * @param directory   Lucene directory the reader reports to Lucene's
     *                    bookkeeping; never written
     * @param snapshot    snapshot the caller holds a lease on
     * @param columnStore off-heap store to serve numeric and boolean
     *                    columns from, or {@code null} to load into heap
     *                    for this request (cache disabled)
     * @param fragmentIds Lance fragment ids to expose as leaves
     * @param filterSql   predicate for request scoped heap column loads,
     *                    or {@code null}; never applied to store loads
     */
    public static LanceDirectoryReader openForSnapshot(
        Directory directory,
        LanceWarmCache.Snapshot snapshot,
        ColumnStore columnStore,
        List<Integer> fragmentIds,
        String filterSql
    ) throws IOException {
        java.util.Set<Integer> wanted = new java.util.HashSet<>(fragmentIds);
        List<LeafReader> leaves = new ArrayList<>(wanted.size());
        List<LanceFragmentLeafReader> rawLeaves = new ArrayList<>(wanted.size());
        Dataset dataset = snapshot.dataset();
        for (LanceWarmCache.FragmentMeta meta : snapshot.fragments()) {
            if (!wanted.contains(meta.id())) {
                continue;
            }
            meta.resolveLiveDocs(dataset);
            LanceFragmentLeafReader raw = new LanceFragmentLeafReader(dataset, meta.id(), meta, snapshot.schema(), filterSql);
            rawLeaves.add(raw);
            leaves.add(LanceSequentialLeafReader.wrap(raw));
        }
        LanceShardColumnCache cache = new LanceShardColumnCache(dataset, filterSql, rawLeaves, columnStore, snapshot.key());
        for (LanceFragmentLeafReader raw : rawLeaves) {
            raw.setShardColumnCache(cache);
        }
        return openWithLeaves(directory, null, dataset, false, cache, leaves, DataFileSizes.NONE);
    }

    private static LanceDirectoryReader openWithLeaves(
        Directory directory,
        IndexCommit commit,
        Dataset dataset,
        boolean ownsDataset,
        LanceShardColumnCache columnCache,
        List<LeafReader> leaves,
        DataFileSizes dataFileSizes
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
            columnCache,
            bridge,
            dataFileSizes
        );
    }

    private LanceDirectoryReader(
        Directory directory,
        LeafReader[] leaves,
        IndexCommit commit,
        Dataset dataset,
        boolean ownsDataset,
        LanceShardColumnCache columnCache,
        DirectoryReader cacheLifetimeBridge,
        DataFileSizes dataFileSizes
    ) throws IOException {
        super(directory, leaves, null);
        this.commit = commit;
        this.dataset = dataset;
        this.ownsDataset = ownsDataset;
        this.columnCache = columnCache;
        this.cacheLifetimeBridge = cacheLifetimeBridge;
        this.dataFileSizes = dataFileSizes;
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
            columnCache.releasePins();
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
        if (first != null) {
            throw first;
        }
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return cacheLifetimeBridge.getReaderCacheHelper();
    }
}
