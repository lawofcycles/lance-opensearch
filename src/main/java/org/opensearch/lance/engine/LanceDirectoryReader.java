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
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.lance.Dataset;
import org.lance.Fragment;

/** DirectoryReader whose leaves are Lance fragments. */
public final class LanceDirectoryReader extends DirectoryReader {

    private final IndexCommit commit;
    // The engine hands us a freshly opened Dataset when it builds a new reader,
    // so this reader takes ownership of it and closes it when the reader is
    // closed. Lucene's ReferenceManager releases the previous reader once the
    // last in-flight searcher completes, which is the point where we also want
    // to release the Lance native handle it was reading from.
    private final Dataset dataset;
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
        for (Fragment fragment : dataset.getFragments()) {
            leaves.add(
                LanceSequentialLeafReader.wrap(
                    new LanceFragmentLeafReader(
                        dataset,
                        fragment.getId(),
                        fragment.metadata().getPhysicalRows(),
                        intField,
                        pkType,
                        multiFields
                    )
                )
            );
        }
        return openWithLeaves(directory, commit, dataset, leaves);
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
        for (Fragment fragment : dataset.getFragments()) {
            if (!wanted.contains(fragment.getId())) {
                continue;
            }
            leaves.add(
                LanceSequentialLeafReader.wrap(
                    new LanceFragmentLeafReader(
                        dataset,
                        fragment.getId(),
                        fragment.metadata().getPhysicalRows(),
                        intField,
                        pkType,
                        multiFields,
                        filterSql
                    )
                )
            );
        }
        return openWithLeaves(directory, commit, dataset, leaves);
    }

    private static LanceDirectoryReader openWithLeaves(Directory directory, IndexCommit commit, Dataset dataset, List<LeafReader> leaves)
        throws IOException {
        ByteBuffersDirectory bridgeDir = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(bridgeDir, new IndexWriterConfig())) {
            writer.addDocument(new Document());
            writer.commit();
        }
        DirectoryReader bridge = DirectoryReader.open(bridgeDir);
        return new LanceDirectoryReader(directory, leaves.toArray(new LeafReader[0]), commit, dataset, bridge);
    }

    private LanceDirectoryReader(
        Directory directory,
        LeafReader[] leaves,
        IndexCommit commit,
        Dataset dataset,
        DirectoryReader cacheLifetimeBridge
    ) throws IOException {
        super(directory, leaves, null);
        this.commit = commit;
        this.dataset = dataset;
        this.cacheLifetimeBridge = cacheLifetimeBridge;
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
        try {
            dataset.close();
        } catch (Exception e) {
            if (first == null && e instanceof IOException) {
                first = (IOException) e;
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
