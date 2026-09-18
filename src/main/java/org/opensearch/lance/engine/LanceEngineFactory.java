/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.ReadOnlyEngine;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;

/**
 * Engine factory producing a read-only engine backed by a Lance table.
 *
 * <p>{@code _search} is intercepted by {@link
 * org.opensearch.lance.dispatch.LanceDispatchActionFilter} and served by the
 * fragment path (see {@link
 * org.opensearch.lance.dispatch.TransportLanceCoordinatorAction}), so the
 * engine returned here rarely runs a query end-to-end. Its remaining
 * responsibilities are:
 * <ul>
 *   <li>{@code GET /_doc/{id}} — {@link #newReadWriteEngine} returns an
 *       engine whose {@link Engine#get(Engine.Get, java.util.function.BiFunction)}
 *       resolves the primary key through a Lance scalar-index-backed
 *       point lookup.</li>
 *   <li>Shard-level stats: {@link Engine#docStats()} and
 *       {@link Engine#segmentsStats(boolean, boolean)} report Lance-provided
 *       row counts instead of Lucene segment stats.</li>
 *   <li>Refresh lifecycle: {@link Engine#refresh(String)} advances the
 *       shared reader when the Lance manifest version advances, so the
 *       fragment executors that open per-fragment leaves see the latest
 *       version. The reader is also the safety net for the handful of
 *       {@code _search} shapes that {@code LanceDispatchActionFilter}
 *       falls through to the shard path ({@code suggest},
 *       {@code highlighter}, {@code search_after} without {@code sort}).</li>
 * </ul>
 *
 * <p>Under the hood, the empty Lucene commit created at shard bootstrap
 * supplies sequence number metadata to the {@link ReadOnlyEngine}
 * superclass; the plugin layers a separate {@link ReferenceManager} on top
 * whose reference is a {@link LanceDirectoryReader}. The manager swaps in
 * a new reader when the Lance manifest advances so refresh does not close
 * the previous reader until in-flight readers release.
 */
public final class LanceEngineFactory implements EngineFactory {

    public static final String TABLE_SETTING = "index.lance.table";
    public static final String PRIMARY_KEY_FIELD_SETTING = "index.lance.primary_key_field";
    /**
     * Pin the Lance manifest version an index reads. Non-negative values pin
     * the dataset to that version; the default {@code -1L} means "follow the
     * latest version" and lets {@link org.opensearch.lance.namespace.LanceNamespaceService}'s
     * poll advance the reader as new fragments land.
     */
    public static final String VERSION_SETTING = "index.lance.version";

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        String table = config.getIndexSettings().getSettings().get(TABLE_SETTING);
        String field = config.getIndexSettings().getSettings().get(PRIMARY_KEY_FIELD_SETTING, "");
        int shardId = config.getShardId().id();
        long versionSetting = config.getIndexSettings().getSettings().getAsLong(VERSION_SETTING, -1L);
        java.util.Optional<Long> pinnedVersion = versionSetting >= 0 ? java.util.Optional.of(versionSetting) : java.util.Optional.empty();
        StorageOptions storageOptions = StorageOptions.fromIndexSettings(config.getIndexSettings().getSettings());
        return new LanceReadOnlyEngine(config, table, field, shardId, pinnedVersion, storageOptions);
    }

    static final class LanceReadOnlyEngine extends ReadOnlyEngine {

        private static final Logger LOG = LogManager.getLogger(LanceReadOnlyEngine.class);

        final String tablePath;
        final String field;
        final int shardId;
        /**
         * Pinned Lance manifest version. When present, {@link #openLanceReader()}
         * passes this through {@link org.opensearch.lance.LanceRegistry#openDataset(String, StorageOptions, java.util.Optional)}
         * so the shard reads a fixed snapshot; the reader manager's version
         * probe below also returns the pinned value, so {@code refreshIfNeeded}
         * short-circuits.
         */
        final java.util.Optional<Long> pinnedVersion;
        final StorageOptions storageOptions;
        private final LanceReaderManager lanceReaderManager;

        LanceReadOnlyEngine(
            EngineConfig config,
            String table,
            String field,
            int shardId,
            java.util.Optional<Long> pinnedVersion,
            StorageOptions storageOptions
        ) {
            super(config, null, null, true, Function.identity(), true);
            this.tablePath = table;
            this.field = field;
            this.shardId = shardId;
            this.pinnedVersion = pinnedVersion;
            this.storageOptions = storageOptions;
            try {
                OpenSearchDirectoryReader initial = openLanceReader();
                long initialVersion;
                try (Dataset probe = LanceRegistry.openDataset(tablePath, storageOptions, pinnedVersion)) {
                    initialVersion = probe.version();
                }
                this.lanceReaderManager = new LanceReaderManager(initial, this, initialVersion);
            } catch (IOException e) {
                throw new EngineException(config.getShardId(), "Failed to open initial Lance reader", e);
            }
        }

        OpenSearchDirectoryReader openLanceReader() throws IOException {
            Directory directory = engineConfig.getStore().directory();
            SegmentInfos infos = getLastCommittedSegmentInfos();
            IndexCommit commit = Lucene.getIndexCommit(infos, directory);
            Dataset dataset = LanceRegistry.openDataset(tablePath, storageOptions, pinnedVersion);
            // If wrapping the dataset in a directory reader fails, close it
            // here — otherwise the JNI-owned Dataset handle leaks and
            // eventually starves the native allocator. `LanceDirectoryReader`
            // takes ownership of `dataset` only on the happy path via its
            // `doClose`.
            OpenSearchDirectoryReader wrapped = null;
            LanceDirectoryReader reader = null;
            try {
                reader = LanceDirectoryReader.open(directory, commit, dataset, field);
                wrapped = OpenSearchDirectoryReader.wrap(reader, config().getShardId());
                return wrapped;
            } catch (Throwable t) {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (Throwable suppressed) {
                        t.addSuppressed(suppressed);
                    }
                } else {
                    try {
                        dataset.close();
                    } catch (Throwable suppressed) {
                        t.addSuppressed(suppressed);
                    }
                }
                if (t instanceof IOException io) {
                    throw io;
                }
                if (t instanceof RuntimeException re) {
                    throw re;
                }
                if (t instanceof Error err) {
                    throw err;
                }
                throw new IOException(t);
            }
        }

        @Override
        protected ReferenceManager<OpenSearchDirectoryReader> getReferenceManager(SearcherScope scope) {
            return lanceReaderManager;
        }

        @Override
        public void refresh(String source) {
            try {
                lanceReaderManager.maybeRefresh();
            } catch (IOException e) {
                LOG.warn("Lance refresh failed for shard {}", config().getShardId(), e);
            }
        }

        // OpenSearch's default docStats() / segmentsStats() implementations
        // walk every leaf and call Lucene.segmentReader(reader) to extract a
        // SegmentReader for byte-size and segment-name bookkeeping. Lance
        // leaves are neither SegmentReaders nor Lucene segments, so those
        // helpers throw on every stats API call and take out _stats /
        // _cat/indices docs.count / _nodes/stats/indices / _cluster/stats
        // across the whole node. Recompute the fields Lance can provide
        // (numDocs / maxDoc from each leaf), leave the ones tied to Lucene
        // segment files empty, and never delegate to Lucene.segmentReader.
        @Override
        public org.opensearch.index.shard.DocsStats docStats() {
            try (Searcher searcher = acquireSearcher("docStats", SearcherScope.INTERNAL)) {
                long numDocs = 0;
                long numDeletedDocs = 0;
                for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                    numDocs += ctx.reader().numDocs();
                    numDeletedDocs += ctx.reader().numDeletedDocs();
                }
                // sizeInBytes is unknown for Lance leaves; leave it at zero.
                // Callers already expect this to be an estimate.
                return new org.opensearch.index.shard.DocsStats.Builder().count(numDocs)
                    .deleted(numDeletedDocs)
                    .totalSizeInBytes(0L)
                    .build();
            }
        }

        @Override
        public org.opensearch.index.engine.SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
            ensureOpen();
            // A Lance-backed index has no Lucene segments to report on. Return
            // an empty stats object so the request completes with sane zeros
            // instead of blowing up on Lucene.segmentReader(reader).
            return new org.opensearch.index.engine.SegmentsStats();
        }

        @Override
        public boolean maybeRefresh(String source) throws EngineException {
            try {
                return lanceReaderManager.maybeRefresh();
            } catch (IOException e) {
                throw new EngineException(config().getShardId(), "Lance refresh failed", e);
            }
        }

        @Override
        protected void closeNoLock(String reason, CountDownLatch closedLatch) {
            try {
                lanceReaderManager.close();
            } catch (IOException e) {
                LOG.warn("Failed to close Lance reader manager", e);
            }
            super.closeNoLock(reason, closedLatch);
        }

        /**
         * RFC contract: {@code _id} get maps to a primary key point lookup.
         * Pushes the equality predicate to the Lance scalar index by scanning
         * the shard's own fragment ids with {@code <field> = <key>} as the
         * filter. A row address batch returned by Lance identifies both the
         * hit fragment and the intra-fragment offset, which are mapped back to
         * the corresponding Lucene leaf so the standard GET path can fetch the
         * source.
         */
        @Override
        public GetResult get(Get get, BiFunction<String, SearcherScope, Engine.Searcher> searcherFactory) {
            if (field == null || field.isEmpty()) {
                // The table did not declare a primary key. With no `_id`
                // lookup column there is nothing to resolve, so return
                // NOT_EXISTS immediately rather than passing an empty field
                // name to Lance and getting a 500 back.
                return GetResult.NOT_EXISTS;
            }
            // Lance-backed indices are always single-shard (attach rejects
            // number_of_shards > 1), so GET /_doc/{id} always lands on the
            // shard holding every fragment. When multi-shard support is
            // revisited, the coordinator will need to fan out to every
            // shard because a Lance primary key has no relationship to
            // OpenSearch's hash(_id) % numShards routing.
            long key;
            try {
                key = Long.parseLong(get.id());
            } catch (NumberFormatException e) {
                return GetResult.NOT_EXISTS;
            }

            Engine.Searcher searcher = searcherFactory.apply("get", SearcherScope.EXTERNAL);
            try {
                List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
                List<Integer> fragmentIds = new ArrayList<>(leaves.size());
                Map<Integer, LeafReaderContext> leavesByFragment = new HashMap<>(leaves.size());
                Dataset dataset = null;
                for (LeafReaderContext ctx : leaves) {
                    LanceFragmentLeafReader lance = LanceFragmentLeafReader.unwrap(ctx.reader());
                    if (lance == null) {
                        continue;
                    }
                    fragmentIds.add(lance.fragmentId());
                    leavesByFragment.put(lance.fragmentId(), ctx);
                    dataset = lance.dataset();
                }
                if (dataset == null || fragmentIds.isEmpty()) {
                    searcher.close();
                    return GetResult.NOT_EXISTS;
                }

                ScanOptions options = new ScanOptions.Builder().filter(field + " = " + key)
                    .columns(Collections.singletonList(field))
                    .fragmentIds(fragmentIds)
                    .withRowAddress(true)
                    .build();

                try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        UInt8Vector rowaddr = (UInt8Vector) root.getVector("_rowaddr");
                        for (int i = 0; i < root.getRowCount(); i++) {
                            long addr = rowaddr.get(i);
                            int fragmentId = (int) (addr >>> 32);
                            int offset = (int) (addr & 0xFFFFFFFFL);
                            LeafReaderContext ctx = leavesByFragment.get(fragmentId);
                            if (ctx == null) {
                                continue;
                            }
                            // The wrapper reader that the security plugin
                            // interposes for DLS applies its filter through
                            // liveDocs. Consult it before returning the hit so
                            // GET honours the same DLS predicate that
                            // _search / _count already respect (otherwise a
                            // user restricted to `rating >= 4` could still
                            // GET a row with `rating = 1`).
                            Bits liveDocs = ctx.reader().getLiveDocs();
                            if (liveDocs != null && !liveDocs.get(offset)) {
                                continue;
                            }
                            DocIdAndVersion dv = new DocIdAndVersion(offset, 1, 1, 1, ctx.reader(), ctx.docBase);
                            return new GetResult(searcher, dv, false);
                        }
                    }
                }
                searcher.close();
                return GetResult.NOT_EXISTS;
            } catch (IOException e) {
                searcher.close();
                throw new java.io.UncheckedIOException(e);
            } catch (Exception e) {
                searcher.close();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Reference manager that swaps in a fresh {@link LanceDirectoryReader}
     * when the Lance manifest advances beyond the served version.
     */
    @SuppressForbidden(reason = "reference counting is required to atomically swap the Lance-backed reader")
    static final class LanceReaderManager extends ReferenceManager<OpenSearchDirectoryReader> {

        private final LanceReadOnlyEngine engine;
        private volatile long servedVersion;

        LanceReaderManager(OpenSearchDirectoryReader initial, LanceReadOnlyEngine engine, long initialVersion) {
            this.current = initial;
            this.engine = engine;
            this.servedVersion = initialVersion;
        }

        @Override
        protected OpenSearchDirectoryReader refreshIfNeeded(OpenSearchDirectoryReader referenceToRefresh) throws IOException {
            // Lucene's ReferenceManager serialises calls into this method
            // (refreshLock), so `servedVersion` does not need CAS: only one
            // refresh advances it at a time. When we return a non-null
            // reference, Lucene swaps it in as `current`, decRefs the old
            // reference, and — because in-flight callers hold at least one
            // additional refcount via `tryIncRef` — the old reader (and its
            // Dataset) stays alive until the last search that acquired it
            // releases. Setting servedVersion before returning is therefore
            // safe: a concurrent `maybeRefresh()` blocks on refreshLock
            // and will observe the updated version once we return.
            long latest;
            try (Dataset probe = LanceRegistry.openDataset(engine.tablePath, engine.storageOptions, engine.pinnedVersion)) {
                latest = probe.version();
            }
            if (latest == servedVersion) {
                return null;
            }
            OpenSearchDirectoryReader newReader = engine.openLanceReader();
            servedVersion = latest;
            return newReader;
        }

        @Override
        protected boolean tryIncRef(OpenSearchDirectoryReader reference) {
            return reference.tryIncRef();
        }

        @Override
        protected int getRefCount(OpenSearchDirectoryReader reference) {
            return reference.getRefCount();
        }

        @Override
        protected void decRef(OpenSearchDirectoryReader reference) throws IOException {
            reference.decRef();
        }
    }
}
