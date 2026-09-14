/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

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

/**
 * Engine factory producing a read-only engine backed by a Lance table.
 *
 * <p>The empty Lucene commit created at shard bootstrap supplies sequence
 * number metadata to the {@link ReadOnlyEngine} superclass; the plugin
 * layers a separate {@link ReferenceManager} on top whose reference is a
 * {@link LanceDirectoryReader}. {@link Engine#refresh(String)} atomically
 * swaps in a new reader when the Lance manifest version advances, so
 * searches continue to be served against the previous reader while the new
 * one is being built.
 */
public final class LanceEngineFactory implements EngineFactory {

    public static final String TABLE_SETTING = "index.lance.table";
    public static final String PRIMARY_KEY_FIELD_SETTING = "index.lance.primary_key_field";

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        String table = config.getIndexSettings().getSettings().get(TABLE_SETTING);
        String field = config.getIndexSettings().getSettings().get(PRIMARY_KEY_FIELD_SETTING, "id");
        int shardId = config.getShardId().id();
        int numShards = config.getIndexSettings().getNumberOfShards();
        return new LanceReadOnlyEngine(config, table, field, shardId, numShards);
    }

    static final class LanceReadOnlyEngine extends ReadOnlyEngine {

        private static final Logger LOG = LogManager.getLogger(LanceReadOnlyEngine.class);

        final String tablePath;
        final String field;
        final int shardId;
        final int numShards;
        private final LanceReaderManager lanceReaderManager;

        LanceReadOnlyEngine(EngineConfig config, String table, String field, int shardId, int numShards) {
            super(config, null, null, true, Function.identity(), true);
            this.tablePath = table;
            this.field = field;
            this.shardId = shardId;
            this.numShards = numShards;
            try {
                OpenSearchDirectoryReader initial = openLanceReader();
                long initialVersion;
                try (Dataset probe = Dataset.open().allocator(LanceRegistry.allocator()).uri(tablePath).build()) {
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
            Dataset dataset = Dataset.open().allocator(LanceRegistry.allocator()).uri(tablePath).build();
            LanceDirectoryReader reader = LanceDirectoryReader.open(directory, commit, dataset, field, shardId, numShards);
            return OpenSearchDirectoryReader.wrap(reader, config().getShardId());
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
            long latest;
            try (Dataset probe = Dataset.open().allocator(LanceRegistry.allocator()).uri(engine.tablePath).build()) {
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
