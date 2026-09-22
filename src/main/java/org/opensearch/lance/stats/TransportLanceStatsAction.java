/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Serves {@link LanceStatsAction}: fans a {@link LanceStatsNodeRequest} out
 * to the requested nodes and has each one run
 * {@link LanceStatsCollector#collect(List)} with the shard readers of the
 * Lance-backed indexes it hosts. OpenSearch's {@code _nodes/stats}
 * sections are a fixed enum in core with no plugin hook, so the plugin
 * exposes its figures through its own nodes action, as the k-NN plugin
 * does. The per node work reads a few counters, acquires and releases
 * one searcher per started Lance shard, and runs on the management pool,
 * off the transport worker.
 */
public final class TransportLanceStatsAction extends TransportNodesAction<
    LanceStatsRequest,
    LanceStatsResponse,
    LanceStatsNodeRequest,
    LanceStatsNodeResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportLanceStatsAction.class);

    /**
     * Reflective handle on {@code IndexService.getReaderWrapper()}, the
     * same accessor the fragment query action resolves, held separately
     * so this action does not depend on the dispatch package's
     * internals. A DLS/FLS reader wrapper hides columns from the
     * request; Lance's {@code describeIndices} metadata does not pass
     * through the wrapper, so the per-column index type report is
     * skipped whenever a wrapper is installed. Unlike the query path,
     * stats must not refuse to load when the accessor is missing:
     * {@code null} here just means "cannot tell", which is treated as
     * "wrapper present" and omits the report.
     */
    private static final Method INDEX_SERVICE_GET_READER_WRAPPER = resolveReaderWrapperAccessor();

    @SuppressForbidden(reason = "IndexService#getReaderWrapper() is package-private in core; reflection is required to tell "
        + "whether a DLS/FLS wrapper is installed so the stats report does not reveal column names the wrapper hides")
    private static Method resolveReaderWrapperAccessor() {
        try {
            Method m = IndexService.class.getDeclaredMethod("getReaderWrapper");
            m.setAccessible(true);
            return m;
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /**
     * Whether {@code indexService} has a reader wrapper installed (a
     * security plugin's DLS/FLS wrapper). Answers {@code true} when the
     * accessor is unavailable or throws, so the caller errs on the side
     * of not revealing column metadata.
     */
    private static boolean hasReaderWrapper(IndexService indexService) {
        if (INDEX_SERVICE_GET_READER_WRAPPER == null) {
            return true;
        }
        try {
            return INDEX_SERVICE_GET_READER_WRAPPER.invoke(indexService) != null;
        } catch (Exception e) {
            return true;
        }
    }

    private final LanceStatsCollector collector;
    private final IndicesService indicesService;

    @Inject
    public TransportLanceStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        LanceStatsCollector collector,
        IndicesService indicesService
    ) {
        super(
            LanceStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            LanceStatsRequest::new,
            LanceStatsNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            LanceStatsNodeResponse.class
        );
        this.collector = collector;
        this.indicesService = indicesService;
    }

    @Override
    protected LanceStatsResponse newResponse(
        LanceStatsRequest request,
        List<LanceStatsNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new LanceStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected LanceStatsNodeRequest newNodeRequest(LanceStatsRequest request) {
        return new LanceStatsNodeRequest();
    }

    @Override
    protected LanceStatsNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new LanceStatsNodeResponse(in);
    }

    @Override
    protected LanceStatsNodeResponse nodeOperation(LanceStatsNodeRequest request) {
        return new LanceStatsNodeResponse(clusterService.localNode(), collector.collect(indexReaderStats()));
    }

    /**
     * The shard reader of every started Lance-backed shard on this node:
     * the table's live rows, the rows the reader holds and whether the
     * table is above the Lucene document bound, read off the
     * {@link LanceDirectoryReader} behind a searcher acquired and released
     * here. A shard that is not started, or closes while the searcher is
     * being acquired, is left out rather than failing the node's stats.
     */
    private List<LanceNodeStats.IndexReaderStats> indexReaderStats() {
        List<LanceNodeStats.IndexReaderStats> stats = new ArrayList<>();
        for (IndexService indexService : indicesService) {
            String table = indexService.getIndexSettings().getSettings().get(LanceEngineFactory.TABLE_SETTING);
            if (table == null || table.isEmpty()) {
                continue;
            }
            for (IndexShard shard : indexService) {
                if (shard.state() != IndexShardState.STARTED) {
                    continue;
                }
                try (Engine.Searcher searcher = shard.acquireSearcher("lance_stats")) {
                    LanceDirectoryReader reader = LanceDirectoryReader.unwrap(searcher.getDirectoryReader());
                    if (reader == null) {
                        continue;
                    }
                    // Count through the searcher's reader chain, not the
                    // Lance reader underneath it: a DLS / FLS wrapper the
                    // index interposes filters through liveDocs, and the
                    // caller's stats must not count rows the wrapper
                    // hides. Only the table's rows outside a cut reader
                    // have no wrapped figure; that one comes from Lance.
                    //
                    // A table with nested columns inflates numDocs with
                    // one hidden child doc per nested element. The visible
                    // parents are counted through the wrapped searcher with
                    // the non nested filter (parents carry _primary_term
                    // doc values, child docs do not), so a wrapper that
                    // hides rows subtracts them from both figures; tables
                    // without nested fields keep the plain numDocs and pay
                    // for no count.
                    long numDocs = searcher.getIndexReader().numDocs();
                    long shardReaderRows;
                    long nestedDocs;
                    if (indexService.mapperService().hasNested()) {
                        shardReaderRows = searcher.count(Queries.newNonNestedFilter());
                        nestedDocs = numDocs - shardReaderRows;
                    } else {
                        shardReaderRows = numDocs;
                        nestedDocs = 0L;
                    }
                    long rows = reader.luceneBoundExceeded() ? reader.tableRows() : shardReaderRows;
                    // One describeIndices on the reader's already-open
                    // dataset per index per stats call (indexes are
                    // single-shard, so per shard is per index). Skipped
                    // when a DLS/FLS reader wrapper is installed: the
                    // Lance metadata does not pass through the wrapper,
                    // and the report must not reveal column names the
                    // wrapper hides. A describeIndices failure likewise
                    // leaves the map empty rather than dropping the
                    // reader's row figures.
                    Map<String, List<String>> indexTypes;
                    if (hasReaderWrapper(indexService)) {
                        indexTypes = Map.of();
                    } else {
                        try {
                            indexTypes = reader.columnIndexTypes();
                        } catch (Exception e) {
                            LOGGER.debug("lance.stats: describeIndices failed for {}: {}", shard.shardId(), e.toString());
                            indexTypes = Map.of();
                        }
                    }
                    stats.add(
                        new LanceNodeStats.IndexReaderStats(
                            shard.shardId().getIndexName(),
                            rows,
                            shardReaderRows,
                            nestedDocs,
                            reader.luceneBoundExceeded(),
                            indexTypes
                        )
                    );
                } catch (Exception e) {
                    LOGGER.debug("lance.stats: shard {} has no reader to report: {}", shard.shardId(), e.toString());
                }
            }
        }
        stats.sort(Comparator.comparing(LanceNodeStats.IndexReaderStats::index));
        return stats;
    }
}
