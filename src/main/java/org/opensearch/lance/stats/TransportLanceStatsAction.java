/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
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
                    stats.add(
                        new LanceNodeStats.IndexReaderStats(
                            shard.shardId().getIndexName(),
                            reader.tableRows(),
                            reader.numDocs(),
                            reader.luceneBoundExceeded()
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
