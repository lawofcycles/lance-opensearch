/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.IndexService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.engine.FetchTakeStats;
import org.opensearch.lance.engine.FragmentGroupScan;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.query.LanceInvalidInput;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.fetch.subphase.FetchFieldsContext;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Per-node handler for {@link LanceFragmentFetchAction}: renders the rows
 * of a merged page that this node's query round collected. The request
 * names the rows by Lance row address and the table version they belong
 * to; the handler takes the snapshot of that version from the node's
 * {@link LanceWarmCache} (the query round left it there), opens a reader
 * over the fragments the addresses name, and renders each row through
 * the same {@link FragmentFetchPhase} the query round renders a page
 * with, under the same per hit projections, so a hit of a two round
 * request is byte for byte the hit of a one round request. The rows go
 * through the leaf's stored fields path, so the fetch cache serves the
 * rows it holds and the take scans are judged by the admission gate and
 * counted under {@code fetch} in the node stats, as on the query round.
 *
 * <p>The reader wrapper the security plugin installs is applied when
 * present, as on the query round; the coordinator does not defer the
 * hits of a wrapped index, and an executor that finds a wrapper renders
 * its page on the query round, so this handler sees a wrapper only when
 * one appeared between the two rounds.
 *
 * <p>Runs on the SEARCH pool like the query round; the intra request
 * work (the takes of the leaves side by side) runs on the
 * {@code index_searcher} pool under
 * {@code plugins.lance.fragment_path.parallelism}.
 */
public final class TransportLanceFragmentFetchAction extends HandledTransportAction<LanceFragmentFetchRequest, LanceFragmentFetchResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportLanceFragmentFetchAction.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final BigArrays bigArrays;
    private final CircuitBreakerService circuitBreakerService;
    private final LanceWarmCache warmCache;
    private final Executor intraRequestExecutor;
    private final FragmentFetchPhase fetchPhase = new FragmentFetchPhase();

    @Inject
    public TransportLanceFragmentFetchAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndicesService indicesService,
        BigArrays bigArrays,
        CircuitBreakerService circuitBreakerService,
        LanceWarmCache warmCache
    ) {
        super(LanceFragmentFetchAction.NAME, transportService, actionFilters, LanceFragmentFetchRequest::new, ThreadPool.Names.SEARCH);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.bigArrays = bigArrays;
        this.circuitBreakerService = circuitBreakerService;
        this.warmCache = warmCache;
        this.intraRequestExecutor = transportService.getThreadPool().executor(TransportLanceFragmentQueryAction.INTRA_REQUEST_POOL);
    }

    @Override
    protected void doExecute(Task task, LanceFragmentFetchRequest request, ActionListener<LanceFragmentFetchResponse> listener) {
        try {
            long start = System.nanoTime();
            LanceFragmentFetchResponse response = execute(request, LanceCancellation.of(task instanceof CancellableTask c ? c : null));
            LOGGER.debug(
                "lance.dispatch: fragment fetch for [{}] of {} rows took {} us",
                request.indexName(),
                request.rowAddrs().length,
                (System.nanoTime() - start) / 1_000L
            );
            listener.onResponse(response);
        } catch (Exception e) {
            // The same reporting as the query round: a cancelled task is
            // the expected outcome of a cancellation, Lance's invalid
            // input keeps its own class so the client sees 400, and
            // everything else is a server error.
            TaskCancelledException cancelled = LanceCancellation.findCancelled(e);
            if (cancelled != null) {
                LOGGER.debug(
                    "fragment fetch for [{}] on [{}] was cancelled: {}",
                    request.indexName(),
                    request.tableUri(),
                    cancelled.getMessage()
                );
                listener.onFailure(cancelled);
                return;
            }
            Exception reported = LanceInvalidInput.unwrap(e);
            if (reported == e) {
                LOGGER.warn("fragment fetch failed on this node for [{}], {} rows", request.tableUri(), request.rowAddrs().length, e);
            }
            listener.onFailure(reported);
        }
    }

    /**
     * Render the request's rows; package-private so unit tests can call
     * it without the transport layer.
     */
    LanceFragmentFetchResponse execute(LanceFragmentFetchRequest request, LanceCancellation cancellation) throws Exception {
        IndexMetadata indexMetadata = clusterService.state().metadata().index(request.indexName());
        if (indexMetadata == null) {
            throw new IllegalStateException("Fragment path cannot resolve OpenSearch index [" + request.indexName() + "] on this node");
        }
        String pkField = LancePlugin.PRIMARY_KEY_FIELD_SETTING.get(indexMetadata.getSettings());
        LancePrimaryKeyType pkType = pkField.isEmpty()
            ? LancePrimaryKeyType.NONE
            : LancePrimaryKeyType.fromSetting(LancePlugin.PRIMARY_KEY_TYPE_SETTING.get(indexMetadata.getSettings()));
        LanceOverrides overrides = LanceOverrides.of(indexMetadata.getSettings());
        // The fragments the addresses name, in address order: the reader
        // is opened over those alone.
        Set<Integer> fragments = new LinkedHashSet<>();
        for (long rowAddr : request.rowAddrs()) {
            fragments.add((int) (rowAddr >>> 32));
        }
        List<Integer> fragmentIds = new ArrayList<>(fragments);
        try (
            LanceWarmCache.Lease lease = warmCache.acquire(
                indexMetadata.getIndexUUID(),
                request.tableUri(),
                request.storageOptions(),
                request.versionOrEmpty(),
                pkField,
                pkType,
                overrides
            )
        ) {
            LanceWarmCache.Snapshot snapshot = lease.snapshot();
            return FragmentExecutorSupport.withIndexService(
                indicesService,
                indexMetadata,
                "fragment fetch",
                indexService -> render(indexService, indexMetadata, snapshot, fragmentIds, request, cancellation)
            );
        }
    }

    private LanceFragmentFetchResponse render(
        IndexService indexService,
        IndexMetadata indexMetadata,
        LanceWarmCache.Snapshot snapshot,
        List<Integer> fragmentIds,
        LanceFragmentFetchRequest request,
        LanceCancellation cancellation
    ) throws Exception {
        long fetchStart = System.nanoTime();
        FetchTakeStats.Accumulator takes = new FetchTakeStats.Accumulator();
        ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper = TransportLanceFragmentQueryAction
            .resolveReaderWrapper(indexService);
        FragmentGroupScan groupScan = new FragmentGroupScan(
            intraRequestExecutor,
            clusterService.getClusterSettings().get(LancePlugin.FRAGMENT_PATH_PARALLELISM_SETTING),
            cancellation
        );
        try (
            DirectoryReader dr = FragmentExecutorSupport.openWrappedReader(
                shardId,
                snapshot,
                snapshot.isCached() ? warmCache.columnStore() : null,
                fragmentIds,
                null,
                readerWrapper,
                groupScan,
                takes,
                request.projection().takeProjection(snapshot.schema()),
                circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
            )
        ) {
            MultiBucketConsumer bucketConsumer = new MultiBucketConsumer(
                Integer.MAX_VALUE,
                circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
            );
            try (
                LanceFragmentSearchContext searchContext = new LanceFragmentSearchContext(
                    shardId,
                    indexService.mapperService(),
                    MatchAllDocsQuery.INSTANCE,
                    new SearchContextAggregations(AggregatorFactories.EMPTY, bucketConsumer),
                    bigArrays,
                    indexService.cache().bitsetFilterCache(),
                    clusterService.localNode().getId()
                ).withCancellation(cancellation)
            ) {
                // One slice and no executor: nothing is collected here,
                // the searcher only carries the reader, the accounting
                // the admission gate judges the takes on, and the
                // cancellation into the fetch phase.
                LanceFragmentIndexSearcher searcher = new LanceFragmentIndexSearcher(
                    dr,
                    indexService.getIndexSettings(),
                    searchContext,
                    circuitBreakerService.getBreaker(CircuitBreaker.REQUEST),
                    null
                );
                searchContext.withSearcher(searcher);
                QueryShardContext qsc = indexService.newQueryShardContext(0, searcher, System::currentTimeMillis, null);
                searchContext.withQueryShardContext(qsc);
                searchContext.withProjection(
                    request.projection().fetchSource(),
                    request.projection().storedFields(),
                    FragmentExecutorSupport.resolveDocValuesContext(request.projection(), indexService),
                    request.projection().fetchFields().isEmpty() ? null : new FetchFieldsContext(request.projection().fetchFields()),
                    false
                );
                List<SearchHit> hits = FragmentHitsPages.render(
                    searchContext,
                    fetchPhase,
                    searcher.getIndexReader(),
                    request.rowAddrs(),
                    groupScan
                );
                LanceFragmentQueryResponse.Profile profile = new LanceFragmentQueryResponse.Profile(
                    0L,
                    (System.nanoTime() - fetchStart) / 1_000_000L,
                    takes.takeCount(),
                    takes.takeRows(),
                    takes.takeMillis(),
                    takes.takeColumns(),
                    0L
                );
                return new LanceFragmentFetchResponse(hits, profile);
            }
        }
    }
}
