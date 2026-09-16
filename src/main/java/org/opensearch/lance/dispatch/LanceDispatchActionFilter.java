/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.lance.Dataset;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.Index;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;

/**
 * ActionFilter that intercepts {@code indices:data/read/search} for
 * Lance-backed indexes when {@code lance.dispatch.mode} is set to
 * {@code fragment}, and routes execution through the plugin's own
 * shard-free dispatch path instead of OpenSearch's standard shard
 * fan-out.
 *
 * <p>Milestone 2 of the shard-free dispatch prototype. The filter now
 * opens the Lance {@code Dataset} directly through the shared
 * {@link LanceRegistry}, enumerates its fragments, and answers
 * {@code match_all} queries with the real row count sourced from
 * {@link Dataset#countRows()}. Other query shapes and every query
 * that ships aggregations, sort clauses, or pagination are still
 * delegated to the standard shard-based path via
 * {@code chain.proceed(...)}, because their dispatch logic lands in
 * later milestones (per-fragment executor, coordinator merge, fetch
 * phase). The point of Milestone 2 is to prove that the plugin can
 * reach real Lance data from inside an {@link ActionFilter} and
 * emit a valid {@link SearchResponse} without touching the
 * shard-scoped machinery, not to be feature-complete.
 */
public class LanceDispatchActionFilter implements ActionFilter {

    private static final Logger LOGGER = LogManager.getLogger(LanceDispatchActionFilter.class);

    /** Transport action name for the top-level search request. */
    private static final String SEARCH_ACTION_NAME = "indices:data/read/search";

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    /**
     * Current dispatch mode. Held as {@link AtomicReference} so the
     * cluster-settings update consumer can hot-swap the value without
     * synchronising the {@link #apply} hot path.
     */
    private final AtomicReference<LanceDispatchMode> mode;

    public LanceDispatchActionFilter(
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        LanceDispatchMode initialMode
    ) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.mode = new AtomicReference<>(initialMode);
    }

    /**
     * Called from the cluster-settings listener installed by
     * {@code LancePlugin.createComponents}. Only the raw string form
     * arrives from settings, so we parse it here.
     */
    public void setMode(String rawValue) {
        LanceDispatchMode next = LanceDispatchMode.parse(rawValue);
        LanceDispatchMode previous = mode.getAndSet(next);
        if (previous != next) {
            LOGGER.info("lance.dispatch.mode changed [{} -> {}]", previous, next);
        }
    }

    public LanceDispatchMode currentMode() {
        return mode.get();
    }

    @Override
    public int order() {
        // Run before OpenSearch's built-in search resolution filters
        // fire so the short-circuit avoids paying their cost when we
        // are going to bypass shard fan-out anyway. Integer.MIN_VALUE
        // is reserved for security, so keep some headroom.
        return Integer.MIN_VALUE + 100;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionRequestMetadata<Request, Response> actionRequestMetadata,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (mode.get() != LanceDispatchMode.FRAGMENT || !SEARCH_ACTION_NAME.equals(action) || !(request instanceof SearchRequest)) {
            chain.proceed(task, action, request, listener);
            return;
        }

        SearchRequest searchRequest = (SearchRequest) request;
        Index[] concrete = resolveConcreteIndexes(searchRequest);
        if (concrete == null || concrete.length == 0 || !allLanceBacked(concrete)) {
            // Mixed and non-Lance requests continue on the standard
            // shard fan-out. Fragment-level dispatch for mixed queries
            // is deferred to a later milestone; Milestone 2 is only
            // concerned with fully Lance-backed requests.
            chain.proceed(task, action, request, listener);
            return;
        }

        if (!isMatchAllOrEmpty(searchRequest)) {
            // Milestone 2 only implements the match_all path. Anything
            // that carries a real query, an aggregation, a sort, a
            // pagination clause, or a highlighter routes through the
            // standard path where the existing shard-based machinery
            // still handles the request end-to-end. Later milestones
            // grow the fragment executor to cover these shapes.
            chain.proceed(task, action, request, listener);
            return;
        }

        try {
            SearchResponse response = executeMatchAll(concrete, searchRequest);
            @SuppressWarnings("unchecked")
            Response typed = (Response) response;
            listener.onResponse(typed);
        } catch (Exception e) {
            LOGGER.warn("fragment dispatch failed for {}; falling back to shard path", (Object) searchRequest.indices(), e);
            chain.proceed(task, action, request, listener);
        }
    }

    /**
     * Milestone 2 executor. Opens the Lance dataset through the
     * shared {@link LanceRegistry}, records how many fragments the
     * table has (for the log that operators will lean on while the
     * prototype matures), and emits a {@link SearchResponse} whose
     * {@code hits.total.value} matches {@link Dataset#countRows()}.
     * The hits array itself is empty because rendering an actual
     * {@link SearchHit} requires Arrow-to-JSON conversion for
     * {@code _source}, which is deferred to Milestone 3.
     */
    private SearchResponse executeMatchAll(Index[] concrete, SearchRequest searchRequest) {
        long start = System.currentTimeMillis();
        long total = 0L;
        int fragmentCount = 0;
        Metadata metadata = clusterService.state().metadata();
        for (Index index : concrete) {
            IndexMetadata indexMetadata = metadata.index(index);
            if (indexMetadata == null) {
                continue;
            }
            String tableUri = indexMetadata.getSettings().get(LanceEngineFactory.TABLE_SETTING);
            if (tableUri == null || tableUri.isEmpty()) {
                continue;
            }
            StorageOptions storageOptions = StorageOptions.fromIndexSettings(indexMetadata.getSettings());
            try (Dataset dataset = LanceRegistry.openDataset(tableUri, storageOptions)) {
                int fragments = dataset.getFragments().size();
                fragmentCount += fragments;
                long rows = dataset.countRows();
                total += rows;
                LOGGER.info(
                    "lance.dispatch.mode=fragment: match_all over index [{}] table [{}] resolved [{}] fragment(s), [{}] row(s)",
                    index.getName(),
                    tableUri,
                    fragments,
                    rows
                );
            }
        }

        long took = System.currentTimeMillis() - start;
        SearchHits emptyHits = new SearchHits(new SearchHit[0], new TotalHits(total, TotalHits.Relation.EQUAL_TO), Float.NaN);
        SearchResponseSections sections = new SearchResponseSections(emptyHits, null, null, false, false, null, 1);
        return new SearchResponse(
            sections,
            null,
            /* totalShards */ fragmentCount,
            /* successfulShards */ fragmentCount,
            /* skippedShards */ 0,
            took,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    /**
     * Resolve the request's index expressions against the current
     * cluster state, returning {@code null} on failure so the caller
     * can drop back to the standard code path and surface the usual
     * OpenSearch error rather than a silent no-op.
     */
    private Index[] resolveConcreteIndexes(SearchRequest searchRequest) {
        try {
            return indexNameExpressionResolver.concreteIndices(clusterService.state(), searchRequest);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * True when every concrete index is Lance-backed, judged by the
     * presence of {@link LanceEngineFactory#TABLE_SETTING} on the
     * index metadata. That setting is stamped by
     * {@code RestAttachAction} and the namespace poller at index
     * creation time.
     */
    private boolean allLanceBacked(Index[] concrete) {
        Metadata metadata = clusterService.state().metadata();
        for (Index index : concrete) {
            IndexMetadata indexMetadata = metadata.index(index);
            if (indexMetadata == null) {
                return false;
            }
            Settings settings = indexMetadata.getSettings();
            String tableSetting = settings.get(LanceEngineFactory.TABLE_SETTING);
            if (tableSetting == null || tableSetting.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the request's source either has no {@code source} block
     * at all (default match_all) or its {@code query} clause is exactly
     * {@link MatchAllQueryBuilder}. Any other query builder, or the
     * presence of aggregations / sort / from / size / highlight /
     * suggest, keeps the request on the standard shard path because
     * Milestone 2 does not yet implement those semantics in the
     * fragment executor.
     */
    private boolean isMatchAllOrEmpty(SearchRequest searchRequest) {
        SearchSourceBuilder source = searchRequest.source();
        if (source == null) {
            return true;
        }
        if (source.aggregations() != null
            || source.sorts() != null
            || source.suggest() != null
            || source.highlighter() != null
            || source.postFilter() != null
            || source.searchAfter() != null
            || source.from() > 0
            || source.size() != -1) {
            return false;
        }
        QueryBuilder query = source.query();
        if (query == null) {
            return true;
        }
        return query instanceof MatchAllQueryBuilder;
    }
}
