/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
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
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.Index;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.query.LanceKnnFilterTranslator;
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
 * <p>Milestone 5-A of the shard-free dispatch prototype. The filter
 * now supports every query type
 * {@link org.opensearch.lance.query.LanceKnnFilterTranslator} can
 * lower to a Lance SQL filter: {@code match_all}, {@code term},
 * {@code terms}, {@code exists}, {@code range}, and any {@code bool}
 * combination of those. For those queries the filter opens the
 * Lance {@link Dataset} directly through the shared
 * {@link LanceRegistry}, enumerates its fragments, and answers
 * {@code hits.total.value} from {@link Dataset#countRows(String)}
 * (or {@link Dataset#countRows()} for match_all). Every other query
 * shape — {@code match}, geo, nested, script, etc. — and every
 * request that ships aggregations, sort clauses, or pagination is
 * still delegated to the standard shard-based path via
 * {@code chain.proceed(...)}: their dispatch logic lands in later
 * milestones (per-fragment executor, coordinator merge, aggregation
 * partial reduce). The point of Milestone 5-A is to prove the
 * fragment path can honour a filter query end-to-end (count +
 * hits + {@code _source}) without touching shard-scoped machinery.
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

        Optional<String> filter = resolveDispatchFilter(searchRequest);
        if (filter.isEmpty()) {
            // Milestone 5-A: the request either ships features the
            // fragment executor cannot answer yet (aggregations,
            // sorts, from > 0, search_after, highlighter, suggester,
            // post_filter) or its top-level query builder is outside
            // the LanceKnnFilterTranslator whitelist. Delegate to the
            // standard shard-based path so the request still gets an
            // answer. Extending the whitelist is safe as long as
            // LanceKnnFilterTranslator can translate the new type.
            chain.proceed(task, action, request, listener);
            return;
        }

        try {
            // Empty string marker inside the Optional means "match_all,
            // no SQL filter needed". Everything else goes to the
            // filter-aware path.
            String sql = filter.get();
            String pushedFilter = sql.isEmpty() ? null : sql;
            SearchResponse response = executeQuery(concrete, searchRequest, pushedFilter);
            @SuppressWarnings("unchecked")
            Response typed = (Response) response;
            listener.onResponse(typed);
        } catch (Exception e) {
            LOGGER.warn("fragment dispatch failed for {}; falling back to shard path", (Object) searchRequest.indices(), e);
            chain.proceed(task, action, request, listener);
        }
    }

    /**
     * Default {@code size} the standard search path resolves when the
     * request does not set one. Mirrors OpenSearch's
     * {@code SearchService.DEFAULT_SIZE} so an operator observing a
     * fragment-mode response sees the same hit count they would have
     * seen through shard mode.
     */
    private static final int DEFAULT_SIZE = 10;

    /**
     * Milestone 5-A executor. Opens the Lance dataset through the
     * shared {@link LanceRegistry}, enumerates fragments, and returns
     * up to {@code size} rows as {@link SearchHit hits} whose
     * {@code _source} is rendered from the Arrow batch.
     *
     * <p>When {@code filterSql} is {@code null} the executor is
     * answering {@code match_all}: it uses {@link Dataset#countRows()}
     * for the total and an unfiltered scan. When {@code filterSql} is
     * non-null the executor pushes it down twice — through
     * {@link Dataset#countRows(String)} for the total and through
     * {@link org.lance.ipc.ScanOptions.Builder#filter(String)} for the
     * hits scan — so the count and the returned hits are guaranteed
     * to agree. The SQL string comes from
     * {@link org.opensearch.lance.query.LanceKnnFilterTranslator} which
     * already handles literal escaping and boolean nesting.
     *
     * <p>The hit's {@code _id} is synthesised from the row's
     * {@code _rowaddr} (fragment id in the upper 32 bits, offset in
     * the lower 32 bits) so the identifier is unique within the table
     * without depending on the primary-key column, and matches the
     * value {@link org.opensearch.lance.engine.LanceFragmentLeafReader}
     * emits in shard mode for tables without a declared primary key.
     */
    private SearchResponse executeQuery(Index[] concrete, SearchRequest searchRequest, String filterSql) throws Exception {
        long start = System.currentTimeMillis();
        int effectiveSize = effectiveSize(searchRequest);
        long total = 0L;
        int fragmentCount = 0;
        List<SearchHit> hits = new ArrayList<>();
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
                // Push the filter into countRows so hits.total.value
                // stays consistent with what a full scan under the
                // same filter would return. Match_all takes the
                // no-argument overload because Lance rejects an empty
                // filter string.
                long rows = (filterSql == null) ? dataset.countRows() : dataset.countRows(filterSql);
                total += rows;
                if (hits.size() < effectiveSize && rows > 0) {
                    hits.addAll(scanTopHits(dataset, effectiveSize - hits.size(), filterSql));
                }
                LOGGER.info(
                    "lance.dispatch.mode=fragment: index [{}] table [{}] filter [{}] resolved [{}] fragment(s), [{}] row(s), returning [{}] hit(s)",
                    index.getName(),
                    tableUri,
                    filterSql == null ? "<match_all>" : filterSql,
                    fragments,
                    rows,
                    hits.size()
                );
            }
        }

        long took = System.currentTimeMillis() - start;
        SearchHits searchHits = new SearchHits(
            hits.toArray(new SearchHit[0]),
            new TotalHits(total, TotalHits.Relation.EQUAL_TO),
            hits.isEmpty() ? Float.NaN : 1.0f
        );
        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, false, null, 1);
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
     * Read up to {@code remaining} rows from {@code dataset} and turn
     * them into {@link SearchHit hits}. Each hit carries the
     * synthesised identifier {@code "<fragmentId>-<offsetInFragment>"}
     * matching {@link org.opensearch.lance.engine.LanceFragmentLeafReader}'s
     * fallback naming for PK-less tables, so callers who compare _id
     * across shard mode and fragment mode see the same value on tables
     * without a declared primary key.
     *
     * <p>When {@code filterSql} is non-null the scan pushes the filter
     * into Lance so only matching rows are read. The count path passed
     * the same string to {@link Dataset#countRows(String)}, so
     * {@code hits.total.value} and the actual number of matching hits
     * are guaranteed to agree.
     */
    private List<SearchHit> scanTopHits(Dataset dataset, int remaining, String filterSql) throws Exception {
        if (remaining <= 0) {
            return Collections.emptyList();
        }
        List<SearchHit> out = new ArrayList<>();
        // withRowAddress=true asks Lance to project a synthetic _rowaddr
        // column whose upper 32 bits encode the fragment id and lower
        // 32 bits encode the offset within the fragment. That is enough
        // to build a unique _id per row without picking a column, and
        // matches the identifier LanceFragmentLeafReader emits in
        // shard mode for tables without a declared primary key.
        // No columns filter: the scan projects every column so the
        // renderer can populate _source with the same fields the
        // shard-mode reader would surface. Unsupported types (Float,
        // Struct, etc.) are dropped in the renderer, matching the
        // mapping-time decisions in RestAttachAction.derive.
        ScanOptions.Builder optionsBuilder = new ScanOptions.Builder().withRowAddress(true).limit((long) remaining);
        if (filterSql != null) {
            optionsBuilder.filter(filterSql);
        }
        ScanOptions options = optionsBuilder.build();
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (out.size() < remaining && reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                int rowCount = root.getRowCount();
                for (int i = 0; i < rowCount && out.size() < remaining; i++) {
                    long addr = rowAddr.get(i);
                    int fragmentId = (int) (addr >>> 32);
                    int offset = (int) (addr & 0xFFFFFFFFL);
                    String idString = fragmentId + "-" + offset;
                    SearchHit hit = new SearchHit(i, idString, Collections.emptyMap(), Collections.emptyMap());
                    hit.score(1.0f);
                    byte[] source = LanceRowSourceRenderer.renderJson(root, i);
                    hit.sourceRef(new BytesArray(source));
                    out.add(hit);
                }
            }
        }
        return out;
    }

    private int effectiveSize(SearchRequest searchRequest) {
        SearchSourceBuilder source = searchRequest.source();
        if (source == null || source.size() < 0) {
            return DEFAULT_SIZE;
        }
        return source.size();
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
     * Decide whether the request can be answered by the fragment
     * executor and, if so, what SQL filter to push into Lance.
     *
     * <p>The three possible outcomes are:
     * <ul>
     *   <li>{@link Optional#empty()} — the request carries features
     *       the fragment executor cannot answer yet (aggregations,
     *       sorts, from &gt; 0, search_after, highlighter, suggester,
     *       post_filter) or its top-level query is outside the
     *       {@link LanceKnnFilterTranslator} whitelist. The caller
     *       falls through to the standard shard path.</li>
     *   <li>{@code Optional.of("")} — match_all or an empty request
     *       body. The caller uses {@link Dataset#countRows()} and
     *       an unfiltered scan. An empty string is chosen as the
     *       sentinel because Lance rejects an empty filter string,
     *       so it cannot collide with a translatable filter.</li>
     *   <li>{@code Optional.of(sql)} — the top-level query is a
     *       {@code term}, {@code terms}, {@code exists},
     *       {@code range}, or {@code bool} combination of those,
     *       already translated to Lance SQL and ready to feed to
     *       {@link Dataset#countRows(String)} and
     *       {@link org.lance.ipc.ScanOptions.Builder#filter(String)}.</li>
     * </ul>
     *
     * <p>A translator failure on a nested clause is treated as
     * "not dispatchable" rather than a request error: the shard path
     * can still answer the query. The failure is logged at debug so
     * an operator can see which requests declined fragment dispatch.
     */
    private Optional<String> resolveDispatchFilter(SearchRequest searchRequest) {
        SearchSourceBuilder source = searchRequest.source();
        if (source == null) {
            return Optional.of("");
        }
        if (source.aggregations() != null
            || source.sorts() != null
            || source.suggest() != null
            || source.highlighter() != null
            || source.postFilter() != null
            || source.searchAfter() != null
            || source.from() > 0) {
            return Optional.empty();
        }
        QueryBuilder query = source.query();
        if (query == null || query instanceof MatchAllQueryBuilder) {
            return Optional.of("");
        }
        // Whitelist the top-level query types the translator supports.
        // Others fall through so the shard path can still answer them
        // (match, geo, nested, script, etc.). Extending this list is
        // safe as long as LanceKnnFilterTranslator supports the new
        // type.
        if (query instanceof TermQueryBuilder
            || query instanceof TermsQueryBuilder
            || query instanceof ExistsQueryBuilder
            || query instanceof RangeQueryBuilder
            || query instanceof BoolQueryBuilder) {
            try {
                return Optional.of(LanceKnnFilterTranslator.toLanceSql(query));
            } catch (IllegalArgumentException e) {
                // Translator rejected a nested clause (unsupported
                // value type, mixed date range, etc.). Fall through
                // so the shard path can answer the request instead
                // of failing it.
                LOGGER.debug("fragment dispatch declined for query [{}]: {}", query.getName(), e.getMessage());
                return Optional.empty();
            }
        }
        return Optional.empty();
    }
}
