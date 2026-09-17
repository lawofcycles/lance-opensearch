/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.BucketCollectorProcessor;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.dfs.DfsSearchResult;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.StoredFieldsContext;
import org.opensearch.search.fetch.subphase.FetchDocValuesContext;
import org.opensearch.search.fetch.subphase.FetchFieldsContext;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.ScriptFieldsContext;
import org.opensearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ScrollContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.profile.Profilers;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.search.suggest.SuggestionSearchContext;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.unit.TimeValue;

/**
 * Minimal {@link SearchContext} implementation the fragment path uses to invoke
 * OpenSearch's stock aggregator machinery against a per-fragment
 * {@link org.opensearch.lance.engine.LanceFragmentLeafReader}.
 *
 * <p>Rationale: the fragment path fan-outs work at the Lance fragment level. On
 * each data node the handler assembles a {@link org.apache.lucene.index.DirectoryReader}
 * whose leaves are the fragments assigned to that node. Running an aggregator
 * over that reader requires a {@code SearchContext}, but OpenSearch's
 * {@code DefaultSearchContext} is bound to a full {@link IndexShard} lifecycle
 * (a shard-local {@code SearcherSupplier}, a bound {@code SearchRequest},
 * fetch phases, and so on) that the fragment handler does not have and does
 * not need. This class exposes only the pieces the aggregator base classes
 * touch during construction and per-leaf collection:
 * {@link #bigArrays()} for accumulator allocations,
 * {@link #searcher()} for {@link ContextIndexSearcher#search},
 * {@link #getQueryShardContext()} for {@code ValuesSourceConfig} resolution,
 * {@link #mapperService()} for field-type lookup,
 * {@link #shardTarget()} for error messages,
 * {@link #addReleasable} for cleanup tracking,
 * {@link #aggregations()} for the built {@link SearchContextAggregations},
 * {@link #bitsetFilterCache()} for nested doc collectors, and a couple of
 * bookkeeping getters ({@link #numberOfShards()}, {@link #query()},
 * {@link #from()}, {@link #size()}, {@link #bucketCollectorProcessor()}).
 * Every other {@link SearchContext} method throws
 * {@link UnsupportedOperationException} on purpose: if a code path the
 * fragment handler drives ever needs one, its failure surfaces immediately
 * with a clear stack trace instead of silently returning a null or default.
 *
 * <p>Non-abstract {@link SearchContext} defaults (for example
 * {@code asLocalBucketCountThresholds}, {@code maxAggRewriteFilters},
 * {@code termsAggregationMaxPrecomputeCardinality},
 * {@code cardinalityAggregationContext}) are inherited unchanged. The
 * {@code AggregatorTestCase} pattern re-declares them through Mockito
 * {@code when(...).thenCallRealMethod()}; here we simply leave them alone.
 *
 * <p>Lifecycle: {@link #close()} releases every {@code Releasable} registered
 * through {@link #addReleasable} (aggregators register themselves during
 * construction). The caller owns the underlying {@link ContextIndexSearcher}
 * and closes it separately when the per-fragment reader is done.
 */
public final class LanceFragmentSearchContext extends SearchContext {

    private final ShardId shardId;
    private final IndexShard indexShard;
    private final MapperService mapperService;
    private QueryShardContext queryShardContext;
    private ContextIndexSearcher searcher;
    private final BigArrays bigArrays;
    private final SearchShardTarget shardTarget;
    private final BitsetFilterCache bitsetFilterCache;
    private final Query query;
    private final SearchContextAggregations aggregations;
    private BucketCollectorProcessor bucketCollectorProcessor = new BucketCollectorProcessor();
    private final List<Releasable> releasables = new ArrayList<>();

    /**
     * Two-phase construction: {@link ContextIndexSearcher} keeps a
     * reference to the enclosing {@link SearchContext}, and
     * {@link org.opensearch.index.IndexService#newQueryShardContext}
     * needs the {@link org.apache.lucene.search.IndexSearcher} at
     * construction. The caller builds this class first (with
     * {@code queryShardContext = null}), then constructs the
     * ContextIndexSearcher against this context, then builds the
     * QueryShardContext with the searcher, then attaches both back
     * through {@link #withSearcher(ContextIndexSearcher)} and
     * {@link #withQueryShardContext(QueryShardContext)}. This mirrors
     * the pattern {@link org.opensearch.search.DefaultSearchContext}
     * uses internally, where the SearchContext and its
     * ContextIndexSearcher refer to each other.
     */
    public LanceFragmentSearchContext(
        IndexShard indexShard,
        Query query,
        SearchContextAggregations aggregations,
        BigArrays bigArrays,
        BitsetFilterCache bitsetFilterCache,
        String localNodeId
    ) {
        this.indexShard = indexShard;
        this.shardId = indexShard.shardId();
        this.mapperService = indexShard.mapperService();
        this.queryShardContext = null;
        this.searcher = null;
        this.bigArrays = bigArrays;
        this.bitsetFilterCache = bitsetFilterCache;
        this.query = query;
        this.aggregations = aggregations;
        this.shardTarget = new SearchShardTarget(localNodeId, shardId, null, OriginalIndices.NONE);
    }

    /**
     * Attach the {@link ContextIndexSearcher} the aggregator machinery
     * will drive. Call exactly once after constructing the fragment
     * search context; {@link SearchContext#searcher()} throws
     * {@link IllegalStateException} until this is called.
     */
    public LanceFragmentSearchContext withSearcher(ContextIndexSearcher searcher) {
        this.searcher = searcher;
        return this;
    }

    /**
     * Attach the {@link QueryShardContext} the aggregator machinery
     * will resolve ValuesSourceConfig against. Call exactly once
     * after constructing the fragment search context and building
     * the QueryShardContext against
     * {@link org.opensearch.index.IndexService#newQueryShardContext}
     * with {@link #searcher()};
     * {@link SearchContext#getQueryShardContext()} throws
     * {@link IllegalStateException} until this is called.
     */
    public LanceFragmentSearchContext withQueryShardContext(QueryShardContext queryShardContext) {
        this.queryShardContext = queryShardContext;
        return this;
    }

    // ------------- Real implementations -------------

    @Override
    public BigArrays bigArrays() {
        return bigArrays;
    }

    @Override
    public ContextIndexSearcher searcher() {
        if (searcher == null) {
            throw new IllegalStateException("LanceFragmentSearchContext.searcher() called before withSearcher(...) attached the ContextIndexSearcher");
        }
        return searcher;
    }

    @Override
    public IndexShard indexShard() {
        return indexShard;
    }

    @Override
    public MapperService mapperService() {
        return mapperService;
    }

    @Override
    public QueryShardContext getQueryShardContext() {
        if (queryShardContext == null) {
            throw new IllegalStateException(
                "LanceFragmentSearchContext.getQueryShardContext() called before withQueryShardContext(...) attached the QueryShardContext"
            );
        }
        return queryShardContext;
    }

    @Override
    public SearchShardTarget shardTarget() {
        return shardTarget;
    }

    @Override
    public BitsetFilterCache bitsetFilterCache() {
        return bitsetFilterCache;
    }

    @Override
    public Query query() {
        return query;
    }

    @Override
    public int numberOfShards() {
        return 1;
    }

    @Override
    public SearchContextAggregations aggregations() {
        return aggregations;
    }

    @Override
    public SearchContext aggregations(SearchContextAggregations aggregations) {
        throw uoe("aggregations(SearchContextAggregations)");
    }

    @Override
    public MappedFieldType fieldType(String name) {
        return mapperService.fieldType(name);
    }

    @Override
    public void addReleasable(Releasable releasable) {
        releasables.add(releasable);
    }

    @Override
    public BucketCollectorProcessor bucketCollectorProcessor() {
        return bucketCollectorProcessor;
    }

    @Override
    public void setBucketCollectorProcessor(BucketCollectorProcessor bucketCollectorProcessor) {
        this.bucketCollectorProcessor = bucketCollectorProcessor;
    }

    @Override
    public long getRelativeTimeInMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public boolean lowLevelCancellation() {
        return false;
    }

    @Override
    public int from() {
        return 0;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public float queryBoost() {
        return 1.0f;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public int getTargetMaxSliceCount() {
        return 1;
    }

    @Override
    public boolean shouldUseTimeSeriesDescSortOptimization() {
        return false;
    }

    @Override
    public Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> queryCollectorManagers() {
        return Map.of();
    }

    @Override
    public void doClose() {
        Releasables.close(releasables);
    }

    // ------------- Unsupported (fragment path aggregator use has no need) -------------

    @Override
    public void setTask(SearchShardTask task) {
        throw uoe("setTask");
    }

    @Override
    public SearchShardTask getTask() {
        throw uoe("getTask");
    }

    @Override
    public void preProcess(boolean rewrite) {
        throw uoe("preProcess");
    }

    @Override
    public Query buildFilteredQuery(Query query) {
        throw uoe("buildFilteredQuery");
    }

    @Override
    public ShardSearchContextId id() {
        throw uoe("id");
    }

    @Override
    public String source() {
        throw uoe("source");
    }

    @Override
    public ShardSearchRequest request() {
        throw uoe("request");
    }

    @Override
    public SearchType searchType() {
        throw uoe("searchType");
    }

    @Override
    public ScrollContext scrollContext() {
        throw uoe("scrollContext");
    }

    @Override
    public void addSearchExt(SearchExtBuilder searchExtBuilder) {
        throw uoe("addSearchExt");
    }

    @Override
    public SearchExtBuilder getSearchExt(String name) {
        throw uoe("getSearchExt");
    }

    @Override
    public SearchHighlightContext highlight() {
        throw uoe("highlight");
    }

    @Override
    public void highlight(SearchHighlightContext highlight) {
        throw uoe("highlight(SearchHighlightContext)");
    }

    @Override
    public SuggestionSearchContext suggest() {
        throw uoe("suggest");
    }

    @Override
    public void suggest(SuggestionSearchContext suggest) {
        throw uoe("suggest(SuggestionSearchContext)");
    }

    @Override
    public List<RescoreContext> rescore() {
        throw uoe("rescore");
    }

    @Override
    public void addRescore(RescoreContext rescore) {
        throw uoe("addRescore");
    }

    @Override
    public boolean hasScriptFields() {
        return false;
    }

    @Override
    public ScriptFieldsContext scriptFields() {
        throw uoe("scriptFields");
    }

    @Override
    public boolean sourceRequested() {
        return false;
    }

    @Override
    public boolean hasFetchSourceContext() {
        return false;
    }

    @Override
    public FetchSourceContext fetchSourceContext() {
        throw uoe("fetchSourceContext");
    }

    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        throw uoe("fetchSourceContext(FetchSourceContext)");
    }

    @Override
    public FetchDocValuesContext docValuesContext() {
        throw uoe("docValuesContext");
    }

    @Override
    public SearchContext docValuesContext(FetchDocValuesContext docValuesContext) {
        throw uoe("docValuesContext(FetchDocValuesContext)");
    }

    @Override
    public FetchFieldsContext fetchFieldsContext() {
        throw uoe("fetchFieldsContext");
    }

    @Override
    public SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext) {
        throw uoe("fetchFieldsContext(FetchFieldsContext)");
    }

    @Override
    public SimilarityService similarityService() {
        throw uoe("similarityService");
    }

    @Override
    public TimeValue timeout() {
        throw uoe("timeout");
    }

    @Override
    public void timeout(TimeValue timeout) {
        throw uoe("timeout(TimeValue)");
    }

    @Override
    public int terminateAfter() {
        throw uoe("terminateAfter");
    }

    @Override
    public void terminateAfter(int terminateAfter) {
        throw uoe("terminateAfter(int)");
    }

    @Override
    public SearchContext minimumScore(float minimumScore) {
        throw uoe("minimumScore(float)");
    }

    @Override
    public Float minimumScore() {
        throw uoe("minimumScore");
    }

    @Override
    public SearchContext sort(SortAndFormats sort) {
        throw uoe("sort(SortAndFormats)");
    }

    @Override
    public SortAndFormats sort() {
        return null;
    }

    @Override
    public SearchContext trackScores(boolean trackScores) {
        throw uoe("trackScores(boolean)");
    }

    @Override
    public boolean trackScores() {
        return false;
    }

    @Override
    public SearchContext trackTotalHitsUpTo(int trackTotalHits) {
        throw uoe("trackTotalHitsUpTo(int)");
    }

    @Override
    public int trackTotalHitsUpTo() {
        throw uoe("trackTotalHitsUpTo");
    }

    @Override
    public SearchContext searchAfter(FieldDoc searchAfter) {
        throw uoe("searchAfter(FieldDoc)");
    }

    @Override
    public FieldDoc searchAfter() {
        return null;
    }

    @Override
    public SearchContext collapse(CollapseContext collapse) {
        throw uoe("collapse(CollapseContext)");
    }

    @Override
    public CollapseContext collapse() {
        return null;
    }

    @Override
    public SearchContext parsedPostFilter(ParsedQuery postFilter) {
        throw uoe("parsedPostFilter(ParsedQuery)");
    }

    @Override
    public ParsedQuery parsedPostFilter() {
        return null;
    }

    @Override
    public Query aliasFilter() {
        return null;
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        throw uoe("parsedQuery(ParsedQuery)");
    }

    @Override
    public ParsedQuery parsedQuery() {
        return null;
    }

    @Override
    public SearchContext from(int from) {
        throw uoe("from(int)");
    }

    @Override
    public SearchContext size(int size) {
        throw uoe("size(int)");
    }

    @Override
    public boolean hasStoredFields() {
        return false;
    }

    @Override
    public boolean hasStoredFieldsContext() {
        return false;
    }

    @Override
    public boolean storedFieldsRequested() {
        return false;
    }

    @Override
    public StoredFieldsContext storedFieldsContext() {
        return null;
    }

    @Override
    public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
        throw uoe("storedFieldsContext(StoredFieldsContext)");
    }

    @Override
    public boolean explain() {
        return false;
    }

    @Override
    public void explain(boolean explain) {
        throw uoe("explain(boolean)");
    }

    @Override
    public List<String> groupStats() {
        return List.of();
    }

    @Override
    public void groupStats(List<String> groupStats) {
        throw uoe("groupStats(List)");
    }

    @Override
    public boolean version() {
        return false;
    }

    @Override
    public void version(boolean version) {
        throw uoe("version(boolean)");
    }

    @Override
    public boolean seqNoAndPrimaryTerm() {
        return false;
    }

    @Override
    public void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {
        throw uoe("seqNoAndPrimaryTerm(boolean)");
    }

    @Override
    public int[] docIdsToLoad() {
        throw uoe("docIdsToLoad");
    }

    @Override
    public int docIdsToLoadFrom() {
        throw uoe("docIdsToLoadFrom");
    }

    @Override
    public int docIdsToLoadSize() {
        throw uoe("docIdsToLoadSize");
    }

    @Override
    public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
        throw uoe("docIdsToLoad(int[], int, int)");
    }

    @Override
    public DfsSearchResult dfsResult() {
        throw uoe("dfsResult");
    }

    @Override
    public QuerySearchResult queryResult() {
        throw uoe("queryResult");
    }

    @Override
    public FetchPhase fetchPhase() {
        throw uoe("fetchPhase");
    }

    @Override
    public FetchSearchResult fetchResult() {
        throw uoe("fetchResult");
    }

    @Override
    public Profilers getProfilers() {
        return null;
    }

    @Override
    public ObjectMapper getObjectMapper(String name) {
        return null;
    }

    @Override
    public ReaderContext readerContext() {
        throw uoe("readerContext");
    }

    @Override
    public InternalAggregation.ReduceContext partialOnShard() {
        throw uoe("partialOnShard");
    }

    // ------------- helpers -------------

    private static UnsupportedOperationException uoe(String methodSignature) {
        return new UnsupportedOperationException(
            "LanceFragmentSearchContext does not support " + methodSignature + "; fragment path aggregator use has no need for it"
        );
    }
}
