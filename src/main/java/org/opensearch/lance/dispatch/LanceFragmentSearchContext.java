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
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.BucketCollectorProcessor;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
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
 * not need. The context is built from the {@link ShardId} and
 * {@link MapperService} of the index alone, so it works on a node that
 * holds no shard copy as long as the index is in cluster state.
 * This class exposes only the pieces the aggregator base classes
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
 * {@link #getTargetMaxSliceCount()}, {@link #shouldUseConcurrentSearch()}
 * and {@link #partialOnShard()} carry the executor's slice count into the
 * searcher and the aggregators so a request collects its fragments on
 * several threads and merges the slice results the way concurrent
 * segment search does on the shard path. The fetch side
 * ({@link #fetchSourceContext()}, {@link #storedFieldsContext()},
 * {@link #docValuesContext()}, {@link #fetchFieldsContext()},
 * {@link #explain()}, {@link #rescore()}, {@link #collapse()},
 * {@link #highlight()} null)
 * answers as {@code DefaultSearchContext} does for the same body, so the
 * stock fetch sub phases {@link FragmentFetchPhase} drives render the
 * hits the shard path would.
 * Every other {@link SearchContext} method throws
 * {@link UnsupportedOperationException} naming the method on purpose: if
 * a code path the fragment handler drives ever needs one (the highlight
 * or suggest sub phases, {@code docIdsToLoad} and {@code fetchResult} of
 * the stock {@code FetchPhase.execute}), its failure surfaces immediately
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
    private final MapperService mapperService;
    private QueryShardContext queryShardContext;
    private ContextIndexSearcher searcher;
    private final BigArrays bigArrays;
    private final SearchShardTarget shardTarget;
    private final BitsetFilterCache bitsetFilterCache;
    private Query query;
    private final SearchContextAggregations aggregations;
    private BucketCollectorProcessor bucketCollectorProcessor = new BucketCollectorProcessor();
    private final List<Releasable> releasables = new ArrayList<>();
    private LanceCancellation cancellation = LanceCancellation.NONE;
    private int targetMaxSliceCount = 1;
    private ScriptService scriptService;
    // The fetch phase's view of the request: what the stock fetch sub
    // phases (source, doc values, fields, explain) read through
    // FetchContext. Absent elements stay null / false, which is what
    // DefaultSearchContext reports for a body without them.
    private FetchSourceContext fetchSourceContext;
    private StoredFieldsContext storedFieldsContext;
    private FetchDocValuesContext docValuesContext;
    private FetchFieldsContext fetchFieldsContext;
    private boolean explain;
    // The second pass over the collected page: the rescore contexts the
    // executor built from the request's rescorers (empty without
    // rescore; ExplainPhase folds their explanations over the query's)
    // and the collapse context whose field the fetch phase adds as a
    // doc value field of every hit (null without collapse).
    private List<RescoreContext> rescore = List.of();
    private CollapseContext collapse;

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
        ShardId shardId,
        MapperService mapperService,
        Query query,
        SearchContextAggregations aggregations,
        BigArrays bigArrays,
        BitsetFilterCache bitsetFilterCache,
        String localNodeId
    ) {
        this.shardId = shardId;
        this.mapperService = mapperService;
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

    /**
     * Attach the cancellation of the task the request runs under. The
     * Lance Weights created against this context's searcher pick it up
     * through {@link LanceCancellation#of(org.apache.lucene.search.IndexSearcher)}
     * and check it between batches; {@link #isCancelled()} reads it.
     * Defaults to {@link LanceCancellation#NONE}.
     */
    public LanceFragmentSearchContext withCancellation(LanceCancellation cancellation) {
        this.cancellation = cancellation == null ? LanceCancellation.NONE : cancellation;
        return this;
    }

    /** The cancellation of the task the request runs under, never null. */
    public LanceCancellation cancellation() {
        return cancellation;
    }

    /**
     * Number of slices the searcher built against this context cuts
     * the reader's leaves into, see {@link #getTargetMaxSliceCount()}.
     * Set before the {@link ContextIndexSearcher} is constructed, since
     * the searcher decides on construction whether it has an executor
     * to run slices on. Values below 1 are treated as 1.
     */
    public LanceFragmentSearchContext withTargetMaxSliceCount(int targetMaxSliceCount) {
        this.targetMaxSliceCount = Math.max(1, targetMaxSliceCount);
        return this;
    }

    /**
     * {@link ScriptService} the slice level reduce of
     * {@link #partialOnShard()} carries. Set before the aggregators run
     * with more than one slice; {@link #partialOnShard()} refuses to
     * build a slice level reduce context without it, rather than hand
     * the reduce a null service that would surface as a
     * {@link NullPointerException} deep inside an aggregation's reduce.
     * None of the aggregations the fragment path accepts runs a script
     * during a reduce today, so the check is what keeps that assumption
     * visible if the allow list ever changes.
     */
    public LanceFragmentSearchContext withScriptService(ScriptService scriptService) {
        this.scriptService = scriptService;
        return this;
    }

    /**
     * The query the fetch phase explains hits against
     * ({@code ExplainPhase} reads it through {@link #query()}). The
     * executor sets it once the request's Lucene query is built: the
     * request's query itself, or the {@link PrebuiltWeightQuery} over the
     * Weight the executor already ran, so an explanation of a Lance
     * scored hit reuses that Weight's scan instead of running a new one.
     * Until then {@link #query()} is the placeholder the constructor
     * received.
     */
    public LanceFragmentSearchContext withQuery(Query query) {
        this.query = query;
        return this;
    }

    /**
     * The per hit projections the fetch phase renders: the
     * {@code _source} filter, the {@code stored_fields} list, the
     * {@code docvalue_fields} resolved against the mapping (patterns
     * expanded, bound by {@code index.max_docvalue_fields_search}, null
     * when the request has none), the {@code fields} list (null when
     * absent) and {@code explain}.
     */
    public LanceFragmentSearchContext withProjection(
        FetchSourceContext fetchSourceContext,
        StoredFieldsContext storedFieldsContext,
        FetchDocValuesContext docValuesContext,
        FetchFieldsContext fetchFieldsContext,
        boolean explain
    ) {
        this.fetchSourceContext = fetchSourceContext;
        this.storedFieldsContext = storedFieldsContext;
        this.docValuesContext = docValuesContext;
        this.fetchFieldsContext = fetchFieldsContext;
        this.explain = explain;
        return this;
    }

    /**
     * The second pass of the request: the {@link RescoreContext}s built
     * from its {@code rescore} list (empty when it has none) and the
     * {@link CollapseContext} built from its {@code collapse} (null when
     * it has none). {@link #rescore()} hands the contexts to the fetch
     * phase's {@code ExplainPhase}, which folds each rescorer's
     * explanation over the query's for the hits the rescorer saw;
     * {@link #collapse()} makes the fetch phase add the collapse field
     * as a doc value field of every hit, the way {@code FetchContext}
     * does on the shard path.
     */
    public LanceFragmentSearchContext withSecondPass(List<RescoreContext> rescore, CollapseContext collapse) {
        this.rescore = rescore == null ? List.of() : List.copyOf(rescore);
        this.collapse = collapse;
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
            throw new IllegalStateException(
                "LanceFragmentSearchContext.searcher() called before withSearcher(...) attached the ContextIndexSearcher"
            );
        }
        return searcher;
    }

    /**
     * Always {@code null}. The fragment path runs on any node that
     * has the index in cluster state, including nodes that hold no
     * shard copy, so there is no {@link IndexShard} to hand out. The
     * stock code reachable from this context does not dereference it:
     * {@link LanceFragmentIndexSearcher} skips the
     * {@code SearchOperationListener} slice callbacks that
     * {@link ContextIndexSearcher} would fetch through it,
     * {@code FilterRewriteOptimizationContext} returns before its
     * {@code indexShard().shardId()} log line because
     * {@link #maxAggRewriteFilters()} keeps the {@link SearchContext}
     * default of 0, and {@code CardinalityAggregator} only reaches
     * its {@code indexShard()} debug line after a successful terms
     * pruning, which the Lance leaf reader never offers
     * ({@code terms(field)} is null). {@code rare_terms}, whose
     * constructor seeds itself from {@code indexShard().shardId()},
     * is not on the {@link LanceAggregationSupport} whitelist and
     * goes to the shard path.
     */
    @Override
    public IndexShard indexShard() {
        return null;
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
        return cancellation.isCancelled();
    }

    /**
     * Upper bound on the slices {@link ContextIndexSearcher#slices}
     * cuts the reader's leaves into: {@code lance.fragment_path.slices}
     * as the executor read it for this request. The leaves are Lance
     * fragments, one per leaf, and the stock supplier bundles them into
     * at most this many slices by row count; a reader with fewer leaves
     * than slices gets one slice per leaf. 1 means the searcher has no
     * executor and collects every leaf on the calling thread.
     */
    @Override
    public int getTargetMaxSliceCount() {
        return targetMaxSliceCount;
    }

    /**
     * True when the searcher may run more than one slice. Read by the
     * aggregators through {@link #asLocalBucketCountThresholds}: under
     * concurrent collection a {@code terms} aggregator keeps every
     * bucket of its slice ({@code shard_min_doc_count} 0) and the slice
     * level reduce of {@link #partialOnShard()} applies the shard
     * thresholds once over the merged buckets, exactly as the shard
     * path does when concurrent segment search is on. With one slice
     * the aggregator applies them itself, as it did before slicing.
     */
    @Override
    public boolean shouldUseConcurrentSearch() {
        return targetMaxSliceCount > 1;
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
        // Read by the fetch phase's nested hit preparation and by the
        // highlight sub phase, which the executor does not run; null is
        // what DefaultSearchContext reports for a body without one.
        return null;
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
        return rescore;
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

    // The _source / stored_fields / docvalue_fields / fields / explain
    // accessors answer as DefaultSearchContext does for the same
    // request elements, so FetchPhase.createStoredFieldsVisitor and the
    // stock fetch sub phases see the request the client sent.

    @Override
    public boolean sourceRequested() {
        return fetchSourceContext != null && fetchSourceContext.fetchSource();
    }

    @Override
    public boolean hasFetchSourceContext() {
        return fetchSourceContext != null;
    }

    @Override
    public FetchSourceContext fetchSourceContext() {
        return fetchSourceContext;
    }

    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    @Override
    public FetchDocValuesContext docValuesContext() {
        return docValuesContext;
    }

    @Override
    public SearchContext docValuesContext(FetchDocValuesContext docValuesContext) {
        this.docValuesContext = docValuesContext;
        return this;
    }

    @Override
    public FetchFieldsContext fetchFieldsContext() {
        return fetchFieldsContext;
    }

    @Override
    public SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext) {
        this.fetchFieldsContext = fetchFieldsContext;
        return this;
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
        return collapse;
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
        return storedFieldsContext != null && storedFieldsContext.fieldNames() != null;
    }

    @Override
    public boolean hasStoredFieldsContext() {
        return storedFieldsContext != null;
    }

    @Override
    public boolean storedFieldsRequested() {
        return storedFieldsContext == null || storedFieldsContext.fetchFields();
    }

    @Override
    public StoredFieldsContext storedFieldsContext() {
        return storedFieldsContext;
    }

    @Override
    public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
        this.storedFieldsContext = storedFieldsContext;
        return this;
    }

    @Override
    public boolean explain() {
        return explain;
    }

    @Override
    public void explain(boolean explain) {
        this.explain = explain;
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

    /**
     * Reduce context for merging the aggregator trees of the slices on
     * the executor, the same one {@code DefaultSearchContext} hands
     * {@code NonGlobalAggCollectorManager}: a partial reduction (no
     * pipelines, no {@code min_doc_count} pruning) marked slice level,
     * so a {@code terms} reduce applies {@code shard_size} and
     * {@code shard_min_doc_count} rather than the request level
     * {@code size} and {@code min_doc_count} the coordinator applies
     * later. The pipeline tree the partial context carries is only read
     * for wire compatibility with nodes before pipelines moved to the
     * coordinator, so it stays empty; the request's pipelines run on
     * the coordinator's final reduce.
     *
     * @throws IllegalStateException when several slices are configured
     *         and no {@link ScriptService} was attached through
     *         {@link #withScriptService}; the slice level reduce is the
     *         only consumer of this context and must not run without one
     */
    @Override
    public InternalAggregation.ReduceContext partialOnShard() {
        if (scriptService == null && shouldUseConcurrentSearch()) {
            throw new IllegalStateException(
                "LanceFragmentSearchContext.partialOnShard() called with "
                    + targetMaxSliceCount
                    + " slices before withScriptService(...) attached the ScriptService the slice level reduce needs"
            );
        }
        InternalAggregation.ReduceContext reduceContext = InternalAggregation.ReduceContext.forPartialReduction(
            bigArrays,
            scriptService,
            () -> PipelineAggregator.PipelineTree.EMPTY
        );
        reduceContext.setSliceLevel(shouldUseConcurrentSearch());
        return reduceContext;
    }

    // ------------- helpers -------------

    private static UnsupportedOperationException uoe(String methodSignature) {
        return new UnsupportedOperationException(
            "LanceFragmentSearchContext does not support "
                + methodSignature
                + "; the fragment executor's hits, aggregation and fetch phases have no need for it"
        );
    }
}
