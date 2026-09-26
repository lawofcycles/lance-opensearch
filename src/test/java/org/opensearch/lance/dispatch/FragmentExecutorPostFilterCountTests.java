/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.LanceAttachResponse;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * A {@code post_filter} page under a {@code track_total_hits} bound is
 * counted by the collector that gathers the page, so the conjunction of
 * the query and the post_filter is run once per request; the exact count
 * of {@code track_total_hits: true} and a {@code size: 0} request count
 * the conjunction in a pass of their own. The post_filter here is a term
 * query whose Lucene Weight counts the leaves it is asked a scorer for:
 * every pass over the conjunction asks once per leaf and the aggregations
 * run over the query alone, so the number of scorer requests divided by
 * the number of fragments is the number of passes over the conjunction.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class FragmentExecutorPostFilterCountTests extends OpenSearchSingleNodeTestCase {

    private static final int FRAGMENTS = 3;
    private static final int ROWS_PER_FRAGMENT = 10_000;
    /** Rows with category c0: i % 3 == 0 and not i % 4 == 3, of 30,000 rows. */
    private static final int C0_ROWS = 7_500;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LancePlugin.class);
    }

    /**
     * One slice: a multi slice searcher asks the Weight for the first
     * leaf's scorer once more on the calling thread before the slices
     * start, which would add one request per pass to the count below.
     */
    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(LancePlugin.FRAGMENT_PATH_SLICES_SETTING.getKey(), 1).build();
    }

    private String attach(String indexName) throws Exception {
        Path dir = createTempDir();
        String tableUri = LanceTableFactory.writeHintFixtureTable(dir, indexName, FRAGMENTS, ROWS_PER_FRAGMENT);
        LanceAttachResponse attached = client().execute(
            LanceAttachAction.INSTANCE,
            new LanceAttachRequest(tableUri, indexName, null, null, StorageOptions.empty(), null)
        ).actionGet();
        assertEquals(indexName, attached.index());
        assertEquals(FRAGMENTS, attached.fragments());
        ensureGreen(indexName);
        return tableUri;
    }

    /** The response of one request and the scorer requests its post_filter clause answered. */
    private record Run(LanceFragmentQueryResponse response, int postFilterScorers) {
    }

    private Run run(String tableUri, String indexName, int size, AggregatorFactories.Builder aggregations, int trackTotalHitsUpTo)
        throws Exception {
        AtomicInteger scorers = new AtomicInteger();
        LanceFragmentQueryRequest request = FragmentRequests.planned(
            getInstanceFromNode(ClusterService.class),
            getInstanceFromNode(LanceWarmCache.class),
            tableUri,
            indexName,
            new MatchAllQueryBuilder(),
            new CountingTermQueryBuilder("category", "c0", scorers),
            List.of(),
            /* searchAfter */ null,
            size,
            aggregations,
            /* fragmentIds */ List.of(),
            /* trackScores */ false,
            trackTotalHitsUpTo
        );
        TransportLanceFragmentQueryAction executor = getInstanceFromNode(TransportLanceFragmentQueryAction.class);
        LanceFragmentQueryResponse response = executor.execute(request);
        return new Run(response, scorers.get());
    }

    private static AggregatorFactories.Builder categoryTerms() {
        return AggregatorFactories.builder().addAggregator(AggregationBuilders.terms("by_category").field("category").size(10));
    }

    /** The aggregation runs over the query alone: every category keeps its full count next to the post_filter. */
    private static void assertUnfilteredBuckets(LanceFragmentQueryResponse response) {
        StringTerms terms = response.aggregations().get("by_category");
        List<String> buckets = terms.getBuckets().stream().map(b -> b.getKeyAsString() + "=" + b.getDocCount()).toList();
        assertEquals(List.of("c0=7500", "c1=7500", "c2=7500"), buckets);
    }

    public void testBoundedPageCountsThePostFilterFromItsCollector() throws Exception {
        String indexName = "post-filter-count-bounded";
        String tableUri = attach(indexName);
        // The bound is below the 7,500 matches: the collector stops
        // counting at the bound and reports a lower bound, and the
        // conjunction was run once, for the page.
        Run run = run(tableUri, indexName, 10, categoryTerms(), 100);
        assertEquals("passes over the conjunction", FRAGMENTS, run.postFilterScorers());
        assertEquals(10, run.response().hits().size());
        assertTrue("the count stopped at the bound: " + run.response().matched(), run.response().matchedIsLowerBound());
        assertTrue("at least the bound was counted: " + run.response().matched(), run.response().matched() >= 100);
        assertTrue("fewer than every match was counted: " + run.response().matched(), run.response().matched() < C0_ROWS);
        assertUnfilteredBuckets(run.response());

        // A bound above the matches: the same one pass, an exact count.
        Run within = run(tableUri, indexName, 10, categoryTerms(), 10_000);
        assertEquals("passes over the conjunction", FRAGMENTS, within.postFilterScorers());
        assertEquals(C0_ROWS, within.response().matched());
        assertFalse(within.response().matchedIsLowerBound());
        assertUnfilteredBuckets(within.response());
    }

    public void testAccuratePageKeepsTheCountOnlyPass() throws Exception {
        String indexName = "post-filter-count-accurate";
        String tableUri = attach(indexName);
        // track_total_hits: true: the page collector stops at its own
        // threshold and the exact count is a second, count only pass
        // over the conjunction.
        Run run = run(tableUri, indexName, 10, categoryTerms(), SearchContext.TRACK_TOTAL_HITS_ACCURATE);
        assertEquals("passes over the conjunction", 2 * FRAGMENTS, run.postFilterScorers());
        assertEquals(10, run.response().hits().size());
        assertEquals(C0_ROWS, run.response().matched());
        assertFalse(run.response().matchedIsLowerBound());
        assertUnfilteredBuckets(run.response());
    }

    public void testDisabledTrackingRunsThePostFilterOnceForThePage() throws Exception {
        String indexName = "post-filter-count-disabled";
        String tableUri = attach(indexName);
        Run run = run(tableUri, indexName, 10, categoryTerms(), SearchContext.TRACK_TOTAL_HITS_DISABLED);
        assertEquals("passes over the conjunction", FRAGMENTS, run.postFilterScorers());
        assertEquals(10, run.response().hits().size());
        assertEquals("no count is tracked", 0L, run.response().matched());
        assertFalse(run.response().matchedIsLowerBound());
        assertUnfilteredBuckets(run.response());
    }

    public void testSizeZeroCountsThePostFilterOnce() throws Exception {
        String indexName = "post-filter-count-size-zero";
        String tableUri = attach(indexName);
        // No page collector: the count is the one pass over the conjunction.
        Run run = run(tableUri, indexName, 0, categoryTerms(), 100);
        assertEquals("passes over the conjunction", FRAGMENTS, run.postFilterScorers());
        assertTrue(run.response().hits().isEmpty());
        assertEquals(C0_ROWS, run.response().matched());
        assertFalse(run.response().matchedIsLowerBound());
        assertUnfilteredBuckets(run.response());
    }

    /** A term query builder whose Lucene query counts the scorer requests its Weight answers. */
    private static final class CountingTermQueryBuilder extends TermQueryBuilder {
        private final AtomicInteger scorers;

        CountingTermQueryBuilder(String field, Object value, AtomicInteger scorers) {
            super(field, value);
            this.scorers = scorers;
        }

        @Override
        protected Query doToQuery(QueryShardContext context) throws IOException {
            return new CountingQuery(super.doToQuery(context), scorers);
        }
    }

    /** Wraps a query; its Weight counts {@link Weight#scorerSupplier} calls and answers no count shortcut. */
    private static final class CountingQuery extends Query {
        private final Query delegate;
        private final AtomicInteger scorers;

        CountingQuery(Query delegate, AtomicInteger scorers) {
            this.delegate = delegate;
            this.scorers = scorers;
        }

        @Override
        public Query rewrite(IndexSearcher searcher) throws IOException {
            Query rewritten = delegate.rewrite(searcher);
            return rewritten == delegate ? this : new CountingQuery(rewritten, scorers);
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            Weight inner = delegate.createWeight(searcher, scoreMode, boost);
            return new FilterWeight(this, inner) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    scorers.incrementAndGet();
                    return in.scorerSupplier(context);
                }

                @Override
                public int count(LeafReaderContext context) {
                    // Every count goes through a scorer, as the doc
                    // values query underneath answers anyway.
                    return -1;
                }
            };
        }

        @Override
        public void visit(QueryVisitor visitor) {
            delegate.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
        }

        @Override
        public String toString(String field) {
            return "counting(" + delegate.toString(field) + ")";
        }

        @Override
        public boolean equals(Object other) {
            return sameClassAs(other) && delegate.equals(((CountingQuery) other).delegate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(classHash(), delegate);
        }
    }
}
