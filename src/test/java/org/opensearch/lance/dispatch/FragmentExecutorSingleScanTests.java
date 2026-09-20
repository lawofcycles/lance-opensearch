/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.lance.LanceCircuitBreaker;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.LanceAttachResponse;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * One Lance scan per fragment request. The executor creates the Weight
 * of a bare Lance clause once and drives the early hint, the hits page,
 * the aggregations and the count through it, also when a
 * {@code post_filter} turns the hits query into a conjunction. Every
 * shard scan of {@code LanceFtsQuery} and {@code LanceKnnQuery} starts
 * with {@code LanceCircuitBreaker.checkAndTrip}, and nothing else in
 * the request path calls it, so the number of breaker checks a request
 * makes is the number of scans it ran.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class FragmentExecutorSingleScanTests extends OpenSearchSingleNodeTestCase {

    private static final int FRAGMENTS = 3;
    private static final int ROWS_PER_FRAGMENT = 200;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LancePlugin.class);
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

    /**
     * Run {@code request} with a breaker that counts its checks and
     * return the response together with the number of scans.
     */
    private record Run(LanceFragmentQueryResponse response, int scans) {
    }

    private Run run(LanceFragmentQueryRequest request) throws Exception {
        TransportLanceFragmentQueryAction executor = getInstanceFromNode(TransportLanceFragmentQueryAction.class);
        CircuitBreaker installed = LanceCircuitBreaker.getBreaker();
        AtomicInteger checks = new AtomicInteger();
        LanceCircuitBreaker.setBreaker(new NoopCircuitBreaker(LanceCircuitBreaker.NAME) {
            @Override
            public long getUsed() {
                checks.incrementAndGet();
                return 0L;
            }
        });
        try {
            LanceFragmentQueryResponse response = executor.execute(request);
            return new Run(response, checks.get());
        } finally {
            LanceCircuitBreaker.setBreaker(installed);
        }
    }

    private static LanceFragmentQueryRequest request(
        String tableUri,
        String indexName,
        QueryBuilder query,
        QueryBuilder postFilter,
        List<SortBuilder<?>> sorts,
        int size,
        AggregatorFactories.Builder aggregations
    ) {
        return new LanceFragmentQueryRequest(
            tableUri,
            indexName,
            StorageOptions.empty(),
            /* pinnedVersion */ -1L,
            /* filterSql */ null,
            query,
            postFilter,
            sorts,
            /* searchAfter */ null,
            size,
            aggregations,
            /* fragmentIds */ List.of(),
            /* trackScores */ false,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
    }

    private static AggregatorFactories.Builder categoryTerms() {
        return AggregatorFactories.builder().addAggregator(AggregationBuilders.terms("by_category").field("category").size(10));
    }

    private static List<String> buckets(LanceFragmentQueryResponse response) {
        StringTerms terms = response.aggregations().get("by_category");
        return terms.getBuckets().stream().map(b -> b.getKeyAsString() + "=" + b.getDocCount()).toList();
    }

    public void testPostFilterPageWithKeywordSortAndTermsRunsOneScan() throws Exception {
        String indexName = "single-scan-post-filter";
        String tableUri = attach(indexName);
        // grp3 matches rows i % 25 == 3 (8 per fragment, 24 in all);
        // category is null for i % 4 == 3, else "c" + (i % 3), so six
        // rows are c0. The early hint, the sorted page, the count and
        // the aggregation all read the hits of the one scan.
        Run run = run(
            request(
                tableUri,
                indexName,
                new LanceMatchQueryBuilder("body", "grp3"),
                new TermQueryBuilder("category", "c0"),
                List.of(new FieldSortBuilder("category").order(SortOrder.ASC), new FieldSortBuilder("id").order(SortOrder.ASC)),
                30,
                categoryTerms()
            )
        );
        assertEquals("scans", 1, run.scans());
        assertEquals(6, run.response().matched());
        assertEquals(6, run.response().hits().size());
        for (SearchHit hit : run.response().hits()) {
            assertEquals("c0", hit.getSortValues()[0].toString());
        }
        assertEquals(List.of("c0=6", "c1=6", "c2=6"), buckets(run.response()));

        // Without the post_filter the same phases share the Weight directly.
        Run plain = run(
            request(
                tableUri,
                indexName,
                new LanceMatchQueryBuilder("body", "grp3"),
                null,
                List.of(new FieldSortBuilder("category").order(SortOrder.ASC), new FieldSortBuilder("id").order(SortOrder.ASC)),
                30,
                categoryTerms()
            )
        );
        assertEquals("scans", 1, plain.scans());
        assertEquals(24, plain.response().matched());
        assertEquals(24, plain.response().hits().size());
        assertEquals(List.of("c0=6", "c1=6", "c2=6"), buckets(plain.response()));
    }

    public void testKnnSizeZeroWithTermsRunsOneScan() throws Exception {
        String indexName = "single-scan-knn";
        String tableUri = attach(indexName);
        float[] vector = new float[8];
        vector[0] = 250.4f;
        // Rows 248..252 are the five nearest: 248 -> c2, 249 -> c0,
        // 250 -> c1, 251 -> null, 252 -> c0. The early hint scans, the
        // aggregation reuses the Weight, and the count goes through the
        // searcher with the same Weight.
        Run run = run(request(tableUri, indexName, new LanceKnnQueryBuilder("embedding", vector, 5), null, List.of(), 0, categoryTerms()));
        assertEquals("scans", 1, run.scans());
        assertEquals(5, run.response().matched());
        assertTrue(run.response().hits().isEmpty());
        assertEquals(List.of("c0=2", "c1=1", "c2=1"), buckets(run.response()));
    }
}
