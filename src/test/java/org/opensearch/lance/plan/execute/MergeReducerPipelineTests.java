/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.execute;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.BigArrays;
import org.opensearch.lance.dispatch.LanceFragmentQueryResponse;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.opensearch.search.aggregations.pipeline.InternalSimpleValue;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The coordinator's aggregation reduce runs the request's pipeline
 * aggregators: the per node trees the executors ship hold only bucket
 * and metric aggregations, and {@link MergeReducer#buildResponse} hands
 * them to {@code InternalAggregations.topLevelReduce} under the final
 * reduce context built from the request's pipeline tree, so a parent
 * pipeline is computed inside the merged buckets and a sibling pipeline
 * over the merged top level, both once over the union of the nodes.
 */
public class MergeReducerPipelineTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        clusterService.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private MergeReducer reducer(AggregatorFactories.Builder aggregations) {
        return new MergeReducer(
            clusterService,
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            aggregations,
            List.of(),
            0,
            0,
            false,
            false,
            false,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
    }

    /** One executor's histogram over {@code rating} with a {@code sum} child per bucket, as the aggregators build it. */
    private static InternalAggregations histogramOf(double[] keys, double[] sums) {
        List<InternalHistogram.Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < keys.length; i++) {
            InternalAggregations children = InternalAggregations.from(List.of(new InternalSum("s", sums[i], DocValueFormat.RAW, Map.of())));
            buckets.add(new InternalHistogram.Bucket(keys[i], 1, false, DocValueFormat.RAW, children));
        }
        InternalHistogram histogram = new InternalHistogram(
            "h",
            buckets,
            BucketOrder.key(true),
            1,
            null,
            DocValueFormat.RAW,
            false,
            Map.of()
        );
        return InternalAggregations.from(List.of(histogram));
    }

    private static LanceFragmentQueryResponse response(InternalAggregations aggregations) {
        return new LanceFragmentQueryResponse(1, false, 1, List.of(), new long[0], aggregations);
    }

    public void testParentAndSiblingPipelinesRunOnceOverTheMergedBuckets() throws IOException {
        // The two executors overlap on bucket 1: the reduce sums the
        // children first (1, 2 + 3, 4), then the cumulative sum walks
        // the merged buckets (1, 6, 10) and the sibling avg_bucket
        // reads the merged sums ((1 + 5 + 4) / 3).
        AggregatorFactories.Builder aggregations = PlanTestFixtures.parse(
            "{\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"rating\",\"interval\":1},"
                + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}},\"cs\":{\"cumulative_sum\":{\"buckets_path\":\"s\"}}}},"
                + "\"ab\":{\"avg_bucket\":{\"buckets_path\":\"h>s\"}}}}"
        ).aggregations();
        MergeReducer reducer = reducer(aggregations);
        reducer.absorbTarget(
            "idx",
            List.of(
                response(histogramOf(new double[] { 0d, 1d }, new double[] { 1d, 2d })),
                response(histogramOf(new double[] { 1d, 2d }, new double[] { 3d, 4d }))
            ),
            false
        );
        SearchResponse response = reducer.buildResponse(System.currentTimeMillis());

        Histogram histogram = response.getAggregations().get("h");
        assertEquals(3, histogram.getBuckets().size());
        double[] expectedSums = { 1d, 5d, 4d };
        double[] expectedCumulative = { 1d, 6d, 10d };
        for (int i = 0; i < 3; i++) {
            Histogram.Bucket bucket = histogram.getBuckets().get(i);
            assertEquals((double) i, ((Number) bucket.getKey()).doubleValue(), 0d);
            NumericMetricsAggregation.SingleValue sum = bucket.getAggregations().get("s");
            assertEquals("sum of bucket " + i, expectedSums[i], sum.value(), 0d);
            InternalSimpleValue cumulative = bucket.getAggregations().get("cs");
            assertEquals("cumulative_sum at bucket " + i, expectedCumulative[i], cumulative.value(), 0d);
        }
        InternalSimpleValue avgBucket = response.getAggregations().get("ab");
        assertEquals(10d / 3d, avgBucket.value(), 1e-9d);
    }

    public void testATreeWithoutPipelinesReducesAsBefore() throws IOException {
        AggregatorFactories.Builder aggregations = PlanTestFixtures.parse(
            "{\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"rating\",\"interval\":1},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}}}"
        ).aggregations();
        MergeReducer reducer = reducer(aggregations);
        reducer.absorbTarget(
            "idx",
            List.of(
                response(histogramOf(new double[] { 0d }, new double[] { 1d })),
                response(histogramOf(new double[] { 0d }, new double[] { 2d }))
            ),
            false
        );
        SearchResponse response = reducer.buildResponse(System.currentTimeMillis());
        Histogram histogram = response.getAggregations().get("h");
        assertEquals(1, histogram.getBuckets().size());
        NumericMetricsAggregation.SingleValue sum = histogram.getBuckets().get(0).getAggregations().get("s");
        assertEquals(3d, sum.value(), 0d);
        assertEquals(1, response.getAggregations().asList().size());
        assertEquals("h", response.getAggregations().asList().get(0).getName());
    }
}
