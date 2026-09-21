/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.List;

import org.opensearch.index.query.QueryBuilders;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.metrics.PercentilesMethod;
import org.opensearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.CumulativeSumPipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The fragment path allow list of {@link LanceAggregationSupport}: which
 * aggregation builders route a request to the fragment path and which
 * keep it on the shard path.
 */
public class LanceAggregationSupportTests extends OpenSearchTestCase {

    private static SearchSourceBuilder sourceWith(AggregationBuilder... aggregations) {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0);
        for (AggregationBuilder aggregation : aggregations) {
            source.aggregation(aggregation);
        }
        return source;
    }

    private static void assertAccepted(AggregationBuilder builder) {
        assertTrue(
            builder.getType() + " [" + builder.getName() + "] belongs on the fragment path",
            LanceAggregationSupport.isBuilderSupported(builder)
        );
        assertTrue(LanceAggregationSupport.isSupported(sourceWith(builder)));
    }

    private static void assertRejected(AggregationBuilder builder) {
        assertFalse(
            builder.getType() + " [" + builder.getName() + "] belongs on the shard path",
            LanceAggregationSupport.isBuilderSupported(builder)
        );
        assertFalse(LanceAggregationSupport.isSupported(sourceWith(builder)));
    }

    public void testNoAggregationsIsSupported() {
        assertTrue(LanceAggregationSupport.isSupported(null));
        assertTrue(LanceAggregationSupport.isSupported(new SearchSourceBuilder()));
        assertFalse(LanceAggregationSupport.hasAggregations(new SearchSourceBuilder()));
        assertTrue(LanceAggregationSupport.hasAggregations(sourceWith(AggregationBuilders.sum("s").field("price"))));
    }

    public void testMetricAggregationsOverAField() {
        assertAccepted(AggregationBuilders.sum("s").field("price"));
        assertAccepted(AggregationBuilders.avg("a").field("price"));
        assertAccepted(AggregationBuilders.min("m").field("price"));
        assertAccepted(AggregationBuilders.max("m").field("price"));
        assertAccepted(AggregationBuilders.count("c").field("price"));
        assertAccepted(AggregationBuilders.stats("st").field("price"));
        assertAccepted(AggregationBuilders.extendedStats("es").field("price").sigma(3d));
        assertAccepted(AggregationBuilders.percentiles("p").field("price"));
        assertAccepted(AggregationBuilders.percentiles("p").field("price").method(PercentilesMethod.HDR));
        assertAccepted(AggregationBuilders.percentiles("p").field("price").percentiles(50d, 99d).keyed(false));
        assertAccepted(AggregationBuilders.percentileRanks("pr", new double[] { 10d, 20d }).field("price"));
        assertAccepted(AggregationBuilders.cardinality("card").field("user_id").precisionThreshold(3000));
        assertAccepted(AggregationBuilders.sum("s").field("price").missing(0d));
    }

    public void testFieldBucketAggregations() {
        assertAccepted(AggregationBuilders.terms("t").field("category").size(20));
        assertAccepted(AggregationBuilders.histogram("h").field("price").interval(10d));
        assertAccepted(AggregationBuilders.dateHistogram("d").field("ts").calendarInterval(DateHistogramInterval.DAY));
        assertAccepted(AggregationBuilders.range("r").field("price").addUnboundedTo(10d).addRange(10d, 100d).addUnboundedFrom(100d));
        assertAccepted(AggregationBuilders.dateRange("dr").field("ts").addRange("2024-01-01", "2024-07-01").addUnboundedFrom("2024-07-01"));
        assertAccepted(AggregationBuilders.missing("mi").field("category"));
    }

    public void testCompositeOverFieldSources() {
        assertAccepted(
            new CompositeAggregationBuilder(
                "c",
                List.of(new TermsValuesSourceBuilder("cat").field("category"), new TermsValuesSourceBuilder("rating").field("rating"))
            ).size(100)
        );
        assertAccepted(
            new CompositeAggregationBuilder(
                "c",
                List.of(
                    new HistogramValuesSourceBuilder("p").field("price").interval(10d),
                    new DateHistogramValuesSourceBuilder("d").field("ts").calendarInterval(DateHistogramInterval.MONTH)
                )
            )
        );
        assertRejected(
            new CompositeAggregationBuilder(
                "c",
                List.of(new TermsValuesSourceBuilder("scripted").script(new Script("doc['rating'].value")))
            )
        );
        assertRejected(
            new CompositeAggregationBuilder(
                "c",
                List.of(
                    new TermsValuesSourceBuilder("cat").field("category"),
                    new TermsValuesSourceBuilder("scripted").field("rating").script(new Script("_value * 2"))
                )
            )
        );
    }

    public void testFilterBucketsOverScalarQueries() {
        assertAccepted(AggregationBuilders.filter("f", QueryBuilders.termQuery("rating", 5)));
        assertAccepted(AggregationBuilders.filter("f", QueryBuilders.matchAllQuery()));
        assertAccepted(AggregationBuilders.filter("f", QueryBuilders.existsQuery("price")));
        assertAccepted(
            AggregationBuilders.filter(
                "f",
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termsQuery("category", "a", "b"))
                    .must(QueryBuilders.rangeQuery("price").gte(10))
                    .mustNot(QueryBuilders.termQuery("rating", 1))
                    .should(QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery("ts")))
            )
        );
        assertAccepted(
            new FiltersAggregationBuilder(
                "fs",
                new KeyedFilter("cheap", QueryBuilders.rangeQuery("price").lt(10)),
                new KeyedFilter("five", QueryBuilders.termQuery("rating", 5))
            ).otherBucket(true)
        );
        assertAccepted(new FiltersAggregationBuilder("fs", QueryBuilders.termQuery("rating", 5), QueryBuilders.termQuery("rating", 4)));

        // Lance queries would run their own scan per bucket.
        assertRejected(AggregationBuilders.filter("f", new LanceMatchQueryBuilder("body", "hello")));
        assertRejected(AggregationBuilders.filter("f", new LanceKnnQueryBuilder("embedding", new float[] { 1f, 0f }, 3)));
        assertRejected(
            AggregationBuilders.filter(
                "f",
                QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("rating", 5)).must(new LanceMatchQueryBuilder("body", "hello"))
            )
        );
        // Every other query type stays on the shard path until exercised.
        assertRejected(AggregationBuilders.filter("f", QueryBuilders.matchQuery("body", "hello")));
        assertRejected(AggregationBuilders.filter("f", QueryBuilders.prefixQuery("category", "c")));
        assertRejected(
            new FiltersAggregationBuilder(
                "fs",
                new KeyedFilter("ok", QueryBuilders.termQuery("rating", 5)),
                new KeyedFilter("fts", new LanceMatchQueryBuilder("body", "hello"))
            )
        );
    }

    public void testScriptsAndUnexercisedTypesStayOnTheShardPath() {
        assertRejected(AggregationBuilders.sum("s").script(new Script("doc['price'].value")));
        assertRejected(AggregationBuilders.sum("s").field("price").script(new Script("_value * 2")));
        assertRejected(AggregationBuilders.terms("t").script(new Script("doc['category'].value")));
        assertRejected(AggregationBuilders.percentiles("p").script(new Script("doc['price'].value")));
        assertRejected(AggregationBuilders.topHits("th").size(1));
        assertRejected(AggregationBuilders.significantTerms("st").field("category"));
        assertRejected(AggregationBuilders.sampler("sa").shardSize(10));
        assertRejected(AggregationBuilders.nested("n", "items"));
        assertRejected(AggregationBuilders.global("g"));
        assertRejected(AggregationBuilders.scriptedMetric("sm").mapScript(new Script("state.x = 1")));
    }

    public void testSubAggregationsAreGatedAsAWhole() {
        assertAccepted(
            AggregationBuilders.terms("t")
                .field("category")
                .subAggregation(AggregationBuilders.percentiles("p").field("price"))
                .subAggregation(AggregationBuilders.cardinality("c").field("user_id"))
        );
        assertAccepted(
            AggregationBuilders.dateHistogram("d")
                .field("ts")
                .calendarInterval(DateHistogramInterval.DAY)
                .subAggregation(AggregationBuilders.stats("s").field("price"))
        );
        assertAccepted(
            AggregationBuilders.filter("f", QueryBuilders.termQuery("rating", 5))
                .subAggregation(AggregationBuilders.terms("t").field("category"))
        );
        assertAccepted(
            new CompositeAggregationBuilder("c", List.of(new TermsValuesSourceBuilder("cat").field("category"))).subAggregation(
                AggregationBuilders.sum("s").field("price")
            )
        );
        assertRejected(AggregationBuilders.terms("t").field("category").subAggregation(AggregationBuilders.topHits("th").size(1)));
        assertRejected(
            AggregationBuilders.dateHistogram("d")
                .field("ts")
                .calendarInterval(DateHistogramInterval.DAY)
                .subAggregation(AggregationBuilders.sum("s").script(new Script("doc['price'].value")))
        );
    }

    public void testPipelineAggregationsStayOnTheShardPath() {
        AggregationBuilder withParentPipeline = AggregationBuilders.dateHistogram("d")
            .field("ts")
            .calendarInterval(DateHistogramInterval.DAY)
            .subAggregation(AggregationBuilders.sum("s").field("price"))
            .subAggregation(new CumulativeSumPipelineAggregationBuilder("cs", "s"));
        assertRejected(withParentPipeline);

        SearchSourceBuilder withSiblingPipeline = sourceWith(
            AggregationBuilders.dateHistogram("d")
                .field("ts")
                .calendarInterval(DateHistogramInterval.DAY)
                .subAggregation(AggregationBuilders.sum("s").field("price"))
        ).aggregation(new AvgBucketPipelineAggregationBuilder("ab", "d>s"));
        assertFalse(LanceAggregationSupport.isSupported(withSiblingPipeline));
    }

    public void testOneRejectedTopLevelAggregationRejectsTheRequest() {
        SearchSourceBuilder mixed = sourceWith(AggregationBuilders.sum("s").field("price"), AggregationBuilders.topHits("th").size(1));
        assertFalse(LanceAggregationSupport.isSupported(mixed));
        SearchSourceBuilder allGood = sourceWith(
            AggregationBuilders.sum("s").field("price"),
            AggregationBuilders.percentiles("p").field("price"),
            new CompositeAggregationBuilder("c", List.of(new TermsValuesSourceBuilder("cat").field("category")))
        );
        assertTrue(LanceAggregationSupport.isSupported(allGood));
    }
}
