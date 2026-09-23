/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.calcite.rel.RelNode;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionTranslation;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.metrics.PercentilesMethod;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

/**
 * The translator alone decides which aggregation trees reach the
 * planner: every shape the coordinator's former structural allow list
 * ({@code LanceAggregationSupport.isPushdownCandidate}) accepted
 * translates to a {@link LanceAggregate}, and every shape it rejected
 * is refused by {@link SearchRequestToRel#translateExecution} with the
 * translator's own message, so removing the allow list changed no
 * routing decision. The shapes are the ones that test enumerated,
 * spelled over the shared fixture schema.
 */
public class AggregationAllowListTests extends OpenSearchTestCase {

    private static ExecutionTranslation translate(AggregatorFactories.Builder tree) {
        ExecutionShape shape = new ExecutionShape(new MatchAllQueryBuilder(), null, List.of(), null, 0, 0, tree, false);
        return SearchRequestToRel.translateExecution(shape, PlanTestFixtures.model(), PlanTestFixtures.factory());
    }

    private static AggregatorFactories.Builder tree(AggregationBuilder... builders) {
        AggregatorFactories.Builder tree = AggregatorFactories.builder();
        for (AggregationBuilder builder : builders) {
            tree.addAggregator(builder);
        }
        return tree;
    }

    private static CompositeAggregationBuilder composite(String name, CompositeValuesSourceBuilder<?>... sources) {
        return new CompositeAggregationBuilder(name, List.of(sources));
    }

    private static void assertTranslates(String label, AggregatorFactories.Builder tree) {
        ExecutionTranslation translation = translate(tree);
        assertNull(label + ": nothing refused, got [" + translation.unplanned() + "]", translation.unplanned());
        RelNode root = translation.root();
        assertTrue(label + ": the root is the aggregate, got " + root, root instanceof LanceAggregate);
    }

    private static void assertRefuses(String label, String message, AggregatorFactories.Builder tree) {
        ExecutionTranslation translation = translate(tree);
        assertNotNull(label + ": the translator refuses the shape", translation.unplanned());
        assertTrue(
            label + ": the refusal names the element, got [" + translation.unplanned() + "]",
            translation.unplanned().contains(message)
        );
        assertFalse(label + ": the root is the bare query tree, got " + translation.root(), translation.root() instanceof LanceAggregate);
    }

    public void testTheAcceptedShapesTranslate() {
        assertTranslates("metrics only", tree(AggregationBuilders.sum("s").field("rating"), AggregationBuilders.avg("a").field("rating")));
        assertTranslates(
            "terms with a metric child",
            tree(AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.max("m").field("rating")))
        );
        assertTranslates("terms by key", tree(AggregationBuilders.terms("c").field("category").order(BucketOrder.key(false))));
        assertTranslates("histogram", tree(AggregationBuilders.histogram("h").field("rating").interval(100)));
        assertTranslates(
            "fixed date_histogram",
            tree(AggregationBuilders.dateHistogram("d").field("ts").fixedInterval(DateHistogramInterval.days(30)))
        );
        for (DateHistogramInterval unit : List.of(
            DateHistogramInterval.MONTH,
            new DateHistogramInterval("1w"),
            DateHistogramInterval.QUARTER,
            DateHistogramInterval.SECOND
        )) {
            assertTranslates(
                "calendar date_histogram " + unit,
                tree(AggregationBuilders.dateHistogram("d").field("ts").calendarInterval(unit))
            );
        }
        assertTranslates(
            "bucket under bucket",
            tree(AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.terms("t").field("flag")))
        );
        assertTranslates(
            "three levels with metrics at each",
            tree(
                AggregationBuilders.terms("c")
                    .field("category")
                    .subAggregation(AggregationBuilders.avg("a").field("rating"))
                    .subAggregation(
                        AggregationBuilders.terms("f")
                            .field("flag")
                            .subAggregation(AggregationBuilders.max("m").field("id"))
                            .subAggregation(
                                AggregationBuilders.histogram("h")
                                    .field("rating")
                                    .interval(100)
                                    .subAggregation(AggregationBuilders.sum("s").field("id"))
                            )
                    )
            )
        );
        assertTranslates(
            "date_histogram under terms",
            tree(
                AggregationBuilders.terms("c")
                    .field("category")
                    .subAggregation(AggregationBuilders.dateHistogram("d").field("ts").fixedInterval(DateHistogramInterval.days(30)))
            )
        );
        assertTranslates(
            "composite over terms sources",
            tree(
                composite("cr", new TermsValuesSourceBuilder("c").field("category"), new TermsValuesSourceBuilder("r").field("rating"))
                    .subAggregation(AggregationBuilders.avg("a").field("id"))
            )
        );
        assertTranslates(
            "composite with a descending source and a date source",
            tree(
                composite(
                    "cd",
                    new TermsValuesSourceBuilder("c").field("category").order(SortOrder.DESC),
                    new DateHistogramValuesSourceBuilder("d").field("ts").fixedInterval(DateHistogramInterval.days(30))
                ).size(5).aggregateAfter(Map.of("c", "c1", "d", 1704067200000L))
            )
        );
        assertTranslates(
            "order by sub aggregation",
            tree(
                AggregationBuilders.terms("c")
                    .field("category")
                    .order(BucketOrder.aggregation("a", false))
                    .subAggregation(AggregationBuilders.avg("a").field("rating"))
            )
        );
        assertTranslates("stats", tree(AggregationBuilders.stats("s").field("rating")));
        assertTranslates("extended_stats", tree(AggregationBuilders.extendedStats("e").field("rating").sigma(3)));
        assertTranslates("cardinality", tree(AggregationBuilders.cardinality("c").field("category").precisionThreshold(100)));
        assertTranslates("percentiles", tree(AggregationBuilders.percentiles("p").field("rating")));
        assertTranslates(
            "percentiles with options",
            tree(AggregationBuilders.percentiles("p").field("rating").percentiles(10, 50).compression(200))
        );
        assertTranslates("percentile_ranks", tree(AggregationBuilders.percentileRanks("pr", new double[] { 100, 500 }).field("rating")));
        assertTranslates(
            "sketch metrics under a bucket",
            tree(
                AggregationBuilders.terms("c")
                    .field("category")
                    .subAggregation(AggregationBuilders.cardinality("u").field("rating"))
                    .subAggregation(AggregationBuilders.percentiles("p").field("id"))
            )
        );
        assertTranslates(
            "range",
            tree(AggregationBuilders.range("r").field("rating").addUnboundedTo(300).addRange(300, 700).addUnboundedFrom(700))
        );
        assertTranslates(
            "date_range",
            tree(AggregationBuilders.dateRange("d").field("ts").addUnboundedTo("2024-03-01").addUnboundedFrom("2024-03-01"))
        );
        assertTranslates("missing", tree(AggregationBuilders.missing("m").field("category")));
        assertTranslates("filter", tree(AggregationBuilders.filter("f", new RangeQueryBuilder("rating").gte(500))));
        assertTranslates("filter match_all", tree(AggregationBuilders.filter("f", new MatchAllQueryBuilder())));
        assertTranslates(
            "keyed filters with other bucket",
            tree(
                AggregationBuilders.filters(
                    "fs",
                    new FiltersAggregator.KeyedFilter("low", new RangeQueryBuilder("rating").lt(200)),
                    new FiltersAggregator.KeyedFilter("c0", new TermQueryBuilder("category", "c0"))
                ).otherBucket(true)
            )
        );
        assertTranslates(
            "anonymous filters",
            tree(AggregationBuilders.filters("fs", new TermsQueryBuilder("category", "c0", "c2"), new MatchAllQueryBuilder()))
        );
        assertTranslates(
            "metrics and a nested bucket under a range",
            tree(
                AggregationBuilders.range("r")
                    .field("rating")
                    .addUnboundedTo(500)
                    .addUnboundedFrom(500)
                    .subAggregation(AggregationBuilders.stats("s").field("id"))
                    .subAggregation(AggregationBuilders.terms("c").field("category"))
            )
        );
        assertTranslates(
            "filters under terms under missing",
            tree(
                AggregationBuilders.missing("m")
                    .field("category")
                    .subAggregation(
                        AggregationBuilders.terms("f")
                            .field("flag")
                            .subAggregation(AggregationBuilders.filters("fs", new RangeQueryBuilder("rating").gte(500)))
                    )
            )
        );
    }

    public void testTheRejectedShapesAreRefusedByTheTranslator() {
        assertRefuses(
            "two buckets",
            "two top level aggregations",
            tree(AggregationBuilders.terms("c").field("category"), AggregationBuilders.terms("f").field("flag"))
        );
        assertRefuses(
            "four levels",
            "bucket tree deeper than 3 levels",
            tree(
                AggregationBuilders.terms("a")
                    .field("category")
                    .subAggregation(
                        AggregationBuilders.terms("b")
                            .field("flag")
                            .subAggregation(
                                AggregationBuilders.terms("c")
                                    .field("rating")
                                    .subAggregation(AggregationBuilders.histogram("d").field("id").interval(10))
                            )
                    )
            )
        );
        assertRefuses(
            "two nested buckets side by side",
            "two bucket aggregations under [c]",
            tree(
                AggregationBuilders.terms("c")
                    .field("category")
                    .subAggregation(AggregationBuilders.terms("f").field("flag"))
                    .subAggregation(AggregationBuilders.terms("r").field("rating"))
            )
        );
        assertRefuses(
            "nested terms ordered by count ascending",
            "terms order",
            tree(
                AggregationBuilders.terms("c")
                    .field("category")
                    .subAggregation(AggregationBuilders.terms("r").field("rating").order(BucketOrder.count(true)))
            )
        );
        assertRefuses(
            "nested bucket with a pipeline",
            "pipeline aggregation",
            tree(
                AggregationBuilders.terms("c")
                    .field("category")
                    .subAggregation(
                        AggregationBuilders.terms("r")
                            .field("rating")
                            .subAggregation(AggregationBuilders.sum("s").field("id"))
                            .subAggregation(PipelineAggregatorBuilders.bucketScript("bs", Map.of("x", "s"), new Script("params.x")))
                    )
            )
        );
        assertRefuses(
            "composite missing_bucket",
            "missing_bucket on composite source [c]",
            tree(composite("c", new TermsValuesSourceBuilder("c").field("category").missingBucket(true)))
        );
        assertRefuses(
            "composite calendar interval",
            "interval on composite source [d] is not a fixed length",
            tree(composite("d", new DateHistogramValuesSourceBuilder("d").field("ts").calendarInterval(DateHistogramInterval.MONTH)))
        );
        assertRefuses(
            "composite date source with time zone",
            "time_zone on composite source [d]",
            tree(
                composite(
                    "d",
                    new DateHistogramValuesSourceBuilder("d").field("ts")
                        .fixedInterval(DateHistogramInterval.days(30))
                        .timeZone(ZoneId.of("+09:00"))
                )
            )
        );
        assertRefuses(
            "composite histogram source",
            "composite source type [HistogramValuesSourceBuilder]",
            tree(composite("h", new HistogramValuesSourceBuilder("h").field("rating").interval(100)))
        );
        assertRefuses(
            "composite script source",
            "script on composite source [s]",
            tree(composite("s", new TermsValuesSourceBuilder("s").script(new Script("doc['rating'].value"))))
        );
        assertRefuses(
            "composite with a bucket child",
            "aggregation type [terms] under composite [c]",
            tree(
                composite("c", new TermsValuesSourceBuilder("c").field("category")).subAggregation(
                    AggregationBuilders.terms("r").field("rating")
                )
            )
        );
        assertRefuses(
            "composite under a bucket",
            "aggregation type [composite]",
            tree(
                AggregationBuilders.terms("c")
                    .field("category")
                    .subAggregation(composite("r", new TermsValuesSourceBuilder("r").field("rating")))
            )
        );
        assertRefuses("script", "script on aggregation [s]", tree(AggregationBuilders.sum("s").script(new Script("doc['rating'].value"))));
        assertRefuses("missing", "missing on aggregation [s]", tree(AggregationBuilders.sum("s").field("rating").missing(0)));
        assertRefuses("count asc", "terms order", tree(AggregationBuilders.terms("c").field("category").order(BucketOrder.count(true))));
        assertRefuses(
            "compound order beyond the key tie breaker",
            "terms order",
            tree(
                AggregationBuilders.terms("c")
                    .field("category")
                    .order(List.of(BucketOrder.aggregation("a", false), BucketOrder.count(false)))
                    .subAggregation(AggregationBuilders.avg("a").field("rating"))
            )
        );
        assertRefuses("min_doc_count 0", "min_doc_count [0]", tree(AggregationBuilders.terms("c").field("category").minDocCount(0)));
        assertRefuses(
            "include",
            "include/exclude on aggregation [c]",
            tree(AggregationBuilders.terms("c").field("category").includeExclude(new IncludeExclude("c1", null)))
        );
        assertRefuses(
            "histogram offset",
            "offset on aggregation [h]",
            tree(AggregationBuilders.histogram("h").field("rating").interval(100).offset(5))
        );
        assertRefuses(
            "histogram extended bounds",
            "extended_bounds on aggregation [h]",
            tree(AggregationBuilders.histogram("h").field("rating").interval(100).extendedBounds(0, 2000))
        );
        assertRefuses(
            "calendar interval with time zone",
            "time_zone on aggregation [d]",
            tree(
                AggregationBuilders.dateHistogram("d")
                    .field("ts")
                    .calendarInterval(DateHistogramInterval.MONTH)
                    .timeZone(ZoneId.of("+09:00"))
            )
        );
        assertRefuses(
            "calendar interval with offset",
            "offset on aggregation [d]",
            tree(AggregationBuilders.dateHistogram("d").field("ts").calendarInterval(DateHistogramInterval.DAY).offset("1h"))
        );
        assertRefuses(
            "fixed interval with time zone",
            "time_zone on aggregation [d]",
            tree(
                AggregationBuilders.dateHistogram("d")
                    .field("ts")
                    .fixedInterval(DateHistogramInterval.days(30))
                    .timeZone(ZoneId.of("+09:00"))
            )
        );
        assertRefuses(
            "sibling pipeline aggregation",
            "pipeline aggregation",
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.sum("s").field("rating"))
                )
                .addPipelineAggregator(PipelineAggregatorBuilders.maxBucket("mb", "c>s"))
        );
        assertRefuses(
            "pipeline under the bucket",
            "pipeline aggregation",
            tree(
                AggregationBuilders.terms("c")
                    .field("category")
                    .subAggregation(AggregationBuilders.sum("s").field("rating"))
                    .subAggregation(PipelineAggregatorBuilders.bucketScript("bs", Map.of("x", "s"), new Script("params.x")))
            )
        );
        assertRefuses(
            "hdr percentiles",
            "hdr percentiles on aggregation [p]",
            tree(AggregationBuilders.percentiles("p").field("rating").method(PercentilesMethod.HDR))
        );
        assertRefuses(
            "hdr percentile ranks",
            "hdr percentile_ranks on aggregation [pr]",
            tree(AggregationBuilders.percentileRanks("pr", new double[] { 100 }).field("rating").method(PercentilesMethod.HDR))
        );
        assertRefuses("stats with missing", "missing on aggregation [s]", tree(AggregationBuilders.stats("s").field("rating").missing(0)));
        assertRefuses("range without ranges", "range aggregation [r] without ranges", tree(AggregationBuilders.range("r").field("rating")));
        assertRefuses(
            "filters over a Lance query",
            "query type [lance_match] in filter of aggregation [fs]",
            tree(AggregationBuilders.filters("fs", new TermQueryBuilder("category", "c0"), new LanceMatchQueryBuilder("body", "hello")))
        );
        assertRefuses(
            "filter over a Lance query",
            "query type [lance_match] in filter of aggregation [f]",
            tree(AggregationBuilders.filter("f", new LanceMatchQueryBuilder("body", "hello")))
        );
        assertRefuses("top_hits", "aggregation type [top_hits]", tree(AggregationBuilders.topHits("t").size(1)));
    }
}
