/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.lance.Dataset;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.LanceAttachResponse;
import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.execute.LanceAggregateResults;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.PushedOperation;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.lance.plan.execute.PlanExecutor;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.InternalComposite;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.filter.InternalFilter;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.Percentile;
import org.opensearch.search.aggregations.metrics.PercentilesMethod;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;

/**
 * The planner driven aggregation pushdown against a real table: which
 * trees the translator, the pushdown rule and the executor's resolution
 * accept end to end, how fields resolve to columns, and that the
 * {@link InternalAggregations} the executor assembles from Lance's group
 * rows equal, field for field, the ones the Lucene aggregators build for
 * the same request. The hint fixture has 3 fragments of 200 rows with
 * {@code rating = (i * 37) % 1000} (null when {@code i % 5 == 4}),
 * {@code category = "c" + (i % 3)} (null when {@code i % 4 == 3}),
 * {@code flag = i % 2 == 0} (null when {@code i % 7 == 6}), a keyword
 * list {@code tags} and an FTS {@code body}. The aggregators run in one
 * slice here: a slice level reduce would mark every {@code terms} bucket
 * of the reference with the "not computed" doc count error the reduce
 * assigns when {@code show_term_doc_count_error} is off, a field the
 * coordinator's reduce normalises on both sides but the field for field
 * comparison would see.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class PlannerRoutingTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LancePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(LancePlugin.FRAGMENT_PATH_SLICES_SETTING.getKey(), 1).build();
    }

    public void testStructuralAllowList() {
        assertTrue(candidate(AggregationBuilders.sum("s").field("rating"), AggregationBuilders.avg("a").field("rating")));
        assertTrue(
            candidate(AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.max("m").field("rating")))
        );
        assertTrue(candidate(AggregationBuilders.terms("c").field("category").order(BucketOrder.key(false))));
        assertTrue(candidate(AggregationBuilders.histogram("h").field("rating").interval(100)));
        assertTrue(candidate(AggregationBuilders.dateHistogram("d").field("ts").fixedInterval(DateHistogramInterval.days(30))));
        assertTrue(candidate(AggregationBuilders.dateHistogram("d").field("ts").calendarInterval(DateHistogramInterval.MONTH)));
        assertTrue(candidate(AggregationBuilders.dateHistogram("d").field("ts").calendarInterval(new DateHistogramInterval("1w"))));
        assertTrue(candidate(AggregationBuilders.dateHistogram("d").field("ts").calendarInterval(DateHistogramInterval.QUARTER)));
        assertTrue(candidate(AggregationBuilders.dateHistogram("d").field("ts").calendarInterval(DateHistogramInterval.SECOND)));

        assertFalse("no aggregations", LanceAggregationSupport.isPushdownCandidate(null));
        assertFalse("no aggregations", LanceAggregationSupport.isPushdownCandidate(AggregatorFactories.builder()));
        assertFalse(
            "two buckets",
            candidate(AggregationBuilders.terms("c").field("category"), AggregationBuilders.terms("f").field("flag"))
        );
        // Nested buckets: one chain of up to three levels, metrics beside
        // the nested bucket at every level.
        assertTrue(
            "bucket under bucket",
            candidate(AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.terms("t").field("tags")))
        );
        assertTrue(
            "three levels with metrics at each",
            candidate(
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
        assertTrue(
            "date_histogram under terms",
            candidate(
                AggregationBuilders.terms("c")
                    .field("category")
                    .subAggregation(AggregationBuilders.dateHistogram("d").field("ts").fixedInterval(DateHistogramInterval.days(30)))
            )
        );
        assertFalse(
            "four levels",
            candidate(
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
        assertFalse(
            "two nested buckets side by side",
            candidate(
                AggregationBuilders.terms("c")
                    .field("category")
                    .subAggregation(AggregationBuilders.terms("f").field("flag"))
                    .subAggregation(AggregationBuilders.terms("r").field("rating"))
            )
        );
        assertFalse(
            "nested bucket outside the allow list",
            candidate(
                AggregationBuilders.terms("c")
                    .field("category")
                    .subAggregation(AggregationBuilders.terms("r").field("rating").order(BucketOrder.count(true)))
            )
        );
        assertFalse(
            "nested bucket with a pipeline",
            candidate(
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
        // Composite: terms and fixed interval date_histogram sources,
        // metric children only.
        assertTrue(
            "composite over terms sources",
            candidate(
                composite("cr", new TermsValuesSourceBuilder("c").field("category"), new TermsValuesSourceBuilder("r").field("rating"))
                    .subAggregation(AggregationBuilders.avg("a").field("id"))
            )
        );
        assertTrue(
            "composite with a descending source and a date source",
            candidate(
                composite(
                    "cd",
                    new TermsValuesSourceBuilder("c").field("category").order(SortOrder.DESC),
                    new DateHistogramValuesSourceBuilder("d").field("ts").fixedInterval(DateHistogramInterval.days(30))
                ).size(5).aggregateAfter(Map.of("c", "c1", "d", 1704067200000L))
            )
        );
        assertFalse(
            "composite missing_bucket",
            candidate(composite("c", new TermsValuesSourceBuilder("c").field("category").missingBucket(true)))
        );
        assertFalse(
            "composite calendar interval",
            candidate(composite("d", new DateHistogramValuesSourceBuilder("d").field("ts").calendarInterval(DateHistogramInterval.MONTH)))
        );
        assertFalse(
            "composite date source with time zone",
            candidate(
                composite(
                    "d",
                    new DateHistogramValuesSourceBuilder("d").field("ts")
                        .fixedInterval(DateHistogramInterval.days(30))
                        .timeZone(ZoneId.of("+09:00"))
                )
            )
        );
        assertFalse(
            "composite histogram source",
            candidate(composite("h", new HistogramValuesSourceBuilder("h").field("rating").interval(100)))
        );
        assertFalse(
            "composite script source",
            candidate(composite("s", new TermsValuesSourceBuilder("s").script(new Script("doc['rating'].value"))))
        );
        assertFalse(
            "composite with a bucket child",
            candidate(
                composite("c", new TermsValuesSourceBuilder("c").field("category")).subAggregation(
                    AggregationBuilders.terms("r").field("rating")
                )
            )
        );
        assertFalse(
            "composite under a bucket",
            candidate(
                AggregationBuilders.terms("c")
                    .field("category")
                    .subAggregation(composite("r", new TermsValuesSourceBuilder("r").field("rating")))
            )
        );
        assertFalse("script", candidate(AggregationBuilders.sum("s").script(new Script("doc['rating'].value"))));
        assertFalse("missing", candidate(AggregationBuilders.sum("s").field("rating").missing(0)));
        assertFalse("count asc", candidate(AggregationBuilders.terms("c").field("category").order(BucketOrder.count(true))));
        assertTrue(
            "order by sub aggregation",
            candidate(
                AggregationBuilders.terms("c")
                    .field("category")
                    .order(BucketOrder.aggregation("a", false))
                    .subAggregation(AggregationBuilders.avg("a").field("rating"))
            )
        );
        assertFalse(
            "compound order beyond the key tie breaker",
            candidate(
                AggregationBuilders.terms("c")
                    .field("category")
                    .order(List.of(BucketOrder.aggregation("a", false), BucketOrder.count(false)))
                    .subAggregation(AggregationBuilders.avg("a").field("rating"))
            )
        );
        assertFalse("min_doc_count 0", candidate(AggregationBuilders.terms("c").field("category").minDocCount(0)));
        assertFalse("include", candidate(AggregationBuilders.terms("c").field("category").includeExclude(new IncludeExclude("c1", null))));
        assertFalse("histogram offset", candidate(AggregationBuilders.histogram("h").field("rating").interval(100).offset(5)));
        assertFalse(
            "histogram extended bounds",
            candidate(AggregationBuilders.histogram("h").field("rating").interval(100).extendedBounds(0, 2000))
        );
        assertFalse(
            "calendar interval with time zone",
            candidate(
                AggregationBuilders.dateHistogram("d")
                    .field("ts")
                    .calendarInterval(DateHistogramInterval.MONTH)
                    .timeZone(ZoneId.of("+09:00"))
            )
        );
        assertFalse(
            "calendar interval with offset",
            candidate(AggregationBuilders.dateHistogram("d").field("ts").calendarInterval(DateHistogramInterval.DAY).offset("1h"))
        );
        assertFalse(
            "time zone",
            candidate(
                AggregationBuilders.dateHistogram("d")
                    .field("ts")
                    .fixedInterval(DateHistogramInterval.days(30))
                    .timeZone(ZoneId.of("+09:00"))
            )
        );
        assertFalse(
            "pipeline aggregation",
            LanceAggregationSupport.isPushdownCandidate(
                AggregatorFactories.builder()
                    .addAggregator(
                        AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.sum("s").field("rating"))
                    )
                    .addPipelineAggregator(PipelineAggregatorBuilders.maxBucket("mb", "c>s"))
            )
        );
        assertFalse(
            "pipeline under the bucket",
            candidate(
                AggregationBuilders.terms("c")
                    .field("category")
                    .subAggregation(AggregationBuilders.sum("s").field("rating"))
                    .subAggregation(PipelineAggregatorBuilders.bucketScript("bs", Map.of("x", "s"), new Script("params.x")))
            )
        );
    }

    public void testStructuralAllowListForTheWiderShapes() {
        // Metrics the scan computes outright, and the sketch metrics.
        assertTrue(candidate(AggregationBuilders.stats("s").field("rating")));
        assertTrue(candidate(AggregationBuilders.extendedStats("e").field("rating").sigma(3)));
        assertTrue(candidate(AggregationBuilders.cardinality("c").field("category").precisionThreshold(100)));
        assertTrue(candidate(AggregationBuilders.percentiles("p").field("rating")));
        assertTrue(candidate(AggregationBuilders.percentiles("p").field("rating").percentiles(10, 50).compression(200)));
        assertTrue(candidate(AggregationBuilders.percentileRanks("pr", new double[] { 100, 500 }).field("rating")));
        assertFalse("hdr percentiles", candidate(AggregationBuilders.percentiles("p").field("rating").method(PercentilesMethod.HDR)));
        assertFalse(
            "hdr percentile ranks",
            candidate(AggregationBuilders.percentileRanks("pr", new double[] { 100 }).field("rating").method(PercentilesMethod.HDR))
        );
        assertFalse("stats with missing", candidate(AggregationBuilders.stats("s").field("rating").missing(0)));
        assertTrue(
            "sketch metrics under a bucket",
            candidate(
                AggregationBuilders.terms("c")
                    .field("category")
                    .subAggregation(AggregationBuilders.cardinality("u").field("rating"))
                    .subAggregation(AggregationBuilders.percentiles("p").field("id"))
            )
        );
        // Range, date_range, missing, filter, filters as bucket levels.
        assertTrue(candidate(AggregationBuilders.range("r").field("rating").addUnboundedTo(300).addRange(300, 700).addUnboundedFrom(700)));
        assertTrue(candidate(AggregationBuilders.dateRange("d").field("ts").addUnboundedTo("2024-03-01").addUnboundedFrom("2024-03-01")));
        assertFalse("range without ranges", candidate(AggregationBuilders.range("r").field("rating")));
        assertTrue(candidate(AggregationBuilders.missing("m").field("category")));
        assertTrue(candidate(AggregationBuilders.filter("f", new RangeQueryBuilder("rating").gte(500))));
        assertTrue(candidate(AggregationBuilders.filter("f", new MatchAllQueryBuilder())));
        assertTrue(
            candidate(
                AggregationBuilders.filters(
                    "fs",
                    new FiltersAggregator.KeyedFilter("low", new RangeQueryBuilder("rating").lt(200)),
                    new FiltersAggregator.KeyedFilter("c0", new TermQueryBuilder("category", "c0"))
                ).otherBucket(true)
            )
        );
        assertTrue(candidate(AggregationBuilders.filters("fs", new TermsQueryBuilder("category", "c0", "c2"), new MatchAllQueryBuilder())));
        assertFalse(
            "filters over a Lance query",
            candidate(
                AggregationBuilders.filters("fs", new TermQueryBuilder("category", "c0"), new LanceMatchQueryBuilder("body", "hello"))
            )
        );
        assertFalse("filter over a Lance query", candidate(AggregationBuilders.filter("f", new LanceMatchQueryBuilder("body", "hello"))));
        assertTrue(
            "metrics and a nested bucket under a range",
            candidate(
                AggregationBuilders.range("r")
                    .field("rating")
                    .addUnboundedTo(500)
                    .addUnboundedFrom(500)
                    .subAggregation(AggregationBuilders.stats("s").field("id"))
                    .subAggregation(AggregationBuilders.terms("c").field("category"))
            )
        );
        assertTrue(
            "filters under terms under missing",
            candidate(
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

    public void testWiderShapesPlanAgainstTheTableSchema() throws Exception {
        String indexName = "pushdown-plan-wider";
        String tableUri = attach(indexName);
        IndexService indexService = getInstanceFromNode(IndicesService.class).indexService(resolveIndex(indexName));
        QueryShardContext qsc = indexService.newQueryShardContext(0, null, () -> 0L, null);
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            Map<String, LinkedHashMap<String, String>> noMultiFields = Map.of();
            assertNotNull("stats on integer", planned(dataset, noMultiFields, qsc, AggregationBuilders.stats("s").field("rating")));
            assertNull("stats on keyword", planned(dataset, noMultiFields, qsc, AggregationBuilders.stats("s").field("category")));
            assertNotNull(
                "cardinality on keyword",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.cardinality("c").field("category"))
            );
            assertNotNull(
                "cardinality on boolean",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.cardinality("c").field("flag"))
            );
            assertNull(
                "cardinality on a keyword list",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.cardinality("c").field("tags"))
            );
            assertNull(
                "two cardinalities would multiply the distinct values",
                planned(
                    dataset,
                    noMultiFields,
                    qsc,
                    AggregatorFactories.builder()
                        .addAggregator(AggregationBuilders.cardinality("a").field("category"))
                        .addAggregator(AggregationBuilders.cardinality("b").field("rating"))
                )
            );
            assertNotNull(
                "percentiles on integer",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.percentiles("p").field("rating"))
            );
            assertNull(
                "percentiles on keyword",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.percentiles("p").field("category"))
            );
            assertNotNull(
                "range on integer",
                planned(
                    dataset,
                    noMultiFields,
                    qsc,
                    AggregationBuilders.range("r").field("rating").addUnboundedTo(500).addUnboundedFrom(500)
                )
            );
            assertNull(
                "range on keyword",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.range("r").field("category").addUnboundedTo(500))
            );
            assertNull(
                "range on boolean",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.range("r").field("flag").addUnboundedTo(1))
            );
            assertNull(
                "date_range on integer",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.dateRange("d").field("rating").addUnboundedTo("2024-01-01"))
            );
            assertNotNull("missing on keyword", planned(dataset, noMultiFields, qsc, AggregationBuilders.missing("m").field("category")));
            assertNull("missing on a keyword list", planned(dataset, noMultiFields, qsc, AggregationBuilders.missing("m").field("tags")));
            assertNotNull(
                "filters over scalar queries",
                planned(
                    dataset,
                    noMultiFields,
                    qsc,
                    AggregationBuilders.filters(
                        "fs",
                        new FiltersAggregator.KeyedFilter("a", new RangeQueryBuilder("rating").gte(100).lt(900)),
                        new FiltersAggregator.KeyedFilter("b", new TermsQueryBuilder("category", "c0", "c1")),
                        new FiltersAggregator.KeyedFilter("c", new ExistsQueryBuilder("flag")),
                        new FiltersAggregator.KeyedFilter(
                            "d",
                            new BoolQueryBuilder().filter(new TermQueryBuilder("flag", true))
                                .mustNot(new TermQueryBuilder("category", "c2"))
                        )
                    )
                )
            );
            assertNull(
                "filter over an unmapped field",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.filter("f", new TermQueryBuilder("nope", 1)))
            );
            assertNull(
                "filter over a keyword list column",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.filter("f", new TermQueryBuilder("tags", "t1")))
            );
            assertNull(
                "filter over a text column",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.filter("f", new TermQueryBuilder("body", "hello")))
            );
            assertNull(
                "range filter on a keyword",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.filter("f", new RangeQueryBuilder("category").gte("c1")))
            );
            assertNull(
                "fractional term on an integer",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.filter("f", new TermQueryBuilder("rating", 1.5)))
            );
            assertNull(
                "bool with minimum_should_match",
                planned(
                    dataset,
                    noMultiFields,
                    qsc,
                    AggregationBuilders.filter(
                        "f",
                        new BoolQueryBuilder().should(new TermQueryBuilder("flag", true))
                            .should(new ExistsQueryBuilder("rating"))
                            .minimumShouldMatch(2)
                    )
                )
            );
        }
    }

    public void testWiderExactShapesEqualAggregatorResults() throws Exception {
        String indexName = "pushdown-wider-equal";
        String tableUri = attach(indexName);
        List<AggregatorFactories.Builder> trees = List.of(
            AggregatorFactories.builder()
                .addAggregator(AggregationBuilders.stats("s").field("rating"))
                .addAggregator(AggregationBuilders.extendedStats("e").field("rating"))
                .addAggregator(AggregationBuilders.extendedStats("e3").field("id").sigma(3))
                .addAggregator(AggregationBuilders.stats("f").field("flag")),
            // range: unbounded ends, keyed, named and unnamed ranges,
            // overlapping ranges (a row counts in every range it is in),
            // a range no row falls in, metric and bucket children
            AggregatorFactories.builder()
                .addAggregator(AggregationBuilders.range("r").field("rating").addUnboundedTo(300).addRange(300, 700).addUnboundedFrom(700)),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.range("r")
                        .field("rating")
                        .keyed(true)
                        .addRange("low", 0, 500)
                        .addRange("mid", 250, 750)
                        .addRange(900, 950)
                        .addRange("none", 2000, 3000)
                        .subAggregation(AggregationBuilders.avg("a").field("id"))
                        .subAggregation(AggregationBuilders.terms("c").field("category"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c")
                        .field("category")
                        .subAggregation(AggregationBuilders.stats("st").field("rating"))
                        .subAggregation(AggregationBuilders.range("r").field("rating").addUnboundedTo(500).addUnboundedFrom(500))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.range("r")
                        .field("id")
                        .addRange(0, 250)
                        .addRange(250, 600)
                        .subAggregation(
                            AggregationBuilders.histogram("h")
                                .field("rating")
                                .interval(250)
                                .minDocCount(0)
                                .subAggregation(AggregationBuilders.terms("c").field("category"))
                        )
                ),
            // missing: alone, with children, nested
            AggregatorFactories.builder().addAggregator(AggregationBuilders.missing("m").field("category")),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.missing("m")
                        .field("rating")
                        .subAggregation(AggregationBuilders.count("n").field("id"))
                        .subAggregation(AggregationBuilders.terms("c").field("category"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.missing("m").field("flag"))
                ),
            // filter: every scalar query shape, including a must_not on a
            // nullable column, a should only bool and a match_all
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.filter("f", new RangeQueryBuilder("rating").gte(500))
                        .subAggregation(AggregationBuilders.terms("c").field("category"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.filter(
                        "f",
                        new BoolQueryBuilder().filter(new ExistsQueryBuilder("category")).mustNot(new TermQueryBuilder("flag", true))
                    )
                ),
            AggregatorFactories.builder()
                .addAggregator(AggregationBuilders.filter("f", new BoolQueryBuilder().mustNot(new TermQueryBuilder("category", "c1")))),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.filter(
                        "f",
                        new BoolQueryBuilder().should(new TermQueryBuilder("category", "c1"))
                            .should(new RangeQueryBuilder("rating").lt(100))
                    )
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.filter(
                        "f",
                        new BoolQueryBuilder().must(new TermQueryBuilder("flag", false)).should(new TermQueryBuilder("category", "c1"))
                    )
                ),
            AggregatorFactories.builder().addAggregator(AggregationBuilders.filter("f", new MatchAllQueryBuilder())),
            AggregatorFactories.builder()
                .addAggregator(AggregationBuilders.filter("f", new TermsQueryBuilder("rating", new int[] { 37, 74, 111, 5000 }))),
            AggregatorFactories.builder().addAggregator(AggregationBuilders.filter("f", new TermQueryBuilder("category", "c7"))),
            // filters: keyed and anonymous, other bucket with and without
            // its own key, overlapping filters, a filter matching nothing
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.filters(
                        "fs",
                        new FiltersAggregator.KeyedFilter("low", new RangeQueryBuilder("rating").lt(200)),
                        new FiltersAggregator.KeyedFilter("c0", new TermQueryBuilder("category", "c0")),
                        new FiltersAggregator.KeyedFilter("none", new TermQueryBuilder("category", "c9"))
                    ).otherBucket(true).otherBucketKey("rest").subAggregation(AggregationBuilders.max("m").field("rating"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.filters(
                        "fs",
                        new FiltersAggregator.KeyedFilter("low", new RangeQueryBuilder("rating").lt(600)),
                        new FiltersAggregator.KeyedFilter("high", new RangeQueryBuilder("rating").gte(400))
                    ).otherBucket(true)
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.filters("fs", new TermsQueryBuilder("category", "c0", "c2"), new MatchAllQueryBuilder())
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.filters(
                        "fs",
                        new FiltersAggregator.KeyedFilter("flagged", new TermQueryBuilder("flag", true)),
                        new FiltersAggregator.KeyedFilter("rated", new ExistsQueryBuilder("rating"))
                    )
                        .subAggregation(
                            AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.sum("s").field("id"))
                        )
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.histogram("h")
                        .field("rating")
                        .interval(500)
                        .minDocCount(0)
                        .subAggregation(
                            AggregationBuilders.filters("fs", new TermQueryBuilder("flag", true), new TermQueryBuilder("flag", false))
                                .otherBucket(true)
                        )
                )
        );
        List<QueryBuilder> queries = List.of(new MatchAllQueryBuilder(), new RangeQueryBuilder("rating").gte(500));
        for (AggregatorFactories.Builder tree : trees) {
            for (QueryBuilder query : queries) {
                compare(tableUri, indexName, query, tree, List.of());
                compare(tableUri, indexName, query, tree, List.of(1));
            }
        }
    }

    public void testDateRangeAndDateFiltersEqualAggregatorResults() throws Exception {
        // The interleaved fixture: ts is 2024-01-01 plus id days on a
        // timestamp[us] column, a keyword category, an integer id.
        String indexName = "pushdown-date-range";
        Path dir = createTempDir();
        String tableUri = LanceTableFactory.writeInterleavedTable(dir, indexName, 3, 40);
        attachTable(indexName, tableUri);
        List<AggregatorFactories.Builder> trees = List.of(
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.dateRange("d")
                        .field("ts")
                        .addUnboundedTo("2024-02-01")
                        .addRange("2024-02-01", "2024-04-01")
                        .addUnboundedFrom("2024-04-01")
                        .subAggregation(AggregationBuilders.terms("c").field("category"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.dateRange("d")
                        .field("ts")
                        .format("yyyy-MM-dd")
                        .keyed(true)
                        .addRange("q1", "2024-01-01", "2024-04-01")
                        .addRange("2024-03-15", "2024-05-01")
                        .subAggregation(AggregationBuilders.stats("s").field("id"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.dateRange("d").field("ts").addUnboundedTo(1706745600000L).addUnboundedFrom(1706745600000L)
                ),
            AggregatorFactories.builder()
                .addAggregator(AggregationBuilders.range("r").field("ts").addUnboundedTo(1706745600000L).addUnboundedFrom(1706745600000L)),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c")
                        .field("category")
                        .subAggregation(
                            AggregationBuilders.dateRange("d").field("ts").addUnboundedTo("2024-03-01").addUnboundedFrom("2024-03-01")
                        )
                ),
            // date bounds in filters: inclusive and exclusive, a day
            // rounded term, a formatted bound with a time zone
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.filters(
                        "fs",
                        new FiltersAggregator.KeyedFilter("jan", new RangeQueryBuilder("ts").gte("2024-01-01").lt("2024-02-01")),
                        new FiltersAggregator.KeyedFilter("feb_on", new RangeQueryBuilder("ts").gt("2024-01-31")),
                        new FiltersAggregator.KeyedFilter("to_feb", new RangeQueryBuilder("ts").lte("2024-02-01")),
                        new FiltersAggregator.KeyedFilter("day", new TermQueryBuilder("ts", "2024-01-10")),
                        new FiltersAggregator.KeyedFilter("millis", new TermQueryBuilder("ts", 1704844800000L)),
                        new FiltersAggregator.KeyedFilter(
                            "zoned",
                            new RangeQueryBuilder("ts").gte("2024/01/10").format("yyyy/MM/dd").timeZone("+09:00")
                        )
                    ).otherBucket(true)
                ),
            AggregatorFactories.builder()
                .addAggregator(AggregationBuilders.filter("f", new RangeQueryBuilder("ts").gte(1704844800000L).lte("2024-02-10T12:00:00")))
        );
        for (AggregatorFactories.Builder tree : trees) {
            for (QueryBuilder query : List.of(new MatchAllQueryBuilder(), new RangeQueryBuilder("id").gte(30))) {
                compare(tableUri, indexName, query, tree, List.of());
                compare(tableUri, indexName, query, tree, List.of(0, 2));
            }
        }
    }

    public void testSketchMetricsAgreeWithTheAggregatorsWithinTolerance() throws Exception {
        // 8 fragments of 100 rows: 640 distinct ratings, 3 categories, 2
        // flags. Cardinality is compared at a relative 1 %, percentiles
        // within one bin width plus the digest's own slack, both at
        // parallelism 1 and 4, on every fragment and on the contiguous
        // fragments 2 and 3 (a gap between the fragments' id ranges would
        // leave the median undefined and the two digests free to differ),
        // alone and under buckets.
        String indexName = "pushdown-sketches";
        String tableUri = attach(indexName, 8, 100);
        List<AggregatorFactories.Builder> trees = List.of(
            AggregatorFactories.builder()
                .addAggregator(AggregationBuilders.cardinality("r").field("rating"))
                .addAggregator(AggregationBuilders.percentiles("p").field("rating"))
                .addAggregator(AggregationBuilders.percentileRanks("pr", new double[] { 100, 500, 900 }).field("rating"))
                .addAggregator(AggregationBuilders.sum("s").field("rating")),
            AggregatorFactories.builder().addAggregator(AggregationBuilders.cardinality("c").field("category")),
            AggregatorFactories.builder().addAggregator(AggregationBuilders.cardinality("f").field("flag").precisionThreshold(10)),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.percentiles("p").field("id").percentiles(1, 25, 50, 75, 99).keyed(false).compression(50)
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c")
                        .field("category")
                        .subAggregation(AggregationBuilders.cardinality("u").field("rating"))
                        .subAggregation(AggregationBuilders.percentiles("p").field("id"))
                        .subAggregation(AggregationBuilders.avg("a").field("rating"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.range("r")
                        .field("rating")
                        .addUnboundedTo(500)
                        .addUnboundedFrom(500)
                        .subAggregation(AggregationBuilders.cardinality("u").field("category"))
                        .subAggregation(AggregationBuilders.percentileRanks("pr", new double[] { 250, 750 }).field("rating"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    composite("cr", new TermsValuesSourceBuilder("c").field("category")).subAggregation(
                        AggregationBuilders.cardinality("u").field("rating")
                    ).subAggregation(AggregationBuilders.percentiles("p").field("rating"))
                )
        );
        List<QueryBuilder> queries = List.of(new MatchAllQueryBuilder(), new RangeQueryBuilder("rating").gte(500));
        for (AggregatorFactories.Builder tree : trees) {
            for (QueryBuilder query : queries) {
                for (List<Integer> fragments : List.of(List.<Integer>of(), List.of(2, 3))) {
                    LanceFragmentQueryRequest request = request(tableUri, indexName, query, tree, fragments);
                    setPushdown(false);
                    LanceFragmentQueryResponse viaAggregators;
                    try {
                        viaAggregators = execute(request);
                    } finally {
                        setPushdown(null);
                    }
                    for (int parallelism : new int[] { 1, 4 }) {
                        setParallelism(parallelism);
                        try {
                            LanceFragmentQueryResponse pushed = execute(request);
                            String label = tree + " with " + query + " on " + fragments + " at parallelism " + parallelism;
                            assertEquals(label, viaAggregators.matched(), pushed.matched());
                            assertSketchesClose(label, viaAggregators.aggregations(), pushed.aggregations(), 1d / 4096d);
                        } finally {
                            setParallelism(null);
                        }
                    }
                }
            }
        }
    }

    /**
     * Share of the rank scale (or, against exact quantiles of gap free
     * data, of the value range) two TDigests of the same compression can
     * disagree by when they saw the same data in a different order or
     * (the pushdown) each bin's rows at the bin's edges and centre: the
     * sketch merges neighbouring rows into one centroid depending on the
     * order it met them and interpolates between centroids. Two percent
     * is well above what compression 100 shows on the fixtures, and well
     * below the sixteen bin width the bin test measures against it.
     */
    private static final double TDIGEST_SLACK = 0.02d;

    public void testPercentilesErrorStaysWithinTheBinWidth() throws Exception {
        // 8 fragments of 100 rows: ratings (i * 37) % 1000 over the 640
        // non null rows span 0..999. The pushdown's percentiles are
        // compared with the exact quantiles of that list: the error is
        // the bin width plus the digest's own interpolation, and shrinks
        // from 16 bins to 100 to the default 4096.
        String indexName = "pushdown-bins";
        String tableUri = attach(indexName, 8, 100);
        double[] percents = new double[] { 1, 5, 25, 50, 75, 95, 99 };
        AggregatorFactories.Builder tree = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.percentiles("p").field("rating").percentiles(percents))
            .addAggregator(AggregationBuilders.percentileRanks("pr", new double[] { 0, 333, 666, 999 }).field("rating"));
        LanceFragmentQueryRequest request = request(tableUri, indexName, new MatchAllQueryBuilder(), tree, List.of());
        List<Long> ratings = new ArrayList<>();
        for (int i = 0; i < 800; i++) {
            if (i % 5 != 4) {
                ratings.add((i * 37L) % 1000L);
            }
        }
        Collections.sort(ratings);
        double range = ratings.get(ratings.size() - 1) - ratings.get(0);
        Map<Integer, Double> worstByBins = new LinkedHashMap<>();
        for (Integer bins : new Integer[] { 16, 100, null }) {
            setBins(bins);
            try {
                LanceFragmentQueryResponse pushed = execute(request);
                InternalTDigestPercentiles percentiles = pushed.aggregations().get("p");
                double binWidth = range / (bins == null ? 4096 : bins);
                double worst = 0d;
                for (double percent : percents) {
                    double exact = ratings.get((int) Math.min(ratings.size() - 1, Math.floor(percent / 100d * ratings.size())));
                    double error = Math.abs(percentiles.percentile(percent) - exact);
                    worst = Math.max(worst, error);
                    assertTrue(
                        "bins " + bins + " percentile " + percent + " exact " + exact + " got " + percentiles.percentile(percent),
                        error <= binWidth + TDIGEST_SLACK * range
                    );
                }
                worstByBins.put(bins == null ? 4096 : bins, worst);
                InternalTDigestPercentileRanks ranks = pushed.aggregations().get("pr");
                for (double value : ranks.getKeys()) {
                    long below = 0;
                    for (long rating : ratings) {
                        if (rating <= value) {
                            below++;
                        }
                    }
                    double exact = 100d * below / ratings.size();
                    assertTrue(
                        "bins " + bins + " rank of " + value + " exact " + exact + " got " + ranks.percent(value),
                        Math.abs(ranks.percent(value) - exact) <= 100d * (binWidth / range + TDIGEST_SLACK)
                    );
                }
            } finally {
                setBins(null);
            }
        }
        // Sixteen bins are far coarser than the digest's own error, so the
        // error has to fall when the bins get finer.
        assertTrue(worstByBins.toString(), worstByBins.get(16) > worstByBins.get(4096));

        // A field of one value: min == max makes one bin whose centre is
        // the value, so every percentile is exact.
        AggregatorFactories.Builder constant = AggregatorFactories.builder()
            .addAggregator(
                AggregationBuilders.filter("one", new TermQueryBuilder("rating", 37))
                    .subAggregation(AggregationBuilders.percentiles("p").field("rating"))
            );
        LanceFragmentQueryResponse one = execute(request(tableUri, indexName, new MatchAllQueryBuilder(), constant, List.of()));
        InternalFilter bucket = one.aggregations().get("one");
        assertEquals(1L, bucket.getDocCount());
        InternalTDigestPercentiles percentiles = bucket.getAggregations().get("p");
        for (Percentile percentile : percentiles) {
            assertEquals(37d, percentile.getValue(), 0d);
        }
        // No value at all: the empty digest the aggregator reports.
        AggregatorFactories.Builder none = AggregatorFactories.builder()
            .addAggregator(
                AggregationBuilders.filter("none", new TermQueryBuilder("category", "c9"))
                    .subAggregation(AggregationBuilders.percentiles("p").field("rating"))
            );
        compare(tableUri, indexName, new MatchAllQueryBuilder(), none, List.of());
    }

    /**
     * Compares two aggregation trees: exact aggregations must be equal,
     * a cardinality within a relative 1 %, a tdigest percentile within
     * {@code share} of the range of the aggregators' own percentiles
     * (the bin width the pushdown sketched with) plus
     * {@link #TDIGEST_SLACK} of the range for the TDigest's own
     * interpolation error, which the two sketches do not share point for
     * point; a percentile rank within the same share of the rank scale.
     */
    private static void assertSketchesClose(String label, InternalAggregations expected, InternalAggregations actual, double share) {
        List<InternalAggregation> expectedList = expected.copyResults();
        List<InternalAggregation> actualList = actual.copyResults();
        assertEquals(label, expectedList.size(), actualList.size());
        for (int i = 0; i < expectedList.size(); i++) {
            InternalAggregation e = expectedList.get(i);
            InternalAggregation a = actualList.get(i);
            assertEquals(label, e.getName(), a.getName());
            if (e instanceof InternalCardinality ec) {
                InternalCardinality ac = (InternalCardinality) a;
                double diff = Math.abs(ec.getValue() - ac.getValue()) / Math.max(1d, ec.getValue());
                assertTrue(label + ": cardinality " + e.getName() + " expected " + ec.getValue() + " got " + ac.getValue(), diff <= 0.01d);
            } else if (e instanceof InternalTDigestPercentiles ep) {
                InternalTDigestPercentiles ap = (InternalTDigestPercentiles) a;
                // Compared in rank space: the reference digest's rank of
                // the pushdown's value has to be within the tolerance of
                // the requested percent. A value comparison would charge
                // the pushdown for the data's gaps (one rank across an
                // empty stretch of the range is a large value move) and
                // for TDigest's own order dependence, which both sketches
                // have. The bin width is the only error the pushdown adds,
                // so the value is widened by it before it is ranked.
                double range = ep.getState().size() == 0 ? 0d : ep.getState().getMax() - ep.getState().getMin();
                double pointShare = ep.getState().size() == 0 ? 0d : 100d / ep.getState().size();
                for (double key : ep.getKeys()) {
                    double expectedValue = ep.percentile(key);
                    double actualValue = ap.percentile(key);
                    if (Double.isNaN(expectedValue)) {
                        assertTrue(label + ": percentile " + key + " expected NaN got " + actualValue, Double.isNaN(actualValue));
                        continue;
                    }
                    double binWidth = share * range;
                    double rankBelow = 100d * ep.getState().cdf(actualValue - binWidth);
                    double rankAbove = 100d * ep.getState().cdf(actualValue + binWidth);
                    double tolerance = 100d * TDIGEST_SLACK + pointShare + 1e-9d;
                    assertTrue(
                        label
                            + ": percentile "
                            + key
                            + " expected "
                            + expectedValue
                            + " got "
                            + actualValue
                            + " ranked "
                            + rankBelow
                            + ".."
                            + rankAbove,
                        key >= rankBelow - tolerance && key <= rankAbove + tolerance
                    );
                }
            } else if (e instanceof InternalTDigestPercentileRanks ep) {
                InternalTDigestPercentileRanks ap = (InternalTDigestPercentileRanks) a;
                double pointShare = ep.getState().size() == 0 ? 0d : 100d / ep.getState().size();
                for (double key : ep.getKeys()) {
                    double expectedValue = ep.percent(key);
                    double actualValue = ap.percent(key);
                    if (Double.isNaN(expectedValue)) {
                        assertTrue(label + ": rank of " + key + " expected NaN got " + actualValue, Double.isNaN(actualValue));
                        continue;
                    }
                    assertTrue(
                        label + ": percentile rank of " + key + " expected " + expectedValue + " got " + actualValue,
                        Math.abs(expectedValue - actualValue) <= 100d * (share + TDIGEST_SLACK) + pointShare + 1e-9d
                    );
                }
            } else if (e instanceof InternalMultiBucketAggregation<?, ?> eb) {
                InternalMultiBucketAggregation<?, ?> ab = (InternalMultiBucketAggregation<?, ?>) a;
                assertEquals(label, eb.getBuckets().size(), ab.getBuckets().size());
                for (int b = 0; b < eb.getBuckets().size(); b++) {
                    assertEquals(label, eb.getBuckets().get(b).getKey(), ab.getBuckets().get(b).getKey());
                    assertEquals(label, eb.getBuckets().get(b).getDocCount(), ab.getBuckets().get(b).getDocCount());
                    assertSketchesClose(
                        label + " > " + eb.getBuckets().get(b).getKey(),
                        (InternalAggregations) eb.getBuckets().get(b).getAggregations(),
                        (InternalAggregations) ab.getBuckets().get(b).getAggregations(),
                        share
                    );
                }
            } else if (e instanceof InternalSingleBucketAggregation eb) {
                InternalSingleBucketAggregation ab = (InternalSingleBucketAggregation) a;
                assertEquals(label, eb.getDocCount(), ab.getDocCount());
                assertSketchesClose(label + " > " + e.getName(), eb.getAggregations(), ab.getAggregations(), share);
            } else {
                assertEquals(label, e, a);
            }
        }
    }

    private void setBins(Integer value) {
        Settings.Builder settings = Settings.builder();
        if (value == null) {
            settings.putNull(LancePlugin.AGGREGATION_PERCENTILES_BINS_SETTING.getKey());
        } else {
            settings.put(LancePlugin.AGGREGATION_PERCENTILES_BINS_SETTING.getKey(), value);
        }
        client().admin().cluster().updateSettings(new ClusterUpdateSettingsRequest().transientSettings(settings)).actionGet();
    }

    public void testPlanResolvesFieldsAgainstTheTableSchema() throws Exception {
        String indexName = "pushdown-plan";
        String tableUri = attach(indexName);
        IndexService indexService = getInstanceFromNode(IndicesService.class).indexService(resolveIndex(indexName));
        QueryShardContext qsc = indexService.newQueryShardContext(0, null, () -> 0L, null);
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            Map<String, LinkedHashMap<String, String>> noMultiFields = Map.of();
            assertNotNull("keyword terms", planned(dataset, noMultiFields, qsc, AggregationBuilders.terms("c").field("category")));
            assertNotNull("integer terms", planned(dataset, noMultiFields, qsc, AggregationBuilders.terms("r").field("rating")));
            assertNotNull("boolean terms", planned(dataset, noMultiFields, qsc, AggregationBuilders.terms("f").field("flag")));
            assertNotNull("value_count on keyword", planned(dataset, noMultiFields, qsc, AggregationBuilders.count("n").field("category")));
            assertNotNull("sum on boolean", planned(dataset, noMultiFields, qsc, AggregationBuilders.sum("s").field("flag")));
            assertNotNull(
                "integer histogram",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.histogram("h").field("rating").interval(50))
            );

            assertNull("keyword list column", planned(dataset, noMultiFields, qsc, AggregationBuilders.terms("t").field("tags")));
            assertNull("text column", planned(dataset, noMultiFields, qsc, AggregationBuilders.terms("b").field("body")));
            assertNull("unmapped field", planned(dataset, noMultiFields, qsc, AggregationBuilders.sum("u").field("nope")));
            assertNull("sum on keyword", planned(dataset, noMultiFields, qsc, AggregationBuilders.sum("s").field("category")));
            assertNull(
                "histogram on keyword",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.histogram("h").field("category").interval(1))
            );
            assertNull(
                "histogram on boolean",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.histogram("h").field("flag").interval(1))
            );
            assertNull(
                "date_histogram on integer",
                planned(
                    dataset,
                    noMultiFields,
                    qsc,
                    AggregationBuilders.dateHistogram("d").field("rating").fixedInterval(DateHistogramInterval.days(1))
                )
            );
            assertNull(
                "one unsupported child refuses the tree",
                planned(
                    dataset,
                    noMultiFields,
                    qsc,
                    AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.sum("s").field("tags"))
                )
            );
            assertNull(
                "sub-field the mapping does not know",
                planned(dataset, noMultiFields, qsc, AggregationBuilders.terms("b").field("body.raw"))
            );
            assertNull(
                "nested terms on a keyword list column",
                planned(
                    dataset,
                    noMultiFields,
                    qsc,
                    AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.terms("t").field("tags"))
                )
            );
            assertNotNull(
                "nested terms on scalar columns",
                planned(
                    dataset,
                    noMultiFields,
                    qsc,
                    AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.terms("r").field("rating"))
                )
            );
            // The estimate multiplies the shard_size of every terms
            // level: 1000 * 1.5 + 10 = 1510 per level, 2,280,100 for two,
            // above the default bound of one million; the bound is the
            // node setting, so the same tree plans under a larger one.
            AggregationBuilder wide = AggregationBuilders.terms("c")
                .field("category")
                .size(1000)
                .subAggregation(AggregationBuilders.terms("r").field("rating").size(1000));
            assertEquals(1_000_000, (int) LancePlugin.AGGREGATION_PUSHDOWN_MAX_GROUPS_SETTING.get(Settings.EMPTY));
            assertNull("group estimate above pushdown_max_groups", planned(dataset, noMultiFields, qsc, wide));
            assertNotNull(
                "group estimate under an explicit bound",
                planned(dataset, noMultiFields, qsc, AggregatorFactories.builder().addAggregator(wide), 3_000_000)
            );
            assertNull(
                "single terms level above the bound",
                planned(
                    dataset,
                    noMultiFields,
                    qsc,
                    AggregatorFactories.builder().addAggregator(AggregationBuilders.terms("r").field("rating").size(100)),
                    100
                )
            );
            assertNotNull(
                "composite over keyword and integer",
                planned(
                    dataset,
                    noMultiFields,
                    qsc,
                    composite("cr", new TermsValuesSourceBuilder("c").field("category"), new TermsValuesSourceBuilder("r").field("rating"))
                        .aggregateAfter(Map.of("c", "c1", "r", 500))
                )
            );
            assertNull(
                "composite after value of the wrong type for a keyword source",
                planned(
                    dataset,
                    noMultiFields,
                    qsc,
                    composite("cr", new TermsValuesSourceBuilder("c").field("category"), new TermsValuesSourceBuilder("r").field("rating"))
                        .aggregateAfter(Map.of("c", 7, "r", 500))
                )
            );
            assertNull(
                "composite after value that does not parse as a number",
                planned(
                    dataset,
                    noMultiFields,
                    qsc,
                    composite("cr", new TermsValuesSourceBuilder("c").field("category"), new TermsValuesSourceBuilder("r").field("rating"))
                        .aggregateAfter(Map.of("c", "c1", "r", "high"))
                )
            );
            assertNull(
                "composite date source on an integer",
                planned(
                    dataset,
                    noMultiFields,
                    qsc,
                    composite("d", new DateHistogramValuesSourceBuilder("d").field("rating").fixedInterval(DateHistogramInterval.days(1)))
                )
            );
            assertNull(
                "composite source on a keyword list column",
                planned(dataset, noMultiFields, qsc, composite("t", new TermsValuesSourceBuilder("t").field("tags")))
            );
        }
    }

    public void testPushdownResultsEqualAggregatorResults() throws Exception {
        String indexName = "pushdown-equal";
        String tableUri = attach(indexName);
        List<AggregatorFactories.Builder> trees = List.of(
            AggregatorFactories.builder()
                .addAggregator(AggregationBuilders.sum("s").field("rating"))
                .addAggregator(AggregationBuilders.avg("a").field("rating"))
                .addAggregator(AggregationBuilders.min("m").field("rating"))
                .addAggregator(AggregationBuilders.max("M").field("rating"))
                .addAggregator(AggregationBuilders.count("c").field("rating"))
                .addAggregator(AggregationBuilders.sum("f").field("flag")),
            AggregatorFactories.builder().addAggregator(AggregationBuilders.terms("c").field("category")),
            AggregatorFactories.builder().addAggregator(AggregationBuilders.terms("c").field("category").size(2)),
            AggregatorFactories.builder().addAggregator(AggregationBuilders.terms("r").field("rating").size(3).shardSize(7)),
            AggregatorFactories.builder()
                .addAggregator(AggregationBuilders.terms("r").field("rating").size(4).order(BucketOrder.key(false))),
            AggregatorFactories.builder().addAggregator(AggregationBuilders.terms("r").field("rating").size(5).showTermDocCountError(true)),
            AggregatorFactories.builder().addAggregator(AggregationBuilders.terms("f").field("flag")),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c")
                        .field("category")
                        .subAggregation(AggregationBuilders.avg("a").field("rating"))
                        .subAggregation(AggregationBuilders.count("n").field("flag"))
                ),
            AggregatorFactories.builder().addAggregator(AggregationBuilders.histogram("h").field("rating").interval(100)),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.histogram("h")
                        .field("rating")
                        .interval(250)
                        .minDocCount(1)
                        .keyed(true)
                        .subAggregation(AggregationBuilders.sum("s").field("id"))
                ),
            // nested buckets: terms under terms with metrics at both
            // levels, three levels, a histogram under terms, terms
            // under a histogram whose empty bucket info carries the
            // nested empty terms, key orders and small sizes so the
            // inner truncation and other counts matter
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c")
                        .field("category")
                        .subAggregation(
                            AggregationBuilders.terms("r").field("rating").size(3).subAggregation(AggregationBuilders.avg("a").field("id"))
                        )
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c")
                        .field("category")
                        .subAggregation(AggregationBuilders.avg("a").field("rating"))
                        .subAggregation(
                            AggregationBuilders.terms("f")
                                .field("flag")
                                .subAggregation(AggregationBuilders.max("m").field("id"))
                                .subAggregation(AggregationBuilders.count("n").field("rating"))
                        )
                        .subAggregation(AggregationBuilders.sum("s").field("id"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c")
                        .field("category")
                        .size(2)
                        .subAggregation(AggregationBuilders.terms("r").field("rating").size(5).order(BucketOrder.key(false)))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c")
                        .field("category")
                        .order(BucketOrder.key(false))
                        .subAggregation(
                            AggregationBuilders.terms("f")
                                .field("flag")
                                .subAggregation(
                                    AggregationBuilders.terms("r")
                                        .field("rating")
                                        .size(2)
                                        .showTermDocCountError(true)
                                        .subAggregation(AggregationBuilders.min("m").field("id"))
                                )
                        )
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c")
                        .field("category")
                        .subAggregation(
                            AggregationBuilders.histogram("h")
                                .field("rating")
                                .interval(250)
                                .subAggregation(AggregationBuilders.sum("s").field("id"))
                        )
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.histogram("h")
                        .field("rating")
                        .interval(250)
                        .minDocCount(0)
                        .subAggregation(
                            AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.avg("a").field("id"))
                        )
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.histogram("h")
                        .field("rating")
                        .interval(200)
                        .keyed(true)
                        .subAggregation(
                            AggregationBuilders.terms("f").field("flag").subAggregation(AggregationBuilders.terms("c").field("category"))
                        )
                ),
            // composite: two terms sources, size and after paging, a
            // descending source, a boolean source, metric children
            AggregatorFactories.builder()
                .addAggregator(
                    composite("cr", new TermsValuesSourceBuilder("c").field("category"), new TermsValuesSourceBuilder("r").field("rating"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    composite("cr", new TermsValuesSourceBuilder("c").field("category"), new TermsValuesSourceBuilder("r").field("rating"))
                        .size(3)
                        .subAggregation(AggregationBuilders.avg("a").field("id"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    composite("cr", new TermsValuesSourceBuilder("c").field("category"), new TermsValuesSourceBuilder("r").field("rating"))
                        .size(4)
                        .aggregateAfter(Map.of("c", "c1", "r", 500))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    composite(
                        "rc",
                        new TermsValuesSourceBuilder("r").field("rating").order(SortOrder.DESC),
                        new TermsValuesSourceBuilder("c").field("category")
                    ).size(5).aggregateAfter(Map.of("r", 900, "c", "c0"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    composite("fc", new TermsValuesSourceBuilder("f").field("flag"), new TermsValuesSourceBuilder("c").field("category"))
                        .size(10)
                        .subAggregation(AggregationBuilders.sum("s").field("rating"))
                        .subAggregation(AggregationBuilders.count("n").field("id"))
                ),
            AggregatorFactories.builder()
                .addAggregator(composite("c", new TermsValuesSourceBuilder("c").field("category").order(SortOrder.DESC)).size(2))
        );
        List<QueryBuilder> queries = List.of(
            new MatchAllQueryBuilder(),
            new RangeQueryBuilder("rating").gte(500),
            new TermQueryBuilder("flag", false)
        );
        for (AggregatorFactories.Builder tree : trees) {
            for (QueryBuilder query : queries) {
                // Every fragment, then the subset the coordinator would
                // hand one of three nodes.
                compare(tableUri, indexName, query, tree, List.of());
                compare(tableUri, indexName, query, tree, List.of(1));
            }
        }
    }

    public void testTermsResultCarriesTheShardSideFields() throws Exception {
        String indexName = "pushdown-terms-fields";
        String tableUri = attach(indexName);
        AggregatorFactories.Builder tree = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.terms("r").field("rating").size(3));
        LanceFragmentQueryResponse response = execute(request(tableUri, indexName, new MatchAllQueryBuilder(), tree, List.of()));
        LongTerms terms = response.aggregations().get("r");
        // 480 distinct ratings of one row each; shard_size defaults to
        // 3 * 1.5 + 10 = 14 buckets, sorted by key for the reduce, the
        // rest summed into sum_other_doc_count, the error left for the
        // reduce to derive.
        assertEquals(14, terms.getBuckets().size());
        assertEquals(480L - 14L, terms.getSumOfOtherDocCounts());
        assertEquals(0L, terms.getDocCountError());
        long previous = Long.MIN_VALUE;
        for (LongTerms.Bucket bucket : terms.getBuckets()) {
            long key = ((Number) bucket.getKey()).longValue();
            assertTrue("buckets sorted by key: " + terms.getBuckets(), key > previous);
            previous = key;
            assertEquals(1L, bucket.getDocCount());
        }
        assertEquals(600L, response.matched());

        AggregatorFactories.Builder keyed = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.terms("c").field("category").order(BucketOrder.key(false)).size(2));
        StringTerms byKey = execute(request(tableUri, indexName, new MatchAllQueryBuilder(), keyed, List.of())).aggregations().get("c");
        assertEquals(List.of("c2", "c1"), byKey.getBuckets().stream().map(StringTerms.Bucket::getKeyAsString).toList());
        assertEquals(150L, byKey.getSumOfOtherDocCounts());

        AggregatorFactories.Builder withAvg = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.avg("a").field("rating")));
        LanceFragmentQueryResponse withSubResponse = execute(
            request(tableUri, indexName, new RangeQueryBuilder("rating").gte(500), withAvg, List.of(1))
        );
        StringTerms withSub = withSubResponse.aggregations().get("c");
        // Every row the filter keeps counts toward the match total; only
        // the rows with a category open a bucket.
        long expectedRows = 0;
        long expectedBucketRows = 0;
        Map<String, long[]> expected = new LinkedHashMap<>();
        for (int i = 200; i < 400; i++) {
            if (i % 5 == 4 || (i * 37L) % 1000L < 500L) {
                continue;
            }
            expectedRows++;
            if (i % 4 == 3) {
                continue;
            }
            expectedBucketRows++;
            long[] sumAndCount = expected.computeIfAbsent("c" + (i % 3), k -> new long[2]);
            sumAndCount[0] += (i * 37L) % 1000L;
            sumAndCount[1]++;
        }
        assertEquals(expectedRows, withSubResponse.matched());
        long bucketRows = 0;
        for (StringTerms.Bucket bucket : withSub.getBuckets()) {
            long[] sumAndCount = expected.get(bucket.getKeyAsString());
            assertEquals(sumAndCount[1], bucket.getDocCount());
            bucketRows += bucket.getDocCount();
            InternalAvg avg = bucket.getAggregations().get("a");
            assertEquals((double) sumAndCount[0] / sumAndCount[1], avg.getValue(), 1e-9d);
        }
        assertEquals(expectedBucketRows, bucketRows);
        assertEquals(expected.size(), withSub.getBuckets().size());

        InternalHistogram histogram = execute(
            request(
                tableUri,
                indexName,
                new MatchAllQueryBuilder(),
                AggregatorFactories.builder().addAggregator(AggregationBuilders.histogram("h").field("rating").interval(100)),
                List.of()
            )
        ).aggregations().get("h");
        assertEquals(10, histogram.getBuckets().size());
        assertEquals(0d, ((Number) histogram.getBuckets().get(0).getKey()).doubleValue(), 0d);
        assertEquals(900d, ((Number) histogram.getBuckets().get(9).getKey()).doubleValue(), 0d);
        long histogramRows = 0;
        for (InternalHistogram.Bucket bucket : histogram.getBuckets()) {
            histogramRows += bucket.getDocCount();
        }
        assertEquals(480L, histogramRows);
    }

    public void testMetricOrderedTermsPlanForTheSingleLevelShapeOnly() throws Exception {
        String indexName = "pushdown-metric-order-plan";
        String tableUri = attach(indexName);
        IndexService indexService = getInstanceFromNode(IndicesService.class).indexService(resolveIndex(indexName));
        QueryShardContext qsc = indexService.newQueryShardContext(0, null, () -> 0L, null);
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            Map<String, LinkedHashMap<String, String>> none = Map.of();
            assertNotNull(
                "order by a sum child",
                planned(
                    dataset,
                    none,
                    qsc,
                    AggregationBuilders.terms("c")
                        .field("category")
                        .order(BucketOrder.aggregation("s", false))
                        .subAggregation(AggregationBuilders.sum("s").field("rating"))
                )
            );
            assertNotNull(
                "order by an avg child through its value path",
                planned(
                    dataset,
                    none,
                    qsc,
                    AggregationBuilders.terms("c")
                        .field("category")
                        .order(BucketOrder.aggregation("a.value", true))
                        .subAggregation(AggregationBuilders.avg("a").field("rating"))
                )
            );
            assertNull(
                "order by a child the level does not have",
                planned(
                    dataset,
                    none,
                    qsc,
                    AggregationBuilders.terms("c")
                        .field("category")
                        .order(BucketOrder.aggregation("x", false))
                        .subAggregation(AggregationBuilders.sum("s").field("rating"))
                )
            );
            assertNull(
                "order by a cardinality child",
                planned(
                    dataset,
                    none,
                    qsc,
                    AggregationBuilders.terms("c")
                        .field("category")
                        .order(BucketOrder.aggregation("k", false))
                        .subAggregation(AggregationBuilders.cardinality("k").field("rating"))
                )
            );
            assertNull(
                "order by a stats child",
                planned(
                    dataset,
                    none,
                    qsc,
                    AggregationBuilders.terms("c")
                        .field("category")
                        .order(BucketOrder.aggregation("st.avg", false))
                        .subAggregation(AggregationBuilders.stats("st").field("rating"))
                )
            );
            assertNull(
                "metric order on a nested terms level",
                planned(
                    dataset,
                    none,
                    qsc,
                    AggregationBuilders.terms("c")
                        .field("category")
                        .subAggregation(
                            AggregationBuilders.terms("r")
                                .field("rating")
                                .order(BucketOrder.aggregation("m", false))
                                .subAggregation(AggregationBuilders.max("m").field("id"))
                        )
                )
            );
            assertNull(
                "metric order on an outer terms with a nested level",
                planned(
                    dataset,
                    none,
                    qsc,
                    AggregationBuilders.terms("c")
                        .field("category")
                        .order(BucketOrder.aggregation("s", false))
                        .subAggregation(AggregationBuilders.sum("s").field("rating"))
                        .subAggregation(AggregationBuilders.terms("r").field("rating"))
                )
            );
        }
    }

    public void testMetricOrderedTermsEqualAggregatorResults() throws Exception {
        String indexName = "pushdown-metric-order";
        String tableUri = attach(indexName);
        // Sums over distinct ids and ratings keep the order values tie
        // free, so the selection is deterministic on both paths.
        List<AggregatorFactories.Builder> trees = List.of(
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c")
                        .field("category")
                        .order(BucketOrder.aggregation("s", false))
                        .subAggregation(AggregationBuilders.sum("s").field("rating"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c")
                        .field("category")
                        .order(BucketOrder.aggregation("a", true))
                        .subAggregation(AggregationBuilders.avg("a").field("rating"))
                ),
            // 480 one row rating groups: shard_size 16 cuts the merged
            // selection by the metric, not by the count
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("r")
                        .field("rating")
                        .size(4)
                        .order(BucketOrder.aggregation("m", false))
                        .subAggregation(AggregationBuilders.max("m").field("id"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("r")
                        .field("rating")
                        .size(4)
                        .order(BucketOrder.aggregation("s.value", true))
                        .subAggregation(AggregationBuilders.sum("s").field("id"))
                        .subAggregation(AggregationBuilders.avg("a").field("id"))
                )
        );
        for (AggregatorFactories.Builder tree : trees) {
            compare(tableUri, indexName, new MatchAllQueryBuilder(), tree, List.of());
        }
        compare(tableUri, indexName, new RangeQueryBuilder("rating").gte(300), trees.get(0), List.of());
        for (int parallelism : new int[] { 1, 8 }) {
            setParallelism(parallelism);
            try {
                compare(tableUri, indexName, new MatchAllQueryBuilder(), trees.get(2), List.of());
            } finally {
                setParallelism(null);
            }
        }
    }

    public void testTopKSlackKeepsTheShardResultRules() throws Exception {
        // 8 fragments of 200 rows: rating r appears at i and i + 1000
        // (different fragments), so about 600 ratings have their count
        // split over two of the 8 per fragment scans. With slack 64
        // every scan retains all of its groups and the result is bit
        // identical to the aggregators; with slack 1 each scan keeps
        // only shard_size groups, and the shard result still obeys the
        // aggregator's rules: shard_size buckets, key sorted, every doc
        // count at most the true count, the counts of everything else
        // in sum_other_doc_count, the error field left for the reduce.
        String indexName = "pushdown-topk-slack";
        String tableUri = attach(indexName, 8, 200);
        AggregatorFactories.Builder tree = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.terms("r").field("rating").size(3));
        setParallelism(8);
        try {
            setSlack(64);
            try {
                compare(tableUri, indexName, new MatchAllQueryBuilder(), tree, List.of());
            } finally {
                setSlack(null);
            }

            Map<Long, Long> expected = new TreeMap<>();
            long keyedRows = 0;
            for (int i = 0; i < 1600; i++) {
                if (i % 5 == 4) {
                    continue;
                }
                keyedRows++;
                expected.merge((i * 37L) % 1000L, 1L, Long::sum);
            }
            setSlack(1);
            try {
                LongTerms terms = execute(request(tableUri, indexName, new MatchAllQueryBuilder(), tree, List.of())).aggregations()
                    .get("r");
                assertEquals(14, terms.getBuckets().size());
                assertEquals(0L, terms.getDocCountError());
                long selected = 0;
                long previous = Long.MIN_VALUE;
                for (LongTerms.Bucket bucket : terms.getBuckets()) {
                    long key = ((Number) bucket.getKey()).longValue();
                    assertTrue("buckets sorted by key: " + terms.getBuckets(), key > previous);
                    previous = key;
                    long docCount = bucket.getDocCount();
                    assertTrue("a bucket cannot exceed its true count", docCount <= expected.get(key));
                    assertTrue("a bucket needs at least one row", docCount >= 1L);
                    selected += docCount;
                }
                assertEquals(
                    "every keyed row is a bucket row or in sum_other_doc_count",
                    keyedRows,
                    selected + terms.getSumOfOtherDocCounts()
                );
            } finally {
                setSlack(null);
            }
        } finally {
            setParallelism(null);
        }
    }

    public void testParallelGroupScansAgreeWithOneScan() throws Exception {
        // 8 fragments of 100 rows: 640 one row rating groups, 3
        // category groups, so shard_size cuts the rating terms and
        // sum_other_doc_count depends on every group being merged before
        // the cut. The same requests are answered with 1, 2 and 8 scans
        // and by the aggregators; the results have to be identical.
        String indexName = "pushdown-parallel";
        String tableUri = attach(indexName, 8, 100);
        List<AggregatorFactories.Builder> trees = List.of(
            AggregatorFactories.builder()
                .addAggregator(AggregationBuilders.sum("s").field("rating"))
                .addAggregator(AggregationBuilders.avg("a").field("rating"))
                .addAggregator(AggregationBuilders.min("m").field("rating"))
                .addAggregator(AggregationBuilders.max("M").field("rating"))
                .addAggregator(AggregationBuilders.count("c").field("category")),
            AggregatorFactories.builder().addAggregator(AggregationBuilders.terms("r").field("rating").size(3)),
            AggregatorFactories.builder().addAggregator(AggregationBuilders.terms("r").field("rating").size(5).showTermDocCountError(true)),
            AggregatorFactories.builder()
                .addAggregator(AggregationBuilders.terms("r").field("rating").size(4).order(BucketOrder.key(false))),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c")
                        .field("category")
                        .size(2)
                        .subAggregation(AggregationBuilders.avg("a").field("rating"))
                        .subAggregation(AggregationBuilders.min("m").field("rating"))
                        .subAggregation(AggregationBuilders.max("M").field("rating"))
                        .subAggregation(AggregationBuilders.count("n").field("flag"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.histogram("h")
                        .field("rating")
                        .interval(100)
                        .subAggregation(AggregationBuilders.sum("s").field("id"))
                ),
            // multi key groups: the partials merge on the full key
            // list, and the inner cut happens once per parent bucket
            // over the merged groups
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c")
                        .field("category")
                        .subAggregation(AggregationBuilders.avg("a").field("rating"))
                        .subAggregation(
                            AggregationBuilders.terms("r").field("rating").size(3).subAggregation(AggregationBuilders.max("m").field("id"))
                        )
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    composite("cr", new TermsValuesSourceBuilder("c").field("category"), new TermsValuesSourceBuilder("r").field("rating"))
                        .size(7)
                        .aggregateAfter(Map.of("c", "c0", "r", 300))
                        .subAggregation(AggregationBuilders.sum("s").field("id"))
                )
        );
        List<QueryBuilder> queries = List.of(new MatchAllQueryBuilder(), new RangeQueryBuilder("rating").gte(500));
        for (AggregatorFactories.Builder tree : trees) {
            for (QueryBuilder query : queries) {
                LanceFragmentQueryRequest request = request(tableUri, indexName, query, tree, List.of());
                Map<Integer, LanceFragmentQueryResponse> byParallelism = new LinkedHashMap<>();
                for (int parallelism : new int[] { 1, 2, 8 }) {
                    setParallelism(parallelism);
                    try {
                        byParallelism.put(parallelism, execute(request));
                    } finally {
                        setParallelism(null);
                    }
                }
                setPushdown(false);
                LanceFragmentQueryResponse viaAggregators;
                try {
                    viaAggregators = execute(request);
                } finally {
                    setPushdown(null);
                }
                for (Map.Entry<Integer, LanceFragmentQueryResponse> entry : byParallelism.entrySet()) {
                    String label = tree + " with " + query + " at parallelism " + entry.getKey();
                    assertEquals(label, viaAggregators.matched(), entry.getValue().matched());
                    assertEquals(label, viaAggregators.aggregations(), entry.getValue().aggregations());
                }
            }
        }

        // The shard side fields of the rating terms after 8 scans: the
        // cut to shard_size 14 happens once over the 640 merged groups,
        // so 626 rows are "other"; a cut per scan before the merge would
        // leave 8 * 14 - 14 = 98.
        setParallelism(8);
        try {
            LongTerms terms = execute(request(tableUri, indexName, new MatchAllQueryBuilder(), trees.get(1), List.of())).aggregations()
                .get("r");
            assertEquals(14, terms.getBuckets().size());
            assertEquals(640L - 14L, terms.getSumOfOtherDocCounts());
            assertEquals(0L, terms.getDocCountError());

            // avg is assembled from the merged sum and count: the value
            // equals the exact quotient over every non null rating.
            InternalAggregations metrics = execute(request(tableUri, indexName, new MatchAllQueryBuilder(), trees.get(0), List.of()))
                .aggregations();
            long sum = 0;
            long count = 0;
            for (int i = 0; i < 800; i++) {
                if (i % 5 != 4) {
                    sum += (i * 37L) % 1000L;
                    count++;
                }
            }
            InternalAvg avg = metrics.get("a");
            assertEquals((double) sum / count, avg.getValue(), 0d);
        } finally {
            setParallelism(null);
        }
    }

    public void testOneFailingGroupFailsTheRequestAndAStarvedPoolStillAnswers() throws Exception {
        String indexName = "pushdown-failing-group";
        String tableUri = attach(indexName, 8, 100);
        IndexService indexService = getInstanceFromNode(IndicesService.class).indexService(resolveIndex(indexName));
        QueryShardContext qsc = indexService.newQueryShardContext(0, null, () -> 0L, null);
        Executor searchPool = getInstanceFromNode(ThreadPool.class).executor(ThreadPool.Names.SEARCH);
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            LanceAggregateResults plan = planned(
                dataset,
                Map.of(),
                qsc,
                AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.sum("s").field("rating"))
            );
            List<Integer> every = List.of(0, 1, 2, 3, 4, 5, 6, 7);
            LanceAggregateResults.Result reference = plan.execute(
                dataset,
                every,
                null,
                1,
                searchPool,
                LanceCancellation.NONE,
                name -> null
            );
            assertEquals(1, reference.scans());
            assertEquals(800L, reference.totalRows());

            // A fragment id the table does not have makes its group's
            // scan fail inside Lance; the request fails instead of
            // answering from the groups that did succeed.
            List<Integer> withMissing = List.of(0, 1, 2, 3, 4, 5, 6, 7, 999);
            Exception failure = expectThrows(
                Exception.class,
                () -> plan.execute(dataset, withMissing, null, 4, searchPool, LanceCancellation.NONE, name -> null)
            );
            assertNotNull(failure.getMessage());

            // An executor that rejects everything, and one that accepts
            // but never runs: the calling thread scans every group itself
            // and the answer is the same.
            Executor rejecting = task -> { throw new RejectedExecutionException("full"); };
            LanceAggregateResults.Result rejected = plan.execute(dataset, every, null, 4, rejecting, LanceCancellation.NONE, name -> null);
            assertEquals(4, rejected.scans());
            assertEquals(reference.aggregations(), rejected.aggregations());
            assertEquals(reference.totalRows(), rejected.totalRows());

            List<Runnable> parked = new ArrayList<>();
            LanceAggregateResults.Result starved = plan.execute(dataset, every, null, 4, parked::add, LanceCancellation.NONE, name -> null);
            assertEquals(4, starved.scans());
            assertEquals(3, parked.size());
            assertEquals(reference.aggregations(), starved.aggregations());
            // The parked tasks find nothing left to do when they finally run.
            for (Runnable task : parked) {
                task.run();
            }

            LanceAggregateResults.Result parallel = plan.execute(dataset, every, null, 8, searchPool, LanceCancellation.NONE, name -> null);
            assertEquals(8, parallel.scans());
            assertEquals(reference.aggregations(), parallel.aggregations());
        }
    }

    public void testCalendarIntervalDateHistogramEqualsAggregatorResult() throws Exception {
        // The dated fixture: six rows on a timestamp[us] column, two of
        // them in March 2024. Every calendar unit date_trunc knows is
        // compared with the aggregators, with and without a metric child
        // and a filter.
        String indexName = "pushdown-calendar";
        Path dir = createTempDir();
        String tableUri = LanceTableFactory.writeDatedTable(dir, indexName);
        attachTable(indexName, tableUri);
        List<DateHistogramInterval> intervals = List.of(
            DateHistogramInterval.MONTH,
            DateHistogramInterval.DAY,
            DateHistogramInterval.WEEK,
            DateHistogramInterval.QUARTER,
            DateHistogramInterval.YEAR,
            DateHistogramInterval.HOUR,
            DateHistogramInterval.MINUTE,
            DateHistogramInterval.SECOND,
            new DateHistogramInterval("1M"),
            new DateHistogramInterval("1d")
        );
        for (DateHistogramInterval interval : intervals) {
            List<AggregatorFactories.Builder> trees = List.of(
                AggregatorFactories.builder().addAggregator(AggregationBuilders.dateHistogram("d").field("ts").calendarInterval(interval)),
                AggregatorFactories.builder()
                    .addAggregator(
                        AggregationBuilders.dateHistogram("d")
                            .field("ts")
                            .calendarInterval(interval)
                            .minDocCount(1)
                            .keyed(true)
                            .subAggregation(AggregationBuilders.sum("s").field("id"))
                            .subAggregation(AggregationBuilders.max("last").field("ts"))
                    ),
                AggregatorFactories.builder()
                    .addAggregator(
                        AggregationBuilders.dateHistogram("d").field("ts").calendarInterval(interval).order(BucketOrder.key(false))
                    ),
                // the calendar interval as a nested level, with and
                // without empty bucket filling, and above a terms level
                AggregatorFactories.builder()
                    .addAggregator(
                        AggregationBuilders.terms("c")
                            .field("category")
                            .subAggregation(
                                AggregationBuilders.dateHistogram("d")
                                    .field("ts")
                                    .calendarInterval(interval)
                                    .minDocCount(0)
                                    .subAggregation(AggregationBuilders.sum("s").field("id"))
                            )
                    ),
                AggregatorFactories.builder()
                    .addAggregator(
                        AggregationBuilders.dateHistogram("d")
                            .field("ts")
                            .calendarInterval(interval)
                            .subAggregation(
                                AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.max("m").field("id"))
                            )
                    )
            );
            for (AggregatorFactories.Builder tree : trees) {
                for (QueryBuilder query : List.of(new MatchAllQueryBuilder(), new TermQueryBuilder("category", "odd"))) {
                    compare(tableUri, indexName, query, tree, List.of());
                }
            }
        }
        InternalDateHistogram months = execute(
            request(
                tableUri,
                indexName,
                new MatchAllQueryBuilder(),
                AggregatorFactories.builder()
                    .addAggregator(AggregationBuilders.dateHistogram("d").field("ts").calendarInterval(DateHistogramInterval.MONTH)),
                List.of()
            )
        ).aggregations().get("d");
        assertEquals(
            List.of(
                "2024-01-01T00:00:00.000Z",
                "2024-02-01T00:00:00.000Z",
                "2024-03-01T00:00:00.000Z",
                "2024-04-01T00:00:00.000Z",
                "2024-05-01T00:00:00.000Z"
            ),
            months.getBuckets().stream().map(InternalDateHistogram.Bucket::getKeyAsString).toList()
        );
        assertEquals(List.of(1L, 1L, 2L, 1L, 1L), months.getBuckets().stream().map(InternalDateHistogram.Bucket::getDocCount).toList());
    }

    public void testNestedDateHistogramAndCompositeDateSourceEqualAggregatorResults() throws Exception {
        // The interleaved fixture has a timestamp[us] column (one day
        // per row from 2024-01-01) beside a keyword category and an
        // integer id, no nulls.
        String indexName = "pushdown-dates";
        Path dir = createTempDir();
        String tableUri = LanceTableFactory.writeInterleavedTable(dir, indexName, 3, 40);
        LanceAttachResponse attached = client().execute(
            LanceAttachAction.INSTANCE,
            new LanceAttachRequest(tableUri, indexName, null, null, StorageOptions.empty(), null)
        ).actionGet();
        assertEquals(indexName, attached.index());
        ensureGreen(indexName);
        List<AggregatorFactories.Builder> trees = List.of(
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c")
                        .field("category")
                        .subAggregation(
                            AggregationBuilders.dateHistogram("d")
                                .field("ts")
                                .fixedInterval(DateHistogramInterval.days(30))
                                .subAggregation(AggregationBuilders.sum("s").field("id"))
                        )
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.terms("c")
                        .field("category")
                        .subAggregation(
                            AggregationBuilders.dateHistogram("d")
                                .field("ts")
                                .fixedInterval(DateHistogramInterval.days(30))
                                .minDocCount(0)
                                .keyed(true)
                                .order(BucketOrder.key(false))
                                .subAggregation(AggregationBuilders.max("m").field("ts"))
                        )
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.dateHistogram("d")
                        .field("ts")
                        .fixedInterval(DateHistogramInterval.days(30))
                        .minDocCount(0)
                        .subAggregation(
                            AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.avg("a").field("id"))
                        )
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    AggregationBuilders.dateHistogram("d")
                        .field("ts")
                        .fixedInterval(DateHistogramInterval.days(7))
                        .subAggregation(
                            AggregationBuilders.terms("c")
                                .field("category")
                                .size(2)
                                .subAggregation(AggregationBuilders.histogram("h").field("id").interval(50))
                        )
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    composite(
                        "cd",
                        new TermsValuesSourceBuilder("c").field("category"),
                        new DateHistogramValuesSourceBuilder("d").field("ts").fixedInterval(DateHistogramInterval.days(30))
                    ).subAggregation(AggregationBuilders.count("n").field("id"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    composite(
                        "dc",
                        new DateHistogramValuesSourceBuilder("d").field("ts")
                            .fixedInterval(DateHistogramInterval.days(30))
                            .order(SortOrder.DESC),
                        new TermsValuesSourceBuilder("c").field("category")
                    ).size(4).aggregateAfter(Map.of("d", 1709510400000L, "c", "c0"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    composite(
                        "dc",
                        new DateHistogramValuesSourceBuilder("d").field("ts")
                            .fixedInterval(DateHistogramInterval.days(30))
                            .format("yyyy-MM-dd"),
                        new TermsValuesSourceBuilder("c").field("category")
                    ).size(3).aggregateAfter(Map.of("d", "2024-01-31", "c", "c2"))
                ),
            AggregatorFactories.builder()
                .addAggregator(
                    composite("t", new TermsValuesSourceBuilder("t").field("ts")).size(5).aggregateAfter(Map.of("t", 1704412800000L))
                )
        );
        List<QueryBuilder> queries = List.of(new MatchAllQueryBuilder(), new RangeQueryBuilder("id").gte(30));
        for (AggregatorFactories.Builder tree : trees) {
            for (QueryBuilder query : queries) {
                compare(tableUri, indexName, query, tree, List.of());
                compare(tableUri, indexName, query, tree, List.of(0, 2));
            }
        }
    }

    public void testNestedTermsAndCompositeCarryTheShardSideFields() throws Exception {
        String indexName = "pushdown-nested-fields";
        String tableUri = attach(indexName);
        // Per category (the rows with i % 4 != 3): every row counts
        // toward the outer bucket, only the rows with a rating open an
        // inner bucket, and every rating is distinct, so the inner
        // terms keeps shard_size = 3 * 1.5 + 10 = 14 one row buckets
        // and puts the rest in sum_other_doc_count.
        Map<String, long[]> rowsAndRated = new LinkedHashMap<>();
        for (int i = 0; i < 600; i++) {
            if (i % 4 == 3) {
                continue;
            }
            long[] counts = rowsAndRated.computeIfAbsent("c" + (i % 3), k -> new long[2]);
            counts[0]++;
            if (i % 5 != 4) {
                counts[1]++;
            }
        }
        AggregatorFactories.Builder nested = AggregatorFactories.builder()
            .addAggregator(
                AggregationBuilders.terms("c")
                    .field("category")
                    .subAggregation(
                        AggregationBuilders.terms("r").field("rating").size(3).subAggregation(AggregationBuilders.avg("a").field("id"))
                    )
            );
        LanceFragmentQueryResponse response = execute(request(tableUri, indexName, new MatchAllQueryBuilder(), nested, List.of()));
        assertEquals(600L, response.matched());
        StringTerms outer = response.aggregations().get("c");
        assertEquals(3, outer.getBuckets().size());
        assertEquals(0L, outer.getSumOfOtherDocCounts());
        for (StringTerms.Bucket bucket : outer.getBuckets()) {
            long[] counts = rowsAndRated.get(bucket.getKeyAsString());
            assertEquals(bucket.getKeyAsString(), counts[0], bucket.getDocCount());
            LongTerms inner = bucket.getAggregations().get("r");
            assertEquals(14, inner.getBuckets().size());
            assertEquals(counts[1] - 14L, inner.getSumOfOtherDocCounts());
            assertEquals(0L, inner.getDocCountError());
            long previous = Long.MIN_VALUE;
            for (LongTerms.Bucket rating : inner.getBuckets()) {
                long key = ((Number) rating.getKey()).longValue();
                assertTrue("inner buckets sorted by key: " + inner.getBuckets(), key > previous);
                previous = key;
                assertEquals(1L, rating.getDocCount());
                InternalAvg avg = rating.getAggregations().get("a");
                // Ratings are distinct below id 1000, so the bucket's one
                // row is the id whose rating is the key.
                long id = -1L;
                for (int i = 0; i < 600; i++) {
                    if ((i * 37L) % 1000L == key && i % 5 != 4) {
                        id = i;
                    }
                }
                assertEquals((double) id, avg.getValue(), 0d);
            }
        }

        // Composite: key combinations in (category, rating) order,
        // size 3 per page, the after key of one page selects the next.
        TreeMap<String, TreeMap<Long, Long>> combinations = new TreeMap<>();
        for (int i = 0; i < 600; i++) {
            if (i % 4 == 3 || i % 5 == 4) {
                continue;
            }
            combinations.computeIfAbsent("c" + (i % 3), k -> new TreeMap<>()).merge((i * 37L) % 1000L, 1L, Long::sum);
        }
        List<Map<String, Object>> expectedKeys = new ArrayList<>();
        for (Map.Entry<String, TreeMap<Long, Long>> category : combinations.entrySet()) {
            for (Long rating : category.getValue().keySet()) {
                expectedKeys.add(Map.of("c", category.getKey(), "r", rating));
            }
        }
        Map<String, Object> after = null;
        int offset = 0;
        for (int page = 0; page < 3; page++) {
            CompositeAggregationBuilder builder = composite(
                "cr",
                new TermsValuesSourceBuilder("c").field("category"),
                new TermsValuesSourceBuilder("r").field("rating")
            ).size(3);
            if (after != null) {
                builder.aggregateAfter(after);
            }
            InternalComposite result = execute(
                request(tableUri, indexName, new MatchAllQueryBuilder(), AggregatorFactories.builder().addAggregator(builder), List.of())
            ).aggregations().get("cr");
            assertEquals(3, result.getBuckets().size());
            for (int i = 0; i < 3; i++) {
                InternalComposite.InternalBucket bucket = result.getBuckets().get(i);
                assertEquals(expectedKeys.get(offset + i), bucket.getKey());
                assertEquals(1L, bucket.getDocCount());
            }
            assertEquals(expectedKeys.get(offset + 2), result.afterKey());
            after = result.afterKey();
            offset += 3;
        }
        // The last page: after the second to last key only one
        // combination remains, and the after key is that one.
        Map<String, Object> last = expectedKeys.get(expectedKeys.size() - 1);
        InternalComposite tail = execute(
            request(
                tableUri,
                indexName,
                new MatchAllQueryBuilder(),
                AggregatorFactories.builder()
                    .addAggregator(
                        composite(
                            "cr",
                            new TermsValuesSourceBuilder("c").field("category"),
                            new TermsValuesSourceBuilder("r").field("rating")
                        ).size(3).aggregateAfter(expectedKeys.get(expectedKeys.size() - 2))
                    ),
                List.of()
            )
        ).aggregations().get("cr");
        assertEquals(1, tail.getBuckets().size());
        assertEquals(last, tail.getBuckets().get(0).getKey());
        assertEquals(last, tail.afterKey());
        InternalComposite beyond = execute(
            request(
                tableUri,
                indexName,
                new MatchAllQueryBuilder(),
                AggregatorFactories.builder()
                    .addAggregator(
                        composite(
                            "cr",
                            new TermsValuesSourceBuilder("c").field("category"),
                            new TermsValuesSourceBuilder("r").field("rating")
                        ).size(3).aggregateAfter(last)
                    ),
                List.of()
            )
        ).aggregations().get("cr");
        assertEquals(0, beyond.getBuckets().size());
        assertNull(beyond.afterKey());
    }

    public void testNegativeValuesFloorLikeTheAggregators() throws Exception {
        // The signed values table crosses zero on an int64, a float64
        // and a millisecond timestamp column: -250 and -201 belong to
        // histogram bucket -300, -200 and -101 to -200, -1 to -100, so
        // a scan that truncated the quotient toward zero instead of
        // flooring it would put them one bucket too high. The pushed
        // ordinals and the aggregators must agree bucket for bucket.
        String indexName = "pushdown-signed";
        Path dir = createTempDir();
        String tableUri = LanceTableFactory.writeSignedValuesTable(dir, indexName);
        attachTable(indexName, tableUri);
        compare(
            tableUri,
            indexName,
            new MatchAllQueryBuilder(),
            AggregatorFactories.builder().addAggregator(AggregationBuilders.histogram("h").field("v").interval(100)),
            List.of()
        );
        compare(
            tableUri,
            indexName,
            new MatchAllQueryBuilder(),
            AggregatorFactories.builder().addAggregator(AggregationBuilders.histogram("hf").field("f").interval(100)),
            List.of()
        );
        compare(
            tableUri,
            indexName,
            new MatchAllQueryBuilder(),
            AggregatorFactories.builder()
                .addAggregator(AggregationBuilders.dateHistogram("d").field("ts").fixedInterval(DateHistogramInterval.days(30))),
            List.of()
        );
    }

    private static CompositeAggregationBuilder composite(String name, CompositeValuesSourceBuilder<?>... sources) {
        return new CompositeAggregationBuilder(name, List.of(sources));
    }

    private static boolean candidate(AggregationBuilder... builders) {
        AggregatorFactories.Builder tree = AggregatorFactories.builder();
        for (AggregationBuilder builder : builders) {
            tree.addAggregator(builder);
        }
        return LanceAggregationSupport.isPushdownCandidate(tree);
    }

    private static LanceAggregateResults planned(
        Dataset dataset,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        AggregationBuilder builder
    ) {
        return planned(dataset, multiFields, qsc, AggregatorFactories.builder().addAggregator(builder));
    }

    private static LanceAggregateResults planned(
        Dataset dataset,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        AggregatorFactories.Builder tree
    ) {
        int maxGroups = LancePlugin.AGGREGATION_PUSHDOWN_MAX_GROUPS_SETTING.get(qsc.getIndexSettings().getNodeSettings());
        return planned(dataset, multiFields, qsc, tree, maxGroups);
    }

    /**
     * The full routing decision as the transport action makes it: the
     * structural gate, the translator, the Volcano planner with the
     * pushdown rule, and the executor's resolution against the mapping.
     * Null when any of them refuses, in which case the aggregators
     * answer.
     */
    private static LanceAggregateResults planned(
        Dataset dataset,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        AggregatorFactories.Builder tree,
        int maxGroups
    ) {
        if (!LanceAggregationSupport.isPushdownCandidate(tree)) {
            return null;
        }
        LancePlannerFactory factory = new LancePlannerFactory(1L << 30, 1L << 30);
        LanceSchemas.IndexModel model = LanceSchemas.model("idx", dataset.getSchema(), multiFields, () -> 600L);
        RelNode logical;
        try {
            logical = SearchRequestToRel.translateAggregations(tree, model, factory);
        } catch (UnsupportedOperationException unsupported) {
            return null;
        }
        RelNode physical = factory.plan(logical);
        if (!(physical instanceof LanceTableScan scan)) {
            return null;
        }
        PushedOperation.PushedAggregate pushed = scan.pushedAggregate().orElse(null);
        if (pushed == null) {
            return null;
        }
        return LanceAggregateResults.resolve(
            pushed.aggregate(),
            pushed.substrait(),
            tree,
            dataset.getSchema(),
            multiFields,
            qsc,
            maxGroups
        );
    }

    private String attach(String indexName) throws Exception {
        return attach(indexName, 3, 200);
    }

    private String attach(String indexName, int fragments, int rowsPerFragment) throws Exception {
        Path dir = createTempDir();
        String tableUri = LanceTableFactory.writeHintFixtureTable(dir, indexName, fragments, rowsPerFragment);
        attachTable(indexName, tableUri);
        return tableUri;
    }

    private void attachTable(String indexName, String tableUri) throws Exception {
        LanceAttachResponse attached = client().execute(
            LanceAttachAction.INSTANCE,
            new LanceAttachRequest(tableUri, indexName, null, null, StorageOptions.empty(), null)
        ).actionGet();
        assertEquals(indexName, attached.index());
        ensureGreen(indexName);
    }

    /**
     * Runs the request with the pushdown on, then off, and asserts the
     * aggregations and the match count agree. {@link InternalAggregations}
     * equality compares every field the wire format carries (buckets and
     * their sub aggregations, orders, thresholds, other doc count, error,
     * formats, metadata), so it also covers what the JSON hides. The
     * tree has to plan, so that the first run really is the pushdown.
     */
    private void compare(String tableUri, String indexName, QueryBuilder query, AggregatorFactories.Builder tree, List<Integer> fragmentIds)
        throws Exception {
        IndexService indexService = getInstanceFromNode(IndicesService.class).indexService(resolveIndex(indexName));
        QueryShardContext qsc = indexService.newQueryShardContext(0, null, () -> 0L, null);
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            assertNotNull("tree must take the pushdown: " + tree, planned(dataset, Map.of(), qsc, tree));
        }
        LanceFragmentQueryRequest request = request(tableUri, indexName, query, tree, fragmentIds);
        LanceFragmentQueryResponse pushed = execute(request);
        setPushdown(false);
        try {
            LanceFragmentQueryResponse viaAggregators = execute(request);
            String label = tree + " with " + query + " on fragments " + fragmentIds;
            assertEquals(label, viaAggregators.matched(), pushed.matched());
            assertEquals(label, viaAggregators.aggregations(), pushed.aggregations());
        } finally {
            setPushdown(null);
        }
    }

    private void setPushdown(Boolean value) {
        Settings.Builder settings = Settings.builder();
        if (value == null) {
            settings.putNull(LancePlugin.AGGREGATION_PUSHDOWN_SETTING.getKey());
        } else {
            settings.put(LancePlugin.AGGREGATION_PUSHDOWN_SETTING.getKey(), value);
        }
        client().admin().cluster().updateSettings(new ClusterUpdateSettingsRequest().transientSettings(settings)).actionGet();
    }

    private void setParallelism(Integer value) {
        Settings.Builder settings = Settings.builder();
        if (value == null) {
            settings.putNull(LancePlugin.AGGREGATION_PUSHDOWN_PARALLELISM_SETTING.getKey());
        } else {
            settings.put(LancePlugin.AGGREGATION_PUSHDOWN_PARALLELISM_SETTING.getKey(), value);
        }
        client().admin().cluster().updateSettings(new ClusterUpdateSettingsRequest().transientSettings(settings)).actionGet();
    }

    private void setSlack(Integer value) {
        Settings.Builder settings = Settings.builder();
        if (value == null) {
            settings.putNull(LancePlugin.AGGREGATION_PUSHDOWN_TOPK_SLACK_SETTING.getKey());
        } else {
            settings.put(LancePlugin.AGGREGATION_PUSHDOWN_TOPK_SLACK_SETTING.getKey(), value);
        }
        client().admin().cluster().updateSettings(new ClusterUpdateSettingsRequest().transientSettings(settings)).actionGet();
    }

    private LanceFragmentQueryResponse execute(LanceFragmentQueryRequest request) throws Exception {
        return getInstanceFromNode(TransportLanceFragmentQueryAction.class).execute(request);
    }

    /**
     * A per node request as the coordinator builds it: the query and,
     * when the query translates, its Lance SQL.
     */
    private LanceFragmentQueryRequest request(
        String tableUri,
        String indexName,
        QueryBuilder query,
        AggregatorFactories.Builder aggregations,
        List<Integer> fragmentIds
    ) {
        IndexMetadata metadata = getInstanceFromNode(ClusterService.class).state().metadata().index(indexName);
        String filterSql;
        try {
            LanceSchemas.IndexModel model = LanceSchemas.build(metadata, getInstanceFromNode(LanceWarmCache.class));
            filterSql = PlanExecutor.resolveScanFilterSql(
                query,
                model,
                PlanExecutor.sqlExcludedColumns(LanceOverrides.of(metadata.getSettings())),
                new LancePlannerFactory(1L << 30, 1L << 30)
            );
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
        assertTrue(
            "the coordinator translates the routed query to Lance SQL: " + query,
            query instanceof MatchAllQueryBuilder || filterSql != null
        );
        return new LanceFragmentQueryRequest(
            tableUri,
            indexName,
            StorageOptions.empty(),
            -1L,
            filterSql,
            query,
            null,
            List.of(),
            null,
            0,
            aggregations,
            fragmentIds,
            false,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
    }

}
