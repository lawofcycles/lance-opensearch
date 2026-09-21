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
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.LanceAttachResponse;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.query.LanceKnnFilterTranslator;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.InternalComposite;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;

/**
 * The Substrait aggregation pushdown against a real table: which trees
 * qualify, how fields resolve to columns, and that the
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
public class LanceAggregatePushdownTests extends OpenSearchSingleNodeTestCase {

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
        assertFalse(
            "order by sub aggregation",
            candidate(
                AggregationBuilders.terms("c")
                    .field("category")
                    .order(BucketOrder.aggregation("a", false))
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

    public void testPlanResolvesFieldsAgainstTheTableSchema() throws Exception {
        String indexName = "pushdown-plan";
        String tableUri = attach(indexName);
        IndexService indexService = getInstanceFromNode(IndicesService.class).indexService(resolveIndex(indexName));
        QueryShardContext qsc = indexService.newQueryShardContext(0, null, () -> 0L, null);
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            Map<String, LinkedHashMap<String, String>> noMultiFields = Map.of();
            assertNotNull("keyword terms", plan(dataset, noMultiFields, qsc, AggregationBuilders.terms("c").field("category")));
            assertNotNull("integer terms", plan(dataset, noMultiFields, qsc, AggregationBuilders.terms("r").field("rating")));
            assertNotNull("boolean terms", plan(dataset, noMultiFields, qsc, AggregationBuilders.terms("f").field("flag")));
            assertNotNull("value_count on keyword", plan(dataset, noMultiFields, qsc, AggregationBuilders.count("n").field("category")));
            assertNotNull("sum on boolean", plan(dataset, noMultiFields, qsc, AggregationBuilders.sum("s").field("flag")));
            assertNotNull(
                "integer histogram",
                plan(dataset, noMultiFields, qsc, AggregationBuilders.histogram("h").field("rating").interval(50))
            );

            assertNull("keyword list column", plan(dataset, noMultiFields, qsc, AggregationBuilders.terms("t").field("tags")));
            assertNull("text column", plan(dataset, noMultiFields, qsc, AggregationBuilders.terms("b").field("body")));
            assertNull("unmapped field", plan(dataset, noMultiFields, qsc, AggregationBuilders.sum("u").field("nope")));
            assertNull("sum on keyword", plan(dataset, noMultiFields, qsc, AggregationBuilders.sum("s").field("category")));
            assertNull(
                "histogram on keyword",
                plan(dataset, noMultiFields, qsc, AggregationBuilders.histogram("h").field("category").interval(1))
            );
            assertNull(
                "histogram on boolean",
                plan(dataset, noMultiFields, qsc, AggregationBuilders.histogram("h").field("flag").interval(1))
            );
            assertNull(
                "date_histogram on integer",
                plan(
                    dataset,
                    noMultiFields,
                    qsc,
                    AggregationBuilders.dateHistogram("d").field("rating").fixedInterval(DateHistogramInterval.days(1))
                )
            );
            assertNull(
                "one unsupported child refuses the tree",
                plan(
                    dataset,
                    noMultiFields,
                    qsc,
                    AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.sum("s").field("tags"))
                )
            );
            assertNull(
                "sub-field the mapping does not know",
                plan(dataset, noMultiFields, qsc, AggregationBuilders.terms("b").field("body.raw"))
            );
            assertNull(
                "nested terms on a keyword list column",
                plan(
                    dataset,
                    noMultiFields,
                    qsc,
                    AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.terms("t").field("tags"))
                )
            );
            assertNotNull(
                "nested terms on scalar columns",
                plan(
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
            assertNull("group estimate above pushdown_max_groups", plan(dataset, noMultiFields, qsc, wide));
            assertNotNull(
                "group estimate under an explicit bound",
                LanceAggregatePushdown.plan(
                    AggregatorFactories.builder().addAggregator(wide),
                    dataset.getSchema(),
                    noMultiFields,
                    qsc,
                    3_000_000
                )
            );
            assertNull(
                "single terms level above the bound",
                LanceAggregatePushdown.plan(
                    AggregatorFactories.builder().addAggregator(AggregationBuilders.terms("r").field("rating").size(100)),
                    dataset.getSchema(),
                    noMultiFields,
                    qsc,
                    100
                )
            );
            assertNotNull(
                "composite over keyword and integer",
                plan(
                    dataset,
                    noMultiFields,
                    qsc,
                    composite("cr", new TermsValuesSourceBuilder("c").field("category"), new TermsValuesSourceBuilder("r").field("rating"))
                        .aggregateAfter(Map.of("c", "c1", "r", 500))
                )
            );
            assertNull(
                "composite after value of the wrong type for a keyword source",
                plan(
                    dataset,
                    noMultiFields,
                    qsc,
                    composite("cr", new TermsValuesSourceBuilder("c").field("category"), new TermsValuesSourceBuilder("r").field("rating"))
                        .aggregateAfter(Map.of("c", 7, "r", 500))
                )
            );
            assertNull(
                "composite after value that does not parse as a number",
                plan(
                    dataset,
                    noMultiFields,
                    qsc,
                    composite("cr", new TermsValuesSourceBuilder("c").field("category"), new TermsValuesSourceBuilder("r").field("rating"))
                        .aggregateAfter(Map.of("c", "c1", "r", "high"))
                )
            );
            assertNull(
                "composite date source on an integer",
                plan(
                    dataset,
                    noMultiFields,
                    qsc,
                    composite("d", new DateHistogramValuesSourceBuilder("d").field("rating").fixedInterval(DateHistogramInterval.days(1)))
                )
            );
            assertNull(
                "composite source on a keyword list column",
                plan(dataset, noMultiFields, qsc, composite("t", new TermsValuesSourceBuilder("t").field("tags")))
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
            LanceAggregatePushdown.Plan plan = plan(
                dataset,
                Map.of(),
                qsc,
                AggregationBuilders.terms("c").field("category").subAggregation(AggregationBuilders.sum("s").field("rating"))
            );
            List<Integer> every = List.of(0, 1, 2, 3, 4, 5, 6, 7);
            LanceAggregatePushdown.Result reference = plan.execute(
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
            LanceAggregatePushdown.Result rejected = plan.execute(dataset, every, null, 4, rejecting, LanceCancellation.NONE, name -> null);
            assertEquals(4, rejected.scans());
            assertEquals(reference.aggregations(), rejected.aggregations());
            assertEquals(reference.totalRows(), rejected.totalRows());

            List<Runnable> parked = new ArrayList<>();
            LanceAggregatePushdown.Result starved = plan.execute(
                dataset,
                every,
                null,
                4,
                parked::add,
                LanceCancellation.NONE,
                name -> null
            );
            assertEquals(4, starved.scans());
            assertEquals(3, parked.size());
            assertEquals(reference.aggregations(), starved.aggregations());
            // The parked tasks find nothing left to do when they finally run.
            for (Runnable task : parked) {
                task.run();
            }

            LanceAggregatePushdown.Result parallel = plan.execute(
                dataset,
                every,
                null,
                8,
                searchPool,
                LanceCancellation.NONE,
                name -> null
            );
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

    private static LanceAggregatePushdown.Plan plan(
        Dataset dataset,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        AggregationBuilder builder
    ) {
        return LanceAggregatePushdown.plan(AggregatorFactories.builder().addAggregator(builder), dataset.getSchema(), multiFields, qsc);
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
            assertNotNull("tree must take the pushdown: " + tree, LanceAggregatePushdown.plan(tree, dataset.getSchema(), Map.of(), qsc));
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
        String filterSql = query instanceof MatchAllQueryBuilder
            ? null
            : LanceKnnFilterTranslator.toLanceSql(query, field -> fieldType(metadata, field));
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

    @SuppressWarnings("unchecked")
    private static String fieldType(IndexMetadata metadata, String field) {
        Map<String, Object> properties = (Map<String, Object>) metadata.mapping().sourceAsMap().get("properties");
        Map<String, Object> definition = (Map<String, Object>) properties.get(field);
        return definition == null ? null : (String) definition.get("type");
    }
}
