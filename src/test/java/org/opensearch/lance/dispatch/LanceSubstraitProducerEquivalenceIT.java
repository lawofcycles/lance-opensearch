/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.util.Text;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.index.IndexService;
import org.opensearch.index.query.BoolQueryBuilder;
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
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchema;
import org.opensearch.lance.plan.calcite.LanceTable;
import org.opensearch.lance.plan.substrait.LanceAggregateSpecs.BucketKind;
import org.opensearch.lance.plan.substrait.LanceAggregateSpecs.BucketSpec;
import org.opensearch.lance.plan.substrait.LanceAggregateSpecs.MetricKind;
import org.opensearch.lance.plan.substrait.LanceAggregateSpecs.MetricSpec;
import org.opensearch.lance.plan.substrait.LanceSubstraitProducer;
import org.opensearch.lance.plan.substrait.SpecAggregate;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan;
import org.opensearch.lance.query.substrait.SubstraitExpressions;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * Runs the bytes of {@code LanceSubstraitProducer} and of the legacy
 * {@code SubstraitAggregatePlan} path through the same
 * {@code Dataset.newScan} on the same fixture table and asserts the
 * result batches are equal: same rows, same values, rows matched after
 * sorting by the group keys, doubles within 1e-9. Byte equality of the
 * two encodings is not asserted; only what Lance computes from them.
 *
 * <p>The Calcite plans are built by hand with the key expressions the
 * legacy producer computes (the histogram ordinal without multiplying
 * the interval back, epoch millis for dates and booleans as 0 / 1),
 * because the legacy executor reads those forms back out of the scan.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceSubstraitProducerEquivalenceIT extends OpenSearchSingleNodeTestCase {

    private static final SqlFunction LANCE_DATE_TRUNC = new SqlFunction(
        "LANCE_DATE_TRUNC",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG1_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.TIMEDATE
    );

    /** 30 days in milliseconds, the fixed interval every date shape uses. */
    private static final long THIRTY_DAYS_MILLIS = 30L * 24L * 60L * 60L * 1000L;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LancePlugin.class);
    }

    // ---------------------------------------------------------------
    // Shapes over the hint fixture (rating int32, category utf8, flag bool)
    // ---------------------------------------------------------------

    public void testScalarShapesMatchTheLegacyProducer() throws Exception {
        String index = "substrait-equivalence";
        String uri = attach(index);
        QueryShardContext qsc = shardContext(index);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            RelBuilder builder = relBuilder(index, dataset);
            RelNode scan = builder.scan("lance", index).build();
            int rating = 2;
            int category = 3;
            int flag = 5;

            // Metric only: every plain metric plus an avg in one request.
            compare(
                "metric only",
                dataset,
                legacy(
                    dataset,
                    qsc,
                    AggregationBuilders.sum("s").field("rating"),
                    AggregationBuilders.avg("a").field("rating"),
                    AggregationBuilders.min("mn").field("rating"),
                    AggregationBuilders.max("mx").field("rating"),
                    AggregationBuilders.count("vc").field("rating")
                ),
                produced(
                    new SpecAggregate(
                        scan,
                        ImmutableBitSet.of(),
                        List.of(
                            call(SqlStdOperatorTable.SUM, scan, 0, rating, "m0"),
                            call(SqlStdOperatorTable.AVG, scan, 0, rating, "m1"),
                            call(SqlStdOperatorTable.MIN, scan, 0, rating, "m2"),
                            call(SqlStdOperatorTable.MAX, scan, 0, rating, "m3"),
                            call(SqlStdOperatorTable.COUNT, scan, 0, rating, "m4")
                        ),
                        List.of(),
                        List.of(
                            MetricSpec.of(MetricKind.SUM, "s"),
                            MetricSpec.of(MetricKind.AVG, "a"),
                            MetricSpec.of(MetricKind.MIN, "mn"),
                            MetricSpec.of(MetricKind.MAX, "mx"),
                            MetricSpec.of(MetricKind.VALUE_COUNT, "vc")
                        )
                    )
                )
            );

            // stats and extended_stats expand into the running values.
            compare(
                "stats",
                dataset,
                legacy(dataset, qsc, AggregationBuilders.stats("s").field("rating")),
                produced(metricOnly(scan, rating, MetricKind.STATS))
            );
            compare(
                "extended_stats",
                dataset,
                legacy(dataset, qsc, AggregationBuilders.extendedStats("e").field("rating").sigma(3)),
                produced(metricOnly(scan, rating, MetricKind.EXTENDED_STATS))
            );

            // cardinality groups the scan by the distinct values.
            compare(
                "cardinality",
                dataset,
                legacy(dataset, qsc, AggregationBuilders.cardinality("c").field("category")),
                produced(metricOnly(scan, category, MetricKind.CARDINALITY))
            );

            // terms with an avg child.
            compare(
                "terms with avg",
                dataset,
                legacy(
                    dataset,
                    qsc,
                    AggregationBuilders.terms("t").field("category").subAggregation(AggregationBuilders.avg("a").field("rating"))
                ),
                produced(
                    new SpecAggregate(
                        scan,
                        ImmutableBitSet.of(category),
                        List.of(call(SqlStdOperatorTable.AVG, scan, 1, rating, "m0")),
                        List.of(BucketSpec.of(BucketKind.TERMS, "t")),
                        List.of(MetricSpec.of(MetricKind.AVG, "a"))
                    )
                )
            );

            // Nested terms over a boolean with an avg at the inner level.
            compare(
                "nested terms terms avg",
                dataset,
                legacy(
                    dataset,
                    qsc,
                    AggregationBuilders.terms("c")
                        .field("category")
                        .subAggregation(
                            AggregationBuilders.terms("f").field("flag").subAggregation(AggregationBuilders.avg("a").field("rating"))
                        )
                ),
                produced(
                    new SpecAggregate(
                        scan,
                        ImmutableBitSet.of(category, flag),
                        List.of(call(SqlStdOperatorTable.AVG, scan, 2, rating, "m0")),
                        List.of(BucketSpec.of(BucketKind.TERMS, "c"), BucketSpec.of(BucketKind.TERMS, "f")),
                        List.of(MetricSpec.of(MetricKind.AVG, "a"))
                    )
                )
            );

            // Histogram: the group key is the bucket ordinal, exactly the
            // expression the legacy producer computes.
            RelBuilder histogram = relBuilder(index, dataset);
            histogram.scan("lance", index);
            RexNode ordinal = histogram.cast(
                histogram.call(
                    SqlStdOperatorTable.FLOOR,
                    histogram.call(
                        SqlStdOperatorTable.DIVIDE,
                        histogram.cast(histogram.field("rating"), SqlTypeName.DOUBLE),
                        histogram.literal(100.0d)
                    )
                ),
                SqlTypeName.BIGINT
            );
            histogram.project(ordinal);
            RelNode histogramInput = histogram.build();
            compare(
                "histogram",
                dataset,
                legacy(dataset, qsc, AggregationBuilders.histogram("h").field("rating").interval(100)),
                produced(
                    new SpecAggregate(
                        histogramInput,
                        ImmutableBitSet.of(0),
                        List.of(),
                        List.of(BucketSpec.of(BucketKind.HISTOGRAM, "h")),
                        List.of()
                    )
                )
            );

            // Range: the match-mask key over the sorted ranges.
            RelBuilder range = relBuilder(index, dataset);
            range.scan("lance", index);
            RexNode value = range.cast(range.field("rating"), SqlTypeName.DOUBLE);
            RexNode below = range.call(SqlStdOperatorTable.LESS_THAN, value, range.literal(300.0d));
            RexNode middle = range.call(
                SqlStdOperatorTable.AND,
                range.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, value, range.literal(300.0d)),
                range.call(SqlStdOperatorTable.LESS_THAN, value, range.literal(700.0d))
            );
            RexNode above = range.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, value, range.literal(700.0d));
            range.project(mask(range, below, middle, above));
            compare(
                "range",
                dataset,
                legacy(
                    dataset,
                    qsc,
                    AggregationBuilders.range("r").field("rating").addUnboundedTo(300).addRange(300, 700).addUnboundedFrom(700)
                ),
                produced(
                    new SpecAggregate(
                        range.build(),
                        ImmutableBitSet.of(0),
                        List.of(),
                        List.of(BucketSpec.of(BucketKind.RANGE, "r")),
                        List.of()
                    )
                )
            );

            // Filters with a range, a term and a must_not clause.
            RelBuilder filters = relBuilder(index, dataset);
            filters.scan("lance", index);
            RexNode ratingAsLong = filters.cast(filters.field("rating"), SqlTypeName.BIGINT);
            RexNode filterA = filters.call(
                SqlStdOperatorTable.AND,
                filters.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ratingAsLong, filters.literal(100L)),
                filters.call(SqlStdOperatorTable.LESS_THAN, ratingAsLong, filters.literal(900L))
            );
            RexNode filterB = filters.call(SqlStdOperatorTable.EQUALS, filters.field("category"), filters.literal("c0"));
            RexNode filterC = filters.call(
                SqlStdOperatorTable.NOT,
                filters.call(
                    SqlStdOperatorTable.IS_TRUE,
                    filters.call(SqlStdOperatorTable.EQUALS, filters.cast(filters.field("flag"), SqlTypeName.BIGINT), filters.literal(1L))
                )
            );
            filters.project(mask(filters, filterA, filterB, filterC));
            compare(
                "filters with other bucket and must_not",
                dataset,
                legacy(
                    dataset,
                    qsc,
                    AggregationBuilders.filters(
                        "fs",
                        new FiltersAggregator.KeyedFilter("a", new RangeQueryBuilder("rating").gte(100).lt(900)),
                        new FiltersAggregator.KeyedFilter("b", new TermQueryBuilder("category", "c0")),
                        new FiltersAggregator.KeyedFilter("c", new BoolQueryBuilder().mustNot(new TermQueryBuilder("flag", true)))
                    ).otherBucket(true)
                ),
                produced(
                    new SpecAggregate(
                        filters.build(),
                        ImmutableBitSet.of(0),
                        List.of(),
                        List.of(BucketSpec.of(BucketKind.FILTERS, "fs")),
                        List.of()
                    )
                )
            );

            // Missing: a single-bit mask over IS NULL.
            RelBuilder missing = relBuilder(index, dataset);
            missing.scan("lance", index);
            missing.project(mask(missing, missing.isNull(missing.field("category"))));
            compare(
                "missing",
                dataset,
                legacy(dataset, qsc, AggregationBuilders.missing("m").field("category")),
                produced(
                    new SpecAggregate(
                        missing.build(),
                        ImmutableBitSet.of(0),
                        List.of(),
                        List.of(BucketSpec.of(BucketKind.MISSING, "m")),
                        List.of()
                    )
                )
            );
        }
    }

    // ---------------------------------------------------------------
    // Shapes over the dated fixture (category utf8, ts timestamp[us])
    // ---------------------------------------------------------------

    public void testDateShapesMatchTheLegacyProducer() throws Exception {
        String index = "substrait-equivalence-dated";
        Path dir = createTempDir();
        String uri = LanceTableFactory.writeDatedTable(dir, index);
        attachTable(index, uri);
        QueryShardContext qsc = shardContext(index);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            int category = 1;

            // Fixed interval date_histogram: the ordinal of the epoch
            // millis divided by the interval.
            RelBuilder fixed = relBuilder(index, dataset);
            fixed.scan("lance", index);
            fixed.project(dateOrdinal(fixed));
            compare(
                "date_histogram fixed",
                dataset,
                legacy(dataset, qsc, AggregationBuilders.dateHistogram("d").field("ts").fixedInterval(DateHistogramInterval.days(30))),
                produced(
                    new SpecAggregate(
                        fixed.build(),
                        ImmutableBitSet.of(0),
                        List.of(),
                        List.of(BucketSpec.of(BucketKind.DATE_HISTOGRAM_FIXED, "d")),
                        List.of()
                    )
                )
            );

            // Calendar date_histogram through the custom truncation call.
            RelBuilder calendar = relBuilder(index, dataset);
            calendar.scan("lance", index);
            calendar.project(calendar.call(LANCE_DATE_TRUNC, calendar.literal("month"), calendar.field("ts")));
            compare(
                "date_histogram calendar month",
                dataset,
                legacy(dataset, qsc, AggregationBuilders.dateHistogram("d").field("ts").calendarInterval(DateHistogramInterval.MONTH)),
                produced(
                    new SpecAggregate(
                        calendar.build(),
                        ImmutableBitSet.of(0),
                        List.of(),
                        List.of(BucketSpec.of(BucketKind.DATE_HISTOGRAM_CALENDAR, "d")),
                        List.of()
                    )
                )
            );

            // Composite over a terms source and a fixed date_histogram source.
            RelBuilder composite = relBuilder(index, dataset);
            composite.scan("lance", index);
            composite.project(composite.field(category), dateOrdinal(composite));
            compare(
                "composite terms and date_histogram",
                dataset,
                legacy(
                    dataset,
                    qsc,
                    new CompositeAggregationBuilder(
                        "cd",
                        List.of(
                            new TermsValuesSourceBuilder("c").field("category"),
                            new DateHistogramValuesSourceBuilder("d").field("ts").fixedInterval(DateHistogramInterval.days(30))
                        )
                    )
                ),
                produced(
                    new SpecAggregate(
                        composite.build(),
                        ImmutableBitSet.of(0, 1),
                        List.of(),
                        List.of(BucketSpec.of(BucketKind.COMPOSITE_TERMS, "c"), BucketSpec.of(BucketKind.COMPOSITE_DATE_HISTOGRAM, "d")),
                        List.of()
                    )
                )
            );
        }
    }

    // ---------------------------------------------------------------
    // Percentiles: the main scan and the bin scan
    // ---------------------------------------------------------------

    public void testPercentilesBinsMatchTheLegacyProducer() throws Exception {
        String index = "substrait-equivalence-bins";
        String uri = attach(index);
        QueryShardContext qsc = shardContext(index);
        int bins = 10;
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            RelNode scan = relBuilder(index, dataset).scan("lance", index).build();
            int rating = 2;
            SpecAggregate aggregate = metricOnly(scan, rating, MetricKind.PERCENTILES);

            ByteBuffer legacyMain = legacy(dataset, qsc, AggregationBuilders.percentiles("p").field("rating"));
            ByteBuffer producedMain = produced(aggregate);
            List<Map<String, Object>> bounds = scanRows(dataset, legacyMain);
            compare("percentiles main scan", dataset, legacyMain, producedMain);

            assertEquals(1, bounds.size());
            double min = ((Number) bounds.get(0).get("m0_mn")).doubleValue();
            double max = ((Number) bounds.get(0).get("m0_mx")).doubleValue();
            double width = max > min ? (max - min) / bins : 1d;

            SubstraitAggregatePlan.Builder legacyBins = new SubstraitAggregatePlan.Builder();
            legacyBins.groupBy(SubstraitExpressions.floorFp64(new SubstraitAggregatePlan.FieldReference(rating), min, width), "m0_b");
            legacyBins.measure(
                "count",
                List.of(new SubstraitAggregatePlan.FieldReference(rating)),
                SubstraitAggregatePlan.ScalarType.I64,
                "m0_bc"
            );
            ByteBuffer producedBins = LanceSubstraitProducer.toLancePercentilesBins(aggregate, 0, min, max, bins).orElseThrow();
            compare("percentiles bin scan", dataset, legacyBins.build(), producedBins);
        }
    }

    // ---------------------------------------------------------------
    // Plan construction helpers
    // ---------------------------------------------------------------

    private RelBuilder relBuilder(String index, Dataset dataset) {
        LancePlannerFactory factory = new LancePlannerFactory(1L << 30, 1L << 30);
        LanceSchema schema = new LanceSchema(Map.of(index, new LanceTable(index, dataset.getSchema(), () -> 600L)));
        return factory.relBuilder(schema).transform(config -> config.withSimplify(false));
    }

    private static AggregateCall call(SqlAggFunction function, RelNode input, int groupCount, int argument, String name) {
        return AggregateCall.create(function, false, false, List.of(argument), -1, groupCount, input, null, name);
    }

    /** A metric-only aggregate whose single call carries {@code kind}; the Calcite function is a typing stand-in. */
    private static SpecAggregate metricOnly(RelNode scan, int argument, MetricKind kind) {
        return new SpecAggregate(
            scan,
            ImmutableBitSet.of(),
            List.of(call(SqlStdOperatorTable.COUNT, scan, 0, argument, "m0")),
            List.of(),
            List.of(MetricSpec.of(kind, "m"))
        );
    }

    /** The legacy match-mask over the conditions: {@code CASE(c0, 1, 0) + CASE(c1, 2, 0) + ...}, left folded. */
    private static RexNode mask(RelBuilder builder, RexNode... conditions) {
        RexNode sum = null;
        for (int i = 0; i < conditions.length; i++) {
            RexNode bit = builder.call(SqlStdOperatorTable.CASE, conditions[i], builder.literal(1L << i), builder.literal(0L));
            sum = sum == null ? bit : builder.call(SqlStdOperatorTable.PLUS, sum, bit);
        }
        return sum;
    }

    /** {@code FLOOR(CAST(ts AS BIGINT) / interval)}: the fixed date_histogram ordinal over the dated fixture. */
    private static RexNode dateOrdinal(RelBuilder builder) {
        RexNode epochMillis = builder.cast(builder.field("ts"), SqlTypeName.BIGINT);
        return builder.call(
            SqlStdOperatorTable.FLOOR,
            builder.call(SqlStdOperatorTable.DIVIDE, epochMillis, builder.literal(THIRTY_DAYS_MILLIS))
        );
    }

    private ByteBuffer legacy(Dataset dataset, QueryShardContext qsc, AggregationBuilder... builders) {
        AggregatorFactories.Builder tree = AggregatorFactories.builder();
        for (AggregationBuilder builder : builders) {
            tree.addAggregator(builder);
        }
        LanceAggregatePushdown.Plan plan = LanceAggregatePushdown.plan(tree, dataset.getSchema(), Map.of(), qsc);
        assertNotNull("the legacy producer must resolve the tree: " + tree, plan);
        return plan.substraitPlan();
    }

    private static ByteBuffer produced(RelNode aggregate) {
        return LanceSubstraitProducer.toLanceAggregate(aggregate)
            .orElseThrow(() -> new AssertionError("the producer must accept the plan"));
    }

    // ---------------------------------------------------------------
    // Scan and comparison helpers
    // ---------------------------------------------------------------

    private void compare(String label, Dataset dataset, ByteBuffer legacyPlan, ByteBuffer producedPlan) throws Exception {
        assertEquivalent(label, scanRows(dataset, legacyPlan), scanRows(dataset, producedPlan));
    }

    private static List<Map<String, Object>> scanRows(Dataset dataset, ByteBuffer plan) throws Exception {
        ScanOptions options = new ScanOptions.Builder().substraitAggregate(plan.duplicate()).build();
        List<Map<String, Object>> rows = new ArrayList<>();
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                for (int row = 0; row < root.getRowCount(); row++) {
                    Map<String, Object> values = new LinkedHashMap<>();
                    for (FieldVector vector : root.getFieldVectors()) {
                        values.put(vector.getName(), normalize(vector.getObject(row)));
                    }
                    rows.add(values);
                }
            }
        }
        return rows;
    }

    private static Object normalize(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Text text) {
            return text.toString();
        }
        if (value instanceof Float || value instanceof Double) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return value;
    }

    private static void assertEquivalent(String label, List<Map<String, Object>> legacy, List<Map<String, Object>> produced) {
        assertEquals(label + ": row count", legacy.size(), produced.size());
        if (legacy.isEmpty()) {
            return;
        }
        assertEquals(label + ": columns", legacy.get(0).keySet(), produced.get(0).keySet());
        Comparator<Map<String, Object>> byKeys = keyOrder(legacy.get(0).keySet());
        List<Map<String, Object>> sortedLegacy = new ArrayList<>(legacy);
        List<Map<String, Object>> sortedProduced = new ArrayList<>(produced);
        sortedLegacy.sort(byKeys);
        sortedProduced.sort(byKeys);
        for (int row = 0; row < sortedLegacy.size(); row++) {
            for (String column : sortedLegacy.get(row).keySet()) {
                Object expected = sortedLegacy.get(row).get(column);
                Object actual = sortedProduced.get(row).get(column);
                String at = label + ": row " + row + " column " + column;
                if (expected instanceof Double d && actual instanceof Double a) {
                    assertEquals(at, d, a, 1e-9);
                } else {
                    assertEquals(at, expected, actual);
                }
            }
        }
    }

    /** Sorts rows by the group key columns ({@code k*}, distinct and bin columns), stringified. */
    private static Comparator<Map<String, Object>> keyOrder(Set<String> columns) {
        List<String> keys = columns.stream()
            .filter(column -> column.startsWith("k") || column.endsWith("_d") || column.endsWith("_b"))
            .sorted()
            .collect(Collectors.toList());
        return Comparator.comparing(row -> keys.stream().map(key -> String.valueOf(row.get(key))).collect(Collectors.joining("\u0000")));
    }

    // ---------------------------------------------------------------
    // Fixture attach helpers
    // ---------------------------------------------------------------

    private String attach(String indexName) throws Exception {
        Path dir = createTempDir();
        String tableUri = LanceTableFactory.writeHintFixtureTable(dir, indexName, 3, 200);
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

    private QueryShardContext shardContext(String indexName) {
        IndexService indexService = getInstanceFromNode(IndicesService.class).indexService(resolveIndex(indexName));
        return indexService.newQueryShardContext(0, null, () -> 0L, null);
    }
}
