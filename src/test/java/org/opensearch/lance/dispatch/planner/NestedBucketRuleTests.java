/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch.planner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lance.dispatch.LanceAggregatePushdown;
import org.opensearch.lance.dispatch.PlannerTestPlans;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link NestedBucketRule} answers a plan for exactly the trees the
 * legacy dispatcher's nested bucket branch answered: a single top
 * level bucket aggregation with up to three nesting levels and metric
 * children, over fields that resolve against the schema and the
 * mapping. A metric top and a composite top answer empty (those are
 * the other two rules' shapes), a tree whose group estimate exceeds
 * the context's bound answers empty, and the plan of the fixture
 * request carries the scan bytes pinned in {@link #WIRE_FIXTURE}.
 */
public class NestedBucketRuleTests extends OpenSearchTestCase {

    private static final int MAX_GROUPS = 1_000_000;
    private static final int BINS = 128;
    private static final int SLACK = 4;

    /**
     * The Substrait bytes of the fixture request, a {@code terms} on
     * the keyword column {@code k} (field index 0) with a {@code sum}
     * child on the long column {@code v} (field index 3), captured from
     * {@code LanceAggregatePushdown.resolveNestedBucketShape} and baked
     * in as a wire form snapshot. The legacy dispatcher no longer
     * carries a nested bucket branch, so there is no second path to
     * compare against and this pin covers the rule path only. It fails
     * when a future edit shifts any encoded byte: the grouping
     * expression (the raw field reference of the keyword column), the
     * group count measure, the {@code sum} measure and its field
     * reference, or the output column names ({@code k0}, {@code n},
     * {@code m0}).
     */
    private static final byte[] WIRE_FIXTURE = HexFormat.of()
        .parseHex(
            "32180800103f18002a106c616e63652d6f70656e736561726368423608011232657874656e73696f6e3a696f2e7375"
                + "627374726169743a66756e6374696f6e735f6167677265676174655f67656e65726963422f0802122b657874656e73"
                + "696f6e3a696f2e7375627374726169743a66756e6374696f6e735f61726974686d6574696312111a0f10011a0963"
                + "6f756e743a616e792001120f1a0d10021a0773756d3a616e7920021a4d124b0a3e223c1a0c0a0a12080a0412020800"
                + "2200220e0a0c080120032a043a0210013001221c0a1a080220032a043a02100130013a0c1a0a12080a041202080322"
                + "0012026b3012016e12026d30"
        );

    /** A four column table: {@code k: utf8}, {@code k2: utf8}, {@code d: timestamp[ms]}, {@code v: int64}. */
    private static Schema fixtureSchema() {
        return new Schema(
            List.of(
                new Field("k", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("k2", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("d", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null),
                new Field("v", FieldType.nullable(new ArrowType.Int(64, true)), null)
            )
        );
    }

    private static QueryShardContext mappedContext() {
        QueryShardContext qsc = mock(QueryShardContext.class);
        when(qsc.fieldMapper("k")).thenReturn(new KeywordFieldMapper.KeywordFieldType("k"));
        when(qsc.fieldMapper("k2")).thenReturn(new KeywordFieldMapper.KeywordFieldType("k2"));
        when(qsc.fieldMapper("d")).thenReturn(new DateFieldMapper.DateFieldType("d"));
        when(qsc.fieldMapper("v")).thenReturn(new NumberFieldMapper.NumberFieldType("v", NumberFieldMapper.NumberType.LONG));
        return qsc;
    }

    private static AggregationRewriteContext context(AggregationBuilder top) {
        return new AggregationRewriteContext(
            AggregatorFactories.builder().addAggregator(top),
            fixtureSchema(),
            Map.of(),
            mappedContext(),
            MAX_GROUPS,
            BINS,
            SLACK
        );
    }

    private static AggregationBuilder fixtureTree() {
        return AggregationBuilders.terms("t").field("k").subAggregation(AggregationBuilders.sum("s").field("v"));
    }

    private static Optional<PushdownPlan> rewrite(AggregationBuilder top) {
        return new NestedBucketRule().tryRewrite(context(top));
    }

    private static byte[] bytesOf(LanceAggregatePushdown.Plan plan) {
        ByteBuffer buffer = PlannerTestPlans.substraitBytes(plan);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    public void testSingleLevelTermsPlusMetricProducesAPlan() {
        assertTrue("a terms with a sum child is this rule's shape", rewrite(fixtureTree()).isPresent());
    }

    public void testTwoLevelNestingProducesAPlan() {
        AggregationBuilder tree = AggregationBuilders.terms("a")
            .field("k")
            .subAggregation(AggregationBuilders.terms("b").field("k2").subAggregation(AggregationBuilders.avg("m").field("v")));

        assertTrue("terms > terms > avg is this rule's shape", rewrite(tree).isPresent());
    }

    public void testThreeLevelNestingProducesAPlan() {
        AggregationBuilder tree = AggregationBuilders.terms("a")
            .field("k")
            .subAggregation(
                AggregationBuilders.terms("b")
                    .field("k2")
                    .subAggregation(
                        AggregationBuilders.dateHistogram("c")
                            .field("d")
                            .fixedInterval(DateHistogramInterval.days(1))
                            .subAggregation(AggregationBuilders.avg("m").field("v"))
                    )
            );

        assertTrue("terms > terms > date_histogram > avg is this rule's shape", rewrite(tree).isPresent());
    }

    public void testDateHistogramCalendarIntervalProducesAPlan() {
        // A calendar day truncates through date_trunc, which needs the
        // column on the UTC calendar; the fixture's timestamp column
        // has no zone, so it qualifies.
        AggregationBuilder tree = AggregationBuilders.dateHistogram("dh")
            .field("d")
            .calendarInterval(DateHistogramInterval.DAY)
            .subAggregation(AggregationBuilders.sum("s").field("v"));

        assertTrue("a calendar interval date_histogram is this rule's shape", rewrite(tree).isPresent());
    }

    public void testFiltersLevelProducesAPlan() {
        AggregationBuilder tree = AggregationBuilders.filters(
            "f",
            new FiltersAggregator.KeyedFilter("a", QueryBuilders.termQuery("k", "a")),
            new FiltersAggregator.KeyedFilter("b", QueryBuilders.termQuery("k", "b"))
        ).subAggregation(AggregationBuilders.sum("s").field("v"));

        assertTrue("a filters bucket over scalar filters is this rule's shape", rewrite(tree).isPresent());
    }

    public void testRangeLevelProducesAPlan() {
        AggregationBuilder tree = AggregationBuilders.range("r")
            .field("v")
            .addUnboundedTo(500)
            .addRange(500, 1000)
            .addUnboundedFrom(1000)
            .subAggregation(AggregationBuilders.sum("s").field("v"));

        assertTrue("a range bucket on a long column is this rule's shape", rewrite(tree).isPresent());
    }

    public void testMissingLevelProducesAPlan() {
        AggregationBuilder tree = AggregationBuilders.missing("m").field("k").subAggregation(AggregationBuilders.sum("s").field("v"));

        assertTrue("a missing bucket is this rule's shape", rewrite(tree).isPresent());
    }

    public void testCardinalityMetricProducesAPlan() {
        AggregationBuilder tree = AggregationBuilders.terms("t").field("k").subAggregation(AggregationBuilders.cardinality("c").field("v"));

        assertTrue("a terms with a cardinality child is this rule's shape", rewrite(tree).isPresent());
    }

    public void testPercentilesMetricProducesAPlan() {
        AggregationBuilder tree = AggregationBuilders.terms("t").field("k").subAggregation(AggregationBuilders.percentiles("p").field("v"));

        assertTrue("a terms with a percentiles child is this rule's shape", rewrite(tree).isPresent());
    }

    public void testMetricTopIsNotMatched() {
        assertTrue("a metric top is the metric only rule's shape", rewrite(AggregationBuilders.sum("s").field("v")).isEmpty());
    }

    public void testCompositeTopIsNotMatched() {
        CompositeAggregationBuilder composite = new CompositeAggregationBuilder("c", List.of(new TermsValuesSourceBuilder("k").field("k")));

        assertTrue("a composite top is the composite rule's shape", rewrite(composite).isEmpty());
    }

    public void testShapeExceedingMaxGroupsReturnsEmpty() {
        // The estimate multiplies the shard_size of every terms level:
        // 1000 * 1.5 + 10 = 1510 per level, 2,280,100 for two, above
        // the context's bound of one million.
        AggregationBuilder tree = AggregationBuilders.terms("a")
            .field("k")
            .size(1000)
            .subAggregation(AggregationBuilders.terms("b").field("k2").size(1000));

        assertTrue("a group estimate above maxGroups resolves to no plan", rewrite(tree).isEmpty());
    }

    public void testRuleEncodesThePinnedScan() {
        Optional<PushdownPlan> out = rewrite(fixtureTree());

        assertTrue(out.isPresent());
        assertArrayEquals("the rule must encode the pinned wire form", WIRE_FIXTURE, bytesOf(out.get().asLegacyPlan()));
    }
}
