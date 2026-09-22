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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lance.dispatch.LanceAggregatePushdown;
import org.opensearch.lance.dispatch.PlannerTestPlans;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link MetricOnlyRule} answers a plan for exactly the trees the
 * legacy dispatcher's metric only branch answered: metric aggregations
 * whose fields resolve against the schema and the mapping, no bucket
 * level. A bucket at the top and a metric the resolution refuses (a
 * {@code sum} on a keyword column) both answer empty, and the plan of
 * a match carries the scan bytes pinned in {@link #WIRE_FIXTURE},
 * through the rule and through the legacy path alike.
 */
public class MetricOnlyRuleTests extends OpenSearchTestCase {

    private static final int MAX_GROUPS = 1_000_000;
    private static final int BINS = 128;
    private static final int SLACK = 4;

    /**
     * The Substrait bytes of the fixture request, a {@code sum} named
     * {@code s} over the second column of {@link #fixtureSchema()},
     * captured from {@code LanceAggregatePushdown.resolveMetricOnly}
     * and baked in as a wire form snapshot. Both dispatch paths call
     * that helper, so comparing them to each other could not catch a
     * wrong encoding inside it; this pin fails when a future edit
     * shifts any encoded byte: the {@code sum} measure, its field
     * reference (index 1), the absence of grouping expressions, or the
     * output column names ({@code n} for the group count, {@code m0}
     * for the metric's slot). The request's metric name is not in the
     * wire form (it only names the result the executor builds), so it
     * is not pinned here.
     */
    private static final byte[] WIRE_FIXTURE = HexFormat.of()
        .parseHex(
            "32180800103f18002a106c616e63652d6f70656e736561726368423608011232657874656e73696f6e3a696f2e737562"
                + "7374726169743a66756e6374696f6e735f6167677265676174655f67656e65726963422f0802122b657874656e73"
                + "696f6e3a696f2e7375627374726169743a66756e6374696f6e735f61726974686d6574696312111a0f10011a0963"
                + "6f756e743a616e792001120f1a0d10021a0773756d3a616e7920021a3b12390a30222e220e0a0c080120032a043a"
                + "0210013001221c0a1a080220032a043a02100130013a0c1a0a12080a0412020801220012016e12026d30"
        );

    /** A two column table, {@code id: int32} and {@code f: int64}, so the metric binds to field index 1. */
    private static Schema fixtureSchema() {
        return new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("f", FieldType.nullable(new ArrowType.Int(64, true)), null)
            )
        );
    }

    private static QueryShardContext longContext() {
        QueryShardContext qsc = mock(QueryShardContext.class);
        when(qsc.fieldMapper("f")).thenReturn(new NumberFieldMapper.NumberFieldType("f", NumberFieldMapper.NumberType.LONG));
        return qsc;
    }

    private static AggregationRewriteContext context(AggregatorFactories.Builder tree, Schema schema, QueryShardContext qsc) {
        return new AggregationRewriteContext(tree, schema, Map.of(), qsc, MAX_GROUPS, BINS, SLACK);
    }

    private static AggregatorFactories.Builder sumTree() {
        return AggregatorFactories.builder().addAggregator(AggregationBuilders.sum("s").field("f"));
    }

    private static byte[] bytesOf(LanceAggregatePushdown.Plan plan) {
        ByteBuffer buffer = PlannerTestPlans.substraitBytes(plan);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    public void testMetricOnlyTreeAnswersThePinnedScan() {
        Optional<PushdownPlan> out = new MetricOnlyRule().tryRewrite(context(sumTree(), fixtureSchema(), longContext()));

        assertTrue("a sum over a mapped long column is this rule's shape", out.isPresent());
        assertArrayEquals("the rule must encode the pinned wire form", WIRE_FIXTURE, bytesOf(out.get().asLegacyPlan()));
    }

    public void testBucketAtTheTopAnswersEmpty() {
        AggregatorFactories.Builder tree = AggregatorFactories.builder().addAggregator(AggregationBuilders.terms("t").field("f"));

        Optional<PushdownPlan> out = new MetricOnlyRule().tryRewrite(context(tree, fixtureSchema(), longContext()));

        assertTrue("a terms bucket is not the metric only shape", out.isEmpty());
    }

    public void testMetricTheResolutionRefusesAnswersEmpty() {
        // The shape check accepts a sum, but a sum needs a number and
        // the column is a keyword, so the field resolution refuses and
        // the request falls through to the aggregators.
        Schema schema = new Schema(List.of(new Field("k", FieldType.nullable(new ArrowType.Utf8()), null)));
        QueryShardContext qsc = mock(QueryShardContext.class);
        when(qsc.fieldMapper("k")).thenReturn(new KeywordFieldMapper.KeywordFieldType("k"));
        AggregatorFactories.Builder tree = AggregatorFactories.builder().addAggregator(AggregationBuilders.sum("s").field("k"));

        Optional<PushdownPlan> out = new MetricOnlyRule().tryRewrite(context(tree, schema, qsc));

        assertTrue("a sum on a keyword column resolves to no plan", out.isEmpty());
    }

    public void testUnmappedFieldAnswersEmpty() {
        // The mock context maps no field at all, so the resolution
        // refuses before it reaches the schema.
        Optional<PushdownPlan> out = new MetricOnlyRule().tryRewrite(context(sumTree(), fixtureSchema(), mock(QueryShardContext.class)));

        assertTrue(out.isEmpty());
    }

    public void testRuleAndLegacyPathBothEncodeThePinnedScan() {
        Schema schema = fixtureSchema();
        QueryShardContext qsc = longContext();

        Optional<PushdownPlan> viaRule = new MetricOnlyRule().tryRewrite(context(sumTree(), schema, qsc));
        LanceAggregatePushdown.Plan viaLegacy = PlannerTestPlans.planWithoutRules(
            sumTree(),
            schema,
            Map.of(),
            qsc,
            MAX_GROUPS,
            BINS,
            SLACK
        );

        assertTrue(viaRule.isPresent());
        assertNotNull(viaLegacy);
        assertArrayEquals("the rule path must encode the pinned wire form", WIRE_FIXTURE, bytesOf(viaRule.get().asLegacyPlan()));
        assertArrayEquals("the legacy path must encode the pinned wire form", WIRE_FIXTURE, bytesOf(viaLegacy));
    }
}
