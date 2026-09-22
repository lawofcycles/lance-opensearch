/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch.planner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
 * a match carries the same scan bytes the legacy path encodes for the
 * same request.
 */
public class MetricOnlyRuleTests extends OpenSearchTestCase {

    private static final int MAX_GROUPS = 1_000_000;
    private static final int BINS = 128;
    private static final int SLACK = 4;

    /** A one column table, {@code f: int64}, mapped as {@code long}. */
    private static Schema longSchema() {
        return new Schema(List.of(new Field("f", FieldType.nullable(new ArrowType.Int(64, true)), null)));
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

    public void testMetricOnlyTreeAnswersAPlan() {
        Optional<PushdownPlan> out = new MetricOnlyRule().tryRewrite(context(sumTree(), longSchema(), longContext()));

        assertTrue("a sum over a mapped long column is this rule's shape", out.isPresent());
        assertNotNull(PlannerTestPlans.substraitBytes(out.get().asLegacyPlan()));
    }

    public void testBucketAtTheTopAnswersEmpty() {
        AggregatorFactories.Builder tree = AggregatorFactories.builder().addAggregator(AggregationBuilders.terms("t").field("f"));

        Optional<PushdownPlan> out = new MetricOnlyRule().tryRewrite(context(tree, longSchema(), longContext()));

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
        Optional<PushdownPlan> out = new MetricOnlyRule().tryRewrite(context(sumTree(), longSchema(), mock(QueryShardContext.class)));

        assertTrue(out.isEmpty());
    }

    public void testRuleAndLegacyPathEncodeTheSameScan() {
        Schema schema = longSchema();
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
        assertEquals(
            "the migration must be byte preserving",
            PlannerTestPlans.substraitBytes(viaLegacy),
            PlannerTestPlans.substraitBytes(viaRule.get().asLegacyPlan())
        );
    }
}
