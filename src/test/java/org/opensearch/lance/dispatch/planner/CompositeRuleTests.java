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
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lance.dispatch.LanceAggregatePushdown;
import org.opensearch.lance.dispatch.PlannerTestPlans;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link CompositeRule} answers a plan for exactly the trees the
 * legacy dispatcher's composite branch answered: a single top level
 * {@code composite} over {@code terms} and fixed interval
 * {@code date_histogram} sources whose fields resolve against the
 * schema and the mapping. A metric or bucket top, a source whose
 * column is unmapped and a {@code missing_bucket} source all answer
 * empty, an {@code after} map lands parsed on the plan's composite,
 * and the plan of the fixture request carries the scan bytes pinned
 * in {@link #WIRE_FIXTURE}.
 */
public class CompositeRuleTests extends OpenSearchTestCase {

    private static final int MAX_GROUPS = 1_000_000;
    private static final int BINS = 128;
    private static final int SLACK = 4;

    /**
     * The Substrait bytes of the fixture request, a {@code composite}
     * with a descending {@code terms} source on the keyword column
     * {@code k} (field index 0) and a descending one day fixed interval
     * {@code date_histogram} source on the date column {@code d} (field
     * index 1), size 100 and no {@code after}, captured from
     * {@code LanceAggregatePushdown.resolveCompositeShape} and baked in
     * as a wire form snapshot. This pin fails when a future edit shifts
     * any encoded byte: the two grouping expressions (the raw field
     * reference for the keyword source, the epoch millis floor division
     * for the date source), the group count measure, or the output
     * column names ({@code k0}, {@code k1}, {@code n}). {@code size},
     * {@code after} and the source directions are not in the wire form
     * (the executor applies them to the sorted group rows), so they are
     * pinned by {@link #testAfterKeyIsHonored} instead.
     */
    private static final byte[] WIRE_FIXTURE = HexFormat.of()
        .parseHex(
            "32180800103f18002a106c616e63652d6f70656e736561726368422f0801122b657874656e73696f6e3a696f2e7375"
                + "627374726169743a66756e6374696f6e735f61726974686d65746963422f0802122b657874656e73696f6e3a696f2e"
                + "7375627374726169743a66756e6374696f6e735f636f6d70617269736f6e423608031232657874656e73696f6e3a69"
                + "6f2e7375627374726169743a66756e6374696f6e735f6167677265676174655f67656e6572696312141a1210011a0c"
                + "73756274726163743a616e79200112121a1010021a0a6469766964653a616e792001120e1a0c10031a066c743a616e"
                + "79200212131a1110041a0b6d6f64756c75733a616e79200112111a0f10051a09636f756e743a616e7920031aba0112"
                + "b7010aa90122a6011a93010a0a12080a041202080022000a84011a81010801222e1a2c1a2a080222181a165a140a04"
                + "3a021001120a12080a041202080122001802220c1a0a0a083880b89929900300224d1a4b5a490a043a021001123f1a"
                + "3d0803222e1a2c1a2a080422181a165a140a043a021001120a12080a041202080122001802220c1a0a0a083880b899"
                + "2990030022091a070a0538009003001802220e0a0c080520032a043a021001300112026b3012026b3112016e"
        );

    /** A three column table: {@code k: utf8}, {@code d: timestamp[ms]}, {@code v: int64}. */
    private static Schema fixtureSchema() {
        return new Schema(
            List.of(
                new Field("k", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("d", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null),
                new Field("v", FieldType.nullable(new ArrowType.Int(64, true)), null)
            )
        );
    }

    private static QueryShardContext mappedContext() {
        QueryShardContext qsc = mock(QueryShardContext.class);
        when(qsc.fieldMapper("k")).thenReturn(new KeywordFieldMapper.KeywordFieldType("k"));
        when(qsc.fieldMapper("d")).thenReturn(new DateFieldMapper.DateFieldType("d"));
        when(qsc.fieldMapper("v")).thenReturn(new NumberFieldMapper.NumberFieldType("v", NumberFieldMapper.NumberType.LONG));
        return qsc;
    }

    private static AggregationRewriteContext context(AggregatorFactories.Builder tree, QueryShardContext qsc) {
        return new AggregationRewriteContext(tree, fixtureSchema(), Map.of(), qsc, MAX_GROUPS, BINS, SLACK);
    }

    private static CompositeAggregationBuilder fixtureComposite() {
        return composite(
            new TermsValuesSourceBuilder("k").field("k").order(SortOrder.DESC),
            new DateHistogramValuesSourceBuilder("d").field("d").fixedInterval(DateHistogramInterval.days(1)).order(SortOrder.DESC)
        ).size(100);
    }

    private static CompositeAggregationBuilder composite(CompositeValuesSourceBuilder<?>... sources) {
        return new CompositeAggregationBuilder("c", List.of(sources));
    }

    private static AggregatorFactories.Builder tree(CompositeAggregationBuilder builder) {
        return AggregatorFactories.builder().addAggregator(builder);
    }

    private static byte[] bytesOf(LanceAggregatePushdown.Plan plan) {
        ByteBuffer buffer = PlannerTestPlans.substraitBytes(plan);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    public void testCompositeTreeAnswersAPlan() {
        CompositeAggregationBuilder builder = fixtureComposite();
        builder.subAggregation(AggregationBuilders.sum("s").field("v"));

        Optional<PushdownPlan> out = new CompositeRule().tryRewrite(context(tree(builder), mappedContext()));

        assertTrue("a composite over mapped terms and date_histogram sources with a metric child is this rule's shape", out.isPresent());
    }

    public void testNonCompositeTopIsNotMatched() {
        QueryShardContext qsc = mappedContext();
        AggregatorFactories.Builder metricTop = AggregatorFactories.builder().addAggregator(AggregationBuilders.sum("s").field("v"));
        AggregatorFactories.Builder bucketTop = AggregatorFactories.builder().addAggregator(AggregationBuilders.terms("t").field("k"));

        assertTrue("a metric top is not the composite shape", new CompositeRule().tryRewrite(context(metricTop, qsc)).isEmpty());
        assertTrue("a bucket top is not the composite shape", new CompositeRule().tryRewrite(context(bucketTop, qsc)).isEmpty());
    }

    public void testCompositeWithUnmappedSourceReturnsEmpty() {
        // The mock context maps k, d and v only, so the source's field
        // resolution refuses and the request falls through.
        CompositeAggregationBuilder builder = composite(
            new TermsValuesSourceBuilder("k").field("k"),
            new TermsValuesSourceBuilder("u").field("unmapped")
        );

        Optional<PushdownPlan> out = new CompositeRule().tryRewrite(context(tree(builder), mappedContext()));

        assertTrue("a composite with an unmapped source column resolves to no plan", out.isEmpty());
    }

    public void testAfterKeyIsHonored() {
        // One day in millis: the date source's after value parses raw,
        // the interval division is the executor's job.
        long day = 86_400_000L;
        CompositeAggregationBuilder builder = fixtureComposite().aggregateAfter(Map.of("k", "c1", "d", day));

        Optional<PushdownPlan> out = new CompositeRule().tryRewrite(context(tree(builder), mappedContext()));

        assertTrue(out.isPresent());
        List<Comparable<?>> after = PlannerTestPlans.compositeAfterValues(out.get().asLegacyPlan());
        assertEquals("one parsed after value per source, in source order", List.of(new BytesRef("c1"), day), after);
    }

    public void testCompositeMissingBucketIsNotMatched() {
        // A missing_bucket source opens a bucket for rows without a
        // value, which the scan's group by does not produce, so the
        // shape check refuses the whole composite.
        CompositeAggregationBuilder builder = composite(
            new TermsValuesSourceBuilder("k").field("k").missingBucket(true),
            new DateHistogramValuesSourceBuilder("d").field("d").fixedInterval(DateHistogramInterval.days(1))
        );

        Optional<PushdownPlan> out = new CompositeRule().tryRewrite(context(tree(builder), mappedContext()));

        assertTrue("a composite with a missing_bucket source resolves to no plan", out.isEmpty());
    }

    public void testRuleEncodesThePinnedScan() {
        Optional<PushdownPlan> out = new CompositeRule().tryRewrite(context(tree(fixtureComposite()), mappedContext()));

        assertTrue(out.isPresent());
        assertArrayEquals("the rule must encode the pinned wire form", WIRE_FIXTURE, bytesOf(out.get().asLegacyPlan()));
    }
}
