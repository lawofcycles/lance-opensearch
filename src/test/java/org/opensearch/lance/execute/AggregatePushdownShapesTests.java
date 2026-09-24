/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.execute;

import java.time.ZoneId;
import java.util.List;

import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesMethod;
import org.opensearch.search.aggregations.pipeline.CumulativeSumPipelineAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The executor side shape predicates of {@link AggregatePushdownShapes}:
 * which builders are metrics the scan computes, which composite sources
 * it groups by, and how a terms order and a date_histogram calendar
 * unit are read back from their builders.
 */
public class AggregatePushdownShapesTests extends OpenSearchTestCase {

    public void testPushdownMetricsOverAPlainField() {
        assertTrue(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.sum("s").field("price")));
        assertTrue(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.avg("a").field("price")));
        assertTrue(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.min("m").field("price")));
        assertTrue(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.max("m").field("price")));
        assertTrue(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.count("c").field("price")));
        assertTrue(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.stats("st").field("price")));
        assertTrue(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.extendedStats("es").field("price").sigma(3d)));
        assertTrue(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.cardinality("card").field("user_id")));
        assertTrue(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.percentiles("p").field("price")));
        assertTrue(
            AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.percentiles("p").field("price").method(PercentilesMethod.TDIGEST))
        );
        assertTrue(
            AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.percentileRanks("pr", new double[] { 10d, 20d }).field("price"))
        );
    }

    public void testHdrPercentilesAndNonMetricsAreNotPushdownMetrics() {
        assertFalse(
            AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.percentiles("p").field("price").method(PercentilesMethod.HDR))
        );
        assertFalse(
            AggregatePushdownShapes.isPushdownMetric(
                AggregationBuilders.percentileRanks("pr", new double[] { 10d }).field("price").method(PercentilesMethod.HDR)
            )
        );
        assertFalse(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.terms("t").field("category")));
        assertFalse(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.weightedAvg("w")));
        assertFalse(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.medianAbsoluteDeviation("mad").field("price")));
        assertFalse(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.topHits("th").size(1)));
    }

    public void testMetricsWithScriptsMissingOrHintsAreNotPushdownMetrics() {
        assertFalse(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.sum("s").script(new Script("doc['price'].value"))));
        assertFalse(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.sum("s").field("price").script(new Script("_value * 2"))));
        assertFalse(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.sum("s").field("price").missing(0d)));
        assertFalse(
            AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.sum("s").field("price").userValueTypeHint(ValueType.LONG))
        );
        assertFalse(AggregatePushdownShapes.isPushdownMetric(AggregationBuilders.sum("s").field("")));
    }

    public void testCompositeSourcesOverPlainFields() {
        assertTrue(AggregatePushdownShapes.isPushdownCompositeSource(new TermsValuesSourceBuilder("cat").field("category")));
        assertTrue(
            AggregatePushdownShapes.isPushdownCompositeSource(new TermsValuesSourceBuilder("cat").field("category").order(SortOrder.DESC))
        );
        assertTrue(
            AggregatePushdownShapes.isPushdownCompositeSource(
                new DateHistogramValuesSourceBuilder("d").field("ts").fixedInterval(DateHistogramInterval.hours(6))
            )
        );
        // A calendar unit of a day or shorter rounds like the fixed interval of the same length in UTC.
        assertTrue(
            AggregatePushdownShapes.isPushdownCompositeSource(
                new DateHistogramValuesSourceBuilder("d").field("ts").calendarInterval(DateHistogramInterval.DAY)
            )
        );
        assertTrue(
            AggregatePushdownShapes.isPushdownCompositeSource(
                new DateHistogramValuesSourceBuilder("d").field("ts").calendarInterval(DateHistogramInterval.HOUR)
            )
        );
    }

    public void testCompositeSourcesTheScanDoesNotGroupBy() {
        assertFalse(AggregatePushdownShapes.isPushdownCompositeSource(new HistogramValuesSourceBuilder("h").field("price").interval(10d)));
        assertFalse(
            AggregatePushdownShapes.isPushdownCompositeSource(
                new TermsValuesSourceBuilder("scripted").script(new Script("doc['rating'].value"))
            )
        );
        assertFalse(
            AggregatePushdownShapes.isPushdownCompositeSource(
                new TermsValuesSourceBuilder("scripted").field("rating").script(new Script("_value * 2"))
            )
        );
        assertFalse(
            AggregatePushdownShapes.isPushdownCompositeSource(new TermsValuesSourceBuilder("cat").field("category").missingBucket(true))
        );
        assertFalse(
            AggregatePushdownShapes.isPushdownCompositeSource(
                new TermsValuesSourceBuilder("cat").field("category").userValuetypeHint(ValueType.STRING)
            )
        );
        // Variable length calendar units, an offset and a time zone all shift the buckets away from a fixed rounding.
        assertFalse(
            AggregatePushdownShapes.isPushdownCompositeSource(
                new DateHistogramValuesSourceBuilder("d").field("ts").calendarInterval(DateHistogramInterval.MONTH)
            )
        );
        assertFalse(
            AggregatePushdownShapes.isPushdownCompositeSource(
                new DateHistogramValuesSourceBuilder("d").field("ts").calendarInterval(DateHistogramInterval.WEEK)
            )
        );
        assertFalse(
            AggregatePushdownShapes.isPushdownCompositeSource(
                new DateHistogramValuesSourceBuilder("d").field("ts").fixedInterval(DateHistogramInterval.DAY).offset(3_600_000L)
            )
        );
        assertFalse(
            AggregatePushdownShapes.isPushdownCompositeSource(
                new DateHistogramValuesSourceBuilder("d").field("ts")
                    .fixedInterval(DateHistogramInterval.DAY)
                    .timeZone(ZoneId.of("Asia/Tokyo"))
            )
        );
    }

    public void testCompositeOverPushdownSourcesWithMetricChildren() {
        assertTrue(
            AggregatePushdownShapes.isPushdownComposite(
                new CompositeAggregationBuilder(
                    "c",
                    List.of(new TermsValuesSourceBuilder("cat").field("category"), new TermsValuesSourceBuilder("rating").field("rating"))
                ).size(100)
            )
        );
        assertTrue(
            AggregatePushdownShapes.isPushdownComposite(
                new CompositeAggregationBuilder("c", List.of(new TermsValuesSourceBuilder("cat").field("category"))).subAggregation(
                    AggregationBuilders.sum("s").field("price")
                ).subAggregation(AggregationBuilders.cardinality("card").field("user_id"))
            )
        );
    }

    public void testCompositeWithAnUnsupportedSourceChildOrPipelineIsNotPushed() {
        assertFalse(
            AggregatePushdownShapes.isPushdownComposite(
                new CompositeAggregationBuilder(
                    "c",
                    List.of(
                        new TermsValuesSourceBuilder("cat").field("category"),
                        new HistogramValuesSourceBuilder("p").field("price").interval(10d)
                    )
                )
            )
        );
        assertFalse(
            AggregatePushdownShapes.isPushdownComposite(
                new CompositeAggregationBuilder("c", List.of(new TermsValuesSourceBuilder("cat").field("category"))).subAggregation(
                    AggregationBuilders.terms("t").field("rating")
                )
            )
        );
        assertFalse(
            AggregatePushdownShapes.isPushdownComposite(
                new CompositeAggregationBuilder("c", List.of(new TermsValuesSourceBuilder("cat").field("category"))).subAggregation(
                    AggregationBuilders.percentiles("p").field("price").method(PercentilesMethod.HDR)
                )
            )
        );
        assertFalse(
            AggregatePushdownShapes.isPushdownComposite(
                new CompositeAggregationBuilder("c", List.of(new TermsValuesSourceBuilder("cat").field("category"))).subAggregation(
                    AggregationBuilders.sum("s").field("price")
                ).subAggregation(new CumulativeSumPipelineAggregationBuilder("cs", "s"))
            )
        );
    }

    public void testAggregationOrderReadsThePathAndDirectionOfAPlainOrder() {
        AggregatePushdownShapes.AggregationOrder descending = AggregatePushdownShapes.aggregationOrder(BucketOrder.aggregation("s", false));
        assertNotNull(descending);
        assertEquals("s", descending.path());
        assertFalse(descending.ascending());

        AggregatePushdownShapes.AggregationOrder ascending = AggregatePushdownShapes.aggregationOrder(
            BucketOrder.aggregation("st", "avg", true)
        );
        assertNotNull(ascending);
        assertEquals("st.avg", ascending.path());
        assertTrue(ascending.ascending());
    }

    public void testAggregationOrderSeesThroughTheKeyTieBreakerTheTermsBuilderAdds() {
        // TermsAggregationBuilder.order wraps a sub aggregation order in a compound with _key ascending.
        TermsAggregationBuilder terms = AggregationBuilders.terms("t").field("category").order(BucketOrder.aggregation("s", false));
        AggregatePushdownShapes.AggregationOrder order = AggregatePushdownShapes.aggregationOrder(terms.order());
        assertNotNull(order);
        assertEquals("s", order.path());
        assertFalse(order.ascending());

        assertNotNull(
            AggregatePushdownShapes.aggregationOrder(BucketOrder.compound(BucketOrder.aggregation("s", true), BucketOrder.key(true)))
        );
    }

    public void testAggregationOrderIsNullForKeyCountAndOtherCompounds() {
        assertNull(AggregatePushdownShapes.aggregationOrder(BucketOrder.key(true)));
        assertNull(AggregatePushdownShapes.aggregationOrder(BucketOrder.key(false)));
        assertNull(AggregatePushdownShapes.aggregationOrder(BucketOrder.count(false)));
        assertNull(AggregatePushdownShapes.aggregationOrder(BucketOrder.count(true)));
        // The default terms order is count descending with the key tie breaker.
        assertNull(AggregatePushdownShapes.aggregationOrder(AggregationBuilders.terms("t").field("category").order()));
        // A compound whose second element is not _key ascending, or that holds more than two elements.
        assertNull(
            AggregatePushdownShapes.aggregationOrder(BucketOrder.compound(BucketOrder.aggregation("s", false), BucketOrder.key(false)))
        );
        assertNull(
            AggregatePushdownShapes.aggregationOrder(
                BucketOrder.compound(BucketOrder.aggregation("s", false), BucketOrder.aggregation("a", true))
            )
        );
        assertNull(
            AggregatePushdownShapes.aggregationOrder(
                BucketOrder.compound(BucketOrder.aggregation("s", false), BucketOrder.aggregation("a", true), BucketOrder.key(true))
            )
        );
        assertNull(AggregatePushdownShapes.aggregationOrder(BucketOrder.compound(BucketOrder.count(false), BucketOrder.key(true))));
    }

    public void testCalendarUnitNamesTheDateTruncUnit() {
        assertEquals("second", calendarUnit(DateHistogramInterval.SECOND));
        assertEquals("minute", calendarUnit(DateHistogramInterval.MINUTE));
        assertEquals("hour", calendarUnit(DateHistogramInterval.HOUR));
        assertEquals("day", calendarUnit(DateHistogramInterval.DAY));
        assertEquals("week", calendarUnit(DateHistogramInterval.WEEK));
        assertEquals("month", calendarUnit(DateHistogramInterval.MONTH));
        assertEquals("quarter", calendarUnit(DateHistogramInterval.QUARTER));
        assertEquals("year", calendarUnit(DateHistogramInterval.YEAR));
        // The word spellings map like the 1x spellings.
        assertEquals("day", calendarUnit(new DateHistogramInterval("day")));
        assertEquals("month", calendarUnit(new DateHistogramInterval("month")));
    }

    public void testCalendarUnitIsNullWithoutACalendarInterval() {
        assertNull(AggregatePushdownShapes.calendarUnit(AggregationBuilders.dateHistogram("d").field("ts")));
        assertNull(
            AggregatePushdownShapes.calendarUnit(
                AggregationBuilders.dateHistogram("d").field("ts").fixedInterval(DateHistogramInterval.DAY)
            )
        );
        assertNull(
            AggregatePushdownShapes.calendarUnit(
                AggregationBuilders.dateHistogram("d").field("ts").fixedInterval(DateHistogramInterval.hours(6))
            )
        );
    }

    private static String calendarUnit(DateHistogramInterval interval) {
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("d").field("ts").calendarInterval(interval);
        return AggregatePushdownShapes.calendarUnit(dateHistogram);
    }
}
