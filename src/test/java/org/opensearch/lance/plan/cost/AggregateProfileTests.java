/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.cost;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * {@link AggregateProfile#of}: the quantities the model reads off a
 * translated aggregation tree over the perf table fixture, whose
 * statistics carry a bitmap distinct count for {@code category}, an
 * integer range for the BTree over {@code rating} and none for the other
 * numeric columns.
 */
public class AggregateProfileTests extends OpenSearchTestCase {

    private static final LanceSchemas.IndexModel PERF1B = PerfTableFixture.perf1b();

    private static AggregateProfile profile(String body) throws IOException {
        LancePlannerFactory factory = new LancePlannerFactory(1L << 30, 1L << 30);
        RelNode logical = PerfTableFixture.translate(PERF1B, factory, body);
        assertTrue("the body translates to an aggregate: " + logical, logical instanceof LanceAggregate);
        LanceAggregate aggregate = (LanceAggregate) logical;
        RelNode node = aggregate.getInput();
        while (node instanceof Project || node instanceof Filter) {
            node = node.getInput(0);
        }
        RelMetadataQuery mq = aggregate.getCluster().getMetadataQuery();
        return AggregateProfile.of(aggregate, (LanceTableScan) node, mq);
    }

    public void testKeywordTermsReadsTheBitmapDistinctCount() throws IOException {
        AggregateProfile p = profile("{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"}}}}");
        assertEquals(1e9, p.tableRows(), 0.0);
        assertEquals(200.0, p.groups(), 0.0);
        assertTrue("the bitmap distinct count grounds the estimate", p.groupsKnown());
        assertEquals("terms size 10 gives shard_size 25, four times that per scan", 100.0, p.mergedGroups(), 0.0);
        assertEquals(1, p.columnsRead());
        assertEquals(
            "a bitmap indexed string is dictionary encoded",
            CostCoefficients.DICTIONARY_STRING_BYTES_PER_ROW,
            p.bytesPerRow(),
            0.0
        );
        assertEquals(1, p.stringKeys());
        assertEquals(0, p.numericKeys());
        assertEquals(1, p.scanPasses());
        assertEquals(0, p.simpleMetrics());
        assertFalse(p.composite());
        assertFalse(p.filtered());
        assertFalse(p.largeGroups());
    }

    public void testNumericTermsWithoutADistinctCountTakesCalcitesShare() throws IOException {
        // price has a BTree whose bounds are floats, which enclose no
        // finite set of values, so the key's domain is guessed.
        AggregateProfile p = profile("{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"price\"}}}}");
        assertEquals(1e9 * CostCoefficients.UNKNOWN_KEY_DISTINCT_SHARE, p.groups(), 0.0);
        assertFalse("a guessed domain is not a known group count", p.groupsKnown());
        assertTrue(p.largeGroups());
        assertEquals(100.0, p.mergedGroups(), 0.0);
        assertEquals(1, p.numericKeys());
        assertEquals(0, p.stringKeys());
        assertEquals(8.0, p.bytesPerRow(), 0.0);
    }

    public void testIntegerTermsReadsTheBTreeRangeAsItsDomain() throws IOException {
        // rating holds the integers 1 to 5: the BTree's min and max
        // bound the key to five values on a billion rows, where the
        // guess would be a hundred million.
        AggregateProfile p = profile("{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"rating\"}}}}");
        assertEquals(PerfTableFixture.RATING_VALUES, p.groups(), 0.0);
        assertTrue("the BTree range grounds the estimate", p.groupsKnown());
        assertFalse(p.largeGroups());
        assertEquals("five groups need no top-k cut", PerfTableFixture.RATING_VALUES, p.mergedGroups(), 0.0);
        assertEquals(1, p.numericKeys());
        assertEquals(4.0, p.bytesPerRow(), 0.0);
    }

    public void testMetricOnlySum() throws IOException {
        AggregateProfile p = profile("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}");
        assertEquals(1.0, p.groups(), 0.0);
        assertEquals(1.0, p.mergedGroups(), 0.0);
        assertTrue("no key, one group", p.groupsKnown());
        assertEquals(1, p.columnsRead());
        assertEquals(8.0, p.bytesPerRow(), 0.0);
        assertEquals(1, p.simpleMetrics());
        assertEquals(0, p.nestedLevels());
        assertFalse(p.bucketed());
        assertEquals("a metric with no bucket key above it accumulates in place", 0, p.bucketedMetrics());
    }

    public void testDateHistogramWithMetricReadsTwoColumns() throws IOException {
        AggregateProfile p = profile(
            "{\"size\":0,\"aggs\":{\"h\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\"},\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}}}"
        );
        assertEquals(2, p.columnsRead());
        assertEquals(16.0, p.bytesPerRow(), 0.0);
        assertEquals(1, p.dateKeys());
        assertEquals("ten assumed years of months", 120.0, p.groups(), 0.0);
        assertTrue("an interval bounds the buckets whatever the row count", p.groupsKnown());
        assertEquals(1, p.simpleMetrics());
        assertTrue(p.bucketed());
        assertEquals("the sum under the histogram is collected per bucket", 1, p.bucketedMetrics());
    }

    public void testFixedDateHistogramBucketsFollowTheInterval() throws IOException {
        AggregateProfile p = profile("{\"size\":0,\"aggs\":{\"h\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"1d\"}}}}");
        assertEquals(1, p.dateKeys());
        assertEquals(CostCoefficients.DATE_SPAN_ASSUMED_YEARS * 365.25, p.groups(), 0.5);
    }

    public void testNestedTermsCountsLevelsAndColumns() throws IOException {
        AggregateProfile p = profile(
            "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"price\"}}}}}}}}"
        );
        assertEquals(3, p.columnsRead());
        assertEquals(2 + 4 + 8, p.bytesPerRow(), 0.0);
        assertEquals(1, p.stringKeys());
        assertEquals(1, p.numericKeys());
        assertEquals(1, p.nestedLevels());
        assertEquals("the levels' domains multiply: 200 categories times 5 ratings", 200 * PerfTableFixture.RATING_VALUES, p.groups(), 0.0);
        assertEquals("a nested tree is not cut per scan", p.groups(), p.mergedGroups(), 0.0);
        assertTrue("both levels are bounded by the statistics", p.groupsKnown());
        assertFalse("a thousand groups stay under the hash table threshold", p.largeGroups());
        assertEquals(1, p.simpleMetrics());
    }

    public void testNestedTermsOverAGuessedDomainIsCappedAtTheRows() throws IOException {
        // price has no bound, so its level is a tenth of the rows and
        // the product with the 200 categories is capped at the table.
        AggregateProfile p = profile(
            "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"p\":{\"terms\":{\"field\":\"price\"}}}}}}"
        );
        assertEquals(1e9, p.groups(), 0.0);
        assertFalse(p.groupsKnown());
        assertTrue(p.largeGroups());
    }

    public void testRangeAndFiltersKeys() throws IOException {
        AggregateProfile range = profile(
            "{\"size\":0,\"aggs\":{\"r\":{\"range\":{\"field\":\"price\",\"ranges\":[{\"to\":20},{\"from\":20,\"to\":100},{\"from\":100,\"to\":500},{\"from\":500}]}}}}"
        );
        assertEquals(1, range.rangeKeys());
        assertEquals(4.0, range.groups(), 0.0);
        AggregateProfile filters = profile(
            "{\"size\":0,\"aggs\":{\"f\":{\"filters\":{\"filters\":{\"r5\":{\"term\":{\"rating\":5}},\"c\":{\"term\":{\"category\":\"cat150\"}},\"hi\":{\"range\":{\"price\":{\"gte\":500}}}}}}}}"
        );
        assertEquals(1, filters.filterKeys());
        assertEquals(3.0, filters.groups(), 0.0);
        assertTrue(range.groupsKnown());
        assertTrue(filters.groupsKnown());
        assertEquals("the three predicates read three columns", 3, filters.columnsRead());
        assertEquals(4 + 2 + 8, filters.bytesPerRow(), 0.0);
    }

    public void testCompositeSources() throws IOException {
        AggregateProfile p = profile(
            "{\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"size\":10,\"sources\":[{\"cat\":{\"terms\":{\"field\":\"category\"}}},{\"r\":{\"terms\":{\"field\":\"rating\"}}}]}}}}"
        );
        assertTrue(p.composite());
        assertEquals(2, p.compositeSources());
        assertEquals(0, p.nestedLevels());
        assertEquals(1, p.stringKeys());
        assertEquals(1, p.numericKeys());
        assertEquals("the sources' domains multiply", 200 * PerfTableFixture.RATING_VALUES, p.groups(), 0.0);
        assertTrue(p.groupsKnown());
        assertEquals("composite group rows are not cut per scan", p.groups(), p.mergedGroups(), 0.0);
    }

    public void testPercentilesScansTwice() throws IOException {
        AggregateProfile p = profile("{\"size\":0,\"aggs\":{\"p\":{\"percentiles\":{\"field\":\"price\"}}}}");
        assertTrue(p.percentiles());
        assertEquals(2, p.scanPasses());
        assertEquals(0, p.simpleMetrics());
    }

    public void testCardinalityWithoutADistinctCountAssumesEveryRowDistinct() throws IOException {
        AggregateProfile p = profile("{\"size\":0,\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"user_id\"}}}}");
        assertTrue(p.cardinality());
        assertEquals(1e9, p.cardinalityDistinct(), 0.0);
        assertTrue("a billion distinct values are hashed row by row", p.cardinalityHashesEveryRow());
        AggregateProfile indexed = profile("{\"size\":0,\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"category\"}}}}");
        assertEquals(200.0, indexed.cardinalityDistinct(), 0.0);
        assertFalse("two hundred distinct values are collected through the ordinals", indexed.cardinalityHashesEveryRow());
    }

    public void testQueryFilterLowersTheSelectivityAndAddsItsColumn() throws IOException {
        AggregateProfile p = profile(
            "{\"size\":0,\"query\":{\"term\":{\"rating\":5}},\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"}}}}"
        );
        assertTrue(p.filtered());
        assertTrue(p.filterSelectivity() > 0.0 && p.filterSelectivity() < 1.0);
        assertEquals("the filter column and the key column", 2, p.columnsRead());
        assertEquals(4 + 2, p.bytesPerRow(), 0.0);
    }
}
