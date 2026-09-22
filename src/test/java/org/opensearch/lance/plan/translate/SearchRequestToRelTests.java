/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pins the translation of the supported shape (match_all, size 0, one
 * metric aggregation) to its plan string, and the message of every
 * unsupported element, because the explain endpoint returns those
 * messages in its 400 body.
 */
public class SearchRequestToRelTests extends OpenSearchTestCase {

    private static final Schema FIXTURE = new Schema(
        List.of(
            field("id", new ArrowType.Int(32, true), false),
            field("price", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), true),
            field("body", new ArrowType.Utf8(), true),
            field("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), true)
        )
    );

    private static Field field(String name, ArrowType arrowType, boolean nullable) {
        return new Field(name, new FieldType(nullable, arrowType, null), null);
    }

    private static LanceSchemas.IndexModel model() {
        LinkedHashMap<String, String> bodySubs = new LinkedHashMap<>();
        bodySubs.put("raw", "keyword");
        return LanceSchemas.model("idx", FIXTURE, Map.of("body", bodySubs), () -> 512L);
    }

    private static LancePlannerFactory factory() {
        return new LancePlannerFactory(1L << 30, 1L << 30);
    }

    private static String translate(SearchSourceBuilder source) {
        RelNode rel = SearchRequestToRel.translate(source, model(), factory());
        return RelOptUtil.toString(rel);
    }

    private static String messageOf(SearchSourceBuilder source) {
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> SearchRequestToRel.translate(source, model(), factory())
        );
        return e.getMessage();
    }

    public void testSumTranslates() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.sum("s").field("price"));
        assertEquals("LogicalAggregate(group=[{}], s=[SUM($1)])\n  LanceTableScan(table=[[lance, idx]])\n", translate(source));
    }

    public void testAvgTranslates() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.avg("a").field("price"));
        assertEquals("LogicalAggregate(group=[{}], a=[AVG($1)])\n  LanceTableScan(table=[[lance, idx]])\n", translate(source));
    }

    public void testMinTranslates() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.min("m").field("id"));
        assertEquals("LogicalAggregate(group=[{}], m=[MIN($0)])\n  LanceTableScan(table=[[lance, idx]])\n", translate(source));
    }

    public void testMaxTranslatesOnTimestampColumn() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.max("M").field("ts"));
        assertEquals("LogicalAggregate(group=[{}], M=[MAX($3)])\n  LanceTableScan(table=[[lance, idx]])\n", translate(source));
    }

    public void testValueCountTranslates() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.count("c").field("price"));
        assertEquals("LogicalAggregate(group=[{}], c=[COUNT($1)])\n  LanceTableScan(table=[[lance, idx]])\n", translate(source));
    }

    public void testValueCountOnNonNullableColumnSimplifiesToCountStar() {
        // Calcite's RelBuilder folds COUNT of a non nullable column to
        // COUNT(*), which counts the same rows.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.count("c").field("id"));
        assertEquals("LogicalAggregate(group=[{}], c=[COUNT()])\n  LanceTableScan(table=[[lance, idx]])\n", translate(source));
    }

    public void testExplicitMatchAllTranslates() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .query(QueryBuilders.matchAllQuery())
            .aggregation(AggregationBuilders.sum("s").field("id"));
        assertEquals("LogicalAggregate(group=[{}], s=[SUM($0)])\n  LanceTableScan(table=[[lance, idx]])\n", translate(source));
    }

    public void testNonMatchAllQueryThrows() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .query(QueryBuilders.termQuery("id", 1))
            .aggregation(AggregationBuilders.sum("s").field("price"));
        assertEquals("query type [term]", messageOf(source));
    }

    public void testNonZeroSizeThrows() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(3).aggregation(AggregationBuilders.sum("s").field("price"));
        assertEquals("size [3] (only 0)", messageOf(source));
    }

    public void testDefaultSizeThrows() {
        // A body without size asks for the default ten hits.
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(AggregationBuilders.sum("s").field("price"));
        assertEquals("size [10] (only 0)", messageOf(source));
    }

    public void testFromThrows() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).from(5).aggregation(AggregationBuilders.sum("s").field("price"));
        assertEquals("from [5]", messageOf(source));
    }

    public void testSortThrows() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .sort("id", SortOrder.ASC)
            .aggregation(AggregationBuilders.sum("s").field("price"));
        assertEquals("sort", messageOf(source));
    }

    public void testSearchAfterThrows() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .searchAfter(new Object[] { 1 })
            .aggregation(AggregationBuilders.sum("s").field("price"));
        assertEquals("search_after", messageOf(source));
    }

    public void testPostFilterThrows() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .postFilter(QueryBuilders.termQuery("id", 1))
            .aggregation(AggregationBuilders.sum("s").field("price"));
        assertEquals("post_filter", messageOf(source));
    }

    public void testFetchSourceThrows() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .fetchSource(false)
            .aggregation(AggregationBuilders.sum("s").field("price"));
        assertEquals("_source", messageOf(source));
    }

    public void testNoAggregationsThrows() {
        assertEquals("no aggregations (exactly one metric aggregation)", messageOf(new SearchSourceBuilder().size(0)));
    }

    public void testEmptyBodyThrows() {
        // No body parses to no source; the default size names the first
        // unsupported element.
        assertEquals("size [10] (only 0)", messageOf(null));
    }

    public void testTwoTopLevelAggregationsThrow() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(AggregationBuilders.sum("s").field("price"))
            .aggregation(AggregationBuilders.avg("a").field("price"));
        assertEquals("two top level aggregations", messageOf(source));
    }

    public void testBucketAggregationThrows() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.terms("t").field("id"));
        assertEquals("aggregation type [terms]", messageOf(source));
    }

    public void testMetricOnKeywordColumnNamesTheColumn() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.sum("s").field("body"));
        assertEquals("column [body] behind aggregation field [body] is not numeric", messageOf(source));
    }

    public void testMetricOnKeywordSubFieldNamesTheBaseColumn() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.sum("s").field("body.raw"));
        assertEquals("column [body] behind aggregation field [body.raw] is not numeric", messageOf(source));
    }

    public void testUnknownFieldThrows() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.sum("s").field("nope"));
        assertEquals("field [nope] does not map to a Lance column", messageOf(source));
    }

    public void testMetricWithoutFieldThrows() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.sum("s"));
        assertEquals("aggregation [s] without a field", messageOf(source));
    }
}
