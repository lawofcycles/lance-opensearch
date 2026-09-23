/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One test per aggregation shape the pushdown refuses today, asserting
 * the translator throws {@link UnsupportedOperationException} with a
 * message naming the element; the explain endpoint returns the message
 * in its 400 body, so the messages are part of the endpoint's contract.
 */
public class AggregationToRelRefusalTests extends OpenSearchTestCase {

    private static String messageOf(String json) throws IOException {
        SearchSourceBuilder source = PlanTestFixtures.parse(json);
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> PlanTestFixtures.translate(source));
        return e.getMessage();
    }

    public void testScriptOnTermsThrows() throws IOException {
        assertEquals(
            "script on aggregation [t]",
            messageOf("{\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"category\",\"script\":{\"source\":\"doc.x\"}}}}}")
        );
    }

    public void testMetricScriptThrows() throws IOException {
        assertEquals(
            "script on aggregation [s]",
            messageOf("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\",\"script\":{\"source\":\"doc.x\"}}}}}")
        );
    }

    public void testMetricMissingThrows() throws IOException {
        assertEquals(
            "missing on aggregation [s]",
            messageOf("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\",\"missing\":0}}}}")
        );
    }

    public void testMetricValueTypeThrows() throws IOException {
        assertEquals(
            "value_type on aggregation [s]",
            messageOf("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\",\"value_type\":\"double\"}}}}")
        );
    }

    public void testTopHitsIsRefusedForTheFragmentPath() throws IOException {
        assertRefusedOutright(
            "aggregation type [top_hits] on [h] is not supported for Lance-backed indices: "
                + "it builds its hits through the search context's fetch phase, which the fragment executor's context does not carry",
            "{\"size\":0,\"aggs\":{\"h\":{\"top_hits\":{\"size\":1}}}}"
        );
    }

    public void testGlobalIsRefusedForTheFragmentPath() throws IOException {
        String message = refusalOf("{\"size\":0,\"aggs\":{\"g\":{\"global\":{},\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}}}");
        assertTrue(message, message.startsWith("aggregation type [global] on [g] is not supported for Lance-backed indices: "));
        assertTrue(message, message.contains("every document of the index"));
    }

    public void testRareTermsIsRefusedForTheFragmentPath() throws IOException {
        String message = refusalOf("{\"size\":0,\"aggs\":{\"r\":{\"rare_terms\":{\"field\":\"category\"}}}}");
        assertTrue(message, message.startsWith("aggregation type [rare_terms] on [r] is not supported for Lance-backed indices: "));
        assertTrue(message, message.contains("shard id"));
    }

    public void testSignificantTermsAndTextAreRefusedForTheFragmentPath() throws IOException {
        String terms = refusalOf("{\"size\":0,\"aggs\":{\"st\":{\"significant_terms\":{\"field\":\"category\"}}}}");
        assertTrue(terms, terms.startsWith("aggregation type [significant_terms] on [st] is not supported for Lance-backed indices: "));
        assertTrue(terms, terms.contains("background frequencies"));
        String text = refusalOf("{\"size\":0,\"aggs\":{\"sx\":{\"significant_text\":{\"field\":\"body\"}}}}");
        assertTrue(text, text.startsWith("aggregation type [significant_text] on [sx] is not supported for Lance-backed indices: "));
    }

    public void testRefusedBuilderBelowABucketRefusesTheTree() throws IOException {
        String message = refusalOf(
            "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"h\":{\"top_hits\":{\"size\":1}}}}}}"
        );
        assertTrue(message, message.startsWith("aggregation type [top_hits] on [h]"));
    }

    public void testRefusedBuilderNextToHitsRefusesTheRequest() throws IOException {
        String message = refusalOf("{\"size\":5,\"aggs\":{\"g\":{\"global\":{}}}}");
        assertTrue(message, message.startsWith("aggregation type [global] on [g]"));
    }

    /**
     * The shapes the pushdown does not spell but the fragment executors
     * serve through the aggregators: the translator throws
     * {@link UnsupportedOperationException} (not the 400), naming the
     * type, so the request runs the aggregators over the bare query.
     */
    public void testServedShapesOutsideThePushdownNameTheType() throws IOException {
        assertEquals("aggregation type [sampler]", messageOf("{\"size\":0,\"aggs\":{\"sm\":{\"sampler\":{\"shard_size\":10}}}}"));
        assertEquals(
            "aggregation type [diversified_sampler]",
            messageOf("{\"size\":0,\"aggs\":{\"ds\":{\"diversified_sampler\":{\"shard_size\":10,\"field\":\"category\"}}}}")
        );
        assertEquals("aggregation type [nested]", messageOf("{\"size\":0,\"aggs\":{\"n\":{\"nested\":{\"path\":\"body\"}}}}"));
        assertEquals("aggregation type [reverse_nested]", messageOf("{\"size\":0,\"aggs\":{\"b\":{\"reverse_nested\":{}}}}"));
        assertEquals(
            "aggregation type [multi_terms]",
            messageOf("{\"size\":0,\"aggs\":{\"mt\":{\"multi_terms\":{\"terms\":[{\"field\":\"category\"},{\"field\":\"flag\"}]}}}}")
        );
        assertEquals(
            "aggregation type [scripted_metric]",
            messageOf(
                "{\"size\":0,\"aggs\":{\"sm\":{\"scripted_metric\":{\"map_script\":\"state.n = 1\",\"combine_script\":\"return state.n\","
                    + "\"reduce_script\":\"return 1\"}}}}"
            )
        );
        assertEquals(
            "aggregation type [geo_centroid]",
            messageOf("{\"size\":0,\"aggs\":{\"gc\":{\"geo_centroid\":{\"field\":\"category\"}}}}")
        );
        assertEquals(
            "aggregation type [geo_distance]",
            messageOf(
                "{\"size\":0,\"aggs\":{\"gd\":{\"geo_distance\":{\"field\":\"category\",\"origin\":\"35,139\",\"ranges\":[{\"to\":10}]}}}}"
            )
        );
        assertEquals(
            "aggregation type [weighted_avg]",
            messageOf(
                "{\"size\":0,\"aggs\":{\"w\":{\"weighted_avg\":{\"value\":{\"field\":\"price\"},\"weight\":{\"field\":\"rating\"}}}}}"
            )
        );
        assertEquals(
            "query type [lance_match] in filter of aggregation [f]",
            messageOf("{\"size\":0,\"aggs\":{\"f\":{\"filter\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}}}")
        );
    }

    public void testSamplerThrows() throws IOException {
        assertEquals("aggregation type [sampler]", messageOf("{\"size\":0,\"aggs\":{\"sm\":{\"sampler\":{\"shard_size\":10}}}}"));
    }

    public void testNestedThrows() throws IOException {
        assertEquals("aggregation type [nested]", messageOf("{\"size\":0,\"aggs\":{\"n\":{\"nested\":{\"path\":\"body\"}}}}"));
    }

    private static String refusalOf(String json) throws IOException {
        SearchSourceBuilder source = PlanTestFixtures.parse(json);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PlanTestFixtures.translate(source));
        return e.getMessage();
    }

    private static void assertRefusedOutright(String message, String json) throws IOException {
        assertEquals(message, refusalOf(json));
    }

    public void testPipelineThrows() throws IOException {
        assertEquals(
            "pipeline aggregation",
            messageOf(
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"1d\"},"
                    + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}},\"cs\":{\"cumulative_sum\":{\"buckets_path\":\"s\"}}}}}}"
            )
        );
    }

    public void testCountAscTermsThrows() throws IOException {
        String message = messageOf("{\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"category\",\"order\":{\"_count\":\"asc\"}}}}}");
        assertTrue("names the order: " + message, message.startsWith("terms order ["));
        assertTrue("names the aggregation: " + message, message.endsWith("on aggregation [t]"));
    }

    public void testSubAggregationOrderOnNestedTermsThrows() throws IOException {
        String message = messageOf(
            "{\"size\":0,\"aggs\":{\"outer\":{\"terms\":{\"field\":\"category\"},"
                + "\"aggs\":{\"inner\":{\"terms\":{\"field\":\"body.raw\",\"order\":{\"m\":\"desc\"}},"
                + "\"aggs\":{\"m\":{\"avg\":{\"field\":\"price\"}}}}}}}}"
        );
        assertEquals("terms order [m] on aggregation [inner]", message);
    }

    public void testMinDocCountThrows() throws IOException {
        assertEquals(
            "min_doc_count [2] on aggregation [t]",
            messageOf("{\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"category\",\"min_doc_count\":2}}}}")
        );
    }

    public void testIncludeExcludeThrows() throws IOException {
        assertEquals(
            "include/exclude on aggregation [t]",
            messageOf("{\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"category\",\"include\":\"a.*\"}}}}")
        );
    }

    public void testMissingOnTermsThrows() throws IOException {
        assertEquals(
            "missing on aggregation [t]",
            messageOf("{\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"category\",\"missing\":\"none\"}}}}")
        );
    }

    public void testHdrPercentilesThrows() throws IOException {
        assertEquals(
            "hdr percentiles on aggregation [p]",
            messageOf(
                "{\"size\":0,\"aggs\":{\"p\":{\"percentiles\":{\"field\":\"price\",\"hdr\":{\"number_of_significant_value_digits\":3}}}}}"
            )
        );
    }

    public void testSecondCardinalityThrows() throws IOException {
        assertEquals(
            "second cardinality aggregation [c2]",
            messageOf(
                "{\"size\":0,\"aggs\":{\"c1\":{\"cardinality\":{\"field\":\"category\"}},\"c2\":{\"cardinality\":{\"field\":\"rating\"}}}}"
            )
        );
    }

    public void testTooManyRangesThrows() throws IOException {
        StringBuilder ranges = new StringBuilder();
        for (int i = 0; i < 63; i++) {
            if (i > 0) {
                ranges.append(',');
            }
            ranges.append("{\"from\":").append(i).append(",\"to\":").append(i + 1).append('}');
        }
        assertEquals(
            "more than 62 ranges on aggregation [r]",
            messageOf("{\"size\":0,\"aggs\":{\"r\":{\"range\":{\"field\":\"price\",\"ranges\":[" + ranges + "]}}}}")
        );
    }

    public void testBucketTreeDeeperThanThreeLevelsThrows() throws IOException {
        assertEquals(
            "bucket tree deeper than 3 levels",
            messageOf(
                "{\"size\":0,\"aggs\":{\"l1\":{\"terms\":{\"field\":\"category\"},"
                    + "\"aggs\":{\"l2\":{\"terms\":{\"field\":\"body.raw\"},"
                    + "\"aggs\":{\"l3\":{\"terms\":{\"field\":\"rating\"},"
                    + "\"aggs\":{\"l4\":{\"terms\":{\"field\":\"id\"}}}}}}}}}}"
            )
        );
    }

    public void testTwoBucketsUnderOneParentThrows() throws IOException {
        assertEquals(
            "two bucket aggregations under [t]",
            messageOf(
                "{\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"category\"},"
                    + "\"aggs\":{\"a\":{\"terms\":{\"field\":\"rating\"}},\"b\":{\"terms\":{\"field\":\"id\"}}}}}}"
            )
        );
    }

    public void testTwoTopLevelBucketAggregationsThrow() throws IOException {
        assertEquals(
            "two top level aggregations",
            messageOf("{\"size\":0,\"aggs\":{\"a\":{\"terms\":{\"field\":\"category\"}},\"s\":{\"sum\":{\"field\":\"price\"}}}}")
        );
    }

    public void testCompositeMissingBucketThrows() throws IOException {
        assertEquals(
            "missing_bucket on composite source [cat]",
            messageOf(
                "{\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"sources\":"
                    + "[{\"cat\":{\"terms\":{\"field\":\"category\",\"missing_bucket\":true}}}]}}}}"
            )
        );
    }

    public void testCompositeHistogramSourceThrows() throws IOException {
        String message = messageOf(
            "{\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"sources\":[{\"h\":{\"histogram\":{\"field\":\"price\",\"interval\":10}}}]}}}}"
        );
        assertTrue("names the source: " + message, message.startsWith("composite source type [") && message.endsWith("on source [h]"));
    }

    public void testCompositeCalendarMonthSourceThrows() throws IOException {
        assertEquals(
            "interval on composite source [d] is not a fixed length",
            messageOf(
                "{\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"sources\":"
                    + "[{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\"}}}]}}}}"
            )
        );
    }

    public void testCompositeBucketChildThrows() throws IOException {
        assertEquals(
            "aggregation type [terms] under composite [c]",
            messageOf(
                "{\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"sources\":[{\"cat\":{\"terms\":{\"field\":\"category\"}}}]},"
                    + "\"aggs\":{\"t\":{\"terms\":{\"field\":\"rating\"}}}}}}"
            )
        );
    }

    public void testHistogramOffsetThrows() throws IOException {
        assertEquals(
            "offset on aggregation [h]",
            messageOf("{\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"price\",\"interval\":10,\"offset\":2}}}}")
        );
    }

    public void testHistogramExtendedBoundsThrows() throws IOException {
        assertEquals(
            "extended_bounds on aggregation [h]",
            messageOf(
                "{\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"price\",\"interval\":10,"
                    + "\"extended_bounds\":{\"min\":0,\"max\":100}}}}}"
            )
        );
    }

    public void testHistogramHardBoundsThrows() throws IOException {
        assertEquals(
            "hard_bounds on aggregation [h]",
            messageOf(
                "{\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"price\",\"interval\":10,"
                    + "\"hard_bounds\":{\"min\":0,\"max\":100}}}}}"
            )
        );
    }

    public void testDateHistogramOffsetThrows() throws IOException {
        assertEquals(
            "offset on aggregation [d]",
            messageOf("{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"1d\",\"offset\":\"1h\"}}}}")
        );
    }

    public void testDateHistogramTimeZoneThrows() throws IOException {
        assertEquals(
            "time_zone on aggregation [d]",
            messageOf(
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":"
                    + "{\"field\":\"ts\",\"fixed_interval\":\"1d\",\"time_zone\":\"Asia/Tokyo\"}}}}"
            )
        );
    }

    public void testCalendarIntervalOnDateColumnThrows() throws IOException {
        assertEquals(
            "calendar_interval on column [day] behind aggregation field [day] (needs a timestamp column in UTC)",
            messageOf("{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"day\",\"calendar_interval\":\"month\"}}}}")
        );
    }

    public void testFilterMatchQueryThrows() throws IOException {
        assertEquals(
            "query type [match] in filter of aggregation [f]",
            messageOf("{\"size\":0,\"aggs\":{\"f\":{\"filter\":{\"match\":{\"category\":\"a\"}}}}}")
        );
    }

    public void testFilterMinimumShouldMatchThrows() throws IOException {
        assertEquals(
            "minimum_should_match in filter of aggregation [f]",
            messageOf(
                "{\"size\":0,\"aggs\":{\"f\":{\"filter\":{\"bool\":{\"should\":"
                    + "[{\"term\":{\"category\":\"a\"}},{\"term\":{\"category\":\"b\"}}],\"minimum_should_match\":2}}}}}"
            )
        );
    }

    public void testHistogramOnDateColumnThrows() throws IOException {
        assertEquals(
            "column [ts] behind aggregation field [ts] is not numeric",
            messageOf("{\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"ts\",\"interval\":10}}}}")
        );
    }

    public void testDateRangeOnNumericColumnThrows() throws IOException {
        assertEquals(
            "column [price] behind aggregation field [price] is not a date",
            messageOf("{\"size\":0,\"aggs\":{\"dr\":{\"date_range\":{\"field\":\"price\",\"ranges\":[{\"to\":\"2024-01-01\"}]}}}}")
        );
    }

    /**
     * A model whose mapping recorded that the Lance table renamed
     * {@code old_price} to {@code price}: the stale name is refused with
     * the rename, everywhere field resolution runs.
     */
    private static LanceSchemas.IndexModel renamedModel() {
        LinkedHashMap<String, String> bodySubs = new LinkedHashMap<>();
        bodySubs.put("raw", "keyword");
        return LanceSchemas.model("idx", PlanTestFixtures.SCHEMA, Map.of("body", bodySubs), Map.of("old_price", "price"), () -> 512L);
    }

    private static String renamedMessageOf(String json) throws IOException {
        SearchSourceBuilder source = PlanTestFixtures.parse(json);
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> SearchRequestToRel.translate(source, renamedModel(), PlanTestFixtures.factory())
        );
        return e.getMessage();
    }

    public void testRenamedFieldOnMetricNamesTheRename() throws IOException {
        assertEquals(
            "field [old_price] was renamed to [price] in the Lance table",
            renamedMessageOf("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"old_price\"}}}}")
        );
    }

    public void testRenamedFieldOnBucketNamesTheRename() throws IOException {
        assertEquals(
            "field [old_price] was renamed to [price] in the Lance table",
            renamedMessageOf("{\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"old_price\"}}}}")
        );
    }

    public void testRenamedFieldInFilterNamesTheRename() throws IOException {
        assertEquals(
            "field [old_price] was renamed to [price] in the Lance table",
            renamedMessageOf("{\"size\":0,\"aggs\":{\"f\":{\"filter\":{\"range\":{\"old_price\":{\"gte\":1}}}}}}")
        );
    }

    public void testUnknownFieldWithoutRenameKeepsGenericMessage() throws IOException {
        assertEquals(
            "field [absent] does not map to a Lance column",
            renamedMessageOf("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"absent\"}}}}")
        );
    }
}
