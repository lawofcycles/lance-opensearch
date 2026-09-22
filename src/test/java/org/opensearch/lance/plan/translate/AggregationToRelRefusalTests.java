/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

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

    public void testTopHitsThrows() throws IOException {
        assertEquals("aggregation type [top_hits]", messageOf("{\"size\":0,\"aggs\":{\"h\":{\"top_hits\":{\"size\":1}}}}"));
    }

    public void testSamplerThrows() throws IOException {
        assertEquals("aggregation type [sampler]", messageOf("{\"size\":0,\"aggs\":{\"sm\":{\"sampler\":{\"shard_size\":10}}}}"));
    }

    public void testNestedThrows() throws IOException {
        assertEquals("aggregation type [nested]", messageOf("{\"size\":0,\"aggs\":{\"n\":{\"nested\":{\"path\":\"body\"}}}}"));
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

    public void testFilterWildcardQueryThrows() throws IOException {
        assertEquals(
            "query type [wildcard] in filter of aggregation [f]",
            messageOf("{\"size\":0,\"aggs\":{\"f\":{\"filter\":{\"wildcard\":{\"category\":\"a*\"}}}}}")
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
}
