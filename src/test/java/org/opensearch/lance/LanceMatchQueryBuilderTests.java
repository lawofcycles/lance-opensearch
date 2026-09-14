/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import org.lance.ipc.FullTextQuery;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

public class LanceMatchQueryBuilderTests extends OpenSearchTestCase {

    public void testStreamRoundTrip() throws Exception {
        LanceMatchQueryBuilder original = new LanceMatchQueryBuilder("body", "quick brown fox");
        original.operator(FullTextQuery.Operator.AND).boost(1.5f);
        original.queryName("hit");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceMatchQueryBuilder copy = new LanceMatchQueryBuilder(in);
                assertEquals(original, copy);
                assertEquals(original.hashCode(), copy.hashCode());
            }
        }
    }

    public void testFromXContentDefaultOperatorIsOr() throws Exception {
        String json = "{\"field\":\"body\",\"query\":\"quick brown\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            LanceMatchQueryBuilder builder = LanceMatchQueryBuilder.fromXContent(parser);
            assertEquals("body", builder.field());
            assertEquals("quick brown", builder.query());
            assertEquals(FullTextQuery.Operator.OR, builder.operator());
        }
    }

    public void testFromXContentReadsAndOperator() throws Exception {
        String json = "{\"field\":\"body\",\"query\":\"quick brown\",\"operator\":\"and\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            LanceMatchQueryBuilder builder = LanceMatchQueryBuilder.fromXContent(parser);
            assertEquals(FullTextQuery.Operator.AND, builder.operator());
        }
    }

    public void testFromXContentRejectsUnknownOperator() throws Exception {
        // 'xor' is not a Lance FTS operator; surface it as 400 rather than
        // silently degrading to the default.
        String json = "{\"field\":\"body\",\"query\":\"quick\",\"operator\":\"xor\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("operator"));
        }
    }

    public void testFromXContentRejectsUnknownParameter() throws Exception {
        // Rejecting fuzziness / minimum_should_match / prefix_length is
        // deliberate: Lance's Java SDK does not expose them today, and
        // silently accepting them would be worse than a clear 400.
        String json = "{\"field\":\"body\",\"query\":\"quick\",\"fuzziness\":\"AUTO\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("fuzziness"));
        }
    }

    public void testFromXContentRejectsMissingField() throws Exception {
        String json = "{\"query\":\"quick\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("field"));
        }
    }

    public void testFromXContentRejectsMissingQuery() throws Exception {
        String json = "{\"field\":\"body\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Exception e = expectThrows(Exception.class, () -> LanceMatchQueryBuilder.fromXContent(parser));
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("query"));
        }
    }

    public void testToXContentIncludesOperatorWhenAnd() throws Exception {
        LanceMatchQueryBuilder builder = new LanceMatchQueryBuilder("body", "hello");
        builder.operator(FullTextQuery.Operator.AND);
        try (org.opensearch.core.xcontent.XContentBuilder out = JsonXContent.contentBuilder()) {
            builder.toXContent(out, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            String json = out.toString();
            assertTrue("expected operator in json: " + json, json.contains("\"operator\""));
            assertTrue("expected 'and' in json: " + json, json.contains("and"));
        }
    }

    public void testToXContentOmitsOperatorWhenOrDefault() throws Exception {
        LanceMatchQueryBuilder builder = new LanceMatchQueryBuilder("body", "hello");
        try (org.opensearch.core.xcontent.XContentBuilder out = JsonXContent.contentBuilder()) {
            builder.toXContent(out, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            String json = out.toString();
            assertFalse("did not expect operator in json when default OR: " + json, json.contains("\"operator\""));
        }
    }
}
